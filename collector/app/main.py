"""FastAPI service: continuous ingestion + query API.

Endpoints
---------
GET  /health                 liveness + counters
GET  /ingest/progress        per-file collection progress (offset/size)
POST /ingest/poll-now        force an immediate collection pass
GET  /events                 paginated events, filter by path / status /
                             status range / source code
GET  /events/{event_id}      single event with the raw JSON line
GET  /malformed              quarantined lines that failed JSON parsing
GET  /verify/{dev}/{inode}   read-only integrity check of one tracked file
                             against its committed batch
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import storage, verifier
from .ingest import CollectorLoop, poll_once

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("collector.api")

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
LOG_DIR = os.getenv("LOG_DIR", "/logs")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))


class Health(BaseModel):
    status: str
    log_dir: str
    db_path: str
    poll_interval_seconds: float
    collector_running: bool
    total_polls: int
    total_ingested: int
    counts: dict


class FileProgress(BaseModel):
    source_code: str
    dev: int
    inode: int
    path: str
    offset: int
    size: Optional[int] = None
    complete: bool
    last_seen: str


class ProgressResponse(BaseModel):
    collector_running: bool
    total_polls: int
    total_ingested: int
    last_poll: Optional[dict] = None
    counts: dict
    files: list[FileProgress]


class Event(BaseModel):
    id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    method: Optional[str]
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    raw_line: str
    ingested_at: str


class EventPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    items: list[Event]


class VerificationBoundary(BaseModel):
    committed_offset: int = Field(
        description="核验开始时已提交的续采位置；只核对 [0, offset) 的字节"
    )
    original_size_at_open: Optional[int] = Field(
        None, description="打开原件时看到的文件大小"
    )
    original_size_after_read: Optional[int] = Field(
        None, description="读完已提交区间后看到的大小（期间可能继续追加）"
    )
    line_count: int = Field(0, description="已提交区间内以换行结尾的行数")
    bytes_after_boundary_ignored: Optional[int] = Field(
        None, description="边界之后未参与核对的字节数（未换行尾部/未入仓新行）"
    )


class VerificationCounts(BaseModel):
    original_lines: int = Field(0, description="原件在已提交区间内的完整行数")
    normal_records: int = Field(0, description="log_events 中该物理文件、边界内的记录数")
    malformed_records: int = Field(0, description="malformed_lines 中该物理文件、边界内的记录数")
    matched_lines: int = Field(0, description="逐行原文完全一致的正常记录数")
    missing_lines: int = Field(0, description="原件有完整行、仓库无任何记录的位置数")
    duplicate_positions: int = Field(0, description="同一字节位置存在多条记录的位置数")
    content_mismatches: int = Field(0, description="记录原文与原件不一致的记录数")
    unexpected_records: int = Field(0, description="仓库有记录、已提交区间中无对应行的记录数")


class MissingLine(BaseModel):
    byte_offset: int
    raw_line: str


class RecordRef(BaseModel):
    table: str
    id: int
    source_code: str
    raw_line: str


class DuplicatePosition(BaseModel):
    byte_offset: int
    count: int
    records: list[RecordRef]


class ContentMismatch(BaseModel):
    byte_offset: int
    table: str
    id: int
    source_code: str
    original_raw_line: str
    record_raw_line: str


class UnexpectedRecord(BaseModel):
    byte_offset: int
    table: str
    id: int
    source_code: str
    raw_line: str


class VerificationReport(BaseModel):
    status: str = Field(description="verified | mismatch | unverified")
    passed: bool = Field(description="仅 verified 时为 true；未核实不报通过")
    dev: int
    inode: int
    source_code: str
    source_path: str = Field(description="已提交记录中的来源路径（可能已因轮转而过时）")
    original_path: Optional[str] = Field(None, description="实际按 inode 找到的原件路径")
    original_present_at_end: Optional[bool]
    last_seen: str
    reason: Optional[str] = Field(None, description="未核实的原因")
    started_at: str
    finished_at: Optional[str]
    duration_ms: Optional[float]
    boundary: VerificationBoundary
    counts: VerificationCounts
    missing_lines: list[MissingLine]
    duplicate_positions: list[DuplicatePosition]
    content_mismatches: list[ContentMismatch]
    unexpected_records: list[UnexpectedRecord]
    details_truncated: bool = Field(
        False, description="明细超过 limit 被截断（计数仍为全量）"
    )


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = storage.connect(DB_PATH)
    storage.init_db(conn)

    loop = CollectorLoop(conn, LOG_DIR, interval=POLL_INTERVAL)
    thread = threading.Thread(target=loop.run_forever, name="collector",
                              daemon=True)
    thread.start()
    state.update(conn=conn, loop=loop, thread=thread)
    log.info("API ready; watching %s, db %s", LOG_DIR, DB_PATH)
    try:
        yield
    finally:
        log.info("shutdown requested, stopping collector loop...")
        loop.request_stop()
        thread.join(timeout=10)
        # A checkpoint persists WAL frames into the main db file on exit.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            log.exception("checkpoint on shutdown failed")
        conn.close()
        log.info("shutdown complete")


app = FastAPI(
    title="Nginx 访问日志入仓 API",
    description=(
        "持续采集共享只读目录中的 JSON 行 Nginx 日志，按 (设备, inode) "
        "识别文件、行与续采偏移同事务提交，支持按路径与状态码分页查询故障请求。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _conn():
    return state["conn"]


@app.get("/health", response_model=Health, tags=["meta"])
def health():
    loop: CollectorLoop = state["loop"]
    return Health(
        status="ok",
        log_dir=LOG_DIR,
        db_path=DB_PATH,
        poll_interval_seconds=POLL_INTERVAL,
        collector_running=threading.main_thread().is_alive() and not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        counts=storage.counts(_conn()),
    )


@app.get("/ingest/progress", response_model=ProgressResponse, tags=["ingest"])
def progress():
    conn = _conn()
    loop: CollectorLoop = state["loop"]
    rows = storage.all_states(conn)
    # Current sizes from the live directory, read-only.
    current: dict[tuple[int, int], int] = {}
    try:
        for name in os.listdir(LOG_DIR):
            try:
                st = os.stat(os.path.join(LOG_DIR, name))
            except FileNotFoundError:
                continue
            current[(st.st_dev, st.st_ino)] = st.st_size
    except FileNotFoundError:
        pass

    files = []
    for row in rows:
        size = current.get((row["dev"], row["inode"]))
        files.append(FileProgress(
            source_code=row["source_code"],
            dev=row["dev"],
            inode=row["inode"],
            path=row["path"],
            offset=row["offset"],
            size=size,
            complete=(size is not None and row["offset"] >= size),
            last_seen=row["last_seen"],
        ))
    return ProgressResponse(
        collector_running=not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        last_poll=loop.last_result,
        counts=storage.counts(conn),
        files=files,
    )


@app.post("/ingest/poll-now", tags=["ingest"])
def poll_now():
    """Force an immediate scan/ingest pass (useful for tests/demos)."""
    loop: CollectorLoop = state["loop"]
    with loop.write_lock:
        return poll_once(_conn(), LOG_DIR)


@app.get("/events", response_model=EventPage, tags=["query"])
def list_events(
    path: Optional[str] = Query(None, description="精确路径，例如 /api/orders"),
    status: Optional[int] = Query(None, description="精确状态码，例如 500"),
    status_min: Optional[int] = Query(None, description="状态码下界，例如 400"),
    status_max: Optional[int] = Query(None, description="状态码上界，例如 599"),
    source_code: Optional[str] = Query(None, description="来源文件代号，例如 access.log.20261113T100000.log"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按路径、状态码分页取数。查故障请求示例：
    ``/events?status_min=500&status_max=599&limit=50``。"""
    return storage.query_events(
        _conn(),
        path=path,
        status=status,
        status_min=status_min,
        status_max=status_max,
        source_code=source_code,
        limit=limit,
        offset=offset,
    )


@app.get("/events/{event_id}", response_model=Event, tags=["query"])
def get_event(event_id: int):
    row = _conn().execute(
        "SELECT * FROM log_events WHERE id = ?", (event_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return dict(row)


@app.get("/malformed", tags=["query"])
def list_malformed(limit: int = Query(100, ge=1, le=1000),
                   offset: int = Query(0, ge=0)):
    conn = _conn()
    total = conn.execute(
        "SELECT COUNT(*) c FROM malformed_lines"
    ).fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM malformed_lines ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    return {"total": total, "limit": limit, "offset": offset,
            "items": [dict(r) for r in rows]}


@app.get("/verify/{dev}/{inode}", response_model=VerificationReport,
         tags=["verify"])
def verify_file(
    dev: int,
    inode: int,
    limit: int = Query(
        500, ge=1, le=10000,
        description="每类问题明细最多返回多少条；计数始终为全量",
    ),
):
    """按 (设备号, inode) 核验一个已追踪文件是否仍与原件一致。

    只读核对同一物理文件、且位于核验开始时所提交续采位置之前的记录；
    新文件、轮转后新增来源、尚未换行或尚未入仓的尾部均不参与。
    原件缺失、被截断或核验期间无法稳定读取时返回 ``unverified``
    （``passed=false``），绝不报通过。
    """
    try:
        return verifier.verify_tracked_file(
            DB_PATH, LOG_DIR, dev, inode, detail_limit=limit
        )
    except verifier.NotTrackedError:
        raise HTTPException(
            status_code=404,
            detail=f"no tracked file with dev={dev} inode={inode}; "
                   f"see /ingest/progress for dev/inode values",
        )
