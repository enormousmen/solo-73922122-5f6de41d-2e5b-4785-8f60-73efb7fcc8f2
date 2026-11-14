#!/usr/bin/env python3
"""HTTP end-to-end test for GET /verify/{dev}/{inode}.

Starts the real FastAPI service (background collector enabled), then:

1. verifies a healthy tracked file over HTTP -> status "verified"
2. builds a controllable rotation: old archive keeps draining, a new
   access.log appears and gets more traffic; the archive still verifies and
   the new file's bytes are never compared against the archive
3. appends uncollected lines + an unterminated tail -> still "verified"
4. deletes one committed row in a separate connection -> "mismatch" with the
   byte offset listed
5. removes the original from the shared directory -> "unverified", passed=false
6. unknown dev/inode -> 404, and the OpenAPI schema exposes the endpoint

Run with ``python -I tests/e2e_verify.py <venv-python-with-deps>`` or directly
from a Python that has fastapi/uvicorn installed.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = tempfile.mkdtemp(prefix="e2e-verify-")
LOG_DIR = os.path.join(BASE, "logs")
DB_PATH = os.path.join(BASE, "data", "warehouse.db")
os.makedirs(LOG_DIR)
os.makedirs(os.path.dirname(DB_PATH))

os.environ["DB_PATH"] = DB_PATH
os.environ["LOG_DIR"] = LOG_DIR
os.environ["POLL_INTERVAL"] = "0.05"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

import uvicorn  # noqa: E402
from app.main import app  # noqa: E402

PORT = 8801
BASE_URL = f"http://127.0.0.1:{PORT}"
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        FAILURES.append(name)


def get(path: str):
    with urllib.request.urlopen(BASE_URL + path, timeout=10) as r:
        return r.status, json.loads(r.read())


def wait_http_ready(timeout: float = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{BASE_URL}/health", timeout=1).read()
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("service did not become ready")


def wait_for_events(n: int, timeout: float = 10) -> dict:
    deadline = time.time() + timeout
    health = {}
    while time.time() < deadline:
        _, health = get("/health")
        if health["counts"]["events"] >= n:
            return health
        time.sleep(0.05)
    return health


def jline(path: str, status: int) -> str:
    return json.dumps({"time": "2026-11-13T10:00:00Z", "path": path,
                       "status": status, "method": "GET"}) + "\n"


def identity(path: str) -> tuple[int, int]:
    st = os.stat(path)
    return st.st_dev, st.st_ino


def verify_http(dev: int, ino: int):
    return get(f"/verify/{dev}/{ino}")


def main() -> int:
    cfg = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning")
    srv = uvicorn.Server(cfg)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    wait_http_ready()
    try:
        cur = os.path.join(LOG_DIR, "access.log")
        with open(cur, "w") as fh:
            for i in range(12):
                fh.write(jline("/api/orders", 200))
            fh.write(jline("/api/orders", 500))
            fh.write("{broken json\n")
            fh.write(jline("/api/payments", 503))
        wait_for_events(14)
        dev, ino = identity(cur)
        st, rep = verify_http(dev, ino)
        check("HTTP 200 for tracked file", st == 200, str(st))
        check("healthy file verified over HTTP",
              rep["status"] == "verified" and rep["passed"] is True,
              str(rep.get("status")))
        check("normal + malformed records compared",
              rep["counts"]["normal_records"] == 14
              and rep["counts"]["malformed_records"] == 1, str(rep["counts"]))
        check("both tables in one line total", rep["boundary"]["line_count"] == 15,
              str(rep["boundary"]))

        print("[B] rotation: archive by inode, new file not confused")
        archive = os.path.join(LOG_DIR, "access.log.incident-1.log")
        os.rename(cur, archive)
        arch_fh = open(archive, "a")     # keep old inode alive, like nginx
        arch_fh.write(jline("/late/in-archive", 502))
        arch_fh.flush()
        with open(cur, "w") as fh:
            for i in range(5):
                fh.write(jline("/api/new", 200))
        deadline = time.time() + 10
        while time.time() < deadline:
            _, p = get("/ingest/progress")
            codes = {f["source_code"]: f for f in p["files"]}
            if ("access.log.incident-1.log" in codes
                    and "access.log" in codes
                    and all(f["complete"] for f in p["files"])):
                break
            time.sleep(0.05)
        dev_old, ino_old = identity(archive)
        st, rep_arch = verify_http(dev_old, ino_old)
        check("archive verified via its own inode after rotation",
              st == 200 and rep_arch["status"] == "verified", str(rep_arch))
        check("archive source code is the archive name",
              rep_arch["source_code"] == "access.log.incident-1.log",
              rep_arch["source_code"])
        check("archive includes the late line",
              rep_arch["counts"]["original_lines"] == 16, str(rep_arch["counts"]))
        dev_new, ino_new = identity(cur)
        check("new access.log has a new identity", ino_new != ino_old)
        _, rep_new = verify_http(dev_new, ino_new)
        check("new file verifies against its own batch",
              rep_new["status"] == "verified"
              and rep_new["counts"]["original_lines"] == 5, str(rep_new["counts"]))

        print("[C] uncollected appends and unterminated tail are ignored")
        before = len(open(archive, "rb").read())
        with open(archive, "a") as fh:
            fh.write(jline("/not/collected-yet", 500))
            fh.write('{"path":"/dangling","status":5')
        _, rep2 = verify_http(dev_old, ino_old)
        check("still verified with uncollected/new-tail bytes present",
              rep2["status"] == "verified", str(rep2.get("reason")))
        check("boundary unchanged; extra bytes ignored",
              rep2["boundary"]["committed_offset"] == before
              and rep2["boundary"]["bytes_after_boundary_ignored"] > 0,
              str(rep2["boundary"]))

        print("[D] tampered warehouse -> mismatch with byte offsets")
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            row = conn.execute(
                "SELECT id, byte_offset FROM log_events "
                "WHERE dev=? AND inode=? ORDER BY byte_offset LIMIT 1",
                (dev_old, ino_old),
            ).fetchone()
            assert row is not None
            conn.execute("DELETE FROM log_events WHERE id=?", (row[0],))
            conn.commit()
        finally:
            conn.close()
        _, bad = verify_http(dev_old, ino_old)
        check("mismatch reported", bad["status"] == "mismatch"
              and bad["passed"] is False, str(bad["status"]))
        check("missing line offset matches the deleted row",
              [m["byte_offset"] for m in bad["missing_lines"]] == [row[1]],
              str(bad["missing_lines"]))

        print("[E] original missing -> unverified, never a pass")
        outside = os.path.join(BASE, "archive-away.log")
        os.rename(archive, outside)
        _, gone = verify_http(dev_old, ino_old)
        check("missing original -> unverified", gone["status"] == "unverified",
              str(gone["status"]))
        check("missing original -> passed false", gone["passed"] is False)
        check("reason explains the missing original",
              "original unavailable" in (gone.get("reason") or ""),
              str(gone.get("reason")))
        os.rename(outside, archive)

        print("[F] untracked identity and OpenAPI entry point")
        try:
            verify_http(1, 999999999)
        except urllib.error.HTTPError as exc:
            code = exc.code
        else:
            code = 200
        check("unknown (dev, inode) -> 404", code == 404, str(code))
        try:
            _, schema = get("/openapi.json")
        except Exception:
            with urllib.request.urlopen(BASE_URL + "/openapi.json") as r:
                schema = json.loads(r.read())
        check("OpenAPI exposes /verify/{dev}/{inode}",
              "/verify/{dev}/{inode}" in schema["paths"],
              str(list(schema.get("paths", {}))[:5]))
        with urllib.request.urlopen(BASE_URL + "/docs") as r:
            docs = r.read().decode("utf-8", "replace")
        check("/docs serves Swagger UI", "swagger" in docs.lower())
        arch_fh.close()
    finally:
        srv.should_exit = True
        thread.join(timeout=10)

    print(f"\n==== verify E2E {'ALL PASS' if not FAILURES else f'{len(FAILURES)} FAIL'} ====")
    shutil.rmtree(BASE, ignore_errors=True)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
