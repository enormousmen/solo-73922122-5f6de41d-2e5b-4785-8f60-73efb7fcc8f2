"""Read-only verification of one tracked source file against the warehouse.

What is verified
================
For a file selected by its physical identity ``(st_dev, st_ino)`` we compare
the *retained original bytes* with the rows committed to SQLite for that same
physical file:

* the normal records in ``log_events`` and
* the quarantined bad lines in ``malformed_lines``.

Boundary rules (important for correctness under concurrency)
------------------------------------------------------------
1. The committed offset stored in ``ingest_state`` is read **once, at the
   start**. It is the hard upper bound: only bytes ``[0, offset)`` of the
   original are read and only records whose ``byte_offset < offset`` take
   part. The collector never commits a row without also advancing the offset
   in the same transaction, so everything below the boundary is one immutable,
   fully committed batch.
2. Anything at or after the boundary is ignored. That includes:

   * a half-written line without a trailing newline,
   * complete lines that have been appended but not yet collected, and
   * the *new* file created by a rotation (it is a different inode anyway).

   None of those can therefore be mistaken for a missing line.
3. The original is located by scanning the shared directory for the inode —
   never by the mutable path stored in ``ingest_state``. After a rename
   rotation the stored path can already point at the new file; the archive is
   still found by its identity. The file is opened and ``fstat``-checked to
   make sure the descriptor really is that inode.

Unverified, never a false pass
------------------------------
The result is ``unverified`` (``passed == false``) whenever the retained
original cannot be read as a stable snapshot:

* no file with the requested identity exists in the log directory,
* the file shrank below the committed offset (truncated/replaced),
* the identity changed while opening/reading,
* a read was short or failed,
* the committed boundary does not fall on a line terminator, or
* the original disappeared while verification was running.

Only a fully read, stable snapshot can produce ``verified`` or ``mismatch``.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

# Read the original in bounded chunks. Tests may shrink this to exercise
# append/rotate interleaving.
READ_CHUNK = 1024 * 1024

# A rename is atomic, but listdir()/lstat() can still observe the directory
# just before or just after it. Retry briefly so a rotation that races with
# verification is not misreported as a missing original.
LOCATE_ATTEMPTS = 5
LOCATE_DELAY = 0.02

STATUS_VERIFIED = "verified"
STATUS_MISMATCH = "mismatch"
STATUS_UNVERIFIED = "unverified"


class NotTrackedError(Exception):
    """No ``ingest_state`` row exists for the requested (dev, inode)."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _connect_reader(db_path: str) -> sqlite3.Connection:
    """A dedicated read-only connection.

    The collector writes through its own connection. Using a separate one
    means verification sees only committed transactions (WAL snapshot) and
    can never observe the collector's uncommitted rows, nor disturb its
    ``BEGIN IMMEDIATE`` write transaction.
    """
    conn = sqlite3.connect(
        db_path, timeout=10, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA query_only=ON")
    return conn


def _read_snapshot(
    db_path: str, dev: int, inode: int
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Capture the committed state and records below its boundary.

    A deferred read transaction gives a consistent snapshot in WAL mode.
    """
    conn = _connect_reader(db_path)
    try:
        conn.execute("BEGIN")
        try:
            state = conn.execute(
                "SELECT dev, inode, source_code, path, offset, last_seen "
                "FROM ingest_state WHERE dev = ? AND inode = ?",
                (dev, inode),
            ).fetchone()
            if state is None:
                raise NotTrackedError(
                    f"no tracked file with dev={dev} inode={inode}"
                )
            boundary = state["offset"]
            normal = conn.execute(
                """SELECT id, source_code, byte_offset, raw_line
                     FROM log_events
                    WHERE dev = ? AND inode = ? AND byte_offset < ?
                    ORDER BY byte_offset, id""",
                (dev, inode, boundary),
            ).fetchall()
            malformed = conn.execute(
                """SELECT id, source_code, byte_offset, raw_line
                     FROM malformed_lines
                    WHERE dev = ? AND inode = ? AND byte_offset < ?
                    ORDER BY byte_offset, id""",
                (dev, inode, boundary),
            ).fetchall()
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
        return dict(state), [dict(r) for r in normal], [dict(r) for r in malformed]
    finally:
        conn.close()


def _locate(directory: str, dev: int, inode: int) -> Optional[str]:
    """Return a path in ``directory`` whose regular file is (dev, inode)."""
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return None
    for name in names:
        path = os.path.join(directory, name)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if stat.S_ISREG(st.st_mode) and st.st_dev == dev and st.st_ino == inode:
            return path
    return None


def _locate_retry(directory: str, dev: int, inode: int,
                  attempts: int = 3) -> bool:
    """Is the inode still present? Retry briefly so that the listdir/lstat
    window of a concurrent rename is not mistaken for a disappearance."""
    for attempt in range(attempts):
        if _locate(directory, dev, inode) is not None:
            return True
        if attempt < attempts - 1:
            time.sleep(LOCATE_DELAY)
    return False


def _open_original(
    directory: str, dev: int, inode: int
) -> tuple[Optional[int], Optional[str], Optional[os.stat_result], Optional[str]]:
    """Locate and open the exact inode, retrying across rename races.

    Returns ``(fd, path, fstat_before, error)``. On failure ``fd`` is None and
    ``error`` explains why.
    """
    last_error = "original file not found in log directory"
    for attempt in range(LOCATE_ATTEMPTS):
        path = _locate(directory, dev, inode)
        if path is not None:
            try:
                fd = os.open(path, os.O_RDONLY)
            except FileNotFoundError:
                # Renamed between listdir() and open(): retry and locate it
                # under its new name.
                last_error = "original file vanished while being opened"
            except OSError as exc:
                return None, path, None, f"cannot open original: {exc}"
            else:
                st = os.fstat(fd)
                if (stat.S_ISREG(st.st_mode)
                        and (st.st_dev, st.st_ino) == (dev, inode)):
                    return fd, path, st, None
                os.close(fd)
                last_error = "opened file is not the requested physical file"
        if attempt < LOCATE_ATTEMPTS - 1:
            time.sleep(LOCATE_DELAY)
    return None, None, None, last_error


def _read_bounded(fd: int, limit: int) -> bytes:
    """Read at most ``limit`` bytes from the start of the file.

    Stopping exactly at the boundary is what keeps uncommitted appends and
    unterminated tails out of the comparison.
    """
    chunks: list[bytes] = []
    remaining = limit
    while remaining > 0:
        chunk = os.read(fd, min(READ_CHUNK, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _parse_boundary_lines(data: bytes) -> list[tuple[int, str]]:
    """Split committed bytes into ``(absolute byte offset, raw line)``.

    The committed offset must be immediately after a newline; otherwise the
    retained original no longer matches the position the collector committed.
    ``\\r`` is stripped, mirroring the collector's raw-line representation.
    """
    if not data:
        return []
    if not data.endswith(b"\n"):
        raise ValueError(
            "committed offset is not at a line boundary in the original"
        )
    lines: list[tuple[int, str]] = []
    pos = 0
    while pos < len(data):
        newline = data.find(b"\n", pos)
        if newline < 0:  # unreachable, endswith check above
            raise ValueError("unterminated line inside committed region")
        piece = data[pos:newline]
        if piece.endswith(b"\r"):
            piece = piece[:-1]
        lines.append((pos, piece.decode("utf-8", errors="replace")))
        pos = newline + 1
    return lines


def _cap(items: list[Any], limit: int) -> tuple[list[Any], bool]:
    """Apply the detail limit, reporting whether anything was dropped."""
    return items[:limit], len(items) > limit


def _empty_counts(
    normal_records: int, malformed_records: int
) -> dict[str, int]:
    return {
        "original_lines": 0,
        "normal_records": normal_records,
        "malformed_records": malformed_records,
        "matched_lines": 0,
        "missing_lines": 0,
        "duplicate_positions": 0,
        "content_mismatches": 0,
        "unexpected_records": 0,
    }


def verify_tracked_file(
    db_path: str,
    directory: str,
    dev: int,
    inode: int,
    *,
    detail_limit: int = 500,
) -> dict[str, Any]:
    """Verify one tracked file against its committed batch.

    Raises :class:`NotTrackedError` if ``(dev, inode)`` is not in
    ``ingest_state``. All readable-but-inconsistent or unstable situations are
    returned as a report with ``status`` set accordingly.
    """
    started_at = _utc_now()
    started = time.monotonic()
    state, normal, malformed = _read_snapshot(db_path, dev, inode)
    boundary = state["offset"]

    records: list[dict[str, Any]] = []
    for row in normal:
        records.append({**row, "table": "log_events"})
    for row in malformed:
        records.append({**row, "table": "malformed_lines"})
    records.sort(key=lambda r: (r["byte_offset"], r["table"], r["id"]))

    base: dict[str, Any] = {
        "status": STATUS_UNVERIFIED,
        "passed": False,
        "dev": dev,
        "inode": inode,
        "source_code": state["source_code"],
        "source_path": state["path"],
        "last_seen": state["last_seen"],
        "original_path": None,
        "original_present_at_end": None,
        "reason": None,
        "started_at": started_at,
        "finished_at": None,
        "duration_ms": None,
        "boundary": {
            "committed_offset": boundary,
            "original_size_at_open": None,
            "original_size_after_read": None,
            "line_count": 0,
            "bytes_after_boundary_ignored": None,
        },
        "counts": _empty_counts(len(normal), len(malformed)),
        "missing_lines": [],
        "duplicate_positions": [],
        "content_mismatches": [],
        "unexpected_records": [],
        "details_truncated": False,
    }

    def finish(**updates: Any) -> dict[str, Any]:
        base.update(updates)
        base["finished_at"] = _utc_now()
        base["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
        return base

    def unverified(reason: str, **updates: Any) -> dict[str, Any]:
        base["status"] = STATUS_UNVERIFIED
        base["passed"] = False
        base["reason"] = reason
        return finish(**updates)

    fd, path, st_before, open_error = _open_original(directory, dev, inode)
    if fd is None:
        return unverified(
            f"original unavailable: {open_error} "
            f"(dev={dev} inode={inode} in {directory})"
        )

    try:
        base["original_path"] = path
        base["boundary"]["original_size_at_open"] = st_before.st_size

        if st_before.st_size < boundary:
            return unverified(
                "original is shorter than the committed offset "
                f"(size={st_before.st_size} < offset={boundary}); "
                "it was truncated or replaced",
                original_present_at_end=_locate_retry(directory, dev, inode),
            )

        base["boundary"]["bytes_after_boundary_ignored"] = (
            st_before.st_size - boundary
        )

        try:
            data = _read_bounded(fd, boundary)
            st_after = os.fstat(fd)
        except OSError as exc:
            return unverified(f"read failed: {exc}")

        base["boundary"]["original_size_after_read"] = st_after.st_size
        if len(data) != boundary:
            return unverified(
                f"short read: got {len(data)} of {boundary} committed bytes"
            )
        if (st_after.st_dev, st_after.st_ino) != (dev, inode):
            return unverified("file identity changed while reading")
        if st_after.st_size < boundary:
            return unverified(
                "original shrank below the committed offset while reading"
            )
    finally:
        os.close(fd)

    try:
        lines = _parse_boundary_lines(data)
    except ValueError as exc:
        return unverified(str(exc))

    # A rotation may rename the original while we read it. Renaming is atomic
    # and keeps the inode in the directory, so a short retry distinguishes it
    # from an actual disappearance.
    present_at_end = _locate_retry(directory, dev, inode)

    by_offset: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        by_offset[rec["byte_offset"]].append(rec)

    missing: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    unexpected: list[dict[str, Any]] = []
    matched = 0
    original_offsets: set[int] = set()

    for offset, raw_line in lines:
        original_offsets.add(offset)
        recs = by_offset.get(offset, [])
        if not recs:
            missing.append({"byte_offset": offset, "raw_line": raw_line})
            continue
        if len(recs) > 1:
            duplicates.append({
                "byte_offset": offset,
                "count": len(recs),
                "records": [
                    {
                        "table": r["table"],
                        "id": r["id"],
                        "source_code": r["source_code"],
                        "raw_line": r["raw_line"],
                    }
                    for r in recs
                ],
            })
        for rec in recs:
            if rec["raw_line"] != raw_line:
                mismatches.append({
                    "byte_offset": offset,
                    "table": rec["table"],
                    "id": rec["id"],
                    "source_code": rec["source_code"],
                    "original_raw_line": raw_line,
                    "record_raw_line": rec["raw_line"],
                })
        if len(recs) == 1 and recs[0]["raw_line"] == raw_line:
            matched += 1

    for rec in records:
        if rec["byte_offset"] not in original_offsets:
            unexpected.append({
                "byte_offset": rec["byte_offset"],
                "table": rec["table"],
                "id": rec["id"],
                "source_code": rec["source_code"],
                "raw_line": rec["raw_line"],
            })

    missing_out, cut_missing = _cap(missing, detail_limit)
    duplicates_out, cut_dup = _cap(duplicates, detail_limit)
    mismatches_out, cut_mismatch = _cap(mismatches, detail_limit)
    unexpected_out, cut_unexpected = _cap(unexpected, detail_limit)

    inconsistent = bool(missing or duplicates or mismatches or unexpected)
    status = STATUS_MISMATCH if inconsistent else STATUS_VERIFIED

    result = finish(
        status=status,
        passed=status == STATUS_VERIFIED,
        original_present_at_end=present_at_end,
        boundary={
            "committed_offset": boundary,
            "original_size_at_open": st_before.st_size,
            "original_size_after_read": st_after.st_size,
            "line_count": len(lines),
            "bytes_after_boundary_ignored": st_after.st_size - boundary,
        },
        counts={
            "original_lines": len(lines),
            "normal_records": len(normal),
            "malformed_records": len(malformed),
            "matched_lines": matched,
            "missing_lines": len(missing),
            "duplicate_positions": len(duplicates),
            "content_mismatches": len(mismatches),
            "unexpected_records": len(unexpected),
        },
        missing_lines=missing_out,
        duplicate_positions=duplicates_out,
        content_mismatches=mismatches_out,
        unexpected_records=unexpected_out,
        details_truncated=(
            cut_missing or cut_dup or cut_mismatch or cut_unexpected
        ),
    )
    if not present_at_end:
        # The bytes we compared were a stable snapshot, but the retained
        # original is gone now. The requirement says: do not report a pass.
        return unverified(
            "original disappeared during verification",
            original_path=path,
            original_present_at_end=False,
        )
    return result
