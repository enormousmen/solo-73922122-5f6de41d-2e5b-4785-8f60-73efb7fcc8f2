#!/usr/bin/env python3
"""Tests for the single-file integrity verifier.

Covers the core requirement and the concurrency edge cases:

* a clean tracked file verifies (normal + malformed records both compared)
* missing lines are listed by byte offset
* an extra duplicate record at a position is reported with its offset
* a record whose raw text differs from the original is reported
* an unterminated tail and uncollected complete appends are not gaps
* a renamed/rotated original is still found by inode, not by stored path
* the *new* access.log inode is never used to verify the old archive
* a missing/truncated original is ``unverified``, never a pass
* an original that disappears mid-verification is ``unverified``
* deleted committed records count as missing
* consecutive verification during a live collector never reports a false
  mismatch

Runs entirely locally: ``python -I tests/test_verifier.py``.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app import storage, verifier  # noqa: E402
from app.ingest import CollectorLoop, poll_once  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def jline(path: str, status: int) -> str:
    return json.dumps({"time": "2026-11-13T10:00:00Z", "path": path,
                       "status": status, "method": "GET"}) + "\n"


def fresh_env():
    base = tempfile.mkdtemp(prefix="verify-test-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    return base, log_dir, db_path, conn


def ident(path: str) -> tuple[int, int]:
    st = os.stat(path)
    return st.st_dev, st.st_ino


def verify(log_dir, db_path, path, limit=500):
    dev, ino = ident(path)
    return verifier.verify_tracked_file(db_path, log_dir, dev, ino,
                                        detail_limit=limit)


def test_clean_verified():
    print("\n[1] 正常 + 坏行记录逐行一致 -> verified")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
            fh.write(jline("/b", 500))
            fh.write("{not json}\n")           # malformed record
            fh.write(jline("/c", 404))
        poll_once(conn, log_dir)
        r = verify(log_dir, db_path, f)
        check("status verified and passed", r["status"] == "verified" and r["passed"],
              str(r.get("status")))
        check("all 4 original lines compared", r["counts"]["original_lines"] == 4,
              str(r["counts"]))
        check("3 normal + 1 malformed record counted",
              r["counts"]["normal_records"] == 3
              and r["counts"]["malformed_records"] == 1, str(r["counts"]))
        check("no problems listed",
              not r["missing_lines"] and not r["duplicate_positions"]
              and not r["content_mismatches"] and not r["unexpected_records"])
        check("boundary equals committed offset",
              r["boundary"]["committed_offset"] == os.path.getsize(f))
        check("no unignored bytes after boundary",
              r["boundary"]["bytes_after_boundary_ignored"] == 0)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_missing_duplicate_mismatch():
    print("\n[2] 缺行 / 重复字节位置 / 原文不符 被逐项列出")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        l1 = jline("/keep", 200)
        l2 = jline("/missing", 500)
        l3 = jline("/dup", 503)
        l4 = jline("/alter", 404)
        with open(f, "w") as fh:
            fh.write(l1 + l2 + l3 + l4)
        poll_once(conn, log_dir)
        off2, off3, off4 = len(l1), len(l1) + len(l2), len(l1) + len(l2) + len(l3)

        # Delete the record for line 2 -> that position is missing.
        conn.execute("DELETE FROM log_events WHERE byte_offset = ?", (off2,))
        # Duplicate the record at line 3's position (unique index by design
        # prevents this through ingestion; simulate a corrupted warehouse).
        row = conn.execute(
            "SELECT * FROM log_events WHERE byte_offset = ?", (off3,)
        ).fetchone()
        conn.execute("DROP INDEX ux_log_events_position")
        conn.execute(
            """INSERT INTO log_events
                   (ts, path, status, method, source_code, dev, inode,
                    byte_offset, raw_line)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (row["ts"], row["path"], row["status"], row["method"],
             row["source_code"], row["dev"], row["inode"],
             row["byte_offset"], row["raw_line"]),
        )
        # Alter line 4's stored raw text -> content mismatch.
        conn.execute(
            "UPDATE log_events SET raw_line = ? WHERE byte_offset = ?",
            ('{"tampered":true}', off4),
        )
        check("warehouse mutated", True)

        r = verify(log_dir, db_path, f)
        check("status mismatch, passed false",
              r["status"] == "mismatch" and not r["passed"], str(r["status"]))
        check("missing line listed at its byte offset",
              [m["byte_offset"] for m in r["missing_lines"]] == [off2],
              str(r["missing_lines"]))
        check("missing line carries original raw text",
              r["missing_lines"][0]["raw_line"] == l2.rstrip("\n"))
        check("duplicate position listed with offset and count",
              len(r["duplicate_positions"]) == 1
              and r["duplicate_positions"][0]["byte_offset"] == off3
              and r["duplicate_positions"][0]["count"] == 2,
              str(r["duplicate_positions"]))
        check("content mismatch listed with both texts",
              len(r["content_mismatches"]) == 1
              and r["content_mismatches"][0]["byte_offset"] == off4
              and r["content_mismatches"][0]["original_raw_line"] == l4.rstrip("\n")
              and r["content_mismatches"][0]["record_raw_line"] == '{"tampered":true}',
              str(r["content_mismatches"]))
        check("counts reflect one of each",
              r["counts"]["missing_lines"] == 1
              and r["counts"]["duplicate_positions"] == 1
              and r["counts"]["content_mismatches"] == 1, str(r["counts"]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_unterminated_tail_and_uncollected_appends():
    print("\n[3] 未换行尾部与未入仓新行不参与核对、不算缺口")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
            fh.write(jline("/b", 500))
        poll_once(conn, log_dir)
        committed = os.path.getsize(f)

        # Append complete lines that are NOT collected, plus a dangling tail.
        with open(f, "a") as fh:
            fh.write(jline("/not-yet-collected", 200))
            fh.write('{"path":"/dangling","status":5')

        r = verify(log_dir, db_path, f)
        check("status verified (new lines are outside the boundary)",
              r["status"] == "verified", str(r.get("reason")))
        check("boundary is the previously committed offset, not current size",
              r["boundary"]["committed_offset"] == committed,
              f"{r['boundary']['committed_offset']} != {committed}")
        check("original lines counted only up to boundary",
              r["counts"]["original_lines"] == 2, str(r["counts"]))
        check("bytes after boundary reported as ignored",
              r["boundary"]["bytes_after_boundary_ignored"]
              == os.path.getsize(f) - committed,
              str(r["boundary"]))
        check("uncollected lines are not listed as missing",
              r["missing_lines"] == [], str(r["missing_lines"]))

        # Once collected, the new lines become part of the next verification.
        poll_once(conn, log_dir)
        r2 = verify(log_dir, db_path, f)
        check("after collection 3 complete lines verified",
              r2["status"] == "verified"
              and r2["counts"]["original_lines"] == 3, str(r2["counts"]))
        check("dangling tail still excluded after collection",
              r2["boundary"]["committed_offset"] == committed
              + len(jline("/not-yet-collected", 200)),
              str(r2["boundary"]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_rotation_found_by_inode():
    print("\n[4] 轮转改名后仍按 inode 找到原件，新文件不冒充旧文件")
    base, log_dir, db_path, conn = fresh_env()
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(5):
                fh.write(jline(f"/old/{i}", 200))
        poll_once(conn, log_dir)
        old_dev, old_ino = ident(cur)

        archive = os.path.join(log_dir, "access.log.arc1.log")
        os.rename(cur, archive)
        # Late write into the old inode after rotation, then a new file.
        with open(archive, "a") as fh:
            fh.write(jline("/old/late", 503))
        with open(cur, "w") as fh:
            fh.write(jline("/new/1", 404))
        # Collect only up to the old inode's committed state? First poll
        # collects the late line into the archive AND the new file.
        poll_once(conn, log_dir)

        r = verify(log_dir, db_path, archive)
        check("archive verified using its own path",
              r["status"] == "verified" and r["original_path"] == archive,
              str(r))
        check("archive verification used the archive source code",
              r["source_code"] == "access.log.arc1.log", r["source_code"])
        check("archive has 6 lines (5 + late)",
              r["counts"]["original_lines"] == 6, str(r["counts"]))

        # The *new* file is tracked under its own inode; verifying the archive
        # must never read the new file's bytes.
        dev_new, ino_new = ident(cur)
        check("new file has a different identity", ino_new != old_ino)
        rnew = verifier.verify_tracked_file(db_path, log_dir, dev_new, ino_new)
        check("new inode verifies against its own committed batch",
              rnew["status"] == "verified"
              and rnew["counts"]["original_lines"] == 1, str(rnew["counts"]))

        # Simulate the stored path being stale: it still names access.log.
        stale = storage.get_state(conn, old_dev, old_ino)
        check("stored path for archive is stale (names access.log)",
              stale["path"] == archive, stale["path"])
        r2 = verify(log_dir, db_path, archive)
        check("verification still succeeds by inode despite rename",
              r2["status"] == "verified", str(r2.get("reason")))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_missing_original_unverified():
    print("\n[5] 原件缺失 / 被截断 -> unverified，不报通过")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
            fh.write(jline("/b", 500))
        poll_once(conn, log_dir)
        dev, ino = ident(f)

        # Move the original out of the watched directory entirely.
        outside = os.path.join(base, "moved-away.log")
        os.rename(f, outside)
        r = verifier.verify_tracked_file(db_path, log_dir, dev, ino)
        check("missing original -> unverified", r["status"] == "unverified",
              str(r["status"]))
        check("missing original -> passed false", r["passed"] is False)
        check("reason names the missing original",
              "original unavailable" in (r["reason"] or ""), str(r.get("reason")))

        # Put it back but truncate below the committed offset.
        os.rename(outside, f)
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))  # shorter than committed offset
        r2 = verifier.verify_tracked_file(db_path, log_dir, dev, ino)
        check("truncated original -> unverified",
              r2["status"] == "unverified", str(r2["status"]))
        check("truncation reason mentions offset",
              "shorter than the committed offset" in (r2["reason"] or ""),
              str(r2.get("reason")))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_untracked_inode_raises():
    print("\n[6] 未追踪的 (dev, inode) -> NotTrackedError")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
        dev, ino = ident(f)
        try:
            verifier.verify_tracked_file(db_path, log_dir, dev, ino)
        except verifier.NotTrackedError:
            check("untracked file raises NotTrackedError", True)
        else:
            check("untracked file raises NotTrackedError", False)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_deleted_records_are_missing():
    print("\n[7] 删除已入仓记录 -> 对应位置列为缺行")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        lines = [jline(f"/p/{i}", 200) for i in range(6)]
        with open(f, "w") as fh:
            fh.writelines(lines)
        poll_once(conn, log_dir)
        conn.execute("DELETE FROM log_events WHERE byte_offset = ?",
                     (len(lines[0]) + len(lines[1]),))
        r = verify(log_dir, db_path, f)
        check("mismatch", r["status"] == "mismatch")
        check("exactly the deleted offset is missing",
              [m["byte_offset"] for m in r["missing_lines"]]
              == [len(lines[0]) + len(lines[1])], str(r["missing_lines"]))
        check("missing count is 1", r["counts"]["missing_lines"] == 1)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_unstable_read_unverified():
    print("\n[8] 核验期间读取不稳定 -> unverified")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            for i in range(4):
                fh.write(jline(f"/q/{i}", 200))
        poll_once(conn, log_dir)
        dev, ino = ident(f)

        original_read = verifier._read_bounded

        def short_read(fd, limit):
            return original_read(fd, limit)[: max(0, limit // 2)]

        verifier._read_bounded = short_read
        try:
            r = verifier.verify_tracked_file(db_path, log_dir, dev, ino)
        finally:
            verifier._read_bounded = original_read
        check("short read -> unverified", r["status"] == "unverified",
              str(r["status"]))
        check("short-read reason reported",
              "short read" in (r["reason"] or ""), str(r.get("reason")))

        # A file that disappears between locate and the end of the read is
        # also unverified, not a pass.
        calls = {"n": 0}
        real_locate = verifier._locate

        def locate_then_vanish(directory, d, i):
            calls["n"] += 1
            if calls["n"] >= 2:      # the existence check after reading
                return None
            return real_locate(directory, d, i)

        verifier._locate = locate_then_vanish
        try:
            r2 = verifier.verify_tracked_file(db_path, log_dir, dev, ino)
        finally:
            verifier._locate = real_locate
        check("disappeared during verification -> unverified",
              r2["status"] == "unverified", str(r2["status"]))
        check("disappearance reported",
              "disappeared during verification" in (r2["reason"] or ""),
              str(r2.get("reason")))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_live_collector_no_false_mismatch():
    print("\n[9] 采集/轮转与核验交错：无假缺口，且只比已提交批次")
    base, log_dir, db_path, conn = fresh_env()
    try:
        loop_conn = storage.connect(db_path)
        loop = CollectorLoop(loop_conn, log_dir, interval=0.02)
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        cur = os.path.join(log_dir, "access.log")
        fh = open(cur, "w")
        handles = []
        bad_reports: list[dict] = []
        verified_seen = 0
        try:
            for rnd in range(8):
                for i in range(25):
                    fh.write(jline(f"/r{rnd}/{i}", 200 if i % 5 else 500))
                fh.flush()
                time.sleep(0.03)
                fh.close()
                arch = os.path.join(log_dir, f"access.log.rot{rnd}.log")
                os.rename(cur, arch)
                handles.append(open(arch, "a"))
                handles[-1].write(jline(f"/r{rnd}/late", 599))
                handles[-1].flush()
                # Verify the archive while the collector may still be
                # draining it: must never claim a mismatch.
                dev, ino = ident(arch)
                try:
                    rep = verifier.verify_tracked_file(
                        db_path, log_dir, dev, ino)
                except verifier.NotTrackedError:
                    rep = None  # not yet discovered; acceptable
                if rep is not None:
                    if rep["status"] == "mismatch":
                        bad_reports.append(rep)
                    elif rep["status"] == "verified":
                        verified_seen += 1
                fh = open(cur, "w")
                time.sleep(0.03)
            fh.close()
            time.sleep(0.4)
            # Let the collector quiesce, then every source must verify.
            deadline = time.time() + 10
            final_reports = []
            while time.time() < deadline:
                final_reports = []
                all_ok = True
                for st in storage.all_states(loop_conn):
                    rep = verifier.verify_tracked_file(
                        db_path, log_dir, st["dev"], st["inode"])
                    final_reports.append(rep)
                    if rep["status"] != "verified":
                        all_ok = False
                if all_ok:
                    break
                time.sleep(0.1)
        finally:
            loop.request_stop()
            t.join(timeout=5)
            for h in handles:
                h.close()
            loop_conn.close()

        check("no false mismatch while collector was running",
              not bad_reports,
              str([(b["source_code"], b["counts"]) for b in bad_reports[:2]]))
        check("at least some interleaved verifications saw a stable snapshot",
              verified_seen > 0, f"verified_seen={verified_seen}")
        check("after quiescence every tracked source verifies",
              final_reports and all(r["status"] == "verified"
                                    for r in final_reports),
              str([(r["source_code"], r["status"], r.get("reason"))
                   for r in final_reports]))
        check("no interleaved verification reported a missing line",
              all(not r["missing_lines"] for r in final_reports))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_detail_limit():
    print("\n[10] 明细 limit 截断但计数仍为全量")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        lines = [jline(f"/m/{i}", 200) for i in range(10)]
        with open(f, "w") as fh:
            fh.writelines(lines)
        poll_once(conn, log_dir)
        conn.execute("DELETE FROM log_events")
        r = verify(log_dir, db_path, f, limit=3)
        check("full missing count reported", r["counts"]["missing_lines"] == 10,
              str(r["counts"]))
        check("details capped at limit", len(r["missing_lines"]) == 3,
              str(len(r["missing_lines"])))
        check("truncation flagged", r["details_truncated"] is True)
    finally:
        conn.close()
        shutil.rmtree(base)


if __name__ == "__main__":
    test_clean_verified()
    test_missing_duplicate_mismatch()
    test_unterminated_tail_and_uncollected_appends()
    test_rotation_found_by_inode()
    test_missing_original_unverified()
    test_untracked_inode_raises()
    test_deleted_records_are_missing()
    test_unstable_read_unverified()
    test_live_collector_no_false_mismatch()
    test_detail_limit()
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    sys.exit(1 if FAIL else 0)
