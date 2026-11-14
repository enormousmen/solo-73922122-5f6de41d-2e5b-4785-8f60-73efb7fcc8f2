#!/usr/bin/env python3
"""Summarise GET /verify results for every tracked file.

Reads the JSON of GET /ingest/progress from stdin and calls
GET /verify/{dev}/{inode} for each tracked file, printing one line per file.

Exit code 0 only if every file reports ``status == "verified"``.
Usage in the demo:
    curl -fsS "$API/ingest/progress" | API=... scripts/verify_summary.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    api = os.environ.get("API", "http://localhost:8000").rstrip("/")
    limit = os.environ.get("VERIFY_DETAIL_LIMIT", "5")
    progress = json.load(sys.stdin)
    files = progress.get("files", [])
    print(f"tracked files: {len(files)}")

    failed = False
    for f in files:
        url = f"{api}/verify/{f['dev']}/{f['inode']}?limit={limit}"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                rep = json.load(resp)
        except urllib.error.HTTPError as exc:
            print(f"  [HTTP {exc.code}] {f['source_code']}")
            failed = True
            continue
        except Exception as exc:  # noqa: BLE001
            print(f"  [ERROR] {f['source_code']}: {exc}")
            failed = True
            continue

        status = rep["status"]
        mark = {"verified": "PASS", "mismatch": "FAIL",
                "unverified": "UNVERIFIED"}.get(status, status.upper())
        if status != "verified":
            failed = True
        c = rep["counts"]
        print(
            f"  [{mark:10}] {f['source_code']:<28} "
            f"lines={c['original_lines']} normal={c['normal_records']} "
            f"malformed={c['malformed_records']} "
            f"missing={c['missing_lines']} dup={c['duplicate_positions']} "
            f"mismatch={c['content_mismatches']}"
        )
        if status == "unverified":
            print(f"               reason: {rep['reason']}")
        for item in rep["missing_lines"][:3]:
            print(f"               missing @{item['byte_offset']}: "
                  f"{item['raw_line'][:70]}")
        for item in rep["content_mismatches"][:3]:
            print(f"               differs @{item['byte_offset']}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
