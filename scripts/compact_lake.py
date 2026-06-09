# scripts/compact_lake.py
"""Compact the raw-L2 lake (merge tiny Parquet files per closed partition).
Lossless + verified; never touches the current hour. Usage:

    python -m scripts.compact_lake <data_dir> [--dry-run]
"""
from __future__ import annotations

import os
import sys
import time

from pavilos.persistence.compaction import compact_lake


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__); return
    base = sys.argv[1]
    if "--dry-run" in sys.argv:
        # count files per closed partition without changing anything
        total = parts = 0
        now = time.time(); cur = now - (now % 3600)
        for root, _dirs, files in os.walk(base):
            pq = [f for f in files if f.endswith(".parquet")]
            if len(pq) > 1:
                parts += 1; total += len(pq)
        print(f"dry-run: {parts} multi-file partitions, {total} parquet files (closed ones would merge to ~{parts})")
        return
    print(f"compacting {base} (closed partitions only)...")
    s = compact_lake(base)
    print(f"done: {s['partitions_compacted']} partitions compacted, "
          f"{s['files_removed']} files removed, {s['partitions_skipped']} skipped")


if __name__ == "__main__":
    main()
