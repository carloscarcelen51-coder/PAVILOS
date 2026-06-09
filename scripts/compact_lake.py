# scripts/compact_lake.py
"""Compact the raw-L2 lake (merge tiny Parquet files per closed partition).
Lossless + verified; never touches the current hour. Usage:

    python -m scripts.compact_lake <data_dir> [--dry-run]
"""
from __future__ import annotations

import sys

from pavilos.persistence.compaction import compact_lake, count_compactable


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__); return
    base = sys.argv[1]
    if "--dry-run" in sys.argv:
        # read-only preview using the SAME closed-partition predicate as the real
        # run, so it never over-counts the live hour or stray temp files.
        c = count_compactable(base)
        print(f"dry-run: {c['partitions']} closed multi-file partitions, "
              f"{c['files']} parquet files (would merge to ~{c['partitions']})")
        return
    print(f"compacting {base} (closed partitions only)...")
    s = compact_lake(base)
    print(f"done: {s['partitions_compacted']} partitions compacted, "
          f"{s['files_removed']} files removed, {s['partitions_skipped']} skipped")


if __name__ == "__main__":
    main()
