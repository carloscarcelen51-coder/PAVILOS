# src/pavilos/persistence/compaction.py
"""Losslessly compact the lake's many tiny Parquet files into one per closed
partition. Verify the merged file equals the originals BEFORE deleting anything;
never touch the current (live) hour the recorder is writing."""
from __future__ import annotations

import calendar
import logging
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

_log = logging.getLogger(__name__)


def _read_file(path: str) -> pa.Table:
    """Read ONE parquet file as its own stored schema, ignoring any Hive
    partition columns inherited from the ``exchange=.../date=.../HH`` path.
    ``pq.read_table`` would auto-infer the Hive partitioning and inject an
    ``exchange`` dictionary column that collides with the ``exchange`` string
    column stored inside the file; ``ParquetFile.read`` reads only the file."""
    return pq.ParquetFile(path).read()


def _rows_equal(merged_path: str, orig_paths: list[str]) -> bool:
    """True iff the merged file's rows are the SAME MULTISET as the originals
    (order-independent; queries re-ORDER BY). Reads the merged file back from disk
    so a write/zstd bug is caught."""
    m = _read_file(merged_path)
    o = pa.concat_tables([_read_file(p) for p in orig_paths])
    if m.num_rows != o.num_rows:
        return False
    return sorted(map(tuple, (r.values() for r in m.to_pylist()))) == \
           sorted(map(tuple, (r.values() for r in o.to_pylist())))


def compact_partition(part_dir: str) -> dict:
    """Merge all *.parquet in ``part_dir`` into one, verified-lossless. Originals
    are deleted ONLY after the merged file is written + verified equal to them."""
    files = sorted(f for f in os.listdir(part_dir) if f.endswith(".parquet"))
    if len(files) <= 1:
        return {"skipped": True, "files": len(files)}
    paths = [os.path.join(part_dir, f) for f in files]
    merged = pa.concat_tables([_read_file(p) for p in paths])
    tmp = os.path.join(part_dir, f"_compacted_{os.getpid()}.parquet")
    pq.write_table(merged, tmp, compression="zstd")
    try:
        if not _rows_equal(tmp, paths):
            raise ValueError(f"compaction verify FAILED for {part_dir}; keeping originals")
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    for p in paths:               # safe: merged written + verified before any delete
        os.remove(p)
    final = os.path.join(part_dir, "compacted.parquet")
    os.replace(tmp, final)
    return {"compacted": True, "files_before": len(files), "rows": merged.num_rows}


def _partition_hour_epoch(date: str, hour: str) -> float:
    # Partition dirs are named from the sink's gmtime (UTC); interpret them as UTC.
    # timegm is DST-safe, matching the recorder's bucketing exactly.
    return float(calendar.timegm(time.strptime(f"{date} {hour}", "%Y-%m-%d %H")))


def compact_lake(base_dir: str, *, now_ts: float | None = None) -> dict:
    """Compact every CLOSED partition (hour strictly before the current hour).
    Skips the live partition the recorder is appending to."""
    if not os.path.isdir(base_dir):
        return {"partitions_compacted": 0, "partitions_skipped": 0, "files_removed": 0}
    now = time.time() if now_ts is None else now_ts
    cur_hour = now - (now % 3600)            # start of the current hour (UTC epoch)
    compacted = skipped = files_removed = 0
    for ex in sorted(os.listdir(base_dir)):
        if not ex.startswith("exchange="):
            continue
        ex_dir = os.path.join(base_dir, ex)
        for d in sorted(os.listdir(ex_dir)):
            if not d.startswith("date="):
                continue
            d_dir = os.path.join(ex_dir, d)
            for hh in sorted(os.listdir(d_dir)):
                part = os.path.join(d_dir, hh)
                if not os.path.isdir(part):
                    continue
                try:
                    hour_epoch = _partition_hour_epoch(d[len("date="):], hh)
                except ValueError:
                    continue
                if hour_epoch + 3600 > cur_hour:   # this hour is the live/current one -> skip
                    skipped += 1
                    continue
                n_before = len([f for f in os.listdir(part) if f.endswith(".parquet")])
                res = compact_partition(part)
                if res.get("compacted"):
                    compacted += 1
                    files_removed += n_before - 1
                else:
                    skipped += 1
    return {"partitions_compacted": compacted, "partitions_skipped": skipped,
            "files_removed": files_removed}
