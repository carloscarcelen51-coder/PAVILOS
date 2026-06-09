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


_TEMP_PREFIX = "_compacted"


_FINAL_NAME = "compacted.parquet"


def compact_partition(part_dir: str) -> dict:
    """Merge all *.parquet in ``part_dir`` into one, verified-lossless and
    **crash-safe**. Intended for a CLOSED partition (a past hour the recorder will
    never write to again — ``compact_lake`` guarantees this); it assumes no
    concurrent writer adds new raw files mid-compaction.

    Ordering is PROMOTE-THEN-DELETE: write the merged file to a temp, verify its
    row multiset equals the originals, atomically ``os.replace`` it to
    ``compacted.parquet``, and only THEN delete the raw originals. Combined with
    the recovery rules below this makes EVERY crash window safe — no data loss and
    no duplication, wherever the process dies:

    - **A ``compacted.parquet`` already exists** → it is authoritative (written
      only after a passing verify). Any leftover raw files are rows it already
      absorbed (a crash after the replace, before the delete); delete them — never
      re-merge them (that would double the data). Idempotent re-runs land here too.
    - **Only a temp survives** (no ``compacted.parquet``, no raw) → the temp was
      written and verified before anything was deleted, so it is a complete copy;
      promote it rather than lose it. (Cannot arise under promote-then-delete, but
      recovered defensively.)
    - **Raw originals present, no ``compacted.parquet``** → the normal path: drop
      any incomplete temp, merge, verify, replace, delete originals.

    A sub-directory whose name ends in ``.parquet`` is ignored (only regular files
    are merged)."""
    def _is_file(name: str) -> bool:
        return os.path.isfile(os.path.join(part_dir, name))

    final_path = os.path.join(part_dir, _FINAL_NAME)
    entries = os.listdir(part_dir)
    temps = [f for f in entries if f.startswith(_TEMP_PREFIX) and f.endswith(".parquet") and _is_file(f)]
    raw = sorted(f for f in entries
                 if f.endswith(".parquet") and f != _FINAL_NAME
                 and not f.startswith(_TEMP_PREFIX) and _is_file(f))

    # RULE 1: a committed compacted.parquet is the authoritative full copy.
    if os.path.isfile(final_path):
        for f in temps + raw:                 # leftover raw are already absorbed; never re-merge
            os.remove(os.path.join(part_dir, f))
        return {"skipped": True, "files": 1, "recovered": bool(temps or raw)}

    # RULE 2 (defensive): only a temp survived -> it is complete+verified, promote it.
    if temps and not raw:
        newest = max(temps, key=lambda f: os.path.getmtime(os.path.join(part_dir, f)))
        os.replace(os.path.join(part_dir, newest), final_path)
        for f in temps:
            p = os.path.join(part_dir, f)
            if os.path.exists(p):
                os.remove(p)
        return {"skipped": True, "files": 1, "recovered": True}

    # Drop incomplete temps from a crashed prior write; then handle the raw originals.
    for f in temps:
        os.remove(os.path.join(part_dir, f))
    if len(raw) <= 1:
        return {"skipped": True, "files": len(raw)}

    paths = [os.path.join(part_dir, f) for f in raw]
    merged = pa.concat_tables([_read_file(p) for p in paths])
    tmp = os.path.join(part_dir, f"{_TEMP_PREFIX}_{os.getpid()}.parquet")
    pq.write_table(merged, tmp, compression="zstd")
    try:
        if not _rows_equal(tmp, paths):
            raise ValueError(f"compaction verify FAILED for {part_dir}; keeping originals")
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    # PROMOTE-THEN-DELETE: land the verified merged file atomically FIRST (a crash
    # after this is cleaned up by RULE 1 next run; a crash before it leaves the raw
    # originals intact), THEN delete the now-absorbed originals.
    os.replace(tmp, final_path)
    for p in paths:
        os.remove(p)
    return {"compacted": True, "files_before": len(raw), "rows": merged.num_rows}


def _partition_hour_epoch(date: str, hour: str) -> float:
    # Partition dirs are named from the sink's gmtime (UTC); interpret them as UTC.
    # timegm is DST-safe, matching the recorder's bucketing exactly.
    return float(calendar.timegm(time.strptime(f"{date} {hour}", "%Y-%m-%d %H")))


def _iter_closed_partitions(base_dir: str, cur_hour: float):
    """Yield ``part_dir`` for every CLOSED partition (hour strictly before the
    current hour). Shared by the real run and the dry-run preview so they agree
    by construction — neither counts the live/current hour the recorder owns."""
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
                if hour_epoch + 3600 > cur_hour:   # live/current hour -> skip
                    continue
                yield part


def count_compactable(base_dir: str, *, now_ts: float | None = None) -> dict:
    """Read-only preview: count CLOSED multi-file partitions (and the real parquet
    files in them) that ``compact_lake`` would merge. Uses the same closed-partition
    predicate as the real run and ignores temp/output files (``_compacted*``), so
    the dry-run never over-counts the live hour or stray temps."""
    if not os.path.isdir(base_dir):
        return {"partitions": 0, "files": 0}
    now = time.time() if now_ts is None else now_ts
    cur_hour = now - (now % 3600)
    parts = files = 0
    for part in _iter_closed_partitions(base_dir, cur_hour):
        real = [f for f in os.listdir(part)
                if f.endswith(".parquet")
                and not f.startswith(_TEMP_PREFIX)
                and os.path.isfile(os.path.join(part, f))]
        if len(real) > 1:
            parts += 1
            files += len(real)
    return {"partitions": parts, "files": files}


def compact_lake(base_dir: str, *, now_ts: float | None = None) -> dict:
    """Compact every CLOSED partition (hour strictly before the current hour).
    Skips the live partition the recorder is appending to."""
    if not os.path.isdir(base_dir):
        return {"partitions_compacted": 0, "partitions_skipped": 0,
                "files_removed": 0, "partitions_failed": 0}
    now = time.time() if now_ts is None else now_ts
    cur_hour = now - (now % 3600)            # start of the current hour (UTC epoch)
    compacted = skipped = files_removed = failed = 0
    for part in _iter_closed_partitions(base_dir, cur_hour):
        n_before = len([f for f in os.listdir(part) if f.endswith(".parquet")])
        try:
            res = compact_partition(part)
        except Exception:
            # A single malformed/corrupt parquet (e.g. truncated by a killed
            # recorder) must NOT abort the whole batch run; report and continue.
            _log.exception("compaction failed for partition %s; skipping", part)
            failed += 1
            continue
        if res.get("compacted"):
            compacted += 1
            files_removed += n_before - 1
        else:
            skipped += 1
    return {"partitions_compacted": compacted, "partitions_skipped": skipped,
            "files_removed": files_removed, "partitions_failed": failed}
