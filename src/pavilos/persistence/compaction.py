# src/pavilos/persistence/compaction.py
"""Losslessly compact the lake's many tiny Parquet files into one per closed
partition. Verify the merged file equals the originals BEFORE deleting anything;
never touch the current (live) hour the recorder is writing."""
from __future__ import annotations

import calendar
import logging
import os
import tempfile
import time

import duckdb

_log = logging.getLogger(__name__)

#: Hard cap on DuckDB RAM. Compacting/verifying a multi-million-row deep-book
#: partition would otherwise build large hash tables / buffers and spike to many
#: GB (a prior run OOM'd and segfaulted). With this cap DuckDB SPILLS to disk
#: instead, keeping memory bounded regardless of partition size.
_DUCKDB_MEM_LIMIT = "3GB"
_SPILL_DIR = os.path.join(tempfile.gettempdir(), "pavilos_duckdb_spill")


def _connect():
    """A memory-capped DuckDB connection (spills to disk past the cap)."""
    os.makedirs(_SPILL_DIR, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{_DUCKDB_MEM_LIMIT}'")
    con.sql(f"SET temp_directory='{_SPILL_DIR.replace(chr(92), '/')}'")
    return con


def _sql_files(paths: list[str]) -> str:
    """A DuckDB ``read_parquet(...)`` over an explicit file list with Hive
    inference OFF — the ``exchange=/date=`` path would otherwise inject partition
    columns that collide with the in-file ``exchange`` column."""
    lst = ", ".join("'" + p.replace("\\", "/").replace("'", "''") + "'" for p in paths)
    return f"read_parquet([{lst}], hive_partitioning=false)"


def _rows_equal(merged_path: str, orig_paths: list[str]) -> bool:
    """True iff the merged file's rows are the SAME MULTISET as the originals
    (order-independent; queries re-ORDER BY). Uses DuckDB ``EXCEPT ALL`` both ways
    (multiset difference), which STREAMS from disk — so it verifies
    multi-million-row partitions without materialising every row in Python. The
    old ``to_pylist`` approach exhausted memory and crashed (MemoryError, then a
    segfault) on large partitions. Reads the merged file back from disk so a
    write/zstd bug is still caught."""
    con = _connect()
    try:
        m, o = _sql_files([merged_path]), _sql_files(orig_paths)
        diff = con.sql(
            f"SELECT (SELECT count(*) FROM (SELECT * FROM {o} EXCEPT ALL SELECT * FROM {m})) "
            f"+ (SELECT count(*) FROM (SELECT * FROM {m} EXCEPT ALL SELECT * FROM {o}))"
        ).fetchone()[0]
        return diff == 0
    finally:
        con.close()


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
    tmp = os.path.join(part_dir, f"{_TEMP_PREFIX}_{os.getpid()}.parquet")
    # STREAM the merge through DuckDB (read the file list -> write ONE zstd file) so
    # memory stays bounded even on multi-million-row deep-book partitions. An arrow
    # concat_tables here materialised every row and spiked to ~14 GB on busy venue
    # hours; the DuckDB COPY streams read->write at a few hundred MB.
    tmp_sql = "'" + tmp.replace("\\", "/").replace("'", "''") + "'"
    con = _connect()
    try:
        con.sql(f"COPY (SELECT * FROM {_sql_files(paths)}) TO {tmp_sql} (FORMAT parquet, COMPRESSION zstd)")
        nrows = con.sql(f"SELECT count(*) FROM {_sql_files([tmp])}").fetchone()[0]
    finally:
        con.close()
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
    return {"compacted": True, "files_before": len(raw), "rows": nrows}


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
