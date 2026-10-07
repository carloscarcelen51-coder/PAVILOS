# src/pavilos/persistence/compaction.py
"""Losslessly compact the lake's many tiny Parquet files into one per closed
partition. Verify the merged file equals the originals BEFORE deleting anything;
never touch the current (live) hour the recorder is writing."""
from __future__ import annotations

import calendar
import logging
import os
import time

from pavilos.persistence.duck import connect as _connect   # memory-capped, known spill dir
from pavilos.persistence.lake_files import check_files

_log = logging.getLogger(__name__)


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


class UnreadableFiles(RuntimeError):
    """A partition holds a Parquet file that cannot be read: it is left untouched."""

#: An hour is compacted only once it ended at least this long ago. The recorder can
#: still write into a just-closed hour: rows are partitioned by their own ``ts``, the
#: writer flushes every ~5 s, and an M17 checkpoint carries ``ts = last_ts`` of its
#: venue (up to the 15 s staleness bound). 15 min covers that with a wide margin.
_GRACE_S = 900.0


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
      only after a passing verify). A leftover raw file whose rows it already
      CONTAINS was absorbed (a crash after the replace, before the delete): delete
      it — never re-merge it (that would double the data). A raw file it does NOT
      contain was written LATE, after the hour was compacted: merge it in with the
      same verify-then-promote-then-delete sequence (deleting it unmerged would
      lose it). Idempotent re-runs land here too.
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

    # Never merge or delete around an unreadable file (a writer killed mid-write before
    # writes became atomic, a damaged footer): leave the hour as it is and say so.
    unreadable = check_files([os.path.join(part_dir, f) for f in raw]
                             + ([final_path] if os.path.isfile(final_path) else []))[1]
    if unreadable:
        raise UnreadableFiles(f"{len(unreadable)} unreadable parquet file(s) in {part_dir} "
                              f"({[os.path.basename(p) for p in unreadable[:5]]}); left as is")

    # RULE 1: a committed compacted.parquet is the authoritative full copy.
    if os.path.isfile(final_path):
        for f in temps:
            os.remove(os.path.join(part_dir, f))
        paths = [os.path.join(part_dir, f) for f in raw]
        # The common case (a crash after the replace) is settled with ONE query; only
        # when something is not contained is each file checked on its own.
        absorbed = paths if paths and _contained(paths, final_path) else [
            p for p in paths if _contained([p], final_path)]
        late = [p for p in paths if p not in absorbed]
        for p in absorbed:
            os.remove(p)                      # absorbed before a crash: never re-merge
        if not late:
            return {"skipped": True, "files": 1, "recovered": bool(temps or raw)}
        nrows = _merge_verified(part_dir, [final_path] + late, final_path)
        for p in late:
            os.remove(p)
        _log.warning("compaction: merged %d late file(s) into %s", len(late), final_path)
        return {"compacted": True, "files_before": 1 + len(late), "rows": nrows,
                "late_merged": len(late)}

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
    nrows = _merge_verified(part_dir, paths, final_path)
    # PROMOTE-THEN-DELETE: the verified merged file has landed atomically (a crash
    # after this is cleaned up by RULE 1 next run; a crash before it leaves the raw
    # originals intact); only now delete the absorbed originals.
    for p in paths:
        os.remove(p)
    return {"compacted": True, "files_before": len(raw), "rows": nrows}


def _contained(paths: list[str], final_path: str) -> bool:
    """True iff every row of ``paths`` (as a multiset) is in ``final_path``."""
    con = _connect()
    try:
        return con.sql(
            f"SELECT count(*) FROM (SELECT * FROM {_sql_files(paths)} "
            f"EXCEPT ALL SELECT * FROM {_sql_files([final_path])})").fetchone()[0] == 0
    finally:
        con.close()


def _merge_verified(part_dir: str, inputs: list[str], final_path: str) -> int:
    """Merge ``inputs`` into a temp, verify its row multiset equals theirs, then
    atomically ``os.replace`` it onto ``final_path``. Deletes nothing else (the
    caller removes the absorbed inputs afterwards). Returns the merged row count."""
    tmp = os.path.join(part_dir, f"{_TEMP_PREFIX}_{os.getpid()}.parquet")
    # STREAM the merge through DuckDB (read the file list -> write ONE zstd file) so
    # memory stays bounded even on multi-million-row deep-book partitions. An arrow
    # concat_tables here materialised every row and spiked to ~14 GB on busy venue
    # hours; the DuckDB COPY streams read->write at a few hundred MB.
    tmp_sql = "'" + tmp.replace("\\", "/").replace("'", "''") + "'"
    con = _connect()
    try:
        con.sql(f"COPY (SELECT * FROM {_sql_files(inputs)}) TO {tmp_sql} (FORMAT parquet, COMPRESSION zstd)")
        nrows = con.sql(f"SELECT count(*) FROM {_sql_files([tmp])}").fetchone()[0]
    except Exception:
        con.close()
        _remove_quietly(tmp)          # a failed COPY can leave a partial/0-byte temp behind
        raise
    con.close()
    try:
        if not _rows_equal(tmp, inputs):
            raise ValueError(f"compaction verify FAILED for {part_dir}; keeping originals")
        os.replace(tmp, final_path)   # Windows: fails while a reader holds final_path open
    except Exception:
        _remove_quietly(tmp)          # tidy up; a leftover temp is also cleaned next pass
        raise
    return nrows


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _partition_hour_epoch(date: str, hour: str) -> float:
    # Partition dirs are named from the sink's gmtime (UTC); interpret them as UTC.
    # timegm is DST-safe, matching the recorder's bucketing exactly.
    return float(calendar.timegm(time.strptime(f"{date} {hour}", "%Y-%m-%d %H")))


def _iter_closed_partitions(base_dir: str, closed_until: float):
    """Yield ``part_dir`` for every CLOSED partition: its hour ENDED at or before
    ``closed_until`` (= now - grace). Shared by the real run and the dry-run preview
    so they agree by construction — neither counts the live/current hour the
    recorder owns, nor a just-closed hour that may still receive late rows."""
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
                if hour_epoch + 3600 > closed_until:   # live or within the grace -> skip
                    continue
                yield part


def count_compactable(base_dir: str, *, now_ts: float | None = None,
                      grace_s: float = _GRACE_S) -> dict:
    """Read-only preview: count CLOSED multi-file partitions (and the real parquet
    files in them) that ``compact_lake`` would merge. Uses the same closed-partition
    predicate as the real run and ignores temp/output files (``_compacted*``), so
    the dry-run never over-counts the live hour or stray temps. It does not open the
    files: an hour holding an unreadable file is counted here and then left untouched
    (reported) by ``compact_lake``."""
    if not os.path.isdir(base_dir):
        return {"partitions": 0, "files": 0}
    now = time.time() if now_ts is None else now_ts
    parts = files = 0
    for part in _iter_closed_partitions(base_dir, now - grace_s):
        real = [f for f in os.listdir(part)
                if f.endswith(".parquet")
                and not f.startswith(_TEMP_PREFIX)
                and os.path.isfile(os.path.join(part, f))]
        if len(real) > 1:
            parts += 1
            files += len(real)
    return {"partitions": parts, "files": files}


_LOCK_NAME = "_compaction.lock"


def compact_lake(base_dir: str, *, now_ts: float | None = None,
                 grace_s: float = _GRACE_S) -> dict:
    """Compact every CLOSED partition (its hour ended >= ``grace_s`` ago). Skips the
    live partition the recorder is appending to and any just-closed hour that can
    still receive late rows. One run at a time: ``_compaction.lock`` in the lake root
    (two overlapping runs could each delete a late file the other had not merged)."""
    if not os.path.isdir(base_dir):
        return {"partitions_compacted": 0, "partitions_skipped": 0,
                "files_removed": 0, "partitions_failed": 0}
    lock = os.path.join(base_dir, _LOCK_NAME)
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(f"another compaction holds {lock}; if none is running (a killed "
                           f"run), delete that file and retry") from None
    with os.fdopen(fd, "w") as fh:
        fh.write(f"pid={os.getpid()} started={time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime())}\n")
    try:
        return _compact_closed(base_dir, time.time() if now_ts is None else now_ts, grace_s)
    finally:
        os.remove(lock)


def _compact_closed(base_dir: str, now: float, grace_s: float) -> dict:
    compacted = skipped = files_removed = failed = 0
    for part in _iter_closed_partitions(base_dir, now - grace_s):
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
