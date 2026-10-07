# src/pavilos/persistence/runs.py
"""Recorder run markers: one JSON line per recorder start in ``<lake>/_runs.jsonl``.

A new recorder process starts with EMPTY books (live) and seq_no from 0, but nothing in
the rows says where one process ends and the next begins. The replay (and
``lake_check``) reset every book at each marked start, so a window just after a
restart neither keeps the books of a process that is gone nor wipes those the new one
has already written.

A start is never earlier than the newest row already in the lake NOR than the
previous start (the PC clock may have stepped back across the restart, and a run can
die before writing anything); the recorder writes no ts below it, so every row of the
new run sorts after its marker and every row of an older run before it. If that puts
the run more than a minute ahead of the PC clock, it is logged at CRITICAL."""
from __future__ import annotations

import json
import logging
import os
import time

import pyarrow.parquet as pq

from pavilos.persistence.lake_files import check_files, newest_hour_dirs, parquet_files

_log = logging.getLogger(__name__)

RUNS_FILE = "_runs.jsonl"
_NEWEST_FILES = 3          # per venue: the last flushes hold its newest rows
_LEAD_CRITICAL_S = 60.0


def lake_max_ts(base_dir: str) -> float | None:
    """The newest row ts in the lake, from the 3 newest READABLE files of every venue's
    newest hour (Parquet statistics; the ``ts`` column itself only if a file has none).
    None if the lake is empty."""
    best = None
    for d in newest_hour_dirs(base_dir):
        taken = 0
        for p in sorted(parquet_files(d), key=os.path.getmtime, reverse=True):
            if taken == _NEWEST_FILES:
                break
            if not check_files([p])[0]:
                continue
            taken += 1
            hi = _file_max_ts(p)
            if hi is not None and (best is None or hi > best):
                best = hi
    return best


def _file_max_ts(p: str) -> float | None:
    md = pq.read_metadata(p)
    col = md.schema.names.index("ts")
    best = None
    for i in range(md.num_row_groups):
        st = md.row_group(i).column(col).statistics
        if st is None or not st.has_min_max:
            # ParquetFile reads the file as is (pq.read_table would infer partition
            # columns from the exchange=/date= path and clash with the file's own).
            ts = pq.ParquetFile(p).read(columns=["ts"]).column("ts").to_pylist()
            return max((t for t in ts if t is not None), default=None)
        best = st.max if best is None else max(best, st.max)
    return None if best is None else float(best)


def _read_starts(path: str) -> list[float]:
    try:
        fh = open(path, encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return []
    out: list[float] = []
    with fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(float(json.loads(line)["start_ts"]))
            except (ValueError, KeyError, TypeError):
                _log.warning("runs: skipping malformed line %d of %s", n, path)
    return out


def mark_run_start(base_dir: str, *, ts: float | None = None) -> float:
    """Append a recorder start and return it. Without ``ts``: now, but never earlier
    than the newest row already written nor than the last marked start. Call it BEFORE
    the recorder writes anything."""
    path = os.path.join(base_dir, RUNS_FILE)
    if ts is None:
        ts = time.time()
        floors = [x for x in (lake_max_ts(base_dir), max(_read_starts(path), default=None))
                  if x is not None]
        if floors and max(floors) >= ts:
            ahead = max(floors) - ts
            start = max(floors) + 0.001
            level = logging.CRITICAL if ahead > _LEAD_CRITICAL_S else logging.WARNING
            _log.log(level, "runs: the PC clock is %.3f s behind the lake (newest row / last run "
                            "start); this run's ts start at %.3f until the clock catches up",
                     ahead, start)
            ts = start
    os.makedirs(base_dir, exist_ok=True)
    line = json.dumps({"start_ts": ts, "pid": os.getpid(),
                       "start_iso": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(ts))})
    glue = os.path.isfile(path) and os.path.getsize(path) > 0 and not _ends_with_newline(path)
    with open(path, "ab") as fh:
        if glue:
            fh.write(b"\n")                      # never glue onto a cut-off last line
        fh.write((line + "\n").encode("utf-8"))
        fh.flush()
        os.fsync(fh.fileno())
    return ts


def _ends_with_newline(path: str) -> bool:
    with open(path, "rb") as fh:
        fh.seek(-1, os.SEEK_END)
        return fh.read(1) == b"\n"


def run_starts(base_dir: str, t0: float, t1: float) -> list[float]:
    """Sorted recorder starts with ``t0 < start <= t1`` (malformed lines skipped)."""
    return sorted(s for s in _read_starts(os.path.join(base_dir, RUNS_FILE)) if t0 < s <= t1)
