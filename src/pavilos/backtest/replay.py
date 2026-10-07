# src/pavilos/backtest/replay.py
"""Re-aggregate the recorded raw-L2 lake (M10) into the combined-snapshot stream,
through the REAL Aggregator, so it is faithful to live by construction.

Lake rows for one BookUpdate share (ts, exchange, seq_no). We stream them in
(ts, exchange, seq_no) order, group into BookUpdates, apply them to a real
Aggregator, and emit a snapshot at each cadence boundary (every interval_s of
recorded time, after applying all updates with ts <= boundary). For a given book
state this reconstructs the snapshot Aggregator.run would produce; because cadence
snapshots are invariant to intra-tick venue ordering, the per-boundary book content
matches live. Boundary timing is RECORDED-time (not wall-clock) by design for
deterministic backtests, and the trailing quiet tail is NOT padded -- exactly one
final boundary is emitted after the last update (live would re-emit unchanged
snapshots each interval during a quiet tail; duplicates carry no new info here).

Reading (M17): only the hour partitions that can hold rows of the requested span are
read -- rows are partitioned by their own ts, so the pruning is exact; an unreadable
Parquet file (truncated, empty, damaged footer) is skipped with a warning instead of
failing every replay; and at every recorder start marked in ``_runs.jsonl`` all books
are reset, since the new live process began with empty books."""
from __future__ import annotations

import logging
import os

import duckdb

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.venues import VENUE_SPECS
from pavilos.persistence.duck import connect
from pavilos.persistence.lake_files import check_files, hour_dirs, parquet_files
from pavilos.persistence.runs import run_starts

_log = logging.getLogger(__name__)

_SPECS_FN = lambda: list(VENUE_SPECS)   # indirection so tests can inject a venue subset
_BATCH = 50_000


def _query(con, sources: list[str], t0: float, t1: float):
    lst = ", ".join("'" + s.replace("\\", "/").replace("'", "''") + "'" for s in sources)
    return con.execute(
        f"SELECT ts, exchange, seq_no, is_snapshot, side, price, size "
        f"FROM read_parquet([{lst}], hive_partitioning=false) "
        f"WHERE ts >= ? AND ts <= ? ORDER BY ts, exchange, seq_no", [float(t0), float(t1)])


def iter_lake(base_dir: str, t0: float, t1: float, *, exclude=()):
    """Yield the lake's BookUpdates with ts in [t0, t1] in (ts, exchange, seq_no) order,
    streamed. ``seq`` carries the recorder's seq_no (per venue, from 0 each process)."""
    dirs = hour_dirs(base_dir, t0, t1, exclude)
    if not dirs:
        return
    con = connect()
    try:
        try:
            res = _query(con, [os.path.join(d, "*.parquet") for d in dirs], t0, t1)
            rows = res.fetchmany(_BATCH)        # ORDER BY: every file is read before row 1
        except duckdb.Error:
            files, bad = check_files([p for d in dirs for p in parquet_files(d)])
            if not bad:
                raise
            _log.warning("replay: skipping %d unreadable parquet file(s) (truncated/empty/damaged), "
                         "e.g. %s", len(bad), bad[:3])
            if not files:
                return
            res = _query(con, files, t0, t1)
            rows = res.fetchmany(_BATCH)
        key = None
        ts = ex = seq = snap = None
        bids: list = []
        asks: list = []
        while rows:
            for r_ts, r_ex, r_seq, r_snap, side, price, size in rows:
                k = (r_ts, r_ex, r_seq)
                if k != key:
                    if key is not None:
                        yield BookUpdate(ex, ts, tuple(bids), tuple(asks), snap, seq)
                    key = k; ts = r_ts; ex = r_ex; seq = r_seq; snap = r_snap; bids = []; asks = []
                (bids if side == "bid" else asks).append((price, size))
            rows = res.fetchmany(_BATCH)
        if key is not None:
            yield BookUpdate(ex, ts, tuple(bids), tuple(asks), snap, seq)
    finally:
        con.close()


def replay_snapshots(base_dir: str, t0: float, t1: float, *, window_bps: float,
                     bin_bps: float, interval_s: float, staleness_s: float,
                     warmup_s: float = 120.0, exclude=()) -> list[CombinedDepthSnapshot]:
    """Reconstruct the combined-snapshot stream for [t0, t1] at the given aggregate
    config. Faithful to live (reuses the real Aggregator + cadence emission).

    ``warmup_s``: updates in [t0 - warmup_s, t0) are applied to build the books but
    emit nothing, so every venue starts the window from a complete base (the recorder
    writes a full checkpoint of every active venue every 60 s, and a venue's first
    write after a silence whole). Boundaries are anchored on the first update at/after
    ``t0`` exactly as without warm-up. ``warmup_s=0`` reads only the window (the
    pre-M17 behaviour, except for the reset at marked recorder starts): with M17 data
    every venue then starts from just the levels that change inside the window --
    keep the default. ``exclude``: venues to leave out (they never become active)."""
    agg = Aggregator(_SPECS_FN(), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=staleness_s)
    lo = t0 - max(0.0, warmup_s)
    starts = run_starts(base_dir, lo, t1)
    out: list[CombinedDepthSnapshot] = []
    next_b: float | None = None
    si = 0
    for u in iter_lake(base_dir, lo, t1, exclude=exclude):
        while si < len(starts) and starts[si] <= u.ts:   # a recorder (re)started before u
            if next_b is not None:
                while next_b < starts[si]:     # boundaries before it: the old run's books
                    s = agg.snapshot(next_b)
                    if s is not None:
                        out.append(s)
                    next_b += interval_s
            agg.reset()                        # the new live process began with empty books
            si += 1
        if next_b is None and u.ts >= t0:
            next_b = u.ts                      # first cadence boundary AT the first update's ts
        if next_b is not None:
            while next_b < u.ts:               # emit boundaries fully covered by applied updates
                s = agg.snapshot(next_b)       # state = all updates with ts <= next_b (u not yet applied)
                if s is not None:
                    out.append(s)
                next_b += interval_s
        agg.apply(u)                           # before t0: warm-up only, nothing emitted
    if next_b is not None:                      # final boundary: everything applied
        while si < len(starts) and starts[si] <= next_b:   # a restart after the last row
            agg.reset()
            si += 1
        s = agg.snapshot(next_b)
        if s is not None:
            out.append(s)
    return out
