# src/pavilos/backtest/replay.py
"""Re-aggregate the recorded raw-L2 lake (M10) into the combined-snapshot stream,
through the REAL Aggregator, so it is faithful to live by construction.

Lake rows for one BookUpdate share (ts, exchange, seq_no). We stream them in
(ts, exchange, seq_no) order, group into BookUpdates, apply them to a real
Aggregator, and emit a snapshot at each cadence boundary (every interval_s of
recorded time, after applying all updates with ts <= boundary) -- exactly like
Aggregator.run. Cadence snapshots are invariant to intra-tick venue ordering, so
this reproduces the live snapshot stream."""
from __future__ import annotations

import duckdb

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.venues import VENUE_SPECS

_SPECS_FN = lambda: list(VENUE_SPECS)   # indirection so tests can inject a venue subset
_BATCH = 50_000


def _iter_updates(base_dir: str, t0: float, t1: float):
    """Yield reconstructed BookUpdates in (ts, exchange, seq_no) order, streamed."""
    con = duckdb.connect()
    try:
        res = con.execute(
            f"SELECT ts, exchange, seq_no, is_snapshot, side, price, size "
            f"FROM '{base_dir}/**/*.parquet' WHERE ts >= ? AND ts <= ? "
            f"ORDER BY ts, exchange, seq_no", [float(t0), float(t1)])
    except duckdb.IOException:
        return                              # no parquet files under base_dir
    key = None
    ts = ex = snap = None
    bids: list = []
    asks: list = []
    while True:
        rows = res.fetchmany(_BATCH)
        if not rows:
            break
        for r_ts, r_ex, r_seq, r_snap, side, price, size in rows:
            k = (r_ts, r_ex, r_seq)
            if k != key:
                if key is not None:
                    yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks),
                                     is_snapshot=snap, seq=None)
                key = k; ts = r_ts; ex = r_ex; snap = r_snap; bids = []; asks = []
            (bids if side == "bid" else asks).append((price, size))
    if key is not None:
        yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks),
                         is_snapshot=snap, seq=None)


def replay_snapshots(base_dir: str, t0: float, t1: float, *, window_bps: float,
                     bin_bps: float, interval_s: float, staleness_s: float) -> list[CombinedDepthSnapshot]:
    """Reconstruct the combined-snapshot stream for [t0, t1] at the given aggregate
    config. Faithful to live (reuses the real Aggregator + cadence emission)."""
    agg = Aggregator(_SPECS_FN(), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=staleness_s)
    out: list[CombinedDepthSnapshot] = []
    next_b: float | None = None
    for u in _iter_updates(base_dir, t0, t1):
        if next_b is None:
            next_b = u.ts                      # first cadence boundary AT the first update's ts
        while next_b < u.ts:                   # emit boundaries fully covered by applied updates
            s = agg.snapshot(next_b)           # state = all updates with ts <= next_b (u not yet applied)
            if s is not None:
                out.append(s)
            next_b += interval_s
        agg.apply(u)
    if next_b is not None:                      # final boundary: everything applied
        s = agg.snapshot(next_b)
        if s is not None:
            out.append(s)
    return out
