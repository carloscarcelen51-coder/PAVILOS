# scripts/_pressure_pilot.py  (SCRATCH analysis — not committed)
"""VALIDATE the user's idea: use the (real but ~3bps) near-touch pressure signal
NOT to scalp the micro move, but as a CONTINUOUS PILOT of a dynamic stop — enter on
bullish pressure, RIDE while pressure persists, exit when it flips. Sometimes a
quick scratch (pay the fee); sometimes a long ride that amortises the fee over a
big gain. Net after a real round-trip fee is the verdict.

State machine over the 12-venue (gemini/kucoin-excluded) combined book:
  FLAT  -> imbalance > +enter_T  => LONG  (entry fee)
        -> imbalance < -enter_T  => SHORT (entry fee)
  LONG  -> imbalance < exit_T    => close (exit fee), realise return
  SHORT -> imbalance > -exit_T   => close (exit fee)
Causal (decide on imbalance<=T, fill at that snap's mid). Reports per-trade net
expectancy in bps after fees, pooled. Prints JSON.

    python -m scripts._pressure_pilot <t0> <t1>
"""
from __future__ import annotations

import json
import sys
from statistics import mean

import duckdb

from pavilos.core.models import BookUpdate
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.venues import VENUE_SPECS
from pavilos.core.runtime import RuntimeConfig

BASE = r"D:\pavilos_book_data"
EXCLUDE = ("gemini", "kucoin")
BAND = 10.0                       # the only band that led (near-touch)
FEE_RT_BPS = 10.0                 # round-trip taker (5bps/side)
# (enter_T, exit_T) grid: exit_T < enter_T rides longer (hysteresis); exit at 0 cuts sooner.
GRID = [(0.1, 0.0), (0.2, 0.0), (0.3, 0.0), (0.2, -0.1), (0.3, -0.2), (0.3, 0.1)]


def _iter_updates(t0, t1):
    con = duckdb.connect(); con.sql("SET memory_limit='4GB'")
    excl = ",".join("'" + e + "'" for e in EXCLUDE)
    res = con.execute(
        f"SELECT ts,exchange,seq_no,is_snapshot,side,price,size FROM '{BASE}/**/*.parquet' "
        f"WHERE ts>=? AND ts<=? AND exchange NOT IN ({excl}) ORDER BY ts,exchange,seq_no",
        [float(t0), float(t1)])
    key = None; ts = ex = snap = None; bids: list = []; asks: list = []
    while True:
        rows = res.fetchmany(50000)
        if not rows:
            break
        for r_ts, r_ex, r_seq, r_snap, side, price, size in rows:
            k = (r_ts, r_ex, r_seq)
            if k != key:
                if key is not None:
                    yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=snap, seq=None)
                key = k; ts = r_ts; ex = r_ex; snap = r_snap; bids = []; asks = []
            (bids if side == "bid" else asks).append((price, size))
    if key is not None:
        yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=snap, seq=None)


def _imb(snap):
    mid = snap.mid
    if mid <= 0:
        return 0.0
    lo = mid * (1 - BAND / 1e4); hi = mid * (1 + BAND / 1e4)
    b = sum(x.size for x in snap.bids if x.price >= lo)
    a = sum(x.size for x in snap.asks if x.price <= hi)
    return (b - a) / (b + a) if (b + a) > 0 else 0.0


def _simulate(mids, imbs, enter_T, exit_T):
    """Return list of per-trade NET returns (bps, after round-trip fee)."""
    trades = []; pos = 0; entry = 0.0
    for i in range(len(mids)):
        im = imbs[i]; m = mids[i]
        if m <= 0:
            continue
        if pos == 0:
            if im > enter_T:
                pos, entry = 1, m
            elif im < -enter_T:
                pos, entry = -1, m
        elif pos == 1:
            if im < exit_T:
                trades.append((m - entry) / entry * 1e4 - FEE_RT_BPS); pos = 0
        elif pos == -1:
            if im > -exit_T:
                trades.append((entry - m) / entry * 1e4 - FEE_RT_BPS); pos = 0
    if pos != 0:   # close at last mid
        m = mids[-1]
        trades.append(((m - entry) if pos == 1 else (entry - m)) / entry * 1e4 - FEE_RT_BPS)
    return trades


def main():
    t0, t1 = float(sys.argv[1]), float(sys.argv[2])
    rc = RuntimeConfig()
    agg = Aggregator(list(VENUE_SPECS), PegProvider(), bin_bps=rc.bin_bps,
                     window_bps=rc.window_bps, staleness_s=rc.staleness_s)
    snaps = []; nb = None
    for u in _iter_updates(t0, t1):
        if nb is None:
            nb = u.ts
        while nb < u.ts:
            s = agg.snapshot(nb)
            if s is not None:
                snaps.append(s)
            nb += rc.snapshot_interval_s
        agg.apply(u)
    if nb is not None:
        s = agg.snapshot(nb)
        if s is not None:
            snaps.append(s)

    mids = [s.mid for s in snaps]
    imbs = [_imb(s) for s in snaps]
    out = {"t0": int(t0), "t1": int(t1), "snapshots": len(snaps), "grid": {}}
    for enter_T, exit_T in GRID:
        tr = _simulate(mids, imbs, enter_T, exit_T)
        if tr:
            wins = [x for x in tr if x > 0]
            out["grid"][f"e{enter_T}_x{exit_T}"] = {
                "trades": len(tr), "win_rate": round(len(wins) / len(tr), 3),
                "net_total_bps": round(sum(tr), 1), "net_per_trade_bps": round(mean(tr), 2),
                "avg_win_bps": round(mean(wins), 1) if wins else 0.0,
                "avg_loss_bps": round(mean([x for x in tr if x <= 0]), 1) if any(x <= 0 for x in tr) else 0.0,
                "best_bps": round(max(tr), 1), "worst_bps": round(min(tr), 1)}
    print("PILOT_RESULT=" + json.dumps(out))


if __name__ == "__main__":
    main()
