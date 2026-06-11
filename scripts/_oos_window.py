# scripts/_oos_window.py  (SCRATCH analysis — not committed)
"""Light static-level study for ONE window, EXCLUDING the over-recording venues
(gemini, kucoin). Measures, per approach episode: the LONG bounce AND the SHORT
breakdown (mirror bracket), plus a DRIFT control — the window net drift and a
generic-short baseline at sampled snapshots — so a "breakdown edge" that is really
just downward drift in the (volatility-selected) window is exposed. Prints JSON.

    python -m scripts._oos_window <t0> <t1>
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from statistics import mean, median

import duckdb

from pavilos.core.models import BookUpdate
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.venues import VENUE_SPECS
from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.static_levels import StaticLevelConfig
from pavilos.backtest.static_study import study_static_approaches, StaticStudyConfig, realized_vol_bps
from pavilos.signals.atr import ATR

BASE = r"D:\pavilos_book_data"
EXCLUDE = ("gemini", "kucoin")


def _iter_updates(t0: float, t1: float):
    con = duckdb.connect()
    con.sql("SET memory_limit='4GB'")
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


def _short_outcome(snaps, i, entry, risk, horizon_s, target_r):
    """Mirror of the long bounce: a SHORT 'breaks' if price falls target_r*R
    (delta <= -target_r*risk) BEFORE rising 1R (delta >= +risk), in time order."""
    entry_ts = snaps[i].ts
    for j in range(i + 1, len(snaps)):
        s = snaps[j]
        if s.ts - entry_ts > horizon_s:
            break
        delta = s.mid - entry
        if risk > 0.0:
            if delta <= -target_r * risk:
                return True, True            # broke (short win)
            if delta >= risk:
                return False, True           # short stopped
    return False, False                      # undecided


def main() -> None:
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

    HORIZON, TARGET, MULT = 300.0, 1.0, 3.0
    slc = StaticLevelConfig(level_bucket_usd=25.0, size_multiple=3.0, stale_s=15.0, min_venues=6,
                            level_threshold=0.0, min_away_bps=25.0, max_reach_bps=400.0,
                            venues_target=8.0, duration_target_s=30.0)
    sc = StaticStudyConfig(static=slc, horizon_s=HORIZON, target_r=TARGET, stop_offset_bps=5.0,
                           atr_stop_mult=MULT, entry_zone_bps=30.0, episode_gap_s=5.0,
                           buckets=[0.0, 0.25, 0.5, 0.75, 1.0], atr_window=14)
    obs = study_static_approaches(snaps, sc)
    ts_to_i = {s.ts: i for i, s in enumerate(snaps)}

    # Causal ATR value AFTER each snapshot (so support-short and baseline-short can use
    # the SAME R = ATR*MULT -> a fair, R-matched comparison).
    atr = ATR(window=14)
    atr_at = []
    for s in snaps:
        atr.update(s.mid)
        atr_at.append(atr.value())

    strong = [o for o in obs if o.level_strength >= 0.75]
    long_dec = [o for o in strong if o.decided]
    long_exp = (mean((TARGET if o.bounced else -1.0) for o in long_dec)) if long_dec else 0.0

    # SUPPORT-short with the LEVEL-based risk the original study used (~tens of bps,
    # not the sub-bp ATR floor). R_bps from this real R.
    sup = []; r_bps = []
    for o in strong:
        i = ts_to_i.get(o.ts)
        if i is None or o.risk <= 0.0:
            continue
        broke, dec = _short_outcome(snaps, i, o.entry, o.risk, HORIZON, TARGET)
        if dec:
            sup.append(TARGET if broke else -1.0)
        r_bps.append(o.risk / o.entry * 1e4)
    sup_exp = mean(sup) if sup else 0.0
    med_risk_abs = median([o.risk for o in strong if o.risk > 0.0]) if strong else 0.0

    # R-MATCHED baseline: generic short at sampled snaps using the SAME (median
    # level-based) absolute risk -> a fair drift control at the SAME bracket scale.
    net_drift_bps = ((snaps[-1].mid - snaps[0].mid) / snaps[0].mid * 1e4) if len(snaps) >= 2 else 0.0
    base = []
    step = max(1, int(30.0 / rc.snapshot_interval_s))
    for i, s in enumerate(snaps):
        if i % step != 0 or med_risk_abs <= 0.0:
            continue
        broke, dec = _short_outcome(snaps, i, s.mid, med_risk_abs, HORIZON, TARGET)
        if dec:
            base.append(TARGET if broke else -1.0)
    base_exp = mean(base) if base else 0.0

    out = {
        "t0": int(t0), "t1": int(t1), "snapshots": len(snaps),
        "vol_bps_tick": round(realized_vol_bps(snaps), 3),
        "net_drift_bps": round(net_drift_bps, 1),
        "strong": len(strong),
        "long_decided": len(long_dec), "long_exp_r": round(long_exp, 3),
        "Rmatched_support_short_decided": len(sup), "Rmatched_support_short_exp_r": round(sup_exp, 3),
        "Rmatched_baseline_short_decided": len(base), "Rmatched_baseline_short_exp_r": round(base_exp, 3),
        "median_R_bps": round(median(r_bps), 1) if r_bps else 0.0,
    }
    print("OOS_RESULT=" + json.dumps(out))


if __name__ == "__main__":
    main()
