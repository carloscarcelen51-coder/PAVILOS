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
from statistics import mean, median

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.replay import replay_snapshots
from pavilos.detection.static_levels import StaticLevelConfig
from pavilos.backtest.static_study import study_static_approaches, StaticStudyConfig, realized_vol_bps
from pavilos.signals.atr import ATR

BASE = r"D:\pavilos_book_data"
EXCLUDE = ("gemini", "kucoin")


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
    # the shared replay: 120 s warm-up (complete books at t0), hour pruning, restart-aware
    snaps = replay_snapshots(BASE, t0, t1, window_bps=rc.window_bps, bin_bps=rc.bin_bps,
                             interval_s=rc.snapshot_interval_s, staleness_s=rc.staleness_s,
                             exclude=EXCLUDE)

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
