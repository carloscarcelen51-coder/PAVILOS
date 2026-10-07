# scripts/_pressure_leads.py  (SCRATCH analysis — not committed)
"""FOUNDATIONAL test: does multi-venue book buying-pressure (depth imbalance) at
time T LEAD the forward mid return over horizon H, with margin over fees?

For each snapshot compute the bid/ask depth imbalance within several bands; for
each future horizon compute the forward mid return (bps). Then bin snapshots by
imbalance QUINTILE and report mean forward return per quintile + the top-minus-
bottom SPREAD (the tradeable lead) + the rank IC. Causal: imbalance at T uses the
book at T; the return uses T+H (the measured outcome). Excludes gemini/kucoin so
the replay is memory-bounded and the imbalance is not dominated by their
snapshot-spam. Subsamples (every ~5s) to curb autocorrelation. Prints JSON.

    python -m scripts._pressure_leads <t0> <t1>
"""
from __future__ import annotations

import json
import sys
from statistics import mean

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.replay import replay_snapshots

BASE = r"D:\pavilos_book_data"
EXCLUDE = ("gemini", "kucoin")
BANDS = (10.0, 30.0, 100.0)        # imbalance bands (bps from mid)
HORIZONS = (5.0, 30.0, 60.0, 300.0)


def _imbalance(snap, mid, band_bps):
    lo = mid * (1 - band_bps / 1e4)
    hi = mid * (1 + band_bps / 1e4)
    b = sum(x.size for x in snap.bids if x.price >= lo)
    a = sum(x.size for x in snap.asks if x.price <= hi)
    tot = b + a
    return (b - a) / tot if tot > 0 else 0.0


def _quintile_spread(pairs):
    """pairs = [(imbalance, fwd_return_bps)]. Sort by imbalance, return mean fwd
    of the TOP quintile minus the BOTTOM quintile (the tradeable lead) + IC sign."""
    pairs = [p for p in pairs if p[0] is not None and p[1] is not None]
    if len(pairs) < 25:
        return None
    pairs.sort(key=lambda p: p[0])
    q = len(pairs) // 5
    bot = mean(p[1] for p in pairs[:q])
    top = mean(p[1] for p in pairs[-q:])
    # rank IC: fraction of concordant high-imbalance==high-return (crude monotonicity)
    import statistics
    mid_i = statistics.median(p[0] for p in pairs)
    mid_r = statistics.median(p[1] for p in pairs)
    conc = sum(1 for im, r in pairs if (im > mid_i) == (r > mid_r))
    return {"top": round(top, 2), "bottom": round(bot, 2), "spread": round(top - bot, 2),
            "concordance": round(conc / len(pairs), 3), "n": len(pairs)}


def main():
    t0, t1 = float(sys.argv[1]), float(sys.argv[2])
    rc = RuntimeConfig()
    # the shared replay: 120 s warm-up (complete books at t0), hour pruning, restart-aware
    snaps = replay_snapshots(BASE, t0, t1, window_bps=rc.window_bps, bin_bps=rc.bin_bps,
                             interval_s=rc.snapshot_interval_s, staleness_s=rc.staleness_s,
                             exclude=EXCLUDE)

    mids = [s.mid for s in snaps]
    n = len(snaps)
    interval = rc.snapshot_interval_s
    sub = max(1, int(5.0 / interval))   # subsample every ~5s to curb autocorrelation

    result = {"t0": int(t0), "t1": int(t1), "snapshots": n,
              "net_drift_bps": round((mids[-1] - mids[0]) / mids[0] * 1e4, 1) if n >= 2 else 0.0,
              "by_band": {}}
    for band in BANDS:
        imb = [(_imbalance(s, s.mid, band) if s.mid > 0 else None) for s in snaps]
        horizons = {}
        for h in HORIZONS:
            off = int(round(h / interval))
            pairs = []
            for i in range(0, n - off, sub):
                if mids[i] <= 0:
                    continue
                fwd = (mids[i + off] - mids[i]) / mids[i] * 1e4
                pairs.append((imb[i], fwd))
            qs = _quintile_spread(pairs)
            if qs:
                horizons[str(int(h))] = qs
        result["by_band"][str(int(band))] = horizons
    print("PRESSURE_RESULT=" + json.dumps(result))


if __name__ == "__main__":
    main()
