# scripts/analyze.py
"""Analyse the recorded raw-L2 lake. Usage:

    python -m scripts.analyze <data_dir> window-sweep [t0 t1]
    python -m scripts.analyze <data_dir> walkforward [n_splits] [t0 t1]

window-sweep: re-aggregate at 200/300/500/1000 bps -> detection profile + backtest.
walkforward : re-aggregate at the configured window -> in-sample-optimise /
              out-of-sample-score the strategy params (OOS return is the verdict).
"""
from __future__ import annotations

import sys

import duckdb

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import BacktestResult
from pavilos.backtest.replay import replay_snapshots
from pavilos.backtest.analysis import window_sweep
from pavilos.backtest.sweep import walk_forward

_WINDOWS = [200.0, 300.0, 500.0, 1000.0]
_WF_GRID = {
    "entry_threshold": [0.4, 0.55, 0.7],
    "opposing_distance_bps": [5.0, 10.0, 20.0],
    "entry_zone_bps": [15.0, 30.0, 60.0],
    "atr_stop_mult": [2.0, 3.0, 5.0],
}


def _lake_span(base_dir: str):
    try:
        r = duckdb.sql(f"SELECT min(ts), max(ts), count(*) FROM '{base_dir}/**/*.parquet'").fetchone()
    except Exception:
        return None
    if r is None or r[0] is None:
        return None
    return float(r[0]), float(r[1]), int(r[2])


def format_sweep_row(row: dict) -> str:
    d = row["detection"]; b = row["backtest"]
    return (f"win={row['window_bps']:.0f}  snaps={row['n_snapshots']}  "
            f"zones/snap={d['avg_zones_per_snapshot']:.1f}  conf={d['avg_confidence']:.2f}  "
            f"venues/zone={d['avg_venues_per_zone']:.1f}  strong={d['frac_snaps_with_strong_zone']*100:.0f}%  "
            f"| trades={b.n_trades} ret={b.return_pct:+.2f}% win={b.win_rate:.0f}%")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__); return
    base, mode = sys.argv[1], sys.argv[2]
    span = _lake_span(base)
    if span is None:
        print(f"no data under {base}"); return
    t0, t1, n = span
    base_cfg = RuntimeConfig()
    print(f"lake: {n:,} rows, span {t1 - t0:.0f}s ({(t1-t0)/3600:.2f}h)")
    if (t1 - t0) < 3600:
        print("WARNING: < 1h of data -- results are PRELIMINARY (noise). Keep recording for a real verdict.")

    if mode == "window-sweep":
        a = float(sys.argv[3]) if len(sys.argv) > 3 else t0
        b = float(sys.argv[4]) if len(sys.argv) > 4 else t1
        print(f"=== window sweep over [{a:.0f},{b:.0f}] ===")
        for row in window_sweep(base, a, b, _WINDOWS, base_config=base_cfg, starting_equity=base_cfg.starting_equity):
            print("  " + format_sweep_row(row))
    elif mode == "walkforward":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        print(f"=== walk-forward, {n_splits} folds, {len(snaps)} snapshots @ window={base_cfg.window_bps} ===")
        folds = walk_forward(snaps, base_config=base_cfg, grid=_WF_GRID, n_splits=n_splits,
                             starting_equity=base_cfg.starting_equity)
        for i, f in enumerate(folds):
            isr, oos = f["is_result"], f["oos_result"]
            print(f"  fold {i}: IS ret={isr.return_pct:+.2f}% ({isr.n_trades} tr) -> "
                  f"OOS ret={oos.return_pct:+.2f}% ({oos.n_trades} tr)")
        if folds:
            avg = sum(f["oos_result"].return_pct for f in folds) / len(folds)
            tot = sum(f["oos_result"].n_trades for f in folds)
            print(f"  >>> mean OOS return = {avg:+.2f}%  over {tot} OOS trades  (the verdict)")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
