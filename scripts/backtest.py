# scripts/backtest.py
"""Offline backtest CLI. Usage:

    python -m scripts.backtest <recording.jsonl> single
    python -m scripts.backtest <recording.jsonl> sweep
    python -m scripts.backtest <recording.jsonl> walkforward [n_splits]

'sweep'/'walkforward' use a small built-in grid over the most impactful params.
"""
from __future__ import annotations

import sys

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.io import load_snapshots
from pavilos.backtest.runner import run_backtest, BacktestResult
from pavilos.backtest.sweep import grid_search, walk_forward

_GRID = {
    "entry_threshold": [0.4, 0.55, 0.7],
    "opposing_distance_bps": [5.0, 10.0, 20.0],
    "entry_zone_bps": [15.0, 30.0, 60.0],
    "atr_stop_mult": [2.0, 3.0, 5.0],
}


def format_result(r: BacktestResult) -> str:
    return (f"trades={r.n_trades} win={r.win_rate:.1f}% ret={r.return_pct:+.2f}% "
            f"pnl={r.realized_pnl:+.2f} fees={r.fees:.2f} maxDD={r.max_drawdown:.2f}"
            f"({r.max_drawdown_pct:.2f}%) eq={r.final_equity:.2f} n={r.n_snapshots}")


def _short(cfg: RuntimeConfig) -> str:
    return (f"entryTh={cfg.entry_threshold} oppBps={cfg.opposing_distance_bps} "
            f"zoneBps={cfg.entry_zone_bps} atrMult={cfg.atr_stop_mult}")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        return
    path, mode = sys.argv[1], sys.argv[2]
    snaps = load_snapshots(path)
    base = RuntimeConfig()
    eq = base.starting_equity
    print(f"loaded {len(snaps)} snapshots from {path}")
    if mode == "single":
        print("single:", format_result(run_backtest(snaps, config=base, starting_equity=eq)))
    elif mode == "sweep":
        ranked = grid_search(snaps, base_config=base, grid=_GRID, starting_equity=eq)
        print(f"=== top 10 of {len(ranked)} configs (by return%) ===")
        for cfg, r in ranked[:10]:
            print(f"  {format_result(r)}   [{_short(cfg)}]")
    elif mode == "walkforward":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        folds = walk_forward(snaps, base_config=base, grid=_GRID, n_splits=n_splits, starting_equity=eq)
        print(f"=== walk-forward, {n_splits} folds (OOS is the honest number) ===")
        for i, f in enumerate(folds):
            print(f"  fold {i}: IS {format_result(f['is_result'])}")
            print(f"          OOS {format_result(f['oos_result'])}   [{_short(f['config'])}]")
        if folds:
            avg_oos = sum(f["oos_result"].return_pct for f in folds) / len(folds)
            print(f"  >>> mean OOS return = {avg_oos:+.2f}%  (the number that matters)")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
