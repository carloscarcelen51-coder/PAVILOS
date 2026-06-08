# src/pavilos/backtest/sweep.py
"""Grid-search over config params + walk-forward validation (optimize in-sample,
score out-of-sample) to avoid overfitting to one slice. Pure."""
from __future__ import annotations

import dataclasses
import itertools

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import run_backtest, BacktestResult


def _configs(base_config: RuntimeConfig, grid: dict) -> list[RuntimeConfig]:
    if not grid:
        return [base_config]
    keys = list(grid)
    combos = itertools.product(*(grid[k] for k in keys))
    return [dataclasses.replace(base_config, **dict(zip(keys, combo))) for combo in combos]


def grid_search(snapshots, *, base_config: RuntimeConfig, grid: dict,
                starting_equity: float) -> list[tuple[RuntimeConfig, BacktestResult]]:
    """Run a backtest for every grid combo; return (config, result) sorted by
    return_pct descending (best first)."""
    out = [(c, run_backtest(snapshots, config=c, starting_equity=starting_equity))
           for c in _configs(base_config, grid)]
    out.sort(key=lambda cr: cr[1].return_pct, reverse=True)
    return out


def walk_forward(snapshots, *, base_config: RuntimeConfig, grid: dict, n_splits: int,
                 starting_equity: float) -> list[dict]:
    """Split snapshots into ``n_splits`` contiguous folds. For each adjacent pair,
    grid-search on fold k (in-sample), then score the winning config on fold k+1
    (out-of-sample). The OOS result is the honest performance estimate."""
    if n_splits < 2:
        raise ValueError("n_splits must be >= 2")
    size = len(snapshots) // n_splits
    if size == 0:
        return []
    folds = [snapshots[i * size:(i + 1) * size] for i in range(n_splits)]
    out: list[dict] = []
    for k in range(n_splits - 1):
        ranked = grid_search(folds[k], base_config=base_config, grid=grid, starting_equity=starting_equity)
        best_cfg, is_result = ranked[0]
        oos_result = run_backtest(folds[k + 1], config=best_cfg, starting_equity=starting_equity)
        out.append({"config": best_cfg, "is_result": is_result, "oos_result": oos_result})
    return out
