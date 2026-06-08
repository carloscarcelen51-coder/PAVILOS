# tests/unit/test_backtest_sweep.py
import dataclasses
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.sweep import grid_search, walk_forward


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snaps(n):
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]
    asks = [_bin(105.0, 1.0)]
    out = [CombinedDepthSnapshot(ts=float(i), mid=99.5 if i < 3 else (103.0 if i % 2 else 92.0),
                                 bids=tuple(bids), asks=tuple(asks), venues_active=("k", "b"), venues_total=2)
           for i in range(n)]
    return out


_BASE = RuntimeConfig(min_persistence_s=0.0, venues_target=2.0, strength_target=5.0,
                      persistence_target_s=1.0, entry_zone_bps=200.0, det_window_bps=500.0)


def test_grid_search_runs_every_combo_and_ranks():
    grid = {"entry_threshold": [0.3, 0.9], "opposing_distance_bps": [8.0, 50.0]}
    results = grid_search(_snaps(12), base_config=_BASE, grid=grid, starting_equity=10_000.0)
    assert len(results) == 4                       # 2 x 2 cartesian
    cfgs = [dataclasses.asdict(c)["entry_threshold"] for c, _ in results]
    assert set(cfgs) == {0.3, 0.9}
    # sorted best-first by return_pct (descending)
    rets = [r.return_pct for _, r in results]
    assert rets == sorted(rets, reverse=True)


def test_walk_forward_reports_in_and_out_of_sample():
    grid = {"entry_threshold": [0.3, 0.9]}
    folds = walk_forward(_snaps(20), base_config=_BASE, grid=grid, n_splits=2, starting_equity=10_000.0)
    assert len(folds) == 1                          # 2 splits -> 1 IS->OOS transition
    f = folds[0]
    assert "is_result" in f and "oos_result" in f and "config" in f
    assert f["is_result"].n_snapshots > 0 and f["oos_result"].n_snapshots > 0
