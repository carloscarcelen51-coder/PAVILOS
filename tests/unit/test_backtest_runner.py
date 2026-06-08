# tests/unit/test_backtest_runner.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import run_backtest, BacktestResult


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=2)


def test_empty_snapshots_gives_flat_result():
    r = run_backtest([], config=RuntimeConfig(), starting_equity=10_000.0)
    assert isinstance(r, BacktestResult)
    assert r.n_snapshots == 0 and r.n_trades == 0 and r.final_equity == 10_000.0
    assert r.realized_pnl == 0.0 and r.win_rate == 0.0 and r.max_drawdown == 0.0


def test_backtest_runs_pipeline_and_reports_trades():
    # config that arms eagerly so the synthetic series produces at least one trade
    cfg = RuntimeConfig(entry_threshold=0.3, min_persistence_s=0.0, venues_target=2.0,
                        strength_target=5.0, persistence_target_s=1.0, entry_zone_bps=200.0,
                        opposing_distance_bps=50.0, det_window_bps=500.0)
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]  # support wall ~99
    asks = [_bin(105.0, 1.0)]
    # warm up persistence, then drive price up (fill) then down (stop) to close a trade
    snaps = [_snap(float(i), 99.5, bids, asks) for i in range(3)]
    snaps += [_snap(3.0, 103.0, bids, asks), _snap(4.0, 90.0, bids, asks)]
    r = run_backtest(snaps, config=cfg, starting_equity=10_000.0)
    assert r.n_snapshots == 5
    assert r.n_trades >= 1
    assert r.wins + r.losses == r.n_trades
    assert r.max_drawdown >= 0.0
    # final equity == starting + realized (flat at end: backtest force-closes any open position)
    assert abs(r.final_equity - (10_000.0 + r.realized_pnl)) < 1e-6
