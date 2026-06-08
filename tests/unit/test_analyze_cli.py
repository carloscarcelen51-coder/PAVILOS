from scripts.analyze import format_sweep_row, _lake_span
from pavilos.backtest.runner import BacktestResult


def test_format_sweep_row_readable():
    row = {"window_bps": 300.0, "n_snapshots": 1000,
           "detection": {"avg_zones_per_snapshot": 4.2, "avg_confidence": 0.55,
                         "avg_venues_per_zone": 3.1, "frac_snaps_with_strong_zone": 0.8},
           "backtest": BacktestResult(1000, 5, 3, 2, 60.0, 12.0, 4.0, 0.12, 10012.0, 8.0, 0.08)}
    s = format_sweep_row(row)
    assert "win=300" in s and "zones/snap=4.2" in s and "strong=80%" in s and "ret=" in s


def test_lake_span_missing_dir_returns_none(tmp_path):
    assert _lake_span(str(tmp_path / "nope")) is None
