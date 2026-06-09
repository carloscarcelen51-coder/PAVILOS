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


def test_format_mode_row_readable():
    from scripts.analyze import format_mode_row
    from pavilos.backtest.runner import BacktestResult
    folds = [{"is_result": BacktestResult(500, 8, 5, 3, 62.5, 40.0, 8.0, 0.40, 10040.0, 20.0, 0.2),
              "oos_result": BacktestResult(500, 6, 2, 4, 33.3, -15.0, 6.0, -0.15, 9985.0, 30.0, 0.3)}]
    s = format_mode_row("reversion", folds)
    assert "reversion" in s and "OOS" in s and "trades=" in s


def test_format_study_row_readable():
    from scripts.analyze import format_study_row
    row = {"bucket": "[0.75,1.00)", "n": 42, "bounce_rate": 0.667,
           "mean_fwd_return_bps": 12.5, "mean_mfe_r": 1.8, "mean_mae_r": -0.4,
           "expectancy_r": 0.95}
    s = format_study_row(row)
    assert "[0.75,1.00)" in s
    assert "n=42" in s
    assert "67%" in s          # bounce_rate as a percentage
    assert "+0.95" in s        # expectancy_r in R, signed
    assert "+12.5" in s        # mean forward return in bps, signed


def test_format_study_row_baseline_negative():
    from scripts.analyze import format_study_row
    row = {"bucket": "ALL", "n": 5, "bounce_rate": 0.2,
           "mean_fwd_return_bps": -3.2, "mean_mfe_r": 0.5, "mean_mae_r": -1.1,
           "expectancy_r": -0.4}
    s = format_study_row(row)
    assert "ALL" in s and "n=5" in s and "20%" in s and "-0.40" in s and "-3.2" in s


def test_format_static_row_readable():
    from scripts.analyze import format_static_row
    # schema produced by pavilos.backtest.static_study.summarize_static
    row = {"bucket": "[0.75,1.00)", "n": 18, "n_decided": 15, "n_undecided": 3,
           "bounce_rate": 0.6, "mean_fwd_return_bps": 9.4, "mean_mfe_r": 1.5,
           "mean_mae_r": -0.5, "expectancy_r": 0.85}
    s = format_static_row(row)
    assert "[0.75,1.00)" in s
    assert "n=18" in s
    assert "und=3" in s        # undecided count surfaced (excluded from exp_R)
    assert "60%" in s          # bounce_rate as a percentage
    assert "+0.85" in s        # expectancy_r in R, signed
    assert "+9.4" in s         # mean forward return in bps, signed


def test_format_static_row_baseline_negative_no_undecided():
    from scripts.analyze import format_static_row
    row = {"bucket": "ALL", "n": 6, "n_decided": 6, "n_undecided": 0,
           "bounce_rate": 0.166, "mean_fwd_return_bps": -2.7, "mean_mfe_r": 0.4,
           "mean_mae_r": -1.0, "expectancy_r": -0.5}
    s = format_static_row(row)
    assert "ALL" in s and "n=6" in s and "17%" in s and "-0.50" in s and "-2.7" in s
    assert "und=" not in s     # no undecided -> no (und=N) clutter
