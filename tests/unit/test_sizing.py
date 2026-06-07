# tests/unit/test_sizing.py
from pavilos.signals.sizing import position_size


def test_size_risks_fixed_fraction_at_stop():
    # equity 10k, risk 1% = $100 risk; entry 100, stop 98 -> $2/unit -> 50 units
    s = position_size(10_000.0, entry=100.0, stop=98.0, risk_pct=0.01, max_leverage=100.0)
    assert abs(s - 50.0) < 1e-9


def test_size_capped_by_leverage():
    # raw size would be huge (tiny stop distance), leverage caps it
    s = position_size(10_000.0, entry=100.0, stop=99.99, risk_pct=0.01, max_leverage=2.0)
    # leverage cap = max_leverage*equity/entry = 2*10000/100 = 200 units
    assert abs(s - 200.0) < 1e-9


def test_zero_or_inverted_distance_returns_zero():
    assert position_size(10_000.0, entry=100.0, stop=100.0, risk_pct=0.01, max_leverage=10.0) == 0.0
    assert position_size(10_000.0, entry=100.0, stop=float("nan"), risk_pct=0.01, max_leverage=10.0) == 0.0


def test_overflow_returns_zero():
    import sys
    huge = sys.float_info.max
    assert position_size(huge, entry=0.5, stop=0.4, risk_pct=10.0, max_leverage=2.0) == 0.0
