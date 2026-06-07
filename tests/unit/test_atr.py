# tests/unit/test_atr.py
from pavilos.signals.atr import ATR


def test_atr_zero_until_two_ticks():
    a = ATR(window=3)
    assert a.value() == 0.0
    a.update(100.0)
    assert a.value() == 0.0


def test_atr_is_mean_abs_change_over_window():
    a = ATR(window=3)
    for p in (100.0, 101.0, 103.0, 106.0):   # diffs 1,2,3
        a.update(p)
    assert abs(a.value() - 2.0) < 1e-9        # mean(1,2,3)


def test_atr_window_drops_old_ticks():
    a = ATR(window=2)
    for p in (100.0, 101.0, 103.0, 106.0):    # last 2 diffs: 2,3
        a.update(p)
    assert abs(a.value() - 2.5) < 1e-9        # mean(2,3)


def test_atr_ignores_non_finite():
    a = ATR(window=3)
    for p in (100.0, float("nan"), 102.0):
        a.update(p)
    assert a.value() == 2.0                    # nan tick skipped -> diff(100,102)=2
