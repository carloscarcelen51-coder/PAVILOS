# tests/unit/test_backtest_forward.py
"""Shared forward-measurement helpers (M14 close-out: de-duplicate R-math).

The M13 ``confluence_study`` and the M14 ``static_study`` both need the SAME
forward MFE/MAE/decided/bounced R-multiple measurement and the same decided-only
expectancy + bucket-label aggregation. Before this module those bodies were
copy-pasted into both files, so a future correction had to be applied twice or
the two studies would silently diverge. This test pins the single source of
truth: both studies import the helpers from :mod:`pavilos.backtest.forward`, and
the helper itself behaves per the M13 R-math contract.
"""
from dataclasses import dataclass

from pavilos.backtest.forward import bucket_label, expectancy, measure_forward


@dataclass(frozen=True)
class _Snap:
    ts: float
    mid: float


@dataclass(frozen=True)
class _O:
    outcome_r: float | None


def test_bucket_label_formats_two_decimals():
    assert bucket_label(0.0, 0.25) == "[0.00,0.25)"


def test_expectancy_is_decided_only_mean():
    # two decided (+2.0, -1.0), one undecided (None) -> mean over decided = 0.5
    members = [_O(2.0), _O(-1.0), _O(None)]
    assert expectancy(members) == 0.5
    # no decided members -> finite zero, not a loss
    assert expectancy([_O(None), _O(None)]) == 0.0
    assert expectancy([]) == 0.0


def test_measure_forward_is_config_agnostic_and_forward_only():
    # bounce: target_r=2.0, risk=10.0 -> +20 reached before -10
    seq = [_Snap(0.0, 100.0), _Snap(1.0, 105.0), _Snap(2.0, 121.0)]
    mfe_r, mae_r, fwd_bps, bounced, decided, n = measure_forward(
        seq, 0, entry=100.0, risk=10.0, horizon_s=10.0, target_r=2.0)
    assert bounced is True and decided is True
    assert mfe_r >= 2.0 and n == 2

    # gap straight through the stop: mae_r <= -1, not bounced
    seq2 = [_Snap(0.0, 100.0), _Snap(1.0, 80.0)]
    mfe_r, mae_r, fwd_bps, bounced, decided, n = measure_forward(
        seq2, 0, entry=100.0, risk=10.0, horizon_s=10.0, target_r=2.0)
    assert bounced is False and decided is True and mae_r <= -1.0

    # empty forward window -> undecided, not a stop-out
    mfe_r, mae_r, fwd_bps, bounced, decided, n = measure_forward(
        [_Snap(0.0, 100.0)], 0, entry=100.0, risk=10.0, horizon_s=10.0, target_r=2.0)
    assert decided is False and bounced is False and n == 0


def test_both_studies_share_the_single_forward_helper():
    """No duplicate R-math: both studies reference the SAME function objects from
    the shared module, not private copies."""
    import pavilos.backtest.confluence_study as cs
    import pavilos.backtest.static_study as ss

    assert cs.measure_forward is measure_forward
    assert ss.measure_forward is measure_forward
    assert cs.expectancy is expectancy
    assert ss.expectancy is expectancy
    assert cs.bucket_label is bucket_label
    assert ss.bucket_label is bucket_label
