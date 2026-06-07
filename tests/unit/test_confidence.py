# tests/unit/test_confidence.py
import pytest

from pavilos.detection.clusters import RawZone
from pavilos.detection.lifecycle import TrackedZone
from pavilos.detection.confidence import score_zone


def _tracked(persistence_s=10.0, venues=("kraken", "binance", "coinbase"), strength=10.0,
             low=99.5, high=100.5, pulled=False):
    rz = RawZone(low=low, high=high, price=(low + high) / 2, strength=strength, venues=venues)
    return TrackedZone(rz, first_seen=0.0, persistence_s=persistence_s, pulled=pulled)


_PARAMS = dict(window_bps=200.0, persistence_target_s=10.0, venues_target=3.0, strength_target=10.0)


def test_strong_zone_scores_high():
    # zone price (100.0) == mid -> proximity 1.0; at/above all targets -> high score
    s = score_zone(_tracked(), mid=100.0, **_PARAMS)
    assert 0.0 < s <= 1.0
    assert s > 0.8


def test_pulled_zone_scores_zero():
    assert score_zone(_tracked(pulled=True), mid=100.5, **_PARAMS) == 0.0


def test_confidence_in_unit_interval_and_monotone_in_persistence():
    low = score_zone(_tracked(persistence_s=1.0), mid=100.5, **_PARAMS)
    high = score_zone(_tracked(persistence_s=10.0), mid=100.5, **_PARAMS)
    assert 0.0 <= low <= high <= 1.0


def test_single_venue_scores_lower_than_multi_venue():
    one = score_zone(_tracked(venues=("kraken",)), mid=100.5, **_PARAMS)
    many = score_zone(_tracked(venues=("kraken", "binance", "coinbase")), mid=100.5, **_PARAMS)
    assert one < many
