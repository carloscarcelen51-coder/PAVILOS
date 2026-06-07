# tests/unit/test_detector.py
import pytest

from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.models import Side
from pavilos.detection.detector import Detector


def _bin(price, size, venues=("kraken", "binance")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("kraken", "binance"), venues_total=2)


def _detector():
    return Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                    match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                    persistence_target_s=5.0, venues_target=2.0, strength_target=5.0)


def test_detects_support_and_resistance_walls():
    d = _detector()
    bids = [_bin(100.0, 1.0), _bin(99.5, 10.0), _bin(99.0, 1.0)]   # 99.5 is the support wall
    asks = [_bin(100.5, 1.0), _bin(101.0, 12.0), _bin(101.5, 1.0)]  # 101.0 is the resistance wall
    a = d.update(_snap(1.0, 100.25, bids, asks))
    assert len(a.supports) == 1 and a.supports[0].side is Side.SUPPORT
    assert abs(a.supports[0].price - 99.5) < 1e-9
    assert len(a.resistances) == 1 and a.resistances[0].side is Side.RESISTANCE
    assert abs(a.resistances[0].price - 101.0) < 1e-9


def test_persistence_raises_confidence_over_two_snapshots():
    d = _detector()
    bids = [_bin(100.0, 1.0), _bin(99.5, 10.0), _bin(99.0, 1.0)]
    asks = [_bin(100.5, 1.0)]
    a1 = d.update(_snap(1.0, 100.25, bids, asks))
    a2 = d.update(_snap(6.0, 100.25, bids, asks))   # +5s, same support persists
    assert a2.supports[0].confidence >= a1.supports[0].confidence
    assert a2.supports[0].persistence_s == 5.0


def test_supports_sorted_by_confidence_desc():
    d = _detector()
    # two support walls; the multi-venue, bigger one should rank first
    bids = [_bin(100.0, 12.0, ("kraken", "binance")), _bin(95.0, 6.0, ("kraken",)), _bin(99.0, 1.0)]
    asks = [_bin(101.0, 1.0)]
    a = d.update(_snap(1.0, 100.5, bids, asks))
    confs = [z.confidence for z in a.supports]
    assert confs == sorted(confs, reverse=True)


def test_invalid_params_raise():
    with pytest.raises(ValueError):
        Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                 match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                 persistence_target_s=-5.0, venues_target=2.0, strength_target=5.0)  # negative target


def test_pulled_zones_are_not_in_output():
    d = _detector()  # grace_s=0 -> immediate pulled
    bids = [_bin(100.0, 1.0), _bin(99.5, 10.0), _bin(99.0, 1.0)]  # 99.5 support wall
    asks = [_bin(100.5, 1.0)]
    a1 = d.update(_snap(1.0, 100.25, bids, asks))
    assert len(a1.supports) == 1 and abs(a1.supports[0].price - 99.5) < 1e-9
    # next snapshot: the 99.5 wall is gone (uniform bids); mid still above it -> pulled
    a2 = d.update(_snap(2.0, 100.25, [_bin(100.0, 1.0), _bin(99.0, 1.0)], asks))
    assert all(not z.pulled for z in a2.supports)                 # no pulled zone leaks out
    assert all(abs(z.price - 99.5) > 1e-6 for z in a2.supports)   # the vanished wall is gone
