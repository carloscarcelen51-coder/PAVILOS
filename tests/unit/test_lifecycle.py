# tests/unit/test_lifecycle.py
from pavilos.detection.clusters import RawZone
from pavilos.detection.lifecycle import ZoneTracker, TrackedZone


def _z(low, high, strength=5.0, venues=("kraken",)):
    return RawZone(low=low, high=high, price=(low + high) / 2, strength=strength, venues=venues)


def test_persistence_accumulates_across_matched_snapshots():
    t = ZoneTracker(match_overlap_bps=10.0)
    out1 = t.update([_z(99.5, 100.5)], mid=101.0, ts=1.0)
    assert out1[0].persistence_s == 0.0 and out1[0].pulled is False
    out2 = t.update([_z(99.6, 100.6)], mid=101.0, ts=3.0)  # overlaps -> same zone
    assert len(out2) == 1
    assert out2[0].persistence_s == 2.0  # 3.0 - 1.0
    assert out2[0].pulled is False


def test_disappeared_zone_with_price_away_is_flagged_pulled():
    t = ZoneTracker(match_overlap_bps=10.0)
    t.update([_z(99.5, 100.5)], mid=101.0, ts=1.0)   # support well below mid 101
    # next snapshot: zone gone, price (mid) still above it -> pulled
    out = t.update([], mid=101.0, ts=2.0)
    pulled = [z for z in out if z.pulled]
    assert len(pulled) == 1 and pulled[0].zone.low == 99.5
    # it's reported once then forgotten
    out2 = t.update([], mid=101.0, ts=3.0)
    assert out2 == []


def test_disappeared_zone_after_price_reached_it_is_not_pulled():
    t = ZoneTracker(match_overlap_bps=10.0)
    t.update([_z(100.0, 100.6)], mid=101.0, ts=1.0)
    # price drops into the zone (mid now 100.3, inside [100.0,100.6]) then zone gone next tick
    t.update([_z(100.0, 100.6)], mid=100.3, ts=2.0)   # price reached it
    out = t.update([], mid=100.3, ts=3.0)             # now gone, but it WAS reached
    assert all(not z.pulled for z in out)             # consumed, not pulled -> no pulled flag
