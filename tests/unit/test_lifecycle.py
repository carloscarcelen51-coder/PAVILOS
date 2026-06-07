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


def test_best_match_avoids_identity_swap_and_is_order_independent():
    # Two close zones of DIFFERENT ages; incoming fed in swapped order must still
    # match by closest center (no identity swap, no order dependence).
    t = ZoneTracker(match_overlap_bps=10.0)  # mid 100 -> tol 0.1
    t.update([_z(100.20, 100.24)], mid=100.0, ts=0.0)                       # A born (older)
    t.update([_z(100.20, 100.24), _z(100.30, 100.34)], mid=100.0, ts=10.0)  # B born (younger)
    out = t.update([_z(100.30, 100.34), _z(100.20, 100.24)], mid=100.0, ts=20.0)  # swapped order
    by_low = {round(z.zone.low, 2): z.persistence_s for z in out}
    assert by_low[100.20] == 20.0   # A keeps its age despite being listed second
    assert by_low[100.30] == 10.0   # B keeps its age


def test_flicker_within_grace_preserves_persistence_and_no_pulled():
    t = ZoneTracker(match_overlap_bps=10.0, grace_s=5.0)
    t.update([_z(99.5, 100.5)], mid=101.0, ts=0.0)
    gone = t.update([], mid=101.0, ts=2.0)            # missing 1 tick, within grace
    assert gone == []                                  # dormant: not emitted, NOT pulled
    back = t.update([_z(99.5, 100.5)], mid=101.0, ts=4.0)  # returns within grace
    assert len(back) == 1 and back[0].pulled is False
    assert back[0].persistence_s == 4.0                # first_seen preserved (4.0 - 0.0)


def test_merged_zone_is_not_flagged_pulled():
    t = ZoneTracker(match_overlap_bps=10.0)
    t.update([_z(99.90, 99.94), _z(100.00, 100.04)], mid=101.0, ts=1.0)  # two adjacent supports
    # next tick: a single wide zone spans both -> the unmatched live one merged, not pulled
    out = t.update([_z(99.90, 100.04)], mid=101.0, ts=2.0)
    assert all(not z.pulled for z in out)
