# tests/unit/test_static_levels.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.static_levels import StaticLevelTracker, StaticLevelConfig


def _bin(price, size, venues):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _fill(prices):
    # small (size-1) baseline bins so the side's median stays low and a real wall
    # (size 50) clears detect_walls' size_multiple x median threshold.
    return [DepthBin(price=p, size=1.0, composition={"k": 1.0}) for p in prices]


def _snap(ts, mid, bids, asks=()):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=14)


def _cfg(**o):
    kw = dict(level_bucket_usd=25.0, size_multiple=3.0, stale_s=30.0, min_venues=6,
              level_threshold=0.0, min_away_bps=25.0, max_reach_bps=400.0,
              venues_target=8.0, duration_target_s=10.0)
    kw.update(o)
    return StaticLevelConfig(**kw)


def test_accrues_presence_at_a_fixed_level_and_unions_venues():
    trk = StaticLevelTracker(_cfg(min_venues=1))
    big = ("k", "b", "o", "x", "g", "m", "h")    # 7 venues
    for ts in range(0, 10):                       # a wall sits at 62700 for 10 ticks while mid=63000
        trk.update(_snap(float(ts), 63000.0,
                         [_bin(62700.0, 50.0, big)] + _fill([62680.0, 62660.0, 62640.0])))
    sup = trk.active_supports(mid=63000.0, now=9.0)
    assert any(abs(s.price - 62700.0) <= 25.0 for s in sup)
    s = next(s for s in sup if abs(s.price - 62700.0) <= 25.0)
    assert s.n_venues == 7 and s.presence >= 10
    # this level is ~48bps below mid -> max_away_bps >= min_away (a real static level)
    assert s.max_away_bps >= 25.0


def test_near_touch_excluded_by_min_away():
    trk = StaticLevelTracker(_cfg(min_venues=1))
    big = ("k", "b", "o", "x", "g", "m", "h")
    # a big wall always ~2bps below a DRIFTING mid (the near-touch, trailing price)
    for i in range(20):
        mid = 63000.0 + i
        trk.update(_snap(float(i), mid,
                         [_bin(mid - 12.0, 50.0, big)] + _fill([mid - 200.0, mid - 220.0, mid - 240.0])))
    # the near-touch wall never got >= min_away_bps from mid -> not an active static support
    sup = trk.active_supports(mid=63019.0, now=19.0)
    assert all(s.max_away_bps < 25.0 for s in sup) or sup == []


def test_prunes_stale_levels():
    trk = StaticLevelTracker(_cfg(min_venues=1, stale_s=5.0))
    big = ("k", "b", "o", "x", "g", "m", "h")
    trk.update(_snap(0.0, 63000.0, [_bin(62700.0, 50.0, big)] + _fill([62680.0, 62660.0, 62640.0])))
    # advance far past stale_s with no wall at 62700
    trk.update(_snap(20.0, 63000.0, [_bin(62800.0, 50.0, big)] + _fill([62780.0, 62760.0, 62740.0])))
    sup = trk.active_supports(mid=63000.0, now=20.0)
    assert all(abs(s.price - 62700.0) > 25.0 for s in sup)   # 62700 pruned (stale)


def test_strength_rises_with_venues_and_duration():
    big = ("k", "b", "o", "x", "g", "m", "h", "i")
    weak_trk = StaticLevelTracker(_cfg(min_venues=1))
    weak_trk.update(_snap(0.0, 63000.0, [_bin(62700.0, 50.0, ("k",))] + _fill([62680.0, 62660.0, 62640.0])))
    strong_trk = StaticLevelTracker(_cfg(min_venues=1))
    for ts in range(0, 30):
        strong_trk.update(_snap(float(ts), 63000.0,
                          [_bin(62700.0, 50.0, big)] + _fill([62680.0, 62660.0, 62640.0])))
    w = weak_trk.active_supports(63000.0, 0.0)
    s = strong_trk.active_supports(63000.0, 29.0)
    assert s and (not w or s[0].strength > w[0].strength)


def test_volatile_fixed_level_is_static_when_price_returns():
    # a FIXED wall at 62700; mid rises away to 63100 (>min_away) then falls back near it.
    trk = StaticLevelTracker(_cfg(min_venues=1))
    big = ("k", "b", "o", "x", "g", "m", "h")
    mids = [63000.0, 63050.0, 63100.0, 63050.0, 62760.0]
    for i, mid in enumerate(mids):
        trk.update(_snap(float(i), mid,
                         [_bin(62700.0, 50.0, big)] + _fill([62680.0, 62660.0, 62640.0])))
    sup = trk.active_supports(mid=62760.0, now=4.0)   # price has returned near the level
    s = next(s for s in sup if abs(s.price - 62700.0) <= 25.0)
    assert s.max_away_bps >= 25.0   # mid had been >=63100 (>63bps away) during its life
