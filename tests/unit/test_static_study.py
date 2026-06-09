# tests/unit/test_static_study.py
"""Approach-episode forward-return study (M14 Task 2).

Synthetic snapshots (no lake): a FIXED-price multi-venue support wall sits at an
absolute price. Price starts well ABOVE it (so the level accrues
``max_away_bps`` >= ``min_away_bps`` — it is a genuine static level, not the
near-touch), then RETURNS down to within ``entry_zone_bps`` of it (the approach),
and either BOUNCES up or BREAKS through. We assert the study emits exactly ONE
observation per approach EPISODE (hysteresis: oscillating in/out within
``episode_gap_s`` does NOT double-count; leaving for > gap then re-approaching is
a NEW episode), measures MFE/MAE in R-multiples reusing M13's R-math, scores the
level strength causally at onset, and that ``summarize_static`` buckets by level
strength with a baseline row + finite expectancy_r. ``realized_vol_bps`` reports
the slice volatility for the verdict.
"""
from math import isfinite

from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.static_levels import StaticLevelConfig
from pavilos.backtest.static_study import (
    study_static_approaches,
    summarize_static,
    realized_vol_bps,
    StaticStudyConfig,
)


def _bin(price, size, venues=("k", "b", "o", "x", "g", "m", "h")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _fill(prices):
    # small (size-1) baseline bins so the side's median stays low and a real wall
    # (size 60) clears detect_walls' size_multiple x median threshold.
    return [DepthBin(price=p, size=1.0, composition={"k": 1.0}) for p in prices]


def _snap(ts, mid, bids, asks=()):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=14)


def _static_cfg(**o):
    kw = dict(level_bucket_usd=25.0, size_multiple=3.0, stale_s=60.0, min_venues=6,
              level_threshold=0.0, min_away_bps=25.0, max_reach_bps=400.0,
              venues_target=8.0, duration_target_s=10.0)
    kw.update(o)
    return StaticLevelConfig(**kw)


def _study_cfg(**o):
    kw = dict(static=_static_cfg(), horizon_s=10.0, target_r=2.0, stop_offset_bps=5.0,
              atr_stop_mult=0.0, entry_zone_bps=15.0, episode_gap_s=5.0,
              buckets=(0.0, 0.5, 1.0))
    kw.update(o)
    return StaticStudyConfig(**kw)


# A fixed wall at 63000; a thin near-best bid that always trails mid (filler).
_WALL = 63000.0


def _wall_bin():
    return _bin(_WALL, 60.0)


# baseline filler well BELOW the wall so the side median stays at 1.0 (wall clears
# 3x median) and the fillers never bucket near or above the wall.
_FILL = [62500.0, 62480.0, 62460.0]


def _book(mid):
    return [_bin(mid - 1.0, 1.0, ("k",)), _wall_bin()] + _fill(_FILL)


def _away_snap(ts, mid):
    """Price well ABOVE the wall (a thin trailing bid keeps mid defined)."""
    return _snap(ts, mid, _book(mid))


def _near_snap(ts, mid):
    """Price has RETURNED to within the zone of the wall."""
    return _snap(ts, mid, _book(mid))


def _approach_then_bounce():
    """Wall at 63000; mid sits ~80bps above (accrues max_away), then returns to
    ~10bps above the wall (the approach), then RISES sharply (a bounce)."""
    snaps = [_away_snap(float(i), 63500.0) for i in range(6)]            # far above -> max_away
    snaps += [_near_snap(6.0, 63060.0)]                                 # approach onset (~9.5bps)
    snaps += [_near_snap(7.0, 63300.0), _near_snap(8.0, 63800.0),
              _near_snap(9.0, 64300.0)]                                 # bounce up
    return snaps


def _approach_then_break():
    """Same approach, then price BREAKS straight DOWN through the level."""
    snaps = [_away_snap(float(i), 63500.0) for i in range(6)]
    snaps += [_near_snap(6.0, 63060.0)]                                 # approach onset
    snaps += [_near_snap(7.0, 62900.0), _near_snap(8.0, 62700.0),
              _near_snap(9.0, 62500.0)]                                 # break down
    return snaps


def test_one_observation_per_approach_episode():
    obs = study_static_approaches(_approach_then_bounce(), _study_cfg())
    assert len(obs) == 1
    o = obs[0]
    assert o.level_strength > 0.0
    assert o.n_venues >= 6
    assert o.bounced in (True, False)
    assert isfinite(o.mfe_r) and isfinite(o.mae_r)


def test_bounce_path_is_positive_and_bounced():
    obs = study_static_approaches(_approach_then_bounce(), _study_cfg())
    assert len(obs) == 1
    o = obs[0]
    assert o.mfe_r > 0.0
    assert o.bounced is True
    assert o.decided is True


def test_break_path_is_negative_and_not_bounced():
    obs = study_static_approaches(_approach_then_break(), _study_cfg())
    assert len(obs) == 1
    o = obs[0]
    assert o.mae_r <= -1.0
    assert o.bounced is False
    assert o.decided is True


def test_oscillation_within_gap_does_not_double_count():
    """Price enters the zone, briefly pops just OUT, then back IN, all within
    episode_gap_s -> hysteresis keeps it ONE episode (not three)."""
    cfg = _study_cfg()
    snaps = [_away_snap(float(i), 63500.0) for i in range(6)]
    # in / out / in / out / in -- all within 5s, brief pops just above the zone.
    snaps += [_near_snap(6.0, 63060.0),                                # in (onset)
              _near_snap(7.0, 63150.0),                                # out (~24bps, brief)
              _near_snap(8.0, 63060.0),                                # back in (within gap)
              _near_snap(9.0, 63150.0),                                # out again
              _near_snap(10.0, 63060.0)]                               # back in (within gap)
    obs = study_static_approaches(snaps, cfg)
    assert len(obs) == 1


def test_leaving_beyond_gap_then_reapproach_is_two_episodes():
    """Approach, then LEAVE the zone for longer than episode_gap_s, then
    re-approach -> TWO episodes."""
    cfg = _study_cfg(episode_gap_s=5.0)
    snaps = [_away_snap(float(i), 63500.0) for i in range(6)]
    snaps += [_near_snap(6.0, 63060.0)]                                # episode 1 onset
    # leave the zone well beyond the gap (8s far above)
    snaps += [_away_snap(7.0 + j, 63500.0) for j in range(8)]
    snaps += [_near_snap(20.0, 63060.0)]                               # episode 2 onset
    snaps += [_near_snap(21.0, 63800.0)]
    obs = study_static_approaches(snaps, cfg)
    assert len(obs) == 2


def test_near_touch_never_opens_an_episode():
    """A wall that always trails ~2bps below a drifting mid (the near-touch)
    never accrues max_away_bps >= min_away_bps, so NO approach episode opens —
    the headline M13 fix."""
    cfg = _study_cfg()
    big = ("k", "b", "o", "x", "g", "m", "h")
    snaps = []
    for i in range(30):
        mid = 63000.0 + i * 5.0
        # ~2bps trailing wall + low filler so it clears detect_walls but never
        # accrues max_away_bps >= min_away_bps.
        snaps.append(_snap(float(i), mid,
                           [_bin(mid - 12.0, 60.0, big)] + _fill([mid - 200.0, mid - 220.0, mid - 240.0])))
    obs = study_static_approaches(snaps, cfg)
    assert obs == []


def test_level_strength_is_causal_at_onset():
    """The level strength recorded on the obs uses only data <= onset: a wall
    seen at MANY venues for a LONG time before the approach scores higher than a
    barely-formed one."""
    cfg = _study_cfg()
    # Long, broad accrual before the approach.
    strong = [_away_snap(float(i), 63500.0) for i in range(20)]
    strong += [_near_snap(20.0, 63060.0), _near_snap(21.0, 63800.0)]
    obs_strong = study_static_approaches(strong, cfg)
    assert len(obs_strong) == 1
    # Minimal accrual: wall present only briefly before the approach.
    weak = [_away_snap(0.0, 63500.0), _near_snap(1.0, 63060.0), _near_snap(2.0, 63800.0)]
    obs_weak = study_static_approaches(weak, cfg)
    assert len(obs_weak) == 1
    assert obs_strong[0].level_strength > obs_weak[0].level_strength


def test_summarize_buckets_by_strength_with_baseline():
    obs = study_static_approaches(_approach_then_bounce(), _study_cfg())
    rows = summarize_static(obs, buckets=(0.0, 0.5, 1.0))
    assert rows, "summary should not be empty"
    baseline = [r for r in rows if r.get("bucket") == "ALL"]
    assert len(baseline) == 1
    assert baseline[0]["n"] == len(obs)
    for r in rows:
        for k in ("n", "bounce_rate", "mean_mfe_r", "mean_mae_r", "expectancy_r"):
            assert k in r
        assert isfinite(r["expectancy_r"])
    bucketed = [r for r in rows if r.get("bucket") != "ALL" and r["n"] >= 1]
    assert bucketed


def test_empty_snapshots_yield_no_observations():
    assert study_static_approaches([], _study_cfg()) == []
    rows = summarize_static([], buckets=(0.0, 0.5, 1.0))
    baseline = [r for r in rows if r.get("bucket") == "ALL"]
    assert len(baseline) == 1 and baseline[0]["n"] == 0
    assert isfinite(baseline[0]["expectancy_r"])


def test_realized_vol_bps_rises_with_movement():
    flat = [_snap(float(i), 63000.0, [_bin(62999.0, 1.0, ("k",))]) for i in range(10)]
    movy = [_snap(float(i), 63000.0 + (i % 2) * 100.0, [_bin(62999.0, 1.0, ("k",))])
            for i in range(10)]
    assert realized_vol_bps(flat) == 0.0
    assert realized_vol_bps(movy) > 0.0
    assert realized_vol_bps([]) == 0.0
