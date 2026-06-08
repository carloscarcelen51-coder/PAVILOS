# tests/unit/test_confluence_study.py
"""Forward-return validation study (M13 Task 3).

Synthetic snapshots (no lake): a strong multi-venue support sits just below
price; in the "bounce" series price then RISES; in the "fall" series price
breaks straight down through the level. We assert the study emits exactly ONE
observation per cluster EPISODE (not one per snapshot), measures MFE/MAE in
R-multiples, flags ``bounced`` causally forward-only, and that
``summarize_study`` buckets by confluence score with a baseline row +
finite expectancy_r.
"""
from math import isfinite

from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.confluence import ConfluenceConfig
from pavilos.backtest.confluence_study import (
    study_observations,
    summarize_study,
    StudyConfig,
)


def _bin(price, size, venues=("k", "b", "o")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b", "o"), venues_total=3)


# A permissive detector so the synthetic wall surfaces as a tradeable cluster.
def _runtime():
    return RuntimeConfig(entry_threshold=0.0, min_persistence_s=0.0, venues_target=2.0,
                         strength_target=5.0, persistence_target_s=1.0,
                         det_window_bps=500.0, atr_window=5)


def _study_cfg():
    return StudyConfig(
        confluence=ConfluenceConfig(confluence_band_bps=20.0, venues_target=2.0,
                                    threshold=0.0, min_venues=1, min_persistence_s=0.0),
        horizon_s=10.0, target_r=2.0, stop_offset_bps=5.0, atr_stop_mult=1.0,
        entry_zone_bps=60.0, episode_gap_s=5.0, buckets=(0.0, 0.5, 1.0))


# A support wall just below 99.5 (~50bps), strong & multi-venue.
_BIDS = [_bin(99.5, 1.0), _bin(99.0, 30.0), _bin(98.5, 1.0)]
_ASKS = [_bin(100.5, 1.0)]


def _bounce_snaps():
    """A persisting support cluster across 4 snaps, then price RISES sharply."""
    snaps = [_snap(float(i), 99.5, _BIDS, _ASKS) for i in range(4)]      # episode persists
    snaps += [_snap(4.0, 100.5, _BIDS, _ASKS), _snap(5.0, 102.0, _BIDS, _ASKS),
              _snap(6.0, 104.0, _BIDS, _ASKS)]                          # bounce up
    return snaps


def _fall_snaps():
    """A persisting support cluster, then price BREAKS straight down through it."""
    snaps = [_snap(float(i), 99.5, _BIDS, _ASKS) for i in range(4)]
    snaps += [_snap(4.0, 98.0, _BIDS, _ASKS), _snap(5.0, 96.0, _BIDS, _ASKS),
              _snap(6.0, 94.0, _BIDS, _ASKS)]                           # fall through
    return snaps


def test_one_observation_per_episode_not_per_snapshot():
    snaps = _bounce_snaps()
    obs = study_observations(snaps, _study_cfg(), runtime=_runtime())
    # The cluster persists across many snapshots but is a SINGLE episode -> 1 obs.
    assert len(obs) == 1
    o = obs[0]
    assert o.confluence_score > 0.0
    assert o.bounced in (True, False)
    assert isfinite(o.mfe_r) and isfinite(o.mae_r) and isfinite(o.fwd_return_bps)


def test_bounce_path_is_positive_and_bounced():
    obs = study_observations(_bounce_snaps(), _study_cfg(), runtime=_runtime())
    assert len(obs) == 1
    o = obs[0]
    # price rose far above entry -> MFE positive, fwd return positive, target hit
    assert o.mfe_r > 0.0
    assert o.fwd_return_bps > 0.0
    assert o.bounced is True


def test_fall_path_is_negative_and_not_bounced():
    obs = study_observations(_fall_snaps(), _study_cfg(), runtime=_runtime())
    assert len(obs) == 1
    o = obs[0]
    # price fell through the stop -> MAE <= -1R, fwd return negative, no bounce
    assert o.mae_r <= -1.0
    assert o.fwd_return_bps < 0.0
    assert o.bounced is False


def test_two_episodes_when_cluster_lapses_and_reforms():
    """A cluster, a gap > episode_gap_s with NO cluster, then it reforms -> 2 obs."""
    cfg = _study_cfg()
    a = [_snap(float(i), 99.5, _BIDS, _ASKS) for i in range(3)]           # episode 1
    flat = [DepthBin(99.0, 1.0, {"k": 1.0})]                              # no wall -> no cluster
    gap = [_snap(3.0 + j, 99.5, [_bin(99.5, 1.0)] + flat, _ASKS) for j in range(8)]  # > gap, no cluster
    b = [_snap(20.0 + i, 99.5, _BIDS, _ASKS) for i in range(3)]           # episode 2 reforms
    fwd = [_snap(30.0, 100.5, _BIDS, _ASKS)]
    obs = study_observations(a + gap + b + fwd, cfg, runtime=_runtime())
    assert len(obs) == 2


def test_summarize_buckets_by_score_with_baseline_and_expectancy():
    obs = study_observations(_bounce_snaps(), _study_cfg(), runtime=_runtime())
    rows = summarize_study(obs, buckets=(0.0, 0.5, 1.0))
    assert rows, "summary should not be empty"
    # baseline row aggregates ALL observations
    baseline = [r for r in rows if r.get("bucket") == "ALL"]
    assert len(baseline) == 1
    assert baseline[0]["n"] == len(obs)
    # every row has the documented fields and finite expectancy
    for r in rows:
        for k in ("n", "bounce_rate", "mean_fwd_return_bps", "mean_mfe_r",
                  "mean_mae_r", "expectancy_r"):
            assert k in r
        assert isfinite(r["expectancy_r"])
    # at least one non-baseline bucket holds the single observation
    bucketed = [r for r in rows if r.get("bucket") != "ALL" and r["n"] >= 1]
    assert bucketed


def test_empty_snapshots_yield_no_observations():
    assert study_observations([], _study_cfg(), runtime=_runtime()) == []
    rows = summarize_study([], buckets=(0.0, 0.5, 1.0))
    # baseline row still present, with n == 0 and finite (zeroed) expectancy
    baseline = [r for r in rows if r.get("bucket") == "ALL"]
    assert len(baseline) == 1 and baseline[0]["n"] == 0
    assert isfinite(baseline[0]["expectancy_r"])


def test_same_wall_with_price_jitter_is_one_episode():
    """CRITICAL (autocorrelation guard): the SAME persisting wall whose
    representative price wobbles a couple of bps snapshot-to-snapshot must dedup
    to ONE observation, not one per snapshot. Single-bin walls yield zero-width
    cluster bands, so a strict band-overlap test would open a new episode every
    snapshot and inflate N by orders of magnitude on real (jittery) data."""
    cfg = _study_cfg()
    asks = [_bin(100.5, 1.0)]
    snaps = []
    for i in range(40):
        jitter = 0.02 * (1 if i % 2 == 0 else -1)        # +/- ~2 bps wobble
        bids = [_bin(99.5, 1.0), _bin(99.0 + jitter, 30.0), _bin(98.5, 1.0)]
        snaps.append(_snap(float(i), 99.5, bids, asks))
    obs = study_observations(snaps, cfg, runtime=_runtime())
    assert len(obs) == 1


def test_wall_jumping_to_a_far_level_opens_a_new_episode():
    """A wall that shifts to a genuinely different price (well beyond the band
    tolerance) within an unbroken persistence run must still open a NEW episode,
    so the jitter tolerance does not collapse distinct levels into one."""
    cfg = _study_cfg()
    near = [_snap(float(i), 99.5, _BIDS, _ASKS) for i in range(3)]        # level ~99.0
    far_bids = [_bin(96.5, 1.0), _bin(96.0, 30.0), _bin(95.5, 1.0)]
    far_asks = [_bin(97.5, 1.0)]
    far = [_snap(3.0 + i, 96.5, far_bids, far_asks) for i in range(3)]    # level ~96.0
    obs = study_observations(near + far, cfg, runtime=_runtime())
    assert len(obs) == 2


def test_undecided_episode_is_excluded_from_expectancy():
    """An episode whose forward window resolves neither +target_R nor -1R must
    be flagged ``decided=False`` and EXCLUDED from expectancy_r — not silently
    scored as a -1R stop-out. summarize_study reports the undecided count
    separately and never charges it as a loss."""
    cfg = _study_cfg()
    # Onset at 99.5, then ONE forward snap that rises +small (never +2R or -1R).
    snaps = [_snap(0.0, 99.5, _BIDS, _ASKS), _snap(1.0, 99.6, _BIDS, _ASKS)]
    obs = study_observations(snaps, cfg, runtime=_runtime())
    assert len(obs) == 1
    o = obs[0]
    assert o.bounced is False
    assert o.decided is False                       # neither target nor stop reached
    rows = summarize_study(obs, buckets=(0.0, 0.5, 1.0), target_r=cfg.target_r)
    baseline = [r for r in rows if r.get("bucket") == "ALL"][0]
    assert baseline["n_undecided"] == 1
    assert baseline["n_decided"] == 0
    # No decided obs -> expectancy is a finite zero, NOT -1R.
    assert baseline["expectancy_r"] == 0.0


def test_decided_bounce_drives_expectancy_not_undecided():
    """A clean bounce is decided and contributes +target_R to expectancy; the
    undecided count is zero on a fully-resolved episode."""
    cfg = _study_cfg()
    obs = study_observations(_bounce_snaps(), cfg, runtime=_runtime())
    assert len(obs) == 1 and obs[0].decided is True and obs[0].bounced is True
    rows = summarize_study(obs, buckets=(0.0, 0.5, 1.0), target_r=cfg.target_r)
    baseline = [r for r in rows if r.get("bucket") == "ALL"][0]
    assert baseline["n_decided"] == 1 and baseline["n_undecided"] == 0
    assert baseline["expectancy_r"] == cfg.target_r


def test_level_history_touches_recorded_on_reformed_episode():
    """LevelHistory (the third confluence factor) is INTEGRATED into the study:
    a support level touched in an earlier episode, lapsed, then reformed records
    a causal ``n_touches >= 1`` on the second episode's observation, while the
    first (no prior history) records 0. summarize_study breaks expectancy down
    by touch presence so the study can show whether touches add lift."""
    cfg = _study_cfg()
    a = [_snap(float(i), 99.5, _BIDS, _ASKS) for i in range(3)]            # episode 1
    flat = [DepthBin(99.0, 1.0, {"k": 1.0})]
    gap = [_snap(3.0 + j, 99.5, [_bin(99.5, 1.0)] + flat, _ASKS) for j in range(8)]
    b = [_snap(20.0 + i, 99.5, _BIDS, _ASKS) for i in range(3)]            # episode 2 reforms
    fwd = [_snap(30.0, 100.5, _BIDS, _ASKS)]
    obs = study_observations(a + gap + b + fwd, cfg, runtime=_runtime())
    assert len(obs) == 2
    assert obs[0].n_touches == 0                     # first episode has no prior history
    assert obs[1].n_touches >= 1                     # second sees the prior episode
    rows = summarize_study(obs, buckets=(0.0, 0.5, 1.0), target_r=cfg.target_r)
    baseline = [r for r in rows if r.get("bucket") == "ALL"][0]
    # touch-presence breakdown exists and is finite.
    assert "expectancy_r_with_touches" in baseline
    assert "expectancy_r_no_touches" in baseline
    assert isfinite(baseline["expectancy_r_with_touches"])
    assert isfinite(baseline["expectancy_r_no_touches"])
