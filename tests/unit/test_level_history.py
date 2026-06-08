from pavilos.detection.level_history import LevelHistory


def test_counts_only_past_distinct_episodes_within_band():
    h = LevelHistory(band_bps=20.0, episode_gap_s=60.0)
    # episode 1 at ~63000, ts 0..10
    for ts in (0.0, 5.0, 10.0):
        h.observe(price_level=63000.0, ts=ts)
    # touches at 63000 BEFORE a new episode: counts past distinct episodes (=1 so far, the current is ongoing)
    assert h.touches(63000.0, now=10.0) >= 0
    # a gap > episode_gap_s, then episode 2 -> now touches() sees 1 prior distinct episode
    h.observe(price_level=63010.0, ts=200.0)
    assert h.touches(63010.0, now=200.0) == 1          # one prior episode (~63000) within band
    # a far level has no history
    assert h.touches(61000.0, now=200.0) == 0


def test_touches_is_causal_ignores_future():
    h = LevelHistory(band_bps=20.0, episode_gap_s=60.0)
    h.observe(63000.0, ts=100.0)
    assert h.touches(63000.0, now=50.0) == 0           # nothing observed before t=50


def test_counts_ongoing_episode_whose_last_touch_is_before_now():
    """An episode still being touched right up to ``now`` counts as one
    past-or-ongoing distinct episode (its last touch is strictly < now). This
    pins the documented 'past-or-ongoing, last touch strictly before now'
    semantics so the count stays causal without dropping the current run."""
    h = LevelHistory(band_bps=20.0, episode_gap_s=60.0)
    for ts in (0.0, 5.0, 10.0):
        h.observe(63000.0, ts=ts)
    # querying just after the last touch sees the one (ongoing) episode
    assert h.touches(63000.0, now=10.0001) == 1
    # two gap-separated episodes -> counted as two distinct episodes
    h.observe(63000.0, ts=200.0)
    h.observe(63000.0, ts=205.0)
    assert h.touches(63000.0, now=210.0) == 2
