# src/pavilos/detection/level_history.py
"""LevelHistory: a causal memory of past support touches.

This is the (c) *historical-touches* confluence factor, kept OUT of the pure
``analyze_confluence`` core (which stays stateless) and reported separately by
the study so we can see whether it adds predictive power.

Semantics
---------
``observe(price_level, ts)`` records that a support was touched at ``price_level``
at time ``ts``. A run of touches at ~the same price band, separated from the
next run by more than ``episode_gap_s``, is one *episode*.

``touches(level, now)`` counts the DISTINCT past-or-ongoing episodes whose price
is within ``band_bps`` of ``level`` and whose **last touch is strictly before**
``now``. It is strictly causal: observations with ``ts >= now`` are ignored (no
look-ahead). So at any time T it reports only how many times this level was
respected up to T — data < T only. (An episode still being touched right up to
``now`` is counted; "ongoing" here just means its last touch is < ``now``.)
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class LevelHistory:
    """Causal past-support memory. See module docstring for semantics."""

    band_bps: float
    episode_gap_s: float
    _obs: list[tuple[float, float]] = field(default_factory=list)  # (ts, price_level)

    def observe(self, price_level: float, ts: float) -> None:
        """Record a support touch at ``price_level`` at time ``ts``."""
        self._obs.append((ts, price_level))

    def touches(self, level: float, now: float) -> int:
        """Count DISTINCT past-or-ongoing episodes within ``band_bps`` of
        ``level`` whose last touch is strictly before ``now``. Causal: ignores
        observations with ``ts >= now``.

        An episode is a gap-separated run (> ``episode_gap_s``) of band-matched
        touches. The ``ts < now`` prefilter already guarantees every grouped
        episode's last touch is < ``now``, so the episode count IS the answer.
        """
        band = level * self.band_bps / 1e4
        # Causal + band filter, ordered by time. Each observation is "past"
        # (ts < now) and within the price band of the queried level.
        matched = sorted(
            ts for ts, price in self._obs
            if ts < now and abs(price - level) <= band
        )
        if not matched:
            return 0
        # Count gap-separated episodes: start at 1, increment on each gap.
        episodes = 1
        prev = matched[0]
        for ts in matched[1:]:
            if ts - prev > self.episode_gap_s:
                episodes += 1
            prev = ts
        return episodes
