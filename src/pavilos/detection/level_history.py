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

``touches(level, now)`` counts the DISTINCT PAST episodes whose price is within
``band_bps`` of ``level`` and which **ended before** ``now``. It is strictly
causal: observations with ``ts >= now`` are ignored (no look-ahead). So at any
time T it reports only how many times this level was respected in the past —
data <= T only.
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
        """Count DISTINCT past episodes within ``band_bps`` of ``level`` that
        ended before ``now``. Causal: ignores observations with ``ts >= now``.

        An episode is a gap-separated run (> ``episode_gap_s``) of band-matched
        touches. Only episodes whose last touch is strictly before ``now`` count.
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
        # Group band-matched touches into gap-separated episodes.
        episodes: list[tuple[float, float]] = []  # (first_ts, last_ts)
        ep_start = ep_end = matched[0]
        for ts in matched[1:]:
            if ts - ep_end > self.episode_gap_s:
                episodes.append((ep_start, ep_end))
                ep_start = ts
            ep_end = ts
        episodes.append((ep_start, ep_end))
        # Count distinct episodes that ended before `now`. All matched touches
        # are already < now, so every grouped episode qualifies.
        return sum(1 for _start, end in episodes if end < now)
