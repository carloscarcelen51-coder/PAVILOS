# src/pavilos/detection/confidence.py
"""Confidence score (0..1) for a tracked zone. Pure."""
from __future__ import annotations

from pavilos.detection.lifecycle import TrackedZone


def _clamp01(x: float) -> float:
    # NaN-safe: ``not (x > 0.0)`` is True for NaN and for x <= 0 -> 0.0, so a
    # non-finite factor can never escape the [0,1] contract (it ranks last).
    return 0.0 if not (x > 0.0) else (1.0 if x > 1.0 else x)


def score_zone(
    tracked: TrackedZone,
    *,
    mid: float,
    window_bps: float,
    persistence_target_s: float,
    venues_target: float,
    strength_target: float,
) -> float:
    """Confidence = persistence x venues x strength x proximity, each clamped to
    [0,1]. A pulled (spoof-like) zone scores 0.0 regardless of the rest."""
    if tracked.pulled:
        return 0.0
    z = tracked.zone
    persistence = _clamp01(tracked.persistence_s / persistence_target_s) if persistence_target_s > 0 else 1.0
    venues = _clamp01(len(z.venues) / venues_target) if venues_target > 0 else 1.0
    strength = _clamp01(z.strength / strength_target) if strength_target > 0 else 1.0
    # proximity: 1.0 at mid, decaying to 0.0 at window_bps away
    half_window = mid * window_bps / 1e4
    distance = abs(z.price - mid)
    proximity = _clamp01(1.0 - distance / half_window) if half_window > 0 else 1.0
    return _clamp01(persistence * venues * strength * proximity)
