# src/pavilos/detection/detector.py
"""Detector: combined depth snapshot -> ranked support/resistance zones."""
from __future__ import annotations

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.detection.walls import detect_walls
from pavilos.detection.clusters import cluster_walls
from pavilos.detection.lifecycle import ZoneTracker, TrackedZone
from pavilos.detection.confidence import score_zone


class Detector:
    """Stateful across snapshots only via two ZoneTrackers (bids/asks)."""

    def __init__(
        self,
        *,
        size_multiple: float,
        min_size: float,
        max_gap_bps: float,
        max_zone_width_bps: float,
        match_overlap_bps: float,
        grace_s: float,
        window_bps: float,
        persistence_target_s: float,
        venues_target: float,
        strength_target: float,
    ) -> None:
        # Fail loud on misconfiguration: a non-positive target/threshold would
        # silently over- or under-score zones.
        for name, val in (("size_multiple", size_multiple), ("max_gap_bps", max_gap_bps),
                          ("max_zone_width_bps", max_zone_width_bps), ("match_overlap_bps", match_overlap_bps),
                          ("window_bps", window_bps), ("persistence_target_s", persistence_target_s),
                          ("venues_target", venues_target), ("strength_target", strength_target)):
            if not (val > 0):
                raise ValueError(f"Detector: {name} must be positive, got {val}")
        for name, val in (("min_size", min_size), ("grace_s", grace_s)):
            if val < 0:
                raise ValueError(f"Detector: {name} must be >= 0, got {val}")
        self._p = dict(size_multiple=size_multiple, min_size=min_size, max_gap_bps=max_gap_bps,
                       max_zone_width_bps=max_zone_width_bps, window_bps=window_bps,
                       persistence_target_s=persistence_target_s, venues_target=venues_target,
                       strength_target=strength_target)
        self._support_tracker = ZoneTracker(match_overlap_bps=match_overlap_bps, grace_s=grace_s)
        self._resist_tracker = ZoneTracker(match_overlap_bps=match_overlap_bps, grace_s=grace_s)

    def update(self, snapshot: CombinedDepthSnapshot) -> DepthAnalysis:
        mid = snapshot.mid
        supports = self._side(snapshot.bids, mid, snapshot.ts, Side.SUPPORT, self._support_tracker)
        resistances = self._side(snapshot.asks, mid, snapshot.ts, Side.RESISTANCE, self._resist_tracker)
        return DepthAnalysis(ts=snapshot.ts, mid=mid, supports=supports, resistances=resistances)

    def _side(self, bins, mid, ts, side, tracker) -> tuple[Zone, ...]:
        walls = detect_walls(bins, size_multiple=self._p["size_multiple"], min_size=self._p["min_size"])
        raw = cluster_walls(walls, mid=mid, max_gap_bps=self._p["max_gap_bps"],
                            max_zone_width_bps=self._p["max_zone_width_bps"])
        tracked = tracker.update(raw, mid=mid, ts=ts)
        # Pulled (vanished) zones are NOT current supports/resistances — the
        # tracker still surfaces them internally (anti-spoof), but they must not
        # pollute the ranked current-zone lists.
        zones = [self._to_zone(t, mid, side) for t in tracked if not t.pulled]
        zones.sort(key=lambda z: z.confidence, reverse=True)
        return tuple(zones)

    def _to_zone(self, t: TrackedZone, mid: float, side: Side) -> Zone:
        conf = score_zone(t, mid=mid, window_bps=self._p["window_bps"],
                          persistence_target_s=self._p["persistence_target_s"],
                          venues_target=self._p["venues_target"], strength_target=self._p["strength_target"])
        z = t.zone
        return Zone(side=side, price=z.price, low=z.low, high=z.high, strength=z.strength,
                    venues=z.venues, persistence_s=t.persistence_s, pulled=t.pulled, confidence=conf)
