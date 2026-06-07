# src/pavilos/detection/lifecycle.py
"""Track zone identity across snapshots: persistence + pulled (spoof) detection."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.detection.clusters import RawZone


@dataclass(slots=True, frozen=True)
class TrackedZone:
    """A RawZone enriched with lifecycle: how long it has existed and whether it
    was pulled (vanished without price ever entering its range)."""

    zone: RawZone
    first_seen: float
    persistence_s: float
    pulled: bool


class _Live:
    __slots__ = ("zone", "first_seen", "reached")

    def __init__(self, zone: RawZone, first_seen: float) -> None:
        self.zone = zone
        self.first_seen = first_seen
        self.reached = False  # has price ever entered this zone's range?


class ZoneTracker:
    """Matches incoming zones to live ones by price-range overlap (within
    ``match_overlap_bps`` of mid). Accumulates persistence; flags a vanished zone
    as ``pulled`` iff price never reached it while it was alive."""

    def __init__(self, *, match_overlap_bps: float = 10.0) -> None:
        self._match_bps = match_overlap_bps
        self._live: list[_Live] = []

    def update(self, raw_zones, mid: float, ts: float) -> list[TrackedZone]:
        tol = mid * self._match_bps / 1e4
        live = self._live
        matched: list[bool] = [False] * len(live)
        out: list[TrackedZone] = []
        new_live: list[_Live] = []

        for rz in raw_zones:
            idx = _best_match(rz, live, matched, tol)
            if idx is not None:
                matched[idx] = True
                cur = live[idx]
                cur.zone = rz
                if rz.low <= mid <= rz.high:
                    cur.reached = True
                out.append(TrackedZone(rz, cur.first_seen, ts - cur.first_seen, pulled=False))
                new_live.append(cur)
            else:
                fresh = _Live(rz, ts)
                if rz.low <= mid <= rz.high:
                    fresh.reached = True
                out.append(TrackedZone(rz, ts, 0.0, pulled=False))
                new_live.append(fresh)

        # zones that vanished this round: pulled iff never reached by price
        for was_matched, cur in zip(matched, live):
            if not was_matched and not cur.reached:
                out.append(TrackedZone(cur.zone, cur.first_seen, ts - cur.first_seen, pulled=True))

        self._live = new_live
        return out


def _best_match(rz: RawZone, live: list[_Live], matched: list[bool], tol: float) -> int | None:
    for i, cur in enumerate(live):
        if matched[i]:
            continue
        # overlap if ranges touch within tolerance
        if rz.low - tol <= cur.zone.high and rz.high + tol >= cur.zone.low:
            return i
    return None
