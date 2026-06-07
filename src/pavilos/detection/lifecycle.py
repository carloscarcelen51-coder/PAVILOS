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
    __slots__ = ("zone", "first_seen", "last_seen", "reached")

    def __init__(self, zone: RawZone, ts: float) -> None:
        self.zone = zone
        self.first_seen = ts
        self.last_seen = ts
        self.reached = False  # has price ever entered this zone's range?


class ZoneTracker:
    """Matches incoming zones to live ones by closest price-range overlap (within
    ``match_overlap_bps`` of mid). Accumulates persistence across matches. A zone
    missing this round survives up to ``grace_s`` (dormant, re-matchable so a
    one-tick flicker doesn't reset persistence); past the grace it is finalized
    and flagged ``pulled`` iff price never reached it AND no current zone overlaps
    it (the latter means it merged, not spoofed)."""

    def __init__(self, *, match_overlap_bps: float = 10.0, grace_s: float = 0.0) -> None:
        self._match_bps = match_overlap_bps
        self._grace_s = grace_s
        self._live: list[_Live] = []

    def update(self, raw_zones, mid: float, ts: float) -> list[TrackedZone]:
        tol = mid * self._match_bps / 1e4
        live = self._live
        matched: list[bool] = [False] * len(live)
        out: list[TrackedZone] = []
        kept: list[_Live] = []

        for rz in raw_zones:
            idx = _best_match(rz, live, matched, tol)
            if idx is not None:
                matched[idx] = True
                cur = live[idx]
                cur.zone = rz
                cur.last_seen = ts
                if rz.low <= mid <= rz.high:
                    cur.reached = True
                out.append(TrackedZone(rz, cur.first_seen, max(0.0, ts - cur.first_seen), pulled=False))
                kept.append(cur)
            else:
                fresh = _Live(rz, ts)
                if rz.low <= mid <= rz.high:
                    fresh.reached = True
                out.append(TrackedZone(rz, ts, 0.0, pulled=False))
                kept.append(fresh)

        for was_matched, cur in zip(matched, live):
            if was_matched:
                continue
            if ts - cur.last_seen <= self._grace_s:
                kept.append(cur)  # dormant within grace: keep (re-matchable), not emitted
            elif not cur.reached and not _overlaps_any(cur.zone, raw_zones, tol):
                # vanished, never reached, not absorbed by a current zone -> pulled (once)
                out.append(TrackedZone(cur.zone, cur.first_seen, max(0.0, ts - cur.first_seen), pulled=True))
            # else: consumed (reached) or merged -> dropped silently

        self._live = kept
        return out


def _best_match(rz: RawZone, live: list[_Live], matched: list[bool], tol: float) -> int | None:
    rz_center = (rz.low + rz.high) / 2.0
    best_i: int | None = None
    best_d: float | None = None
    for i, cur in enumerate(live):
        if matched[i]:
            continue
        if rz.low - tol <= cur.zone.high and rz.high + tol >= cur.zone.low:  # ranges overlap
            d = abs(rz_center - (cur.zone.low + cur.zone.high) / 2.0)
            if best_d is None or d < best_d:
                best_d = d
                best_i = i
    return best_i


def _overlaps_any(zone: RawZone, raw_zones, tol: float) -> bool:
    for rz in raw_zones:
        if zone.low - tol <= rz.high and zone.high + tol >= rz.low:
            return True
    return False
