# src/pavilos/detection/clusters.py
"""Group adjacent wall bins into zones."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.detection.walls import WallBin


@dataclass(slots=True, frozen=True)
class RawZone:
    """A clustered zone before lifecycle/confidence: price bounds, strength-
    weighted representative price, total strength, and contributing venues."""

    low: float
    high: float
    price: float
    strength: float
    venues: tuple[str, ...]


def cluster_walls(walls, *, mid: float, max_gap_bps: float, max_zone_width_bps: float) -> list[RawZone]:
    """Cluster walls whose adjacent price gap is within ``max_gap_bps`` AND whose
    total span (from the group's highest price) stays within ``max_zone_width_bps``
    (both relative to ``mid``) into single zones. The width cap prevents a long
    staircase of walls from collapsing into one unbounded zone. Returns zones
    sorted by price descending."""
    if not walls:
        return []
    ordered = sorted(walls, key=lambda w: w.bin.price, reverse=True)
    max_gap = mid * max_gap_bps / 1e4
    max_width = mid * max_zone_width_bps / 1e4
    groups: list[list[WallBin]] = [[ordered[0]]]
    for w in ordered[1:]:
        prev_price = groups[-1][-1].bin.price
        head_price = groups[-1][0].bin.price   # highest price in the group (sorted desc)
        if prev_price - w.bin.price <= max_gap and head_price - w.bin.price <= max_width:
            groups[-1].append(w)
        else:
            groups.append([w])
    zones: list[RawZone] = []
    for group in groups:
        strength = sum(w.bin.size for w in group)
        low = min(w.bin.price for w in group)
        high = max(w.bin.price for w in group)
        price = sum(w.bin.price * w.bin.size for w in group) / strength if strength else high
        venues: dict[str, None] = {}
        for w in group:
            for v in w.bin.composition:
                venues[v] = None
        zones.append(RawZone(low=low, high=high, price=price, strength=strength, venues=tuple(venues)))
    return zones
