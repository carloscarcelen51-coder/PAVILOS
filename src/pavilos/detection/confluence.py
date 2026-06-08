# src/pavilos/detection/confluence.py
"""ConfluenceAnalyzer: merge same-side zones that sit within a price band into
multi-venue confluence clusters, scored by venue-consensus + stacking.

Pure and stateless. The score at time T uses only the snapshot's zones (data
<= T) — a causal SETUP score, never a forward outcome."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.detection.models import DepthAnalysis, Side, Zone


@dataclass(slots=True, frozen=True)
class ConfluenceConfig:
    """Tuning for :func:`analyze_confluence`.

    ``confluence_band_bps`` — same-side zones whose bands sit within this many
    bps (relative to mid) are merged into one cluster. ``venues_target`` — the
    venue count treated as full consensus (score saturates here). ``threshold``
    — minimum score for ``tradeable``. ``min_venues`` — minimum unique venues
    for ``tradeable``. ``min_persistence_s`` — minimum cluster persistence for
    ``tradeable``."""

    confluence_band_bps: float
    venues_target: float
    threshold: float
    min_venues: int
    min_persistence_s: float = 0.0


@dataclass(slots=True, frozen=True)
class ConfluenceCluster:
    """A band of merged same-side zones. ``price_lo``/``price_hi`` bound the
    merged band; ``venues`` is the union of contributing exchanges; ``n_zones``
    counts the merged zones (stacking); ``max_confidence``/``max_persistence_s``
    take the best member; ``score`` is venue-consensus-dominant + stacking;
    ``tradeable`` is the gate verdict."""

    side: Side
    price_lo: float
    price_hi: float
    n_zones: int
    venues: tuple[str, ...]
    n_venues: int
    max_confidence: float
    max_persistence_s: float
    score: float
    tradeable: bool


def _merge_side(side: Side, zones: tuple[Zone, ...], mid: float,
                cfg: ConfluenceConfig) -> list[ConfluenceCluster]:
    if not zones:
        return []
    band = mid * cfg.confluence_band_bps / 1e4
    ordered = sorted(zones, key=lambda z: z.low)
    groups: list[list[Zone]] = [[ordered[0]]]
    for z in ordered[1:]:
        running_hi = max(g.high for g in groups[-1])
        # merge when this zone's band sits within `band` of the running band
        if z.low - running_hi <= band:
            groups[-1].append(z)
        else:
            groups.append([z])
    clusters: list[ConfluenceCluster] = []
    for group in groups:
        venues: dict[str, None] = {}
        for z in group:
            for v in z.venues:
                venues[v] = None
        venue_tuple = tuple(venues)
        n_venues = len(venue_tuple)
        n_zones = len(group)
        max_confidence = max(z.confidence for z in group)
        max_persistence_s = max(z.persistence_s for z in group)
        price_lo = min(z.low for z in group)
        price_hi = max(z.high for z in group)
        consensus = min(n_venues / cfg.venues_target, 1.0) if cfg.venues_target > 0 else 0.0
        stacking = min(n_zones, 3) / 3
        raw = 0.6 * consensus + 0.4 * stacking * max_confidence
        score = min(max(raw, 0.0), 1.0)
        tradeable = (score >= cfg.threshold
                     and n_venues >= cfg.min_venues
                     and max_persistence_s >= cfg.min_persistence_s)
        clusters.append(ConfluenceCluster(
            side=side, price_lo=price_lo, price_hi=price_hi, n_zones=n_zones,
            venues=venue_tuple, n_venues=n_venues, max_confidence=max_confidence,
            max_persistence_s=max_persistence_s, score=score, tradeable=tradeable))
    return clusters


def analyze_confluence(analysis: DepthAnalysis, cfg: ConfluenceConfig) -> list[ConfluenceCluster]:
    """Merge same-side zones within ``confluence_band_bps`` into confluence
    clusters and score them. Returns supports then resistances; pure."""
    clusters: list[ConfluenceCluster] = []
    clusters.extend(_merge_side(Side.SUPPORT, analysis.supports, analysis.mid, cfg))
    clusters.extend(_merge_side(Side.RESISTANCE, analysis.resistances, analysis.mid, cfg))
    return clusters
