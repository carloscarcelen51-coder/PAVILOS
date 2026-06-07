# src/pavilos/detection/models.py
"""Immutable detection result types. No logic."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    SUPPORT = "support"        # below mid (a buy wall)
    RESISTANCE = "resistance"  # above mid (a sell wall)


@dataclass(slots=True, frozen=True)
class Zone:
    """A detected support/resistance zone in the combined book.

    ``price`` is the strength-weighted representative USD price; ``low``/``high``
    bound the zone; ``strength`` is the total base size (BTC) in the zone;
    ``venues`` are the exchanges contributing; ``persistence_s`` is how long the
    zone has existed; ``pulled`` flags a zone observed to vanish without price
    reaching it (spoof-like); ``confidence`` is 0..1."""

    side: Side
    price: float
    low: float
    high: float
    strength: float
    venues: tuple[str, ...]
    persistence_s: float
    pulled: bool
    confidence: float


@dataclass(slots=True, frozen=True)
class DepthAnalysis:
    """Detector output for one snapshot: ranked supports (below mid) and
    resistances (above mid), each sorted by confidence descending."""

    ts: float
    mid: float
    supports: tuple[Zone, ...]
    resistances: tuple[Zone, ...]
