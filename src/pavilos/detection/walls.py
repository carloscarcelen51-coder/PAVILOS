# src/pavilos/detection/walls.py
"""Detect liquidity walls: bins that stand out vs the side's typical depth."""
from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median

from pavilos.core.models import DepthBin


@dataclass(slots=True, frozen=True)
class WallBin:
    """A bin flagged as a wall, with its prominence (size / side median)."""

    bin: DepthBin
    prominence: float


def detect_walls(bins, *, size_multiple: float, min_size: float) -> list[WallBin]:
    """Return the bins whose size is >= ``size_multiple`` x the median size of all
    ``bins`` AND >= ``min_size``. ``prominence`` = size / median. Empty/uniform
    books yield no walls. ``bins`` is one side (bids or asks) of a snapshot."""
    finite = [b for b in bins if math.isfinite(b.size) and math.isfinite(b.price)]
    sizes = [b.size for b in finite]
    if not sizes:
        return []
    med = median(sizes)
    if med <= 0:
        return []
    threshold = size_multiple * med
    walls: list[WallBin] = []
    for b in finite:
        if b.size >= threshold and b.size >= min_size:
            walls.append(WallBin(bin=b, prominence=b.size / med))
    return walls
