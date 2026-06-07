# src/pavilos/signals/sizing.py
"""Risk-based position sizing with a leverage cap. Pure."""
from __future__ import annotations

import math


def position_size(equity: float, *, entry: float, stop: float,
                  risk_pct: float, max_leverage: float) -> float:
    """Units sized so a stop-out loses ``risk_pct`` of ``equity``, capped so the
    notional never exceeds ``max_leverage * equity``. Returns 0.0 on a
    non-positive/zero stop distance or any non-finite input."""
    if not all(math.isfinite(x) for x in (equity, entry, stop, risk_pct, max_leverage)):
        return 0.0
    if equity <= 0 or entry <= 0 or risk_pct <= 0 or max_leverage <= 0:
        return 0.0
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    size = (equity * risk_pct) / risk_per_unit
    max_size = max_leverage * equity / entry
    return max(0.0, min(size, max_size))
