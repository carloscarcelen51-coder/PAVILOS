# src/pavilos/signals/atr.py
"""Rolling ATR proxy: mean absolute tick-to-tick price change over a window."""
from __future__ import annotations

import math
from collections import deque


class ATR:
    """Feed mids via ``update(price)``; ``value()`` is the mean of the last
    ``window`` absolute consecutive-tick changes (0.0 until two ticks seen).
    Non-finite prices are ignored."""

    def __init__(self, window: int) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self._window = window
        self._diffs: deque[float] = deque(maxlen=window)
        self._prev: float | None = None

    def update(self, price: float) -> None:
        if not math.isfinite(price):
            return
        if self._prev is not None:
            self._diffs.append(abs(price - self._prev))
        self._prev = price

    def value(self) -> float:
        if not self._diffs:
            return 0.0
        return sum(self._diffs) / len(self._diffs)
