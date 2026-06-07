# src/pavilos/aggregator/book_state.py
"""Maintain ONE exchange's L2 order book from absolute-size updates."""
from __future__ import annotations

import math
from collections.abc import Iterable

from pavilos.core.models import BookUpdate


class BookState:
    """Per-exchange L2 book held as ``price -> size`` maps.

    Sizes are absolute; a size <= 0 removes the level. Snapshots reset the
    book. When ``track_seq`` is set and updates carry ``seq``, updates whose
    ``seq`` is not strictly greater than the last seen ``seq`` are dropped
    (stale/duplicate). Prices remain in the exchange's quote currency.
    """

    def __init__(self, exchange: str, *, track_seq: bool = False) -> None:
        self.exchange = exchange
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self.last_ts: float = 0.0
        self.last_seq: int | None = None
        self._track_seq = track_seq

    def apply(self, u: BookUpdate) -> None:
        if not u.is_snapshot and self._track_seq and u.seq is not None and self.last_seq is not None:
            if u.seq <= self.last_seq:
                return  # stale / duplicate
        if u.is_snapshot:
            self._bids = {p: s for p, s in u.bids if s > 0 and math.isfinite(p) and math.isfinite(s)}
            self._asks = {p: s for p, s in u.asks if s > 0 and math.isfinite(p) and math.isfinite(s)}
        else:
            self._apply_side(self._bids, u.bids)
            self._apply_side(self._asks, u.asks)
        if u.seq is not None:
            self.last_seq = u.seq
        self.last_ts = u.ts

    @staticmethod
    def _apply_side(book: dict[float, float], levels: Iterable[tuple[float, float]]) -> None:
        for price, size in levels:
            if not (math.isfinite(price) and math.isfinite(size)):
                continue  # never let a malformed (NaN/inf) level enter the book
            if size <= 0:
                book.pop(price, None)
            else:
                book[price] = size

    def bids(self) -> dict[float, float]:
        return self._bids

    def asks(self) -> dict[float, float]:
        return self._asks

    def best_bid(self) -> float | None:
        return max(self._bids) if self._bids else None

    def best_ask(self) -> float | None:
        return min(self._asks) if self._asks else None

    def mid(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2.0
