# src/pavilos/connectors/bybit.py
"""Bybit v5 spot orderbook sequencer (pure). Integrity is data.u continuity;
type 'snapshot' or u==1 RESETS the book; no checksum."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(p), float(s)) for p, s in rows)


class BybitFeed:
    """Turns Bybit ``orderbook.*`` frames into ``BookUpdate``s. A ``type:"snapshot"``
    OR ``u == 1`` (service restart) resets the book; a ``delta`` is valid iff
    ``u == last_u + 1``. A non-consecutive ``u`` raises ResyncRequired. ``seq`` is
    NOT used for continuity. Sizes absolute; ``"0"`` removes."""

    def __init__(self, exchange: str = "bybit") -> None:
        self.exchange = exchange
        self._last_u: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if "topic" not in msg or "type" not in msg or "data" not in msg:
            return None  # op/pong/subscribe ack
        data = msg["data"]
        u = data.get("u")
        is_snapshot = msg["type"] == "snapshot" or u == 1
        if not is_snapshot:
            if self._last_u is None:
                raise ResyncRequired("bybit: delta before snapshot")
            if u is None:
                raise ResyncRequired("bybit: delta missing u")
            if u != self._last_u + 1:
                raise ResyncRequired(f"bybit u gap: {u} != {self._last_u}+1")
        if u is not None:
            self._last_u = u
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data.get("b", [])),
                          asks=_levels(data.get("a", [])), is_snapshot=is_snapshot, seq=u)
