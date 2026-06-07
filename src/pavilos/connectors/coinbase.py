# src/pavilos/connectors/coinbase.py
"""Coinbase Advanced Trade level2 sequencer (pure). Messages arrive as
channel 'l2_data'; integrity is per-product sequence_num (+1 exact)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


class CoinbaseFeed:
    """Turns Coinbase ``l2_data`` frames into ``BookUpdate``s. ``new_quantity`` is
    absolute (``"0"`` removes; passed through for BookState to drop). ``side`` is
    ``bid``/``offer`` (offer -> ask). Gap on ``sequence_num`` raises ResyncRequired;
    a lower/equal sequence_num is ignored (duplicate / out-of-order)."""

    def __init__(self, exchange: str = "coinbase") -> None:
        self.exchange = exchange
        self._last_seq: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if msg.get("channel") != "l2_data":
            return None  # subscriptions / heartbeats / other
        seq = msg.get("sequence_num")
        if seq is not None and self._last_seq is not None:
            if seq <= self._last_seq:
                return None  # duplicate / out-of-order
            if seq > self._last_seq + 1:
                raise ResyncRequired(f"coinbase sequence gap: {seq} > {self._last_seq}+1")
        is_snapshot = False
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        for event in msg.get("events", []):
            if event.get("type") == "snapshot":
                is_snapshot = True
            for upd in event.get("updates", []):
                price = float(upd["price_level"])
                size = float(upd["new_quantity"])
                if upd["side"] == "bid":
                    bids.append((price, size))
                else:  # "offer"
                    asks.append((price, size))
        if seq is not None:
            self._last_seq = seq
        return BookUpdate(exchange=self.exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                          is_snapshot=is_snapshot, seq=seq)
