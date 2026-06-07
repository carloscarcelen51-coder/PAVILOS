# src/pavilos/connectors/coinbase.py
"""Coinbase Advanced Trade level2 sequencer (pure). Book updates arrive as
channel 'l2_data'; integrity is the connection-level sequence_num (+1 exact)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


class CoinbaseFeed:
    """Turns Coinbase ``l2_data`` frames into ``BookUpdate``s. ``new_quantity`` is
    absolute (``"0"`` removes; passed through for BookState to drop). ``side`` is
    ``bid``/``offer`` (offer -> ask).

    ``sequence_num`` is a CONNECTION-LEVEL counter that increments for EVERY
    message on the socket (l2_data, heartbeats, subscriptions), so continuity is
    validated across ALL frames — otherwise an interleaved heartbeat looks like a
    gap and triggers a false resync. Only ``l2_data`` frames emit a BookUpdate; a
    gap raises ResyncRequired, a lower/equal sequence_num is ignored, and an
    update arriving before the first snapshot raises ResyncRequired."""

    def __init__(self, exchange: str = "coinbase") -> None:
        self.exchange = exchange
        self._last_seq: int | None = None
        self._have_snapshot = False

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        seq = msg.get("sequence_num")
        if seq is not None and self._last_seq is not None:
            if seq <= self._last_seq:
                return None  # duplicate / out-of-order
            if seq > self._last_seq + 1:
                raise ResyncRequired(f"coinbase sequence gap: {seq} > {self._last_seq}+1")
        if seq is not None:
            self._last_seq = seq  # advance for EVERY frame (connection-level counter)
        if msg.get("channel") != "l2_data":
            return None  # heartbeats / subscriptions: counted above for continuity, not emitted
        is_snapshot = False
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        for event in msg.get("events", []):
            if event.get("type") == "snapshot":
                is_snapshot = True
            for upd in event.get("updates", []):
                price = float(upd["price_level"])
                size = float(upd["new_quantity"])
                side = upd["side"]
                if side == "bid":
                    bids.append((price, size))
                elif side == "offer":
                    asks.append((price, size))
                else:
                    raise ResyncRequired(f"coinbase: unexpected side {side!r}")
        if is_snapshot:
            self._have_snapshot = True
        elif not self._have_snapshot:
            raise ResyncRequired("coinbase: update before snapshot")
        return BookUpdate(exchange=self.exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                          is_snapshot=is_snapshot, seq=seq)
