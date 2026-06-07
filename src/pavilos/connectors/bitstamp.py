# src/pavilos/connectors/bitstamp.py
"""Bitstamp diff_order_book sequencer (pure). No WS snapshot: seed from REST,
then reconcile diffs by microtimestamp (microseconds). No checksum/seq id."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(p), float(a)) for p, a in rows)


class BitstampDepthFeed:
    """Seeds from a REST order_book snapshot, then applies WS diffs, dropping any
    diff whose ``microtimestamp`` is <= the current watermark. Sizes absolute;
    amount ``"0"`` removes.

    INHERENT LIMITATION: Bitstamp's diff channel carries no sequence number and
    no checksum, so a silently dropped intermediate diff is NOT detectable from
    the microtimestamp stream alone (a later microtimestamp is always "valid").
    The connector recovers on an explicit ``bts:request_reconnect`` (handled in
    ``BitstampConnector``), but there is currently NO crossed-book / staleness
    detector — adding one (best_bid >= best_ask -> re-seed) is a deferred
    aggregator-level enhancement, since the assembled book lives in BookState."""

    def __init__(self, symbol: str = "btcusd", *, exchange: str = "bitstamp") -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._watermark: int | None = None

    def seed(self, snapshot: dict, *, ts: float) -> BookUpdate:
        self._watermark = int(snapshot["microtimestamp"])
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(snapshot["bids"]),
                          asks=_levels(snapshot["asks"]), is_snapshot=True, seq=None)

    def apply(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if self._watermark is None:
            raise ResyncRequired("bitstamp: apply before seed")
        if msg.get("event") != "data":
            return None  # bts:subscription_succeeded / other control events
        data = msg["data"]
        micro = int(data["microtimestamp"])
        if micro <= self._watermark:
            return None  # already covered by the snapshot / out-of-order
        self._watermark = micro
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data["bids"]),
                          asks=_levels(data["asks"]), is_snapshot=False, seq=None)
