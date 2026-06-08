# src/pavilos/connectors/kraken_book.py
"""Full-precision Kraken book kept only to verify the v2 CRC32 checksum.

Separate from the aggregator's float ``BookState`` (which is fine for binning
but loses the precision the checksum needs). Price strings are dict keys, valid
because Kraken sends each price at a fixed precision per pair."""
from __future__ import annotations

import heapq
from decimal import Decimal

from pavilos.connectors.kraken import book_checksum


def _to_str(v: object) -> str:
    """Normalize a Kraken price/qty (str when from a fake, Decimal when decoded
    with ``parse_float=Decimal``) to its plain-decimal string form."""
    if isinstance(v, str):
        return v
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


class KrakenRawBook:
    """Maintains one Kraken book at full string precision for CRC32 verification.

    Price strings are dict keys, which assumes Kraken sends each price at a fixed
    precision per pair (true for v2). If the same price ever arrived in two string
    forms (e.g. "100.0" vs "100.00"), the computed checksum would diverge from the
    frame's and the connector would re-subscribe (a self-healing spurious resync,
    not data corruption)."""

    def __init__(self, symbol: str, *, depth: int = 10) -> None:
        self.symbol = symbol
        self._depth = depth
        self._bids: dict[str, str] = {}
        self._asks: dict[str, str] = {}

    def apply(self, msg: dict) -> None:
        data = msg["data"][0]
        if msg["type"] == "snapshot":
            self._bids = {}
            self._asks = {}
        for lvl in data["bids"]:
            self._set(self._bids, lvl)
        for lvl in data["asks"]:
            self._set(self._asks, lvl)
        # Bound each side to the top `depth` by price -- but only when it has actually
        # grown past depth. In the steady state the book sits at ~depth, so this is a
        # cheap length check, NOT a full Decimal re-sort on every update (that sort was
        # the event-loop hog at depth=1000, ~0.5ms/update, starving the ccxt WS feeds).
        self._bids = self._bound(self._bids, reverse=True)
        self._asks = self._bound(self._asks, reverse=False)

    def _bound(self, side: dict[str, str], *, reverse: bool) -> dict[str, str]:
        if len(side) <= self._depth:
            return side                       # already within depth -> no work (common case)
        pick = heapq.nlargest if reverse else heapq.nsmallest   # bids keep highest, asks lowest
        return dict(pick(self._depth, side.items(), key=lambda kv: Decimal(kv[0])))

    @staticmethod
    def _set(side: dict[str, str], lvl: dict) -> None:
        price = _to_str(lvl["price"])
        qty = _to_str(lvl["qty"])
        if Decimal(qty) == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def checksum(self) -> int:
        # Kraken's CRC is ALWAYS over the top 10 levels, regardless of subscribed depth.
        # Select them with heapq (no full sort of all `depth` levels). float orders/picks
        # the 10 -- exact for Kraken's distinct fixed-precision prices -- while the CRC is
        # computed over the exact wire STRINGS, so its value is identical to a Decimal sort.
        asks = heapq.nsmallest(10, self._asks.items(), key=lambda kv: float(kv[0]))
        bids = heapq.nlargest(10, self._bids.items(), key=lambda kv: float(kv[0]))
        return book_checksum([(p, q) for p, q in asks], [(p, q) for p, q in bids])

    def verify(self, expected: int) -> bool:
        return self.checksum() == expected
