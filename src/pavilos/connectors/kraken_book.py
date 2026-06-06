# src/pavilos/connectors/kraken_book.py
"""Full-precision Kraken book kept only to verify the v2 CRC32 checksum.

Separate from the aggregator's float ``BookState`` (which is fine for binning
but loses the precision the checksum needs). Price strings are dict keys, valid
because Kraken sends each price at a fixed precision per pair."""
from __future__ import annotations

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
        self._bids = dict(sorted(self._bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[: self._depth])
        self._asks = dict(sorted(self._asks.items(), key=lambda kv: Decimal(kv[0]))[: self._depth])

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
        asks = sorted(self._asks.items(), key=lambda kv: Decimal(kv[0]))[:10]
        bids = sorted(self._bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[:10]
        return book_checksum([(p, q) for p, q in asks], [(p, q) for p, q in bids])

    def verify(self, expected: int) -> bool:
        return self.checksum() == expected
