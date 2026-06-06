# src/pavilos/connectors/kraken.py
"""Kraken Spot WS v2 `book` channel: pure checksum + frame parsing. No I/O."""
from __future__ import annotations

import zlib

from pavilos.core.models import BookUpdate


def _fmt(value: str) -> str:
    """Kraken checksum formatting for one price or qty string: remove the decimal
    point and strip leading zeros (e.g. '0.00100000' -> '100000'). Returns '0'
    for an all-zero result (defensive; removed levels never reach here)."""
    return value.replace(".", "").lstrip("0") or "0"


def _crc32(s: str) -> int:
    """CRC32 of the ASCII bytes of ``s``, cast to unsigned 32-bit (Kraken's cast)."""
    return zlib.crc32(s.encode("ascii")) & 0xFFFFFFFF


def book_checksum(asks: list[tuple[str, str]], bids: list[tuple[str, str]]) -> int:
    """Kraken v2 book CRC32 over the top-10 asks (price low->high) then top-10
    bids (price high->low). Each side must already be sorted in that order;
    only the first 10 of each are used. ``asks``/``bids`` are (price, qty)
    strings at full wire precision."""
    parts: list[str] = []
    for price, qty in asks[:10]:
        parts.append(_fmt(price) + _fmt(qty))
    for price, qty in bids[:10]:
        parts.append(_fmt(price) + _fmt(qty))
    return _crc32("".join(parts))


def parse_kraken_message(msg: dict, *, ts: float, exchange: str = "kraken") -> BookUpdate:
    """Convert a decoded Kraken v2 ``book`` message into a ``BookUpdate``.

    ``type:"snapshot"`` -> ``is_snapshot=True``; ``"update"`` -> ``False``.
    Levels are taken from ``data[0]`` and converted to float (price, qty) tuples;
    ``qty == 0`` levels are preserved verbatim (``BookState`` removes them on
    apply). The book channel has no sequence number, so ``seq`` is ``None`` —
    integrity is verified separately via the CRC32 checksum."""
    data = msg["data"][0]
    bids = tuple((float(lvl["price"]), float(lvl["qty"])) for lvl in data["bids"])
    asks = tuple((float(lvl["price"]), float(lvl["qty"])) for lvl in data["asks"])
    return BookUpdate(
        exchange=exchange,
        ts=ts,
        bids=bids,
        asks=asks,
        is_snapshot=(msg["type"] == "snapshot"),
        seq=None,
    )
