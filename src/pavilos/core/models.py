# src/pavilos/core/models.py
"""Shared immutable data types for PAVILOS. No logic lives here."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Side(str, Enum):
    BID = "bid"
    ASK = "ask"


class Quote(str, Enum):
    USD = "USD"
    USDT = "USDT"
    USDC = "USDC"
    EUR = "EUR"
    KRW = "KRW"
    JPY = "JPY"


class Tier(str, Enum):
    A = "A"  # price-comparable core (USD/USDT/USDC) -> mixed into the USD level map
    B = "B"  # context/breadth (KRW/JPY/EUR) -> kept separate, not in the level map


#: Quotes that are price-comparable in USD and may be mixed into the combined book.
TIER_A_QUOTES: frozenset[Quote] = frozenset({Quote.USD, Quote.USDT, Quote.USDC})


@dataclass(slots=True, frozen=True)
class BookUpdate:
    """A normalized L2 update from one exchange.

    Sizes are ABSOLUTE base-asset (BTC) quantities at each price level; a size
    of 0.0 removes that level. ``is_snapshot=True`` means the receiver should
    reset its book to exactly these levels before applying. Prices are in the
    exchange's own quote currency — USD normalization happens downstream.
    ``seq`` is the feed's sequence number when available (else None).
    """

    exchange: str
    ts: float
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    is_snapshot: bool
    seq: int | None = None


@dataclass(slots=True, frozen=True)
class VenueSpec:
    """Static metadata about one venue used by the aggregator."""

    exchange: str
    quote: Quote
    tier: Tier


@dataclass(slots=True, frozen=True)
class DepthBin:
    """One binned price level of the combined book.

    ``price`` is the bin's representative (center) USD price, ``size`` is the
    total base size across contributing venues, and ``composition`` maps each
    contributing exchange to the size it added.
    """

    price: float
    size: float
    composition: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CombinedDepthSnapshot:
    """The combined, USD-normalized, binned book at one instant.

    ``bids`` are sorted by price descending (best bid first); ``asks`` ascending
    (best ask first). ``venues_active`` are the Tier-A venues that contributed;
    ``venues_total`` is the count of configured Tier-A venues.
    """

    ts: float
    mid: float
    bids: tuple[DepthBin, ...]
    asks: tuple[DepthBin, ...]
    venues_active: tuple[str, ...]
    venues_total: int
