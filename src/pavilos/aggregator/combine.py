# src/pavilos/aggregator/combine.py
"""Combine several venues' books into one binned, USD-normalized snapshot."""
from __future__ import annotations

import math
from statistics import median

from pavilos.core.models import (
    CombinedDepthSnapshot, DepthBin, Tier, VenueSpec,
)
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.normalize import PegProvider


def bin_index(price: float, mid: float, bin_bps: float) -> int:
    """Signed bin index of ``price`` relative to ``mid``, in units of ``bin_bps``.

    Negative for prices below mid (bids), >= 0 for prices above mid (asks).
    """
    bps = (price - mid) / mid * 1e4
    return int(math.floor(bps / bin_bps))


def _bin_center_price(idx: int, mid: float, bin_bps: float) -> float:
    """Representative USD price for a bin index (its center)."""
    return mid * (1.0 + (idx + 0.5) * bin_bps / 1e4)


def build_combined(
    books: dict[str, BookState],
    specs: dict[str, VenueSpec],
    peg: PegProvider,
    *,
    bin_bps: float,
    window_bps: float,
    ts: float,
    active: set[str],
) -> CombinedDepthSnapshot | None:
    """Build a combined depth snapshot from the active Tier-A venues.

    Only venues that are (a) Tier A, (b) in ``active``, and (c) have a valid mid
    contribute. Prices are converted to USD via ``peg``, binned in ``bin_bps``
    buckets around the combined mid, and summed across venues. Levels beyond
    ``window_bps`` from mid are ignored. Returns ``None`` if no Tier-A venue is
    active.

    Callers must pass positive ``bin_bps`` and ``window_bps`` (validated at
    config-load time, not here). Levels on the wrong side of the combined mid
    (a venue's bid priced above mid, or ask priced below it — possible when
    venues' books straddle the median mid) are intentionally dropped.
    """
    contributors: list[tuple[str, BookState, float]] = []  # (exchange, book, usd_mid)
    for exchange, book in books.items():
        spec = specs.get(exchange)
        if spec is None or spec.tier is not Tier.A or exchange not in active:
            continue
        m = book.mid()
        if m is None:
            continue
        contributors.append((exchange, book, peg.to_usd(m, spec.quote)))

    if not contributors:
        return None

    mid = float(median(usd_mid for _, _, usd_mid in contributors))
    half_window = mid * window_bps / 1e4

    bid_size: dict[int, float] = {}
    bid_comp: dict[int, dict[str, float]] = {}
    ask_size: dict[int, float] = {}
    ask_comp: dict[int, dict[str, float]] = {}

    def _accumulate(levels, quote, exchange, *, is_bid, size_map, comp_map):
        for price, size in levels:
            usd = peg.to_usd(price, quote)
            if is_bid:
                if not (mid - half_window <= usd < mid):
                    continue
            else:
                if not (mid < usd <= mid + half_window):
                    continue
            idx = bin_index(usd, mid, bin_bps)
            size_map[idx] = size_map.get(idx, 0.0) + size
            comp = comp_map.setdefault(idx, {})
            comp[exchange] = comp.get(exchange, 0.0) + size

    for exchange, book, _ in contributors:
        quote = specs[exchange].quote
        _accumulate(book.bids().items(), quote, exchange, is_bid=True, size_map=bid_size, comp_map=bid_comp)
        _accumulate(book.asks().items(), quote, exchange, is_bid=False, size_map=ask_size, comp_map=ask_comp)

    bids = tuple(
        DepthBin(price=_bin_center_price(idx, mid, bin_bps), size=bid_size[idx], composition=bid_comp[idx])
        for idx in sorted(bid_size, reverse=True)
    )
    asks = tuple(
        DepthBin(price=_bin_center_price(idx, mid, bin_bps), size=ask_size[idx], composition=ask_comp[idx])
        for idx in sorted(ask_size)
    )
    venues_total = sum(1 for s in specs.values() if s.tier is Tier.A)
    venues_active = tuple(sorted(ex for ex, _, _ in contributors))
    return CombinedDepthSnapshot(
        ts=ts, mid=mid, bids=bids, asks=asks,
        venues_active=venues_active, venues_total=venues_total,
    )
