# tests/unit/test_combine.py
import math
import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.combine import bin_index, build_combined


def test_bin_index_sign_and_magnitude():
    mid = 100.5
    # 100.0 is ~ -49.75 bps from mid -> floor(-0.4975) = -1
    assert bin_index(100.0, mid, bin_bps=100.0) == -1
    # 99.0 is ~ -149.25 bps -> floor(-1.4925) = -2
    assert bin_index(99.0, mid, bin_bps=100.0) == -2
    # 101.0 is ~ +49.75 bps -> floor(0.4975) = 0
    assert bin_index(101.0, mid, bin_bps=100.0) == 0
    # 102.0 is ~ +149.25 bps -> floor(1.4925) = 1
    assert bin_index(102.0, mid, bin_bps=100.0) == 1


def _book(exchange, bids, asks):
    bs = BookState(exchange)
    bs.apply(BookUpdate(exchange=exchange, ts=1.0, bids=tuple(bids), asks=tuple(asks),
                        is_snapshot=True, seq=None))
    return bs


def test_build_combined_sums_sizes_and_tracks_composition():
    books = {
        "kraken": _book("kraken", [(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0), (102.0, 3.0)]),
        "coinbase": _book("coinbase", [(100.0, 0.5)], [(101.0, 0.5)]),
    }
    specs = {
        "kraken": VenueSpec("kraken", Quote.USD, Tier.A),
        "coinbase": VenueSpec("coinbase", Quote.USD, Tier.A),
    }
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken", "coinbase"})
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    assert snap.venues_total == 2
    assert set(snap.venues_active) == {"kraken", "coinbase"}

    # bids: bin -1 (the 100.0 level from both venues) and bin -2 (kraken 99.0)
    bids_by_size = sorted((b.size for b in snap.bids), reverse=True)
    assert bids_by_size == pytest.approx([2.0, 1.5])
    top_bid = max(snap.bids, key=lambda b: b.price)        # nearest to mid = bin -1
    assert top_bid.size == pytest.approx(1.5)
    assert top_bid.composition == {"kraken": pytest.approx(1.0), "coinbase": pytest.approx(0.5)}
    assert top_bid.price == pytest.approx(100.5 * (1 + (-0.5) * 100.0 / 1e4))  # ~99.9975

    # asks: bin 0 (101.0 from both) and bin 1 (kraken 102.0)
    asks_by_size = sorted((a.size for a in snap.asks))
    assert asks_by_size == pytest.approx([1.5, 3.0])
    best_ask = min(snap.asks, key=lambda a: a.price)        # nearest to mid = bin 0
    assert best_ask.composition == {"kraken": pytest.approx(1.0), "coinbase": pytest.approx(0.5)}


def test_build_combined_excludes_tier_b_and_inactive():
    books = {
        "kraken": _book("kraken", [(100.0, 1.0)], [(101.0, 1.0)]),
        "upbit": _book("upbit", [(100.0, 9.0)], [(101.0, 9.0)]),    # Tier B -> excluded
        "okx": _book("okx", [(100.0, 7.0)], [(101.0, 7.0)]),        # inactive -> excluded
    }
    specs = {
        "kraken": VenueSpec("kraken", Quote.USD, Tier.A),
        "upbit": VenueSpec("upbit", Quote.KRW, Tier.B),
        "okx": VenueSpec("okx", Quote.USDT, Tier.A),
    }
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken"})
    assert snap is not None
    # only kraken contributes -> top bid size 1.0, composition only kraken
    top_bid = max(snap.bids, key=lambda b: b.price)
    assert top_bid.size == pytest.approx(1.0)
    assert top_bid.composition == {"kraken": pytest.approx(1.0)}
    assert snap.venues_total == 2          # kraken + okx are Tier A
    assert snap.venues_active == ("kraken",)


def test_build_combined_returns_none_without_active_tier_a():
    books = {"upbit": _book("upbit", [(100.0, 1.0)], [(101.0, 1.0)])}
    specs = {"upbit": VenueSpec("upbit", Quote.KRW, Tier.B)}
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active=set())
    assert snap is None
