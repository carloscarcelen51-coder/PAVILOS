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


def test_build_combined_collapses_multiple_levels_into_one_bin():
    books = {"kraken": _book("kraken", [(100.4, 2.0), (100.3, 2.0), (100.2, 2.0)], [(100.6, 1.0)])}
    specs = {"kraken": VenueSpec("kraken", Quote.USD, Tier.A)}
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken"})
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    # the three bids fall in the same bin -> one combined bin of size 6.0
    assert len(snap.bids) == 1
    assert snap.bids[0].size == pytest.approx(6.0)
    assert snap.bids[0].composition == {"kraken": pytest.approx(6.0)}


def test_build_combined_drops_levels_outside_window():
    books = {"kraken": _book("kraken", [(100.0, 1.0), (95.0, 5.0)], [(101.0, 1.0)])}
    specs = {"kraken": VenueSpec("kraken", Quote.USD, Tier.A)}
    # mid = 100.5, window 200 bps -> half-window ~ $2.01, so 95.0 is excluded, 100.0 kept
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken"})
    assert snap is not None
    assert sum(b.size for b in snap.bids) == pytest.approx(1.0)   # the 95.0/size-5 level dropped
    assert all(b.price > 98.0 for b in snap.bids)


def test_build_combined_applies_non_unit_peg_rate():
    peg = PegProvider()
    peg.set_rate(Quote.USDT, 0.99)        # USDT priced below USD
    books = {"binance": _book("binance", [(101.0, 1.0)], [(102.0, 1.0)])}
    specs = {"binance": VenueSpec("binance", Quote.USDT, Tier.A)}
    snap = build_combined(books, specs, peg, bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"binance"})
    assert snap is not None
    # USDT mid 101.5 * 0.99 = 100.485 -> combined USD mid reflects the conversion
    assert snap.mid == pytest.approx(100.485)
    assert snap.venues_active == ("binance",)
    assert sum(b.size for b in snap.bids) == pytest.approx(1.0)
    assert sum(a.size for a in snap.asks) == pytest.approx(1.0)
