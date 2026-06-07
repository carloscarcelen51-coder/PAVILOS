# tests/unit/test_book_state.py
from pavilos.core.models import BookUpdate
from pavilos.aggregator.book_state import BookState


def _snap(exchange="kraken", ts=1.0, bids=(), asks=(), seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=True, seq=seq)


def _upd(exchange="kraken", ts=2.0, bids=(), asks=(), seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=False, seq=seq)


def test_snapshot_sets_book_and_drops_zero_sizes():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0), (99.0, 0.0)], asks=[(101.0, 2.0)]))
    assert bs.bids() == {100.0: 1.0}        # the 0-size level is dropped
    assert bs.asks() == {101.0: 2.0}
    assert bs.best_bid() == 100.0
    assert bs.best_ask() == 101.0
    assert bs.mid() == 100.5
    assert bs.last_ts == 1.0


def test_update_adds_updates_and_removes_levels():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
    bs.apply(_upd(ts=2.0, bids=[(100.0, 3.0), (99.5, 1.0)], asks=[(101.0, 0.0)]))
    assert bs.bids() == {100.0: 3.0, 99.5: 1.0}   # 100.0 updated, 99.5 added
    assert bs.asks() == {}                          # 101.0 removed by 0-size
    assert bs.best_ask() is None
    assert bs.mid() is None                          # no ask side -> no mid
    assert bs.last_ts == 2.0


def test_snapshot_resets_previous_state():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0), (99.0, 5.0)], asks=[(101.0, 2.0)]))
    bs.apply(_snap(ts=3.0, bids=[(98.0, 1.0)], asks=[(102.0, 1.0)]))
    assert bs.bids() == {98.0: 1.0}                  # old levels gone
    assert bs.asks() == {102.0: 1.0}


def test_non_finite_price_or_size_levels_are_dropped():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0), (float("nan"), 5.0), (99.0, float("inf"))], asks=[(101.0, 2.0)]))
    assert bs.bids() == {100.0: 1.0}   # NaN-price and inf-size levels never enter the book
    assert bs.asks() == {101.0: 2.0}
    bs.apply(_upd(ts=2.0, bids=[(98.0, float("nan")), (100.0, 3.0)], asks=[]))
    assert bs.bids() == {100.0: 3.0}   # malformed level skipped; valid update still applied


def test_stale_or_duplicate_seq_is_ignored():
    bs = BookState("bybit", track_seq=True)
    bs.apply(_snap(exchange="bybit", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], seq=10))
    bs.apply(_upd(exchange="bybit", ts=2.0, bids=[(100.0, 2.0)], seq=9))   # stale
    assert bs.bids() == {100.0: 1.0}                 # ignored
    bs.apply(_upd(exchange="bybit", ts=3.0, bids=[(100.0, 2.0)], seq=11))  # fresh
    assert bs.bids() == {100.0: 2.0}
    assert bs.last_seq == 11
