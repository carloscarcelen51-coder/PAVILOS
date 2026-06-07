# tests/unit/test_coinbase.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.coinbase import CoinbaseFeed


def _msg(mtype, seq, updates):
    return {"channel": "l2_data", "sequence_num": seq,
            "events": [{"type": mtype, "product_id": "BTC-USD", "updates": updates}]}


def _u(side, price, qty):
    return {"side": side, "event_time": "2023-02-09T20:32:50Z", "price_level": price, "new_quantity": qty}


def test_skips_non_l2_data_frames():
    feed = CoinbaseFeed()
    assert feed.process({"channel": "subscriptions"}, ts=1.0) is None
    assert feed.process({"channel": "heartbeats", "heartbeat_counter": 1}, ts=1.0) is None


def test_snapshot_then_update_with_offer_and_removal():
    feed = CoinbaseFeed()
    snap = feed.process(_msg("snapshot", 0, [_u("bid", "100.0", "1.0"), _u("offer", "101.0", "2.0")]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "coinbase" and snap.is_snapshot is True and snap.ts == 5.0 and snap.seq == 0
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)  # 'offer' -> ask
    upd = feed.process(_msg("update", 1, [_u("bid", "100.0", "0")]), ts=6.0)  # removal
    assert upd.is_snapshot is False and upd.seq == 1
    assert upd.bids == ((100.0, 0.0),) and upd.asks == ()


def test_sequence_gap_raises_resync():
    feed = CoinbaseFeed()
    feed.process(_msg("snapshot", 10, [_u("bid", "100.0", "1.0")]), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 12, [_u("bid", "100.0", "2.0")]), ts=2.0)  # 12 > 10+1


def test_duplicate_or_out_of_order_sequence_ignored():
    feed = CoinbaseFeed()
    feed.process(_msg("snapshot", 10, [_u("bid", "100.0", "1.0")]), ts=1.0)
    assert feed.process(_msg("update", 9, [_u("bid", "1.0", "1.0")]), ts=2.0) is None   # <= last
    upd = feed.process(_msg("update", 11, [_u("bid", "100.0", "2.0")]), ts=3.0)         # contiguous
    assert upd is not None and upd.seq == 11


def test_unexpected_side_raises_resync():
    feed = CoinbaseFeed()
    # any side other than bid/offer must fail loud (-> reconnect), not silently
    # mis-file as an ask and corrupt the book.
    with pytest.raises(ResyncRequired):
        feed.process(_msg("snapshot", 0, [_u("sell", "100.0", "1.0")]), ts=1.0)


def test_update_before_snapshot_raises_resync():
    feed = CoinbaseFeed()
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 5, [_u("bid", "100.0", "1.0")]), ts=1.0)
