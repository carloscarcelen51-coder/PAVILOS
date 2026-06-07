# tests/unit/test_bitstamp.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.bitstamp import BitstampDepthFeed


def _snapshot(micro, bids, asks):
    return {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}


def _diff(micro, bids, asks):
    return {"event": "data", "channel": "diff_order_book_btcusd",
            "data": {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}}


def test_seed_emits_snapshot_and_sets_watermark():
    feed = BitstampDepthFeed("btcusd")
    snap = feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "bitstamp" and snap.is_snapshot is True and snap.ts == 5.0
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)


def test_apply_drops_stale_then_applies_with_removal():
    feed = BitstampDepthFeed("btcusd")
    feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert feed.apply(_diff(1000, [["1.0", "1.0"]], []), ts=6.0) is None   # micro <= watermark
    assert feed.apply(_diff(999, [["1.0", "1.0"]], []), ts=6.0) is None    # older still dropped
    upd = feed.apply(_diff(2000, [["100.0", "0"]], [["101.5", "3.0"]]), ts=7.0)
    assert upd.is_snapshot is False and upd.bids == ((100.0, 0.0),) and upd.asks == ((101.5, 3.0),)


def test_apply_before_seed_raises():
    feed = BitstampDepthFeed("btcusd")
    with pytest.raises(ResyncRequired):
        feed.apply(_diff(1000, [], []), ts=1.0)


def test_non_data_event_ignored():
    feed = BitstampDepthFeed("btcusd")
    feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert feed.apply({"event": "bts:subscription_succeeded", "channel": "diff_order_book_btcusd", "data": {}}, ts=6.0) is None
