# tests/unit/test_binance.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.binance import BinanceDepthFeed


def _snapshot(last_update_id, bids, asks):
    return {"lastUpdateId": last_update_id, "bids": bids, "asks": asks}


def _event(U, u, bids, asks, E=1_000):
    return {"e": "depthUpdate", "E": E, "s": "BTCUSDT", "U": U, "u": u, "b": bids, "a": asks}


def test_seed_emits_snapshot_bookupdate():
    feed = BinanceDepthFeed("BTCUSDT")
    snap = feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "binance"
    assert snap.is_snapshot is True
    assert snap.ts == 5.0
    assert snap.seq == 100
    assert snap.bids == ((100.0, 1.0),)
    assert snap.asks == ((101.0, 2.0),)


def test_apply_contiguous_event_emits_update():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # first event must straddle lastUpdateId+1 = 101
    u = feed.apply(_event(U=101, u=105, bids=[["100.0", "1.5"]], asks=[["101.0", "0"]], E=6_000))
    assert u is not None
    assert u.is_snapshot is False
    assert u.seq == 105
    assert u.ts == 6.0
    assert u.bids == ((100.0, 1.5),)
    assert u.asks == ((101.0, 0.0),)   # removal preserved
    # next event must be contiguous: U == previous u + 1 == 106
    u2 = feed.apply(_event(U=106, u=108, bids=[["99.5", "4.0"]], asks=[]))
    assert u2 is not None
    assert u2.seq == 108


def test_stale_event_is_ignored():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # u <= lastUpdateId -> stale -> ignored (returns None), state unchanged
    assert feed.apply(_event(U=90, u=99, bids=[["1.0", "1.0"]], asks=[])) is None
    # a contiguous event after the stale one still applies from lastUpdateId=100
    u = feed.apply(_event(U=101, u=102, bids=[["100.0", "2.0"]], asks=[]))
    assert u is not None and u.seq == 102


def test_gap_raises_resync_required():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # U (103) > lastUpdateId+1 (101) -> missed events -> resync
    with pytest.raises(ResyncRequired):
        feed.apply(_event(U=103, u=104, bids=[["100.0", "1.0"]], asks=[]))


def test_apply_before_seed_raises():
    feed = BinanceDepthFeed("BTCUSDT")
    with pytest.raises(ResyncRequired):
        feed.apply(_event(U=1, u=2, bids=[], asks=[]))
