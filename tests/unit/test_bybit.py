# tests/unit/test_bybit.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.bybit import BybitFeed


def _msg(mtype, u, b, a, seq=1):
    return {"topic": "orderbook.200.BTCUSDT", "type": mtype, "ts": 1700000000000,
            "data": {"s": "BTCUSDT", "b": b, "a": a, "u": u, "seq": seq}}


def test_skips_non_data_frames():
    feed = BybitFeed()
    assert feed.process({"op": "subscribe", "success": True}, ts=1.0) is None
    assert feed.process({"op": "ping", "ret_msg": "pong"}, ts=1.0) is None


def test_snapshot_then_contiguous_delta_with_removal():
    feed = BybitFeed()
    snap = feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "bybit" and snap.is_snapshot is True and snap.seq == 100
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)
    upd = feed.process(_msg("delta", 101, [["100.0", "0"]], []), ts=6.0)  # u==last+1
    assert upd.is_snapshot is False and upd.seq == 101 and upd.bids == ((100.0, 0.0),)


def test_u_gap_raises_resync():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("delta", 103, [["100.0", "2.0"]], []), ts=2.0)  # 103 != 100+1


def test_u_equals_one_is_reset_snapshot():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    # service restart: u==1 must be treated as a fresh snapshot, NOT a gap
    out = feed.process(_msg("delta", 1, [["100.0", "5.0"]], []), ts=2.0)
    assert out.is_snapshot is True and out.seq == 1 and out.bids == ((100.0, 5.0),)


def test_mid_stream_snapshot_resets():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    out = feed.process(_msg("snapshot", 200, [["100.0", "9.0"]], []), ts=2.0)  # re-sent snapshot
    assert out.is_snapshot is True and out.seq == 200


def test_delta_before_snapshot_raises_resync():
    feed = BybitFeed()
    with pytest.raises(ResyncRequired):
        feed.process(_msg("delta", 500, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=1.0)


def test_delta_missing_u_raises_resync():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    msg = _msg("delta", 101, [["100.0", "2.0"]], [])
    del msg["data"]["u"]   # a delta with no u must not bypass continuity
    with pytest.raises(ResyncRequired):
        feed.process(msg, ts=2.0)
