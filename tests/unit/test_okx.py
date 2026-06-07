# tests/unit/test_okx.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.okx import OKXFeed


def _msg(action, seq_id, prev, bids, asks):
    def lv(rows):
        return [[p, s, "0", "1"] for p, s in rows]
    return {"arg": {"channel": "books", "instId": "BTC-USDT"}, "action": action,
            "data": [{"asks": lv(asks), "bids": lv(bids), "ts": "1700000000000",
                      "checksum": 0, "prevSeqId": prev, "seqId": seq_id}]}


def test_skips_non_books_frames():
    feed = OKXFeed()
    assert feed.process({"event": "subscribe", "arg": {"channel": "books"}}, ts=1.0) is None
    assert feed.process({"arg": {"channel": "tickers"}, "data": []}, ts=1.0) is None


def test_snapshot_then_contiguous_update_with_removal():
    feed = OKXFeed()
    snap = feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], [("101.0", "2.0")]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "okx" and snap.is_snapshot is True and snap.seq == 100
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)
    upd = feed.process(_msg("update", 101, 100, [("100.0", "0")], []), ts=6.0)  # prevSeqId==last
    assert upd.is_snapshot is False and upd.seq == 101
    assert upd.bids == ((100.0, 0.0),)  # size "0" removal passed through


def test_seqid_gap_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 105, 104, [("100.0", "2.0")], []), ts=2.0)  # prev 104 != last 100


def test_seqid_equal_prev_is_benign_noop():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    upd = feed.process(_msg("update", 100, 100, [], []), ts=2.0)  # seqId==prevSeqId resend
    assert upd is not None and upd.seq == 100


def test_seqid_reset_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 5, 100, [("100.0", "2.0")], []), ts=2.0)  # seqId<prevSeqId reset


def test_update_before_snapshot_raises_resync():
    feed = OKXFeed()
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 101, 100, [("100.0", "1.0")], []), ts=1.0)


def test_update_missing_prevseqid_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    # prevSeqId absent/None on a non-snapshot update must NOT bypass continuity
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 99999, None, [("100.0", "9.0")], []), ts=2.0)


def test_update_with_minus_one_prev_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    # prevSeqId == -1 on an UPDATE is malformed (only snapshots carry -1) -> resync
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 99999, -1, [("100.0", "9.0")], []), ts=2.0)


def test_mid_stream_snapshot_resets_after_gap():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    # a re-pushed snapshot (prevSeqId=-1) after the stream resets cleanly, no raise
    snap2 = feed.process(_msg("snapshot", 500, -1, [("100.0", "5.0")], []), ts=2.0)
    assert snap2 is not None and snap2.is_snapshot is True and snap2.seq == 500
