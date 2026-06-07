# tests/unit/test_snapshot_delta_connector.py
import asyncio

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.snapshot_delta_connector import SnapshotDeltaConnector


def _run(coro):
    return asyncio.run(coro)


class _FakeFeed:
    """process(): {'skip':True} -> None; {'gap':True} -> ResyncRequired;
    else -> a BookUpdate echoing the frame."""
    def __init__(self):
        self.seen = 0

    def process(self, msg, *, ts):
        if msg.get("skip"):
            return None
        if msg.get("gap"):
            raise ResyncRequired("boom")
        self.seen += 1
        return BookUpdate(exchange="fake", ts=ts, bids=((msg["p"], msg["s"]),), asks=(),
                          is_snapshot=msg.get("snap", False), seq=msg.get("seq"))


def test_emits_skips_and_reconnects_on_resync():
    sessions = [
        [{"skip": True}, {"p": 100.0, "s": 1.0, "snap": True, "seq": 1}, {"gap": True}],
        [{"p": 100.0, "s": 2.0, "snap": True, "seq": 9}],
    ]
    calls = {"n": 0}

    async def fake_connect():
        i = calls["n"]; calls["n"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = SnapshotDeltaConnector("fake", _FakeFeed, connect=fake_connect, now=lambda: 1.0,
                                      sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u1, u2, calls["n"], conn.health()

    u1, u2, n, h = _run(scenario())
    assert u1.is_snapshot is True and u1.bids == ((100.0, 1.0),)
    assert u2.is_snapshot is True and u2.bids == ((100.0, 2.0),)
    assert n >= 2 and h.resyncs >= 1


def test_connect_failure_increments_errors():
    calls = {"n": 0}

    async def failing_connect():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("down")

        async def gen():
            yield {"p": 100.0, "s": 1.0, "snap": True, "seq": 1}
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = SnapshotDeltaConnector("fake", _FakeFeed, connect=failing_connect, now=lambda: 1.0,
                                      sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u, conn.health()

    u, h = _run(scenario())
    assert u.is_snapshot is True
    assert h.errors >= 1
