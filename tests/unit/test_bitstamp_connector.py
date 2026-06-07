# tests/unit/test_bitstamp_connector.py
import asyncio

from pavilos.connectors.bitstamp_connector import BitstampConnector


def _snapshot(micro, bids, asks):
    return {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}


def _diff(micro, bids, asks):
    return {"event": "data", "channel": "diff_order_book_btcusd",
            "data": {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}}


def _run(coro):
    return asyncio.run(coro)


def test_seeds_then_applies_diffs():
    diffs = [_diff(2000, [["100.0", "1.5"]], []), _diff(3000, [], [["101.0", "0"]])]

    async def fake_connect():
        async def gen():
            for d in diffs:
                yield d
        return gen()

    async def fake_fetch():
        return _snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]])

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BitstampConnector("btcusd", connect=fake_connect, fetch_snapshot=fake_fetch,
                                 now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap, u1, u2

    snap, u1, u2 = _run(scenario())
    assert snap.is_snapshot is True and snap.exchange == "bitstamp"
    assert u1.is_snapshot is False and u1.bids == ((100.0, 1.5),)
    assert u2.asks == ((101.0, 0.0),)


def test_request_reconnect_triggers_reseed():
    sessions = [
        [{"event": "bts:request_reconnect", "channel": "", "data": {}}],   # forces resync
        [_diff(2000, [["100.0", "1.5"]], [])],
    ]
    snaps = [_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]),
             _snapshot(1500, [["100.0", "1.0"]], [["101.0", "2.0"]])]
    calls = {"c": 0, "s": 0}

    async def fake_connect():
        i = calls["c"]; calls["c"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def fake_fetch():
        i = calls["s"]; calls["s"] += 1
        return snaps[min(i, len(snaps) - 1)]

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BitstampConnector("btcusd", connect=fake_connect, fetch_snapshot=fake_fetch,
                                 now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        for _ in range(3):
            u = await asyncio.wait_for(out_q.get(), timeout=1.0)
            if u.bids == ((100.0, 1.5),):
                break
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return calls["c"]

    c = _run(scenario())
    assert c >= 2   # reconnected after bts:request_reconnect
