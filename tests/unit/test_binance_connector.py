# tests/unit/test_binance_connector.py
import asyncio

from pavilos.connectors.binance_connector import BinanceConnector


def _snapshot(last_update_id, bids, asks):
    return {"lastUpdateId": last_update_id, "bids": bids, "asks": asks}


def _event(U, u, bids, asks, E=1_000):
    return {"e": "depthUpdate", "E": E, "s": "BTCUSDT", "U": U, "u": u, "b": bids, "a": asks}


def _run(coro):
    return asyncio.run(coro)


def _empty_gen():
    async def gen():
        if False:        # yields nothing -> simulates a clean stream end
            yield {}
    return gen()


def test_seeds_then_applies_contiguous_events():
    events = [
        _event(U=101, u=105, bids=[["100.0", "1.5"]], asks=[], E=6_000),
        _event(U=106, u=108, bids=[], asks=[["101.0", "0"]], E=7_000),
    ]

    async def fake_connect():
        async def gen():
            for e in events:
                yield e
        return gen()

    async def fake_fetch():
        return _snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]])

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=fake_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap, u1, u2

    snap, u1, u2 = _run(scenario())
    assert snap.is_snapshot is True and snap.seq == 100 and snap.exchange == "binance"
    assert u1.is_snapshot is False and u1.seq == 105
    assert u2.seq == 108


def test_gap_triggers_reseed():
    sessions = [
        [_event(U=200, u=201, bids=[["100.0", "9.0"]], asks=[], E=6_000)],  # U=200 > 100+1 -> gap
        [_event(U=301, u=302, bids=[["100.0", "2.0"]], asks=[], E=8_000)],
    ]
    snaps = [_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]),
             _snapshot(300, [["100.0", "1.0"]], [["101.0", "2.0"]])]
    calls = {"c": 0, "s": 0}

    async def fake_connect():
        i = calls["c"]; calls["c"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for e in session:
                yield e
        return gen()

    async def fake_fetch():
        i = calls["s"]; calls["s"] += 1
        return snaps[min(i, len(snaps) - 1)]

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=fake_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        seqs = []
        for _ in range(4):
            u = await asyncio.wait_for(out_q.get(), timeout=1.0)
            seqs.append(u.seq)
            if u.seq == 302:
                break
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return seqs, calls["c"]

    seqs, c = _run(scenario())
    assert 100 in seqs and 300 in seqs and 302 in seqs   # reseeded and resumed
    assert c >= 2


def test_clean_stream_end_triggers_reseed():
    snaps = [_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]),
             _snapshot(200, [["100.0", "1.0"]], [["101.0", "2.0"]])]
    calls = {"c": 0, "s": 0}

    async def fake_connect():
        calls["c"] += 1
        return _empty_gen()      # no events -> clean end -> reconnect + reseed

    async def fake_fetch():
        i = calls["s"]; calls["s"] += 1
        return snaps[min(i, len(snaps) - 1)]

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=fake_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        s1 = await asyncio.wait_for(out_q.get(), timeout=1.0)   # seed1
        s2 = await asyncio.wait_for(out_q.get(), timeout=1.0)   # after clean end -> reseed
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return s1, s2, calls["c"]

    s1, s2, c = _run(scenario())
    assert s1.seq == 100 and s2.seq == 200
    assert c >= 2


def test_connect_failure_increments_errors():
    calls = {"c": 0}

    async def failing_connect():
        calls["c"] += 1
        if calls["c"] == 1:
            raise OSError("boom")     # first connect fails -> counted as error
        return _empty_gen()

    async def fake_fetch():
        return _snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]])

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=failing_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        s = await asyncio.wait_for(out_q.get(), timeout=1.0)   # recovers on 2nd connect -> seed
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return s, conn.health()

    s, h = _run(scenario())
    assert s.seq == 100
    assert h.errors >= 1
