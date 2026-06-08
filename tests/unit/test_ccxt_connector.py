# tests/unit/test_ccxt_connector.py
import asyncio

from pavilos.connectors.ccxt_connector import CcxtConnector


class _FakeExchange:
    def __init__(self, books):
        self._books = list(books)
        self.closed = False

    async def watch_order_book(self, symbol):
        await asyncio.sleep(0)
        if self._books:
            return self._books.pop(0)
        await asyncio.Event().wait()

    async def close(self):
        self.closed = True


def test_emits_snapshots_from_ccxt_books():
    books = [{"bids": [[100.0, 1.0]], "asks": [[101.0, 2.0]], "nonce": 1},
             {"bids": [[100.0, 1.5]], "asks": [[101.0, 2.0]], "nonce": 2}]
    fake = _FakeExchange(books)

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = CcxtConnector("gate", "BTC/USDT", make_exchange=lambda: fake,
                             now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        task.cancel()  # blocked on the 3rd watch -> cancel to unwind
        await asyncio.gather(task, return_exceptions=True)
        return u1, u2, conn.health(), fake.closed

    u1, u2, health, closed = asyncio.run(scenario())
    assert u1.exchange == "gate" and u1.is_snapshot is True and u1.ts == 5.0
    assert u1.bids == ((100.0, 1.0),) and u1.asks == ((101.0, 2.0),)
    assert u2.bids == ((100.0, 1.5),) and u2.seq == 2
    assert health.exchange == "gate" and health.connected is False
    assert closed is True   # exchange closed in finally


def test_reconnects_and_counts_errors_on_watch_failure():
    calls = {"n": 0}

    class _Flaky:
        async def watch_order_book(self, symbol):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("ws down")
            await asyncio.sleep(0)
            return {"bids": [[100.0, 1.0]], "asks": [[101.0, 2.0]], "nonce": 9}
        async def close(self):
            pass

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = CcxtConnector("mexc", "BTC/USDT", make_exchange=lambda: _Flaky(),
                             now=lambda: 1.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)  # recovers on the 2nd exchange
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return u, conn.health()

    u, h = asyncio.run(scenario())
    assert u.is_snapshot is True and h.errors >= 1
