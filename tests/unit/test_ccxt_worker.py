# tests/unit/test_ccxt_worker.py
import asyncio
import queue
import threading

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_worker import _worker_main


class _FakeConn:
    """Module-of-test fake: emits scripted BookUpdates then idles; health() static."""
    def __init__(self, exchange):
        self.exchange = exchange
        self._n = 0

    async def run(self, out_q, stop):
        for i in range(3):
            if stop.is_set():
                return
            await out_q.put(BookUpdate(exchange=self.exchange, ts=float(i),
                                       bids=((100.0, 1.0),), asks=((101.0, 1.0),),
                                       is_snapshot=True, seq=i))
            self._n += 1
            await asyncio.sleep(0)
        await asyncio.Event().wait()

    def health(self):
        return ConnectorHealth(self.exchange, True, float(self._n), 0, 0)


def _factory(venue, symbol):
    return _FakeConn(venue)


def test_worker_forwards_books_and_health_then_stops():
    book_q: queue.Queue = queue.Queue()
    health_q: queue.Queue = queue.Queue()
    stop_evt = threading.Event()

    async def scenario():
        task = asyncio.create_task(_worker_main(
            book_q, health_q, stop_evt, {"gate": "BTC/USDT", "mexc": "BTC/USDT"},
            connector_factory=_factory, health_interval_s=0.01))
        # collect a few forwarded updates
        deadline = 0
        seen = []
        while len(seen) < 4 and deadline < 200:
            try:
                seen.append(book_q.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01); deadline += 1
        stop_evt.set()
        await asyncio.wait_for(task, timeout=2.0)
        return seen

    seen = asyncio.run(scenario())
    assert {u.exchange for u in seen} == {"gate", "mexc"}
    assert all(isinstance(u, BookUpdate) and u.is_snapshot for u in seen)
    # a health snapshot (list of ConnectorHealth) was forwarded
    drained = []
    while True:
        try:
            drained.append(health_q.get_nowait())
        except queue.Empty:
            break
    assert drained and all(isinstance(h, ConnectorHealth) for h in drained[-1])
