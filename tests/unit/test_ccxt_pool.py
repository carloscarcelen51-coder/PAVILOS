# tests/unit/test_ccxt_pool.py
import asyncio
import queue
import threading

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_pool import CcxtPoolConnector


class _FakeProc:
    def __init__(self, *a, **k):
        self._alive = True
        self.started = False
        self.terminated = False
    def start(self): self.started = True
    def is_alive(self): return self._alive
    def die(self): self._alive = False          # simulate the child crashing mid-run
    def join(self, timeout=None): self._alive = False
    def terminate(self): self.terminated = True; self._alive = False


class _FakeCtx:
    def __init__(self): self.procs = []
    def Queue(self, maxsize=0): return queue.Queue(maxsize)
    def Event(self): return threading.Event()
    def Process(self, *a, **k):
        p = _FakeProc(*a, **k); self.procs.append(p); return p


def test_pool_forwards_books_updates_health_and_shuts_down():
    ctx = _FakeCtx()
    pool = CcxtPoolConnector({"gate": "BTC/USDT", "mexc": "BTC/USDT"},
                             ctx=ctx, entry=lambda *a, **k: None, join_grace_s=0.5)
    # before run: all disconnected
    assert {h.exchange for h in pool.healths()} == {"gate", "mexc"}
    assert all(not h.connected for h in pool.healths())

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(out_q, stop))
        await asyncio.sleep(0.05)
        # the pool created its queues on the fake ctx; feed through them directly:
        pool._book_q.put(BookUpdate(exchange="gate", ts=1.0, bids=((1.0, 1.0),),
                                    asks=((2.0, 1.0),), is_snapshot=True, seq=1))
        pool._health_q.put([ConnectorHealth("gate", True, 9.0, 0, 0)])
        u = await asyncio.wait_for(out_q.get(), timeout=2.0)
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
        return u

    u = asyncio.run(scenario())
    assert u.exchange == "gate" and u.is_snapshot
    assert any(h.exchange == "gate" and h.connected for h in pool.healths()) is False  # marked disconnected on shutdown
    assert ctx.procs[0].started is True


def test_pool_marks_disconnected_when_worker_dies_midrun():
    """If the child process dies before stop is set, healths() must flip every
    venue to connected=False (no frozen green light) without waiting for stop."""
    ctx = _FakeCtx()
    pool = CcxtPoolConnector({"gate": "BTC/USDT", "mexc": "BTC/USDT"},
                             ctx=ctx, entry=lambda *a, **k: None, join_grace_s=0.5)

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(out_q, stop))
        await asyncio.sleep(0.05)
        # worker reports healthy, then the child dies mid-run (stop NOT set)
        pool._health_q.put([ConnectorHealth("gate", True, 9.0, 0, 0),
                            ConnectorHealth("mexc", True, 9.0, 0, 0)])
        await asyncio.sleep(0.1)
        assert all(h.connected for h in pool.healths())  # green while alive
        ctx.procs[0].die()
        # liveness watcher should flip everything to disconnected without us setting stop
        for _ in range(40):
            await asyncio.sleep(0.05)
            if all(not h.connected for h in pool.healths()):
                break
        flipped = all(not h.connected for h in pool.healths())
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
        return flipped

    assert asyncio.run(scenario()) is True
