# src/pavilos/connectors/ccxt_worker.py
"""Child-process worker: runs the ccxt venue connectors in their OWN asyncio loop,
isolated from the parent's native feeds + detection + dashboard (whose periodic
synchronous bursts were starving the ccxt WS keepalives). BookUpdates and per-venue
ConnectorHealth are forwarded to the parent over multiprocessing queues.

Spawned with the 'spawn' start method, so ``ccxt_worker_entry`` and its arguments
must be picklable and this module must have NO import-time side effects."""
from __future__ import annotations

import asyncio
import logging
import queue as _queue

_log = logging.getLogger(__name__)

_HEALTH_INTERVAL_S = 1.0


def ccxt_worker_entry(book_q, health_q, stop_evt, venue_symbols) -> None:
    """Process entry point (top-level => picklable for spawn)."""
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(_worker_main(book_q, health_q, stop_evt, venue_symbols))
    except Exception:                       # a child must never die silently
        _log.exception("ccxt worker crashed")


async def _worker_main(book_q, health_q, stop_evt, venue_symbols, *,
                       connector_factory=None, health_interval_s: float = _HEALTH_INTERVAL_S) -> None:
    from pavilos.connectors.venues import build_connector
    factory = connector_factory or build_connector
    local_q: "asyncio.Queue" = asyncio.Queue()
    stop = asyncio.Event()
    conns = [factory(v, s) for v, s in venue_symbols.items()]
    tasks = [asyncio.create_task(c.run(local_q, stop)) for c in conns]
    tasks.append(asyncio.create_task(_watch_stop(stop_evt, stop)))
    tasks.append(asyncio.create_task(_forward_books(local_q, book_q, stop)))
    tasks.append(asyncio.create_task(_forward_health(conns, health_q, stop, health_interval_s)))
    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _watch_stop(stop_evt, stop) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        if await loop.run_in_executor(None, stop_evt.wait, 0.25):   # mp.Event.wait is blocking
            break
    stop.set()


async def _forward_books(local_q, book_q, stop) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        try:
            u = await asyncio.wait_for(local_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        # Offload the cross-process put to the default executor ON PURPOSE: even a
        # non-blocking mp.Queue.put_nowait can micro-stall the loop on the feeder
        # lock / pickling, and keeping the child's WS-keepalive loop unstalled is the
        # whole reason M9 exists. (Parent side avoids this differently, via
        # call_soon_threadsafe from a dedicated drain thread.) A dropped book update
        # here is safe because every ccxt BookUpdate is a full snapshot
        # (is_snapshot=True) — the next one fully replaces it, so we lose no delta.
        await loop.run_in_executor(None, _put_drop, book_q, u)


async def _forward_health(conns, health_q, stop, interval_s) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        snap = [c.health() for c in conns]
        await loop.run_in_executor(None, _put_drop, health_q, snap)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


def _put_drop(q, item) -> None:
    try:
        q.put_nowait(item)
    except _queue.Full:
        # Parent is behind; drop. Safe for both queues: health snaps are point-in-time
        # (a newer one supersedes), and book updates are full ccxt snapshots (the next
        # one fully replaces this one — see _forward_books — so no delta is lost).
        pass
