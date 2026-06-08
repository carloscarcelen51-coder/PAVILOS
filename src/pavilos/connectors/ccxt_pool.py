# src/pavilos/connectors/ccxt_pool.py
"""Parent-process bridge to the ccxt worker process. Presents as ONE Engine
connector but manages all ccxt venues in a child process, forwarding their
BookUpdates into the Engine's update queue and exposing per-venue health()."""
from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import queue as _queue
import threading

from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_worker import ccxt_worker_entry

_log = logging.getLogger(__name__)


class CcxtPoolConnector:
    """Engine-connector facade over a child process running the ccxt venues."""

    exchange = "ccxt-pool"

    def __init__(self, venue_symbols: dict, *, ctx=None, entry=ccxt_worker_entry,
                 join_grace_s: float = 5.0) -> None:
        self._venue_symbols = dict(venue_symbols)
        self._ctx = ctx if ctx is not None else mp.get_context("spawn")
        self._entry = entry
        self._join_grace_s = join_grace_s
        self._healths = {v: ConnectorHealth(v, False, 0.0, 0, 0) for v in venue_symbols}
        self._proc = None
        self._book_q = None
        self._health_q = None

    def healths(self) -> list:
        return [self._healths[v] for v in self._venue_symbols]

    async def run(self, out_q, stop) -> None:
        ctx = self._ctx
        self._book_q = ctx.Queue(maxsize=20000)
        self._health_q = ctx.Queue(maxsize=100)
        stop_evt = ctx.Event()
        self._proc = ctx.Process(target=self._entry,
                                 args=(self._book_q, self._health_q, stop_evt, self._venue_symbols),
                                 daemon=True)
        self._proc.start()

        loop = asyncio.get_running_loop()
        thread_stop = threading.Event()
        drain_thread = threading.Thread(target=_book_drain, name="ccxt-book-drain",
                                        args=(self._book_q, loop, out_q, thread_stop), daemon=True)
        drain_thread.start()
        health_task = asyncio.create_task(self._drain_health(stop))
        try:
            await stop.wait()
        finally:
            thread_stop.set()
            health_task.cancel()
            await asyncio.gather(health_task, return_exceptions=True)
            await loop.run_in_executor(None, drain_thread.join, 2.0)
            await self._shutdown(stop_evt)

    async def _drain_health(self, stop) -> None:
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            snap = await loop.run_in_executor(None, _get, self._health_q, 0.5)
            if snap:
                for h in snap:
                    if h.exchange in self._healths:
                        self._healths[h.exchange] = h

    async def _shutdown(self, stop_evt) -> None:
        proc = self._proc
        loop = asyncio.get_running_loop()
        try:
            if proc is not None:
                stop_evt.set()
                await loop.run_in_executor(None, proc.join, self._join_grace_s)
                if proc.is_alive():
                    _log.warning("ccxt worker did not exit in %.1fs; terminating", self._join_grace_s)
                    proc.terminate()
                    await loop.run_in_executor(None, proc.join, 2.0)
        finally:
            for q in (self._book_q, self._health_q):
                _close(q)
            # mark every venue disconnected (the child is gone)
            self._healths = {v: ConnectorHealth(v, False, h.last_update_ts, h.resyncs, h.errors)
                             for v, h in self._healths.items()}


def _book_drain(book_q, loop, out_q, thread_stop) -> None:
    """Blocking-get the child's BookUpdates and hand them to the asyncio loop."""
    while not thread_stop.is_set():
        try:
            u = book_q.get(timeout=0.5)
        except _queue.Empty:
            continue
        except (OSError, ValueError, EOFError):
            break    # queue closed
        try:
            loop.call_soon_threadsafe(out_q.put_nowait, u)
        except RuntimeError:
            break    # loop closed


def _get(q, timeout):
    try:
        return q.get(timeout=timeout)
    except _queue.Empty:
        return None
    except (OSError, ValueError, EOFError):
        return None


def _close(q) -> None:
    close = getattr(q, "close", None)
    if close is not None:
        try:
            close()
        except Exception:
            pass
