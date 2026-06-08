# src/pavilos/connectors/ccxt_connector.py
"""Long-tail venue connector via ccxt.pro. watch_order_book returns the venue's
full maintained book each WS update -> emitted as a snapshot BookUpdate, exactly
like the native connectors. ccxt owns the sequencing/resync internally. The
exchange factory is injected so the run-loop is unit-testable without network."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth

_log = logging.getLogger(__name__)


class CcxtConnector:
    """Streams a ccxt.pro exchange's order book into ``BookUpdate`` snapshots.
    ``make_exchange`` builds an object exposing ``async watch_order_book(symbol)``
    (-> ``{"bids": [[p, a], ...], "asks": [...], "nonce": int|None}``) and
    ``async close()``. Any error reconnects (a fresh exchange) with stop-aware
    backoff. ``exchange`` is the ccxt id, used as the venue name."""

    def __init__(self, exchange_id: str, symbol: str, *,
                 make_exchange: Callable[[], object] | None = None,
                 now: Callable[[], float] | None = None,
                 sleep: Callable[[float], Awaitable[None]] | None = None,
                 max_backoff: float = 30.0, startup_delay: float = 0.0,
                 ccxt_options: dict | None = None) -> None:
        self.exchange = exchange_id
        self.symbol = symbol
        self._make_exchange = make_exchange or self._default_make_exchange
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._startup_delay = startup_delay   # stagger concurrent ccxt connects to avoid a load_markets storm
        self._ccxt_options = ccxt_options or {}   # per-venue ccxt config (e.g. bitget checksum off)
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts,
                               self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        if self._startup_delay > 0 and not stop.is_set():
            try:                                  # stagger startup (stop-aware) so the ccxt
                await asyncio.wait_for(stop.wait(), timeout=self._startup_delay)  # venues don't all
            except asyncio.TimeoutError:          # load_markets + WS-connect at the same instant
                pass
        backoff = 1.0
        while not stop.is_set():
            ex = None
            try:
                ex = self._make_exchange()
                self._connected = True
                while not stop.is_set():
                    ob = await ex.watch_order_book(self.symbol)
                    if stop.is_set():
                        break
                    out = BookUpdate(
                        exchange=self.exchange, ts=self._now(),
                        bids=tuple((float(p), float(a)) for p, a in ob.get("bids", [])),
                        asks=tuple((float(p), float(a)) for p, a in ob.get("asks", [])),
                        is_snapshot=True, seq=ob.get("nonce"))
                    await out_q.put(out)
                    self._last_update_ts = out.ts
                    backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                self._errors += 1
                _log.exception("ccxt connector %s error; will reconnect", self.exchange)
            finally:
                self._connected = False
                await _close_exchange(ex)
            if stop.is_set():
                break
            delay = min(backoff, self._max_backoff)
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
            else:
                await self._sleep(0)
            backoff = min(backoff * 2, self._max_backoff) if self._max_backoff else 0.0

    def _default_make_exchange(self) -> object:
        import ccxt.pro  # imported lazily so unit tests never need ccxt
        import aiohttp
        ex = getattr(ccxt.pro, self.exchange)({"enableRateLimit": True, **self._ccxt_options})
        # aiodns (aiohttp's default async resolver) is unreliable on this host
        # ("Could not contact DNS servers"); give ccxt an aiohttp session using the
        # stdlib ThreadedResolver so its REST (load_markets) + WS DNS both resolve.
        ex.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver()))
        return ex


async def _close_exchange(ex: object) -> None:
    close = getattr(ex, "close", None)
    if close is not None:
        try:
            await close()
        except Exception:
            pass


def _wall_now() -> float:
    import time
    return time.time()
