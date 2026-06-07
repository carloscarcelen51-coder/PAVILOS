# src/pavilos/connectors/binance_connector.py
"""Async Binance spot depth connector: REST seed + diff stream -> BookUpdates,
with reconnect/re-seed on gap. Transport (`connect`/`fetch_snapshot`) injected."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable

import aiohttp
import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.binance import BinanceDepthFeed

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_REST_URL = "https://api.binance.com/api/v3/depth"

_log = logging.getLogger(__name__)


class BinanceConnector:
    """Streams Binance spot depth into ``BookUpdate``s: opens the diff stream,
    seeds from REST, then applies diffs via ``BinanceDepthFeed``. A gap raises
    ``ResyncRequired`` and the loop reconnects + re-seeds with backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        url: str = BINANCE_WS_URL,
        rest_url: str = BINANCE_REST_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        fetch_snapshot: Callable[[], Awaitable[dict]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "binance"
        self._url = url
        self._rest_url = rest_url
        self._connect = connect or self._default_connect
        self._fetch_snapshot = fetch_snapshot or self._default_fetch_snapshot
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._proxy = proxy
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts, self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            feed = BinanceDepthFeed(self.symbol, exchange=self.exchange)
            try:
                stream = await self._connect()          # open first so events buffer
                self._connected = True
                snapshot = await self._fetch_snapshot()
                snap = feed.seed(snapshot, ts=self._now())
                await out_q.put(snap)
                self._last_update_ts = snap.ts
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("e") != "depthUpdate":
                        continue
                    out = feed.apply(msg)               # None if stale; raises on gap
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
                _log.exception("binance connector error; will reconnect")
            finally:
                self._connected = False
            if stop.is_set():
                break
            delay = min(backoff, self._max_backoff)
            if delay > 0:
                # Stop-aware backoff: wake immediately if stop is set during the wait.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
            else:
                await self._sleep(0)  # yield (test path / no backoff)
            backoff = min(backoff * 2, self._max_backoff) if self._max_backoff else 0.0

    async def _default_connect(self) -> AsyncIterator[dict]:
        stream_url = f"{self._url}/{self.symbol.lower()}@depth@100ms"
        ws = await websockets.connect(stream_url, proxy=self._proxy, max_size=None) if self._proxy else await websockets.connect(stream_url, max_size=None)

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()

        return gen()

    async def _default_fetch_snapshot(self) -> dict:
        # Binance REST /api/v3/depth requires an UPPERCASE symbol (rejects
        # lowercase with -1121); the WS stream path uses lowercase. Normalize.
        params = {"symbol": self.symbol.upper(), "limit": 5000}
        async with aiohttp.ClientSession() as session:
            async with session.get(self._rest_url, params=params, proxy=self._proxy) as resp:
                resp.raise_for_status()
                return await resp.json()


def _wall_now() -> float:
    import time
    return time.time()
