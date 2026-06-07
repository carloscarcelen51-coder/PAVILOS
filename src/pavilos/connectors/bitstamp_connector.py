# src/pavilos/connectors/bitstamp_connector.py
"""Async Bitstamp connector: REST seed + diff stream -> BookUpdates. Handles
bts:request_reconnect (forces resync). Transport injected."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable

import aiohttp
import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.bitstamp import BitstampDepthFeed

BITSTAMP_WS_URL = "wss://ws.bitstamp.net"
BITSTAMP_REST_URL = "https://www.bitstamp.net/api/v2/order_book"

_log = logging.getLogger(__name__)


class BitstampConnector:
    """Seeds from REST then applies WS diffs via ``BitstampDepthFeed``. A
    ``bts:request_reconnect`` control frame (or any error) forces reconnect +
    re-seed with stop-aware backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        url: str = BITSTAMP_WS_URL,
        rest_url: str = BITSTAMP_REST_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        fetch_snapshot: Callable[[], Awaitable[dict]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "bitstamp"
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
            feed = BitstampDepthFeed(self.symbol, exchange=self.exchange)
            stream = None
            try:
                stream = await self._connect()          # open first so diffs buffer
                self._connected = True
                snapshot = await self._fetch_snapshot()
                snap = feed.seed(snapshot, ts=self._now())
                await out_q.put(snap)
                self._last_update_ts = snap.ts
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("event") == "bts:request_reconnect":
                        raise ResyncRequired("bitstamp: server requested reconnect")
                    out = feed.apply(msg, ts=self._now())   # None if stale/control
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
                _log.exception("bitstamp connector error; will reconnect")
            finally:
                self._connected = False
                await _aclose(stream)
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

    async def _default_connect(self) -> AsyncIterator[dict]:
        ws = await websockets.connect(self._url, proxy=self._proxy) if self._proxy else await websockets.connect(self._url)
        await ws.send(json.dumps({"event": "bts:subscribe",
                                  "data": {"channel": f"diff_order_book_{self.symbol}"}}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()

        return gen()

    async def _default_fetch_snapshot(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._rest_url}/{self.symbol}/", proxy=self._proxy) as resp:
                resp.raise_for_status()
                return await resp.json()


async def _aclose(stream: object) -> None:
    """Best-effort close of an async-iterator stream (idempotent, never raises)."""
    aclose = getattr(stream, "aclose", None)
    if aclose is not None:
        try:
            await aclose()
        except Exception:
            pass


def _wall_now() -> float:
    import time
    return time.time()
