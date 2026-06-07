# src/pavilos/connectors/kraken_connector.py
"""Async Kraken v2 book connector: emits BookUpdates + verifies CRC32, with
reconnect/resync. Transport (`connect`) is injected for deterministic tests."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal

import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.kraken import parse_kraken_message
from pavilos.connectors.kraken_book import KrakenRawBook

import logging

_log = logging.getLogger(__name__)

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"


class KrakenConnector:
    """Streams Kraken ``book`` frames into ``BookUpdate``s on an output queue,
    verifying each frame's CRC32 against a full-precision local book. On a
    checksum mismatch or disconnect it reconnects (a fresh subscribe re-snapshots)
    with exponential backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        depth: int = 10,
        url: str = KRAKEN_WS_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "kraken"
        self._depth = depth
        self._url = url
        self._connect = connect or self._default_connect
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
            book = KrakenRawBook(self.symbol, depth=self._depth)
            try:
                stream = await self._connect()
                self._connected = True
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("channel") != "book":
                        continue
                    ts = self._now()
                    out = parse_kraken_message(msg, ts=ts, exchange=self.exchange)
                    book.apply(msg)
                    if not book.verify(int(msg["data"][0]["checksum"])):
                        self._resyncs += 1
                        raise ResyncRequired("kraken checksum mismatch")
                    await out_q.put(out)
                    self._last_update_ts = ts
                    backoff = 1.0
            except ResyncRequired:
                pass
            except Exception:
                self._errors += 1
                _log.exception("kraken connector error; will reconnect")
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
        ws = await websockets.connect(self._url, proxy=self._proxy) if self._proxy else await websockets.connect(self._url)
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "book", "symbol": [self.symbol], "depth": self._depth},
        }))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw, parse_float=Decimal)
            finally:
                await ws.close()

        return gen()


def _wall_now() -> float:
    import time
    return time.time()
