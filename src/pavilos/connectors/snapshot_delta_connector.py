# src/pavilos/connectors/snapshot_delta_connector.py
"""Generic async connector for full-snapshot-WS + deltas venues (Coinbase, OKX,
Bybit). Parameterized by a per-venue feed; transport (`connect`) injected."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired

_log = logging.getLogger(__name__)


async def _aclose(stream: object) -> None:
    """Best-effort close of an async-iterator stream (idempotent, never raises)."""
    aclose = getattr(stream, "aclose", None)
    if aclose is not None:
        try:
            await aclose()
        except Exception:
            pass


class SnapshotDeltaConnector:
    """Streams a venue's snapshot+delta frames through a per-venue feed into
    ``BookUpdate``s. ``make_feed`` is a zero-arg factory; a FRESH feed is created
    per connection (so resync starts clean). ``connect`` returns a live async
    iterator of decoded dict frames."""

    def __init__(
        self,
        exchange: str,
        make_feed: Callable[[], object],
        *,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]],
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
    ) -> None:
        self.exchange = exchange
        self._make_feed = make_feed
        self._connect = connect
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts, self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            stream = None
            try:
                feed = self._make_feed()
                stream = await self._connect()
                self._connected = True
                async for msg in stream:
                    if stop.is_set():
                        break
                    out = feed.process(msg, ts=self._now())  # None=skip; raises ResyncRequired on gap
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
                _log.exception("%s connector error; will reconnect", self.exchange)
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


def _wall_now() -> float:
    import time
    return time.time()
