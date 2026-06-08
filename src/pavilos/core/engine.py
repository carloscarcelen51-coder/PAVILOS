# src/pavilos/core/engine.py
"""Engine: run connectors + the Aggregator concurrently and emit combined
snapshots. Connectors are injected (real ones in production, fakes in tests)."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot
from pavilos.aggregator.aggregator import Aggregator
from pavilos.connectors.base import ConnectorHealth


class Engine:
    """Composes connectors → one BookUpdate queue → Aggregator.run → snapshot
    queue. Each connector must expose ``exchange`` and ``async run(out_q, stop)``
    and optionally ``health()``."""

    def __init__(
        self,
        connectors: Sequence[object],
        aggregator: Aggregator,
        *,
        interval_s: float = 0.1,
        now: Callable[[], float] | None = None,
        on_update: Callable[[BookUpdate], None] | None = None,
    ) -> None:
        self._connectors = list(connectors)
        self._aggregator = aggregator
        self._interval_s = interval_s
        self._now = now or _wall_now
        self._on_update = on_update
        self._updates: "asyncio.Queue[BookUpdate]" = asyncio.Queue()
        self.snapshots: "asyncio.Queue[CombinedDepthSnapshot]" = asyncio.Queue()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._stop.clear()
        for c in self._connectors:
            self._tasks.append(asyncio.create_task(c.run(self._updates, self._stop)))
        self._tasks.append(asyncio.create_task(
            self._aggregator.run(self._updates, self.snapshots, interval_s=self._interval_s,
                                 now=self._now, stop=self._stop, on_update=self._on_update)
        ))

    async def stop(self, *, grace: float = 2.0) -> None:
        self._stop.set()
        if self._tasks:
            # Give tasks a grace period to exit cleanly, then force-cancel any
            # stragglers (e.g. a connector wedged in a dead connect/read) so
            # shutdown is bounded regardless of connector state.
            _, pending = await asyncio.wait(self._tasks, timeout=grace)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._tasks.clear()

    def health(self) -> list[ConnectorHealth]:
        return [c.health() for c in self._connectors if hasattr(c, "health")]


def _wall_now() -> float:
    import time
    return time.time()
