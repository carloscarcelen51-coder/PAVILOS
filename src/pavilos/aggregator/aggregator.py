# src/pavilos/aggregator/aggregator.py
"""Owns per-exchange BookStates and produces combined snapshots."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot, VenueSpec
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.combine import build_combined
from pavilos.aggregator.normalize import PegProvider


class Aggregator:
    """Routes ``BookUpdate``s to per-exchange ``BookState``s and, on demand,
    builds a combined snapshot from the venues that are fresh (not stale)."""

    def __init__(
        self,
        specs: Sequence[VenueSpec],
        peg: PegProvider,
        *,
        bin_bps: float,
        window_bps: float,
        staleness_s: float,
    ) -> None:
        self._specs = {s.exchange: s for s in specs}
        self._books = {s.exchange: BookState(s.exchange) for s in specs}
        self._peg = peg
        self._bin_bps = bin_bps
        self._window_bps = window_bps
        self._staleness_s = staleness_s

    def apply(self, u: BookUpdate) -> None:
        book = self._books.get(u.exchange)
        if book is None:
            # Fail loud: a misrouted feed is a config/connector bug, not a new venue.
            raise KeyError(
                f"unknown exchange {u.exchange!r}; configured: {sorted(self._books)}"
            )
        book.apply(u)

    def active(self, now: float) -> set[str]:
        return {
            ex
            for ex, book in self._books.items()
            if book.last_ts > 0.0 and (now - book.last_ts) <= self._staleness_s and book.mid() is not None
        }

    def snapshot(self, now: float) -> CombinedDepthSnapshot | None:
        return build_combined(
            self._books,
            self._specs,
            self._peg,
            bin_bps=self._bin_bps,
            window_bps=self._window_bps,
            ts=now,
            active=self.active(now),
        )

    async def run(
        self,
        in_q: "asyncio.Queue[BookUpdate]",
        out_q: "asyncio.Queue[CombinedDepthSnapshot]",
        *,
        interval_s: float,
        now: Callable[[], float],
        stop: "asyncio.Event",
    ) -> None:
        """Drain ``in_q`` into the books and emit a snapshot every ``interval_s``.

        Drains all immediately-available updates each tick, then emits one
        combined snapshot (if any Tier-A venue is fresh). Exits when ``stop``
        is set. ``now`` is injected for deterministic testing.
        """
        while not stop.is_set():
            # Drain everything currently queued without blocking.
            while True:
                try:
                    self.apply(in_q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            snap = self.snapshot(now())
            if snap is not None:
                await out_q.put(snap)
            if interval_s > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(0)  # yield control
