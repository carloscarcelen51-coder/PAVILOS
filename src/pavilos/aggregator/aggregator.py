# src/pavilos/aggregator/aggregator.py
"""Owns per-exchange BookStates and produces combined snapshots."""
from __future__ import annotations

import asyncio
import math
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

    def reset(self) -> None:
        """Forget every book (a replay crossing a recorder restart: live started empty)."""
        self._books = {ex: BookState(ex) for ex in self._books}

    def active(self, now: float) -> set[str]:
        return {
            ex
            for ex, book in self._books.items()
            if book.last_ts > 0.0 and (now - book.last_ts) <= self._staleness_s and book.mid() is not None
        }

    def checkpoint_updates(self, now: float) -> list[BookUpdate]:
        """A full copy of every ACTIVE venue's live book, for the recorder only (never
        applied here). ``ts = book.last_ts`` -- not ``now`` -- so in a replay it sorts
        right after the last update it already contains (the recorder never lets a
        venue's ts go backwards, and seq_no breaks ties in arrival order) and does not
        fake the venue's freshness."""
        out = []
        for ex in sorted(self.active(now)):
            book = self._books[ex]
            out.append(BookUpdate(ex, book.last_ts, tuple(book.bids().items()),
                                  tuple(book.asks().items()), True, None, checkpoint=True))
        return out

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
        on_update: Callable[[BookUpdate], None] | None = None,
        checkpoint_every_s: float | None = None,
    ) -> None:
        """Drain ``in_q`` into the books and emit a snapshot every ``interval_s``.

        Drains all immediately-available updates each tick, then emits one
        combined snapshot (if any Tier-A venue is fresh). Exits when ``stop``
        is set. ``now`` is injected for deterministic testing.

        With ``on_update`` and ``checkpoint_every_s`` set, every
        ``checkpoint_every_s`` it also hands ``on_update`` a full copy of each
        active venue's live book (``checkpoint_updates``), AFTER the drain so the
        copy reflects every update already passed to ``on_update``. Checkpoints
        are never applied to the books.

        The checkpoint schedule follows ``now``; if that clock jumps BACK by more
        than a period, the schedule re-arms instead of going silent until the clock
        catches up. ``checkpoint_every_s`` must be finite and > 0 (or None).

        Pass a POSITIVE ``interval_s`` in production: ``interval_s == 0``
        busy-spins (yields each tick, emitting as fast as the scheduler allows)
        and is intended only for tests. The caller owns ``out_q`` sizing: this
        uses ``await out_q.put(...)``, so passing a bounded queue yields natural
        back-pressure, while an unbounded queue with a slow consumer can grow
        without limit.
        """
        check_checkpoint_every(checkpoint_every_s)
        ck_due: float | None = None
        while not stop.is_set():
            # Drain everything currently queued without blocking.
            while True:
                try:
                    u = in_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self.apply(u)
                if on_update is not None:
                    on_update(u)
            if on_update is not None and checkpoint_every_s:
                t = now()
                if ck_due is None or t < ck_due - checkpoint_every_s:
                    ck_due = t + checkpoint_every_s           # first tick, or the clock went back
                elif t >= ck_due:
                    for ck in self.checkpoint_updates(t):
                        on_update(ck)
                    ck_due = t + checkpoint_every_s
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


def check_checkpoint_every(every: float | None) -> None:
    """Raise unless ``every`` is None or a finite number > 0."""
    if every is not None and not (math.isfinite(every) and every > 0):
        raise ValueError(f"checkpoint_every_s must be finite and > 0, got {every!r}")
