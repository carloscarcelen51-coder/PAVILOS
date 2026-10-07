# src/pavilos/persistence/encoder.py
"""Compact, LOSSLESS encoding of the BookUpdate stream before it is persisted.

ccxt venues deliver the WHOLE maintained book on every message (kucoin: ~16k levels
x ~5/s); persisting each one verbatim made those venues ~97% of the lake. The
encoder keeps a per-venue mirror -- normalized exactly like ``BookState`` -- and
writes only what a replay needs to reproduce the live book:

- checkpoint (``u.checkpoint``)  -> the full book (a replay base for every ACTIVE
  venue, and it heals any update the recorder had to drop);
- snapshot with no mirror yet    -> the full book;
- snapshot with a mirror         -> the difference: new/changed levels at their
  absolute size, vanished levels at 0.0;
- delta                          -> passed through, normalized (non-finite levels
  dropped, a price repeated within the update -> last one wins, size <= 0 -> 0.0);
- anything that would be empty   -> a one-level no-op heartbeat (best level at its
  CURRENT size) so the replay refreshes ``last_ts`` exactly like the live book did.

Three rules make the replay ORDER and BASE match the live book:

- the written ``ts`` never goes backwards within a venue, nor below ``set_floor``
  (the run's start, after every row an earlier run wrote): the replay sorts by ts and
  a PC clock can step back. Ties keep arrival order through ``seq_no``;
- a venue's FIRST write and its first write after more than ``rebase_after_s`` of
  silence (a reconnect, a stall) is the WHOLE book from the mirror. The aggregator
  only checkpoints ACTIVE venues, so this is what gives a replay warm-up a base for a
  venue that was inactive at the last checkpoints;
- after ``rebase()`` -- the recorder calls it when a write failed, a batch failed or a
  pause ended: what was encoded never reached the lake -- the venue's next write is
  its whole book plus a LOSS MARK, one level at a non-finite price that ``BookState``
  ignores. Even an EMPTY book is then written (as the mark alone): the lake may still
  hold levels the lost writes would have removed. ``lake_check`` always compares a
  marked book, so the loss is reported.

Invariant (tested): applying the written updates to a fresh ``BookState`` yields the
live ``BookState`` after every update -- levels always; ``last_ts`` whenever the
book has a mid (the venue's highest ts so far, i.e. the live one while ts only moves
forward). Every written side lists each price once, so the order in which a group's
rows come back from the lake cannot matter."""
from __future__ import annotations

import logging
import math
from collections.abc import Iterable

from pavilos.core.models import BookUpdate

_log = logging.getLogger(__name__)

_Book = dict[float, float]
_LOSS_MARK = (math.nan, 0.0)       # a level BookState drops: "this whole book replaces a lost span"
_CLAMP_WARN_S = 2.0
_CLAMP_CRITICAL_S = 60.0


def _snapshot_levels(levels: Iterable[tuple[float, float]]) -> _Book:
    # Identical to BookState's snapshot comprehension (non-positive entries are SKIPPED,
    # not treated as removals), so the mirror can never drift from the live book.
    return {p: s for p, s in levels if s > 0 and math.isfinite(p) and math.isfinite(s)}


def _delta_levels(levels: Iterable[tuple[float, float]]) -> _Book:
    # Sequential last-wins, exactly how BookState applies a delta level by level.
    out: _Book = {}
    for p, s in levels:
        if not (math.isfinite(p) and math.isfinite(s)):
            continue
        out[p] = s if s > 0 else 0.0
    return out


def _diff(old: _Book, new: _Book) -> tuple[tuple[float, float], ...]:
    changed = [(p, s) for p, s in new.items() if old.get(p) != s]
    removed = [(p, 0.0) for p in old if p not in new]
    return tuple(changed + removed)


class BookEncoder:
    """Per-venue mirror producing the compact write stream (see module doc)."""

    def __init__(self, *, rebase_after_s: float = 15.0) -> None:
        self._mirror: dict[str, tuple[_Book, _Book]] = {}
        self._last_ts: dict[str, float] = {}         # last ENCODED ts per venue
        self._needs_rebase: set[str] = set()
        self._clamped: set[str] = set()
        self._clamped_critical: set[str] = set()
        self._lead: dict[str, float] = {}
        self.clamp_lead_s = 0.0      # largest current lead of written ts over a venue's own ts
        self._floor: float | None = None
        self._rebase_after_s = rebase_after_s

    def set_floor(self, ts: float) -> None:
        """No written ts below ``ts``: the recorder passes its run's start, which is
        after every row an earlier run wrote, so the clamp holds across a restart."""
        self._floor = ts

    def rebase(self, exchange: str | None = None) -> None:
        """Make the next write of ``exchange`` (of every venue if None) its whole book
        with a loss mark: what was encoded for it did not reach the lake."""
        if exchange is None:
            self._needs_rebase.update(self._mirror)
        else:
            self._needs_rebase.add(exchange)

    def encode(self, u: BookUpdate) -> BookUpdate | None:
        last = self._last_ts.get(u.exchange)
        lo = last if self._floor is None else (self._floor if last is None else max(last, self._floor))
        ts = u.ts if lo is None or u.ts >= lo else lo
        self._watch_clamp(u.exchange, lo, u.ts)
        w = self._encode(u, ts)
        lost = u.exchange in self._needs_rebase
        if lost or (w is not None and not w.is_snapshot
                    and (last is None or ts - last > self._rebase_after_s)):
            bids, asks = self._mirror.get(u.exchange, ({}, {}))
            if bids or asks or lost:                  # an empty mirror keeps its removals
                mark = (_LOSS_MARK,) if lost else ()
                w = BookUpdate(u.exchange, ts, tuple(bids.items()) + mark,
                               tuple(asks.items()), True, u.seq)
        if w is None:
            return None
        if w.is_snapshot:
            self._needs_rebase.discard(u.exchange)
        self._last_ts[u.exchange] = ts
        return w

    def _watch_clamp(self, exchange: str, lo: float | None, ts: float) -> None:
        """WARNING once per episode when a venue's ts falls > 2 s behind what is already
        written (CRITICAL past a minute: the run's timeline is being compressed), and
        keep ``clamp_lead_s`` for the dashboard."""
        lead = lo - ts if lo is not None and ts < lo else 0.0
        if lead > _CLAMP_WARN_S:
            critical = lead > _CLAMP_CRITICAL_S
            if exchange not in self._clamped or (critical and exchange not in self._clamped_critical):
                self._clamped.add(exchange)
                if critical:
                    self._clamped_critical.add(exchange)
                _log.log(logging.CRITICAL if critical else logging.WARNING,
                         "encoder: %s ts is %.1f s behind what is already written (PC clock "
                         "stepped back?); writing ts=%.3f until it catches up", exchange, lead, lo)
        elif lead == 0.0:
            self._clamped.discard(exchange)
            self._clamped_critical.discard(exchange)
        if lead:
            self._lead[exchange] = lead
            self.clamp_lead_s = max(self._lead.values())
        elif exchange in self._lead:
            del self._lead[exchange]
            self.clamp_lead_s = max(self._lead.values(), default=0.0)

    def _encode(self, u: BookUpdate, ts: float) -> BookUpdate | None:
        if u.is_snapshot:
            bids, asks = _snapshot_levels(u.bids), _snapshot_levels(u.asks)
            prev = self._mirror.get(u.exchange)
            self._mirror[u.exchange] = (bids, asks)
            if u.checkpoint or prev is None:
                return _full(u, ts, bids, asks, prev)
            db, da = _diff(prev[0], bids), _diff(prev[1], asks)
            if db or da:
                return BookUpdate(u.exchange, ts, db, da, False, u.seq)
            return _heartbeat(u, ts, bids, asks)
        bids_m, asks_m = self._mirror.setdefault(u.exchange, ({}, {}))
        db, da = _delta_levels(u.bids), _delta_levels(u.asks)
        for book, changes in ((bids_m, db), (asks_m, da)):
            for p, s in changes.items():
                if s > 0:
                    book[p] = s
                else:
                    book.pop(p, None)
        if db or da:
            return BookUpdate(u.exchange, ts, tuple(db.items()), tuple(da.items()), False, u.seq)
        return _heartbeat(u, ts, bids_m, asks_m)


def _full(u: BookUpdate, ts: float, bids: _Book, asks: _Book,
          prev: tuple[_Book, _Book] | None) -> BookUpdate | None:
    if bids or asks:
        return BookUpdate(u.exchange, ts, tuple(bids.items()), tuple(asks.items()), True, u.seq)
    if prev is not None and (prev[0] or prev[1]):
        # A zero-level snapshot would leave NO rows, so the replay would never see the
        # reset; write it as the removal of every previously written level instead.
        return BookUpdate(u.exchange, ts, tuple((p, 0.0) for p in prev[0]),
                          tuple((p, 0.0) for p in prev[1]), False, u.seq)
    return None


def _heartbeat(u: BookUpdate, ts: float, bids: _Book, asks: _Book) -> BookUpdate | None:
    """One no-op level (best bid, else best ask) at its CURRENT size: refreshes the
    replay's ``last_ts`` without changing the book. ``None`` if the book is empty
    (an empty book has no mid, so its freshness is irrelevant)."""
    if bids:
        p = max(bids)
        return BookUpdate(u.exchange, ts, ((p, bids[p]),), (), False, u.seq)
    if asks:
        p = min(asks)
        return BookUpdate(u.exchange, ts, (), ((p, asks[p]),), False, u.seq)
    return None
