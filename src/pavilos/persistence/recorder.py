# src/pavilos/persistence/recorder.py
"""Tap the BookUpdate stream and persist it as raw L2 rows via a ParquetSink.

record() does ONE O(1) queue put (safe to call from the event loop). A dedicated
writer thread drains the queue, passes every update through a ``BookEncoder``
(lossless: repeated full books become differences, checkpoints are kept whole --
see persistence/encoder.py), expands what must be written into per-level rows
(assigning a monotonic seq_no per exchange so an update's levels stay groupable for
replay), and hands batches to the sink. Backpressure = drop-and-count (never blocks
ingest); drops are LOGGED, and the next aggregator checkpoint heals the book.
``set_paused`` (the retention janitor's disk floor) keeps ENCODING but writes
nothing, so the mirror stays the live book; when the pause ends, and when a write
fails, the affected venues' next write is their whole book (``BookEncoder.rebase``),
so the lake is exact again at once. Write failures are counted, logged at CRITICAL
when they start and once they have stopped for a full stats interval (the level that
also reaches the supervisor's log; intermittent failures do not flap) and shown on
the dashboard through ``status()``."""
from __future__ import annotations

import logging
import queue
import threading
import time

from pavilos.core.models import BookUpdate
from pavilos.persistence.encoder import BookEncoder

_log = logging.getLogger(__name__)

_SENTINEL = object()    # poison-pill to wake the writer's blocking get() on stop()


class BookRecorder:
    def __init__(self, sink, *, flush_interval_s: float = 5.0, max_queue: int = 200_000,
                 stats_interval_s: float = 60.0, rebase_after_s: float = 15.0) -> None:
        self._sink = sink
        self._flush_interval_s = flush_interval_s
        self._q: "queue.Queue[BookUpdate]" = queue.Queue(maxsize=max_queue)
        self._seq: dict[str, int] = {}
        self._encoder = BookEncoder(rebase_after_s=rebase_after_s)
        self.dropped = 0
        self._dropped_logged = 0
        self._dropped_log_t: float | None = None
        self._last_drop_mono: float | None = None
        self.paused_skipped = 0
        self._paused = False
        self._writer_paused = False              # the writer thread's view (rebase on resume)
        self._paused_skipped_at_pause = 0
        self.write_errors = 0
        self._writing_ok = True                  # writer thread: CRITICAL on each transition
        self._last_write_error_mono: float | None = None
        self._error_log_t: float | None = None   # rate limit for the per-failure ERROR lines
        self._stats_interval_s = stats_interval_s
        self._stats = {"received": 0, "written": 0, "rows": 0}
        self._stats_t0 = time.monotonic()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def record(self, update: BookUpdate) -> None:
        try:
            self._q.put_nowait(update)          # O(1); safe from the event loop
        except queue.Full:
            self.dropped += 1                   # writer behind -> drop (never block ingest)
            self._last_drop_mono = time.monotonic()

    @property
    def paused(self) -> bool:
        return self._paused

    def set_ts_floor(self, ts: float) -> None:
        """No row of this run below ``ts`` (its start in ``_runs.jsonl``). Call before
        ``start()``."""
        self._encoder.set_floor(ts)

    def status(self, *, recent_s: float = 600.0) -> dict:
        """For the dashboard: paused or not, the totals lost, whether drops or write
        failures happened in the last ``recent_s`` (checkpoints heal drops), and how far
        the written ts runs ahead of a venue's clock (> 0 after a PC clock step back)."""
        now = time.monotonic()

        def recent(t: float | None) -> bool:
            return t is not None and now - t < recent_s

        return {"paused": self._paused, "dropped": self.dropped,
                "paused_skipped": self.paused_skipped, "write_errors": self.write_errors,
                "dropped_recently": recent(self._last_drop_mono),
                "write_errors_recently": recent(self._last_write_error_mono),
                "clock_behind_s": round(self._encoder.clamp_lead_s, 3)}

    def set_paused(self, paused: bool) -> None:
        """Stop / resume persisting (the retention janitor pauses while the drive is
        below its free-space floor). Idempotent; transitions are logged. While paused
        the writer still encodes every update and writes nothing; after the resume each
        venue's next write is its whole book, so the lake is exact again at once."""
        if paused == self._paused:
            return
        # CRITICAL on purpose (both ways): it is the only level that also reaches the
        # supervisor's log, where an operator who never opens logs/pavilos.log looks.
        if paused:
            self._paused_skipped_at_pause = self.paused_skipped
            self._paused = True
            _log.critical("book recorder PAUSED (disk floor): updates are not persisted until resumed")
        else:
            self._paused = False
            _log.critical("book recorder RESUMED after skipping %d updates; each venue's next "
                          "write is its whole book", self.paused_skipped - self._paused_skipped_at_pause)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return                               # idempotent: no duplicate writer thread
        self._stop.clear()                       # allow a clean restart after stop()
        self._thread = threading.Thread(target=self._run, name="book-recorder", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 10.0) -> None:
        self._stop.set()
        try:
            self._q.put_nowait(_SENTINEL)        # wake a blocking get() immediately
        except queue.Full:
            pass                                 # writer is busy draining; it'll see _stop
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._step(self._collect(self._flush_interval_s))
        self._step(self._drain_nowait(), force=True)  # final flush on shutdown

    def _step(self, batch: list[BookUpdate], *, force: bool = False) -> None:
        # Never let one bad batch kill the writer thread: a dead writer would fill the
        # queue and drop every update silently (the drop log runs on this thread).
        try:
            if batch:
                self._flush(batch)
        except Exception:
            # (updates of the batch not encoded yet are missing from the mirror too: the
            # whole book written next is then exact again from the next checkpoint)
            self._encoder.rebase()                # whatever was encoded never reached the lake
            _log.exception("book recorder: a batch of %d updates was not written; every venue's "
                           "next write is its whole book", len(batch))
        try:
            self._report(force=force)
        except Exception:
            _log.exception("book recorder: stats report failed")

    def _report(self, *, force: bool = False) -> None:
        """Log drops as soon as they grow (they mean the recorded stream diverged from
        the live book until the next checkpoint) and a compact per-interval summary."""
        dropped = self.dropped
        now = time.monotonic()
        if dropped > self._dropped_logged and (force or self._dropped_log_t is None
                                               or now - self._dropped_log_t >= self._stats_interval_s):
            # the first drop at once, then at most once per stats interval under overload
            _log.warning("book recorder DROPPED %d updates (queue full; total %d) -- "
                         "healed at the next checkpoint", dropped - self._dropped_logged, dropped)
            self._dropped_logged = dropped
            self._dropped_log_t = now
        if force or now - self._stats_t0 >= self._stats_interval_s:
            s = self._stats
            if s["received"]:
                _log.info("book recorder: received=%d written=%d rows=%d dropped_total=%d "
                          "write_errors_total=%d (%.0fs)", s["received"], s["written"], s["rows"],
                          dropped, self.write_errors, now - self._stats_t0)
            self._stats = {"received": 0, "written": 0, "rows": 0}
            self._stats_t0 = now

    def _collect(self, window_s: float) -> list[BookUpdate]:
        """Accumulate every update that arrives within ``window_s`` into ONE batch,
        then return it. This writes ~one file per exchange per interval instead of
        one per drain (which, under continuous streaming, produced thousands of tiny
        files). Blocking gets are capped at 0.5s so stop() is honoured within ~0.5s
        regardless of ``window_s``; the stop sentinel ends the window immediately."""
        out: list[BookUpdate] = []
        end = time.monotonic() + window_s
        while not self._stop.is_set():
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            try:
                item = self._q.get(timeout=min(remaining, 0.5))
            except queue.Empty:
                continue
            if item is _SENTINEL:
                break
            out.append(item)
            while True:                          # absorb whatever else is already queued
                try:
                    more = self._q.get_nowait()
                except queue.Empty:
                    break
                if more is not _SENTINEL:
                    out.append(more)
        return out

    def _drain_nowait(self) -> list[BookUpdate]:
        out: list[BookUpdate] = []
        while True:
            try:
                item = self._q.get_nowait()
            except queue.Empty:
                break
            if item is not _SENTINEL:
                out.append(item)
        return out

    def _flush(self, updates: list[BookUpdate]) -> None:
        paused = self._paused
        if self._writer_paused and not paused:
            self._encoder.rebase()                # the lake missed what was encoded while paused
        self._writer_paused = paused
        rows_by_ex: dict[str, list[dict]] = {}
        counts: dict[str, list[int]] = {}         # exchange -> [updates, rows] to write
        self._stats["received"] += len(updates)
        for received in updates:
            try:
                u = self._encoder.encode(received)   # in arrival order: the mirror stays in step
            except Exception:
                self._encoder.rebase(received.exchange)
                self._error("book recorder: could not encode a %s update; its next write is its "
                            "whole book", received.exchange)
                continue
            if u is None:
                continue                          # empty book: nothing a replay could need
            if paused:
                self.paused_skipped += 1          # disk floor: encoded (mirror = live), not written
                continue
            seq = self._seq.get(u.exchange, 0)
            self._seq[u.exchange] = seq + 1
            rows = rows_by_ex.setdefault(u.exchange, [])
            for price, size in u.bids:
                rows.append(_row(seq, u, "bid", price, size))
            for price, size in u.asks:
                rows.append(_row(seq, u, "ask", price, size))
            c = counts.setdefault(u.exchange, [0, 0])
            c[0] += 1
            c[1] += len(u.bids) + len(u.asks)
        failed = []
        for exchange, rows in rows_by_ex.items():
            try:
                self._sink.write(exchange, rows)
            except Exception as e:
                failed.append(exchange)
                self.write_errors += 1
                self._encoder.rebase(exchange)    # those rows are lost: the next write is whole
                if self._writing_ok:
                    _log.critical("book recorder CANNOT WRITE the lake (%s: %s); retrying every "
                                  "flush, each failed venue re-bases on its next write",
                                  type(e).__name__, e)
                    self._writing_ok = False
                self._error("book recorder failed to write %s rows", exchange)
            else:
                self._stats["written"] += counts[exchange][0]
                self._stats["rows"] += counts[exchange][1]
        now = time.monotonic()
        if failed:
            self._last_write_error_mono = now
        elif (rows_by_ex and not self._writing_ok
              and now - self._last_write_error_mono >= self._stats_interval_s):
            self._writing_ok = True               # a whole interval without a failure
            _log.critical("book recorder WRITING AGAIN (%d failed writes in total)", self.write_errors)

    def _error(self, msg: str, *args) -> None:
        """ERROR with traceback, at most once per stats interval (a failing disk would
        otherwise log ~14 tracebacks every flush)."""
        now = time.monotonic()
        if self._error_log_t is None or now - self._error_log_t >= self._stats_interval_s:
            self._error_log_t = now
            _log.exception(msg, *args)


def _row(seq: int, u: BookUpdate, side: str, price: float, size: float) -> dict:
    return {"seq_no": seq, "ts": u.ts, "exchange": u.exchange,
            "is_snapshot": u.is_snapshot, "side": side, "price": float(price), "size": float(size)}
