# src/pavilos/persistence/recorder.py
"""Tap the BookUpdate stream and persist it as raw L2 rows via a ParquetSink.

record() does ONE O(1) queue put (safe to call from the event loop). A dedicated
writer thread drains the queue, expands each update into per-level rows (assigning a
monotonic seq_no per exchange so an update's levels stay groupable for replay), and
hands batches to the sink. Backpressure = drop-and-count (never blocks ingest)."""
from __future__ import annotations

import logging
import queue
import threading
import time

from pavilos.core.models import BookUpdate

_log = logging.getLogger(__name__)

_SENTINEL = object()    # poison-pill to wake the writer's blocking get() on stop()


class BookRecorder:
    def __init__(self, sink, *, flush_interval_s: float = 5.0, max_queue: int = 200_000) -> None:
        self._sink = sink
        self._flush_interval_s = flush_interval_s
        self._q: "queue.Queue[BookUpdate]" = queue.Queue(maxsize=max_queue)
        self._seq: dict[str, int] = {}
        self.dropped = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def record(self, update: BookUpdate) -> None:
        try:
            self._q.put_nowait(update)          # O(1); safe from the event loop
        except queue.Full:
            self.dropped += 1                   # writer behind -> drop (never block ingest)

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
            batch = self._collect(self._flush_interval_s)
            if batch:
                self._flush(batch)
        rest = self._drain_nowait()              # final flush on shutdown
        if rest:
            self._flush(rest)

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
        rows_by_ex: dict[str, list[dict]] = {}
        for u in updates:
            seq = self._seq.get(u.exchange, 0)
            self._seq[u.exchange] = seq + 1
            rows = rows_by_ex.setdefault(u.exchange, [])
            for price, size in u.bids:
                rows.append(_row(seq, u, "bid", price, size))
            for price, size in u.asks:
                rows.append(_row(seq, u, "ask", price, size))
        for exchange, rows in rows_by_ex.items():
            try:
                self._sink.write(exchange, rows)
            except Exception:
                _log.exception("book recorder failed to write %s rows", exchange)


def _row(seq: int, u: BookUpdate, side: str, price: float, size: float) -> dict:
    return {"seq_no": seq, "ts": u.ts, "exchange": u.exchange,
            "is_snapshot": u.is_snapshot, "side": side, "price": float(price), "size": float(size)}
