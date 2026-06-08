# tests/unit/test_recorder.py
import threading
import time

from pavilos.core.models import BookUpdate
from pavilos.persistence.recorder import BookRecorder


class _FakeSink:
    def __init__(self):
        self.rows_by_ex: dict[str, list] = {}
        self.write_calls = 0
        self._lock = threading.Lock()
    def write(self, exchange, rows):
        with self._lock:
            self.rows_by_ex.setdefault(exchange, []).extend(rows)
            self.write_calls += 1
        return 1


def _u(ex, ts, bids, asks, snap=True, seq=1):
    return BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=snap, seq=seq)


def test_batches_per_interval_not_per_update_under_steady_arrival():
    # 10 updates trickling over ~1 flush window must collapse into a few writes,
    # NOT ~10 (one per drain) -- the time-boxed collect is what keeps file count sane.
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.25)
    rec.start()
    try:
        for i in range(10):
            rec.record(_u("kraken", float(i), [(1.0, 1.0)], []))
            time.sleep(0.02)
        time.sleep(0.4)
    finally:
        rec.stop()
    assert len(sink.rows_by_ex["kraken"]) == 10   # nothing lost
    assert sink.write_calls <= 4                  # batched per window, not one-per-update


def test_record_expands_levels_and_flushes_via_writer_thread():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.02)
    rec.start()
    try:
        rec.record(_u("kraken", 1.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
        rec.record(_u("kraken", 2.0, [(100.0, 0.0)], [], snap=False))
        # wait for the writer thread to flush
        deadline = time.time() + 2.0
        while time.time() < deadline and len(sink.rows_by_ex.get("kraken", [])) < 4:
            time.sleep(0.01)
    finally:
        rec.stop()
    rows = sink.rows_by_ex["kraken"]
    assert len(rows) == 4   # 2+1 levels from update 1, 1 from update 2
    # seq_no monotonic per exchange, groups an update's levels
    seqs = sorted({r["seq_no"] for r in rows})
    assert seqs == [0, 1]
    assert {r["side"] for r in rows} == {"bid", "ask"}
    bid_remove = [r for r in rows if r["seq_no"] == 1]
    assert bid_remove[0]["size"] == 0.0 and bid_remove[0]["is_snapshot"] is False


def test_record_is_nonblocking_and_drops_when_queue_full():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=100.0, max_queue=3)  # writer effectively idle
    for i in range(10):
        rec.record(_u("okx", float(i), [(1.0, 1.0)], []))
    assert rec.dropped >= 1     # overflow dropped, never blocked
    # stop flushes whatever made it into the queue without hanging
    rec.start(); rec.stop()


def test_start_is_idempotent_no_duplicate_writer_thread():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.02)
    rec.start()
    try:
        first = rec._thread
        rec.start()                       # second start must NOT spawn a duplicate
        assert rec._thread is first
    finally:
        rec.stop()


def test_stop_on_idle_recorder_does_not_block_for_flush_interval():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=100.0)   # huge interval; idle queue
    rec.start()
    t0 = time.time()
    rec.stop(timeout=5.0)
    assert time.time() - t0 < 2.0     # stop must wake the writer promptly
