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


def _flushed(rec, sink, ex, n_rows, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline and len(sink.rows_by_ex.get(ex, [])) < n_rows:
        time.sleep(0.01)


def test_repeated_full_books_are_written_as_differences():
    # ccxt venues resend the WHOLE book each message; only the change may be persisted.
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.02)
    rec.start()
    try:
        book = [(100.0 - i, 1.0) for i in range(50)]
        rec.record(_u("kucoin", 1.0, book, [(101.0, 1.0)]))                 # full: 51 rows
        changed = [(100.0, 2.0)] + book[1:]                                  # one level changed
        rec.record(_u("kucoin", 2.0, changed, [(101.0, 1.0)]))              # diff: 1 row
        _flushed(rec, sink, "kucoin", 52)
    finally:
        rec.stop()
    rows = sink.rows_by_ex["kucoin"]
    assert len(rows) == 52
    second = [r for r in rows if r["seq_no"] == 1]
    assert second == [{"seq_no": 1, "ts": 2.0, "exchange": "kucoin", "is_snapshot": False,
                       "side": "bid", "price": 100.0, "size": 2.0}]


def test_checkpoint_is_written_whole_even_when_unchanged():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.02)
    rec.start()
    try:
        rec.record(_u("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        rec.record(BookUpdate("kraken", 1.0, ((100.0, 1.0),), ((101.0, 1.0),), True, None, checkpoint=True))
        _flushed(rec, sink, "kraken", 4)
    finally:
        rec.stop()
    ck = [r for r in sink.rows_by_ex["kraken"] if r["seq_no"] == 1]
    assert len(ck) == 2 and all(r["is_snapshot"] for r in ck)


def test_drops_are_logged(caplog):
    import logging
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=100.0, max_queue=3)
    for i in range(10):
        rec.record(_u("okx", float(i), [(1.0, 1.0)], []))
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.recorder"):
        rec.start(); rec.stop()
    assert rec.dropped >= 1
    assert any("DROPPED" in r.getMessage() and str(rec.dropped) in r.getMessage() for r in caplog.records)


def test_paused_recorder_persists_nothing_and_resumes_cleanly(caplog):
    """While paused the writer still ENCODES (its mirror stays the live book) and writes
    nothing; after the resume the venue's next write is its WHOLE book, so the lake is
    exact again at once (not only from the next checkpoint)."""
    import logging
    from pavilos.aggregator.book_state import BookState
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.05)
    live = BookState("kraken")
    first = BookUpdate("kraken", 1.0, ((99.0, 1.0), (98.0, 1.0)), ((101.0, 1.0),), True, None)
    during = [BookUpdate("kraken", 2.0, ((99.0, 5.0),), (), False, None),
              BookUpdate("kraken", 3.0, ((98.0, 0.0),), ((102.0, 2.0),), False, None)]
    after = BookUpdate("kraken", 4.0, ((97.0, 1.0),), (), False, None)
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.recorder"):
        rec.start()
        rec.record(first); live.apply(first)
        time.sleep(0.3)
        rec.set_paused(True)
        rec.set_paused(True)                       # idempotent: logged once
        for u in during:
            rec.record(u); live.apply(u)
        time.sleep(0.3)
        written_while_paused = len(sink.rows_by_ex["kraken"])
        rec.set_paused(False)
        rec.record(after); live.apply(after)
        time.sleep(0.3)
        rec.stop()
    rows = sink.rows_by_ex["kraken"]
    assert written_while_paused == 3               # only the first book (2 bids + 1 ask)
    assert rec.paused_skipped == 2 and not rec.paused
    resumed = [r for r in rows if r["ts"] == 4.0]
    assert resumed and all(r["is_snapshot"] for r in resumed)          # the whole book
    assert _book(resumed, "bid") == live.bids() and _book(resumed, "ask") == live.asks()
    assert any(r["price"] != r["price"] for r in resumed)              # + the loss mark (NaN)
    msgs = [r.getMessage() for r in caplog.records]
    assert sum("PAUSED" in m for m in msgs) == 1
    assert any("RESUMED after skipping" in m for m in msgs)
    # CRITICAL both ways: the only level that also reaches the supervisor's log
    assert all(r.levelno == logging.CRITICAL for r in caplog.records
               if "PAUSED" in r.getMessage() or "RESUMED" in r.getMessage())
    assert rec.status() == {"paused": False, "dropped": 0, "paused_skipped": 2, "write_errors": 0,
                            "dropped_recently": False, "write_errors_recently": False,
                            "clock_behind_s": 0.0}


def test_periodic_summary_reports_received_written_and_rows(caplog):
    import logging
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.05, stats_interval_s=0.0)
    with caplog.at_level(logging.INFO, logger="pavilos.persistence.recorder"):
        rec.start()
        rec.record(_u("kraken", 1.0, [(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)]))
        rec.record(_u("kraken", 2.0, [(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)]))   # unchanged -> heartbeat
        time.sleep(0.3)
        rec.stop()
    import re
    totals = [0, 0, 0]
    for r in caplog.records:
        m = re.search(r"received=(\d+) written=(\d+) rows=(\d+) dropped_total=0", r.getMessage())
        if m:
            totals = [a + int(b) for a, b in zip(totals, m.groups())]
    assert totals == [2, 2, 4]          # full book (3 rows) + one-row heartbeat, however batched


def test_writer_survives_a_failing_batch(caplog):
    import logging
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.05)
    real = rec._encoder.encode
    calls = {"n": 0}

    def flaky(u):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return real(u)

    rec._encoder.encode = flaky
    with caplog.at_level(logging.ERROR, logger="pavilos.persistence.recorder"):
        rec.start()
        rec.record(_u("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        time.sleep(0.3)
        rec.record(_u("kraken", 2.0, [(100.0, 2.0)], [(101.0, 1.0)]))
        time.sleep(0.3)
        rec.stop()
    assert "kraken" in sink.rows_by_ex                    # kept writing after the bad update
    assert any("could not encode a kraken update" in r.getMessage() for r in caplog.records)


def test_drop_warnings_are_rate_limited(caplog):
    import logging
    rec = BookRecorder(_FakeSink(), max_queue=1, stats_interval_s=60.0)    # not started
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.recorder"):
        for i in range(3):
            rec.record(_u("kraken", float(i), [(1.0, 1.0)], []))         # 1 queued, 2 dropped
        rec._report()                                                    # first drop: at once
        n_first = len([r for r in caplog.records if "DROPPED" in r.getMessage()])
        rec.record(_u("kraken", 9.0, [(1.0, 1.0)], []))                   # dropped again
        rec._report()                                                    # same interval: silent
        n_second = len([r for r in caplog.records if "DROPPED" in r.getMessage()])
        rec._report(force=True)                                          # shutdown: reported
    drops = [r.getMessage() for r in caplog.records if "DROPPED" in r.getMessage()]
    assert (n_first, n_second, len(drops)) == (1, 1, 2)
    assert "DROPPED 2 updates" in drops[0] and "DROPPED 1 updates (queue full; total 3)" in drops[1]


def test_a_failed_write_makes_that_venues_next_write_its_whole_book():
    """ccxt-like venue (whole book every message, written as differences): if one write
    fails, the lake missed those differences -- the next write must be the whole book or
    every later difference would apply to a book the lake never had."""
    class _FlakySink(_FakeSink):
        fail = True
        def write(self, exchange, rows):
            if self.fail:
                self.fail = False
                raise OSError(28, "No space left on device")
            return super().write(exchange, rows)

    sink = _FlakySink()
    rec = BookRecorder(sink, flush_interval_s=0.05)
    book = {100.0 + i: 1.0 for i in range(5)}
    rec.start()
    rec.record(_u("gemini", 1.0, sorted(book.items()), [(200.0, 1.0)]))        # lost (write fails)
    time.sleep(0.3)
    book[101.0] = 9.0
    rec.record(_u("gemini", 2.0, sorted(book.items()), [(200.0, 1.0)]))
    time.sleep(0.3)
    rec.stop()
    rows = sink.rows_by_ex["gemini"]
    assert rows and all(r["is_snapshot"] for r in rows)                        # whole, not a diff
    assert _book(rows, "bid") == book and any(r["price"] != r["price"] for r in rows)


def test_an_update_that_fails_after_encoding_rebases_its_venue():
    """The encoder advanced its mirror, then raised: the rest of the batch is still
    written, and that venue's next write is its whole book (+ loss mark), not a diff
    against a mirror the lake never received."""
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.05)
    rec.start()
    rec.record(_u("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    assert _wait_for(lambda: "kraken" in sink.rows_by_ex)
    real = rec._encoder.encode
    seen = {"n": 0}

    def fails_second(u):
        out = real(u)                       # the mirror DOES advance...
        seen["n"] += 1
        if seen["n"] == 2:
            raise RuntimeError("boom")      # ...then this update dies before being written
        return out

    rec._encoder.encode = fails_second
    rec._q.put_nowait(_u("kraken", 2.0, [(99.0, 2.0)], [], snap=False))
    rec._q.put_nowait(_u("kraken", 3.0, [(98.0, 3.0)], [], snap=False))
    assert _wait_for(lambda: seen["n"] >= 2)
    rec._encoder.encode = real
    rec.record(_u("kraken", 4.0, [(97.0, 4.0)], [], snap=False))
    assert _wait_for(lambda: any(r["ts"] == 4.0 for r in sink.rows_by_ex["kraken"]))
    rec.stop()
    rows = sink.rows_by_ex["kraken"]
    assert [r["ts"] for r in rows if r["ts"] == 2.0]                   # the rest of the batch
    last = [r for r in rows if r["ts"] == 4.0]
    assert all(r["is_snapshot"] for r in last)
    assert _book(last, "bid") == {100.0: 1.0, 99.0: 2.0, 98.0: 3.0, 97.0: 4.0}


def _book(rows, side):
    """The finite levels of written rows (the NaN loss mark is not a level)."""
    return {r["price"]: r["size"] for r in rows if r["side"] == side and r["price"] == r["price"]}


def _wait_for(cond, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def test_write_failures_are_counted_shown_and_logged_critical_both_ways(caplog):
    import logging

    class _Disk(_FakeSink):
        broken = True
        def write(self, exchange, rows):
            if self.broken:
                raise OSError(28, "No space left on device")
            return super().write(exchange, rows)

    sink = _Disk()
    rec = BookRecorder(sink, flush_interval_s=0.05, stats_interval_s=0.3)
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.recorder"):
        rec.start()
        rec.record(_u("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        rec.record(_u("okx", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        assert _wait_for(lambda: rec.write_errors >= 2)
        st = rec.status()
        assert st["write_errors"] >= 2 and st["write_errors_recently"]
        sink.broken = False
        rec.record(_u("kraken", 2.0, [(100.0, 2.0)], [(101.0, 1.0)]))
        assert _wait_for(lambda: "kraken" in sink.rows_by_ex)
        time.sleep(0.4)                                          # a whole interval without failures
        rec.record(_u("kraken", 3.0, [(100.0, 3.0)], [(101.0, 1.0)]))
        assert _wait_for(lambda: any(r["ts"] == 3.0 for r in sink.rows_by_ex["kraken"]))
        rec.stop()
    crit = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert sum("CANNOT WRITE" in m for m in crit) == 1           # once, not per venue/flush
    assert sum("WRITING AGAIN" in m for m in crit) == 1
    assert rec.status(recent_s=0.0)["write_errors_recently"] is False


def test_status_reports_recent_drops_only():
    rec = BookRecorder(_FakeSink(), max_queue=1)                 # not started
    assert rec.status()["dropped_recently"] is False
    rec.record(_u("kraken", 1.0, [(1.0, 1.0)], []))
    rec.record(_u("kraken", 2.0, [(1.0, 1.0)], []))              # dropped
    assert rec.status()["dropped_recently"] is True and rec.status()["dropped"] == 1
    assert rec.status(recent_s=0.0)["dropped_recently"] is False


def test_ts_floor_reaches_the_encoder():
    rec = BookRecorder(_FakeSink())
    rec.set_ts_floor(123.0)
    w = rec._encoder.encode(_u("kraken", 100.0, [(1.0, 1.0)], [(2.0, 1.0)]))
    assert w.ts == 123.0


def test_intermittent_write_failures_do_not_flap_the_critical(caplog):
    import logging

    class _Flaky(_FakeSink):
        n = 0
        def write(self, exchange, rows):
            self.n += 1
            if self.n % 2:                    # every other write fails
                raise OSError(28, "No space left on device")
            return super().write(exchange, rows)

    rec = BookRecorder(_Flaky(), flush_interval_s=0.02, stats_interval_s=60.0)
    with caplog.at_level(logging.CRITICAL, logger="pavilos.persistence.recorder"):
        rec.start()
        for i in range(30):
            rec.record(_u("kraken", float(i), [(100.0, 1.0 + i)], [(101.0, 1.0)]))
            time.sleep(0.03)
        rec.stop()
    msgs = [r.getMessage() for r in caplog.records]
    assert sum("CANNOT WRITE" in m for m in msgs) == 1
    assert sum("WRITING AGAIN" in m for m in msgs) == 0      # never a clean interval


def test_recency_does_not_follow_the_wall_clock(monkeypatch):
    import pavilos.persistence.recorder as recmod
    rec = BookRecorder(_FakeSink(), max_queue=1)
    rec.record(_u("kraken", 1.0, [(1.0, 1.0)], []))
    rec.record(_u("kraken", 2.0, [(1.0, 1.0)], []))         # dropped
    real = recmod.time.time
    monkeypatch.setattr(recmod.time, "time", lambda: real() + 3600.0)   # the PC clock jumps
    assert rec.status()["dropped_recently"] is True


def test_the_summary_counts_only_what_was_written(caplog):
    import logging
    import re

    class _Broken(_FakeSink):
        def write(self, exchange, rows):
            raise OSError(28, "No space left on device")

    rec = BookRecorder(_Broken(), flush_interval_s=0.05, stats_interval_s=0.0)
    with caplog.at_level(logging.INFO, logger="pavilos.persistence.recorder"):
        rec.start()
        rec.record(_u("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        assert _wait_for(lambda: rec.write_errors >= 1)
        rec.stop()
    totals = [0, 0, 0]
    for r in caplog.records:
        m = re.search(r"received=(\d+) written=(\d+) rows=(\d+)", r.getMessage())
        if m:
            totals = [a + int(b) for a, b in zip(totals, m.groups())]
    assert totals == [1, 0, 0]


def test_status_reports_how_far_the_clock_is_behind():
    rec = BookRecorder(_FakeSink())
    rec._encoder.encode(_u("kraken", 1000.0, [(1.0, 1.0)], [(2.0, 1.0)]))
    rec._encoder.encode(_u("kraken", 900.0, [(1.5, 1.0)], [], snap=False))   # 100 s back
    assert rec.status()["clock_behind_s"] == 100.0
    rec._encoder.encode(_u("kraken", 1001.0, [(1.6, 1.0)], [], snap=False))  # caught up
    assert rec.status()["clock_behind_s"] == 0.0
