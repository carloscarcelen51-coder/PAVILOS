# tests/unit/test_lake_check.py
import random

from pavilos.aggregator.book_state import BookState
from pavilos.backtest.lake_check import check_checkpoints
from pavilos.core.models import BookUpdate
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.recorder import BookRecorder


def _stream(seed, *, t0=1000.0, steps=300):
    """kraken native-like (snapshot then deltas) + coinbase ccxt-like (whole book each
    message), with live checkpoints every 20 steps. Returns the updates to record."""
    rng = random.Random(seed)
    live = {"kraken": BookState("kraken"), "coinbase": BookState("coinbase")}
    cb = {100.0 + i * 0.1: 1.0 for i in range(10)}
    first = BookUpdate("kraken", t0, tuple((99.0 - i * 0.5, 1.0 + i) for i in range(8)),
                       tuple((101.0 + i * 0.5, 1.0 + i) for i in range(8)), True, None)
    live["kraken"].apply(first)
    recorded = [first]
    ts = t0
    for step in range(steps):
        ts += rng.choice([0.05, 0.3, 0.9])
        if step % 20 == 19:
            for ex in ("kraken", "coinbase"):
                b = live[ex]
                if b.mid() is not None:
                    recorded.append(BookUpdate(ex, b.last_ts, tuple(b.bids().items()),
                                               tuple(b.asks().items()), True, None, checkpoint=True))
            continue
        if rng.random() < 0.5:
            u = BookUpdate("kraken", ts,
                           ((round(99.0 - rng.randint(0, 12) * 0.5, 2), rng.choice([0.0, 0.5, 2.0])),),
                           ((round(101.0 + rng.randint(0, 12) * 0.5, 2), rng.choice([0.0, 0.5, 2.0])),),
                           False, None)
        else:
            cb[round(100.0 + rng.randint(0, 12) * 0.1, 1)] = rng.choice([0.5, 1.0, 4.0])
            u = BookUpdate("coinbase", ts, tuple(sorted(cb.items(), reverse=True)), ((102.0, 1.0),),
                           True, None)
        live[u.exchange].apply(u)
        recorded.append(u)
    return recorded


def _record(base, updates):
    rec = BookRecorder(ParquetSink(base), flush_interval_s=0.05)
    rec.start()
    for u in updates:
        rec.record(u)
    rec.stop()
    assert rec.dropped == 0


def test_a_faithful_lake_has_zero_mismatches(tmp_path):
    updates = _stream(3)
    _record(str(tmp_path), updates)
    res = check_checkpoints(str(tmp_path), 0.0, 1e9)
    assert set(res) == {"kraken", "coinbase"}
    for ex, v in res.items():
        assert v["checks"] >= 10, (ex, v)
        assert v["mismatches"] == 0, (ex, v)
        assert v["bases"] == 1 and v["unbased"] == 0


def test_a_lost_update_is_caught_at_the_next_checkpoint_and_then_heals(tmp_path):
    live = BookState("kraken")
    snap = BookUpdate("kraken", 1.0, ((99.0, 1.0), (98.0, 1.0)), ((101.0, 1.0),), True, None)
    lost = BookUpdate("kraken", 2.0, ((97.0, 5.0),), (), False, None)
    later = BookUpdate("kraken", 3.0, ((96.0, 1.0),), (), False, None)

    def ck():
        return BookUpdate("kraken", live.last_ts, tuple(live.bids().items()),
                          tuple(live.asks().items()), True, None, checkpoint=True)

    live.apply(snap)
    live.apply(lost)                     # live saw it; the recorder never did (queue full)
    ck1 = ck()
    live.apply(later)
    ck2 = ck()
    _record(str(tmp_path), [snap, ck1, later, ck2])
    res = check_checkpoints(str(tmp_path), 0.0, 1e9)["kraken"]
    assert (res["bases"], res["checks"], res["mismatches"]) == (1, 2, 1)
    assert res["examples"] == [{"ts": 2.0, "levels_differing": 1, "replayed_last_ts": 1.0}]


def test_restart_rebases_instead_of_reporting_a_mismatch(tmp_path):
    a = _stream(5, t0=1000.0, steps=100)
    _record(str(tmp_path), a)                               # process 1
    # process 2 (seq_no starts again at 0); its first kraken book differs from process 1's
    b = _stream(6, t0=2000.0, steps=100)
    _record(str(tmp_path), b)
    res = check_checkpoints(str(tmp_path), 0.0, 1e9)
    for ex, v in res.items():
        assert v["mismatches"] == 0, (ex, v)
        assert v["bases"] == 2, (ex, v)                     # one per process


def test_window_without_data_is_empty(tmp_path):
    assert check_checkpoints(str(tmp_path / "nothing"), 0.0, 1.0) == {}


def _raw(base, rows):
    """Write lake rows directly: (seq_no, ts, is_snapshot, side, price, size) for kraken."""
    ParquetSink(base).write("kraken", [
        {"seq_no": s, "ts": t, "exchange": "kraken", "is_snapshot": snap, "side": side,
         "price": p, "size": z} for s, t, snap, side, p, z in rows])


def test_out_of_order_rows_are_reported_and_a_restart_is_not(tmp_path):
    base = str(tmp_path)
    _raw(base, [
        (0, 1.0, True, "bid", 100.0, 1.0), (0, 1.0, True, "ask", 101.0, 1.0),
        (1, 3.0, False, "bid", 99.0, 1.0),
        (2, 2.0, False, "bid", 98.0, 1.0),          # written after seq 1 but with an older ts
        (0, 10.0, True, "bid", 100.0, 2.0), (0, 10.0, True, "ask", 101.0, 2.0),   # restart
    ])
    v = check_checkpoints(base, 0.0, 1e9)["kraken"]
    assert v["reordered"] == 1
    assert v["bases"] == 2 and v["mismatches"] == 0


def test_a_whole_book_after_a_silence_is_a_base_not_a_mismatch(tmp_path):
    live = BookState("kraken")
    a = BookUpdate("kraken", 1.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None)
    b = BookUpdate("kraken", 2.0, ((98.0, 1.0),), (), False, None)
    c = BookUpdate("kraken", 40.0, ((97.0, 1.0),), (), False, None)   # after 38 s: written whole
    for u in (a, b, c):
        live.apply(u)
    ck = BookUpdate("kraken", live.last_ts, tuple(live.bids().items()), tuple(live.asks().items()),
                    True, None, checkpoint=True)
    _record(str(tmp_path), [a, b, c, ck])
    v = check_checkpoints(str(tmp_path), 0.0, 1e9)["kraken"]
    assert (v["bases"], v["checks"], v["mismatches"], v["reordered"]) == (2, 1, 0, 0)


def test_a_loss_longer_than_the_silence_rule_is_still_reported(tmp_path):
    """20 s of failed writes end in a whole book: it carries the loss mark, so it is
    COMPARED (and the loss reported) instead of being taken for a silence base."""
    import math

    class _Flaky(ParquetSink):
        fail_until = None
        def write(self, exchange, rows):
            if self.fail_until is not None and rows and rows[0]["ts"] < self.fail_until:
                raise OSError(28, "No space left on device")
            return super().write(exchange, rows)

    sink = _Flaky(str(tmp_path))
    rec = BookRecorder(sink, flush_interval_s=0.05)
    live = BookState("kraken")
    first = BookUpdate("kraken", 1.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None)
    rec.start()
    rec.record(first); live.apply(first)
    _settle(rec)
    sink.fail_until = 25.0
    for t in range(2, 22):                                   # 20 s of writes that fail
        u = BookUpdate("kraken", float(t), ((99.0 - t * 0.01, 1.0),), (), False, None)
        rec.record(u); live.apply(u)
    _settle(rec)
    sink.fail_until = None
    after = BookUpdate("kraken", 30.0, ((90.0, 1.0),), (), False, None)
    rec.record(after); live.apply(after)
    _settle(rec)
    rec.stop()
    v = check_checkpoints(str(tmp_path), 0.0, 1e9)["kraken"]
    assert v["loss_marks"] == 1 and v["checks"] == 1
    assert (v["loss_mismatches"], v["mismatches"], v["examples"]) == (1, 0, [])   # explained
    assert not math.isnan(v["loss_marks"])


def _settle(rec, timeout=5.0):
    import time as _t
    end = _t.monotonic() + timeout
    while rec._q.qsize() and _t.monotonic() < end:
        _t.sleep(0.01)
    _t.sleep(0.2)


def test_a_new_run_whose_first_write_failed_is_not_reordered(tmp_path):
    """Run 2's first kraken write fails (seq_no 0 consumed), so its first row in the lake
    has seq_no 1. The run marker resets the books: no false 'reordered'."""
    from pavilos.persistence.runs import mark_run_start
    base = str(tmp_path)
    _record(base, [BookUpdate("kraken", 1.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
                   BookUpdate("kraken", 2.0, ((98.0, 1.0),), (), False, None),
                   BookUpdate("kraken", 3.0, ((97.0, 1.0),), (), False, None),
                   BookUpdate("kraken", 4.0, ((96.0, 1.0),), (), False, None)])   # seq_no 0..3
    mark_run_start(base, ts=5.0)

    class _FailsFirst(ParquetSink):
        first = True
        def write(self, exchange, rows):
            if self.first:
                self.first = False
                raise OSError(28, "No space left on device")
            return super().write(exchange, rows)

    rec = BookRecorder(_FailsFirst(base), flush_interval_s=0.05)
    rec.start()
    rec.record(BookUpdate("kraken", 6.0, ((99.0, 2.0),), ((101.0, 2.0),), True, None))
    _settle(rec)
    rec.record(BookUpdate("kraken", 7.0, ((98.0, 1.0),), (), False, None))
    _settle(rec)
    rec.stop()
    v = check_checkpoints(base, 0.0, 1e9)["kraken"]
    assert v["reordered"] == 0 and v["mismatches"] == 0
