# tests/unit/test_runs.py
import json
import logging
import time

from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.runs import RUNS_FILE, lake_max_ts, mark_run_start, run_starts


def _rows(ex, ts_list):
    return [{"seq_no": i, "ts": t, "exchange": ex, "is_snapshot": True, "side": "bid",
             "price": 1.0, "size": 1.0} for i, t in enumerate(ts_list)]


def test_marks_are_appended_and_filtered_by_span(tmp_path):
    base = str(tmp_path)
    for ts in (10.0, 20.0, 30.0):
        assert mark_run_start(base, ts=ts) == ts
    with open(tmp_path / RUNS_FILE, "a", encoding="utf-8") as fh:
        fh.write("not json\n{\"pid\": 1}\n\n")
    assert run_starts(base, 10.0, 30.0) == [20.0, 30.0]       # (t0, t1]
    assert run_starts(base, 0.0, 100.0) == [10.0, 20.0, 30.0]
    assert run_starts(str(tmp_path / "none"), 0.0, 1e9) == []
    line = json.loads((tmp_path / RUNS_FILE).read_text(encoding="utf-8").splitlines()[0])
    assert set(line) == {"start_ts", "pid", "start_iso"}


def test_lake_max_ts_reads_the_newest_files_statistics(tmp_path):
    sink = ParquetSink(str(tmp_path))
    sink.write("kraken", _rows("kraken", [3600.0 * 5 + 1, 3600.0 * 5 + 7]))
    sink.write("okx", _rows("okx", [3600.0 * 5 + 3]))
    sink.write("okx", _rows("okx", [3600.0 * 2 + 3]))          # an older hour
    assert lake_max_ts(str(tmp_path)) == 3600.0 * 5 + 7
    assert lake_max_ts(str(tmp_path / "empty")) is None


def test_a_start_is_never_before_the_newest_row(tmp_path, caplog):
    future = time.time() + 1000.0                           # the PC clock stepped back
    ParquetSink(str(tmp_path)).write("kraken", _rows("kraken", [future]))
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.runs"):
        s = mark_run_start(str(tmp_path))
    assert s > future and abs(s - future) < 0.01
    assert any("behind the lake" in r.getMessage() and r.levelno == logging.CRITICAL
               for r in caplog.records)                     # > 60 s ahead: CRITICAL
    assert run_starts(str(tmp_path), 0.0, s) == [s]
    now_start = mark_run_start(str(tmp_path / "fresh"))
    assert abs(now_start - time.time()) < 5.0


def test_a_start_is_never_before_the_last_marked_start(tmp_path):
    """A run that wrote nothing (started paused, or died before its first flush) still
    marked its start; if the clock then steps back, the next start must come after it."""
    base = str(tmp_path)
    future = time.time() + 500.0
    mark_run_start(base, ts=future)           # the empty run, marked by a clock now set back
    s = mark_run_start(base)
    assert s > future
    assert run_starts(base, 0.0, 1e12) == sorted(run_starts(base, 0.0, 1e12)) == [future, s]


def test_garbage_bytes_and_a_cut_last_line_do_not_break_the_markers(tmp_path):
    base = str(tmp_path)
    mark_run_start(base, ts=100.0)
    with open(tmp_path / RUNS_FILE, "ab") as fh:
        fh.write(b"\xff\xfe\x00 not utf-8\n")
        fh.write(b'{"start_ts": 150.0, "pi')     # cut off: no newline
    mark_run_start(base, ts=200.0)
    assert run_starts(base, 0.0, 1e9) == [100.0, 200.0]


def test_lake_max_ts_without_statistics(tmp_path):
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
    part = tmp_path / "exchange=kraken" / "date=1970-01-01" / "05"
    part.mkdir(parents=True)
    table = pa.table({"seq_no": [0, 1], "ts": [18001.0, 18099.5], "exchange": ["kraken"] * 2,
                      "is_snapshot": [True, True], "side": ["bid", "bid"],
                      "price": [1.0, 2.0], "size": [1.0, 1.0]})
    pq.write_table(table, str(part / "a.parquet"), write_statistics=False)
    assert lake_max_ts(str(tmp_path)) == 18099.5
    assert os.path.exists(part / "a.parquet")
