# tests/unit/test_compaction.py
import os
import duckdb
import pyarrow.parquet as pq
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.compaction import compact_partition, compact_lake, count_compactable


def _seed_partition(base, exchange, date, hour, n_files, rows_per):
    """Write n_files small parquet files into one exchange/date/HH partition."""
    sink = ParquetSink(base)
    seq = 0
    for _ in range(n_files):
        rows = []
        for _ in range(rows_per):
            rows.append({"seq_no": seq, "ts": _hour_ts(date, hour) + (seq % 50),
                         "exchange": exchange, "is_snapshot": True, "side": "bid",
                         "price": 63000.0 + seq, "size": 1.0 + (seq % 7)})
            seq += 1
        sink.write(exchange, rows)  # ts in the same hour -> same partition


def _hour_ts(date, hour):
    import calendar
    import time
    # UTC epoch for the wall-clock hour; matches the sink's gmtime-based bucketing
    # (DST-safe, unlike mktime-time.timezone which over-corrects in summer).
    return float(calendar.timegm(time.strptime(f"{date} {hour}", "%Y-%m-%d %H")))


def _part_dir(base, exchange, date, hour):
    return os.path.join(base, f"exchange={exchange}", f"date={date}", hour)


def _rowset(paths):
    import pyarrow as pa
    # Read each file by its own stored schema (ignore Hive partition columns
    # auto-inferred from the exchange=/date= path, which collide with the
    # in-file `exchange` column under pyarrow's read_table).
    t = pa.concat_tables([pq.ParquetFile(p).read() for p in paths])
    return sorted(tuple(r.values()) for r in t.to_pylist())


def test_compact_partition_is_lossless_and_reduces_files(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "10", n_files=8, rows_per=20)
    part = _part_dir(base, "kraken", "2026-06-01", "10")
    before = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    before_rows = _rowset([os.path.join(part, f) for f in before])
    res = compact_partition(part)
    after = [p for p in os.listdir(part) if p.endswith(".parquet")]
    assert res["compacted"] is True and res["files_before"] == 8
    assert len(after) == 1                                   # 8 -> 1
    after_rows = _rowset([os.path.join(part, after[0])])
    assert after_rows == before_rows                         # EXACT same rows (multiset)
    # DuckDB sees identical data
    n = duckdb.sql(f"SELECT count(*), sum(size) FROM '{part}/*.parquet'").fetchone()
    assert n[0] == 160


def test_compact_partition_idempotent_single_file(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "okx", "2026-06-01", "09", n_files=1, rows_per=5)
    part = _part_dir(base, "okx", "2026-06-01", "09")
    res = compact_partition(part)
    assert res.get("skipped") is True
    assert len([p for p in os.listdir(part) if p.endswith(".parquet")]) == 1


def test_compact_partition_keeps_originals_if_verify_fails(tmp_path, monkeypatch):
    base = str(tmp_path)
    _seed_partition(base, "gate", "2026-06-01", "08", n_files=4, rows_per=10)
    part = _part_dir(base, "gate", "2026-06-01", "08")
    before = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    import pavilos.persistence.compaction as comp
    monkeypatch.setattr(comp, "_rows_equal", lambda *a, **k: False)   # force verify failure
    try:
        compact_partition(part)
    except Exception:
        pass
    after = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    assert after == before                                   # originals intact, NO data loss
    assert not any(f.startswith("_compacted") for f in os.listdir(part))  # temp cleaned up


def test_compact_lake_skips_the_live_current_hour(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "10", n_files=5, rows_per=10)  # past
    _seed_partition(base, "kraken", "2026-06-01", "12", n_files=5, rows_per=10)  # the "current" hour
    now = _hour_ts("2026-06-01", "12") + 1800   # we are in hour 12
    summary = compact_lake(base, now_ts=now)
    past = _part_dir(base, "kraken", "2026-06-01", "10")
    live = _part_dir(base, "kraken", "2026-06-01", "12")
    assert len([p for p in os.listdir(past) if p.endswith(".parquet")]) == 1   # past compacted
    assert len([p for p in os.listdir(live) if p.endswith(".parquet")]) == 5   # live untouched
    assert summary["partitions_compacted"] >= 1


def test_compact_partition_ignores_crash_orphan_temp(tmp_path):
    """A `_compacted_<old_pid>.parquet` left by a crashed prior run must NOT be
    merged (it already holds a full copy of every row) — re-ingesting it would
    DOUBLE the rows yet still pass verify (both sides equally doubled)."""
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "07", n_files=4, rows_per=6)
    part = _part_dir(base, "kraken", "2026-06-01", "07")
    real = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    real_rows = _rowset([os.path.join(part, f) for f in real])
    # Simulate a crash-orphan temp: a full copy of all rows, under a DIFFERENT pid.
    import pyarrow as pa
    orphan_tbl = pa.concat_tables([pq.ParquetFile(os.path.join(part, f)).read() for f in real])
    pq.write_table(orphan_tbl, os.path.join(part, "_compacted_999999.parquet"), compression="zstd")
    res = compact_partition(part)
    assert res.get("compacted") is True
    after = [p for p in os.listdir(part) if p.endswith(".parquet")]
    assert after == ["compacted.parquet"]                    # single output, orphan gone
    after_rows = _rowset([os.path.join(part, after[0])])
    assert after_rows == real_rows                           # NOT doubled


def test_compact_partition_ignores_dir_named_parquet(tmp_path):
    """A sub-directory whose name ends in '.parquet' must be skipped, not opened
    as a file (which would raise a confusing low-level error)."""
    base = str(tmp_path)
    _seed_partition(base, "okx", "2026-06-01", "06", n_files=3, rows_per=5)
    part = _part_dir(base, "okx", "2026-06-01", "06")
    real = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    real_rows = _rowset([os.path.join(part, f) for f in real])
    os.makedirs(os.path.join(part, "stray.parquet"))         # directory, not a file
    res = compact_partition(part)
    assert res.get("compacted") is True
    files = sorted(p for p in os.listdir(part) if os.path.isfile(os.path.join(part, p)))
    assert files == ["compacted.parquet"]
    assert os.path.isdir(os.path.join(part, "stray.parquet"))  # stray dir left untouched
    assert _rowset([os.path.join(part, "compacted.parquet")]) == real_rows


def test_compact_lake_continues_past_corrupt_partition(tmp_path):
    """One corrupt parquet in a closed partition must be reported, not abort the
    whole run; other closed partitions still compact."""
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "10", n_files=4, rows_per=5)  # ok
    _seed_partition(base, "kraken", "2026-06-01", "11", n_files=4, rows_per=5)  # corrupt
    _seed_partition(base, "kraken", "2026-06-01", "13", n_files=4, rows_per=5)  # ok, after 11
    # Truncate one file in hour 11 so pyarrow cannot read it.
    bad_part = _part_dir(base, "kraken", "2026-06-01", "11")
    bad_file = next(p for p in os.listdir(bad_part) if p.endswith(".parquet"))
    with open(os.path.join(bad_part, bad_file), "wb") as fh:
        fh.write(b"not a parquet file")
    now = _hour_ts("2026-06-01", "14")          # all of 10/11/13 are closed
    summary = compact_lake(base, now_ts=now)
    h10 = _part_dir(base, "kraken", "2026-06-01", "10")
    h13 = _part_dir(base, "kraken", "2026-06-01", "13")
    assert len([p for p in os.listdir(h10) if p.endswith(".parquet")]) == 1   # compacted
    assert len([p for p in os.listdir(h13) if p.endswith(".parquet")]) == 1   # compacted
    assert summary["partitions_compacted"] == 2
    assert summary["partitions_failed"] == 1
    # The corrupt partition's originals are untouched (no data loss, no temp left).
    assert len([p for p in os.listdir(bad_part) if p.endswith(".parquet")]) == 4
    assert not any(f.startswith("_compacted") for f in os.listdir(bad_part))


def test_dry_run_count_excludes_live_hour(tmp_path):
    """The --dry-run preview must use the same closed-partition predicate as the
    real run: a lake whose ONLY multi-file partition is the live current hour
    reports 0 (the real run skips it), not over-counted."""
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "12", n_files=4, rows_per=5)  # live hour
    now = _hour_ts("2026-06-01", "12") + 600    # we are in hour 12
    c = count_compactable(base, now_ts=now)
    assert c == {"partitions": 0, "files": 0}
    # And the real run agrees: nothing compacted, live hour untouched.
    summary = compact_lake(base, now_ts=now)
    assert summary["partitions_compacted"] == 0
    live = _part_dir(base, "kraken", "2026-06-01", "12")
    assert len([p for p in os.listdir(live) if p.endswith(".parquet")]) == 4


def test_dry_run_count_excludes_stray_temp(tmp_path):
    """The dry-run must not count a stray `_compacted_*.parquet` temp toward the
    file total (the real run ignores it too)."""
    base = str(tmp_path)
    _seed_partition(base, "okx", "2026-06-01", "08", n_files=3, rows_per=5)  # closed
    part = _part_dir(base, "okx", "2026-06-01", "08")
    pq.write_table(pq.ParquetFile(os.path.join(
        part, next(f for f in os.listdir(part) if f.endswith(".parquet")))).read(),
        os.path.join(part, "_compacted_424242.parquet"), compression="zstd")
    now = _hour_ts("2026-06-01", "12")          # hour 08 is closed
    c = count_compactable(base, now_ts=now)
    assert c == {"partitions": 1, "files": 3}   # 3 real files, stray temp NOT counted
