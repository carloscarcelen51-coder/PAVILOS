# tests/unit/test_compaction.py
import os
import duckdb
import pyarrow.parquet as pq
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.compaction import compact_partition, compact_lake


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
