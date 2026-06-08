# tests/unit/test_parquet_sink.py
import duckdb
from pavilos.persistence.parquet_sink import ParquetSink, ROW_FIELDS


def test_writes_partitioned_parquet_readable_by_duckdb(tmp_path):
    sink = ParquetSink(str(tmp_path))
    # two updates' worth of rows, same exchange, ts within one hour (epoch 1_700_000_000 ~ 2023-11)
    rows = [
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.5},
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "ask", "price": 101.0, "size": 2.0},
        {"seq_no": 1, "ts": 1_700_000_001.0, "exchange": "kraken", "is_snapshot": False, "side": "bid", "price": 100.0, "size": 0.0},
    ]
    sink.write("kraken", rows)
    files = list(tmp_path.rglob("*.parquet"))
    assert files, "a parquet file was written"
    # partition path includes exchange + date
    assert any("exchange=kraken" in str(f) for f in files)
    got = duckdb.sql(f"SELECT count(*) c, sum(size) s FROM '{tmp_path}/**/*.parquet'").fetchone()
    assert got[0] == 3 and abs(got[1] - 3.5) < 1e-9
    # schema columns present
    cols = set(duckdb.sql(f"SELECT * FROM '{tmp_path}/**/*.parquet' LIMIT 0").columns)
    assert set(ROW_FIELDS).issubset(cols)


def test_two_writes_same_partition_do_not_overwrite(tmp_path):
    sink = ParquetSink(str(tmp_path))
    r = [{"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "okx", "is_snapshot": True, "side": "bid", "price": 1.0, "size": 1.0}]
    sink.write("okx", r)
    sink.write("okx", [{**r[0], "seq_no": 1, "price": 2.0}])
    n = duckdb.sql(f"SELECT count(*) FROM '{tmp_path}/**/*.parquet'").fetchone()[0]
    assert n == 2   # second write must not clobber the first


def test_fresh_sink_does_not_overwrite_prior_partition_file(tmp_path):
    # A process restart constructs a NEW ParquetSink (counter resets to {}).
    # Writing into an existing (exchange,date,hour) partition must NOT clobber
    # the prior run's file.
    r = [{"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "okx", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.0}]
    ParquetSink(str(tmp_path)).write("okx", r)               # run 1
    ParquetSink(str(tmp_path)).write("okx", [{**r[0], "price": 200.0}])  # run 2 (fresh sink)
    prices = {row[0] for row in duckdb.sql(
        f"SELECT price FROM '{tmp_path}/**/*.parquet'").fetchall()}
    assert prices == {100.0, 200.0}   # both runs' rows survive
