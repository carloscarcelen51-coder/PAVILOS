from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.query import load_range, reconstruct_book, summary


def test_summary_counts_rows_per_exchange(tmp_path):
    # exercises the summary SQL (would catch the reserved-keyword 'rows' alias bug)
    _seed(str(tmp_path))
    s = summary(str(tmp_path))
    assert len(s) == 1 and s[0]["exchange"] == "kraken" and s[0]["n"] == 4


def test_summary_empty_lake_returns_empty(tmp_path):
    assert summary(str(tmp_path)) == []


def _seed(base):
    sink = ParquetSink(base)
    # snapshot then a delta that removes a bid and adds an ask
    sink.write("kraken", [
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.0},
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "ask", "price": 101.0, "size": 1.0},
    ])
    sink.write("kraken", [
        {"seq_no": 1, "ts": 1_700_000_005.0, "exchange": "kraken", "is_snapshot": False, "side": "bid", "price": 100.0, "size": 0.0},
        {"seq_no": 1, "ts": 1_700_000_005.0, "exchange": "kraken", "is_snapshot": False, "side": "ask", "price": 102.0, "size": 2.0},
    ])


def test_load_range_counts_rows(tmp_path):
    _seed(str(tmp_path))
    rows = load_range(str(tmp_path), "kraken", 1_700_000_000.0, 1_700_000_010.0)
    assert len(rows) == 4


def test_reconstruct_book_replays_snapshot_then_delta(tmp_path):
    _seed(str(tmp_path))
    bids, asks = reconstruct_book(str(tmp_path), "kraken", at_ts=1_700_000_006.0)
    assert 100.0 not in bids            # removed by the delta (size 0)
    assert asks.get(101.0) == 1.0 and asks.get(102.0) == 2.0


def test_reconstruct_book_resets_on_post_restart_snapshot_reusing_seq0(tmp_path):
    # A recorder restart makes BookRecorder._seq reset to 0, so the first
    # post-restart snapshot is written with seq_no=0 again. The new snapshot
    # MUST reset the book; stale pre-restart levels must NOT survive.
    sink = ParquetSink(str(tmp_path))
    sink.write("kraken", [  # snapshot seq0: bid 100
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.0},
    ])
    sink.write("kraken", [  # delta seq1: bid 99
        {"seq_no": 1, "ts": 1_700_000_001.0, "exchange": "kraken", "is_snapshot": False, "side": "bid", "price": 99.0, "size": 5.0},
    ])
    sink.write("kraken", [  # post-restart snapshot reuses seq0: bid 200 only
        {"seq_no": 0, "ts": 1_700_000_002.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 200.0, "size": 2.0},
    ])
    bids, _asks = reconstruct_book(str(tmp_path), "kraken", at_ts=1_700_000_003.0)
    assert bids == {200.0: 2.0}        # only the post-restart snapshot survives


def test_load_range_empty_lake_returns_empty(tmp_path):
    # Self-Review #7: DuckDB glob on an empty/missing dir returns nothing, no crash.
    assert load_range(str(tmp_path), "kraken", 0.0, 9e9) == []
    assert load_range(str(tmp_path / "does_not_exist"), "kraken", 0.0, 9e9) == []


def test_reconstruct_book_empty_lake_returns_empty(tmp_path):
    assert reconstruct_book(str(tmp_path), "kraken", at_ts=9e9) == ({}, {})
    assert reconstruct_book(str(tmp_path / "missing"), "kraken", at_ts=9e9) == ({}, {})


def test_query_handles_quote_bearing_exchange_without_crashing(tmp_path):
    # exchange is interpolated into SQL; a quote-bearing value must not break the query.
    _seed(str(tmp_path))
    assert load_range(str(tmp_path), "x' OR '1'='1", 0.0, 9e9) == []
    assert reconstruct_book(str(tmp_path), "x' OR '1'='1", at_ts=9e9) == ({}, {})
