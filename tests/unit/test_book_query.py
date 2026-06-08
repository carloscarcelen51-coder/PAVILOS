from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.query import load_range, reconstruct_book


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
