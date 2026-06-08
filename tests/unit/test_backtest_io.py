# tests/unit/test_backtest_io.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.backtest.io import snapshot_to_dict, snapshot_from_dict, dumps_snapshot, loads_snapshot


def _snap():
    return CombinedDepthSnapshot(
        ts=5.0, mid=100.0,
        bids=(DepthBin(price=99.0, size=10.0, composition={"kraken": 6.0, "binance": 4.0}),),
        asks=(DepthBin(price=101.0, size=2.0, composition={"okx": 2.0}),),
        venues_active=("kraken", "binance", "okx"), venues_total=6)


def test_dict_roundtrip_preserves_all_fields():
    s = _snap()
    d = snapshot_to_dict(s)
    assert d["ts"] == 5.0 and d["mid"] == 100.0 and d["venues_total"] == 6
    assert d["bids"][0] == [99.0, 10.0, {"kraken": 6.0, "binance": 4.0}]
    r = snapshot_from_dict(d)
    assert r.ts == s.ts and r.mid == s.mid and r.venues_total == s.venues_total
    assert r.venues_active == ("kraken", "binance", "okx")
    assert r.bids[0].price == 99.0 and r.bids[0].size == 10.0
    assert r.bids[0].composition == {"kraken": 6.0, "binance": 4.0}
    assert r.asks[0].price == 101.0


def test_jsonl_line_roundtrip():
    s = _snap()
    line = dumps_snapshot(s)
    assert "\n" not in line
    r = loads_snapshot(line)
    assert r.mid == 100.0 and r.bids[0].composition == {"kraken": 6.0, "binance": 4.0}
