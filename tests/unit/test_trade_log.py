# tests/unit/test_trade_log.py
from pavilos.execution.broker import Trade
from pavilos.execution.trade_log import TradeLog, summarize


def _t(pnl, reason="close"):
    return Trade(side="LONG", size=1.0, entry=100.0, exit=100.0 + pnl, entry_ts=1.0, exit_ts=2.0,
                 pnl=pnl, fee=0.1, return_pct=pnl, reason=reason)


def test_append_then_load_roundtrip(tmp_path):
    p = tmp_path / "trades.jsonl"
    log = TradeLog(str(p))
    assert log.load() == []                 # missing file -> empty
    log.append(_t(5.0)); log.append(_t(-2.0))
    loaded = log.load()
    assert len(loaded) == 2 and isinstance(loaded[0], Trade)
    assert loaded[0].pnl == 5.0 and loaded[1].pnl == -2.0


def test_load_skips_corrupt_lines(tmp_path):
    p = tmp_path / "trades.jsonl"
    p.write_text('{"bad json\n' + '{"side":"LONG","size":1.0,"entry":100.0,"exit":105.0,"entry_ts":1.0,'
                 '"exit_ts":2.0,"pnl":5.0,"fee":0.1,"return_pct":5.0,"reason":"close"}\n', encoding="utf-8")
    loaded = TradeLog(str(p)).load()
    assert len(loaded) == 1 and loaded[0].pnl == 5.0


def test_summarize_computes_pnl_winrate_return():
    s = summarize([_t(10.0), _t(-4.0), _t(6.0)], base_equity=1000.0)
    assert s["n_trades"] == 3 and s["wins"] == 2 and s["losses"] == 1
    assert abs(s["realized_pnl"] - 12.0) < 1e-9
    assert abs(s["win_rate"] - (2 / 3 * 100.0)) < 1e-9
    assert abs(s["return_pct"] - (12.0 / 1000.0 * 100.0)) < 1e-9


def test_summarize_empty():
    s = summarize([], base_equity=1000.0)
    assert s["n_trades"] == 0 and s["realized_pnl"] == 0.0 and s["win_rate"] == 0.0
