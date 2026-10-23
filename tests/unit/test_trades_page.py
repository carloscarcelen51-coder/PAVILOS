# tests/unit/test_trades_page.py
"""Historical trades page (/trades): API, page, the Trade's pair, and the JS metrics
(run under Node) checked against an independent Python implementation."""
import json
import math
import random
import shutil
import statistics
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pavilos.execution.broker import DEFAULT_SYMBOL, PaperBroker, Trade
from pavilos.execution.trade_log import TradeLog, summarize
from pavilos.web.server import TRADE_FIELDS, create_app
from pavilos.web.state import DashboardState

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "src" / "pavilos" / "web" / "static"
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _trade(pnl=1.0, *, entry_ts=1.0, exit_ts=2.0, symbol=DEFAULT_SYMBOL):
    return Trade(side="LONG", size=1.0, entry=100.0, exit=100.0 + pnl, entry_ts=entry_ts, exit_ts=exit_ts,
                 pnl=pnl, fee=0.0, return_pct=pnl, reason="close", symbol=symbol)


# ── the Trade's pair ─────────────────────────────────────────────────────────
def test_log_lines_written_before_the_pair_existed_load_as_btc(tmp_path):
    p = tmp_path / "trades.jsonl"
    old = {"side": "LONG", "size": 1.0, "entry": 100.0, "exit": 101.0, "entry_ts": 1.0, "exit_ts": 2.0,
           "pnl": 1.0, "fee": 0.0, "return_pct": 1.0, "reason": "close"}
    p.write_text(json.dumps(old) + "\n", encoding="utf-8")
    TradeLog(str(p)).append(_trade(symbol="ETH/USD"))
    loaded = TradeLog(str(p)).load()
    assert [t.symbol for t in loaded] == ["BTC/USD", "ETH/USD"]
    assert json.loads(p.read_text(encoding="utf-8").splitlines()[1])["symbol"] == "ETH/USD"


def test_log_lines_from_a_newer_version_keep_loading(tmp_path):
    # A field this version doesn't know (e.g. "venue") must not drop the trade.
    p = tmp_path / "trades.jsonl"
    line = {"side": "SHORT", "size": 2.0, "entry": 100.0, "exit": 99.0, "entry_ts": 1.0, "exit_ts": 2.0,
            "pnl": 2.0, "fee": 0.0, "return_pct": 1.0, "reason": "stop", "symbol": "SOL/USD", "venue": "kraken"}
    p.write_text(json.dumps(line) + "\n" + "[1, 2]\n" + '{"side": "LONG"}\n', encoding="utf-8")
    loaded = TradeLog(str(p)).load()
    assert len(loaded) == 1 and loaded[0].symbol == "SOL/USD" and loaded[0].side == "SHORT"


def test_broker_stamps_its_pair_on_every_trade():
    for kwargs, expected in (({}, "BTC/USD"), ({"symbol": "ETH/USD"}, "ETH/USD")):
        bk = PaperBroker(starting_equity=1000.0, **kwargs)
        bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
        bk.on_price(100.0, ts=1.0)
        bk.on_price(101.0, ts=2.0)
        bk.close(ts=3.0)
        assert [t.symbol for t in bk.trades()] == [expected]


# ── /api/trades ──────────────────────────────────────────────────────────────
def test_api_trades_serves_everything_then_only_the_new_ones():
    trades = [_trade(1.0), _trade(-2.0, symbol="ETH/USD")]
    client = TestClient(create_app(DashboardState(), trades=lambda: trades, starting_equity=5000.0))
    body = client.get("/api/trades").json()
    assert body["fields"] == list(TRADE_FIELDS)
    assert body["total"] == 2 and body["since"] == 0 and body["starting_equity"] == 5000.0
    assert [r[0] for r in body["rows"]] == [0, 1]
    row = dict(zip(body["fields"], body["rows"][1]))
    assert row == {"id": 1, "symbol": "ETH/USD", "side": "LONG", "size": 1.0, "entry": 100.0, "exit": 98.0,
                   "entry_ts": 1.0, "exit_ts": 2.0, "pnl": -2.0, "fee": 0.0, "return_pct": -2.0,
                   "reason": "close", "context": None}
    assert body["ref_notional"] == 10_000.0 and body["stable_since"] > 0
    boot = body["boot"]

    trades.append(_trade(3.0))                           # closed after the page loaded
    new = client.get("/api/trades", params={"since": 2}).json()
    assert new["total"] == 3 and new["since"] == 2 and [r[0] for r in new["rows"]] == [2]
    assert new["boot"] == boot                           # same process: no reload
    assert client.get("/api/trades", params={"since": 3}).json()["rows"] == []


def test_api_trades_unknown_position_sends_everything():
    trades = [_trade(1.0)]
    client = TestClient(create_app(DashboardState(), trades=lambda: trades))
    for since in (5, -1):
        body = client.get("/api/trades", params={"since": since}).json()
        assert body["since"] == 0 and [r[0] for r in body["rows"]] == [0]


def test_api_trades_non_finite_values_go_out_as_null():
    bad = Trade(side="LONG", size=1.0, entry=100.0, exit=float("nan"), entry_ts=1.0, exit_ts=2.0,
                pnl=float("inf"), fee=0.0, return_pct=float("-inf"), reason="close")
    r = TestClient(create_app(DashboardState(), trades=lambda: [bad])).get("/api/trades")
    assert r.status_code == 200
    row = dict(zip(r.json()["fields"], r.json()["rows"][0]))
    assert row["exit"] is None and row["pnl"] is None and row["return_pct"] is None and row["entry"] == 100.0


def test_api_trades_sends_the_entry_context_json_safe():
    t = Trade(side="LONG", size=1.0, entry=100.0, exit=101.0, entry_ts=1.0, exit_ts=2.0, pnl=1.0, fee=0.0,
              return_pct=1.0, reason="close", context={"room_bps": 12.5, "stop_bps": float("nan"), "n": 3})
    body = TestClient(create_app(DashboardState(), trades=lambda: [t])).get("/api/trades").json()
    row = dict(zip(body["fields"], body["rows"][0]))
    assert row["context"] == {"room_bps": 12.5, "stop_bps": None, "n": 3}


def test_api_runs_merges_the_deduced_june_starts_with_the_lake_markers(tmp_path):
    from pavilos.web.history import PRE_SUPERVISOR_RUN_STARTS, STABLE_SETUP_SINCE
    runs = tmp_path / "_runs.jsonl"
    runs.write_text('{"start_ts": 1782843339.0, "pid": 1}\nnot json\n{"start_ts": 1792605326.29, "pid": 2}\n',
                    encoding="utf-8")
    body = TestClient(create_app(DashboardState(), runs_file=str(runs))).get("/api/runs").json()
    starts = [r["start"] for r in body["runs"]]
    assert starts == sorted(starts) and len(starts) == len(PRE_SUPERVISOR_RUN_STARTS) + 2
    assert [r["source"] for r in body["runs"]][:3] == ["deducido"] * 3
    assert body["runs"][-1] == {"start": 1792605326.29, "source": "registro"}
    assert body["stable_since"] == STABLE_SETUP_SINCE == PRE_SUPERVISOR_RUN_STARTS[1]
    none = TestClient(create_app(DashboardState())).get("/api/runs").json()
    assert len(none["runs"]) == len(PRE_SUPERVISOR_RUN_STARTS)


def test_api_trades_without_a_provider_is_empty():
    body = TestClient(create_app(DashboardState())).get("/api/trades").json()
    assert body["total"] == 0 and body["rows"] == [] and body["starting_equity"] == 10_000.0


def test_runtime_web_app_serves_the_loaded_history_and_new_trades(tmp_path):
    from pavilos.connectors.base import ConnectorHealth
    from pavilos.core.runtime import Runtime, RuntimeConfig

    class _FakeConnector:
        def __init__(self, ex): self.exchange = ex
        async def run(self, out_q, stop): await stop.wait()
        def health(self): return ConnectorHealth(self.exchange, True, 0.0, 0, 0)

    p = tmp_path / "trades.jsonl"
    TradeLog(str(p)).append(_trade(10.0))
    rt = Runtime.build(RuntimeConfig(trade_log_path=str(p), starting_equity=2500.0, book_data_dir=None),
                       connector_factory=lambda v, sym: _FakeConnector(v))
    client = TestClient(rt.web_app())
    body = client.get("/api/trades").json()
    assert body["total"] == 1 and body["starting_equity"] == 2500.0
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=5.0)
    bk.close(ts=7.0)
    new = client.get("/api/trades", params={"since": 1}).json()
    assert [r[0] for r in new["rows"]] == [1] and new["rows"][0][1] == "BTC/USD"
    assert client.get("/").status_code == 200            # the live page is still there


# ── pages ────────────────────────────────────────────────────────────────────
def test_trades_page_and_its_script_are_served():
    client = TestClient(create_app(DashboardState()))
    page = client.get("/trades")
    assert page.status_code == 200 and "text/html" in page.headers["content-type"]
    assert "/static/trades_metrics.js" in page.text and "/api/trades" in page.text
    js = client.get("/static/trades_metrics.js")
    assert js.status_code == 200 and "javascript" in js.headers["content-type"]
    assert "TradesMetrics" in js.text


def test_live_page_and_trades_page_link_to_each_other():
    client = TestClient(create_app(DashboardState()))
    live = client.get("/").text
    assert '<a href="/trades"' in live and "Trades históricos" in live
    assert '<a href="/">Panel en vivo</a>' in client.get("/trades").text


def test_main_page_is_served_byte_for_byte_from_index_html():
    html = TestClient(create_app(DashboardState())).get("/").text
    assert html == (STATIC / "index.html").read_text(encoding="utf-8")


# ── JS metrics ───────────────────────────────────────────────────────────────
@needs_node
def test_js_unit_tests_pass():
    r = subprocess.run([NODE, "--test", str(ROOT / "tests" / "js" / "trades_metrics.test.js")],
                       cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-4000:]


def _synthetic_trades(n=3000, seed=7):
    """Non-overlapping trades over ~2 months: both sides, two pairs, stops, a few flats."""
    rng = random.Random(seed)
    ts = datetime(2026, 8, 1, tzinfo=timezone.utc).timestamp()
    out = []
    for i in range(n):
        ts += rng.expovariate(1 / 1700.0)
        dur = rng.choice([0.3, 4.0, 10.0, 45.0, 600.0]) * rng.random() + 0.05
        side = "SHORT" if rng.random() < 0.1 else "LONG"
        entry = 60_000.0 + rng.gauss(0.0, 3000.0)
        size = rng.uniform(0.0001, 0.3)
        exit_ = entry * (1.0 + rng.gauss(0.0, 8.0) / 1e4)
        gross = size * ((exit_ - entry) if side == "LONG" else (entry - exit_))
        fee = size * (entry + exit_) * 0.0005
        pnl = gross - fee
        if i % 400 == 0:                                  # exact flats
            exit_, fee, pnl = entry, 0.0, 0.0
        out.append(Trade(side=side, size=size, entry=entry, exit=exit_, entry_ts=ts, exit_ts=ts + dur,
                         pnl=pnl, fee=fee, return_pct=pnl / (entry * size) * 100.0,
                         reason="stop" if rng.random() < 0.05 else "close",
                         symbol="ETH/USD" if rng.random() < 0.2 else "BTC/USD"))
        ts += dur
    return out


def _reference(trades, start):
    """Independent Python version of the JS metrics (UTC days)."""
    t = sorted(trades, key=lambda x: (x.exit_ts, x.entry_ts))
    pnl = [x.pnl for x in t]
    wins = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    bal = peak = start
    max_dd = max_dd_pct = 0.0
    run_w = run_l = best_w = best_l = 0
    for p in pnl:
        bal += p
        peak = max(peak, bal)
        max_dd = max(max_dd, peak - bal)
        max_dd_pct = max(max_dd_pct, (peak - bal) / peak * 100.0)
        if p > 0:
            run_w, run_l = run_w + 1, 0
        elif p < 0:
            run_l, run_w = run_l + 1, 0
        best_w, best_l = max(best_w, run_w), max(best_l, run_l)

    def day(ts):
        return datetime.fromtimestamp(ts, timezone.utc).date()

    per_day = defaultdict(float)
    for x in t:
        per_day[day(x.exit_ts)] += x.pnl
    first = min(x.entry_ts for x in t)
    last = t[-1].exit_ts
    d0, d1 = day(first), day(last)
    series = [per_day.get(d0 + timedelta(days=k), 0.0) for k in range((d1 - d0).days + 1)]
    mean, sd = statistics.fmean(series), statistics.stdev(series)
    downside = math.sqrt(sum(min(v, 0.0) ** 2 for v in series) / len(series))
    months = defaultdict(lambda: [0, 0.0])
    for x in t:
        k = datetime.fromtimestamp(x.exit_ts, timezone.utc).strftime("%Y-%m")
        months[k][0] += 1
        months[k][1] += x.pnl
    net = sum(pnl)
    return {
        "n": len(t), "net": net, "fees": sum(x.fee for x in t), "gross": net + sum(x.fee for x in t),
        "wins": len(wins), "losses": len(losses), "flats": len(t) - len(wins) - len(losses),
        "win_rate": len(wins) / len(t) * 100.0, "profit_factor": sum(wins) / -sum(losses),
        "expectancy": net / len(t), "final_balance": start + net, "roi_pct": net / start * 100.0,
        "max_dd": max_dd, "max_dd_pct": max_dd_pct, "max_win_streak": best_w, "max_loss_streak": best_l,
        "days_traded": len(per_day), "calendar_days": len(series),
        "best_day": max(per_day.values()), "worst_day": min(per_day.values()),
        "sharpe": mean / sd * math.sqrt(365), "sortino": mean / downside * math.sqrt(365),
        "exposure_pct": sum(x.exit_ts - x.entry_ts for x in t) / (last - first) * 100.0,
        "avg_duration": statistics.fmean(x.exit_ts - x.entry_ts for x in t),
        "median_duration": statistics.median(x.exit_ts - x.entry_ts for x in t),
        "period_days": (last - first) / 86400.0,
        "months": [[k, v[0], v[1]] for k, v in sorted(months.items())],
    }


@needs_node
def test_js_metrics_match_python_end_to_end(tmp_path):
    # log -> TradeLog.load -> /api/trades -> JS (Node) == independent Python == summarize()
    start = 10_000.0
    log = TradeLog(str(tmp_path / "trades.jsonl"))
    for t in _synthetic_trades():
        log.append(t)
    loaded = log.load()
    assert all(a.exit_ts <= b.entry_ts for a, b in zip(loaded, loaded[1:]))   # the reference assumes it
    api = TestClient(create_app(DashboardState(), trades=lambda: loaded, starting_equity=start)).get("/api/trades")
    body = api.json()
    inp = tmp_path / "in.json"
    inp.write_text(json.dumps({"fields": body["fields"], "rows": body["rows"], "start": start, "tz": "utc"}),
                   encoding="utf-8")
    r = subprocess.run([NODE, str(ROOT / "tests" / "js" / "compute_cli.js"), str(inp)],
                       capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert r.returncode == 0, r.stderr
    js = json.loads(r.stdout)

    ref = _reference(loaded, start)
    assert js["dropped"] == 0 and js["symbols"] == ["BTC/USD", "ETH/USD"]
    for k, v in ref.items():
        if k == "months":
            assert [m[:2] for m in js["months"]] == [m[:2] for m in v]
            for a, b in zip(js["months"], v):
                assert math.isclose(a[2], b[2], rel_tol=1e-9, abs_tol=1e-6), (a, b)
        elif isinstance(v, int):
            assert js[k] == v, (k, js[k], v)
        else:
            assert math.isclose(js[k], v, rel_tol=1e-9, abs_tol=1e-6), (k, js[k], v)

    s = summarize(loaded, base_equity=start)              # what the live page shows
    assert js["n"] == s["n_trades"] and js["wins"] == s["wins"] and js["losses"] == s["losses"]
    assert math.isclose(js["net"], s["realized_pnl"], rel_tol=1e-9, abs_tol=1e-6)
    assert math.isclose(js["win_rate"], s["win_rate"], rel_tol=1e-12)
    assert math.isclose(js["roi_pct"], s["return_pct"], rel_tol=1e-9, abs_tol=1e-9)
