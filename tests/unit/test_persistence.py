# tests/unit/test_persistence.py
"""Everything persists (2026-10-23): measurement periods (count from now without deleting
anything), stop moves and a heartbeat in the paper state, a save + recorder flush when
Windows ends the session, and a clean planned restart."""
import asyncio
import json
import os

import pytest
from fastapi.testclient import TestClient

from pavilos.execution.broker import PaperBroker, Trade
from pavilos.execution.paper_state import PaperState
from pavilos.execution.trade_log import TradeLog


def _runtime(tmp_path, *, now=None, **kw):
    from pavilos.connectors.base import ConnectorHealth
    from pavilos.core.runtime import Runtime, RuntimeConfig

    class _FakeConnector:
        def __init__(self, ex): self.exchange = ex
        async def run(self, out_q, stop): await stop.wait()
        def health(self): return ConnectorHealth(self.exchange, True, 0.0, 0, 0)

    kw.setdefault("paper_state_path", str(tmp_path / "paper_state.json"))
    cfg = RuntimeConfig(trade_log_path=str(tmp_path / "trades.jsonl"), book_data_dir=None, peg_live=False, **kw)
    extra = {"now": now} if now is not None else {}
    return Runtime.build(cfg, connector_factory=lambda v, sym: _FakeConnector(v), **extra)


def _trade(entry_ts, pnl, context=None):
    return Trade("LONG", 1.0, 100.0, 100.0 + pnl, entry_ts, entry_ts + 5, pnl, 0.0, pnl, "close", context=context)


# ── measurement period ───────────────────────────────────────────────────────
def test_an_old_state_gets_the_period_of_the_truthful_measurement(tmp_path):
    log = TradeLog(str(tmp_path / "trades.jsonl"))
    log.append(_trade(100.0, -5.0))                         # history (no context)
    log.append(_trade(500.0, -1.0, context={"room_bps": 9.0}))  # first trade of the new measurement
    (tmp_path / "paper_state.json").write_text(json.dumps(
        {"version": 1, "saved_ts": 600.0, "equity": 9_999.0, "position": None}), encoding="utf-8")
    rt = _runtime(tmp_path, now=lambda: 700.0)
    assert rt.period["since"] == 499.0 and rt.period["equity0"] == 10_000.0
    saved = json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))
    assert saved["version"] == 2 and saved["period"]["since"] == 499.0 and saved["last_alive"] == 700.0
    summary = _summary(rt)
    assert summary["n_trades"] == 1 and summary["since"] == 499.0       # counts from the period only
    assert len(rt.all_trades) == 2                                       # nothing is dropped


def _summary(rt):
    from pavilos.core.models import CombinedDepthSnapshot, DepthBin
    snap = CombinedDepthSnapshot(ts=1.0, mid=100.0, bids=(DepthBin(price=99.0, size=1.0, composition={"k": 1.0}),),
                                 asks=(DepthBin(price=101.0, size=1.0, composition={"k": 1.0}),),
                                 venues_active=("k",), venues_total=1)
    rt.trading_engine.process(snap)
    return rt.state.snapshot()["summary"]


def test_without_a_saved_account_the_summary_is_the_whole_history(tmp_path):
    log = TradeLog(str(tmp_path / "trades.jsonl"))
    log.append(_trade(100.0, -5.0))
    rt = _runtime(tmp_path, paper_state_path=None)
    assert _summary(rt)["n_trades"] == 1 and rt.period["since"] == 0.0


def test_a_new_period_resets_only_the_cash_and_keeps_everything(tmp_path):
    clock = {"t": 1_000.0}
    rt = _runtime(tmp_path, now=lambda: clock["t"])
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1_000.0)
    clock["t"] = 1_050.0
    with pytest.raises(RuntimeError):                       # never with a position open
        rt.start_new_period()
    bk.on_price(90.0, ts=1_051.0)                           # stopped out: -10 + fees
    equity_end = bk.equity()
    bk.place_entry("LONG", trigger=105.0, stop=95.0, size=1.0)   # a pending entry is dropped
    clock["t"] = 2_000.0
    info = rt.start_new_period("prueba")
    assert info["since"] == 2_000.0 and info["equity0"] == 10_000.0 and info["equity"] == 10_000.0
    assert bk.equity() == 10_000.0 and bk.pending_entry() is None
    assert rt.period["history"][-1]["equity_end"] == pytest.approx(equity_end)
    assert len(TradeLog(str(tmp_path / "trades.jsonl")).load()) == 1    # the trade is still there
    saved = json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))
    assert saved["equity"] == 10_000.0 and saved["period"]["since"] == 2_000.0
    assert saved["period"]["note"] == "prueba" and len(saved["period"]["history"]) == 1
    assert _summary(rt)["n_trades"] == 0                    # counting from the new period
    rt2 = _runtime(tmp_path, now=lambda: 2_100.0)          # and it survives a restart
    assert rt2.period["since"] == 2_000.0 and rt2.trading_engine.broker.equity() == 10_000.0


def test_stop_moves_are_saved_at_most_once_a_second(tmp_path):
    clock = {"t": 1_000.0}
    rt = _runtime(tmp_path, now=lambda: clock["t"])
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1_000.0)

    def saved_stop():
        return json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))["position"]["stop"]

    clock["t"] = 1_001.5
    bk.modify_stop(96.0)
    assert saved_stop() == 96.0
    clock["t"] = 1_001.9
    bk.modify_stop(97.0)                                    # 0.4 s later: not saved yet
    assert saved_stop() == 96.0
    clock["t"] = 1_003.0
    bk.modify_stop(98.0)
    assert saved_stop() == 98.0
    rt.save_state_now()                                     # the heartbeat saves whatever stands
    assert saved_stop() == 98.0


def test_reset_cash_rules():
    bk = PaperBroker(starting_equity=10_000.0)
    with pytest.raises(ValueError):
        bk.reset_cash(0.0)
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1.0)
    with pytest.raises(RuntimeError):
        bk.reset_cash(10_000.0)


def test_signal_engine_resume_and_cancel():
    from pavilos.signals.engine import SignalEngine
    e = SignalEngine(entry_threshold=0.6, trail_threshold=0.6, opposing_threshold=0.7, min_persistence_s=5.0,
                     min_venues=2, entry_offset_bps=2.0, stop_offset_bps=2.0, atr_stop_mult=3.0,
                     opposing_distance_bps=8.0, risk_pct=0.01, max_leverage=10.0, entry_zone_bps=30.0,
                     pending_timeout_s=10.0)
    e.resume_position("SHORT", tp=95.0)
    assert e.state == "IN_POSITION" and e._dir == "SHORT" and e._tp == 95.0
    with pytest.raises(ValueError):
        e.resume_position("UP")
    e.state = "PENDING_ENTRY"
    e.cancel_pending()
    assert e.state == "IDLE"


def test_paper_state_v2_round_trip_and_bad_period(tmp_path):
    st = PaperState(str(tmp_path / "s.json"), now=lambda: 42.0)
    st.period = {"since": 10.0, "equity0": 10_000.0, "history": []}
    st.save({"equity": 1.0, "position": None, "stop_only": True})
    got = st.load()
    assert got["period"]["since"] == 10.0 and got["last_alive"] == 42.0
    (tmp_path / "s.json").write_text(json.dumps({"equity": 1.0, "period": {"since": "x"}}), encoding="utf-8")
    assert st.load() is None and list(tmp_path.glob("s.json.unreadable-*"))


# ── dashboard endpoints ──────────────────────────────────────────────────────
def test_paper_endpoints(tmp_path):
    rt = _runtime(tmp_path)
    client = TestClient(rt.web_app())
    info = client.get("/api/paper").json()
    assert info["available"] and info["equity0"] == 10_000.0 and info["persisted"] is True
    assert client.post("/api/paper/new-period", json={"note": "x"}).status_code == 403   # header missing
    r = client.post("/api/paper/new-period", json={"note": "desde ahora"}, headers={"X-Pavilos": "1"})
    assert r.status_code == 200 and r.json()["ok"] and r.json()["equity"] == 10_000.0
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1.0)
    r = client.post("/api/paper/new-period", headers={"X-Pavilos": "1"})
    assert r.status_code == 409 and "posición abierta" in r.json()["error"]
    assert client.post("/api/admin/restart").status_code == 403
    r = client.post("/api/admin/restart", headers={"X-Pavilos": "1"})
    assert r.status_code == 202 and rt.restart_requested is True


def test_endpoints_without_a_runtime_say_so():
    from pavilos.web.server import create_app
    from pavilos.web.state import DashboardState
    c = TestClient(create_app(DashboardState()))
    assert c.get("/api/paper").json() == {"available": False}
    assert c.post("/api/paper/new-period", headers={"X-Pavilos": "1"}).status_code == 503
    assert c.post("/api/admin/restart", headers={"X-Pavilos": "1"}).status_code == 503


# ── planned restart and session end ──────────────────────────────────────────
def test_main_exits_with_the_restart_code_when_a_restart_was_requested(monkeypatch, tmp_path):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "setup_logging", lambda: tmp_path / "x.log")

    class _Rt:
        restart_requested = False
        async def run_app(self):
            self.restart_requested = True

    monkeypatch.setattr(m.Runtime, "build", classmethod(lambda cls, cfg: _Rt()))
    with pytest.raises(SystemExit) as e:
        m.main()
    assert e.value.code == m.RESTART_EXIT_CODE != 0          # 0 would stop the supervisor


def test_session_end_saves_the_account_and_flushes_the_recorder(tmp_path):
    rt = _runtime(tmp_path, now=lambda: 5_000.0)
    flushed = []

    class _Rec:
        def stop(self, timeout):
            flushed.append(timeout)

    rt.recorder = _Rec()
    os.remove(tmp_path / "paper_state.json")
    rt._on_session_end("windows-endsession")
    assert flushed and flushed[0] <= 5.0
    assert json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))["last_alive"] == 5_000.0


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_shutdown_watcher_reacts_to_the_real_windows_message():
    import ctypes
    from pavilos.core.shutdown_watch import WM_ENDSESSION, WM_QUERYENDSESSION, ShutdownWatcher
    calls = []
    w = ShutdownWatcher(calls.append, class_name="PavilosTestWatcher")
    assert w.start()
    try:
        u = ctypes.WinDLL("user32")
        u.FindWindowW.restype = ctypes.c_void_p
        u.SendMessageW.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_size_t, ctypes.c_ssize_t]
        u.SendMessageW.restype = ctypes.c_ssize_t
        h = u.FindWindowW(w.class_name, None)
        assert h and not u.IsWindowVisible(ctypes.c_void_p(h))
        assert u.SendMessageW(h, WM_QUERYENDSESSION, 0, 0) == 1      # never blocks a shutdown
        u.SendMessageW(h, WM_ENDSESSION, 0, 0)                       # cancelled: nothing
        assert calls == []
        u.SendMessageW(h, WM_ENDSESSION, 1, 0)
        u.SendMessageW(h, WM_ENDSESSION, 1, 0)
        assert calls == ["windows-endsession"]                       # once
    finally:
        w.stop()
    assert not w._thread.is_alive()


def test_run_app_heartbeat_and_clean_stop_save_the_account(tmp_path, monkeypatch):
    import uvicorn
    import pavilos.core.runtime as runtime_mod

    class _Server:
        def __init__(self, config):
            self.should_exit = False
        async def serve(self):
            while not self.should_exit:
                await asyncio.sleep(0.01)

    monkeypatch.setattr(uvicorn, "Server", _Server)
    monkeypatch.setattr(uvicorn, "Config", lambda *a, **k: None)
    clock = {"t": 100.0}
    rt = _runtime(tmp_path, now=lambda: clock["t"], paper_heartbeat_s=0.05, shutdown_watch=False)

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(rt.run_app(stop))
        await asyncio.sleep(0.02)
        clock["t"] = 200.0
        await asyncio.sleep(0.15)                              # a few heartbeats
        alive = json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))["last_alive"]
        rt.request_restart()                                   # the planned-restart path
        await asyncio.wait_for(task, timeout=5)
        return alive

    assert asyncio.run(go()) == 200.0
    assert rt.restart_requested is True
    assert runtime_mod is not None
