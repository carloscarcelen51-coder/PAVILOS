# tests/unit/test_measurement.py
"""Truthful paper measurement (2026-10-23): fixed-notional sizing, the entry context on
every trade, the paper account surviving restarts, and live USDT/USDC rates."""
import asyncio
import json
import math

import pytest

from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.peg_feed import PEGS_FILE, PegFeed, parse_kraken_ticker
from pavilos.core.models import Quote
from pavilos.detection.models import DepthAnalysis, Side, Zone
from pavilos.execution.broker import PaperBroker, Trade
from pavilos.execution.paper_state import PaperState
from pavilos.execution.trade_log import TradeLog
from pavilos.signals.engine import SignalEngine
from pavilos.signals.sizing import fixed_notional_size


def _zone(side, price, low, high, conf=0.9, persistence_s=100.0, venues=("k", "b", "c")):
    return Zone(side=side, price=price, low=low, high=high, strength=20.0,
                venues=venues, persistence_s=persistence_s, pulled=False, confidence=conf)


def _analysis(ts, mid, supports=(), resistances=()):
    return DepthAnalysis(ts=ts, mid=mid, supports=tuple(supports), resistances=tuple(resistances))


def _engine(**over):
    kw = dict(entry_threshold=0.6, trail_threshold=0.6, opposing_threshold=0.7,
              min_persistence_s=5.0, min_venues=2, entry_offset_bps=2.0,
              stop_offset_bps=2.0, atr_stop_mult=3.0, opposing_distance_bps=30.0,
              risk_pct=0.01, max_leverage=10.0, entry_zone_bps=100.0, pending_timeout_s=10.0)
    kw.update(over)
    return SignalEngine(**kw)


# ── fixed-notional sizing ────────────────────────────────────────────────────
def test_fixed_notional_size():
    assert fixed_notional_size(10_000.0, entry=80_000.0) == pytest.approx(0.125)
    assert fixed_notional_size(0.0, entry=80_000.0) == 0.0
    assert fixed_notional_size(10_000.0, entry=0.0) == 0.0
    assert fixed_notional_size(float("nan"), entry=1.0) == 0.0


def test_fixed_notional_ignores_equity_even_when_the_account_is_below_zero():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0)
    bk.restore(equity=-5_000.0)                         # a fixed-size paper account can go negative
    e = _engine(fixed_notional=10_000.0)
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=0.01, broker=bk)
    pend = bk.pending_entry()
    assert pend is not None and pend["size"] * pend["trigger"] == pytest.approx(10_000.0)
    with pytest.raises(ValueError):
        _engine(fixed_notional=0.0)


def test_risk_sizing_is_unchanged_without_fixed_notional():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0)
    e = _engine()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    pend = bk.pending_entry()
    # 1 % of 10,000 over the stop distance (3.02), capped at 10x leverage (10 x 10,000 / trigger)
    assert pend["size"] == pytest.approx(min(100.0 / (pend["trigger"] - pend["stop"]), 10 * 10_000.0 / pend["trigger"]))
    assert pend["size"] == pytest.approx(100.0 / 3.02)


# ── entry context on the trade ───────────────────────────────────────────────
def test_the_trade_carries_what_the_strategy_saw_at_entry():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005)
    e = _engine(fixed_notional=10_000.0)
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2, conf=0.85)
    res = _zone(Side.RESISTANCE, price=101.0, low=100.9, high=101.1, conf=0.8)
    e.update(_analysis(1.0, mid=100.0, supports=[sup], resistances=[res]), atr=0.01, broker=bk)
    e.update(_analysis(2.0, mid=100.03, supports=[sup], resistances=[res]), atr=0.01, broker=bk)  # fills
    assert e.state == "IN_POSITION"
    bk.close(ts=3.0)
    (t,) = bk.trades()
    c = t.context
    assert c["mode"] == "momentum" and c["sizing"] == "fixed"
    assert c["arm_mid"] == 100.0 and c["entry_ref"] == pytest.approx(100.02)
    assert c["zone_conf"] == 0.85 and c["zone_venues"] == 3 and c["zone_persist_s"] == 100.0
    assert c["zone_dist_bps"] == pytest.approx(80.0)                    # (100 - 99.2) / 100
    assert c["room_bps"] == pytest.approx((100.9 - 100.02) / 100.02 * 1e4, abs=0.01)
    assert c["stop_bps"] > 0 and c["equity"] == 10_000.0
    # and it survives the trade log
    tl = TradeLog(str(_tmp_file("ctx.jsonl")))
    tl.append(t)
    assert tl.load()[0].context == c


def _tmp_file(name):
    import tempfile
    from pathlib import Path
    return Path(tempfile.mkdtemp()) / name


# ── paper account across restarts ────────────────────────────────────────────
def test_broker_state_round_trip_and_restart_close():
    saved = []
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, on_state=saved.append)
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=10.0, context={"room_bps": 50.0})
    bk.on_price(100.0, ts=10.0)                         # fills: equity pays the entry fee
    assert saved[-1]["position"]["entry_ts"] == 10.0 and saved[-1]["position"]["context"] == {"room_bps": 50.0}
    state = saved[-1]

    trades = []
    bk2 = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, on_trade=trades.append, on_state=saved.append)
    bk2.restore(equity=state["equity"], position=state["position"])
    assert bk2.position() is not None
    bk2.on_price(103.0, ts=5_000.0)                     # first price after the restart
    (t,) = trades
    assert t.reason == "restart" and t.entry_ts == 10.0 and t.exit_ts == 5_000.0 and t.exit == 103.0
    entry_fee, exit_fee = 10 * 100.0 * 0.0005, 10 * 103.0 * 0.0005
    assert t.pnl == pytest.approx(30.0 - entry_fee - exit_fee)
    assert t.context == {"room_bps": 50.0}
    assert bk2.position() is None and saved[-1]["position"] is None
    assert bk2.equity() == pytest.approx(10_000.0 + t.pnl)
    bk2.on_price(104.0, ts=5_001.0)                     # only once
    assert len(trades) == 1


def test_restore_validates_and_accepts_negative_equity():
    bk = PaperBroker(starting_equity=10_000.0)
    bk.restore(equity=-123.0)
    assert bk.equity() == -123.0
    with pytest.raises(ValueError):
        PaperBroker(starting_equity=1.0).restore(equity=float("inf"))
    with pytest.raises(ValueError):
        PaperBroker(starting_equity=1.0).restore(equity=1.0, position={"side": "UP", "size": 1, "entry": 1,
                                                                       "stop": 1, "entry_ts": 0})
    busy = PaperBroker(starting_equity=1.0)
    busy.place_entry("LONG", trigger=10.0, stop=9.0, size=1.0)
    with pytest.raises(RuntimeError):
        busy.restore(equity=1.0)


def test_paper_state_file_round_trip_and_unreadable_file_is_kept(tmp_path):
    p = tmp_path / "paper_state.json"
    st = PaperState(str(p))
    assert st.load() is None
    st.save({"equity": 9_000.5, "position": None})
    assert st.load()["equity"] == 9_000.5 and not (tmp_path / "paper_state.json.tmp").exists()
    p.write_text("{not json", encoding="utf-8")
    assert st.load() is None
    kept = list(tmp_path.glob("paper_state.json.unreadable-*"))
    assert len(kept) == 1 and kept[0].read_text(encoding="utf-8") == "{not json"
    with pytest.raises(ValueError):
        st.save({"equity": float("nan"), "position": None})


def _runtime(tmp_path, *, now=None, **kw):
    from pavilos.connectors.base import ConnectorHealth
    from pavilos.core.runtime import Runtime, RuntimeConfig

    class _FakeConnector:
        def __init__(self, ex): self.exchange = ex
        async def run(self, out_q, stop): await stop.wait()
        def health(self): return ConnectorHealth(self.exchange, True, 0.0, 0, 0)

    cfg = RuntimeConfig(trade_log_path=str(tmp_path / "trades.jsonl"), book_data_dir=None, peg_live=False,
                        paper_state_path=str(tmp_path / "paper_state.json"), **kw)
    extra = {"now": now} if now is not None else {}
    return Runtime.build(cfg, connector_factory=lambda v, sym: _FakeConnector(v), **extra)


def test_after_a_long_outage_an_orphan_position_closes_as_restart(tmp_path):
    rt = _runtime(tmp_path, now=lambda: 1_000.0)
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1_000.0)                      # open ... and the process "dies" here
    saved = json.loads((tmp_path / "paper_state.json").read_text(encoding="utf-8"))
    assert saved["position"]["side"] == "LONG" and saved["last_alive"] == 1_000.0

    rt2 = _runtime(tmp_path, now=lambda: 1_000.0 + 3_600)  # an hour later: what happened is unknown
    bk2 = rt2.trading_engine.broker
    assert bk2.equity() == pytest.approx(saved["equity"]) and bk2.position() is not None
    bk2.on_price(101.0, ts=4_600.0)
    logged = TradeLog(str(tmp_path / "trades.jsonl")).load()
    assert [t.reason for t in logged] == ["restart"] and rt2.all_trades[-1].reason == "restart"
    rt3 = _runtime(tmp_path, now=lambda: 4_700.0)       # nothing left open: a plain continuation
    assert rt3.trading_engine.broker.position() is None
    assert rt3.trading_engine.broker.equity() == pytest.approx(bk2.equity())


def test_after_a_short_outage_the_position_goes_on_with_its_saved_stop(tmp_path):
    rt = _runtime(tmp_path, now=lambda: 1_000.0)
    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=95.0, size=1.0)
    bk.on_price(100.0, ts=1_000.0)
    bk.modify_stop(97.0)                                # trailing move: saved too

    rt2 = _runtime(tmp_path, now=lambda: 1_000.0 + 20)  # 20 s later: the supervisor relaunched it
    bk2, sig2 = rt2.trading_engine.broker, rt2.trading_engine.signal
    assert bk2.position() is not None and bk2.position().stop == 97.0
    assert sig2.state == "IN_POSITION"
    bk2.on_price(100.5, ts=1_021.0)                     # inside the stop: it simply goes on
    assert bk2.position() is not None and TradeLog(str(tmp_path / "trades.jsonl")).load() == []
    bk2.on_price(96.0, ts=1_022.0)                      # the saved stop still protects it
    (t,) = TradeLog(str(tmp_path / "trades.jsonl")).load()
    assert t.reason == "stop" and t.entry_ts == 1_000.0 and t.exit == 96.0


def test_runtime_does_not_duplicate_a_trade_already_in_the_log(tmp_path):
    # died after logging the close but before saving the state
    TradeLog(str(tmp_path / "trades.jsonl")).append(
        Trade("LONG", 1.0, 100.0, 101.0, 7.0, 8.0, 1.0, 0.0, 1.0, "close"))
    PaperState(str(tmp_path / "paper_state.json")).save(
        {"equity": 10_001.0, "position": {"side": "LONG", "size": 1.0, "entry": 100.0, "stop": 95.0,
                                         "entry_ts": 7.0, "entry_fee": 0.0, "context": None}})
    rt = _runtime(tmp_path)
    assert rt.trading_engine.broker.position() is None
    assert rt.trading_engine.broker.equity() == 10_001.0


def test_runtime_without_a_state_path_starts_fresh_every_time(tmp_path):
    from pavilos.core.runtime import RuntimeConfig
    assert RuntimeConfig().paper_state_path is None and RuntimeConfig().fixed_notional == 10_000.0


# ── live USDT/USDC rates ─────────────────────────────────────────────────────
KRAKEN_BODY = {"error": [], "result": {
    "USDTZUSD": {"a": ["0.99879", "1", "1.0"], "b": ["0.99878", "1", "1.0"]},
    "USDCUSD": {"a": ["0.99990", "1", "1.0"], "b": ["0.99980", "1", "1.0"]}}}


def test_parse_kraken_ticker():
    r = parse_kraken_ticker(KRAKEN_BODY)
    assert r["USDT"] == pytest.approx(0.998785) and r["USDC"] == pytest.approx(0.99985)
    with pytest.raises(RuntimeError):
        parse_kraken_ticker({"error": ["EGeneral:Too many requests"]})


def test_peg_feed_applies_records_and_rejects_bad_readings(tmp_path):
    peg = PegProvider()
    readings = [{"USDT": 0.998785, "USDC": 0.99985}, {"USDT": 0.5}, RuntimeError("down"), {"USDT": 0.9990}]

    async def fetch():
        r = readings.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    feed = PegFeed(peg, lake_dir=str(tmp_path), fetch=fetch, now=lambda: 1_000.0)

    async def go():
        assert await feed.refresh_once() is True
        assert peg.to_usd(80_000.0, Quote.USDT) == pytest.approx(80_000.0 * 0.998785)
        assert peg.to_usd(1.0, Quote.USDC) == pytest.approx(0.99985)
        assert await feed.refresh_once() is False      # 0.5 is not a stablecoin reading
        assert peg.to_usd(1.0, Quote.USDT) == pytest.approx(0.998785)
        assert await feed.refresh_once() is False      # network error keeps the last good rate
        assert peg.to_usd(1.0, Quote.USDT) == pytest.approx(0.998785)
        assert await feed.refresh_once() is True
        assert peg.to_usd(1.0, Quote.USDT) == pytest.approx(0.9990)

    asyncio.run(go())
    lines = [json.loads(x) for x in (tmp_path / PEGS_FILE).read_text(encoding="utf-8").splitlines()]
    assert lines == [{"ts": 1000.0, "USDT": 0.998785, "USDC": 0.99985, "src": "kraken"},
                     {"ts": 1000.0, "USDT": 0.999, "src": "kraken"}]
    assert peg.to_usd(1.0, Quote.USD) == 1.0


def test_peg_feed_run_stops_promptly():
    calls = []

    async def fetch():
        calls.append(1)
        return {"USDT": 0.999}

    async def go():
        stop = asyncio.Event()
        feed = PegFeed(PegProvider(), lake_dir=None, interval_s=3600, fetch=fetch)
        task = asyncio.create_task(feed.run(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(go())
    assert calls == [1]


def test_runtime_feeds_the_aggregators_own_peg_provider(tmp_path):
    from pavilos.connectors.base import ConnectorHealth
    from pavilos.core.runtime import Runtime, RuntimeConfig

    class _FakeConnector:
        def __init__(self, ex): self.exchange = ex
        async def run(self, out_q, stop): await stop.wait()
        def health(self): return ConnectorHealth(self.exchange, True, 0.0, 0, 0)

    rt = Runtime.build(RuntimeConfig(trade_log_path=str(tmp_path / "t.jsonl"), book_data_dir=None),
                       connector_factory=lambda v, sym: _FakeConnector(v))
    assert rt.peg_feed is not None
    assert rt.peg_feed._peg is rt.engine._aggregator._peg
    off = Runtime.build(RuntimeConfig(trade_log_path=str(tmp_path / "t.jsonl"), book_data_dir=None, peg_live=False),
                        connector_factory=lambda v, sym: _FakeConnector(v))
    assert off.peg_feed is None
    assert math.isclose(off.engine._aggregator._peg.to_usd(1.0, Quote.USDT), 1.0)
