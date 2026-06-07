# tests/unit/test_runtime.py
import asyncio

from pavilos.core.runtime import Runtime, RuntimeConfig


class _FakeConnector:
    def __init__(self, exchange):
        self.exchange = exchange
    async def run(self, out_q, stop):
        await stop.wait()
    def health(self):
        from pavilos.connectors.base import ConnectorHealth
        return ConnectorHealth(self.exchange, True, 0.0, 0, 0)


def test_build_wires_full_graph_with_injected_connectors():
    built = {}
    rt = Runtime.build(RuntimeConfig(), connector_factory=lambda v, sym: _FakeConnector(v))
    # all 6 venues wired; the trading engine has the dashboard observer
    assert len(rt.engine._connectors) == 6
    assert rt.trading_engine.observer is not None
    assert rt.state.snapshot()["state"] == "IDLE"


def test_observer_feeds_wall_clock_not_feed_ts():
    # The staleness flag is only meaningful if the observer compares a real wall
    # clock against the feed ts. Inject a clock far ahead of the feed ts and assert
    # the served snapshot reports stale=True (the runtime must NOT pass now=ts).
    from pavilos.core.models import DepthBin, CombinedDepthSnapshot

    cfg = RuntimeConfig(staleness_s=15.0)
    rt = Runtime.build(cfg, connector_factory=lambda v, sym: _FakeConnector(v),
                       now=lambda: 1_000_000.0)  # wall clock far ahead of feed ts

    snap = CombinedDepthSnapshot(
        ts=1.0, mid=100.0,
        bids=(DepthBin(price=99.0, size=1.0, composition={"k": 1.0}),),
        asks=(DepthBin(price=101.0, size=1.0, composition={"k": 1.0}),),
        venues_active=("k",), venues_total=1)
    rt.trading_engine.process(snap)
    assert rt.state.snapshot()["stale"] is True


def test_observer_error_does_not_propagate_out_of_process():
    # A dashboard/serialization bug (here: state.update raising) must NEVER crash
    # the trading loop. Build a real Runtime, monkeypatch state.update to raise,
    # feed a fake snapshot through TradingEngine.process, and assert no exception
    # escapes (observer is best-effort telemetry).
    from pavilos.core.models import DepthBin, CombinedDepthSnapshot

    rt = Runtime.build(RuntimeConfig(), connector_factory=lambda v, sym: _FakeConnector(v))

    def boom(*args, **kwargs):
        raise RuntimeError("dashboard serialization bug")

    rt.state.update = boom  # type: ignore[assignment]

    snap = CombinedDepthSnapshot(
        ts=1.0, mid=100.0,
        bids=(DepthBin(price=99.0, size=1.0, composition={"k": 1.0}),),
        asks=(DepthBin(price=101.0, size=1.0, composition={"k": 1.0}),),
        venues_active=("k",), venues_total=1)

    # Must not raise — the observer swallows the error and trading continues.
    rt.trading_engine.process(snap)


def test_runtime_loads_history_and_publishes_summary(tmp_path):
    from pavilos.execution.broker import Trade
    from pavilos.execution.trade_log import TradeLog
    from pavilos.core.runtime import Runtime, RuntimeConfig
    p = tmp_path / "trades.jsonl"
    TradeLog(str(p)).append(Trade("LONG", 1.0, 100.0, 110.0, 1.0, 2.0, 10.0, 0.0, 10.0, "close"))

    class _FakeConnector:
        def __init__(self, ex): self.exchange = ex
        async def run(self, out_q, stop): await stop.wait()
        def health(self):
            from pavilos.connectors.base import ConnectorHealth
            return ConnectorHealth(self.exchange, True, 0.0, 0, 0)

    rt = Runtime.build(RuntimeConfig(trade_log_path=str(p)),
                       connector_factory=lambda v, sym: _FakeConnector(v))
    # a new trade closed this session is appended to the log AND the in-memory all-time list
    rt.trading_engine.broker.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    rt.trading_engine.broker.on_price(100.0, ts=5.0)
    rt.trading_engine.broker.on_price(105.0, ts=6.0)
    rt.trading_engine.broker.close(ts=7.0)
    assert len(TradeLog(str(p)).load()) == 2          # history (1) + this session (1) persisted
    assert len(rt.all_trades) == 2


def test_on_trade_persistence_failure_does_not_propagate_or_corrupt_broker(tmp_path):
    # A disk/IO failure inside the trade-log persistence (TradeLog.append raising)
    # must NEVER crash the trading path or strand the broker with a ghost position.
    # Build a real Runtime, monkeypatch trade_log.append to raise, open + stop-out a
    # position via the broker, and assert: no exception escapes, the broker is flat
    # (position cleared), a subsequent place_entry succeeds, and the in-memory
    # all_trades record was still appended (best-effort, persistence-independent).
    p = tmp_path / "trades.jsonl"
    rt = Runtime.build(RuntimeConfig(trade_log_path=str(p)),
                       connector_factory=lambda v, sym: _FakeConnector(v))

    def boom(_t):
        raise OSError("disk full")

    rt.trade_log.append = boom  # type: ignore[assignment]

    bk = rt.trading_engine.broker
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=1.0)   # entry fill
    # Stop-out drives _close_at -> _on_trade (which calls the failing append).
    # Must not raise out of the trading path.
    bk.on_price(97.0, ts=2.0)

    assert bk.position() is None                 # broker is flat (no ghost position)
    assert len(rt.all_trades) == 1              # in-memory record still kept
    # broker can be re-armed: a persistence failure left no half-updated state
    bk.place_entry("SHORT", trigger=95.0, stop=97.0, size=1.0)
    assert bk.pending_entry() is not None


def test_supervisor_restarts_a_crashing_trading_loop():
    rt = Runtime.build(RuntimeConfig(), connector_factory=lambda v, sym: _FakeConnector(v))
    calls = {"n": 0}

    async def flaky_run(q, stop):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        await stop.wait()

    rt.trading_engine.run = flaky_run  # type: ignore[assignment]

    async def scenario():
        stop = asyncio.Event()
        task = asyncio.create_task(rt._supervise_trading(stop, restart_delay=0.0))
        for _ in range(100):
            if calls["n"] >= 2:
                break
            await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return calls["n"]

    assert asyncio.run(scenario()) >= 2  # crashed once, restarted, then idled on stop
