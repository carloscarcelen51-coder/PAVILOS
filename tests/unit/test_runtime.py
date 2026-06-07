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
