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
