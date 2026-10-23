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
    # all 14 venues wired; the trading engine has the dashboard observer
    assert len(rt.engine._connectors) == 14
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


def test_recording_defaults_are_eternal_with_a_disk_floor():
    cfg = RuntimeConfig()
    assert cfg.book_retention_days is None          # nothing is ever deleted automatically
    assert cfg.book_min_free_gb == 20.0             # operator's decision (2026-10-23); was 100
    assert cfg.book_checkpoint_every_s == 60.0
    assert cfg.book_janitor_every_s == 600.0


def _recording_runtime(tmp_path, **kw):
    kw.setdefault("peg_live", False)          # no network in tests
    cfg = RuntimeConfig(book_data_dir=str(tmp_path / "lake"), **kw)
    return Runtime.build(cfg, connector_factory=lambda v, sym: _FakeConnector(v))


def test_start_recording_with_eternal_retention_runs_and_stops_cleanly(tmp_path):
    # Regression: run_app used to call prune_old_partitions(dir, None) -> TypeError at startup.
    import os
    rt = _recording_runtime(tmp_path, book_min_free_gb=None)
    old = tmp_path / "lake" / "exchange=kraken" / "date=2001-01-01" / "00"
    old.mkdir(parents=True)
    janitor = rt._start_recording()
    try:
        assert rt.recorder._thread.is_alive() and not rt.recorder.paused
        assert janitor._thread.is_alive()
    finally:
        rt._stop_recording(janitor)
    assert not rt.recorder._thread.is_alive() and not janitor._thread.is_alive()
    assert os.path.isdir(old)                       # eternal: untouched


def test_start_recording_below_the_floor_starts_paused(tmp_path):
    rt = _recording_runtime(tmp_path, book_min_free_gb=1e12)   # no drive has 1e12 GB free
    janitor = rt._start_recording()
    try:
        assert rt.recorder.paused
    finally:
        rt._stop_recording(janitor)


def test_checkpoints_are_enabled_only_when_recording(tmp_path):
    rec = _recording_runtime(tmp_path)
    assert rec.recorder is not None and rec.engine._checkpoint_every_s == 60.0
    off = Runtime.build(RuntimeConfig(book_data_dir=None),
                        connector_factory=lambda v, sym: _FakeConnector(v))
    assert off.recorder is None and off.engine._checkpoint_every_s is None


def test_run_app_checks_retention_before_ingest_and_stops_the_janitor(tmp_path, monkeypatch):
    """run_app must take the retention pass (disk floor) BEFORE the engine starts, then
    stop the janitor and the recorder on the way out."""
    import uvicorn
    from pavilos.persistence import retention

    calls = []

    class _Server:
        def __init__(self, config):
            self.should_exit = False
        async def serve(self):
            await asyncio.sleep(0.05)       # the app "serves" briefly, then returns

    class _Config:
        def __init__(self, *a, **k):
            calls.append(("config", k.get("log_config", "missing")))

    monkeypatch.setattr(uvicorn, "Server", _Server)
    monkeypatch.setattr(uvicorn, "Config", _Config)
    real_run_once = retention.RetentionJanitor.run_once

    def spy_run_once(self):
        calls.append("retention")
        return real_run_once(self)

    monkeypatch.setattr(retention.RetentionJanitor, "run_once", spy_run_once)
    rt = _recording_runtime(tmp_path, book_min_free_gb=None)
    real_start = rt.engine.start

    async def spy_start():
        calls.append("engine")
        await real_start()

    rt.engine.start = spy_start
    started = {}
    real_start_recording = rt._start_recording

    def spy_start_recording():
        j = real_start_recording()
        started["janitor"] = j
        return j

    rt._start_recording = spy_start_recording
    asyncio.run(rt.run_app())
    assert ("config", None) in calls                  # uvicorn logs through our handlers
    calls = [c for c in calls if isinstance(c, str)]
    assert calls[:2] == ["retention", "engine"]
    assert not started["janitor"]._thread.is_alive()
    assert not rt.recorder._thread.is_alive()


def test_recorder_rebases_after_the_aggregator_staleness(tmp_path):
    rt = _recording_runtime(tmp_path, staleness_s=12.0)
    assert rt.recorder._encoder._rebase_after_s == 12.0


def test_start_recording_marks_the_run_and_floors_the_encoder(tmp_path):
    from pavilos.persistence.runs import run_starts
    rt = _recording_runtime(tmp_path, book_min_free_gb=None)
    janitor = rt._start_recording()
    try:
        starts = run_starts(str(tmp_path / "lake"), 0.0, 1e12)
        assert len(starts) == 1 and rt.recorder._encoder._floor == starts[0]
    finally:
        rt._stop_recording(janitor)


def test_the_dashboard_shows_the_recorders_status(tmp_path):
    from pavilos.core.models import DepthBin, CombinedDepthSnapshot
    rt = _recording_runtime(tmp_path)
    rt.recorder.set_paused(True)
    snap = CombinedDepthSnapshot(ts=1.0, mid=100.0,
                                 bids=(DepthBin(price=99.0, size=1.0, composition={"k": 1.0}),),
                                 asks=(DepthBin(price=101.0, size=1.0, composition={"k": 1.0}),),
                                 venues_active=("k",), venues_total=1)
    rt.trading_engine.process(snap)
    rec = rt.state.snapshot()["recording"]
    assert rec["paused"] is True and rec == rt.recorder.status()
