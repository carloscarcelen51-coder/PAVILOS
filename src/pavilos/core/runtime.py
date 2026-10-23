# src/pavilos/core/runtime.py
"""Assemble the live PAVILOS object graph and run it: Engine (12 venues) ->
Detector -> SignalEngine -> PaperBroker, publishing each tick to a DashboardState,
served by a FastAPI/uvicorn dashboard. Bounded shutdown; supervised trading loop."""
from __future__ import annotations

import asyncio
import bisect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.core.engine import Engine
from pavilos.connectors.peg_feed import PegFeed
from pavilos.connectors.venues import VENUE_SPECS, build_connector
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.execution.broker import PaperBroker
from pavilos.execution.paper_state import PaperState
from pavilos.execution.trade_log import TradeLog, summarize
from pavilos.core.trading_engine import TradingEngine
from pavilos.web.state import DashboardState

_log = logging.getLogger(__name__)

_SYMBOLS = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
            "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd",
            "gate": "BTC/USDT", "mexc": "BTC/USDT", "cryptocom": "BTC/USDT",
            "bitget": "BTC/USDT", "kucoin": "BTC/USDT", "htx": "BTC/USDT",
            "bitfinex": "BTC/USD", "gemini": "BTC/USD"}


def _wall_now() -> float:
    import time
    return time.time()


def _first_period(all_trades, starting_equity: float, now: float) -> dict:
    """The period of a paper state saved before periods existed (2026-10-23): it began with the
    truthful measurement, i.e. just before the first trade that carries an entry context."""
    first = next((t.entry_ts for t in all_trades if getattr(t, "context", None)), None)
    return {"since": (first - 1.0) if first is not None else now, "equity0": starting_equity, "history": [],
            "note": "medición veraz (23-10)"}


def _loop_exception_handler(loop, context: dict) -> None:
    """Log an unhandled exception that surfaced in an asyncio callback/task (e.g. a
    flaky venue's internal ccxt WS callback) and let the loop KEEP RUNNING — one bad
    venue must never silently kill 24/7 recording. Never raises; ignores benign
    cancellation/shutdown. The outer supervisor handles whole-process death."""
    exc = context.get("exception")
    if isinstance(exc, (asyncio.CancelledError, SystemExit, KeyboardInterrupt)):
        return
    msg = context.get("message") or "unhandled asyncio exception"
    _log.error("asyncio loop exception (logged, recorder continues): %s", msg, exc_info=exc)


@dataclass(frozen=True)
class RuntimeConfig:
    symbols: dict = field(default_factory=lambda: dict(_SYMBOLS))
    starting_equity: float = 10_000.0
    trade_log_path: str = "paper_trades.jsonl"
    bin_bps: float = 5.0
    window_bps: float = 300.0   # ±3% aggregate window — calibrated 2026-06-07 from a live probe:
                                # ±50bps was too tight (flat book -> no walls stood out -> 0 zones -> 0 trades);
                                # at ±300bps the top bin is ~36x the median and zones surface every snapshot.
    staleness_s: float = 15.0
    snapshot_interval_s: float = 0.2   # 5Hz. Support/resistance zones persist seconds-to-minutes, so
                                       # 10Hz was wasted CPU and a 2x-bigger synchronous build_combined
                                       # block per tick (which was starving the ccxt WS keepalives).
    atr_window: int = 50
    host: str = "127.0.0.1"
    port: int = 8800
    # detector
    size_multiple: float = 3.0
    min_size: float = 0.0
    max_gap_bps: float = 20.0
    max_zone_width_bps: float = 50.0
    match_overlap_bps: float = 10.0
    grace_s: float = 2.0
    det_window_bps: float = 300.0   # proximity scale to match the aggregate window (probe: conf p90~0.93)
    persistence_target_s: float = 30.0
    venues_target: float = 3.0
    strength_target: float = 15.0
    # signal
    entry_threshold: float = 0.6
    trail_threshold: float = 0.6
    opposing_threshold: float = 0.7
    min_persistence_s: float = 10.0
    min_venues: int = 2
    entry_offset_bps: float = 2.0
    stop_offset_bps: float = 5.0
    atr_stop_mult: float = 3.0
    opposing_distance_bps: float = 8.0    # "at the opposing wall" — tight vs the ~60bps zone spacing so
                                          # entries find room and exits fire only when price truly reaches it
    risk_pct: float = 0.01
    max_leverage: float = 10.0
    entry_zone_bps: float = 30.0       # only trade a support/resistance when price is within this of it
    pending_timeout_s: float = 10.0    # cancel a pending entry that has not filled in this long
    entry_mode: str = "momentum"       # "momentum" (default, breakout) | "reversion" (bounce at support)
    tp_mult: float = 2.0               # reversion take-profit at tp_mult x risk distance
    # paper account (since 2026-10-23, so its results are comparable and survive restarts)
    fixed_notional: float | None = 10_000.0   # every trade has this notional (EUR) whatever the equity;
                                              # None = risk_pct of equity capped by max_leverage (shrank
                                              # trades to dust as each session's account drained)
    paper_state_path: str | None = None       # cash + open position kept across restarts; __main__ sets it
                                              # (None here so tests never read or write the live file)
    # USDT/USDC -> USD from Kraken into the aggregator (was fixed at 1.0; USDT ~ -12 bp)
    peg_live: bool = True
    peg_interval_s: float = 30.0
    paper_resume_max_s: float = 300.0   # outage up to this long: an open position simply goes on;
                                        # longer: it closes at the first price (reason "restart")
    paper_heartbeat_s: float = 30.0     # the paper state is re-saved this often (its last_alive gives
                                        # the length of an outage to the next start)
    shutdown_watch: bool = True         # on a PC shutdown / log-off: save the account, flush the recorder
    # raw-L2 data layer. Recording is ON, writing ONLY inside this new D: folder
    # (never touches other files on D:). Lossless compact encoding (M17): GB/day measured after deploy.
    book_data_dir: str | None = r"D:\pavilos_book_data"
    book_flush_interval_s: float = 5.0
    book_checkpoint_every_s: float = 60.0   # full live copy of every active book -> replay base + heals drops
    book_retention_days: int | None = None  # None = ETERNAL: nothing is ever deleted automatically
    book_min_free_gb: float | None = 20.0   # shared D: floor (GiB, as Explorer shows): below it recording PAUSES
                                            # (lowered 100 -> 20 by the operator on 2026-10-23)
    book_janitor_every_s: float = 600.0     # how often the floor (and the age limit, if any) is re-checked


class Runtime:
    def __init__(self, engine: Engine, trading_engine: TradingEngine,
                 state: DashboardState, config: RuntimeConfig,
                 all_trades, trade_log, recorder=None, peg_feed: PegFeed | None = None, *,
                 paper_state: PaperState | None = None, period: dict | None = None,
                 now: Callable[[], float] = _wall_now) -> None:
        self.engine = engine
        self.trading_engine = trading_engine
        self.state = state
        self.config = config
        self.all_trades = all_trades
        self.trade_log = trade_log
        self.recorder = recorder
        self.peg_feed = peg_feed
        self.paper_state = paper_state
        self.period = period if period is not None else {"since": 0.0, "equity0": config.starting_equity,
                                                         "history": []}
        self._now = now
        self._stop_event: asyncio.Event | None = None
        self.restart_requested = False

    # --- paper account: measurement period, saving -----------------------------
    def paper_info(self) -> dict:
        broker = self.trading_engine.broker
        return {"since": self.period["since"], "equity0": self.period["equity0"],
                "history": list(self.period.get("history", [])), "equity": broker.equity(),
                "position_open": broker.position() is not None,
                "starting_equity": self.config.starting_equity,
                "persisted": self.paper_state is not None}

    def start_new_period(self, note: str = "") -> dict:
        """A new measurement period: cash back to the starting equity and the dashboards count
        from now. Nothing is deleted (the trade log is untouched; the old period is kept in the
        history). Runs on the event loop, like the trading; RuntimeError if a position is open."""
        broker = self.trading_engine.broker
        if broker.position() is not None:
            raise RuntimeError("hay una posición abierta: reinténtalo cuando se cierre (suelen durar segundos)")
        t = self._now()
        old = {"since": self.period["since"], "until": t, "equity0": self.period["equity0"],
               "equity_end": broker.equity(), "note": self.period.get("note", "")}
        self.period.setdefault("history", []).append(old)
        self.period.update(since=t, equity0=self.config.starting_equity, note=note)
        broker.reset_cash(self.config.starting_equity)       # saves the state with the new period
        self.trading_engine.signal.cancel_pending()
        _log.warning("paper account: new measurement period from now (cash %.2f -> %.2f)%s",
                     old["equity_end"], self.config.starting_equity, f": {note}" if note else "")
        return self.paper_info()

    def save_state_now(self) -> None:
        if self.paper_state is None:
            return
        try:
            self.paper_state.save(self.trading_engine.broker.export_state())
        except Exception:
            _log.exception("failed to save the paper account to %s", self.paper_state.path)

    def request_restart(self) -> None:
        """Planned restart: stop cleanly (recorder flushed, account saved) and exit with a non-zero
        code so the supervisor starts a fresh process -- unlike a kill, nothing is lost."""
        self.restart_requested = True
        _log.warning("restart requested: stopping cleanly, the supervisor starts a fresh process")
        if self._stop_event is not None:
            self._stop_event.set()

    def _on_session_end(self, reason: str) -> None:
        # Runs on the shutdown watcher's thread while Windows waits (a few seconds): save the
        # account and flush whatever the recorder still holds. The process is killed after.
        _log.warning("Windows is ending the session (%s): saving the paper account and flushing "
                     "the recorder before the process is terminated", reason)
        self.save_state_now()
        if self.recorder is not None:
            try:
                self.recorder.stop(timeout=4.0)
            except Exception:
                _log.exception("recorder flush on session end failed")
        for h in logging.getLogger().handlers:
            try:
                h.flush()
            except Exception:
                pass

    async def _heartbeat(self, stop: "asyncio.Event") -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.paper_heartbeat_s)
            except asyncio.TimeoutError:
                pass
            self.save_state_now()

    @classmethod
    def build(cls, config: RuntimeConfig, *,
              connector_factory: Callable[[str, str], object] = build_connector,
              now: Callable[[], float] = _wall_now) -> "Runtime":
        connectors = [connector_factory(v, config.symbols[v]) for v in config.symbols]
        peg = PegProvider()
        agg = Aggregator(list(VENUE_SPECS), peg, bin_bps=config.bin_bps,
                         window_bps=config.window_bps, staleness_s=config.staleness_s)
        peg_feed = (PegFeed(peg, lake_dir=config.book_data_dir, interval_s=config.peg_interval_s)
                    if config.peg_live else None)
        recorder = None
        if config.book_data_dir:
            from pavilos.persistence.parquet_sink import ParquetSink
            from pavilos.persistence.recorder import BookRecorder
            recorder = BookRecorder(ParquetSink(config.book_data_dir),
                                    flush_interval_s=config.book_flush_interval_s,
                                    rebase_after_s=config.staleness_s)
        engine = Engine(connectors, agg, interval_s=config.snapshot_interval_s,
                        on_update=(recorder.record if recorder else None),
                        checkpoint_every_s=(config.book_checkpoint_every_s if recorder else None))
        detector = Detector(size_multiple=config.size_multiple, min_size=config.min_size,
                            max_gap_bps=config.max_gap_bps, max_zone_width_bps=config.max_zone_width_bps,
                            match_overlap_bps=config.match_overlap_bps, grace_s=config.grace_s,
                            window_bps=config.det_window_bps, persistence_target_s=config.persistence_target_s,
                            venues_target=config.venues_target, strength_target=config.strength_target)
        signal = SignalEngine(entry_threshold=config.entry_threshold, trail_threshold=config.trail_threshold,
                              opposing_threshold=config.opposing_threshold, min_persistence_s=config.min_persistence_s,
                              min_venues=config.min_venues, entry_offset_bps=config.entry_offset_bps,
                              stop_offset_bps=config.stop_offset_bps, atr_stop_mult=config.atr_stop_mult,
                              opposing_distance_bps=config.opposing_distance_bps, risk_pct=config.risk_pct,
                              max_leverage=config.max_leverage, entry_zone_bps=config.entry_zone_bps,
                              pending_timeout_s=config.pending_timeout_s,
                              entry_mode=config.entry_mode, tp_mult=config.tp_mult,
                              fixed_notional=config.fixed_notional)
        trade_log = TradeLog(config.trade_log_path)
        all_trades = trade_log.load()

        def _on_trade(t) -> None:
            # A disk/IO failure must NEVER crash trading or strand the broker.
            # Keep the in-memory all-time record first (persistence-independent),
            # then attempt the durable append; swallow+log any I/O failure so a
            # transient disk fault can't propagate out of broker.close()/on_price().
            try:
                all_trades.append(t)
            except Exception:
                _log.exception("failed to record trade in memory")
            try:
                trade_log.append(t)
            except Exception:
                _log.exception("failed to persist trade to %s", config.trade_log_path)

        paper_state = PaperState(config.paper_state_path, now=now) if config.paper_state_path else None
        last_stop_save = [0.0]

        def _on_state(snapshot: dict) -> None:
            # a trailing-stop move saves at most once a second; entries and exits always
            if snapshot.get("stop_only"):
                t = now()
                if t - last_stop_save[0] < 1.0:
                    return
                last_stop_save[0] = t
            try:
                paper_state.save(snapshot)
            except Exception:
                _log.exception("failed to save the paper account to %s", config.paper_state_path)

        broker = PaperBroker(starting_equity=config.starting_equity, on_trade=_on_trade,
                             on_state=(_on_state if paper_state is not None else None))
        # Measurement period: the dashboards count trades from period["since"] by default. Without
        # a saved account (tests, backtests) it is the whole history, as before.
        period = {"since": 0.0, "equity0": config.starting_equity, "history": []}
        if paper_state is not None:
            saved = paper_state.load()
            if saved is not None:
                period = saved["period"] or _first_period(all_trades, config.starting_equity, now())
                period.setdefault("history", [])
                pos = saved["position"]
                if pos is not None and any(t.entry_ts == pos.get("entry_ts") for t in all_trades[-20:]):
                    # it died after logging the close but before saving the state: already a trade
                    pos = None
                down = now() - saved["last_alive"] if saved["last_alive"] is not None else None
                resume = pos is not None and down is not None and 0 <= down <= config.paper_resume_max_s
                try:
                    broker.restore(equity=saved["equity"], position=pos, resume=resume)
                    if resume:
                        signal.resume_position(pos["side"], tp=(pos.get("context") or {}).get("tp"))
                    _log.info("paper account continued: equity %.2f%s", saved["equity"],
                              "" if pos is None else
                              f"; the {pos['side']} position open {down:.0f} s ago goes on" if resume else
                              f"; the {pos['side']} position open at the stop closes at the first price"
                              f" (outage {'unknown' if down is None else f'{down:.0f} s'})")
                except Exception:
                    _log.exception("paper account in %s not restored; starting from %.2f",
                                   config.paper_state_path, config.starting_equity)
            paper_state.period = period
            try:
                paper_state.save(broker.export_state())          # period and last_alive on disk now
            except Exception:
                _log.exception("failed to save the paper account to %s", config.paper_state_path)

        def period_trades():
            i = bisect.bisect_left(all_trades, period["since"], key=lambda t: t.entry_ts)
            return all_trades[i:]
        state = DashboardState()

        def observer(snapshot, analysis, brk) -> None:
            # Use a real wall clock (not analysis.ts) so (now - analysis.ts)
            # measures actual feed lag and the dashboard stale flag is meaningful;
            # passing now=analysis.ts would make the difference always 0.
            state.update(analysis, brk, engine.health(), engine_state=signal.state,
                         now=now(), staleness_s=config.staleness_s,
                         trades=all_trades[-50:],
                         summary={**summarize(period_trades(), base_equity=period["equity0"]),
                                  "since": period["since"]},
                         recording=(recorder.status() if recorder is not None else None))

        trading_engine = TradingEngine(detector, ATR(window=config.atr_window), signal, broker, observer=observer)
        return cls(engine, trading_engine, state, config, all_trades, trade_log, recorder, peg_feed,
                   paper_state=paper_state, period=period, now=now)

    async def _supervise_trading(self, stop: "asyncio.Event", *, restart_delay: float = 1.0) -> None:
        """Run TradingEngine.run; on a crash-loud exception, log and restart until stop."""
        while not stop.is_set():
            try:
                await self.trading_engine.run(self.engine.snapshots, stop)
            except asyncio.CancelledError:
                raise
            except Exception:
                _log.exception("trading loop crashed; restarting")
                if stop.is_set():
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=restart_delay)
                except asyncio.TimeoutError:
                    pass
            else:
                break  # clean return (stop observed)

    def _start_recording(self):
        """Retention pass first (age limit if configured, disk floor -> may start the
        recorder PAUSED), then the run's start in ``_runs.jsonl`` (the replay resets
        every book there; no row of this run is stamped below it), then the writer
        thread, then the janitor that repeats the pass every ``book_janitor_every_s``.
        Returns the janitor (for ``_stop_recording``)."""
        from pavilos.persistence.retention import RetentionJanitor
        recorder = self.recorder
        janitor = RetentionJanitor(self.config.book_data_dir,
                                   retention_days=self.config.book_retention_days,
                                   min_free_gb=self.config.book_min_free_gb,
                                   every_s=self.config.book_janitor_every_s,
                                   on_floor=lambda ok: recorder.set_paused(not ok))
        janitor.run_once()
        try:
            from pavilos.persistence.runs import mark_run_start
            recorder.set_ts_floor(mark_run_start(self.config.book_data_dir))
        except Exception:
            _log.exception("recorder start NOT marked in %s: a replay right after this restart "
                           "may keep the previous run's books for up to staleness_s",
                           self.config.book_data_dir)
        recorder.start()
        janitor.start()
        return janitor

    def _stop_recording(self, janitor) -> None:
        # The recorder stops AFTER the engine (caller's order) so the final drained
        # updates are captured; stop() drains the queue then joins (bounded).
        if janitor is not None:
            janitor.stop()
        self.recorder.stop()

    def web_app(self):
        """Dashboard app: live state at / and the all-time trade history at /trades."""
        import os
        from pavilos.web.server import create_app
        cfg = self.config
        return create_app(self.state, trades=lambda: self.all_trades,
                          starting_equity=cfg.starting_equity,
                          ref_notional=cfg.fixed_notional or 10_000.0,
                          runs_file=os.path.join(cfg.book_data_dir, "_runs.jsonl") if cfg.book_data_dir else None,
                          paper=self.paper_info, new_period=self.start_new_period,
                          restart=self.request_restart)

    async def run_app(self, stop: "asyncio.Event | None" = None) -> None:
        import uvicorn
        from pavilos.core.shutdown_watch import ShutdownWatcher
        stop = stop or asyncio.Event()
        self._stop_event = stop
        asyncio.get_running_loop().set_exception_handler(_loop_exception_handler)
        janitor = self._start_recording() if self.recorder is not None else None
        watcher = ShutdownWatcher(self._on_session_end) if self.config.shutdown_watch else None
        if watcher is not None and not watcher.start():
            watcher = None                     # not Windows, or the window failed (already logged)
        if watcher is not None:
            _log.info("shutdown watcher listening: a PC shutdown or log-off saves the paper account "
                      "and flushes the recorder first")
        heartbeat = asyncio.create_task(self._heartbeat(stop)) if self.paper_state is not None else None
        # Live USDT/USDC rates: its first reading lands within a second, long before any
        # zone is persistent enough to trade; it never blocks the recorder's start.
        pegging = asyncio.create_task(self.peg_feed.run(stop)) if self.peg_feed is not None else None
        await self.engine.start()
        # log_config=None: uvicorn logs through OUR handlers (logs/pavilos.log) instead of
        # installing its own stderr handlers, which would bypass the rotating file.
        server = uvicorn.Server(uvicorn.Config(self.web_app(), host=self.config.host,
                                               port=self.config.port, log_level="warning",
                                               log_config=None))
        trading = asyncio.create_task(self._supervise_trading(stop))
        serving = asyncio.create_task(server.serve())
        try:
            await asyncio.wait({trading, serving}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop.set()
            server.should_exit = True
            await self.engine.stop()
            if self.recorder is not None:
                self._stop_recording(janitor)
            tasks = [t for t in (trading, serving, pegging, heartbeat) if t is not None]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.save_state_now()                  # the account as it stands at a clean stop
            if watcher is not None:
                watcher.stop()
