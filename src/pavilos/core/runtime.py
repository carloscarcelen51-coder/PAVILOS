# src/pavilos/core/runtime.py
"""Assemble the live PAVILOS object graph and run it: Engine (12 venues) ->
Detector -> SignalEngine -> PaperBroker, publishing each tick to a DashboardState,
served by a FastAPI/uvicorn dashboard. Bounded shutdown; supervised trading loop."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.core.engine import Engine
from pavilos.connectors.venues import VENUE_SPECS, build_connector
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.execution.broker import PaperBroker
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
    # raw-L2 data layer. Recording is ON, writing ONLY inside this new D: folder
    # (never touches other files on D:). 7-day retention keeps the disk bounded.
    book_data_dir: str | None = r"D:\pavilos_book_data"
    book_flush_interval_s: float = 5.0
    book_retention_days: int = 7


class Runtime:
    def __init__(self, engine: Engine, trading_engine: TradingEngine,
                 state: DashboardState, config: RuntimeConfig,
                 all_trades, trade_log, recorder=None) -> None:
        self.engine = engine
        self.trading_engine = trading_engine
        self.state = state
        self.config = config
        self.all_trades = all_trades
        self.trade_log = trade_log
        self.recorder = recorder

    @classmethod
    def build(cls, config: RuntimeConfig, *,
              connector_factory: Callable[[str, str], object] = build_connector,
              now: Callable[[], float] = _wall_now) -> "Runtime":
        connectors = [connector_factory(v, config.symbols[v]) for v in config.symbols]
        agg = Aggregator(list(VENUE_SPECS), PegProvider(), bin_bps=config.bin_bps,
                         window_bps=config.window_bps, staleness_s=config.staleness_s)
        recorder = None
        if config.book_data_dir:
            from pavilos.persistence.parquet_sink import ParquetSink
            from pavilos.persistence.recorder import BookRecorder
            recorder = BookRecorder(ParquetSink(config.book_data_dir),
                                    flush_interval_s=config.book_flush_interval_s)
        engine = Engine(connectors, agg, interval_s=config.snapshot_interval_s,
                        on_update=(recorder.record if recorder else None))
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
                              pending_timeout_s=config.pending_timeout_s)
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

        broker = PaperBroker(starting_equity=config.starting_equity, on_trade=_on_trade)
        state = DashboardState()

        def observer(snapshot, analysis, brk) -> None:
            # Use a real wall clock (not analysis.ts) so (now - analysis.ts)
            # measures actual feed lag and the dashboard stale flag is meaningful;
            # passing now=analysis.ts would make the difference always 0.
            state.update(analysis, brk, engine.health(), engine_state=signal.state,
                         now=now(), staleness_s=config.staleness_s,
                         trades=all_trades[-50:],
                         summary=summarize(all_trades, base_equity=config.starting_equity))

        trading_engine = TradingEngine(detector, ATR(window=config.atr_window), signal, broker, observer=observer)
        return cls(engine, trading_engine, state, config, all_trades, trade_log, recorder)

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

    async def run_app(self, stop: "asyncio.Event | None" = None) -> None:
        import uvicorn
        from pavilos.web.server import create_app
        stop = stop or asyncio.Event()
        if self.recorder is not None:
            from pavilos.persistence.retention import prune_old_partitions
            prune_old_partitions(self.config.book_data_dir, self.config.book_retention_days)
            self.recorder.start()
        await self.engine.start()
        server = uvicorn.Server(uvicorn.Config(create_app(self.state), host=self.config.host,
                                               port=self.config.port, log_level="warning"))
        trading = asyncio.create_task(self._supervise_trading(stop))
        serving = asyncio.create_task(server.serve())
        try:
            await asyncio.wait({trading, serving}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop.set()
            server.should_exit = True
            await self.engine.stop()
            # Stop the recorder AFTER the engine so the final drained updates are
            # captured; stop() drains the queue then joins (bounded).
            if self.recorder is not None:
                self.recorder.stop()
            for t in (trading, serving):
                t.cancel()
            await asyncio.gather(trading, serving, return_exceptions=True)
