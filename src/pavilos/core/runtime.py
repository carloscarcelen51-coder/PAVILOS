# src/pavilos/core/runtime.py
"""Assemble the live PAVILOS object graph and run it: Engine (6 venues) ->
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
from pavilos.core.trading_engine import TradingEngine
from pavilos.web.state import DashboardState

_log = logging.getLogger(__name__)

_SYMBOLS = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
            "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd"}


@dataclass(frozen=True)
class RuntimeConfig:
    symbols: dict = field(default_factory=lambda: dict(_SYMBOLS))
    starting_equity: float = 10_000.0
    bin_bps: float = 5.0
    window_bps: float = 50.0
    staleness_s: float = 15.0
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
    det_window_bps: float = 200.0
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
    opposing_distance_bps: float = 30.0
    risk_pct: float = 0.01
    max_leverage: float = 10.0


class Runtime:
    def __init__(self, engine: Engine, trading_engine: TradingEngine,
                 state: DashboardState, config: RuntimeConfig) -> None:
        self.engine = engine
        self.trading_engine = trading_engine
        self.state = state
        self.config = config

    @classmethod
    def build(cls, config: RuntimeConfig, *,
              connector_factory: Callable[[str, str], object] = build_connector) -> "Runtime":
        connectors = [connector_factory(v, config.symbols[v]) for v in config.symbols]
        agg = Aggregator(list(VENUE_SPECS), PegProvider(), bin_bps=config.bin_bps,
                         window_bps=config.window_bps, staleness_s=config.staleness_s)
        engine = Engine(connectors, agg)
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
                              max_leverage=config.max_leverage)
        broker = PaperBroker(starting_equity=config.starting_equity)
        state = DashboardState()

        def observer(snapshot, analysis, brk) -> None:
            state.update(analysis, brk, engine.health(), engine_state=signal.state,
                         now=analysis.ts, staleness_s=config.staleness_s)

        trading_engine = TradingEngine(detector, ATR(window=config.atr_window), signal, broker, observer=observer)
        return cls(engine, trading_engine, state, config)

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
            for t in (trading, serving):
                t.cancel()
            await asyncio.gather(trading, serving, return_exceptions=True)
