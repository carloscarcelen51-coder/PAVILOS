# src/pavilos/backtest/runner.py
"""Replay a recorded combined-snapshot stream through the real detection->signals
->paper-broker pipeline for one config, and report performance metrics. Pure."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.execution.broker import PaperBroker


@dataclass(slots=True, frozen=True)
class BacktestResult:
    n_snapshots: int
    n_trades: int
    wins: int
    losses: int
    win_rate: float
    realized_pnl: float
    fees: float
    return_pct: float
    final_equity: float
    max_drawdown: float
    max_drawdown_pct: float


def _detector(c: RuntimeConfig) -> Detector:
    return Detector(size_multiple=c.size_multiple, min_size=c.min_size, max_gap_bps=c.max_gap_bps,
                    max_zone_width_bps=c.max_zone_width_bps, match_overlap_bps=c.match_overlap_bps,
                    grace_s=c.grace_s, window_bps=c.det_window_bps, persistence_target_s=c.persistence_target_s,
                    venues_target=c.venues_target, strength_target=c.strength_target)


def _signal(c: RuntimeConfig) -> SignalEngine:
    return SignalEngine(entry_threshold=c.entry_threshold, trail_threshold=c.trail_threshold,
                        opposing_threshold=c.opposing_threshold, min_persistence_s=c.min_persistence_s,
                        min_venues=c.min_venues, entry_offset_bps=c.entry_offset_bps,
                        stop_offset_bps=c.stop_offset_bps, atr_stop_mult=c.atr_stop_mult,
                        opposing_distance_bps=c.opposing_distance_bps, risk_pct=c.risk_pct,
                        max_leverage=c.max_leverage, entry_zone_bps=c.entry_zone_bps,
                        pending_timeout_s=c.pending_timeout_s)


def run_backtest(snapshots, *, config: RuntimeConfig, starting_equity: float) -> BacktestResult:
    detector = _detector(config)
    atr = ATR(window=config.atr_window)
    signal = _signal(config)
    broker = PaperBroker(starting_equity=starting_equity)
    last_mid = None
    peak = starting_equity
    max_dd = 0.0
    n = 0
    for snap in snapshots:
        n += 1
        last_mid = snap.mid
        analysis = detector.update(snap)
        atr.update(snap.mid)
        signal.update(analysis, atr.value(), broker)
        eq = broker.equity(snap.mid)
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
    if broker.position() is not None and last_mid is not None:
        broker.close(ts=snapshots[-1].ts)
    trades = broker.trades()
    realized = sum(t.pnl for t in trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = sum(1 for t in trades if t.pnl < 0)
    return BacktestResult(
        n_snapshots=n, n_trades=len(trades), wins=wins, losses=losses,
        win_rate=(wins / len(trades) * 100.0) if trades else 0.0,
        realized_pnl=realized, fees=sum(t.fee for t in trades),
        return_pct=(realized / starting_equity * 100.0) if starting_equity else 0.0,
        final_equity=broker.equity(last_mid) if last_mid is not None else starting_equity,
        max_drawdown=max_dd,
        max_drawdown_pct=(max_dd / peak * 100.0) if peak else 0.0,
    )
