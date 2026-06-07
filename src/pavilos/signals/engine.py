# src/pavilos/signals/engine.py
"""SignalEngine: drives a PaperBroker from DepthAnalysis. IDLE -> PENDING_ENTRY
-> IN_POSITION -> IDLE.

Entry is a *momentum* breakout in the trade direction, anchored by the detected
zone as the protective stop: a strong support below price arms a LONG buy-stop
just ABOVE the current price (fills as price ticks up — "se ejecuta subiendo")
with the stop just below the support; a resistance above price arms a SHORT
sell-stop just below price with the stop just above the resistance. If the
thesis zone vanishes before the breakout fills, the pending entry is withdrawn.
Long off supports, short off resistances (mirrored). One position at a time."""
from __future__ import annotations

from pavilos.detection.models import DepthAnalysis, Zone
from pavilos.signals.sizing import position_size


class SignalEngine:
    def __init__(self, *, entry_threshold: float, trail_threshold: float, opposing_threshold: float,
                 min_persistence_s: float, min_venues: int, entry_offset_bps: float,
                 stop_offset_bps: float, atr_stop_mult: float, opposing_distance_bps: float,
                 risk_pct: float, max_leverage: float) -> None:
        for name, v in (("entry_threshold", entry_threshold), ("trail_threshold", trail_threshold),
                        ("opposing_threshold", opposing_threshold), ("atr_stop_mult", atr_stop_mult),
                        ("opposing_distance_bps", opposing_distance_bps), ("risk_pct", risk_pct),
                        ("max_leverage", max_leverage)):
            if not (v > 0):
                raise ValueError(f"SignalEngine: {name} must be positive, got {v}")
        self.entry_threshold = entry_threshold
        self.trail_threshold = trail_threshold
        self.opposing_threshold = opposing_threshold
        self.min_persistence_s = min_persistence_s
        self.min_venues = min_venues
        self.entry_offset_bps = entry_offset_bps
        self.stop_offset_bps = stop_offset_bps
        self.atr_stop_mult = atr_stop_mult
        self.opposing_distance_bps = opposing_distance_bps
        self.risk_pct = risk_pct
        self.max_leverage = max_leverage
        self.state = "IDLE"
        self._thesis: Zone | None = None
        self._dir: str | None = None

    def update(self, analysis: DepthAnalysis, atr: float, broker) -> None:
        price = analysis.mid
        broker.on_price(price, ts=analysis.ts)   # process fills/funding first
        pos = broker.position()
        # sync state from broker outcomes
        if self.state == "PENDING_ENTRY" and pos is not None:
            self.state = "IN_POSITION"
        elif self.state == "IN_POSITION" and pos is None:
            self.state, self._thesis, self._dir = "IDLE", None, None

        if self.state == "IDLE":
            self._maybe_enter(analysis, price, broker)
        elif self.state == "PENDING_ENTRY":
            self._maybe_cancel(analysis, broker)
        elif self.state == "IN_POSITION":
            self._manage(analysis, price, atr, pos, broker)

    def _operable(self, z: Zone) -> bool:
        return (z.confidence >= self.entry_threshold and z.persistence_s >= self.min_persistence_s
                and len(z.venues) >= self.min_venues)

    def _maybe_enter(self, analysis: DepthAnalysis, price: float, broker) -> None:
        best: Zone | None = None
        best_dir: str | None = None
        for z in analysis.supports:                      # LONG: support below price
            if self._operable(z) and z.high < price and (best is None or z.confidence > best.confidence):
                best, best_dir = z, "LONG"
        for z in analysis.resistances:                   # SHORT: resistance above price
            if self._operable(z) and z.low > price and (best is None or z.confidence > best.confidence):
                best, best_dir = z, "SHORT"
        if best is None:
            return
        if best_dir == "LONG":
            # buy-stop just ABOVE price (fills on an up-tick), stop below the support
            trigger = price * (1 + self.entry_offset_bps / 1e4)
            stop = best.low * (1 - self.stop_offset_bps / 1e4)
        else:
            # sell-stop just BELOW price (fills on a down-tick), stop above the resistance
            trigger = price * (1 - self.entry_offset_bps / 1e4)
            stop = best.high * (1 + self.stop_offset_bps / 1e4)
        size = position_size(broker.equity(), entry=trigger, stop=stop,
                             risk_pct=self.risk_pct, max_leverage=self.max_leverage)
        if size <= 0:
            return
        broker.place_entry(best_dir, trigger=trigger, stop=stop, size=size)
        self.state, self._thesis, self._dir = "PENDING_ENTRY", best, best_dir

    def _thesis_present(self, analysis: DepthAnalysis) -> bool:
        zones = analysis.supports if self._dir == "LONG" else analysis.resistances
        t = self._thesis
        return any(z.low <= t.high and z.high >= t.low and z.confidence >= self.entry_threshold
                   for z in zones)

    def _maybe_cancel(self, analysis: DepthAnalysis, broker) -> None:
        if not self._thesis_present(analysis):
            broker.cancel_entry()
            self.state, self._thesis, self._dir = "IDLE", None, None

    def _manage(self, analysis: DepthAnalysis, price: float, atr: float, pos, broker) -> None:
        if pos.side == "LONG":
            stops = [z.low * (1 - self.stop_offset_bps / 1e4) for z in analysis.supports
                     if z.confidence >= self.trail_threshold and z.high < price]
            atr_floor = price - atr * self.atr_stop_mult
            if stops:
                desired = min(max(stops), atr_floor)     # not tighter than the ATR floor
                if desired > pos.stop:
                    broker.modify_stop(desired)
            near = [z for z in analysis.resistances if z.confidence >= self.opposing_threshold
                    and z.low > price and (z.low - price) <= price * self.opposing_distance_bps / 1e4]
            if near:
                broker.close(ts=analysis.ts)
                self.state, self._thesis, self._dir = "IDLE", None, None
        else:  # SHORT (mirrored)
            stops = [z.high * (1 + self.stop_offset_bps / 1e4) for z in analysis.resistances
                     if z.confidence >= self.trail_threshold and z.low > price]
            atr_floor = price + atr * self.atr_stop_mult
            if stops:
                desired = max(min(stops), atr_floor)
                if desired < pos.stop:
                    broker.modify_stop(desired)
            near = [z for z in analysis.supports if z.confidence >= self.opposing_threshold
                    and z.high < price and (price - z.high) <= price * self.opposing_distance_bps / 1e4]
            if near:
                broker.close(ts=analysis.ts)
                self.state, self._thesis, self._dir = "IDLE", None, None
