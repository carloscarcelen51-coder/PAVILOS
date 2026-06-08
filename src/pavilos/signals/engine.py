# src/pavilos/signals/engine.py
"""SignalEngine: drives a PaperBroker from DepthAnalysis. IDLE -> PENDING_ENTRY
-> IN_POSITION -> IDLE.

Entry is a *momentum* breakout in the trade direction, anchored by the detected
zone as the protective stop, and gated so it is a real bounce, not noise:
- a strong support BELOW but NEAR the price (within ``entry_zone_bps``) arms a
  LONG buy-stop just above price (fills as price ticks up — "se ejecuta subiendo");
- a resistance ABOVE but NEAR the price arms a SHORT sell-stop just below price.
The initial stop sits beyond the zone but never tighter than ``atr_stop_mult`` x
ATR from price (anti-whipsaw). A pending entry is withdrawn if the thesis zone
vanishes OR it has not filled within ``pending_timeout_s`` (avoids stale orders).
Long off supports, short off resistances (mirrored). One position at a time."""
from __future__ import annotations

from pavilos.detection.models import DepthAnalysis, Zone
from pavilos.signals.sizing import position_size


class SignalEngine:
    def __init__(self, *, entry_threshold: float, trail_threshold: float, opposing_threshold: float,
                 min_persistence_s: float, min_venues: int, entry_offset_bps: float,
                 stop_offset_bps: float, atr_stop_mult: float, opposing_distance_bps: float,
                 risk_pct: float, max_leverage: float,
                 entry_zone_bps: float, pending_timeout_s: float,
                 entry_mode: str = "momentum", tp_mult: float = 2.0) -> None:
        for name, v in (("entry_threshold", entry_threshold), ("trail_threshold", trail_threshold),
                        ("opposing_threshold", opposing_threshold), ("atr_stop_mult", atr_stop_mult),
                        ("opposing_distance_bps", opposing_distance_bps), ("risk_pct", risk_pct),
                        ("max_leverage", max_leverage), ("entry_offset_bps", entry_offset_bps),
                        ("stop_offset_bps", stop_offset_bps), ("entry_zone_bps", entry_zone_bps),
                        ("pending_timeout_s", pending_timeout_s), ("tp_mult", tp_mult)):
            if not (v > 0):
                raise ValueError(f"SignalEngine: {name} must be positive, got {v}")
        if entry_mode not in ("momentum", "reversion"):
            raise ValueError(f"SignalEngine: entry_mode must be 'momentum' or 'reversion', got {entry_mode!r}")
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
        self.entry_zone_bps = entry_zone_bps
        self.pending_timeout_s = pending_timeout_s
        self.entry_mode = entry_mode
        self.tp_mult = tp_mult
        self.state = "IDLE"
        self._thesis: Zone | None = None
        self._dir: str | None = None
        self._armed_ts = 0.0
        self._tp = 0.0

    def update(self, analysis: DepthAnalysis, atr: float, broker) -> None:
        price = analysis.mid
        broker.on_price(price, ts=analysis.ts)   # process fills/funding first
        pos = broker.position()
        # sync state from broker outcomes
        if self.state == "PENDING_ENTRY" and pos is not None:
            # just filled this tick -> let the position breathe; manage/exit only from
            # the NEXT snapshot (avoids a same-tick fill-then-opposing-exit that closes
            # at the entry price and bleeds the round-trip fees for zero move)
            self.state = "IN_POSITION"
            return
        elif self.state == "IN_POSITION" and pos is None:
            # position closed by the broker (stop-out) -> back to IDLE, and defer
            # any new setup to the NEXT snapshot (no same-tick whipsaw re-entry;
            # consistent with the discretionary-exit path, which also re-arms next tick)
            self.state, self._thesis, self._dir = "IDLE", None, None
            return

        if self.state == "IDLE":
            if self.entry_mode == "reversion":
                self._maybe_enter_reversion(analysis, price, atr, broker)
            else:
                self._maybe_enter(analysis, price, atr, broker)
        elif self.state == "PENDING_ENTRY":          # momentum-only
            self._maybe_cancel(analysis, broker)
        elif self.state == "IN_POSITION":
            if self.entry_mode == "reversion":
                self._manage_reversion(analysis, price, broker)
            else:
                self._manage(analysis, price, atr, pos, broker)

    def _operable(self, z: Zone) -> bool:
        return (z.confidence >= self.entry_threshold and z.persistence_s >= self.min_persistence_s
                and len(z.venues) >= self.min_venues)

    def _maybe_enter(self, analysis: DepthAnalysis, price: float, atr: float, broker) -> None:
        zone_tol = price * self.entry_zone_bps / 1e4
        best: Zone | None = None
        best_dir: str | None = None
        for z in analysis.supports:                      # LONG: price just ABOVE a NEARBY support
            if (self._operable(z) and z.high < price and (price - z.high) <= zone_tol
                    and (best is None or z.confidence > best.confidence)):
                best, best_dir = z, "LONG"
        for z in analysis.resistances:                   # SHORT: price just BELOW a NEARBY resistance
            if (self._operable(z) and z.low > price and (z.low - price) <= zone_tol
                    and (best is None or z.confidence > best.confidence)):
                best, best_dir = z, "SHORT"
        if best is None:
            return
        if self._opposing_near(analysis, price, best_dir):
            return  # boxed in: a near opposing wall would exit us instantly -> no room to run
        if best_dir == "LONG":
            # buy-stop just ABOVE price (fills on an up-tick); stop below the support,
            # but never tighter than the ATR floor (don't get noise-stopped)
            trigger = price * (1 + self.entry_offset_bps / 1e4)
            stop = min(best.low * (1 - self.stop_offset_bps / 1e4), price - atr * self.atr_stop_mult)
        else:
            # sell-stop just BELOW price (fills on a down-tick); stop above the resistance,
            # never tighter than the ATR floor
            trigger = price * (1 - self.entry_offset_bps / 1e4)
            stop = max(best.high * (1 + self.stop_offset_bps / 1e4), price + atr * self.atr_stop_mult)
        # skip a pathological stop (e.g. garbage ATR pushing it <=0 / to the wrong
        # side) rather than arm an order with an unreachable protective stop
        ok = (0.0 < stop < trigger) if best_dir == "LONG" else (stop > trigger > 0.0)
        if not ok:
            return
        size = position_size(broker.equity(), entry=trigger, stop=stop,
                             risk_pct=self.risk_pct, max_leverage=self.max_leverage)
        if size <= 0:
            return
        broker.place_entry(best_dir, trigger=trigger, stop=stop, size=size)
        self.state, self._thesis, self._dir, self._armed_ts = "PENDING_ENTRY", best, best_dir, analysis.ts

    def _opposing_near(self, analysis: DepthAnalysis, price: float, direction: str) -> bool:
        """True if a confident opposing wall sits within ``opposing_distance_bps`` —
        the same condition that triggers the discretionary exit, so entering into it
        would close instantly. Mirrors _manage's exit check."""
        tol = price * self.opposing_distance_bps / 1e4
        if direction == "LONG":
            return any(z.confidence >= self.opposing_threshold and z.low > price
                       and (z.low - price) <= tol for z in analysis.resistances)
        return any(z.confidence >= self.opposing_threshold and z.high < price
                   and (price - z.high) <= tol for z in analysis.supports)

    def _maybe_enter_reversion(self, analysis: DepthAnalysis, price: float, atr: float, broker) -> None:
        """Mean-reversion bounce: enter MARKET at a near strong support (LONG) or
        resistance (SHORT) within ``entry_zone_bps``, stop beyond the zone (ATR-floored),
        take-profit at ``tp_mult`` x the risk distance. No pending state — IDLE -> IN_POSITION."""
        zone_tol = price * self.entry_zone_bps / 1e4
        best: Zone | None = None
        best_dir: str | None = None
        for z in analysis.supports:                  # LONG: bounce up off a near support below
            if (self._operable(z) and z.high < price and (price - z.high) <= zone_tol
                    and (best is None or z.confidence > best.confidence)):
                best, best_dir = z, "LONG"
        for z in analysis.resistances:               # SHORT: fade down off a near resistance above
            if (self._operable(z) and z.low > price and (z.low - price) <= zone_tol
                    and (best is None or z.confidence > best.confidence)):
                best, best_dir = z, "SHORT"
        if best is None:
            return
        if best_dir == "LONG":
            stop = min(best.low * (1 - self.stop_offset_bps / 1e4), price - atr * self.atr_stop_mult)
            ok = 0.0 < stop < price
        else:
            stop = max(best.high * (1 + self.stop_offset_bps / 1e4), price + atr * self.atr_stop_mult)
            ok = stop > price > 0.0
        if not ok:
            return
        size = position_size(broker.equity(), entry=price, stop=stop,
                             risk_pct=self.risk_pct, max_leverage=self.max_leverage)
        if size <= 0:
            return
        risk = abs(price - stop)
        self._tp = price + self.tp_mult * risk if best_dir == "LONG" else price - self.tp_mult * risk
        broker.enter_market(best_dir, stop=stop, size=size, ts=analysis.ts)
        self.state, self._thesis, self._dir = "IN_POSITION", best, best_dir

    def _manage_reversion(self, analysis: DepthAnalysis, price: float, broker) -> None:
        pos = broker.position()
        if pos is None:
            return
        hit_tp = price >= self._tp if pos.side == "LONG" else price <= self._tp
        if hit_tp:
            broker.close(ts=analysis.ts)
            self.state, self._thesis, self._dir = "IDLE", None, None

    def _thesis_present(self, analysis: DepthAnalysis) -> bool:
        zones = analysis.supports if self._dir == "LONG" else analysis.resistances
        t = self._thesis
        return any(z.low <= t.high and z.high >= t.low and z.confidence >= self.entry_threshold
                   for z in zones)

    def _maybe_cancel(self, analysis: DepthAnalysis, broker) -> None:
        # withdraw the pending entry if the thesis zone vanished OR it has gone stale
        # (price never reached the trigger within pending_timeout_s)
        timed_out = (analysis.ts - self._armed_ts) > self.pending_timeout_s
        if timed_out or not self._thesis_present(analysis):
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
