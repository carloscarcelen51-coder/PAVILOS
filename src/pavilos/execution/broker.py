# src/pavilos/execution/broker.py
"""Paper broker for a linear USD perp (Kraken PF_XBTUSD model). Driven by
on_price(price, ts); no network. Entry and stop orders fill at the breaching
market price (the tick that triggers them), so gap-through is modelled and stops
never fill optimistically."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

_log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class Position:
    side: str          # "LONG" | "SHORT"
    size: float        # base units (BTC), > 0
    entry: float       # fill price
    stop: float        # current stop-loss price


@dataclass(slots=True, frozen=True)
class Fill:
    ts: float
    side: str          # "LONG" | "SHORT"
    price: float
    size: float
    fee: float
    kind: str          # "entry" | "stop" | "close"


DEFAULT_SYMBOL = "BTC/USD"


@dataclass(slots=True, frozen=True)
class Trade:
    side: str            # "LONG" | "SHORT"
    size: float
    entry: float
    exit: float
    entry_ts: float
    exit_ts: float
    pnl: float           # NET realized = gross price P&L - entry_fee - exit_fee (excludes funding)
    fee: float           # entry_fee + exit_fee
    return_pct: float    # pnl / (entry*size) * 100
    reason: str          # "stop" | "close" | "restart" (open when the process died; closed at the next price)
    symbol: str = DEFAULT_SYMBOL   # traded pair; log lines written before it existed are BTC/USD
    context: dict | None = None    # what the strategy saw when it armed the entry (None before 2026-10-23)


class PaperBroker:
    """Single-position paper broker. ``place_entry`` arms a stop entry (LONG fills
    when price >= trigger, SHORT when price <= trigger). A LONG stop-loss fills
    when price <= stop; SHORT when price >= stop. Fills are charged ``taker_fee``;
    funding is charged hourly on the notional (LONG pays, SHORT receives)."""

    def __init__(self, *, starting_equity: float, taker_fee: float = 0.0005,
                 maker_fee: float = 0.0002, funding_rate_hourly: float = 0.0,
                 on_trade=None, symbol: str = DEFAULT_SYMBOL, on_state=None) -> None:
        if starting_equity <= 0:
            raise ValueError("starting_equity must be positive")
        self._symbol = symbol                     # stamped on every Trade
        self._on_state = on_state                 # called with export_state() after every entry and exit
        self._entry_context: dict | None = None
        self._restart_close = False               # a restored open position closes at the next price
        self._equity = starting_equity            # realized cash
        self._taker = taker_fee
        self._maker = maker_fee
        self._funding = funding_rate_hourly
        self._position: Position | None = None
        self._pending: dict | None = None         # {side, trigger, stop, size}
        self._last_price = 0.0
        self._funding_anchor_ts: float | None = None
        self._fills: list[Fill] = []
        self._on_trade = on_trade
        self._trades: list[Trade] = []
        self._entry_fee = 0.0
        self._entry_ts = 0.0

    # --- order management -------------------------------------------------
    def place_entry(self, side: str, *, trigger: float, stop: float, size: float,
                    context: dict | None = None) -> None:
        if side not in ("LONG", "SHORT"):
            raise ValueError(f"bad side {side!r}")
        if self._position is not None or self._pending is not None:
            raise RuntimeError("broker already has a position or pending entry")
        if size <= 0:
            raise ValueError("size must be positive")
        if not (math.isfinite(trigger) and math.isfinite(stop)):
            raise ValueError("trigger and stop must be finite")
        if side == "LONG" and not (stop < trigger):
            raise ValueError("LONG stop must be below trigger")
        if side == "SHORT" and not (stop > trigger):
            raise ValueError("SHORT stop must be above trigger")
        self._pending = {"side": side, "trigger": trigger, "stop": stop, "size": size, "context": context}

    def enter_market(self, side: str, *, stop: float, size: float, ts: float,
                     context: dict | None = None) -> None:
        """Open a position IMMEDIATELY at the current price (for mean-reversion entries
        that fill at the support, not on a stop above it). Fills at ``_last_price``."""
        if side not in ("LONG", "SHORT"):
            raise ValueError(f"bad side {side!r}")
        if self._position is not None or self._pending is not None:
            raise RuntimeError("broker already has a position or pending entry")
        if size <= 0:
            raise ValueError("size must be positive")
        price = self._last_price
        if not (math.isfinite(stop) and math.isfinite(price) and price > 0):
            raise ValueError("stop and current price must be finite/positive")
        if side == "LONG" and not (stop < price):
            raise ValueError("LONG stop must be below the entry price")
        if side == "SHORT" and not (stop > price):
            raise ValueError("SHORT stop must be above the entry price")
        self._open(side, price, stop, size, ts, context)

    def cancel_entry(self) -> None:
        self._pending = None

    def modify_stop(self, new_stop: float) -> None:
        if self._position is None:
            raise RuntimeError("no position to modify")
        p = self._position
        self._position = Position(p.side, p.size, p.entry, new_stop)

    def close(self, *, ts: float) -> None:
        if self._position is not None:
            self._close_at(self._last_price, ts, "close")

    # --- price-driven simulation -----------------------------------------
    def on_price(self, price: float, ts: float) -> None:
        if not math.isfinite(price):
            return
        if self._restart_close:
            # A position restored from before a restart: what the price did while the
            # process was down is unknown, so it is closed at the first price seen now.
            self._restart_close = False
            if self._position is not None:
                self._close_at(price, ts, "restart")
                self._last_price = price
                return
        self._apply_funding(price, ts)
        if self._pending is not None:
            d = self._pending
            triggered = price >= d["trigger"] if d["side"] == "LONG" else price <= d["trigger"]
            if triggered:
                self._pending = None
                self._open(d["side"], price, d["stop"], d["size"], ts, d.get("context"))
        if self._position is not None:
            p = self._position
            hit = price <= p.stop if p.side == "LONG" else price >= p.stop
            if hit:
                self._close_at(price, ts, "stop")
        self._last_price = price

    # --- queries ----------------------------------------------------------
    def position(self) -> Position | None:
        return self._position

    def pending_entry(self) -> dict | None:
        return dict(self._pending) if self._pending is not None else None

    def equity(self, price: float | None = None) -> float:
        p = self._last_price if price is None else price
        if self._position is None:
            return self._equity
        pos = self._position
        unreal = pos.size * (p - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - p)
        return self._equity + unreal

    def fills(self) -> list[Fill]:
        return list(self._fills)

    def trades(self) -> list[Trade]:
        return list(self._trades)

    # --- internals --------------------------------------------------------
    def _open(self, side: str, price: float, stop: float, size: float, ts: float,
              context: dict | None = None) -> None:
        fee = size * price * self._taker
        self._equity -= fee
        self._position = Position(side, size, price, stop)
        self._funding_anchor_ts = ts
        self._last_price = price
        self._fills.append(Fill(ts, side, price, size, fee, "entry"))
        self._entry_fee = fee
        self._entry_ts = ts
        self._entry_context = context
        self._notify_state()

    def _close_at(self, price: float, ts: float, kind: str) -> None:
        pos = self._position
        assert pos is not None
        gross = pos.size * (price - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - price)
        exit_fee = pos.size * price * self._taker
        self._equity += gross - exit_fee
        self._fills.append(Fill(ts, pos.side, price, pos.size, exit_fee, kind))
        notional = pos.entry * pos.size
        net = gross - self._entry_fee - exit_fee
        trade = Trade(side=pos.side, size=pos.size, entry=pos.entry, exit=price,
                      entry_ts=self._entry_ts, exit_ts=ts, pnl=net, fee=self._entry_fee + exit_fee,
                      return_pct=(net / notional * 100.0) if notional else 0.0, reason=kind,
                      symbol=self._symbol, context=self._entry_context)
        self._trades.append(trade)
        # Close atomically BEFORE notifying: clear position state first so a failing
        # on_trade callback (e.g. disk error during persistence) can neither leave a
        # ghost position nor over-count equity on a subsequent re-close. The callback
        # is best-effort telemetry — isolate it so it can never propagate out of the
        # price-driven trading path or strand the broker in a half-updated state.
        self._position = None
        self._funding_anchor_ts = None
        self._entry_context = None
        if self._on_trade is not None:
            try:
                self._on_trade(trade)
            except Exception:
                _log.exception("on_trade callback failed; trade recorded, position closed")
        # After the trade is logged: dying in between leaves the position in the saved
        # state, and Runtime.build() then finds it already in the log (no duplicate).
        self._notify_state()

    # --- state across restarts --------------------------------------------
    def export_state(self) -> dict:
        """Realized cash and the open position (the pending entry is not kept: an order
        that has not filled is simply re-armed by the strategy after a restart)."""
        pos = self._position
        return {
            "equity": self._equity,
            "position": None if pos is None else {
                "side": pos.side, "size": pos.size, "entry": pos.entry, "stop": pos.stop,
                "entry_ts": self._entry_ts, "entry_fee": self._entry_fee, "context": self._entry_context,
            },
        }

    def restore(self, *, equity: float, position: dict | None = None) -> None:
        """Continue a paper account after a restart: its realized cash (any finite value:
        a fixed-notional paper account can go below zero) and the position that was open
        when the process died, which is closed at the FIRST price seen (reason "restart")."""
        if not math.isfinite(equity):
            raise ValueError("restore: equity must be finite")
        if self._position is not None or self._pending is not None:
            raise RuntimeError("restore() must run before trading starts")
        restored = None
        if position:
            side = position["side"]
            size, entry, stop = float(position["size"]), float(position["entry"]), float(position["stop"])
            if side not in ("LONG", "SHORT") or not all(math.isfinite(x) for x in (size, entry, stop)) \
                    or size <= 0 or entry <= 0:
                raise ValueError(f"restore: invalid position {position!r}")
            restored = Position(side, size, entry, stop)
        self._equity = equity
        if restored is not None:
            self._position = restored
            self._last_price = restored.entry       # no unrealized P&L until a real price arrives
            self._entry_ts = float(position["entry_ts"])
            self._entry_fee = float(position.get("entry_fee", 0.0))
            self._entry_context = position.get("context")
            self._restart_close = True

    def _notify_state(self) -> None:
        if self._on_state is None:
            return
        try:
            self._on_state(self.export_state())
        except Exception:
            _log.exception("on_state callback failed; paper account state not saved")

    def _apply_funding(self, price: float, ts: float) -> None:
        if self._position is None or self._funding_anchor_ts is None or self._funding == 0.0:
            return
        elapsed = ts - self._funding_anchor_ts
        hours = int(elapsed // 3600)
        if hours < 1:
            return
        notional = self._position.size * price
        sign = 1.0 if self._position.side == "LONG" else -1.0
        self._equity -= sign * hours * notional * self._funding
        self._funding_anchor_ts += hours * 3600
