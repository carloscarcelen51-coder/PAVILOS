# PAVILOS M3: Signals + Paper Broker (network-free core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification (try to break it: a price sequence that double-fills, a stop that trails the WRONG way, a cancel that races a fill, equity that drifts, a state stuck after a stop-out, sizing that exceeds leverage). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn detected support/resistance zones into **paper trades**: position just above a support (LONG) / below a resistance (SHORT), wait for the breakout fill, trail the stop up the rising supports (ATR-floored), exit on the opposite signal, and **cancel the pending entry if the thesis zone vanishes before filling** — all simulated against a `PaperBroker` modelling Kraken Futures `PF_XBTUSD` (fees + funding), driven by the combined-book stream.

**Architecture:** Pure, deterministic units under `src/pavilos/execution/` and `src/pavilos/signals/`, plus a thin `TradingEngine` that wires the existing `Engine` snapshot stream → `Detector` (M2) → `SignalEngine` → `PaperBroker`. The signal state machine and broker are driven by `(DepthAnalysis, price, ts)` inputs with no network, so the whole trading loop is unit-testable from scripted snapshot sequences.

**Tech Stack:** Python 3.13, stdlib only (`dataclasses`, `enum`, `collections.deque`, `math`), `pytest` (+ `pytest-asyncio` for the wiring task). Builds on merged M1 (`Engine`, `CombinedDepthSnapshot`) and M2 (`Detector`, `DepthAnalysis`, `Zone`).

---

## Scope decisions (READ FIRST)

1. **Paper fills against the combined mid.** We have no Kraken Futures `PF_XBTUSD` trade feed (M1 connectors are spot order books). The `PaperBroker` is driven by `on_price(price, ts)` where `price` is the combined mid from each `CombinedDepthSnapshot`. Realistic fills against `PF_XBTUSD` trade prints are **deferred** to a futures-trade-feed milestone. Entry/stop orders fill at their **trigger price** (deterministic; slippage modelling is a noted refinement).
2. **Entry = breakout confirmation.** A LONG enters with a **buy-stop just above** an operable support's high (fills when price rises through it — the support held and price is bouncing up); stop-loss just **below** the support's low. SHORT is symmetric around a resistance. This matches the user's "posicionarse encima del soporte y esperar a que se ejecute subiendo".
3. **One position at a time** (Fase-1 simplicity). While in a position or with a pending entry, no new setups.
4. **Long and short** both, on a linear USD perp; size capped by `max_leverage` (~10x EEA) modelled.
5. **Parameters need calibration** (M-later) against real data; tests pin the *logic*, defaults are reasonable starting points.

**Deferred (not gaps):** futures-trade-feed fills; slippage/partial fills; multiple concurrent positions; maker/limit resting at the support (we use stop entries); pulled-event-driven cancel (we cancel on zone-absence, which subsumes pulled since M2 excludes pulled zones from output); dashboard/Telegram (M4).

---

## File Structure

```
PAVILOS/
├── src/pavilos/execution/
│   ├── __init__.py
│   └── broker.py            # OrderSide/Position/Fill + PaperBroker [NEW]
├── src/pavilos/signals/
│   ├── __init__.py
│   ├── atr.py               # rolling ATR from a price stream [NEW]
│   ├── sizing.py            # position_size(equity, entry, stop, ...) [NEW]
│   └── engine.py            # SignalEngine state machine [NEW]
├── src/pavilos/core/
│   └── trading_engine.py    # wire Engine.snapshots -> Detector -> SignalEngine -> PaperBroker [NEW]
└── tests/unit/
    ├── test_paper_broker.py
    ├── test_atr.py
    ├── test_sizing.py
    ├── test_signal_engine.py
    └── test_trading_engine.py
```

---

## Task 1: PaperBroker

**Files:** Create `src/pavilos/execution/__init__.py` (empty), `src/pavilos/execution/broker.py`; Test `tests/unit/test_paper_broker.py`.

- [ ] **Step 1: Create `src/pavilos/execution/__init__.py` as an EMPTY file.**

- [ ] **Step 2: Failing test — `tests/unit/test_paper_broker.py`:**

```python
# tests/unit/test_paper_broker.py
from pavilos.execution.broker import PaperBroker, Position


def _bk(**kw):
    return PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002,
                       funding_rate_hourly=0.0, **kw)


def test_long_entry_fills_on_trigger_and_charges_fee():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(99.0, ts=0.0)              # below trigger -> no fill
    assert bk.position() is None and bk.pending_entry() is not None
    bk.on_price(100.0, ts=1.0)            # touches trigger -> fill
    pos = bk.position()
    assert isinstance(pos, Position) and pos.side == "LONG" and pos.size == 2.0
    assert pos.entry == 100.0 and pos.stop == 98.0
    assert bk.pending_entry() is None
    # entry fee = size*entry*taker = 2*100*0.0005 = 0.10
    assert abs(bk.equity() - (10_000.0 - 0.10)) < 1e-9


def test_long_stop_out_realizes_loss_and_clears_position():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)            # fill
    bk.on_price(97.0, ts=2.0)            # below stop -> stop fills at stop=98.0
    assert bk.position() is None
    # pnl = 2*(98-100) = -4.0 ; entry fee 0.10 ; exit fee 2*98*0.0005=0.098
    assert abs(bk.equity() - (10_000.0 - 4.0 - 0.10 - 0.098)) < 1e-9


def test_short_entry_and_stop_are_mirrored():
    bk = _bk()
    bk.place_entry("SHORT", trigger=100.0, stop=102.0, size=1.0)
    bk.on_price(101.0, ts=0.0)           # above trigger -> no short fill
    assert bk.position() is None
    bk.on_price(100.0, ts=1.0)           # touches trigger -> short fills
    assert bk.position().side == "SHORT"
    bk.on_price(103.0, ts=2.0)           # above stop -> stop fills at 102.0
    assert bk.position() is None
    # pnl = 1*(100-102) = -2.0 ; fees 1*100*.0005 + 1*102*.0005
    assert abs(bk.equity() - (10_000.0 - 2.0 - 0.05 - 0.051)) < 1e-9


def test_cancel_entry_clears_pending():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.cancel_entry()
    bk.on_price(100.0, ts=1.0)
    assert bk.pending_entry() is None and bk.position() is None
    assert bk.equity() == 10_000.0


def test_modify_stop_and_close_take_profit():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)
    bk.on_price(110.0, ts=2.0)           # price rose; equity reflects unrealized
    assert abs(bk.equity() - (10_000.0 - 0.10 + 2.0 * (110.0 - 100.0))) < 1e-9
    bk.modify_stop(105.0)                 # trail up
    assert bk.position().stop == 105.0
    bk.close(ts=3.0)                      # market close at last price 110
    assert bk.position() is None
    # realized pnl = 2*(110-100)=20 ; exit fee 2*110*.0005=0.11
    assert abs(bk.equity() - (10_000.0 - 0.10 + 20.0 - 0.11)) < 1e-9


def test_funding_charged_hourly_to_longs():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0,
                     funding_rate_hourly=0.0001)
    bk.place_entry("LONG", trigger=100.0, stop=90.0, size=1.0)
    bk.on_price(100.0, ts=0.0)           # fill at t=0
    bk.on_price(100.0, ts=3600.0)        # +1h -> funding = notional*rate = 100*0.0001 = 0.01
    assert abs(bk.equity() - (10_000.0 - 0.01)) < 1e-9
```

- [ ] **Step 3:** run → FAIL.

- [ ] **Step 4: Implement — `src/pavilos/execution/broker.py`:**

```python
# src/pavilos/execution/broker.py
"""Paper broker for a linear USD perp (Kraken PF_XBTUSD model). Driven by
on_price(price, ts); no network. Entry/stop orders fill at their trigger price."""
from __future__ import annotations

import math
from dataclasses import dataclass


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


class PaperBroker:
    """Single-position paper broker. ``place_entry`` arms a stop entry (LONG fills
    when price >= trigger, SHORT when price <= trigger). A LONG stop-loss fills
    when price <= stop; SHORT when price >= stop. Fills are charged ``taker_fee``;
    funding is charged hourly on the notional (LONG pays, SHORT receives)."""

    def __init__(self, *, starting_equity: float, taker_fee: float = 0.0005,
                 maker_fee: float = 0.0002, funding_rate_hourly: float = 0.0) -> None:
        if starting_equity <= 0:
            raise ValueError("starting_equity must be positive")
        self._equity = starting_equity            # realized cash
        self._taker = taker_fee
        self._maker = maker_fee
        self._funding = funding_rate_hourly
        self._position: Position | None = None
        self._pending: dict | None = None         # {side, trigger, stop, size}
        self._last_price = 0.0
        self._funding_anchor_ts: float | None = None
        self._fills: list[Fill] = []

    # --- order management -------------------------------------------------
    def place_entry(self, side: str, *, trigger: float, stop: float, size: float) -> None:
        if side not in ("LONG", "SHORT"):
            raise ValueError(f"bad side {side!r}")
        if self._position is not None or self._pending is not None:
            raise RuntimeError("broker already has a position or pending entry")
        if size <= 0:
            raise ValueError("size must be positive")
        self._pending = {"side": side, "trigger": trigger, "stop": stop, "size": size}

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
        self._apply_funding(price, ts)
        if self._pending is not None:
            d = self._pending
            triggered = price >= d["trigger"] if d["side"] == "LONG" else price <= d["trigger"]
            if triggered:
                self._open(d["side"], d["trigger"], d["stop"], d["size"], ts)
                self._pending = None
        if self._position is not None:
            p = self._position
            hit = price <= p.stop if p.side == "LONG" else price >= p.stop
            if hit:
                self._close_at(p.stop, ts, "stop")
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

    # --- internals --------------------------------------------------------
    def _open(self, side: str, price: float, stop: float, size: float, ts: float) -> None:
        fee = size * price * self._taker
        self._equity -= fee
        self._position = Position(side, size, price, stop)
        self._funding_anchor_ts = ts
        self._last_price = price
        self._fills.append(Fill(ts, side, price, size, fee, "entry"))

    def _close_at(self, price: float, ts: float, kind: str) -> None:
        pos = self._position
        assert pos is not None
        pnl = pos.size * (price - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - price)
        fee = pos.size * price * self._taker
        self._equity += pnl - fee
        self._fills.append(Fill(ts, pos.side, price, pos.size, fee, kind))
        self._position = None
        self._funding_anchor_ts = None

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
```

- [ ] **Step 5:** run target → 6 passed. **Step 6:** full suite → 131 passed (125 + 6). **Step 7:** Commit `feat(execution): add PaperBroker (PF_XBTUSD model: fees + funding + stop entries)`.

---

## Task 2: Rolling ATR

**Files:** Create `src/pavilos/signals/__init__.py` (empty), `src/pavilos/signals/atr.py`; Test `tests/unit/test_atr.py`.

> No OHLC bars in the combined stream, so ATR is the mean absolute tick-to-tick
> price change over the last ``window`` ticks — a volatility proxy for the stop floor.

- [ ] **Step 1: Create `src/pavilos/signals/__init__.py` as an EMPTY file.**

- [ ] **Step 2: Failing test — `tests/unit/test_atr.py`:**

```python
# tests/unit/test_atr.py
from pavilos.signals.atr import ATR


def test_atr_zero_until_two_ticks():
    a = ATR(window=3)
    assert a.value() == 0.0
    a.update(100.0)
    assert a.value() == 0.0


def test_atr_is_mean_abs_change_over_window():
    a = ATR(window=3)
    for p in (100.0, 101.0, 103.0, 106.0):   # diffs 1,2,3
        a.update(p)
    assert abs(a.value() - 2.0) < 1e-9        # mean(1,2,3)

def test_atr_window_drops_old_ticks():
    a = ATR(window=2)
    for p in (100.0, 101.0, 103.0, 106.0):    # last 2 diffs: 2,3
        a.update(p)
    assert abs(a.value() - 2.5) < 1e-9        # mean(2,3)


def test_atr_ignores_non_finite():
    a = ATR(window=3)
    for p in (100.0, float("nan"), 102.0):
        a.update(p)
    assert a.value() == 2.0                    # nan tick skipped -> diff(100,102)=2
```

- [ ] **Step 3:** run → FAIL.

- [ ] **Step 4: Implement — `src/pavilos/signals/atr.py`:**

```python
# src/pavilos/signals/atr.py
"""Rolling ATR proxy: mean absolute tick-to-tick price change over a window."""
from __future__ import annotations

import math
from collections import deque


class ATR:
    """Feed mids via ``update(price)``; ``value()`` is the mean of the last
    ``window`` absolute consecutive-tick changes (0.0 until two ticks seen).
    Non-finite prices are ignored."""

    def __init__(self, window: int) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self._window = window
        self._diffs: deque[float] = deque(maxlen=window)
        self._prev: float | None = None

    def update(self, price: float) -> None:
        if not math.isfinite(price):
            return
        if self._prev is not None:
            self._diffs.append(abs(price - self._prev))
        self._prev = price

    def value(self) -> float:
        if not self._diffs:
            return 0.0
        return sum(self._diffs) / len(self._diffs)
```

- [ ] **Step 5:** run → 4 passed. **Step 6:** full suite → 135 passed. **Step 7:** Commit `feat(signals): add rolling ATR (mean abs tick change)`.

---

## Task 3: Position sizing

**Files:** Create `src/pavilos/signals/sizing.py`; Test `tests/unit/test_sizing.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_sizing.py`:**

```python
# tests/unit/test_sizing.py
from pavilos.signals.sizing import position_size


def test_size_risks_fixed_fraction_at_stop():
    # equity 10k, risk 1% = $100 risk; entry 100, stop 98 -> $2/unit -> 50 units
    s = position_size(10_000.0, entry=100.0, stop=98.0, risk_pct=0.01, max_leverage=100.0)
    assert abs(s - 50.0) < 1e-9


def test_size_capped_by_leverage():
    # raw size would be 50 units (notional 5000), but max_leverage 2x on 10k = 20k cap...
    # tighten stop so raw size explodes, then leverage caps it
    s = position_size(10_000.0, entry=100.0, stop=99.99, risk_pct=0.01, max_leverage=2.0)
    # leverage cap = max_leverage*equity/entry = 2*10000/100 = 200 units
    assert abs(s - 200.0) < 1e-9


def test_zero_or_inverted_distance_returns_zero():
    assert position_size(10_000.0, entry=100.0, stop=100.0, risk_pct=0.01, max_leverage=10.0) == 0.0
    assert position_size(10_000.0, entry=100.0, stop=float("nan"), risk_pct=0.01, max_leverage=10.0) == 0.0
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/signals/sizing.py`:**

```python
# src/pavilos/signals/sizing.py
"""Risk-based position sizing with a leverage cap. Pure."""
from __future__ import annotations

import math


def position_size(equity: float, *, entry: float, stop: float,
                  risk_pct: float, max_leverage: float) -> float:
    """Units sized so a stop-out loses ``risk_pct`` of ``equity``, capped so the
    notional never exceeds ``max_leverage * equity``. Returns 0.0 on a
    non-positive/zero stop distance or any non-finite input."""
    if not all(math.isfinite(x) for x in (equity, entry, stop, risk_pct, max_leverage)):
        return 0.0
    if equity <= 0 or entry <= 0 or risk_pct <= 0 or max_leverage <= 0:
        return 0.0
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    size = (equity * risk_pct) / risk_per_unit
    max_size = max_leverage * equity / entry
    return max(0.0, min(size, max_size))
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite → 138 passed. **Step 6:** Commit `feat(signals): add risk-based position sizing with leverage cap`.

---

## Task 4: SignalEngine state machine

**Files:** Create `src/pavilos/signals/engine.py`; Test `tests/unit/test_signal_engine.py`.

> States: IDLE → PENDING_ENTRY → IN_POSITION → (stop/exit) → IDLE. Drives the
> broker from `(DepthAnalysis, atr)`. Picks the highest-confidence OPERABLE zone
> (confidence/persistence/venues gates) on the correct side of price, arms a
> breakout entry, cancels if the thesis zone vanishes, trails the stop up rising
> supports (ATR-floored), exits on a near opposing wall.

- [ ] **Step 1: Failing test — `tests/unit/test_signal_engine.py`:**

```python
# tests/unit/test_signal_engine.py
import pytest

from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.execution.broker import PaperBroker
from pavilos.signals.engine import SignalEngine


def _zone(side, price, low, high, conf=0.9, persistence_s=100.0, venues=("k", "b", "c")):
    return Zone(side=side, price=price, low=low, high=high, strength=20.0,
                venues=venues, persistence_s=persistence_s, pulled=False, confidence=conf)


def _analysis(ts, mid, supports=(), resistances=()):
    return DepthAnalysis(ts=ts, mid=mid, supports=tuple(supports), resistances=tuple(resistances))


def _engine():
    return SignalEngine(entry_threshold=0.6, trail_threshold=0.6, opposing_threshold=0.7,
                        min_persistence_s=5.0, min_venues=2, entry_offset_bps=2.0,
                        stop_offset_bps=2.0, atr_stop_mult=3.0, opposing_distance_bps=30.0,
                        risk_pct=0.01, max_leverage=10.0)


def _bk():
    return PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0, funding_rate_hourly=0.0)


def test_arms_entry_above_operable_support():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    assert e.state == "PENDING_ENTRY"
    pend = bk.pending_entry()
    assert pend["side"] == "LONG"
    assert abs(pend["trigger"] - 99.2 * (1 + 2.0 / 1e4)) < 1e-9   # just above support.high
    assert abs(pend["stop"] - 98.8 * (1 - 2.0 / 1e4)) < 1e-9      # just below support.low


def test_cancels_pending_when_thesis_support_vanishes():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    assert e.state == "PENDING_ENTRY"
    # next snapshot: support gone, entry not yet filled (price never reached trigger)
    e.update(_analysis(2.0, mid=100.0, supports=[]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.pending_entry() is None and bk.position() is None


def test_fill_transitions_to_in_position():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=99.0, supports=[sup]), atr=1.0, broker=bk)   # arm; mid below trigger
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # mid crosses trigger -> fill
    assert e.state == "IN_POSITION" and bk.position().side == "LONG"


def test_trails_stop_up_as_higher_supports_form_but_not_inside_atr_floor():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=99.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill, stop ~98.8
    stop0 = bk.position().stop
    higher = _zone(Side.SUPPORT, price=104.0, low=103.8, high=104.2)
    # price 110, atr 1, atr_floor=110-3=107; support_stop=103.8*(1-2bps)=~103.78 < 107 -> stop->~103.78
    e.update(_analysis(3.0, mid=110.0, supports=[higher]), atr=1.0, broker=bk)
    assert bk.position().stop > stop0
    assert abs(bk.position().stop - 103.8 * (1 - 2.0 / 1e4)) < 1e-6


def test_exits_on_near_opposing_resistance():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=99.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill
    assert e.state == "IN_POSITION"
    res = _zone(Side.RESISTANCE, price=101.2, low=101.1, high=101.3, conf=0.9)  # ~10bps above 101
    e.update(_analysis(3.0, mid=101.0, supports=[sup], resistances=[res]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.position() is None


def test_stop_out_returns_to_idle():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=99.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill
    e.update(_analysis(3.0, mid=98.0, supports=[sup]), atr=1.0, broker=bk)   # below stop -> stop-out
    assert e.state == "IDLE" and bk.position() is None


def test_ignores_non_operable_zone():
    e, bk = _engine(), _bk()
    weak = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2, conf=0.5)  # below entry_threshold
    e.update(_analysis(1.0, mid=100.0, supports=[weak]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.pending_entry() is None
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/signals/engine.py`:**

```python
# src/pavilos/signals/engine.py
"""SignalEngine: drives a PaperBroker from DepthAnalysis. IDLE -> PENDING_ENTRY
-> IN_POSITION -> IDLE. Long off supports, short off resistances (mirrored)."""
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
            trigger = best.high * (1 + self.entry_offset_bps / 1e4)
            stop = best.low * (1 - self.stop_offset_bps / 1e4)
        else:
            trigger = best.low * (1 - self.entry_offset_bps / 1e4)
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
```

- [ ] **Step 4:** run → 7 passed. **Step 5:** full suite → 145 passed. **Step 6:** Commit `feat(signals): add SignalEngine state machine (entry/cancel/trail/exit)`.

---

## Task 5: TradingEngine wiring

**Files:** Create `src/pavilos/core/trading_engine.py`; Test `tests/unit/test_trading_engine.py`.

> Consumes `CombinedDepthSnapshot`s (from `Engine.snapshots`) and drives the full
> loop: `Detector.update(snap)` → feed ATR with `snap.mid` → `SignalEngine.update(analysis, atr, broker)`.
> `run(stop)` pulls from the injected snapshot queue; `process(snap)` is the
> sync per-snapshot step (unit-tested directly).

- [ ] **Step 1: Failing test — `tests/unit/test_trading_engine.py`:**

```python
# tests/unit/test_trading_engine.py
import asyncio

from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.execution.broker import PaperBroker
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.core.trading_engine import TradingEngine


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=2)


def _components():
    detector = Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                        match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                        persistence_target_s=1.0, venues_target=2.0, strength_target=5.0)
    signal = SignalEngine(entry_threshold=0.3, trail_threshold=0.3, opposing_threshold=0.7,
                          min_persistence_s=0.0, min_venues=2, entry_offset_bps=2.0,
                          stop_offset_bps=2.0, atr_stop_mult=3.0, opposing_distance_bps=30.0,
                          risk_pct=0.01, max_leverage=10.0)
    broker = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0, funding_rate_hourly=0.0)
    return TradingEngine(detector, signal, broker, ATR(window=10))


def test_process_runs_pipeline_and_arms_then_fills():
    te = _components()
    # snapshot with a support wall at 99.x, mid below the future trigger
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]
    asks = [_bin(100.5, 1.0)]
    te.process(_snap(1.0, 99.5, bids, asks))   # detect support, arm entry
    assert te.signal.state == "PENDING_ENTRY"
    te.process(_snap(2.0, 102.0, bids, asks))  # mid crosses trigger -> fill
    assert te.signal.state == "IN_POSITION" and te.broker.position() is not None


def test_run_consumes_queue_until_stop():
    te = _components()
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]
    asks = [_bin(100.5, 1.0)]

    async def scenario():
        q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await q.put(_snap(1.0, 99.5, bids, asks))
        await q.put(_snap(2.0, 102.0, bids, asks))
        task = asyncio.create_task(te.run(q, stop))
        # let it drain both
        for _ in range(50):
            if te.broker.position() is not None:
                break
            await asyncio.sleep(0)
        stop.set()
        await q.put(_snap(3.0, 102.0, bids, asks))  # unblock the queue.get
        await asyncio.wait_for(task, timeout=1.0)
        return te.broker.position()

    assert asyncio.run(scenario()) is not None
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/core/trading_engine.py`:**

```python
# src/pavilos/core/trading_engine.py
"""TradingEngine: wire the combined-book snapshot stream through detection ->
signals -> paper broker. Network-free; the snapshot source is injected."""
from __future__ import annotations

import asyncio

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine


class TradingEngine:
    def __init__(self, detector: Detector, signal: SignalEngine, broker, atr: ATR) -> None:
        self.detector = detector
        self.signal = signal
        self.broker = broker
        self.atr = atr

    def process(self, snapshot: CombinedDepthSnapshot) -> None:
        """One snapshot through the full pipeline (sync, deterministic)."""
        analysis = self.detector.update(snapshot)
        self.atr.update(snapshot.mid)
        self.signal.update(analysis, self.atr.value(), self.broker)

    async def run(self, snapshots: "asyncio.Queue[CombinedDepthSnapshot]",
                  stop: "asyncio.Event") -> None:
        """Consume snapshots until ``stop`` is set."""
        while not stop.is_set():
            snap = await snapshots.get()
            if stop.is_set():
                break
            self.process(snap)
```

- [ ] **Step 4:** run → 2 passed. **Step 5:** full suite → 147 passed. **Step 6:** Commit `feat(core): add TradingEngine wiring detection->signals->paper broker`.

---

## Task 6: Full suite green + close-out

- [ ] **Step 1:** `python -m pytest -v` → ALL pass (147: 125 prior + 22 new — broker 6, atr 4, sizing 3, signal 7, trading 2).
- [ ] **Step 2:** `git status` clean.
- [ ] **Step 3:** `git tag m3-signals-paper && git log --oneline -8`.

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.4 signals + §5.5 execution):**
- Position above support / below resistance → Task 4 `_maybe_enter` (buy-stop above `support.high`, sell-stop below `resistance.low`) ✅.
- Wait for fill (breakout) → Task 1 `on_price` trigger fills + Task 4 PENDING_ENTRY→IN_POSITION ✅.
- Trail the stop up while supports hold, ATR floor → Task 4 `_manage` (raise to highest qualifying support, bounded by `price - atr*mult`) ✅.
- Close on opposite signal → Task 4 `_manage` near-opposing-wall exit ✅; stop-out → Task 1 stop fill ✅.
- Withdraw the pending entry if the support vanishes before filling → Task 4 `_maybe_cancel` ✅.
- Kraken `PF_XBTUSD` fees + funding → Task 1 PaperBroker (taker fee on fills, hourly funding) ✅; leverage cap → Task 3 ✅.
- Combined multi-exchange book drives it → Task 5 wires `Engine.snapshots` → Detector → SignalEngine ✅.
- *Deferred (documented, not gaps):* futures-trade-feed fills, slippage/partials, multi-position, maker resting entries, dashboard/Telegram (M4).

**Placeholder scan:** none; every step has full runnable code.

**Type consistency:** `Position(side, size, entry, stop)`, `Fill(ts, side, price, size, fee, kind)`, `PaperBroker.place_entry(side, *, trigger, stop, size)` / `on_price(price, ts)` / `modify_stop(new_stop)` / `close(*, ts)` / `position()` / `pending_entry()` / `equity(price=None)`; `ATR(window)` `.update`/`.value`; `position_size(equity, *, entry, stop, risk_pct, max_leverage)`; `SignalEngine(... ).update(analysis, atr, broker)` with `state` in {IDLE, PENDING_ENTRY, IN_POSITION}; `TradingEngine(detector, signal, broker, atr).process(snap)` / `run(queue, stop)`. Consistent across Tasks 1–5. The `Detector` constructor args match M2's merged signature (`size_multiple, min_size, max_gap_bps, max_zone_width_bps, match_overlap_bps, grace_s, window_bps, persistence_target_s, venues_target, strength_target`). `Engine.snapshots` is an `asyncio.Queue[CombinedDepthSnapshot]` (verified in `core/engine.py:32`).

**Calibration note:** all thresholds/offsets/risk params are constructor-injected; production VALUES need calibration (entry/trail/opposing thresholds, offsets, ATR mult, risk_pct). Tests pin the LOGIC (arms above support, cancels on vanish, fills→position, trails up & ATR-floored, exits on opposing wall, stop-out→IDLE, sizing risk+cap), not calibrated numbers.

**Adversarial focus (3rd barrier):** double-fill on a single price tick (entry+stop same tick?); cancel racing a fill within `update`; stop trailing the WRONG direction (must only tighten toward price, never loosen); equity conservation across entry→trail→close (no drift); state stuck after stop-out; funding sign (long pays, short receives) + multi-hour gaps; sizing never exceeds `max_leverage*equity/entry`; a snapshot with both an operable support and resistance (pick best, only one position); `on_price` non-finite guard; `place_entry` rejecting a second position; trigger crossed by a gap (fills at trigger, not the gapped price — slippage noted as deferred).

---

## Implementation notes (2026-06-07, shipped deviations)

These notes record where the shipped code intentionally diverges from the plan
draft above. The embedded code blocks earlier in this plan are kept verbatim as
the original draft; the bullets below are the source of truth for what shipped.

- **Entry geometry is MOMENTUM, not the draft's support.high trigger.** The
  shipped `SignalEngine._maybe_enter` arms a *breakout* in the trade direction:
  a LONG arms a buy-stop just ABOVE the current price at
  `price * (1 + entry_offset_bps / 1e4)`, with the protective stop below the
  detected support at `support.low * (1 - stop_offset_bps / 1e4)`; a SHORT is
  mirrored (sell-stop just BELOW price at `price * (1 - entry_offset_bps / 1e4)`,
  stop above the resistance at `resistance.high * (1 + stop_offset_bps / 1e4)`).
  The plan-draft's `support.high`-based trigger sat *below* the current price and
  would have instant-filled on arming, so it was replaced.
- **Offsets are validated positive.** The constructor rejects non-positive
  `entry_offset_bps` / `stop_offset_bps` (alongside the other tunables), so the
  trigger is always strictly above (LONG) / below (SHORT) the price and the stop
  always sits on the protective side of the zone.
- **Re-arm is deferred one tick after a stop-out.** When the broker closes a
  position, `update` returns to IDLE and waits for the NEXT snapshot before
  considering a new setup (no same-tick whipsaw re-entry), matching the
  discretionary-exit path.
- **The PaperBroker fills at the breaching market price.** Entry and stop orders
  fill at the price of the tick that triggers them, so a gap-through is modelled
  honestly and a stop never fills optimistically (no best-case stop price).
- **The T5 test uses a persistence warm-up snapshot.** A freshly-seen zone has
  `persistence_s == 0` and therefore `confidence == 0` on its first sighting, so
  it cannot clear `entry_threshold` on the first snapshot. The `TradingEngine`
  test feeds an identical warm-up snapshot first so the support persists and
  arms on the second snapshot.

**Close-out hardening (2026-06-07 barrier review):** `TradingEngine.run` now
honours `stop` while idle — it races `snapshots.get()` against `stop.wait()`
(FIRST_COMPLETED) so an empty queue still wakes the loop on shutdown, mirroring
`Aggregator.run`'s bounded-shutdown contract; on a stop/arrival race the
in-flight snapshot is processed rather than dropped. The loop is documented as
crash-loud (a per-tick exception propagates out for an awaiting supervisor to
surface). The constructor argument order is `(detector, atr, signal, broker)` to
mirror the `detect -> atr -> signal` pipeline, and the class carries a docstring
and a `broker: PaperBroker` type hint consistent with its collaborators.
`position_size` also returns `0.0` when the computed size or leverage cap is
non-finite (overflow guard).
