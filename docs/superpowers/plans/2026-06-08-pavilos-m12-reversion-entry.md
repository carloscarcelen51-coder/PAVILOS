# PAVILOS M12: Mean-Reversion Entry Mode + Strategy Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Add a principled SECOND entry thesis — **mean-reversion bounce** (buy AT a strong support with a fixed stop + R-multiple take-profit) — alongside the existing momentum mode, selectable by config, and compare them honestly with walk-forward on the recorded lake. The momentum thesis barely sets up (structural low frequency); the bounce thesis fires on every approach to a support, so it should trade far more — letting us actually test for edge.

**Architecture:** A new `PaperBroker.enter_market` opens a position immediately at the current price (the bounce enters AT the support, not on a stop above it). `SignalEngine` gains `entry_mode` ("momentum" | "reversion"); reversion is a clean fixed bracket: market entry near a strong support, stop beyond it (ATR-floored), take-profit at `tp_mult`× the risk distance — no opposing-wall exit (that caused fee-bleed). Everything is config-driven (`RuntimeConfig.entry_mode`, `tp_mult`) and backtested by the existing faithful `run_backtest`/`walk_forward` (M6) on M11-replayed snapshots. A CLI `mode-compare` walk-forwards each mode and prints OOS side by side.

**Tech Stack:** Python 3.13, reuses merged M1–M11 (`PaperBroker`, `SignalEngine`, `Detector`, `run_backtest`, `walk_forward`, M11 `replay_snapshots`). `pytest`.

---

## Anti-overfitting discipline (non-negotiable, per [[project-brujita-walkforward-leak]] + [[feedback-avoid-inflation-patterns]])
- Exactly TWO principled theses (momentum, reversion) — NOT a fishing expedition over dozens of ideas.
- Each mode is judged by **walk-forward OOS** (optimise in-sample, score out-of-sample), never in-sample.
- The CLI prints **#OOS trades** next to every return; a good return on few trades is noise, stated as such.
- The reversion exit is a FIXED bracket (stop + R-multiple TP) — no data-fitted exit rule.
- Report whatever the OOS shows, including "still no edge". Do not tune to green.

---

## Scope decisions
1. **Reversion = fixed bracket.** Entry market at a near strong support; stop beyond the support, ATR-floored; TP at `tp_mult * (entry - stop)` (LONG) / mirrored. Exits ONLY on stop (broker) or TP (signal). No opposing-wall exit, no trail (v1 — keep it clean + testable; trailing/support-vanished are deferred refinements).
2. **Immediate market entry** via new `PaperBroker.enter_market(side, *, stop, size, ts)` — opens at the broker's current `_last_price`. Reversion has NO pending state (IDLE → IN_POSITION directly).
3. **Both modes coexist**, selected by `RuntimeConfig.entry_mode`; momentum is unchanged + default, so all existing tests/behaviour hold.
4. **Faithful backtest reuse:** `run_backtest`'s SignalEngine builder passes `entry_mode`/`tp_mult`; the M11 replay + M6 walk_forward are unchanged.
5. **Comparison, not just one mode:** the CLI runs walk_forward for EACH mode on the same slice and prints both OOS results.

**Deferred:** trailing the reversion stop, exit-on-support-vanished, partial take-profits, a third thesis (order-flow imbalance), joint mode×param optimisation.

---

## File Structure
```
PAVILOS/
├── src/pavilos/execution/broker.py      # + enter_market [MODIFY]
├── src/pavilos/signals/engine.py        # + entry_mode reversion path [MODIFY]
├── src/pavilos/core/runtime.py          # RuntimeConfig.entry_mode + tp_mult [MODIFY]
├── src/pavilos/backtest/runner.py       # _signal passes entry_mode/tp_mult [MODIFY]
├── scripts/analyze.py                   # + mode-compare [MODIFY]
└── tests/unit/
    ├── test_broker.py                   # enter_market [MODIFY]
    ├── test_signal_engine.py            # reversion mode [MODIFY]
    └── test_analyze_cli.py              # mode-compare format [MODIFY]
```

---

## Task 1: PaperBroker.enter_market

**Files:** Modify `src/pavilos/execution/broker.py`; Test `tests/unit/test_broker.py`.

- [ ] **Step 1: Failing test — add to `tests/unit/test_broker.py`:**
```python
def test_enter_market_opens_immediately_at_last_price():
    from pavilos.execution.broker import PaperBroker
    b = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005)
    b.on_price(100.0, ts=1.0)                       # establishes last price
    b.enter_market("LONG", stop=98.0, size=2.0, ts=1.0)
    pos = b.position()
    assert pos is not None and pos.side == "LONG" and pos.entry == 100.0 and pos.stop == 98.0
    # taker fee charged on entry notional
    assert abs(b.equity(100.0) - (10_000.0 - 2.0 * 100.0 * 0.0005)) < 1e-9
    # a LONG stop below the entry still fills via on_price
    b.on_price(97.0, ts=2.0)
    assert b.position() is None and b.trades()[-1].reason == "stop"


def test_enter_market_validates_stop_side_and_state():
    import pytest
    from pavilos.execution.broker import PaperBroker
    b = PaperBroker(starting_equity=10_000.0)
    b.on_price(100.0, ts=1.0)
    with pytest.raises(ValueError):                  # LONG stop must be below price
        b.enter_market("LONG", stop=101.0, size=1.0, ts=1.0)
    with pytest.raises(ValueError):
        b.enter_market("SHORT", stop=99.0, size=1.0, ts=1.0)   # SHORT stop must be above
    b.enter_market("LONG", stop=98.0, size=1.0, ts=1.0)
    with pytest.raises(RuntimeError):                # already in a position
        b.enter_market("LONG", stop=98.0, size=1.0, ts=1.0)
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — add `enter_market` to `PaperBroker` (after `place_entry`):**
```python
    def enter_market(self, side: str, *, stop: float, size: float, ts: float) -> None:
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
        self._open(side, price, stop, size, ts)
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(broker): add enter_market (immediate fill at current price)`.

---

## Task 2: SignalEngine reversion mode

**Files:** Modify `src/pavilos/signals/engine.py`; Test `tests/unit/test_signal_engine.py`.

- [ ] **Step 1: Failing test — add to `tests/unit/test_signal_engine.py`** (uses the file's existing `_engine`/analysis helpers; add a reversion engine builder):
```python
def _rev_engine(**over):
    kw = dict(entry_threshold=0.5, trail_threshold=0.5, opposing_threshold=0.7,
              min_persistence_s=0.0, min_venues=1, entry_offset_bps=2.0, stop_offset_bps=5.0,
              atr_stop_mult=3.0, opposing_distance_bps=8.0, risk_pct=0.01, max_leverage=10.0,
              entry_zone_bps=50.0, pending_timeout_s=10.0, entry_mode="reversion", tp_mult=2.0)
    kw.update(over)
    return SignalEngine(**kw)


def test_reversion_enters_market_at_near_support_with_bracket():
    eng = _rev_engine()
    broker = _Broker()                       # the test double already used in this file
    # price just above a strong support -> reversion enters LONG market immediately
    a = _analysis(mid=100.0, supports=[_zone(low=99.6, high=99.8, conf=0.9, persistence_s=30, venues=("k","b","o"))],
                  resistances=[], atr=0.5, ts=1.0)
    eng.update(a, atr=0.5, broker=broker)
    pos = broker.position()
    assert pos is not None and pos.side == "LONG"      # entered immediately (no pending)
    assert eng.state == "IN_POSITION"
    assert pos.stop < pos.entry                        # stop below
    # take-profit recorded at tp_mult x risk above entry
    risk = pos.entry - pos.stop
    assert abs(eng._tp - (pos.entry + 2.0 * risk)) < 1e-6


def test_reversion_takes_profit_at_r_multiple():
    eng = _rev_engine()
    broker = _Broker()
    a = _analysis(mid=100.0, supports=[_zone(99.6, 99.8, 0.9, 30, ("k","b"))], resistances=[], atr=0.5, ts=1.0)
    eng.update(a, atr=0.5, broker=broker)
    tp = eng._tp
    # next snapshot: price reaches the TP -> exit via close (not stop), back to IDLE
    a2 = _analysis(mid=tp + 0.1, supports=[_zone(99.6, 99.8, 0.9, 30, ("k","b"))], resistances=[], atr=0.5, ts=2.0)
    eng.update(a2, atr=0.5, broker=broker)
    assert broker.position() is None and eng.state == "IDLE"
    assert broker.trades()[-1].reason == "close" and broker.trades()[-1].pnl > 0


def test_reversion_does_not_arm_without_near_support():
    eng = _rev_engine()
    broker = _Broker()
    a = _analysis(mid=100.0, supports=[_zone(95.0, 95.2, 0.9, 30, ("k","b"))],   # far below (> entry_zone)
                  resistances=[], atr=0.5, ts=1.0)
    eng.update(a, atr=0.5, broker=broker)
    assert broker.position() is None and eng.state == "IDLE"
```
  NOTE to implementer: reuse whatever test doubles/builders already exist in `test_signal_engine.py` (`_Broker`, `_analysis`, `_zone`, `_engine`). If their names differ, adapt these tests to the existing helpers (do NOT rewrite the file's harness). The three assertions that matter: reversion enters IMMEDIATELY at a near support (no pending state), exits at the R-multiple TP as a "close" with positive pnl, and does not arm when no support is within `entry_zone_bps`.

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement in `src/pavilos/signals/engine.py`:**
  (a) `__init__`: add params `entry_mode: str = "momentum"`, `tp_mult: float = 2.0`. Validate `entry_mode in ("momentum", "reversion")` and `tp_mult > 0`. Store them; add `self._tp = 0.0`.
  (b) `update`: keep the existing fill/stop-out state sync. Then dispatch by mode:
```python
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
```
  (c) Add `_maybe_enter_reversion`: pick the highest-confidence operable support within `entry_zone_bps` below price (LONG) or resistance above (SHORT), mirroring `_maybe_enter`'s near-gate; then enter MARKET with an ATR-floored stop beyond the zone and a TP at `tp_mult * risk`:
```python
    def _maybe_enter_reversion(self, analysis, price, atr, broker):
        zone_tol = price * self.entry_zone_bps / 1e4
        best, best_dir = None, None
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

    def _manage_reversion(self, analysis, price, broker):
        pos = broker.position()
        if pos is None:
            return
        hit_tp = price >= self._tp if pos.side == "LONG" else price <= self._tp
        if hit_tp:
            broker.close(ts=analysis.ts)
            self.state, self._thesis, self._dir = "IDLE", None, None
```
  (The broker's `on_price` already closes on the stop as `reason="stop"`; the signal only needs to handle the TP. The fill-tick `return` in `update` (state set to IN_POSITION on the same tick the position appears) already exists for momentum — for reversion the entry is synchronous so `update` sets IN_POSITION directly; make sure the post-entry `update` does not immediately re-manage on the SAME call: after `enter_market` + setting state, `_maybe_enter_reversion` returns and `update` ends, so the next snapshot manages — good.)

- [ ] **Step 4:** Run → pass (+ existing momentum tests still pass). **Step 5:** full suite. **Step 6:** Commit `feat(signals): add reversion entry mode (bounce at support, R-multiple bracket)`.

---

## Task 3: wire entry_mode through config/backtest + CLI mode-compare

**Files:** Modify `src/pavilos/core/runtime.py`, `src/pavilos/backtest/runner.py`, `scripts/analyze.py`; Test `tests/unit/test_analyze_cli.py`.

- [ ] **Step 1:** `RuntimeConfig`: add `entry_mode: str = "momentum"` and `tp_mult: float = 2.0`.
- [ ] **Step 2:** `runner.py` `_signal(c)`: pass `entry_mode=c.entry_mode, tp_mult=c.tp_mult` to `SignalEngine(...)`. (Runtime.build's live SignalEngine should also pass them — add there too for consistency.)
- [ ] **Step 3: Failing test — add to `tests/unit/test_analyze_cli.py`:**
```python
def test_format_mode_row_readable():
    from scripts.analyze import format_mode_row
    from pavilos.backtest.runner import BacktestResult
    folds = [{"is_result": BacktestResult(500, 8, 5, 3, 62.5, 40.0, 8.0, 0.40, 10040.0, 20.0, 0.2),
              "oos_result": BacktestResult(500, 6, 2, 4, 33.3, -15.0, 6.0, -0.15, 9985.0, 30.0, 0.3)}]
    s = format_mode_row("reversion", folds)
    assert "reversion" in s and "OOS" in s and "trades=" in s
```
- [ ] **Step 4:** Run → FAIL.
- [ ] **Step 5: Implement in `scripts/analyze.py`:**
  (a) `format_mode_row(mode, folds)` — mean OOS return + total OOS trades + mean IS return:
```python
def format_mode_row(mode: str, folds: list) -> str:
    if not folds:
        return f"  {mode:<10} (no folds)"
    oos = sum(f["oos_result"].return_pct for f in folds) / len(folds)
    tr = sum(f["oos_result"].n_trades for f in folds)
    is_ret = sum(f["is_result"].return_pct for f in folds) / len(folds)
    return f"  {mode:<10} mean IS={is_ret:+.2f}%  ->  mean OOS={oos:+.2f}%  over {tr} OOS trades"
```
  (b) Add a `mode-compare` mode to `main()`: replay once at the configured window, then run `walk_forward` for each of `["momentum", "reversion"]` with a per-mode grid that fixes `entry_mode` and varies the relevant params:
```python
    elif mode == "mode-compare":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        print(f"=== mode compare, {n_splits} folds, {len(snaps)} snapshots ===")
        import dataclasses as _dc
        for m, grid in (("momentum", _MOM_GRID), ("reversion", _REV_GRID)):
            cfg = _dc.replace(base_cfg, entry_mode=m)
            folds = walk_forward(snaps, base_config=cfg, grid=grid, n_splits=n_splits,
                                 starting_equity=base_cfg.starting_equity)
            print(format_mode_row(m, folds))
```
  with module-level grids:
```python
_MOM_GRID = {"entry_threshold": [0.4, 0.55, 0.7], "opposing_distance_bps": [5.0, 10.0, 20.0],
             "entry_zone_bps": [15.0, 30.0, 60.0], "atr_stop_mult": [2.0, 3.0, 5.0]}
_REV_GRID = {"entry_threshold": [0.4, 0.55, 0.7], "entry_zone_bps": [15.0, 30.0, 60.0],
             "atr_stop_mult": [2.0, 3.0, 5.0], "tp_mult": [1.5, 2.0, 3.0]}
```
  (Update the module docstring usage to list `mode-compare`.)
- [ ] **Step 6:** Run test → pass; `python -c "import scripts.analyze"`. **Step 7:** full suite. **Step 8:** Commit `feat: wire entry_mode + tp_mult; analyze mode-compare (momentum vs reversion OOS)`.

---

## Task 4: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (246 prior + ~9 new); existing momentum/broker/runtime suites still green (momentum is the default, unchanged).
- [ ] **Step 2:** Imports clean: `python -c "import scripts.analyze; print('OK')"`.
- [ ] **Step 3:** `git status` clean; `git tag m12-reversion-entry`.
- [ ] **Step 4 (operator):** `python -m scripts.analyze D:\pavilos_book_data mode-compare 4 <t0> <t1>` on a recorded slice → compare momentum vs reversion mean OOS + #trades, with the small-data caveat.

---

## Self-Review (plan author)
**Coverage:** broker market entry (T1) → reversion mode (T2) → config/backtest wiring + CLI compare (T3) → suite/tag/run (T4). Delivers a principled second thesis + an honest head-to-head.
**Discipline:** two theses only; walk-forward OOS is the verdict; #trades shown; fixed bracket exit (no fitted exit). Momentum unchanged + default → no regression.
**Type consistency:** `enter_market(side, *, stop, size, ts)`; `SignalEngine(..., entry_mode, tp_mult)` with `self._tp`; `RuntimeConfig.entry_mode/tp_mult`; `runner._signal` passes them; `format_mode_row(mode, folds)`. Reversion reuses `_operable`, `position_size`, and the broker's stop handling; only the TP is signal-managed.
**Faithfulness/no-leak:** reversion enters at the CURRENT snapshot's price (causal); TP/stop are forward levels checked per subsequent snapshot (no look-ahead); the backtest path (replay → Detector → SignalEngine → PaperBroker) is the verified M6/M11 pipeline. enter_market fills at `_last_price` set by the same tick's `on_price`.
**Adversarial focus (3rd barrier):** (1) reversion enters IMMEDIATELY (no pending) at a near support; verify it does NOT enter when no support within entry_zone, and the stop/TP geometry is correct both directions; (2) no same-tick enter-then-exit fee-bleed (entry tick sets IN_POSITION and returns; TP/stop only evaluated next tick) — verify a reversion trade spans ≥2 ticks unless price genuinely gaps to stop/TP; (3) enter_market validation (stop side, already-in-position, non-finite); (4) momentum mode + ALL existing tests unchanged (default path); (5) walk_forward over each mode is leak-free (IS/OOS split intact); (6) the mode-compare grids fix entry_mode (a reversion grid must not silently run momentum); (7) garbage ATR / stop on wrong side → no entry (the `ok` guard). Item (2) no-fee-bleed and (4) momentum-unchanged are the headline.
