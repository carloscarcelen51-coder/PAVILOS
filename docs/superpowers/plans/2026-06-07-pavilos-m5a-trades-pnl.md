# PAVILOS M5a: Trade history + P&L + persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Make completed paper trades + cumulative P&L **consultable in the dashboard and durable across restarts**: the `PaperBroker` records each round-trip `Trade` (entry→exit, net P&L, %, reason); a `TradeLog` persists them to `paper_trades.jsonl` and loads them on startup; the dashboard shows a **Trades/History** table and an **all-time P&L summary** (realized €, return %, # trades, win-rate, W/L).

**Architecture:** The `PaperBroker` gains a `Trade` record on every close (net of fees) and an optional `on_trade` callback. A `TradeLog` (file I/O, isolated + testable with tmp paths) appends each closed trade as a JSONL line and `load()`s all on startup. The `Runtime` loads history, keeps an all-time trade list (history + this session via the `on_trade` callback), and publishes `trades` + `summary` into `DashboardState` each tick. Session equity stays fresh per run (`starting_equity`); the durable all-time P&L is a separate view computed from the log — no equity re-seeding, no double counting.

**Tech Stack:** Python 3.13, stdlib (`dataclasses`, `json`, `pathlib`), `pytest`. Builds on merged M1–M4.

---

## Scope decisions
1. **Trade.pnl is NET of fees** (gross price P&L − entry_fee − exit_fee), **excludes funding** (funding is a small separate equity cost; default `funding_rate_hourly=0.0`). Documented on the dataclass.
2. **Session equity is fresh each run**; the durable thing is the **trade log** (all-time history + cumulative realized P&L). The dashboard shows both: live session (position/equity/uPnL) + all-time (trades table + summary).
3. **JSONL persistence** at `RuntimeConfig.trade_log_path` (default `paper_trades.jsonl` in cwd). Corrupt lines are skipped on load (robust).
4. The `TradeLog` does the file I/O (not the broker — broker stays pure/unit-testable); the broker exposes `on_trade` + `trades()`.

**Deferred:** equity-curve chart, per-trade funding allocation, CSV export, DB storage, Telegram (all later).

---

## File Structure
```
PAVILOS/
├── src/pavilos/execution/
│   ├── broker.py            # + Trade dataclass, on_trade cb, _trades, net-pnl recording [MODIFY]
│   └── trade_log.py         # TradeLog(JSONL) + summarize() [NEW]
├── src/pavilos/web/
│   ├── state.py             # snapshot += trades + summary [MODIFY]
│   └── static/index.html    # + Trades/History panel + all-time P&L summary [MODIFY]
├── src/pavilos/core/runtime.py  # wire TradeLog + publish trades/summary [MODIFY]
└── tests/unit/
    ├── test_paper_broker_trades.py
    ├── test_trade_log.py
    ├── test_dashboard_state.py     # extend
    └── test_runtime.py             # extend
```

---

## Task 1: PaperBroker records round-trip Trades

**Files:** Modify `src/pavilos/execution/broker.py`; Test `tests/unit/test_paper_broker_trades.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_paper_broker_trades.py`:**
```python
# tests/unit/test_paper_broker_trades.py
from pavilos.execution.broker import PaperBroker, Trade


def test_close_records_a_net_pnl_trade_and_calls_callback():
    seen = []
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002, on_trade=seen.append)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)      # entry fill; entry fee = 2*100*0.0005 = 0.10
    bk.on_price(110.0, ts=2.0)      # mark up
    bk.close(ts=3.0)                # close at 110; exit fee = 2*110*0.0005 = 0.11
    assert len(bk.trades()) == 1 and len(seen) == 1
    t = bk.trades()[0]
    assert isinstance(t, Trade) and t.side == "LONG" and t.size == 2.0
    assert t.entry == 100.0 and t.exit == 110.0 and t.reason == "close"
    assert t.entry_ts == 1.0 and t.exit_ts == 3.0
    # gross = 2*(110-100)=20 ; fees = 0.10+0.11=0.21 ; net = 19.79
    assert abs(t.fee - 0.21) < 1e-9
    assert abs(t.pnl - 19.79) < 1e-9
    assert abs(t.return_pct - (19.79 / (100.0 * 2.0) * 100.0)) < 1e-9
    # net pnl reconciles with equity change vs starting (no funding)
    assert abs(bk.equity() - (10_000.0 + 19.79)) < 1e-9


def test_stop_out_records_loss_trade_with_reason_stop():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002)
    bk.place_entry("SHORT", trigger=100.0, stop=102.0, size=1.0)
    bk.on_price(100.0, ts=1.0)      # short fill; entry fee 0.05
    bk.on_price(103.0, ts=2.0)      # >= stop -> stop fills at breaching price 103; exit fee 1*103*0.0005=0.0515
    assert bk.position() is None
    t = bk.trades()[0]
    # gross = 1*(100-103) = -3 ; fees 0.05+0.0515 ; net = -3.1015
    assert t.reason == "stop" and t.side == "SHORT"
    assert abs(t.pnl - (-3.0 - 0.05 - 0.0515)) < 1e-9
    assert t.exit == 103.0


def test_no_trade_recorded_without_a_close():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=1.0)      # only an entry, still open
    assert bk.trades() == []
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/execution/broker.py`.**
  (a) Add the `Trade` dataclass after `Fill`:
```python
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
    reason: str          # "stop" | "close"
```
  (b) Constructor: add `on_trade=None` as the last param; store `self._on_trade = on_trade`, and add `self._trades: list[Trade] = []`, `self._entry_fee = 0.0`, `self._entry_ts = 0.0`.
  (c) In `_open`, after `self._equity -= fee` and setting the position, record the entry cost+time:
```python
        self._entry_fee = fee
        self._entry_ts = ts
```
  (d) Replace `_close_at` so it records a Trade (equity math unchanged):
```python
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
                      return_pct=(net / notional * 100.0) if notional else 0.0, reason=kind)
        self._trades.append(trade)
        if self._on_trade is not None:
            self._on_trade(trade)
        self._position = None
        self._funding_anchor_ts = None
```
  (e) Add a query:
```python
    def trades(self) -> list[Trade]:
        return list(self._trades)
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite — the EXISTING `tests/unit/test_paper_broker.py` must STILL pass (equity math unchanged; Trade recording is additive). **Step 6:** Commit `feat(execution): record round-trip Trades (net P&L) + on_trade callback`.

---

## Task 2: TradeLog (JSONL persistence + summarize)

**Files:** Create `src/pavilos/execution/trade_log.py`; Test `tests/unit/test_trade_log.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_trade_log.py`:**
```python
# tests/unit/test_trade_log.py
from pavilos.execution.broker import Trade
from pavilos.execution.trade_log import TradeLog, summarize


def _t(pnl, reason="close"):
    return Trade(side="LONG", size=1.0, entry=100.0, exit=100.0 + pnl, entry_ts=1.0, exit_ts=2.0,
                 pnl=pnl, fee=0.1, return_pct=pnl, reason=reason)


def test_append_then_load_roundtrip(tmp_path):
    p = tmp_path / "trades.jsonl"
    log = TradeLog(str(p))
    assert log.load() == []                 # missing file -> empty
    log.append(_t(5.0)); log.append(_t(-2.0))
    loaded = log.load()
    assert len(loaded) == 2 and isinstance(loaded[0], Trade)
    assert loaded[0].pnl == 5.0 and loaded[1].pnl == -2.0


def test_load_skips_corrupt_lines(tmp_path):
    p = tmp_path / "trades.jsonl"
    p.write_text('{"bad json\n' + '{"side":"LONG","size":1.0,"entry":100.0,"exit":105.0,"entry_ts":1.0,'
                 '"exit_ts":2.0,"pnl":5.0,"fee":0.1,"return_pct":5.0,"reason":"close"}\n', encoding="utf-8")
    loaded = TradeLog(str(p)).load()
    assert len(loaded) == 1 and loaded[0].pnl == 5.0


def test_summarize_computes_pnl_winrate_return():
    s = summarize([_t(10.0), _t(-4.0), _t(6.0)], base_equity=1000.0)
    assert s["n_trades"] == 3 and s["wins"] == 2 and s["losses"] == 1
    assert abs(s["realized_pnl"] - 12.0) < 1e-9
    assert abs(s["win_rate"] - (2 / 3 * 100.0)) < 1e-9
    assert abs(s["return_pct"] - (12.0 / 1000.0 * 100.0)) < 1e-9


def test_summarize_empty():
    s = summarize([], base_equity=1000.0)
    assert s["n_trades"] == 0 and s["realized_pnl"] == 0.0 and s["win_rate"] == 0.0
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/execution/trade_log.py`:**
```python
# src/pavilos/execution/trade_log.py
"""Durable JSONL trade log (one closed Trade per line) + summary stats. File I/O
is isolated here so the PaperBroker stays pure/unit-testable."""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path

from pavilos.execution.broker import Trade


class TradeLog:
    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def append(self, trade: Trade) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(trade)) + "\n")

    def load(self) -> list[Trade]:
        if not self._path.exists():
            return []
        out: list[Trade] = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(Trade(**json.loads(line)))
            except Exception:
                continue  # skip corrupt / partially-written lines
        return out


def summarize(trades, *, base_equity: float) -> dict:
    n = len(trades)
    realized = sum(t.pnl for t in trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = sum(1 for t in trades if t.pnl < 0)
    return {
        "n_trades": n,
        "realized_pnl": realized,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / n * 100.0) if n else 0.0,
        "return_pct": (realized / base_equity * 100.0) if base_equity else 0.0,
        "gross_win": sum(t.pnl for t in trades if t.pnl > 0),
        "gross_loss": sum(t.pnl for t in trades if t.pnl < 0),
    }
```

- [ ] **Step 4:** run → 4 passed. **Step 5:** full suite. **Step 6:** Commit `feat(execution): add TradeLog (JSONL persistence) + summarize`.

---

## Task 3: DashboardState surfaces trades + summary

**Files:** Modify `src/pavilos/web/state.py`; Test `tests/unit/test_dashboard_state.py` (extend).

- [ ] **Step 1: Add a failing test to `tests/unit/test_dashboard_state.py`:**
```python
def test_update_includes_trades_and_summary():
    from pavilos.execution.broker import Trade
    from pavilos.execution.trade_log import summarize
    st = DashboardState()
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    trades = [Trade("LONG", 1.0, 100.0, 105.0, 1.0, 2.0, 5.0, 0.0, 5.0, "close")]
    st.update(_analysis(), bk, [], engine_state="IDLE", now=10.0,
              trades=trades, summary=summarize(trades, base_equity=10_000.0))
    snap = st.snapshot()
    assert snap["trades"][0]["pnl"] == 5.0 and snap["trades"][0]["reason"] == "close"
    assert snap["summary"]["n_trades"] == 1 and snap["summary"]["wins"] == 1


def test_initial_snapshot_has_empty_trades_and_summary():
    snap = DashboardState().snapshot()
    assert snap["trades"] == [] and snap["summary"] == {}
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/web/state.py`:**
  (a) Add `"trades": [], "summary": {}` to the `_EMPTY` dict.
  (b) Add a serializer near `_zone`:
```python
def _trade(t) -> dict:
    return {"side": t.side, "size": t.size, "entry": t.entry, "exit": t.exit,
            "entry_ts": t.entry_ts, "exit_ts": t.exit_ts, "pnl": t.pnl,
            "fee": t.fee, "return_pct": t.return_pct, "reason": t.reason}
```
  (c) Change `update(...)` signature to add `trades=(), summary=None` keyword params, and add to the built `snap`:
```python
            "trades": [_trade(t) for t in trades],
            "summary": dict(summary) if summary else {},
```
  (Keep everything else identical; `trades`/`summary` default to empty so existing callers/tests are unaffected.)

- [ ] **Step 4:** run → both new pass. **Step 5:** full suite (existing dashboard_state tests still pass). **Step 6:** Commit `feat(web): surface trades + P&L summary in DashboardState`.

---

## Task 4: Runtime wires the TradeLog

**Files:** Modify `src/pavilos/core/runtime.py`; Test `tests/unit/test_runtime.py` (extend).

- [ ] **Step 1: Add a failing test to `tests/unit/test_runtime.py`:**
```python
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
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/core/runtime.py`:**
  (a) Add `trade_log_path: str = "paper_trades.jsonl"` to `RuntimeConfig`.
  (b) Import `from pavilos.execution.trade_log import TradeLog, summarize`.
  (c) In `Runtime.__init__`, accept + store `all_trades` and `trade_log`: change signature to `def __init__(self, engine, trading_engine, state, config, all_trades, trade_log)` and store them.
  (d) In `Runtime.build`, before creating the broker:
```python
        trade_log = TradeLog(config.trade_log_path)
        all_trades = trade_log.load()

        def _on_trade(t) -> None:
            trade_log.append(t)
            all_trades.append(t)

        broker = PaperBroker(starting_equity=config.starting_equity, on_trade=_on_trade)
```
  (e) Update the observer to publish trades + summary:
```python
        def observer(snapshot, analysis, brk) -> None:
            state.update(analysis, brk, engine.health(), engine_state=signal.state,
                         now=now(), staleness_s=config.staleness_s,
                         trades=all_trades[-50:],
                         summary=summarize(all_trades, base_equity=config.starting_equity))
```
  (f) Return `cls(engine, trading_engine, state, config, all_trades, trade_log)`.
  (NOTE: `now` is the wall-clock callable added in the M4 close-out — keep using it.)

- [ ] **Step 4:** run → pass. **Step 5:** full suite (existing runtime tests still pass — the 2-arg `__init__` change must be reflected; the existing `test_build_*` uses `Runtime.build`, not the ctor, so it's fine). **Step 6:** Commit `feat(core): wire TradeLog into Runtime (load history, persist + publish trades/summary)`.

---

## Task 5: Dashboard — Trades/History panel + all-time P&L summary

**Files:** Modify `src/pavilos/web/static/index.html`; Test `tests/unit/test_web_server.py` (extend).

- [ ] **Step 1: Add a server test asserting the page references the new data** to `tests/unit/test_web_server.py`:
```python
def test_dashboard_page_references_trades_and_summary():
    from fastapi.testclient import TestClient
    from pavilos.web.state import DashboardState
    from pavilos.web.server import create_app
    html = TestClient(create_app(DashboardState())).get("/").text
    assert "trades" in html and "summary" in html  # the JS consumes snap.trades + snap.summary
```

- [ ] **Step 2:** run → likely FAIL (until the HTML reads those fields).

- [ ] **Step 3: Modify `src/pavilos/web/static/index.html`** (use frontend-design sensibilities; keep the existing dark aesthetic). Add:
  - A **P&L summary** block (in or beside the position panel): all-time **Realized P&L** (€, green/red), **Return %**, **# Trades**, **Win-rate %**, **W / L**, read from `snap.summary` (`realized_pnl`, `return_pct`, `n_trades`, `win_rate`, `wins`, `losses`). Handle empty `{}` gracefully (show dashes).
  - A **Trades / History** table (newest first) from `snap.trades`: time (exit_ts), side (LONG green / SHORT red), entry→exit, size, **P&L €** (colored), **P&L %**, reason (stop/close). Cap to the rows provided (~50). Thousands separators on prices; 2-dp on money.
  - The JS `render()` must read `snap.trades` (array) and `snap.summary` (object) and never throw on the empty/initial state.
  Keep `PAVILOS` + `PAPER` + `fetch("/api/state")` intact.

- [ ] **Step 4:** run `python -m pytest tests/unit/test_web_server.py -v` → pass. **Step 5:** full suite. **Step 6:** Commit `feat(web): add Trades/History panel + all-time P&L summary to the dashboard`.

---

## Task 6: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (171 prior + ~12 new). Existing `test_paper_broker.py`, `test_dashboard_state.py`, `test_runtime.py`, `test_web_server.py` all still green.
- [ ] **Step 2:** Add `paper_trades.jsonl` to `.gitignore` (the live log is runtime data, not source).
- [ ] **Step 3:** `git status` clean (except the ignored log); `git tag m5a-trades-pnl`.
- [ ] **Step 4 (operator smoke, optional):** restart `python -m pavilos`, let a trade close, confirm the Trades panel + P&L summary populate and survive a restart.

---

## Self-Review (plan author)
**Coverage:** round-trip Trade with net P&L + reason (T1); durable JSONL + summary (T2); dashboard data (T3); load-on-startup + persist + publish (T4); UI table + summary (T5); gitignore the log (T6). Matches "historial y paper con beneficios", persistent.
**Equity model:** session equity stays `starting_equity` per run; all-time realized P&L is from the log (no re-seed, no double count). Trade.pnl is net-of-fees, funding-excluded (documented; funding default 0).
**Type consistency:** `Trade(side,size,entry,exit,entry_ts,exit_ts,pnl,fee,return_pct,reason)`; `PaperBroker(..., on_trade=None)` + `trades()`; `TradeLog(path).append/load`; `summarize(trades, *, base_equity)`; `DashboardState.update(..., trades=(), summary=None)`; `Runtime.__init__(engine, trading_engine, state, config, all_trades, trade_log)` + `RuntimeConfig.trade_log_path`. Existing broker equity/fill math unchanged → existing tests stay green.
**Adversarial focus (3rd barrier):** existing broker tests still pass (equity unchanged); net-pnl reconciles with equity delta when funding=0; corrupt JSONL line skipped; load of a missing file → []; on_trade exception must not corrupt broker state (consider guarding in Runtime's `_on_trade`); summary on empty trades; return_pct with notional 0; the dashboard JS must not throw on empty trades/summary; concurrent append + load (single writer = the loop, fine); the all_trades list grows unbounded over a long run (dashboard caps to 50, summary is O(n) per tick — acceptable for paper, note for later).
