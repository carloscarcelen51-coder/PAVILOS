# PAVILOS M4: Live Wiring + Web Dashboard (no Telegram) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. The frontend task (Task 4) additionally uses **frontend-design** for a polished, non-generic dashboard. Steps use checkbox (`- [ ]`).

**Goal:** Make PAVILOS actually *run*: an entrypoint that ingests live from the 6 exchanges → detection → signals → paper broker, supervised with bounded shutdown, plus a **FastAPI web dashboard** to watch the combined book, detected supports/resistances, the live paper position, equity/P&L, and per-venue health. **No Telegram** (visibility is via the UI for now).

**Architecture:** A `DashboardState` holder (single writer = the trading loop, many readers = the web) decouples the async trading loop from the web layer. The existing `TradingEngine` gains an optional `observer` hook so each processed snapshot publishes `(snapshot, analysis, broker)` to the state. A `Runtime` assembles the whole object graph (connectors via `venues.build_connector`, `Aggregator`, `Engine`, `Detector`, `ATR`, `SignalEngine`, `PaperBroker`, `TradingEngine`) and runs the `Engine`, a **supervised** `TradingEngine.run`, and the `uvicorn` server concurrently. The web layer (`FastAPI`) serves `/api/state` (JSON) + `/` (a single polished static dashboard polling it). Everything except the live sockets + uvicorn is unit-tested with injected fakes.

**Tech Stack:** Python 3.13, `FastAPI` + `uvicorn[standard]` (new runtime deps), `httpx` (new dev dep, for `fastapi.testclient.TestClient`), stdlib `asyncio`/`logging`. Builds on merged M1–M3.

---

## Scope decisions (READ FIRST)

1. **Paper only.** The dashboard shows the paper `PaperBroker` state; no real orders. Big "PAPER" badge in the UI.
2. **Polling, not WebSocket (v1).** The dashboard polls `GET /api/state` every ~1s. A WebSocket push is a deferred refinement (simpler + robust first).
3. **No Telegram** (deferred). Visibility = the web dashboard.
4. **Live connect is smoke-only.** `Runtime.build(...)` is unit-tested with an injected connector factory (fakes, no network); the real run against the 6 venues is validated manually (operator `python -m pavilos`), like the M1 live-smoke.
5. **Crash-loud TradingEngine is supervised.** Per M3's documented policy, a per-tick exception propagates; the M4 `Runtime` wraps `TradingEngine.run` in a bounded-restart supervisor that logs and restarts (so the strategy recovers instead of dying silently), with graceful bounded shutdown via the shared `stop` event + `Engine.stop()`.
6. **Config via a frozen dataclass** with sensible defaults (symbols per venue, detector/signal/broker params, starting equity, host/port). Tunable params still need real-data calibration (deferred), defaults are reasonable.

**Deferred (not gaps):** Telegram; WebSocket push; persistence of the equity curve/trade log to disk/DB; auth on the dashboard; real-order broker; threshold calibration; native-distro migration (separate step, at the end).

---

## Dashboard layout (approved design target)

Single dark page, ~3 columns, auto-refresh 1s. ASCII target:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ PAVILOS  ● live   BTC mid: $104,231.5   [PAPER]        venues 6/6 ok       │
├───────────────────────────┬───────────────────────────┬────────────────────┤
│ RESISTANCES (sell walls)  │ POSITION                  │ VENUE HEALTH       │
│  105,800  ████████ 0.81   │  SHORT 0.42 BTC           │ kraken   ● 0/0     │
│  105,200  ████ 0.44       │  entry 104,900  stop 105,4│ binance  ● 0/0     │
│ ── mid 104,231 ──────────  │  uPnL +€182.40            │ coinbase ● 0/0     │
│ SUPPORTS (buy walls)      │  equity €10,182.40        │ okx      ● 1/0     │
│  103,900  ██████ 0.67     │  state IN_POSITION        │ bybit    ● 0/0     │
│  103,100  ███ 0.39        │  pending —                │ bitstamp ● 0/0     │
│  (strength·venues·persist) │                           │ resyncs/errors     │
├───────────────────────────┴───────────────────────────┴────────────────────┤
│ RECENT FILLS:  12:03:11 entry SHORT 0.42 @104,900 · 11:58:02 close +€57 ... │
└──────────────────────────────────────────────────────────────────────────┘
```
Supports green, resistances red, confidence as a bar (0..1). Zones sorted by confidence desc; show top N. Position panel switches on `state` (IDLE / PENDING_ENTRY / IN_POSITION). Stale (>staleness_s since last update) → amber "stale" indicator.

---

## File Structure

```
PAVILOS/
├── pyproject.toml                         # + fastapi, uvicorn[standard]; dev: httpx [MODIFY]
├── src/pavilos/
│   ├── web/
│   │   ├── __init__.py                     # [NEW]
│   │   ├── state.py                        # DashboardState [NEW]
│   │   ├── server.py                       # create_app(state) -> FastAPI [NEW]
│   │   └── static/
│   │       └── index.html                  # the dashboard (HTML+CSS+vanilla JS) [NEW]
│   ├── core/
│   │   ├── trading_engine.py               # + optional observer hook [MODIFY]
│   │   └── runtime.py                       # Runtime.build(...) + run_app(...) [NEW]
│   └── __main__.py                          # `python -m pavilos` entrypoint [NEW]
└── tests/unit/
    ├── test_dashboard_state.py
    ├── test_trading_engine_observer.py
    ├── test_web_server.py
    └── test_runtime.py
```

---

## Task 1: pyproject deps

**Files:** Modify `pyproject.toml`; Test `tests/unit/test_deps_importable.py` (exists — extend).

- [ ] **Step 1:** Read `pyproject.toml`. In `[project] dependencies` add `"fastapi>=0.110"` and `"uvicorn[standard]>=0.29"`. In the dev/test extra (where `pytest` lives) add `"httpx>=0.27"`.
- [ ] **Step 2:** Install into the active environment: `python -m pip install "fastapi>=0.110" "uvicorn[standard]>=0.29" "httpx>=0.27"`.
- [ ] **Step 3:** Append to `tests/unit/test_deps_importable.py`:
```python
def test_web_deps_importable():
    import fastapi, uvicorn, httpx  # noqa: F401
```
- [ ] **Step 4:** `python -m pytest tests/unit/test_deps_importable.py -v` → PASS.
- [ ] **Step 5:** Commit `chore(deps): add fastapi + uvicorn + httpx for the web dashboard`.

---

## Task 2: DashboardState

**Files:** Create `src/pavilos/web/__init__.py` (empty), `src/pavilos/web/state.py`; Test `tests/unit/test_dashboard_state.py`.

> Single-writer (trading loop) / many-reader (web) holder. Stores the latest
> fully-serialized snapshot dict; `snapshot()` returns it (atomic reference read).
> Pure transformation of domain objects → JSON-able dict.

- [ ] **Step 1: Create `src/pavilos/web/__init__.py` empty.**

- [ ] **Step 2: Failing test — `tests/unit/test_dashboard_state.py`:**
```python
# tests/unit/test_dashboard_state.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.connectors.base import ConnectorHealth
from pavilos.execution.broker import PaperBroker
from pavilos.web.state import DashboardState


def _zone(side, price, conf):
    return Zone(side=side, price=price, low=price - 1, high=price + 1, strength=12.0,
                venues=("kraken", "binance"), persistence_s=8.0, pulled=False, confidence=conf)


def _analysis():
    return DepthAnalysis(ts=10.0, mid=100.0,
                         supports=(_zone(Side.SUPPORT, 99.0, 0.7),),
                         resistances=(_zone(Side.RESISTANCE, 101.0, 0.5),))


def test_initial_snapshot_is_empty_but_shaped():
    s = DashboardState().snapshot()
    assert s["mid"] is None and s["supports"] == [] and s["position"] is None
    assert s["venues"] == [] and s["state"] == "IDLE"


def test_update_serializes_domain_objects():
    st = DashboardState()
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=10.0)  # fill -> position open
    health = [ConnectorHealth("kraken", True, 10.0, 0, 0)]
    st.update(_analysis(), bk, health, engine_state="IN_POSITION", now=10.0)
    snap = st.snapshot()
    assert snap["mid"] == 100.0 and snap["state"] == "IN_POSITION"
    assert snap["supports"][0]["price"] == 99.0 and snap["supports"][0]["confidence"] == 0.7
    assert snap["resistances"][0]["side"] == "resistance"
    assert snap["position"]["side"] == "LONG" and snap["position"]["size"] == 1.0
    assert snap["equity"] == 10_000.0  # unrealized 0 at mark 100
    assert snap["venues"][0]["exchange"] == "kraken" and snap["venues"][0]["connected"] is True
```

- [ ] **Step 3:** run → FAIL.

- [ ] **Step 4: Implement — `src/pavilos/web/state.py`:**
```python
# src/pavilos/web/state.py
"""Latest-state holder bridging the async trading loop (single writer) and the
web layer (many readers). update() serializes domain objects to a JSON-able dict;
snapshot() returns the latest by atomic reference read (no lock needed: CPython
attribute assignment is atomic, and readers always get a complete prior dict)."""
from __future__ import annotations

from pavilos.detection.models import DepthAnalysis
from pavilos.execution.broker import PaperBroker

_EMPTY: dict = {
    "ts": None, "mid": None, "state": "IDLE", "supports": [], "resistances": [],
    "position": None, "pending": None, "equity": None, "realized_equity": None,
    "fills": [], "venues": [], "stale": False,
}


def _zone(z) -> dict:
    return {"side": z.side.value, "price": z.price, "low": z.low, "high": z.high,
            "strength": z.strength, "venues": list(z.venues),
            "persistence_s": z.persistence_s, "confidence": z.confidence}


class DashboardState:
    def __init__(self) -> None:
        self._snap: dict = dict(_EMPTY)

    def snapshot(self) -> dict:
        return self._snap

    def update(self, analysis: DepthAnalysis, broker: PaperBroker, health,
               *, engine_state: str, now: float, staleness_s: float = 15.0) -> None:
        pos = broker.position()
        pend = broker.pending_entry()
        fills = broker.fills()[-12:]
        snap = {
            "ts": analysis.ts,
            "mid": analysis.mid,
            "state": engine_state,
            "supports": [_zone(z) for z in analysis.supports],
            "resistances": [_zone(z) for z in analysis.resistances],
            "position": None if pos is None else {
                "side": pos.side, "size": pos.size, "entry": pos.entry, "stop": pos.stop},
            "pending": None if pend is None else {
                "side": pend["side"], "trigger": pend["trigger"], "stop": pend["stop"], "size": pend["size"]},
            "equity": broker.equity(analysis.mid),
            "realized_equity": broker.equity(analysis.mid) if pos is None else broker.equity(analysis.mid) - (
                pos.size * (analysis.mid - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - analysis.mid)),
            "fills": [{"ts": f.ts, "side": f.side, "price": f.price, "size": f.size,
                       "fee": f.fee, "kind": f.kind} for f in fills],
            "venues": [{"exchange": h.exchange, "connected": h.connected,
                        "last_update_ts": h.last_update_ts, "resyncs": h.resyncs, "errors": h.errors}
                       for h in health],
            "stale": (now - analysis.ts) > staleness_s,
        }
        self._snap = snap  # atomic swap
```

- [ ] **Step 5:** run → 2 passed. **Step 6:** full suite (no failures). **Step 7:** Commit `feat(web): add DashboardState (domain -> JSON-able latest-state holder)`.

---

## Task 3: TradingEngine observer hook

**Files:** Modify `src/pavilos/core/trading_engine.py`; Test `tests/unit/test_trading_engine_observer.py`.

> Add an optional `observer(snapshot, analysis, broker)` called at the end of
> `process()`. Default `None` → no behavior change (existing tests stay green).

- [ ] **Step 1: Failing test — `tests/unit/test_trading_engine_observer.py`:**
```python
# tests/unit/test_trading_engine_observer.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.execution.broker import PaperBroker
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.core.trading_engine import TradingEngine


def _snap(ts, mid):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=(), asks=(),
                                 venues_active=("k",), venues_total=1)


def _te(observer):
    d = Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                 match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                 persistence_target_s=1.0, venues_target=2.0, strength_target=5.0)
    s = SignalEngine(entry_threshold=0.3, trail_threshold=0.3, opposing_threshold=0.7,
                     min_persistence_s=0.0, min_venues=2, entry_offset_bps=2.0, stop_offset_bps=2.0,
                     atr_stop_mult=3.0, opposing_distance_bps=30.0, risk_pct=0.01, max_leverage=10.0)
    b = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    return TradingEngine(d, ATR(window=10), s, b, observer=observer)


def test_observer_called_with_analysis_and_broker():
    seen = []
    te = _te(lambda snap, analysis, broker: seen.append((analysis.mid, broker)))
    te.process(_snap(1.0, 100.0))
    assert len(seen) == 1 and seen[0][0] == 100.0


def test_observer_optional_default_none():
    te = _te(None)
    te.process(_snap(1.0, 100.0))  # must not raise
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/core/trading_engine.py`.** Change the constructor signature to add `observer=None` as the last param, store `self.observer = observer`, and at the END of `process()` add:
```python
        if self.observer is not None:
            self.observer(snapshot, analysis, self.broker)
```
The full updated constructor + process:
```python
    def __init__(self, detector: Detector, atr: ATR, signal: SignalEngine, broker,
                 observer=None) -> None:
        self.detector = detector
        self.atr = atr
        self.signal = signal
        self.broker = broker
        self.observer = observer

    def process(self, snapshot: CombinedDepthSnapshot) -> None:
        """One snapshot through the full pipeline (sync, deterministic)."""
        analysis = self.detector.update(snapshot)
        self.atr.update(snapshot.mid)
        self.signal.update(analysis, self.atr.value(), self.broker)
        if self.observer is not None:
            self.observer(snapshot, analysis, self.broker)
```
(Keep `run()` unchanged.)

- [ ] **Step 4:** run → 2 passed. **Step 5:** full suite — the existing `test_trading_engine.py` must STILL pass (observer defaults None). **Step 6:** Commit `feat(core): add optional observer hook to TradingEngine.process`.

---

## Task 4: FastAPI server + dashboard page

**Files:** Create `src/pavilos/web/server.py`, `src/pavilos/web/static/index.html`; Test `tests/unit/test_web_server.py`. **Use the frontend-design skill for `index.html`.**

- [ ] **Step 1: Failing test — `tests/unit/test_web_server.py`:**
```python
# tests/unit/test_web_server.py
from fastapi.testclient import TestClient

from pavilos.web.state import DashboardState
from pavilos.web.server import create_app


def test_api_state_returns_current_snapshot():
    state = DashboardState()
    client = TestClient(create_app(state))
    r = client.get("/api/state")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "IDLE" and body["supports"] == []


def test_root_serves_dashboard_html():
    client = TestClient(create_app(DashboardState()))
    r = client.get("/")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert "PAVILOS" in r.text


def test_api_state_reflects_updates():
    state = DashboardState()
    client = TestClient(create_app(state))
    # mutate the holder directly (simulates the trading loop writing)
    state._snap = {**state.snapshot(), "mid": 104231.5, "state": "IN_POSITION"}
    body = client.get("/api/state").json()
    assert body["mid"] == 104231.5 and body["state"] == "IN_POSITION"
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/web/server.py`:**
```python
# src/pavilos/web/server.py
"""FastAPI app serving the dashboard JSON + static page. Read-only over a
DashboardState. No business logic here."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from pavilos.web.state import DashboardState

_STATIC = Path(__file__).parent / "static"


def create_app(state: DashboardState) -> FastAPI:
    app = FastAPI(title="PAVILOS", docs_url=None, redoc_url=None)

    @app.get("/api/state")
    def api_state() -> JSONResponse:
        return JSONResponse(state.snapshot())

    @app.get("/api/health")
    def api_health() -> JSONResponse:
        snap = state.snapshot()
        return JSONResponse({"venues": snap["venues"], "stale": snap["stale"], "ts": snap["ts"]})

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse((_STATIC / "index.html").read_text(encoding="utf-8"))

    return app
```

- [ ] **Step 4: Create `src/pavilos/web/static/index.html`** — a single self-contained dark dashboard (HTML + CSS + vanilla JS, no build step) that polls `GET /api/state` every 1000ms and renders the layout in the "Dashboard layout" section above. **Invoke the frontend-design skill** to produce a polished, distinctive (non-generic) trading UI. Hard requirements the test enforces: the literal string `PAPER` and `PAVILOS` appear; it must `fetch("/api/state")`. It must render: header (mid, PAPER badge, venues N/M ok, stale indicator), resistances (red, confidence bars, sorted desc) above a mid line, supports (green) below, a position/pending/equity panel keyed on `state`, a venue-health list (connected dot + resyncs/errors), and a recent-fills strip. Handle the empty/IDLE initial state gracefully. Number-format prices with thousands separators.

- [ ] **Step 5:** run `python -m pytest tests/unit/test_web_server.py -v` → 3 passed. **Step 6:** full suite. **Step 7:** Commit `feat(web): add FastAPI server + dashboard page`.

---

## Task 5: Runtime assembly + entrypoint

**Files:** Create `src/pavilos/core/runtime.py`, `src/pavilos/__main__.py`; Test `tests/unit/test_runtime.py`.

> `Runtime.build(config, *, connector_factory=...)` assembles the object graph
> and wires the `DashboardState` observer; `run_app()` runs Engine + a supervised
> TradingEngine.run + uvicorn concurrently with bounded shutdown. The connector
> factory is injectable so the assembly is unit-testable with fakes (no network).

- [ ] **Step 1: Failing test — `tests/unit/test_runtime.py`:**
```python
# tests/unit/test_runtime.py
import asyncio

from pavilos.core.runtime import Runtime, RuntimeConfig


class _FakeConnector:
    def __init__(self, exchange):
        self.exchange = exchange
    async def run(self, out_q, stop):
        await stop.wait()
    def health(self):
        from pavilos.connectors.base import ConnectorHealth
        return ConnectorHealth(self.exchange, True, 0.0, 0, 0)


def test_build_wires_full_graph_with_injected_connectors():
    built = {}
    rt = Runtime.build(RuntimeConfig(), connector_factory=lambda v, sym: _FakeConnector(v))
    # all 6 venues wired; the trading engine has the dashboard observer
    assert len(rt.engine._connectors) == 6
    assert rt.trading_engine.observer is not None
    assert rt.state.snapshot()["state"] == "IDLE"


def test_supervisor_restarts_a_crashing_trading_loop():
    rt = Runtime.build(RuntimeConfig(), connector_factory=lambda v, sym: _FakeConnector(v))
    calls = {"n": 0}

    async def flaky_run(q, stop):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        await stop.wait()

    rt.trading_engine.run = flaky_run  # type: ignore[assignment]

    async def scenario():
        stop = asyncio.Event()
        task = asyncio.create_task(rt._supervise_trading(stop, restart_delay=0.0))
        for _ in range(100):
            if calls["n"] >= 2:
                break
            await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return calls["n"]

    assert asyncio.run(scenario()) >= 2  # crashed once, restarted, then idled on stop
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/core/runtime.py`:**
```python
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
```

- [ ] **Step 4: Create `src/pavilos/__main__.py`:**
```python
# src/pavilos/__main__.py
"""`python -m pavilos` — run the live paper-trading app + dashboard."""
from __future__ import annotations

import asyncio
import logging

from pavilos.core.runtime import Runtime, RuntimeConfig


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = RuntimeConfig()
    rt = Runtime.build(cfg)
    _log = logging.getLogger("pavilos")
    _log.info("PAVILOS paper dashboard on http://%s:%d (PAPER mode, 6 venues)", cfg.host, cfg.port)
    try:
        asyncio.run(rt.run_app())
    except KeyboardInterrupt:
        _log.info("shutting down")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5:** run `python -m pytest tests/unit/test_runtime.py -v` → 2 passed. **Step 6:** full suite. **Step 7:** Confirm `python -c "import pavilos.__main__, pavilos.core.runtime, pavilos.web.server"` imports cleanly (no network at import). **Step 8:** Commit `feat(core): add Runtime assembly + supervised run_app + python -m pavilos entrypoint`.

---

## Task 6: Close-out

- [ ] **Step 1:** `python -m pytest -v` → ALL pass (156 prior + ~9 new: deps 1, state 2, observer 2, server 3, runtime 2 — minus any consolidations; confirm the existing `test_trading_engine.py` still passes after the observer change).
- [ ] **Step 2:** Manual smoke (operator, optional, network): `python -m pavilos` then open `http://127.0.0.1:8800` — confirm the dashboard renders and venues connect. Document in the report; not a unit test.
- [ ] **Step 3:** `git status` clean; `git tag m4-dashboard-live`.

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.6 dashboard + live operation):**
- Live ingestion from 6 venues + full loop → Task 5 `Runtime.build`/`run_app` ✅. Bounded shutdown + supervised crash-loud loop → Task 5 `_supervise_trading`/`run_app` ✅.
- Web dashboard (combined book, supports/resistances, position, equity/PnL, health) → Tasks 2 (state), 4 (server + page) ✅. PAPER badge ✅. Stale indicator ✅.
- Decoupling loop ↔ web via single-writer state → Task 2 ✅; tick publish via observer → Task 3 ✅.
- **No Telegram** (per user) — deferred, documented.
- *Deferred:* WebSocket push, persistence, auth, real-order broker, calibration, native-distro migration.

**Placeholder scan:** Task 4's `index.html` is the only non-verbatim artifact — it is fully specified by the layout section + hard requirements + the JSON contract from Task 2, and built via frontend-design. Everything else is complete runnable code.

**Type consistency:** `DashboardState.update(analysis, broker, health, *, engine_state, now, staleness_s)` / `.snapshot() -> dict`; `create_app(state) -> FastAPI` with `/api/state`, `/api/health`, `/`; `TradingEngine.__init__(detector, atr, signal, broker, observer=None)` (matches M3's shipped reordered ctor `(detector, atr, signal, broker)` + new observer); `Runtime.build(config, *, connector_factory)` / `_supervise_trading(stop, *, restart_delay)` / `run_app(stop=None)`; `RuntimeConfig` frozen dataclass. The Detector/SignalEngine/Aggregator/Engine/PaperBroker constructor args all match the merged M1–M3 signatures (verified: `Engine(connectors, aggregator)`, `engine.snapshots`, `engine.health()`, `Aggregator(specs, peg, *, bin_bps, window_bps, staleness_s)`, `PegProvider()`).

**Adversarial focus (3rd barrier):** state read/write race (reader gets a torn dict? — no, atomic ref swap, but confirm update builds a NEW dict and never mutates the served one); equity/realized split math in state; observer exception propagating into the trading loop (should it? it runs inside process → would crash-loud; consider guarding the observer so a dashboard bug can't kill trading); `run_app` shutdown bounded when uvicorn or engine hangs; supervisor restart storm (no successful progress → tight loop? restart_delay guards it); `/api/state` returns valid JSON for the empty initial state; the dashboard page handles `position: null` / empty zones without JS errors; port already in use; `python -m pavilos` import has no network side-effects at import time.
