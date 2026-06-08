# PAVILOS M7: Breadth via ccxt (6 more venues) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Lift BTC-spot order-book coverage from ~60-65% to **~95%+** of trusted CEX volume by adding **Gate, MEXC, Crypto.com, Bitget, KuCoin, HTX** through a single `ccxt.pro` adapter — keeping the 6 native connectors for the high-integrity core (incl. Kraken, the execution venue). All new venues are Tier-A (USDT, pegged ~1.0).

**Architecture:** A `CcxtConnector` wraps `ccxt.pro.<id>().watch_order_book(symbol)` — which returns the venue's full maintained book each WS update — and emits it as a snapshot `BookUpdate` into the Engine's update queue, exactly like the native connectors. ccxt owns the sequencing/resync internally (acceptable for *detection* breadth, not execution). The transport (`make_exchange`) is injectable so the run-loop is unit-tested with a fake exchange, zero network. The venue registry gains the 6 ids; the Aggregator treats them as Tier-A USD-comparable.

**Tech Stack:** Python 3.13, `ccxt>=4.4` (new dep; `ccxt.pro` namespace is free since 2022), `pytest`. Builds on merged M1–M6.

---

## Scope decisions
1. **ccxt for the long tail, native for the core.** The 6 native connectors (Binance/Coinbase/OKX/Bybit/Kraken/Bitstamp) stay — they give full depth + hand-rolled integrity, and Kraken is the execution venue. The 6 ccxt venues are detection-only breadth where per-venue native engineering isn't justified.
2. **Each ccxt `watch_order_book` result is emitted as a full snapshot** (`is_snapshot=True`) — ccxt returns the current full book each update, and `BookState` replaces the venue book on snapshot. No diff/sequence handling on our side (ccxt does it).
3. **All 6 use BTC/USDT, Tier-A** (USDT≈USD via the 1.0 peg; the live FX/peg updater is still deferred). Crypto.com's USD pair is ~equivalent; USDT keeps one uniform symbol and the deepest book.
4. **Graceful partial coverage.** If a venue is geo-blocked/flaky from the host, its connector logs errors + reconnects (bounded); the Aggregator just runs with `venues_active < venues_total`. The operator live-smoke reveals which connect.
5. **Lower depth-quality venues** (MEXC/HTX/KuCoin have high asset-velocity) are included for volume coverage; their book is consensus context, not gospel — the detector already weights by venue count + persistence.

**Deferred:** Bitfinex/Gemini (high quality but low volume — optional later), KRW venues (Upbit/Bithumb — Tier-B/FX, excluded from the USD map), per-venue ccxt depth tuning, ccxt rate-limit/load_markets edge hardening beyond the reconnect loop.

---

## File Structure
```
PAVILOS/
├── pyproject.toml                          # + ccxt>=4.4 [MODIFY]
├── src/pavilos/connectors/
│   ├── ccxt_connector.py                    # CcxtConnector [NEW]
│   └── venues.py                            # VENUE_SPECS +6, build_connector +ccxt branch [MODIFY]
├── src/pavilos/core/runtime.py              # _SYMBOLS +6 [MODIFY]
├── scripts/live_smoke.py                    # symbols +6 [MODIFY]
└── tests/unit/
    ├── test_ccxt_connector.py
    ├── test_venues.py                       # expect 12 venues [MODIFY]
    └── test_deps_importable.py              # + ccxt [MODIFY]
```

---

## Task 1: ccxt dependency

**Files:** Modify `pyproject.toml`, `tests/unit/test_deps_importable.py`.

- [ ] **Step 1:** In `pyproject.toml` `[project] dependencies`, add `"ccxt>=4.4"`. (Package is already pip-installed in the env — do NOT pip install.)
- [ ] **Step 2:** Append to `tests/unit/test_deps_importable.py`:
```python
def test_ccxt_importable():
    import ccxt, ccxt.pro  # noqa: F401
    assert ccxt.pro.gate().has.get("watchOrderBook") is True
```
- [ ] **Step 3:** `python -m pytest tests/unit/test_deps_importable.py -v` → PASS.
- [ ] **Step 4:** Commit `chore(deps): add ccxt for long-tail venue breadth`.

---

## Task 2: CcxtConnector

**Files:** Create `src/pavilos/connectors/ccxt_connector.py`; Test `tests/unit/test_ccxt_connector.py`.

- [ ] **Step 1: Failing test — create `tests/unit/test_ccxt_connector.py` with EXACTLY:**
```python
# tests/unit/test_ccxt_connector.py
import asyncio

from pavilos.connectors.ccxt_connector import CcxtConnector


class _FakeExchange:
    def __init__(self, books):
        self._books = list(books)
        self.closed = False

    async def watch_order_book(self, symbol):
        await asyncio.sleep(0)
        if self._books:
            return self._books.pop(0)
        await asyncio.Event().wait()

    async def close(self):
        self.closed = True


def test_emits_snapshots_from_ccxt_books():
    books = [{"bids": [[100.0, 1.0]], "asks": [[101.0, 2.0]], "nonce": 1},
             {"bids": [[100.0, 1.5]], "asks": [[101.0, 2.0]], "nonce": 2}]
    fake = _FakeExchange(books)

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = CcxtConnector("gate", "BTC/USDT", make_exchange=lambda: fake,
                             now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        task.cancel()  # blocked on the 3rd watch -> cancel to unwind
        await asyncio.gather(task, return_exceptions=True)
        return u1, u2, conn.health(), fake.closed

    u1, u2, health, closed = asyncio.run(scenario())
    assert u1.exchange == "gate" and u1.is_snapshot is True and u1.ts == 5.0
    assert u1.bids == ((100.0, 1.0),) and u1.asks == ((101.0, 2.0),)
    assert u2.bids == ((100.0, 1.5),) and u2.seq == 2
    assert health.exchange == "gate" and health.connected is False
    assert closed is True   # exchange closed in finally


def test_reconnects_and_counts_errors_on_watch_failure():
    calls = {"n": 0}

    class _Flaky:
        async def watch_order_book(self, symbol):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("ws down")
            await asyncio.sleep(0)
            return {"bids": [[100.0, 1.0]], "asks": [[101.0, 2.0]], "nonce": 9}
        async def close(self):
            pass

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = CcxtConnector("mexc", "BTC/USDT", make_exchange=lambda: _Flaky(),
                             now=lambda: 1.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)  # recovers on the 2nd exchange
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return u, conn.health()

    u, h = asyncio.run(scenario())
    assert u.is_snapshot is True and h.errors >= 1
```

- [ ] **Step 2:** Run `python -m pytest tests/unit/test_ccxt_connector.py -v` — expect FAIL.

- [ ] **Step 3: Implement — `src/pavilos/connectors/ccxt_connector.py`:**
```python
# src/pavilos/connectors/ccxt_connector.py
"""Long-tail venue connector via ccxt.pro. watch_order_book returns the venue's
full maintained book each WS update -> emitted as a snapshot BookUpdate, exactly
like the native connectors. ccxt owns the sequencing/resync internally. The
exchange factory is injected so the run-loop is unit-testable without network."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth

_log = logging.getLogger(__name__)


class CcxtConnector:
    """Streams a ccxt.pro exchange's order book into ``BookUpdate`` snapshots.
    ``make_exchange`` builds an object exposing ``async watch_order_book(symbol)``
    (-> ``{"bids": [[p, a], ...], "asks": [...], "nonce": int|None}``) and
    ``async close()``. Any error reconnects (a fresh exchange) with stop-aware
    backoff. ``exchange`` is the ccxt id, used as the venue name."""

    def __init__(self, exchange_id: str, symbol: str, *,
                 make_exchange: Callable[[], object] | None = None,
                 now: Callable[[], float] | None = None,
                 sleep: Callable[[float], Awaitable[None]] | None = None,
                 max_backoff: float = 30.0) -> None:
        self.exchange = exchange_id
        self.symbol = symbol
        self._make_exchange = make_exchange or self._default_make_exchange
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts,
                               self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            ex = None
            try:
                ex = self._make_exchange()
                self._connected = True
                while not stop.is_set():
                    ob = await ex.watch_order_book(self.symbol)
                    if stop.is_set():
                        break
                    out = BookUpdate(
                        exchange=self.exchange, ts=self._now(),
                        bids=tuple((float(p), float(a)) for p, a in ob.get("bids", [])),
                        asks=tuple((float(p), float(a)) for p, a in ob.get("asks", [])),
                        is_snapshot=True, seq=ob.get("nonce"))
                    await out_q.put(out)
                    self._last_update_ts = out.ts
                    backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                self._errors += 1
                _log.exception("ccxt connector %s error; will reconnect", self.exchange)
            finally:
                self._connected = False
                await _close_exchange(ex)
            if stop.is_set():
                break
            delay = min(backoff, self._max_backoff)
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
            else:
                await self._sleep(0)
            backoff = min(backoff * 2, self._max_backoff) if self._max_backoff else 0.0

    def _default_make_exchange(self) -> object:
        import ccxt.pro  # imported lazily so unit tests never need ccxt
        return getattr(ccxt.pro, self.exchange)({"enableRateLimit": True})


async def _close_exchange(ex: object) -> None:
    close = getattr(ex, "close", None)
    if close is not None:
        try:
            await close()
        except Exception:
            pass


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4:** Run `python -m pytest tests/unit/test_ccxt_connector.py -v` — expect 2 passed.
- [ ] **Step 5:** full suite. **Step 6:** Commit `feat(connectors): add CcxtConnector (ccxt.pro watch_order_book -> snapshot BookUpdates)`.

---

## Task 3: Register the 6 ccxt venues

**Files:** Modify `src/pavilos/connectors/venues.py`, `src/pavilos/core/runtime.py`, `scripts/live_smoke.py`; Test `tests/unit/test_venues.py`.

- [ ] **Step 1: Update the failing test — in `tests/unit/test_venues.py`** change `test_venue_specs_cover_six_tier_a` to expect all 12 (rename ok):
```python
def test_venue_specs_cover_all_tier_a():
    names = {s.exchange for s in VENUE_SPECS}
    assert names == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp",
                     "gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"}
    assert all(s.tier is Tier.A for s in VENUE_SPECS)
    quotes = {s.exchange: s.quote for s in VENUE_SPECS}
    assert quotes["coinbase"] is Quote.USD and quotes["okx"] is Quote.USDT
    assert quotes["gate"] is Quote.USDT and quotes["htx"] is Quote.USDT
```
  And extend `test_build_connector_returns_runnable_for_each_venue`'s `symbols` dict with the 6 new ids → `"BTC/USDT"` each, asserting each `conn.exchange == venue` and has `run`/`health` (the ccxt connector builds with the default lazy exchange factory; constructing it does NOT touch the network — only `run()` would).

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/connectors/venues.py`:**
  (a) Import: `from pavilos.connectors.ccxt_connector import CcxtConnector`.
  (b) Add the 6 specs to `VENUE_SPECS` (after bitstamp):
```python
    VenueSpec("gate", Quote.USDT, Tier.A),
    VenueSpec("mexc", Quote.USDT, Tier.A),
    VenueSpec("cryptocom", Quote.USDT, Tier.A),
    VenueSpec("bitget", Quote.USDT, Tier.A),
    VenueSpec("kucoin", Quote.USDT, Tier.A),
    VenueSpec("htx", Quote.USDT, Tier.A),
```
  (c) In `build_connector`, before the final `raise KeyError`, add:
```python
    if venue in ("gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"):
        return CcxtConnector(venue, symbol)
```

- [ ] **Step 4: Modify `src/pavilos/core/runtime.py`** — extend `_SYMBOLS` with the 6:
```python
            "gate": "BTC/USDT", "mexc": "BTC/USDT", "cryptocom": "BTC/USDT",
            "bitget": "BTC/USDT", "kucoin": "BTC/USDT", "htx": "BTC/USDT",
```

- [ ] **Step 5: Modify `scripts/live_smoke.py`** — extend its `symbols` dict with the same 6 `"BTC/USDT"` entries (so the smoke covers all 12).

- [ ] **Step 6:** Run `python -m pytest tests/unit/test_venues.py -v` → pass. **Step 7:** full suite (`import scripts.live_smoke` still clean, no network). **Step 8:** Commit `feat(connectors): register 6 ccxt venues (gate/mexc/cryptocom/bitget/kucoin/htx) -> 12 venues`.

---

## Task 4: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (202 prior + ~4 new). Existing suites green.
- [ ] **Step 2:** `python -c "import scripts.live_smoke, scripts.record_book; print('import OK')"` (no network at import).
- [ ] **Step 3:** `git status` clean; `git tag m7-breadth-ccxt`.
- [ ] **Step 4 (operator, network):** `python -m scripts.live_smoke 30` — confirm which of the 12 venues connect from the Spanish residential IP (the 6 ccxt ones are the unknowns; some may need `load_markets`/geo). Note any with errors>0; they degrade gracefully (Aggregator runs on `venues_active`).

---

## Self-Review (plan author)
**Coverage:** ccxt dep (T1) → CcxtConnector with injectable transport + reconnect (T2) → 6 venues in the registry + runtime + live-smoke (T3) → suite + tag + operator smoke (T4). Lifts detection from ~60% to ~95% per the research.
**Determinism/test:** the ccxt connector is unit-tested with a fake exchange (no network); `_default_make_exchange` lazily imports ccxt.pro so tests don't need it loaded. Mirrors the native connectors' injectable pattern + stop-aware backoff + CancelledError re-raise + best-effort close.
**Type consistency:** `CcxtConnector(exchange_id, symbol, *, make_exchange, now, sleep, max_backoff)` exposes `exchange`/`async run(out_q, stop)`/`health() -> ConnectorHealth` — the exact surface the `Engine` consumes (verified vs `core/engine.py`). `BookUpdate(exchange, ts, bids, asks, is_snapshot, seq)` matches the model. `VENUE_SPECS`/`build_connector` extended consistently; `_SYMBOLS` + live_smoke symbols carry the 6 ids.
**Graceful degradation:** a geo-blocked/flaky ccxt venue increments errors + reconnects (bounded by max_backoff + Engine cancel); the Aggregator already handles `venues_active < venues_total`, so partial coverage never breaks the book.
**Adversarial focus (3rd barrier):** watch hanging on shutdown → bounded only by Engine grace-then-cancel (accepted project pattern; CancelledError is re-raised, not swallowed); a venue returning malformed/empty bids/asks → empty snapshot (BookState clears that venue, no crash); non-finite price/size from ccxt → BookState drops them at entry (M2 systemic guard); `close()` best-effort/idempotent on every path incl. `ex is None`; reconnect-storm bounded by backoff; constructing 6 CcxtConnectors does NOT touch the network (lazy import + factory); the 12-venue Aggregator window/peg still holds (USDT pegged 1.0).
