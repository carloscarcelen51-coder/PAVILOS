# PAVILOS M9: ccxt Process Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Run the 6 ccxt venues (gate/mexc/cryptocom/bitget/kucoin/htx) in a **separate process** with its own asyncio event loop, isolated from the main process's native feeds + detection + dashboard, so the main loop's periodic synchronous bursts (build_combined) can no longer starve the ccxt WS keepalives — getting all **12 venues stable** (currently a stable 9/12 single-process ceiling).

**Architecture:** A child process (`multiprocessing`, spawn) runs the ccxt `CcxtConnector`s against a local asyncio queue and forwards every `BookUpdate` to the parent over a `multiprocessing.Queue`; it also forwards per-venue `ConnectorHealth` over a second queue. In the parent, `CcxtPoolConnector` looks like ONE Engine connector: a dedicated daemon thread blocks on the book queue and hands updates to the asyncio update queue via `loop.call_soon_threadsafe`; an async task polls the health queue. `Engine.health()` is extended so a connector may report MANY healths (`healths()`), preserving per-venue dashboard rows. Shutdown is bounded: signal the child via an `mp.Event`, join with a grace period, `terminate()` if it overstays — no zombies. A `RuntimeConfig.isolate_ccxt` flag lets us fall back to the in-process pool.

**Tech Stack:** Python 3.13, `multiprocessing` (spawn ctx), `asyncio`, `threading`, `pytest`. Builds on merged M1–M8. Why a process, not a thread: the GIL serialises threads, so the main loop's synchronous build_combined burst would still block a thread's event loop from running ccxt pings on time — only a separate process (own GIL + own loop) isolates it.

---

## Scope decisions
1. **multiprocessing, not subprocess+socket.** `BookUpdate`/`ConnectorHealth` are plain picklable dataclasses, so `mp.Queue` carries them directly — no hand-rolled wire format. Spawn start method (Windows default + safest): the worker entry + its args must be picklable and the worker module must have NO import-time side effects.
2. **One pool connector for all 6 ccxt venues.** The Engine sees `native(6) + pool(1)`. The pool exposes `healths()` (plural) so the 6 venues still appear individually in the dashboard. `BookUpdate.exchange` already carries the true venue, so the aggregator routes them unchanged.
3. **Books via a dedicated drain THREAD** (blocking `mp.Queue.get` → `loop.call_soon_threadsafe`), not per-update `run_in_executor` (avoids a thread-hop per high-frequency update). Health via a low-frequency async executor poll.
4. **Bounded, zombie-free shutdown.** `stop_evt.set()` → `join(grace)` → `terminate()` + `join` if still alive. Daemon process as a backstop. Queues closed.
5. **Backpressure = drop, not block.** Bounded `mp.Queue`; the worker drops a book update if the parent is behind (a newer snapshot supersedes it) and counts drops. The book queue must never block the worker's loop (would defeat the isolation).
6. **`isolate_ccxt` flag** (default True) so the in-process path (current behaviour) stays available and testable.

**Deferred:** auto-restart of a crashed child (logged; the supervisor + next session can add it), shared-memory transport, more than one ccxt pool, Windows-vs-POSIX start-method tuning beyond spawn.

---

## File Structure
```
PAVILOS/
├── src/pavilos/connectors/
│   ├── ccxt_worker.py        # child-process entry + async forward loops [NEW]
│   ├── ccxt_pool.py          # CcxtPoolConnector (parent bridge) [NEW]
│   └── venues.py             # NATIVE_VENUES / CCXT_VENUES tuples [MODIFY]
├── src/pavilos/core/
│   ├── engine.py             # health() collects healths()+health() [MODIFY]
│   └── runtime.py            # build native + pool; isolate_ccxt flag [MODIFY]
└── tests/unit/
    ├── test_ccxt_worker.py
    ├── test_ccxt_pool.py
    ├── test_engine.py        # +health collects plural [MODIFY]
    └── test_runtime.py       # +pool wiring [MODIFY]
```

---

## Task 1: Engine.health() collects plural healths

**Files:** Modify `src/pavilos/core/engine.py`; Test `tests/unit/test_engine.py`.

- [ ] **Step 1: Failing test — add to `tests/unit/test_engine.py`:**
```python
def test_health_collects_plural_healths_from_pool_like_connectors():
    from pavilos.core.engine import Engine
    from pavilos.aggregator.aggregator import Aggregator
    from pavilos.aggregator.normalize import PegProvider
    from pavilos.connectors.base import ConnectorHealth

    class _Single:
        exchange = "kraken"
        def health(self): return ConnectorHealth("kraken", True, 1.0, 0, 0)
        async def run(self, q, s): ...

    class _Pool:
        exchange = "ccxt-pool"
        def healths(self): return [ConnectorHealth("gate", True, 2.0, 0, 0),
                                   ConnectorHealth("mexc", False, 0.0, 0, 3)]
        async def run(self, q, s): ...

    agg = Aggregator([], PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=15.0)
    eng = Engine([_Single(), _Pool()], agg)
    names = {h.exchange for h in eng.health()}
    assert names == {"kraken", "gate", "mexc"}
    assert any(h.exchange == "mexc" and h.errors == 3 for h in eng.health())
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Modify `Engine.health()` in `src/pavilos/core/engine.py`:**
```python
    def health(self) -> list[ConnectorHealth]:
        out: list[ConnectorHealth] = []
        for c in self._connectors:
            if hasattr(c, "healths"):       # a pool connector reports many venues
                out.extend(c.healths())
            elif hasattr(c, "health"):
                out.append(c.health())
        return out
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(engine): health() collects plural healths() from pool connectors`.

---

## Task 2: venue-group constants

**Files:** Modify `src/pavilos/connectors/venues.py`; Test `tests/unit/test_venues.py`.

- [ ] **Step 1: Add to `tests/unit/test_venues.py`:**
```python
def test_native_and_ccxt_venue_groups_partition_specs():
    from pavilos.connectors.venues import NATIVE_VENUES, CCXT_VENUES, VENUE_SPECS
    allnames = {s.exchange for s in VENUE_SPECS}
    assert set(NATIVE_VENUES) | set(CCXT_VENUES) == allnames
    assert set(NATIVE_VENUES) & set(CCXT_VENUES) == set()
    assert set(CCXT_VENUES) == {"gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"}
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Add to `src/pavilos/connectors/venues.py`** (near `_CCXT_STAGGER`):
```python
NATIVE_VENUES = ("kraken", "binance", "coinbase", "okx", "bybit", "bitstamp")
CCXT_VENUES = ("gate", "mexc", "cryptocom", "bitget", "kucoin", "htx")
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(venues): export NATIVE_VENUES / CCXT_VENUES groups`.

---

## Task 3: ccxt worker (child process)

**Files:** Create `src/pavilos/connectors/ccxt_worker.py`; Test `tests/unit/test_ccxt_worker.py`.

The worker's async core is tested IN-PROCESS with stub queues (`queue.Queue`), a `threading.Event` as the stop event, and a module-level fake connector factory — no real process, no network. The spawn entry is exercised live.

- [ ] **Step 1: Failing test — `tests/unit/test_ccxt_worker.py`:**
```python
# tests/unit/test_ccxt_worker.py
import asyncio
import queue
import threading

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_worker import _worker_main


class _FakeConn:
    """Module-of-test fake: emits scripted BookUpdates then idles; health() static."""
    def __init__(self, exchange):
        self.exchange = exchange
        self._n = 0

    async def run(self, out_q, stop):
        for i in range(3):
            if stop.is_set():
                return
            await out_q.put(BookUpdate(exchange=self.exchange, ts=float(i),
                                       bids=((100.0, 1.0),), asks=((101.0, 1.0),),
                                       is_snapshot=True, seq=i))
            self._n += 1
            await asyncio.sleep(0)
        await asyncio.Event().wait()

    def health(self):
        return ConnectorHealth(self.exchange, True, float(self._n), 0, 0)


def _factory(venue, symbol):
    return _FakeConn(venue)


def test_worker_forwards_books_and_health_then_stops():
    book_q: queue.Queue = queue.Queue()
    health_q: queue.Queue = queue.Queue()
    stop_evt = threading.Event()

    async def scenario():
        task = asyncio.create_task(_worker_main(
            book_q, health_q, stop_evt, {"gate": "BTC/USDT", "mexc": "BTC/USDT"},
            connector_factory=_factory, health_interval_s=0.01))
        # collect a few forwarded updates
        deadline = 0
        seen = []
        while len(seen) < 4 and deadline < 200:
            try:
                seen.append(book_q.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01); deadline += 1
        stop_evt.set()
        await asyncio.wait_for(task, timeout=2.0)
        return seen

    seen = asyncio.run(scenario())
    assert {u.exchange for u in seen} == {"gate", "mexc"}
    assert all(isinstance(u, BookUpdate) and u.is_snapshot for u in seen)
    # a health snapshot (list of ConnectorHealth) was forwarded
    drained = []
    while True:
        try:
            drained.append(health_q.get_nowait())
        except queue.Empty:
            break
    assert drained and all(isinstance(h, ConnectorHealth) for h in drained[-1])
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/connectors/ccxt_worker.py`:**
```python
# src/pavilos/connectors/ccxt_worker.py
"""Child-process worker: runs the ccxt venue connectors in their OWN asyncio loop,
isolated from the parent's native feeds + detection + dashboard (whose periodic
synchronous bursts were starving the ccxt WS keepalives). BookUpdates and per-venue
ConnectorHealth are forwarded to the parent over multiprocessing queues.

Spawned with the 'spawn' start method, so ``ccxt_worker_entry`` and its arguments
must be picklable and this module must have NO import-time side effects."""
from __future__ import annotations

import asyncio
import logging
import queue as _queue

_log = logging.getLogger(__name__)

_HEALTH_INTERVAL_S = 1.0


def ccxt_worker_entry(book_q, health_q, stop_evt, venue_symbols) -> None:
    """Process entry point (top-level => picklable for spawn)."""
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(_worker_main(book_q, health_q, stop_evt, venue_symbols))
    except Exception:                       # a child must never die silently
        _log.exception("ccxt worker crashed")


async def _worker_main(book_q, health_q, stop_evt, venue_symbols, *,
                       connector_factory=None, health_interval_s: float = _HEALTH_INTERVAL_S) -> None:
    from pavilos.connectors.venues import build_connector
    factory = connector_factory or build_connector
    local_q: "asyncio.Queue" = asyncio.Queue()
    stop = asyncio.Event()
    conns = [factory(v, s) for v, s in venue_symbols.items()]
    tasks = [asyncio.create_task(c.run(local_q, stop)) for c in conns]
    tasks.append(asyncio.create_task(_watch_stop(stop_evt, stop)))
    tasks.append(asyncio.create_task(_forward_books(local_q, book_q, stop)))
    tasks.append(asyncio.create_task(_forward_health(conns, health_q, stop, health_interval_s)))
    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _watch_stop(stop_evt, stop) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        if await loop.run_in_executor(None, stop_evt.wait, 0.25):   # mp.Event.wait is blocking
            break
    stop.set()


async def _forward_books(local_q, book_q, stop) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        try:
            u = await asyncio.wait_for(local_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        await loop.run_in_executor(None, _put_drop, book_q, u)


async def _forward_health(conns, health_q, stop, interval_s) -> None:
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        snap = [c.health() for c in conns]
        await loop.run_in_executor(None, _put_drop, health_q, snap)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


def _put_drop(q, item) -> None:
    try:
        q.put_nowait(item)
    except _queue.Full:
        pass    # parent is behind; a newer snapshot supersedes this one
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(connectors): add ccxt worker (child-process forward loops)`.

---

## Task 4: CcxtPoolConnector (parent bridge)

**Files:** Create `src/pavilos/connectors/ccxt_pool.py`; Test `tests/unit/test_ccxt_pool.py`.

Tested in-process with a FAKE multiprocessing context (`queue.Queue` for queues, `threading.Event` for the event, a stub Process that runs nothing) — so `run()`'s drain + health + shutdown logic is exercised with no real process or network.

- [ ] **Step 1: Failing test — `tests/unit/test_ccxt_pool.py`:**
```python
# tests/unit/test_ccxt_pool.py
import asyncio
import queue
import threading

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_pool import CcxtPoolConnector


class _FakeProc:
    def __init__(self, *a, **k):
        self._alive = True
        self.started = False
        self.terminated = False
    def start(self): self.started = True
    def is_alive(self): return self._alive
    def join(self, timeout=None): self._alive = False
    def terminate(self): self.terminated = True; self._alive = False


class _FakeCtx:
    def __init__(self): self.procs = []
    def Queue(self, maxsize=0): return queue.Queue(maxsize)
    def Event(self): return threading.Event()
    def Process(self, *a, **k):
        p = _FakeProc(*a, **k); self.procs.append(p); return p


def test_pool_forwards_books_updates_health_and_shuts_down():
    ctx = _FakeCtx()
    pool = CcxtPoolConnector({"gate": "BTC/USDT", "mexc": "BTC/USDT"},
                             ctx=ctx, entry=lambda *a, **k: None, join_grace_s=0.5)
    # before run: all disconnected
    assert {h.exchange for h in pool.healths()} == {"gate", "mexc"}
    assert all(not h.connected for h in pool.healths())

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(out_q, stop))
        await asyncio.sleep(0.05)
        # the pool created its queues on the fake ctx; push a book + a health snapshot
        book_q = ctx.procs[0]  # not the queue; grab queues via the pool instead
        # feed through the pool's queues:
        pool._book_q.put(BookUpdate(exchange="gate", ts=1.0, bids=((1.0, 1.0),),
                                    asks=((2.0, 1.0),), is_snapshot=True, seq=1))
        pool._health_q.put([ConnectorHealth("gate", True, 9.0, 0, 0)])
        u = await asyncio.wait_for(out_q.get(), timeout=2.0)
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
        return u

    u = asyncio.run(scenario())
    assert u.exchange == "gate" and u.is_snapshot
    assert any(h.exchange == "gate" and h.connected for h in pool.healths()) is False  # marked disconnected on shutdown
    assert ctx.procs[0].started is True
```

  NOTE: the pool must expose `self._book_q` / `self._health_q` after `run()` starts (assigned at the top of `run`), and mark all venues disconnected in shutdown — the assertions above pin that contract.

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/connectors/ccxt_pool.py`:**
```python
# src/pavilos/connectors/ccxt_pool.py
"""Parent-process bridge to the ccxt worker process. Presents as ONE Engine
connector but manages all ccxt venues in a child process, forwarding their
BookUpdates into the Engine's update queue and exposing per-venue health()."""
from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import queue as _queue
import threading

from pavilos.connectors.base import ConnectorHealth
from pavilos.connectors.ccxt_worker import ccxt_worker_entry

_log = logging.getLogger(__name__)


class CcxtPoolConnector:
    """Engine-connector facade over a child process running the ccxt venues."""

    exchange = "ccxt-pool"

    def __init__(self, venue_symbols: dict, *, ctx=None, entry=ccxt_worker_entry,
                 join_grace_s: float = 5.0) -> None:
        self._venue_symbols = dict(venue_symbols)
        self._ctx = ctx if ctx is not None else mp.get_context("spawn")
        self._entry = entry
        self._join_grace_s = join_grace_s
        self._healths = {v: ConnectorHealth(v, False, 0.0, 0, 0) for v in venue_symbols}
        self._proc = None
        self._book_q = None
        self._health_q = None

    def healths(self) -> list:
        return [self._healths[v] for v in self._venue_symbols]

    async def run(self, out_q, stop) -> None:
        ctx = self._ctx
        self._book_q = ctx.Queue(maxsize=20000)
        self._health_q = ctx.Queue(maxsize=100)
        stop_evt = ctx.Event()
        self._proc = ctx.Process(target=self._entry,
                                 args=(self._book_q, self._health_q, stop_evt, self._venue_symbols),
                                 daemon=True)
        self._proc.start()

        loop = asyncio.get_running_loop()
        thread_stop = threading.Event()
        drain_thread = threading.Thread(target=_book_drain, name="ccxt-book-drain",
                                        args=(self._book_q, loop, out_q, thread_stop), daemon=True)
        drain_thread.start()
        health_task = asyncio.create_task(self._drain_health(stop))
        try:
            await stop.wait()
        finally:
            thread_stop.set()
            health_task.cancel()
            await asyncio.gather(health_task, return_exceptions=True)
            await loop.run_in_executor(None, drain_thread.join, 2.0)
            await self._shutdown(stop_evt)

    async def _drain_health(self, stop) -> None:
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            snap = await loop.run_in_executor(None, _get, self._health_q, 0.5)
            if snap:
                for h in snap:
                    if h.exchange in self._healths:
                        self._healths[h.exchange] = h

    async def _shutdown(self, stop_evt) -> None:
        proc = self._proc
        loop = asyncio.get_running_loop()
        try:
            if proc is not None:
                stop_evt.set()
                await loop.run_in_executor(None, proc.join, self._join_grace_s)
                if proc.is_alive():
                    _log.warning("ccxt worker did not exit in %.1fs; terminating", self._join_grace_s)
                    proc.terminate()
                    await loop.run_in_executor(None, proc.join, 2.0)
        finally:
            for q in (self._book_q, self._health_q):
                _close(q)
            # mark every venue disconnected (the child is gone)
            self._healths = {v: ConnectorHealth(v, False, h.last_update_ts, h.resyncs, h.errors)
                             for v, h in self._healths.items()}


def _book_drain(book_q, loop, out_q, thread_stop) -> None:
    """Blocking-get the child's BookUpdates and hand them to the asyncio loop."""
    while not thread_stop.is_set():
        try:
            u = book_q.get(timeout=0.5)
        except _queue.Empty:
            continue
        except (OSError, ValueError, EOFError):
            break    # queue closed
        try:
            loop.call_soon_threadsafe(out_q.put_nowait, u)
        except RuntimeError:
            break    # loop closed


def _get(q, timeout):
    try:
        return q.get(timeout=timeout)
    except _queue.Empty:
        return None
    except (OSError, ValueError, EOFError):
        return None


def _close(q) -> None:
    close = getattr(q, "close", None)
    if close is not None:
        try:
            close()
        except Exception:
            pass
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(connectors): add CcxtPoolConnector (parent bridge to ccxt worker process)`.

---

## Task 5: wire the pool into Runtime

**Files:** Modify `src/pavilos/core/runtime.py`; Test `tests/unit/test_runtime.py`.

- [ ] **Step 1: Update `tests/unit/test_runtime.py`** — the existing graph-wiring test builds with injected connectors. Add a focused test that with `isolate_ccxt=True` the engine has 6 native connectors + 1 pool (7 total) and the pool covers the ccxt venues; with `isolate_ccxt=False` it has 12. Use the injected `connector_factory` for native venues (it is only called for native venues now):
```python
def test_build_isolates_ccxt_into_pool_by_default():
    from pavilos.core.runtime import Runtime, RuntimeConfig
    from pavilos.connectors.ccxt_pool import CcxtPoolConnector

    built = []
    def fake_factory(v, s):
        class _C:
            exchange = v
            def health(self): ...
            async def run(self, q, st): ...
        built.append(v); return _C()

    rt = Runtime.build(RuntimeConfig(), connector_factory=fake_factory)
    conns = rt.engine._connectors
    pools = [c for c in conns if isinstance(c, CcxtPoolConnector)]
    assert len(pools) == 1
    assert set(pools[0]._venue_symbols) == {"gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"}
    # the injected factory was used for the 6 native venues only
    assert set(built) == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp"}
    assert len(conns) == 7

def test_build_in_process_ccxt_when_isolate_disabled():
    from pavilos.core.runtime import Runtime, RuntimeConfig
    from pavilos.connectors.ccxt_pool import CcxtPoolConnector
    def fake_factory(v, s):
        class _C:
            exchange = v
            def health(self): ...
            async def run(self, q, st): ...
        return _C()
    rt = Runtime.build(RuntimeConfig(isolate_ccxt=False), connector_factory=fake_factory)
    conns = rt.engine._connectors
    assert not any(isinstance(c, CcxtPoolConnector) for c in conns)
    assert len(conns) == 12
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Modify `src/pavilos/core/runtime.py`:**
  (a) Import: `from pavilos.connectors.venues import VENUE_SPECS, build_connector, NATIVE_VENUES, CCXT_VENUES` and `from pavilos.connectors.ccxt_pool import CcxtPoolConnector`.
  (b) Add to `RuntimeConfig`: `isolate_ccxt: bool = True   # run the ccxt venues in a separate process (own event loop) so the main loop can't starve their WS keepalives`.
  (c) Replace the connector list build in `Runtime.build` (currently
`connectors = [connector_factory(v, config.symbols[v]) for v in config.symbols]`) with:
```python
        if config.isolate_ccxt:
            native = [connector_factory(v, config.symbols[v])
                      for v in config.symbols if v in NATIVE_VENUES]
            ccxt_syms = {v: config.symbols[v] for v in config.symbols if v in CCXT_VENUES}
            connectors = native + ([CcxtPoolConnector(ccxt_syms)] if ccxt_syms else [])
        else:
            connectors = [connector_factory(v, config.symbols[v]) for v in config.symbols]
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(runtime): isolate ccxt venues into a CcxtPoolConnector process (isolate_ccxt flag)`.

---

## Task 6: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (206 prior + ~6 new).
- [ ] **Step 2:** Confirm import-clean + no import-time process spawn: `python -c "import pavilos.connectors.ccxt_worker, pavilos.connectors.ccxt_pool; print('import OK')"`.
- [ ] **Step 3:** `git status` clean; `git tag m9-ccxt-isolation`.
- [ ] **Step 4 (operator, live):** `python -m pavilos`, wait ~40s, check `http://127.0.0.1:8800/api/state` → target **12/12 connected and STABLE** over a 2-minute window (the real acceptance test). Confirm no orphaned python child process remains after Ctrl-C (bounded shutdown).

---

## Self-Review (plan author)
**Coverage:** plural health (T1) → venue groups (T2) → worker child loops (T3) → parent bridge (T4) → runtime wiring + flag (T5) → suite/tag/live (T6). Delivers the separate-process isolation that the single-process hot-path opt (M8) could not.
**Testability:** worker + pool are tested IN-PROCESS with stub queues (`queue.Queue`), `threading.Event`, a fake mp ctx, and a module-level fake connector factory — no real spawn, no network. The real spawn is the live acceptance test (T6). This mirrors the codebase's injected-transport pattern.
**Type consistency:** `CcxtPoolConnector(venue_symbols, *, ctx, entry, join_grace_s)` exposes `exchange`, `async run(out_q, stop)`, `healths() -> list[ConnectorHealth]`; `ccxt_worker_entry(book_q, health_q, stop_evt, venue_symbols)` + `_worker_main(..., connector_factory, health_interval_s)`. `Engine.health()` collects `healths()`+`health()`. `BookUpdate`/`ConnectorHealth` are picklable dataclasses (verified) so they cross `mp.Queue` unchanged.
**Adversarial focus (3rd barrier):** (1) **no zombies** — `stop_evt.set()` → `join(grace)` → `terminate()`+join; daemon backstop; verify the child exits on stop and after a parent crash. (2) **spawn picklability** — entry is top-level, args are picklable (dict/queues/event); the worker module has NO import-time side effects (no spawn at import). (3) **drain thread vs loop shutdown** — `call_soon_threadsafe` after the loop closes raises RuntimeError → caught, thread exits; `thread_stop` + bounded join. (4) **backpressure** — bounded book queue, worker drops (never blocks its own loop); a slow parent must not wedge the child. (5) **health race** — the async health poll only updates known venues; on shutdown all are marked disconnected. (6) **queue closed mid-get** — `_get`/`_book_drain` swallow OSError/EOFError/ValueError and exit. (7) **the in-process fallback** (`isolate_ccxt=False`) still builds 12 and behaves as before. (8) confirm `out_q.put_nowait` (unbounded Engine update queue) can't raise QueueFull. The headline acceptance is the live 12/12-stable test; if isolation still doesn't stabilise all 12, report it honestly rather than declaring success.
