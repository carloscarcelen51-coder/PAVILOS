# PAVILOS M1c: Transport (Kraken + Binance live clients + engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the network-free connector logic (M1b) to real exchanges: async WebSocket/REST clients for Kraken and Binance that produce live `BookUpdate`s, verify integrity (Kraken CRC32 over a raw Decimal book; Binance sequence continuity), recover from disconnects/resyncs with backoff, and feed an `Engine` that drives the `Aggregator` to emit combined depth snapshots. All run-loop logic is unit-tested deterministically with injected fake transports (no network); real connectivity is verified by a separate skippable live-smoke script.

**Architecture:** Each connector's run loop depends on an injected `connect` coroutine (returns a live async iterator of decoded messages) and, for Binance, a `fetch_snapshot` coroutine — plus injected `now`/`sleep`. Real defaults use `websockets` + `aiohttp`; tests pass fakes that yield scripted frames and simulate disconnects. This keeps the trickiest behavior (reconnect/backoff, checksum-mismatch resync, seed-then-apply ordering) fully deterministic in CI while the real I/O is thin and exercised by `scripts/live_smoke.py`. The `Engine` composes N connectors → one `asyncio.Queue` → `Aggregator.run` → combined snapshot queue.

**Tech Stack:** Python 3.13, `asyncio`, **new deps `websockets>=12` + `aiohttp>=3.9`**, `decimal` (raw Kraken book), `pytest`. Builds on merged M1-core + M1b. Decode Kraken frames with `parse_float=Decimal` to preserve checksum precision.

---

## Protocol facts this plan relies on (from M1b research, verified)
- **Kraken:** `wss://ws.kraken.com/v2`; subscribe `{"method":"subscribe","params":{"channel":"book","symbol":["BTC/USD"],"depth":10}}`. Messages `{channel:"book", type:"snapshot"|"update", data:[{bids,asks,checksum,...}]}`. Non-book frames (ack/status/heartbeat) must be skipped. Re-subscribing yields a fresh snapshot (resync recovery). CRC32 over raw top-10 strings (M1b `book_checksum`).
- **Binance:** `wss://stream.binance.com:9443/ws/btcusdt@depth@100ms` (event `depthUpdate`); REST seed `GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000`. Open the stream BEFORE fetching the snapshot so events buffer; `BinanceDepthFeed` drops stale + detects gaps. Resync = re-seed.
- **Geo:** residential ES host → direct. Optional proxy is plumbed but defaults off (contingency for datacenter egress on Bybit/Binance — a later milestone concern).

---

## File Structure

```
PAVILOS/
├── pyproject.toml                    # + dependencies: websockets, aiohttp
├── src/pavilos/connectors/
│   ├── kraken_book.py                # KrakenRawBook (Decimal book for CRC verify) [NEW]
│   ├── kraken_connector.py           # KrakenConnector (async run loop) [NEW]
│   └── binance_connector.py          # BinanceConnector (async run loop) [NEW]
├── src/pavilos/core/
│   └── engine.py                     # Engine: connectors + Aggregator wiring [NEW]
├── scripts/
│   ├── live_smoke.py                 # manual: connect live, print combined book [NEW]
│   └── capture.py                    # manual: record real frames to fixtures [NEW]
└── tests/unit/
    ├── test_kraken_book.py
    ├── test_kraken_connector.py
    ├── test_binance_connector.py
    └── test_engine.py
```

**Responsibility per file:**
- `kraken_book.py` — maintains Kraken's book at FULL string precision (separate from the aggregator's float `BookState`) solely to recompute and verify the CRC32. Pure, no I/O.
- `kraken_connector.py` — async loop: connect → for each `book` frame, emit `BookUpdate` + verify checksum (raise `ResyncRequired` on mismatch) → reconnect with backoff. Transport injected.
- `binance_connector.py` — async loop: connect → fetch snapshot → seed → apply diffs (`BinanceDepthFeed`) → emit; `ResyncRequired` → re-seed; reconnect with backoff. Transport injected.
- `engine.py` — owns the connectors + the `Aggregator`; runs them concurrently; exposes the combined-snapshot output queue and per-connector health.
- `scripts/live_smoke.py` / `capture.py` — manual, network-using tools (NOT pytest).

---

## Task 1: Add async transport dependencies

**Files:**
- Modify: `pyproject.toml`
- Test: `tests/unit/test_deps_importable.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_deps_importable.py`:**

```python
# tests/unit/test_deps_importable.py
def test_async_transport_deps_importable():
    import websockets  # noqa: F401
    import aiohttp     # noqa: F401
    assert websockets is not None
    assert aiohttp is not None
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_deps_importable.py -v`** — expect FAIL (`ModuleNotFoundError: No module named 'websockets'` or `'aiohttp'`) if not yet installed.

- [ ] **Step 3: Edit `pyproject.toml`** — change `dependencies = []` to:

```toml
dependencies = [
    "websockets>=12",
    "aiohttp>=3.9",
]
```

- [ ] **Step 4: Install** — run `python -m pip install -e .` (this pulls websockets + aiohttp from PyPI).

- [ ] **Step 5: Run `python -m pytest tests/unit/test_deps_importable.py -v`** — expect PASS (1 passed).

- [ ] **Step 6: Run full suite `python -m pytest`** — expect 45 passed (44 prior + 1).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml tests/unit/test_deps_importable.py
git commit -m "chore(connectors): add websockets + aiohttp transport dependencies"
```

---

## Task 2: KrakenRawBook (Decimal book for checksum verification)

**Files:**
- Create: `src/pavilos/connectors/kraken_book.py`
- Test: `tests/unit/test_kraken_book.py`

> Kraken's CRC32 must be computed over the book at full wire precision. The
> aggregator's `BookState` is float-based (fine for binning, wrong for checksum),
> so the connector keeps a separate string/Decimal book ONLY for verification.
> Price strings are used as dict keys — valid because Kraken sends each price at
> a fixed, consistent precision for a given pair, so the same level always has
> the same string. `apply` mirrors Kraken semantics (snapshot resets; qty 0
> removes; truncate each side to `depth`). `checksum()` reuses M1b `book_checksum`.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_kraken_book.py`:**

```python
# tests/unit/test_kraken_book.py
from pavilos.connectors.kraken_book import KrakenRawBook
from pavilos.connectors.kraken import book_checksum


def _snap(bids, asks, checksum=0):
    return {"channel": "book", "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks], "checksum": checksum}]}


def _upd(bids, asks, checksum=0):
    return {"channel": "book", "type": "update",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks], "checksum": checksum}]}


def test_snapshot_checksum_matches_book_checksum():
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[("100.0", "1.5"), ("99.5", "3.0")], asks=[("100.5", "2.0"), ("101.0", "0.5")]))
    # expected = book_checksum over asks(low->high) then bids(high->low) strings
    expected = book_checksum([("100.5", "2.0"), ("101.0", "0.5")], [("100.0", "1.5"), ("99.5", "3.0")])
    assert book.checksum() == expected
    assert book.verify(expected) is True
    assert book.verify(expected ^ 0xFF) is False


def test_update_applies_and_removes_then_rechecksums():
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")]))
    book.apply(_upd(bids=[("100.0", "0")], asks=[("101.0", "2.5"), ("101.5", "1.0")]))
    # bid 100.0 removed; asks now 101.0->2.5 and 101.5->1.0
    expected = book_checksum([("101.0", "2.5"), ("101.5", "1.0")], [])
    assert book.checksum() == expected


def test_truncates_each_side_to_depth():
    book = KrakenRawBook("BTC/USD", depth=2)
    book.apply(_snap(
        bids=[("100.0", "1"), ("99.0", "1"), ("98.0", "1")],
        asks=[("101.0", "1"), ("102.0", "1"), ("103.0", "1")],
    ))
    # only top-2 each side kept: bids 100/99, asks 101/102
    expected = book_checksum([("101.0", "1"), ("102.0", "1")], [("100.0", "1"), ("99.0", "1")])
    assert book.checksum() == expected
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_kraken_book.py -v`** — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/kraken_book.py`:**

```python
# src/pavilos/connectors/kraken_book.py
"""Full-precision Kraken book kept only to verify the v2 CRC32 checksum.

Separate from the aggregator's float ``BookState`` (which is fine for binning
but loses the precision the checksum needs). Price strings are dict keys, valid
because Kraken sends each price at a fixed precision per pair."""
from __future__ import annotations

from decimal import Decimal

from pavilos.connectors.kraken import book_checksum


def _to_str(v: object) -> str:
    """Normalize a Kraken price/qty (str when from a fake, Decimal when decoded
    with ``parse_float=Decimal``) to its plain-decimal string form."""
    if isinstance(v, str):
        return v
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


class KrakenRawBook:
    """Maintains one Kraken book at full string precision for CRC32 verification."""

    def __init__(self, symbol: str, *, depth: int = 10) -> None:
        self.symbol = symbol
        self._depth = depth
        self._bids: dict[str, str] = {}
        self._asks: dict[str, str] = {}

    def apply(self, msg: dict) -> None:
        data = msg["data"][0]
        if msg["type"] == "snapshot":
            self._bids = {}
            self._asks = {}
        for lvl in data["bids"]:
            self._set(self._bids, lvl)
        for lvl in data["asks"]:
            self._set(self._asks, lvl)
        self._bids = dict(sorted(self._bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[: self._depth])
        self._asks = dict(sorted(self._asks.items(), key=lambda kv: Decimal(kv[0]))[: self._depth])

    @staticmethod
    def _set(side: dict[str, str], lvl: dict) -> None:
        price = _to_str(lvl["price"])
        qty = _to_str(lvl["qty"])
        if Decimal(qty) == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def checksum(self) -> int:
        asks = sorted(self._asks.items(), key=lambda kv: Decimal(kv[0]))[:10]
        bids = sorted(self._bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[:10]
        return book_checksum([(p, q) for p, q in asks], [(p, q) for p, q in bids])

    def verify(self, expected: int) -> bool:
        return self.checksum() == expected
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_kraken_book.py -v`** — expect PASS (3 passed).

- [ ] **Step 5: Full suite `python -m pytest`** — expect 48 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/kraken_book.py tests/unit/test_kraken_book.py
git commit -m "feat(connectors): add KrakenRawBook (Decimal book for CRC32 verification)"
```

---

## Task 3: KrakenConnector (async run loop, injectable transport)

> **Correction applied during implementation (2026-06-07):** the draft test's
> `_frame("update", ...)` helper computed the frame checksum over only the delta
> levels, but Kraken's CRC32 (and `KrakenRawBook`, correctly) covers the FULL
> cumulative top-10 book after the update. The shipped test therefore uses a
> `_FrameBuilder` (backed by `KrakenRawBook`) that checksums the cumulative book
> per frame; `_frame` is kept only for standalone snapshots (where the listed
> levels ARE the whole book). The connector implementation below is unchanged
> and correct.

**Files:**
- Create: `src/pavilos/connectors/kraken_connector.py`
- Test: `tests/unit/test_kraken_connector.py`

> The run loop is driven by an injected `connect` coroutine returning a live
> async iterator of DECODED `book` messages. The real default uses
> `websockets` (decoding with `parse_float=Decimal`). Each frame: skip non-book;
> `parse_kraken_message` → `BookUpdate` → `out_q`; mirror into `KrakenRawBook`
> and verify the frame's checksum → `ResyncRequired` on mismatch. On any
> disconnect/resync, reconnect (a fresh subscribe yields a new snapshot) after
> backoff. `now`/`sleep` injected for deterministic tests.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_kraken_connector.py`:**

```python
# tests/unit/test_kraken_connector.py
import asyncio

from pavilos.connectors.kraken import book_checksum
from pavilos.connectors.kraken_connector import KrakenConnector


def _frame(mtype, bids, asks):
    cs = book_checksum(
        sorted(asks, key=lambda x: float(x[0])),
        sorted(bids, key=lambda x: float(x[0]), reverse=True),
    )
    return {"channel": "book", "type": mtype,
            "data": [{"symbol": "BTC/USD",
                      "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks],
                      "checksum": cs}]}


def _run(coro):
    return asyncio.run(coro)


def test_emits_bookupdates_and_skips_non_book_frames():
    frames = [
        {"channel": "status", "type": "update", "data": []},   # must be skipped
        _frame("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")]),
        _frame("update", bids=[("100.0", "1.5")], asks=[]),
    ]

    async def fake_connect():
        async def gen():
            for f in frames:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = KrakenConnector("BTC/USD", connect=fake_connect, now=lambda: 1.0,
                               sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u1, u2

    u1, u2 = _run(scenario())
    assert u1.exchange == "kraken" and u1.is_snapshot is True
    assert u1.bids == ((100.0, 1.0),)
    assert u2.is_snapshot is False and u2.bids == ((100.0, 1.5),)


def test_checksum_mismatch_triggers_reconnect():
    bad = _frame("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")])
    bad["data"][0]["checksum"] = 1  # deliberately wrong
    good = _frame("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")])
    sessions = [[bad], [good]]   # first session bad -> reconnect -> second good
    calls = {"n": 0}

    async def fake_connect():
        i = calls["n"]
        calls["n"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = KrakenConnector("BTC/USD", connect=fake_connect, now=lambda: 1.0,
                               sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)  # from the GOOD session
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u, calls["n"]

    u, n = _run(scenario())
    assert u.is_snapshot is True
    assert n >= 2   # reconnected at least once after the bad checksum
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_kraken_connector.py -v`** — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/kraken_connector.py`:**

```python
# src/pavilos/connectors/kraken_connector.py
"""Async Kraken v2 book connector: emits BookUpdates + verifies CRC32, with
reconnect/resync. Transport (`connect`) is injected for deterministic tests."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal

import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.kraken import parse_kraken_message
from pavilos.connectors.kraken_book import KrakenRawBook

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"


class KrakenConnector:
    """Streams Kraken ``book`` frames into ``BookUpdate``s on an output queue,
    verifying each frame's CRC32 against a full-precision local book. On a
    checksum mismatch or disconnect it reconnects (a fresh subscribe re-snapshots)
    with exponential backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        depth: int = 10,
        url: str = KRAKEN_WS_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "kraken"
        self._depth = depth
        self._url = url
        self._connect = connect or self._default_connect
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._proxy = proxy
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts, self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            book = KrakenRawBook(self.symbol, depth=self._depth)
            try:
                stream = await self._connect()
                self._connected = True
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("channel") != "book":
                        continue
                    ts = self._now()
                    out = parse_kraken_message(msg, ts=ts, exchange=self.exchange)
                    book.apply(msg)
                    if not book.verify(int(msg["data"][0]["checksum"])):
                        self._resyncs += 1
                        raise ResyncRequired("kraken checksum mismatch")
                    await out_q.put(out)
                    self._last_update_ts = ts
                    backoff = 1.0
            except ResyncRequired:
                pass
            except Exception:
                self._errors += 1
            finally:
                self._connected = False
            if stop.is_set():
                break
            await self._sleep(min(backoff, self._max_backoff))
            backoff = min(backoff * 2, self._max_backoff) if self._max_backoff else 0.0

    async def _default_connect(self) -> AsyncIterator[dict]:
        ws = await websockets.connect(self._url, proxy=self._proxy) if self._proxy else await websockets.connect(self._url)
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "book", "symbol": [self.symbol], "depth": self._depth},
        }))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw, parse_float=Decimal)
            finally:
                await ws.close()

        return gen()


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_kraken_connector.py -v`** — expect PASS (2 passed).

- [ ] **Step 5: Full suite `python -m pytest`** — expect 50 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/kraken_connector.py tests/unit/test_kraken_connector.py
git commit -m "feat(connectors): add async KrakenConnector with CRC32 verify + reconnect"
```

---

## Task 4: BinanceConnector (async run loop, injectable transport)

**Files:**
- Create: `src/pavilos/connectors/binance_connector.py`
- Test: `tests/unit/test_binance_connector.py`

> The loop opens the stream (so events buffer), fetches the REST snapshot, seeds
> `BinanceDepthFeed`, emits the snapshot `BookUpdate`, then applies diff events
> (stale → skipped; gap → `ResyncRequired` → reconnect/re-seed). `connect` and
> `fetch_snapshot` are injected; real defaults use `websockets`/`aiohttp`.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_binance_connector.py`:**

```python
# tests/unit/test_binance_connector.py
import asyncio

from pavilos.connectors.binance_connector import BinanceConnector


def _snapshot(last_update_id, bids, asks):
    return {"lastUpdateId": last_update_id, "bids": bids, "asks": asks}


def _event(U, u, bids, asks, E=1_000):
    return {"e": "depthUpdate", "E": E, "s": "BTCUSDT", "U": U, "u": u, "b": bids, "a": asks}


def test_seeds_then_applies_contiguous_events():
    events = [
        _event(U=101, u=105, bids=[["100.0", "1.5"]], asks=[], E=6_000),
        _event(U=106, u=108, bids=[], asks=[["101.0", "0"]], E=7_000),
    ]

    async def fake_connect():
        async def gen():
            for e in events:
                yield e
        return gen()

    async def fake_fetch():
        return _snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]])

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=fake_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap, u1, u2

    snap, u1, u2 = _run(scenario())
    assert snap.is_snapshot is True and snap.seq == 100 and snap.exchange == "binance"
    assert u1.is_snapshot is False and u1.seq == 105
    assert u2.seq == 108


def test_gap_triggers_reseed():
    sessions = [
        [  # first session: a gap after seed -> ResyncRequired -> reconnect
            _event(U=200, u=201, bids=[["100.0", "9.0"]], asks=[], E=6_000),  # U=200 > 100+1 -> gap
        ],
        [  # second session resumes cleanly from a fresh snapshot (lastUpdateId 300)
            _event(U=301, u=302, bids=[["100.0", "2.0"]], asks=[], E=8_000),
        ],
    ]
    snaps = [_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]),
             _snapshot(300, [["100.0", "1.0"]], [["101.0", "2.0"]])]
    calls = {"c": 0, "s": 0}

    async def fake_connect():
        i = calls["c"]; calls["c"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for e in session:
                yield e
        return gen()

    async def fake_fetch():
        i = calls["s"]; calls["s"] += 1
        return snaps[min(i, len(snaps) - 1)]

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BinanceConnector("BTCUSDT", connect=fake_connect, fetch_snapshot=fake_fetch,
                                now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        seqs = []
        for _ in range(3):  # seed1, (gap), seed2, update2
            u = await asyncio.wait_for(out_q.get(), timeout=1.0)
            seqs.append(u.seq)
            if u.seq == 302:
                break
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return seqs, calls["c"]

    seqs, c = _run(scenario())
    assert 100 in seqs and 300 in seqs and 302 in seqs   # reseeded and resumed
    assert c >= 2


def _run(coro):
    return asyncio.run(coro)
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_binance_connector.py -v`** — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/binance_connector.py`:**

```python
# src/pavilos/connectors/binance_connector.py
"""Async Binance spot depth connector: REST seed + diff stream -> BookUpdates,
with reconnect/re-seed on gap. Transport (`connect`/`fetch_snapshot`) injected."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable

import aiohttp
import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.binance import BinanceDepthFeed

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_REST_URL = "https://api.binance.com/api/v3/depth"


class BinanceConnector:
    """Streams Binance spot depth into ``BookUpdate``s: opens the diff stream,
    seeds from REST, then applies diffs via ``BinanceDepthFeed``. A gap raises
    ``ResyncRequired`` and the loop reconnects + re-seeds with backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        url: str = BINANCE_WS_URL,
        rest_url: str = BINANCE_REST_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        fetch_snapshot: Callable[[], Awaitable[dict]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "binance"
        self._url = url
        self._rest_url = rest_url
        self._connect = connect or self._default_connect
        self._fetch_snapshot = fetch_snapshot or self._default_fetch_snapshot
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._proxy = proxy
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts, self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            feed = BinanceDepthFeed(self.symbol, exchange=self.exchange)
            try:
                stream = await self._connect()          # open first so events buffer
                self._connected = True
                snapshot = await self._fetch_snapshot()
                snap = feed.seed(snapshot, ts=self._now())
                await out_q.put(snap)
                self._last_update_ts = snap.ts
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("e") != "depthUpdate":
                        continue
                    out = feed.apply(msg)               # None if stale; raises on gap
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
            finally:
                self._connected = False
            if stop.is_set():
                break
            await self._sleep(min(backoff, self._max_backoff))
            backoff = min(backoff * 2, self._max_backoff) if self._max_backoff else 0.0

    async def _default_connect(self) -> AsyncIterator[dict]:
        stream_url = f"{self._url}/{self.symbol.lower()}@depth@100ms"
        ws = await websockets.connect(stream_url, proxy=self._proxy) if self._proxy else await websockets.connect(stream_url)

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()

        return gen()

    async def _default_fetch_snapshot(self) -> dict:
        params = {"symbol": self.symbol, "limit": 5000}
        async with aiohttp.ClientSession() as session:
            async with session.get(self._rest_url, params=params, proxy=self._proxy) as resp:
                resp.raise_for_status()
                return await resp.json()


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_binance_connector.py -v`** — expect PASS (2 passed).

- [ ] **Step 5: Full suite `python -m pytest`** — expect 52 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/binance_connector.py tests/unit/test_binance_connector.py
git commit -m "feat(connectors): add async BinanceConnector with seed/diff + reseed"
```

---

## Task 5: Engine (connectors + Aggregator wiring)

**Files:**
- Create: `src/pavilos/core/engine.py`
- Test: `tests/unit/test_engine.py`

> `Engine` runs N connectors (each with a `run(out_q, stop)` coroutine) all
> writing to ONE shared `BookUpdate` queue, plus the `Aggregator.run` loop
> draining it into a combined-snapshot queue. It exposes `snapshots` (the output
> queue), `start()`, `stop()`, and `health()`. Connectors are injected (the test
> passes fakes; production passes real KrakenConnector/BinanceConnector).

- [ ] **Step 1: Write the failing test — create `tests/unit/test_engine.py`:**

```python
# tests/unit/test_engine.py
import asyncio

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.engine import Engine


class _FakeConnector:
    """Emits a fixed list of BookUpdates onto out_q, then idles until stop."""
    def __init__(self, exchange, updates):
        self.exchange = exchange
        self._updates = updates

    async def run(self, out_q, stop):
        for u in self._updates:
            await out_q.put(u)
        await stop.wait()


def _snap(exchange, ts, bids, asks):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=True, seq=None)


def test_engine_produces_combined_snapshot_from_connectors():
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("binance", Quote.USDT, Tier.A)]
    connectors = [
        _FakeConnector("kraken", [_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)])]),
        _FakeConnector("binance", [_snap("binance", 1.0, [(100.0, 0.5)], [(101.0, 0.5)])]),
    ]

    async def scenario():
        agg = Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)
        engine = Engine(connectors, agg, interval_s=0.0, now=lambda: 2.0)
        await engine.start()
        snap = await asyncio.wait_for(engine.snapshots.get(), timeout=1.0)
        await engine.stop()
        return snap

    snap = asyncio.run(scenario())
    assert snap is not None
    assert set(snap.venues_active) == {"kraken", "binance"}
    assert snap.mid == 100.5
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_engine.py -v`** — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/core/engine.py`:**

```python
# src/pavilos/core/engine.py
"""Engine: run connectors + the Aggregator concurrently and emit combined
snapshots. Connectors are injected (real ones in production, fakes in tests)."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot
from pavilos.aggregator.aggregator import Aggregator
from pavilos.connectors.base import ConnectorHealth


class Engine:
    """Composes connectors → one BookUpdate queue → Aggregator.run → snapshot
    queue. Each connector must expose ``exchange`` and ``async run(out_q, stop)``
    and optionally ``health()``."""

    def __init__(
        self,
        connectors: Sequence[object],
        aggregator: Aggregator,
        *,
        interval_s: float = 0.1,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._connectors = list(connectors)
        self._aggregator = aggregator
        self._interval_s = interval_s
        self._now = now or _wall_now
        self._updates: "asyncio.Queue[BookUpdate]" = asyncio.Queue()
        self.snapshots: "asyncio.Queue[CombinedDepthSnapshot]" = asyncio.Queue()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._stop.clear()
        for c in self._connectors:
            self._tasks.append(asyncio.create_task(c.run(self._updates, self._stop)))
        self._tasks.append(asyncio.create_task(
            self._aggregator.run(self._updates, self.snapshots,
                                 interval_s=self._interval_s, now=self._now, stop=self._stop)
        ))

    async def stop(self) -> None:
        self._stop.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def health(self) -> list[ConnectorHealth]:
        return [c.health() for c in self._connectors if hasattr(c, "health")]


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_engine.py -v`** — expect PASS (1 passed).

- [ ] **Step 5: Full suite `python -m pytest`** — expect 53 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/core/engine.py tests/unit/test_engine.py
git commit -m "feat(core): add Engine wiring connectors into the Aggregator"
```

---

## Task 6: Live smoke + capture scripts (manual, network)

**Files:**
- Create: `scripts/live_smoke.py`
- Create: `scripts/capture.py`

> These are MANUAL tools (not pytest) that use the real network, run from the
> residential host. `live_smoke.py` runs Kraken+Binance connectors through the
> Engine for ~N seconds and prints the combined book + per-connector health.
> `capture.py` records raw frames to a JSONL fixture for future regression use.

- [ ] **Step 1: Create `scripts/live_smoke.py`:**

```python
# scripts/live_smoke.py
"""MANUAL live smoke (uses the network): run Kraken+Binance through the Engine
for a few seconds and print the combined book + health. Not a pytest test.

Usage: python -m scripts.live_smoke [seconds]
"""
from __future__ import annotations

import asyncio
import sys

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.engine import Engine
from pavilos.connectors.kraken_connector import KrakenConnector
from pavilos.connectors.binance_connector import BinanceConnector


async def main(seconds: float) -> int:
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("binance", Quote.USDT, Tier.A)]
    agg = Aggregator(specs, PegProvider(), bin_bps=5.0, window_bps=50.0, staleness_s=15.0)
    connectors = [KrakenConnector("BTC/USD", depth=25), BinanceConnector("BTCUSDT")]
    engine = Engine(connectors, agg, interval_s=1.0)
    await engine.start()
    try:
        deadline = asyncio.get_event_loop().time() + seconds
        last = None
        while asyncio.get_event_loop().time() < deadline:
            try:
                last = await asyncio.wait_for(engine.snapshots.get(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        if last is None:
            print("NO SNAPSHOT — check connectivity")
            return 1
        print(f"mid={last.mid:.2f} venues={last.venues_active}/{last.venues_total} "
              f"bids={len(last.bids)} asks={len(last.asks)}")
        for b in last.bids[:5]:
            print(f"  BID {b.price:.2f} size={b.size:.4f} {b.composition}")
        for h in engine.health():
            print(f"  health {h.exchange}: connected={h.connected} resyncs={h.resyncs} errors={h.errors}")
        return 0
    finally:
        await engine.stop()


if __name__ == "__main__":
    secs = float(sys.argv[1]) if len(sys.argv) > 1 else 15.0
    raise SystemExit(asyncio.run(main(secs)))
```

- [ ] **Step 2: Create `scripts/capture.py`:**

```python
# scripts/capture.py
"""MANUAL frame capture (uses the network): record raw decoded frames from one
exchange WS to a JSONL file for future regression fixtures. Not a pytest test.

Usage: python -m scripts.capture kraken|binance <out.jsonl> [count]
"""
from __future__ import annotations

import asyncio
import json
import sys

from pavilos.connectors.kraken_connector import KrakenConnector
from pavilos.connectors.binance_connector import BinanceConnector


async def main(exchange: str, out_path: str, count: int) -> int:
    if exchange == "kraken":
        stream = await KrakenConnector("BTC/USD", depth=25)._default_connect()
    elif exchange == "binance":
        stream = await BinanceConnector("BTCUSDT")._default_connect()
    else:
        print("exchange must be kraken|binance", file=sys.stderr)
        return 2
    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        async for msg in stream:
            f.write(json.dumps(msg, default=str) + "\n")
            n += 1
            if n >= count:
                break
    print(f"captured {n} frames to {out_path}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python -m scripts.capture kraken|binance <out.jsonl> [count]", file=sys.stderr)
        raise SystemExit(2)
    ex, out = sys.argv[1], sys.argv[2]
    cnt = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    raise SystemExit(asyncio.run(main(ex, out, cnt)))
```

- [ ] **Step 3: Smoke-check the scripts import cleanly (no network):**

Run: `python -c "import scripts.live_smoke, scripts.capture; print('import OK')"`
Expected: `import OK` (this only imports — it does not connect).

- [ ] **Step 4: Commit**

```bash
git add scripts/live_smoke.py scripts/capture.py
git commit -m "feat(scripts): add manual live-smoke + frame-capture tools"
```

> NOTE: Actually running `python -m scripts.live_smoke 15` requires network from the residential host and is an operator step, NOT part of CI. Run it once to confirm real connectivity after the unit tasks are green.

---

## Task 7: Full suite green + close-out

**Files:** none (verification only)

- [ ] **Step 1: Run the entire suite** — `python -m pytest -v` — expect ALL pass (53: 44 prior + 9 new — deps 1, kraken_book 3, kraken_connector 2, binance_connector 2, engine 1).

- [ ] **Step 2: Confirm clean tree** — `git status` — working tree clean.

- [ ] **Step 3: Tag** — `git tag m1c-transport && git log --oneline -8`.

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.1 connectors — transport portion + §4 wiring):**
- Async WS clients for the two reference venues → Tasks 3, 4 ✅
- Kraken full-snapshot model + CRC32 verification over a raw Decimal book → Tasks 2, 3 ✅
- Binance REST-seed + diff with seed-then-apply ordering + gap re-seed → Task 4 ✅
- Reconnection with exponential backoff + resync recovery → Tasks 3, 4 (injected `sleep` makes it deterministic) ✅
- Connectors feed `Aggregator.run`; Engine emits combined snapshots → Task 5 ✅
- Proxy plumbed (default off) → Tasks 3, 4 (`proxy` param) ✅
- Per-connector health (`ConnectorHealth`) → Tasks 3, 4, 5 ✅
- Live verification path (operator-run, network) → Task 6 ✅
- *Deferred to M1d (correctly out of scope):* remaining 4 native connectors (Coinbase/OKX[seqId-primary, CRC32 deprecating 2026-06-23]/Bybit/Bitstamp) + ccxt long-tail wrapper; live peg/FX updater (USDT/USD, KRW/JPY); heartbeat/ping keepalive tuning per venue; replay-fixture regression tests built from `capture.py` output; dashboard/Telegram (M2+).

**Placeholder scan:** No TBD/TODO; every code step has complete runnable code. The two manual scripts are explicitly operator-run (network), with an import-only CI check.

**Type consistency:** connectors expose `exchange` + `async run(out_q, stop)` + `health() -> ConnectorHealth`, consumed uniformly by `Engine`; `Aggregator.run(in_q, out_q, *, interval_s, now, stop)` matches the M1-core signature; `BinanceDepthFeed`/`parse_kraken_message`/`book_checksum`/`KrakenRawBook` used per their M1b/Task-2 signatures.

**Determinism note:** every run-loop test injects `connect`/`fetch_snapshot`/`now`/`sleep` and uses `asyncio.run` + `wait_for` timeouts — no real network, no wall-clock sleeps (`sleep=lambda d: asyncio.sleep(0)`, `max_backoff=0.0`). The only network code paths (`_default_connect`/`_default_fetch_snapshot`) are exercised by the Task 6 operator smoke, not CI.

**Risk note (precision boundary):** `KrakenConnector._default_connect` decodes with `parse_float=Decimal` so `KrakenRawBook` sees full precision for the checksum, while `parse_kraken_message` floats the same values for the aggregator. The fake-stream tests use string prices (also full precision), so both paths are precision-correct in tests and in production.
