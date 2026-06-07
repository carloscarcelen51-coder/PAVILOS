# PAVILOS M1d: Remaining Native Connectors (Coinbase, OKX, Bybit, Bitstamp) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task, with a THIRD barrier per task: after spec-compliance review and code-quality review, an **adversarial verification** pass (a skeptic that tries to BREAK the unit — find a frame sequence that corrupts the book, a mishandled gap, an untested branch). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Complete the deep-liquidity venue coverage by adding the four remaining native WS connectors — Coinbase (level2), OKX (books), Bybit (orderbook.200), Bitstamp (diff_order_book) — each producing live `BookUpdate`s with correct per-venue integrity (sequence continuity), reconnect/resync, and wired into the Engine. All sequencing logic is unit-tested deterministically with synthetic frames (no network); real connectivity is operator-verified via the live smoke.

**Architecture:** Three of the four venues (Coinbase/OKX/Bybit) share the **full-snapshot-WS + deltas** shape, so they use ONE generic `SnapshotDeltaConnector` parameterized by a per-venue **feed** (a pure `process(msg, *, ts) -> BookUpdate | None` that returns `None` for non-data frames, emits a snapshot/update `BookUpdate`, and raises `ResyncRequired` on a sequence gap). Bitstamp uses the **REST-seed + WS-diff** shape (like Binance) via a `BitstampDepthFeed` (`seed`/`apply`, reconciled by `microtimestamp`) and a `BitstampConnector`. The existing Kraken/Binance connectors are LEFT AS-IS (they have venue-specific checksum/seed loops that work and are tested; migrating them to the generic is a deferred, separate cleanup). Every feed is pure and network-free; the real WS/REST transport (`_default_connect`/`_default_fetch_snapshot`, with app-level ping) is live-smoke-only.

**Tech Stack:** Python 3.13, asyncio, `websockets`/`aiohttp` (already deps), `pytest`. Builds on merged M1-core + M1b + M1c. Reuses `pavilos.core.models.BookUpdate`, `pavilos.connectors.base.{ResyncRequired, ConnectorHealth}`, `pavilos.core.engine.Engine`.

---

## Protocol facts this plan encodes (verified 2026-06-07, official docs)

| Venue | WS URL | Channel / subscribe | Model | Integrity | Removal |
|---|---|---|---|---|---|
| Coinbase | `wss://advanced-trade-ws.coinbase.com` | `{"type":"subscribe","product_ids":["BTC-USD"],"channel":"level2"}` (msgs arrive as `channel:"l2_data"`) | full-snapshot WS + deltas | `sequence_num` +1 exact; gap if `> last+1` → resync; `<= last` ignore | `new_quantity=="0"`; abs; side `bid`/`offer` |
| OKX | `wss://ws.okx.com:8443/ws/v5/public` (EEA `wseea.okx.com`) | `{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}` | full-snapshot WS + deltas | `seqId`/`prevSeqId`: valid iff `update.prevSeqId == last seqId`; snapshot `prevSeqId==-1`; `seqId==prevSeqId` no-op; `seqId<prevSeqId` reset → resync. CRC32 deprecates 2026-06-23→0 (seqId PRIMARY) | size `"0"`; abs; level `[price,size,_,_]` |
| Bybit | `wss://stream.bybit.com/v5/public/spot` | `{"op":"subscribe","args":["orderbook.200.BTCUSDT"]}` | full-snapshot WS + deltas | `data.u` +1; gap → resync; `type:"snapshot"` or `u==1` → RESET book; `seq` is NOT continuity. ping `{"op":"ping"}`/20s | size `"0"`; abs; `b`/`a` `[price,size]` |
| Bitstamp | `wss://ws.bitstamp.net` | `{"event":"bts:subscribe","data":{"channel":"diff_order_book_btcusd"}}` | **REST-seed** (`/api/v2/order_book/btcusd/`) + WS diff (NO WS snapshot) | `microtimestamp` (µs str) monotonic; drop diffs `<=` snapshot µs; `bts:request_reconnect` → resync | amount `"0"`; abs; `[price,amount]` |

---

## File Structure

```
PAVILOS/
├── src/pavilos/connectors/
│   ├── snapshot_delta_connector.py   # generic async loop for snapshot+delta venues [NEW]
│   ├── coinbase.py                   # CoinbaseFeed (process) [NEW]
│   ├── okx.py                        # OKXFeed (process) [NEW]
│   ├── bybit.py                      # BybitFeed (process) [NEW]
│   ├── bitstamp.py                   # BitstampDepthFeed (seed/apply) [NEW]
│   ├── bitstamp_connector.py         # BitstampConnector (REST-seed async) [NEW]
│   └── venues.py                     # build_connector(venue, symbol) registry + real transports [NEW]
└── tests/unit/
    ├── test_snapshot_delta_connector.py
    ├── test_coinbase.py
    ├── test_okx.py
    ├── test_bybit.py
    ├── test_bitstamp.py
    └── test_bitstamp_connector.py
```

**Responsibility per file:** each `*Feed` isolates one venue's parsing + sequence rules (pure, no I/O). `SnapshotDeltaConnector` is the shared transport for the 3 snapshot+delta venues. `BitstampConnector` is the REST-seed transport for Bitstamp. `venues.py` builds ready-to-run connectors with the real WS/REST transports wired (live-smoke).

---

## Task 1: Generic SnapshotDeltaConnector

**Files:**
- Create: `src/pavilos/connectors/snapshot_delta_connector.py`
- Test: `tests/unit/test_snapshot_delta_connector.py`

> Generic async loop for venues whose WS sends a full snapshot then deltas. It
> is parameterized by `make_feed` (a zero-arg factory returning a feed with
> `process(msg, *, ts) -> BookUpdate | None`) and an injected `connect`. Per
> frame: `out = feed.process(msg, ts=now())`; `None` → skip; `BookUpdate` → emit;
> `ResyncRequired` → reconnect with a FRESH feed. Stop-aware backoff + logging +
> health, mirroring the M1c connectors.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_snapshot_delta_connector.py`:**

```python
# tests/unit/test_snapshot_delta_connector.py
import asyncio

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.snapshot_delta_connector import SnapshotDeltaConnector


def _run(coro):
    return asyncio.run(coro)


class _FakeFeed:
    """process(): non-dict-with-'x' -> None skip; {'gap':True} -> ResyncRequired;
    else -> a BookUpdate echoing the frame."""
    def __init__(self):
        self.seen = 0

    def process(self, msg, *, ts):
        if msg.get("skip"):
            return None
        if msg.get("gap"):
            raise ResyncRequired("boom")
        self.seen += 1
        return BookUpdate(exchange="fake", ts=ts, bids=((msg["p"], msg["s"]),), asks=(),
                          is_snapshot=msg.get("snap", False), seq=msg.get("seq"))


def test_emits_skips_and_reconnects_on_resync():
    sessions = [
        [{"skip": True}, {"p": 100.0, "s": 1.0, "snap": True, "seq": 1}, {"gap": True}],
        [{"p": 100.0, "s": 2.0, "snap": True, "seq": 9}],
    ]
    calls = {"n": 0}

    async def fake_connect():
        i = calls["n"]; calls["n"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = SnapshotDeltaConnector("fake", _FakeFeed, connect=fake_connect, now=lambda: 1.0,
                                      sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)   # session1 snapshot (skip dropped)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)   # session2 snapshot after resync
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u1, u2, calls["n"], conn.health()

    u1, u2, n, h = _run(scenario())
    assert u1.is_snapshot is True and u1.bids == ((100.0, 1.0),)
    assert u2.is_snapshot is True and u2.bids == ((100.0, 2.0),)
    assert n >= 2 and h.resyncs >= 1


def test_connect_failure_increments_errors():
    calls = {"n": 0}

    async def failing_connect():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("down")

        async def gen():
            yield {"p": 100.0, "s": 1.0, "snap": True, "seq": 1}
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = SnapshotDeltaConnector("fake", _FakeFeed, connect=failing_connect, now=lambda: 1.0,
                                      sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u, conn.health()

    u, h = _run(scenario())
    assert u.is_snapshot is True
    assert h.errors >= 1
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_snapshot_delta_connector.py -v`** — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/snapshot_delta_connector.py`:**

```python
# src/pavilos/connectors/snapshot_delta_connector.py
"""Generic async connector for full-snapshot-WS + deltas venues (Coinbase, OKX,
Bybit). Parameterized by a per-venue feed; transport (`connect`) injected."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired

_log = logging.getLogger(__name__)


class SnapshotDeltaConnector:
    """Streams a venue's snapshot+delta frames through a per-venue feed into
    ``BookUpdate``s. ``make_feed`` is a zero-arg factory; a FRESH feed is created
    per connection (so resync starts clean). ``connect`` returns a live async
    iterator of decoded dict frames."""

    def __init__(
        self,
        exchange: str,
        make_feed: Callable[[], object],
        *,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]],
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
    ) -> None:
        self.exchange = exchange
        self._make_feed = make_feed
        self._connect = connect
        self._now = now or _wall_now
        self._sleep = sleep or asyncio.sleep
        self._max_backoff = max_backoff
        self._resyncs = 0
        self._errors = 0
        self._last_update_ts = 0.0
        self._connected = False

    def health(self) -> ConnectorHealth:
        return ConnectorHealth(self.exchange, self._connected, self._last_update_ts, self._resyncs, self._errors)

    async def run(self, out_q: "asyncio.Queue[BookUpdate]", stop: "asyncio.Event") -> None:
        backoff = 1.0
        while not stop.is_set():
            feed = self._make_feed()
            try:
                stream = await self._connect()
                self._connected = True
                async for msg in stream:
                    if stop.is_set():
                        break
                    out = feed.process(msg, ts=self._now())  # None=skip; raises ResyncRequired on gap
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
                _log.exception("%s connector error; will reconnect", self.exchange)
            finally:
                self._connected = False
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


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_snapshot_delta_connector.py -v`** — expect PASS (2 passed).

- [ ] **Step 5: Full suite `python -m pytest`** — expect 62 passed (60 + 2).

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/snapshot_delta_connector.py tests/unit/test_snapshot_delta_connector.py
git commit -m "feat(connectors): add generic SnapshotDeltaConnector (feed-parameterized)"
```

---

## Task 2: CoinbaseFeed

**Files:**
- Create: `src/pavilos/connectors/coinbase.py`
- Test: `tests/unit/test_coinbase.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_coinbase.py`:**

```python
# tests/unit/test_coinbase.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.coinbase import CoinbaseFeed


def _msg(mtype, seq, updates):
    return {"channel": "l2_data", "sequence_num": seq,
            "events": [{"type": mtype, "product_id": "BTC-USD", "updates": updates}]}


def _u(side, price, qty):
    return {"side": side, "event_time": "2023-02-09T20:32:50Z", "price_level": price, "new_quantity": qty}


def test_skips_non_l2_data_frames():
    feed = CoinbaseFeed()
    assert feed.process({"channel": "subscriptions"}, ts=1.0) is None
    assert feed.process({"channel": "heartbeats", "heartbeat_counter": 1}, ts=1.0) is None


def test_snapshot_then_update_with_offer_and_removal():
    feed = CoinbaseFeed()
    snap = feed.process(_msg("snapshot", 0, [_u("bid", "100.0", "1.0"), _u("offer", "101.0", "2.0")]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "coinbase" and snap.is_snapshot is True and snap.ts == 5.0 and snap.seq == 0
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)  # 'offer' -> ask
    upd = feed.process(_msg("update", 1, [_u("bid", "100.0", "0")]), ts=6.0)  # removal
    assert upd.is_snapshot is False and upd.seq == 1
    assert upd.bids == ((100.0, 0.0),) and upd.asks == ()


def test_sequence_gap_raises_resync():
    feed = CoinbaseFeed()
    feed.process(_msg("snapshot", 10, [_u("bid", "100.0", "1.0")]), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 12, [_u("bid", "100.0", "2.0")]), ts=2.0)  # 12 > 10+1


def test_duplicate_or_out_of_order_sequence_ignored():
    feed = CoinbaseFeed()
    feed.process(_msg("snapshot", 10, [_u("bid", "100.0", "1.0")]), ts=1.0)
    assert feed.process(_msg("update", 9, [_u("bid", "1.0", "1.0")]), ts=2.0) is None   # <= last
    upd = feed.process(_msg("update", 11, [_u("bid", "100.0", "2.0")]), ts=3.0)         # contiguous
    assert upd is not None and upd.seq == 11
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_coinbase.py -v` — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/coinbase.py`:**

```python
# src/pavilos/connectors/coinbase.py
"""Coinbase Advanced Trade level2 sequencer (pure). Messages arrive as
channel 'l2_data'; integrity is per-product sequence_num (+1 exact)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


class CoinbaseFeed:
    """Turns Coinbase ``l2_data`` frames into ``BookUpdate``s. ``new_quantity`` is
    absolute (``"0"`` removes; passed through for BookState to drop). ``side`` is
    ``bid``/``offer`` (offer -> ask). Gap on ``sequence_num`` raises ResyncRequired;
    a lower/equal sequence_num is ignored (duplicate / out-of-order)."""

    def __init__(self, exchange: str = "coinbase") -> None:
        self.exchange = exchange
        self._last_seq: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if msg.get("channel") != "l2_data":
            return None  # subscriptions / heartbeats / other
        seq = msg.get("sequence_num")
        if seq is not None and self._last_seq is not None:
            if seq <= self._last_seq:
                return None  # duplicate / out-of-order
            if seq > self._last_seq + 1:
                raise ResyncRequired(f"coinbase sequence gap: {seq} > {self._last_seq}+1")
        is_snapshot = False
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        for event in msg.get("events", []):
            if event.get("type") == "snapshot":
                is_snapshot = True
            for upd in event.get("updates", []):
                price = float(upd["price_level"])
                size = float(upd["new_quantity"])
                if upd["side"] == "bid":
                    bids.append((price, size))
                else:  # "offer"
                    asks.append((price, size))
        if seq is not None:
            self._last_seq = seq
        return BookUpdate(exchange=self.exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                          is_snapshot=is_snapshot, seq=seq)
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_coinbase.py -v` — expect PASS (4 passed).

- [ ] **Step 5: Full suite** `python -m pytest` — expect 66 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/coinbase.py tests/unit/test_coinbase.py
git commit -m "feat(connectors): add CoinbaseFeed (l2_data sequencer)"
```

---

## Task 3: OKXFeed

**Files:**
- Create: `src/pavilos/connectors/okx.py`
- Test: `tests/unit/test_okx.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_okx.py`:**

```python
# tests/unit/test_okx.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.okx import OKXFeed


def _msg(action, seq_id, prev, bids, asks):
    def lv(rows):
        return [[p, s, "0", "1"] for p, s in rows]
    return {"arg": {"channel": "books", "instId": "BTC-USDT"}, "action": action,
            "data": [{"asks": lv(asks), "bids": lv(bids), "ts": "1700000000000",
                      "checksum": 0, "prevSeqId": prev, "seqId": seq_id}]}


def test_skips_non_books_frames():
    feed = OKXFeed()
    assert feed.process({"event": "subscribe", "arg": {"channel": "books"}}, ts=1.0) is None
    assert feed.process({"arg": {"channel": "tickers"}, "data": []}, ts=1.0) is None


def test_snapshot_then_contiguous_update_with_removal():
    feed = OKXFeed()
    snap = feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], [("101.0", "2.0")]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "okx" and snap.is_snapshot is True and snap.seq == 100
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)
    upd = feed.process(_msg("update", 101, 100, [("100.0", "0")], []), ts=6.0)  # prevSeqId==last
    assert upd.is_snapshot is False and upd.seq == 101
    assert upd.bids == ((100.0, 0.0),)  # size "0" removal passed through


def test_seqid_gap_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 105, 104, [("100.0", "2.0")], []), ts=2.0)  # prev 104 != last 100


def test_seqid_equal_prev_is_benign_noop():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    upd = feed.process(_msg("update", 100, 100, [], []), ts=2.0)  # seqId==prevSeqId resend
    assert upd is not None and upd.seq == 100


def test_seqid_reset_raises_resync():
    feed = OKXFeed()
    feed.process(_msg("snapshot", 100, -1, [("100.0", "1.0")], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("update", 5, 100, [("100.0", "2.0")], []), ts=2.0)  # seqId<prevSeqId reset
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_okx.py -v` — expect FAIL.

- [ ] **Step 3: Implement — create `src/pavilos/connectors/okx.py`:**

```python
# src/pavilos/connectors/okx.py
"""OKX v5 ``books`` channel sequencer (pure). Integrity is seqId/prevSeqId
continuity (CRC32 is being deprecated to 0 on 2026-06-23, so seqId is primary)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    # each row is [price, size, deprecated, num_orders]; size "0" removes (abs)
    return tuple((float(r[0]), float(r[1])) for r in rows)


class OKXFeed:
    """Turns OKX ``books`` frames into ``BookUpdate``s. A snapshot (action
    'snapshot', prevSeqId == -1) resets; an update is valid iff its prevSeqId
    equals the last seqId. A gap (prevSeqId mismatch) or a reset (seqId <
    prevSeqId) raises ResyncRequired. seqId == prevSeqId is a benign no-op."""

    def __init__(self, exchange: str = "okx") -> None:
        self.exchange = exchange
        self._last_seq: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if msg.get("arg", {}).get("channel") != "books" or "action" not in msg or not msg.get("data"):
            return None  # subscribe ack / event / other channel
        action = msg["action"]
        data = msg["data"][0]
        seq_id = data.get("seqId")
        prev = data.get("prevSeqId")
        is_snapshot = action == "snapshot"
        if not is_snapshot:
            if seq_id is not None and prev is not None and seq_id < prev:
                raise ResyncRequired(f"okx seqId reset: seqId={seq_id} < prevSeqId={prev}")
            if self._last_seq is not None and prev is not None and prev != -1 and prev != self._last_seq:
                raise ResyncRequired(f"okx seqId gap: prevSeqId={prev} != last={self._last_seq}")
        if seq_id is not None:
            self._last_seq = seq_id
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data.get("bids", [])),
                          asks=_levels(data.get("asks", [])), is_snapshot=is_snapshot, seq=seq_id)
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_okx.py -v` — expect PASS (5 passed).

- [ ] **Step 5: Full suite** `python -m pytest` — expect 71 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/okx.py tests/unit/test_okx.py
git commit -m "feat(connectors): add OKXFeed (books seqId/prevSeqId sequencer)"
```

---

## Task 4: BybitFeed

**Files:**
- Create: `src/pavilos/connectors/bybit.py`
- Test: `tests/unit/test_bybit.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_bybit.py`:**

```python
# tests/unit/test_bybit.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.bybit import BybitFeed


def _msg(mtype, u, b, a, seq=1):
    return {"topic": "orderbook.200.BTCUSDT", "type": mtype, "ts": 1700000000000,
            "data": {"s": "BTCUSDT", "b": b, "a": a, "u": u, "seq": seq}}


def test_skips_non_data_frames():
    feed = BybitFeed()
    assert feed.process({"op": "subscribe", "success": True}, ts=1.0) is None
    assert feed.process({"op": "ping", "ret_msg": "pong"}, ts=1.0) is None


def test_snapshot_then_contiguous_delta_with_removal():
    feed = BybitFeed()
    snap = feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "bybit" and snap.is_snapshot is True and snap.seq == 100
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)
    upd = feed.process(_msg("delta", 101, [["100.0", "0"]], []), ts=6.0)  # u==last+1
    assert upd.is_snapshot is False and upd.seq == 101 and upd.bids == ((100.0, 0.0),)


def test_u_gap_raises_resync():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    with pytest.raises(ResyncRequired):
        feed.process(_msg("delta", 103, [["100.0", "2.0"]], []), ts=2.0)  # 103 != 100+1


def test_u_equals_one_is_reset_snapshot():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    # service restart: u==1 must be treated as a fresh snapshot, NOT a gap
    out = feed.process(_msg("delta", 1, [["100.0", "5.0"]], []), ts=2.0)
    assert out.is_snapshot is True and out.seq == 1 and out.bids == ((100.0, 5.0),)


def test_mid_stream_snapshot_resets():
    feed = BybitFeed()
    feed.process(_msg("snapshot", 100, [["100.0", "1.0"]], []), ts=1.0)
    out = feed.process(_msg("snapshot", 200, [["100.0", "9.0"]], []), ts=2.0)  # re-sent snapshot
    assert out.is_snapshot is True and out.seq == 200
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_bybit.py -v` — expect FAIL.

- [ ] **Step 3: Implement — create `src/pavilos/connectors/bybit.py`:**

```python
# src/pavilos/connectors/bybit.py
"""Bybit v5 spot orderbook sequencer (pure). Integrity is data.u continuity;
type 'snapshot' or u==1 RESETS the book; no checksum."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(p), float(s)) for p, s in rows)


class BybitFeed:
    """Turns Bybit ``orderbook.*`` frames into ``BookUpdate``s. A ``type:"snapshot"``
    OR ``u == 1`` (service restart) resets the book; a ``delta`` is valid iff
    ``u == last_u + 1``. A non-consecutive ``u`` raises ResyncRequired. ``seq`` is
    NOT used for continuity. Sizes absolute; ``"0"`` removes."""

    def __init__(self, exchange: str = "bybit") -> None:
        self.exchange = exchange
        self._last_u: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if "topic" not in msg or "type" not in msg or "data" not in msg:
            return None  # op/pong/subscribe ack
        data = msg["data"]
        u = data.get("u")
        is_snapshot = msg["type"] == "snapshot" or u == 1
        if not is_snapshot and self._last_u is not None and u is not None and u != self._last_u + 1:
            raise ResyncRequired(f"bybit u gap: {u} != {self._last_u}+1")
        if u is not None:
            self._last_u = u
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data.get("b", [])),
                          asks=_levels(data.get("a", [])), is_snapshot=is_snapshot, seq=u)
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_bybit.py -v` — expect PASS (5 passed).

- [ ] **Step 5: Full suite** `python -m pytest` — expect 76 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/bybit.py tests/unit/test_bybit.py
git commit -m "feat(connectors): add BybitFeed (orderbook u-continuity sequencer)"
```

---

## Task 5: BitstampDepthFeed (REST-seed + microtimestamp)

**Files:**
- Create: `src/pavilos/connectors/bitstamp.py`
- Test: `tests/unit/test_bitstamp.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_bitstamp.py`:**

```python
# tests/unit/test_bitstamp.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.bitstamp import BitstampDepthFeed


def _snapshot(micro, bids, asks):
    return {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}


def _diff(micro, bids, asks):
    return {"event": "data", "channel": "diff_order_book_btcusd",
            "data": {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}}


def test_seed_emits_snapshot_and_sets_watermark():
    feed = BitstampDepthFeed("btcusd")
    snap = feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "bitstamp" and snap.is_snapshot is True and snap.ts == 5.0
    assert snap.bids == ((100.0, 1.0),) and snap.asks == ((101.0, 2.0),)


def test_apply_drops_stale_then_applies_with_removal():
    feed = BitstampDepthFeed("btcusd")
    feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert feed.apply(_diff(1000, [["1.0", "1.0"]], []), ts=6.0) is None   # micro <= watermark
    assert feed.apply(_diff(999, [["1.0", "1.0"]], []), ts=6.0) is None    # older still dropped
    upd = feed.apply(_diff(2000, [["100.0", "0"]], [["101.5", "3.0"]]), ts=7.0)
    assert upd.is_snapshot is False and upd.bids == ((100.0, 0.0),) and upd.asks == ((101.5, 3.0),)


def test_apply_before_seed_raises():
    feed = BitstampDepthFeed("btcusd")
    with pytest.raises(ResyncRequired):
        feed.apply(_diff(1000, [], []), ts=1.0)


def test_non_data_event_ignored():
    feed = BitstampDepthFeed("btcusd")
    feed.seed(_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert feed.apply({"event": "bts:subscription_succeeded", "channel": "diff_order_book_btcusd", "data": {}}, ts=6.0) is None
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_bitstamp.py -v` — expect FAIL.

- [ ] **Step 3: Implement — create `src/pavilos/connectors/bitstamp.py`:**

```python
# src/pavilos/connectors/bitstamp.py
"""Bitstamp diff_order_book sequencer (pure). No WS snapshot: seed from REST,
then reconcile diffs by microtimestamp (microseconds). No checksum/seq id."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(p), float(a)) for p, a in rows)


class BitstampDepthFeed:
    """Seeds from a REST order_book snapshot, then applies WS diffs, dropping any
    diff whose ``microtimestamp`` is <= the current watermark. Sizes absolute;
    amount ``"0"`` removes. Gaps cannot be detected from the microtimestamp alone,
    so the transport drives resync on ``bts:request_reconnect`` / crossed book."""

    def __init__(self, symbol: str = "btcusd", *, exchange: str = "bitstamp") -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._watermark: int | None = None

    def seed(self, snapshot: dict, *, ts: float) -> BookUpdate:
        self._watermark = int(snapshot["microtimestamp"])
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(snapshot["bids"]),
                          asks=_levels(snapshot["asks"]), is_snapshot=True, seq=None)

    def apply(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if self._watermark is None:
            raise ResyncRequired("bitstamp: apply before seed")
        if msg.get("event") != "data":
            return None  # bts:subscription_succeeded / other control events
        data = msg["data"]
        micro = int(data["microtimestamp"])
        if micro <= self._watermark:
            return None  # already covered by the snapshot / out-of-order
        self._watermark = micro
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data["bids"]),
                          asks=_levels(data["asks"]), is_snapshot=False, seq=None)
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_bitstamp.py -v` — expect PASS (4 passed).

- [ ] **Step 5: Full suite** `python -m pytest` — expect 80 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/bitstamp.py tests/unit/test_bitstamp.py
git commit -m "feat(connectors): add BitstampDepthFeed (REST-seed + microtimestamp)"
```

---

## Task 6: BitstampConnector (REST-seed async)

**Files:**
- Create: `src/pavilos/connectors/bitstamp_connector.py`
- Test: `tests/unit/test_bitstamp_connector.py`

> Mirrors `BinanceConnector` (open stream → buffer → fetch REST → seed → apply),
> plus: a `bts:request_reconnect` control frame forces a resync, and the WS
> subscribe is sent on connect. Injected `connect`/`fetch_snapshot` for tests.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_bitstamp_connector.py`:**

```python
# tests/unit/test_bitstamp_connector.py
import asyncio

from pavilos.connectors.bitstamp_connector import BitstampConnector


def _snapshot(micro, bids, asks):
    return {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}


def _diff(micro, bids, asks):
    return {"event": "data", "channel": "diff_order_book_btcusd",
            "data": {"timestamp": str(micro // 1_000_000), "microtimestamp": str(micro), "bids": bids, "asks": asks}}


def _run(coro):
    return asyncio.run(coro)


def test_seeds_then_applies_diffs():
    diffs = [_diff(2000, [["100.0", "1.5"]], []), _diff(3000, [], [["101.0", "0"]])]

    async def fake_connect():
        async def gen():
            for d in diffs:
                yield d
        return gen()

    async def fake_fetch():
        return _snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]])

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BitstampConnector("btcusd", connect=fake_connect, fetch_snapshot=fake_fetch,
                                 now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap, u1, u2

    snap, u1, u2 = _run(scenario())
    assert snap.is_snapshot is True and snap.exchange == "bitstamp"
    assert u1.is_snapshot is False and u1.bids == ((100.0, 1.5),)
    assert u2.asks == ((101.0, 0.0),)


def test_request_reconnect_triggers_reseed():
    sessions = [
        [{"event": "bts:request_reconnect", "channel": "", "data": {}}],   # forces resync
        [_diff(2000, [["100.0", "1.5"]], [])],
    ]
    snaps = [_snapshot(1000, [["100.0", "1.0"]], [["101.0", "2.0"]]),
             _snapshot(1500, [["100.0", "1.0"]], [["101.0", "2.0"]])]
    calls = {"c": 0, "s": 0}

    async def fake_connect():
        i = calls["c"]; calls["c"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def fake_fetch():
        i = calls["s"]; calls["s"] += 1
        return snaps[min(i, len(snaps) - 1)]

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = BitstampConnector("btcusd", connect=fake_connect, fetch_snapshot=fake_fetch,
                                 now=lambda: 5.0, sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        seqs = []
        for _ in range(3):
            u = await asyncio.wait_for(out_q.get(), timeout=1.0)
            seqs.append((u.is_snapshot, u.bids))
            if u.bids == ((100.0, 1.5),):
                break
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return calls["c"]

    c = _run(scenario())
    assert c >= 2   # reconnected after bts:request_reconnect
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_bitstamp_connector.py -v` — expect FAIL.

- [ ] **Step 3: Implement — create `src/pavilos/connectors/bitstamp_connector.py`:**

```python
# src/pavilos/connectors/bitstamp_connector.py
"""Async Bitstamp connector: REST seed + diff stream -> BookUpdates. Handles
bts:request_reconnect (forces resync). Transport injected."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable

import aiohttp
import websockets

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ConnectorHealth, ResyncRequired
from pavilos.connectors.bitstamp import BitstampDepthFeed

BITSTAMP_WS_URL = "wss://ws.bitstamp.net"
BITSTAMP_REST_URL = "https://www.bitstamp.net/api/v2/order_book"

_log = logging.getLogger(__name__)


class BitstampConnector:
    """Seeds from REST then applies WS diffs via ``BitstampDepthFeed``. A
    ``bts:request_reconnect`` control frame (or any error) forces reconnect +
    re-seed with stop-aware backoff."""

    def __init__(
        self,
        symbol: str,
        *,
        url: str = BITSTAMP_WS_URL,
        rest_url: str = BITSTAMP_REST_URL,
        connect: Callable[[], Awaitable[AsyncIterator[dict]]] | None = None,
        fetch_snapshot: Callable[[], Awaitable[dict]] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        max_backoff: float = 30.0,
        proxy: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = "bitstamp"
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
            feed = BitstampDepthFeed(self.symbol, exchange=self.exchange)
            try:
                stream = await self._connect()          # open first so diffs buffer
                self._connected = True
                snapshot = await self._fetch_snapshot()
                snap = feed.seed(snapshot, ts=self._now())
                await out_q.put(snap)
                self._last_update_ts = snap.ts
                async for msg in stream:
                    if stop.is_set():
                        break
                    if msg.get("event") == "bts:request_reconnect":
                        raise ResyncRequired("bitstamp: server requested reconnect")
                    out = feed.apply(msg, ts=self._now())   # None if stale/control
                    if out is not None:
                        await out_q.put(out)
                        self._last_update_ts = out.ts
                    backoff = 1.0
            except ResyncRequired:
                self._resyncs += 1
            except Exception:
                self._errors += 1
                _log.exception("bitstamp connector error; will reconnect")
            finally:
                self._connected = False
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

    async def _default_connect(self) -> AsyncIterator[dict]:
        ws = await websockets.connect(self._url, proxy=self._proxy) if self._proxy else await websockets.connect(self._url)
        await ws.send(json.dumps({"event": "bts:subscribe",
                                  "data": {"channel": f"diff_order_book_{self.symbol}"}}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()

        return gen()

    async def _default_fetch_snapshot(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._rest_url}/{self.symbol}/", proxy=self._proxy) as resp:
                resp.raise_for_status()
                return await resp.json()


def _wall_now() -> float:
    import time
    return time.time()
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_bitstamp_connector.py -v` — expect PASS (2 passed).

- [ ] **Step 5: Full suite** `python -m pytest` — expect 82 passed.

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/bitstamp_connector.py tests/unit/test_bitstamp_connector.py
git commit -m "feat(connectors): add async BitstampConnector (REST-seed + request_reconnect)"
```

---

## Task 7: Venue registry + real transports + wire into live smoke

**Files:**
- Create: `src/pavilos/connectors/venues.py`
- Modify: `scripts/live_smoke.py` (use the registry for all 6 venues)
- Test: `tests/unit/test_venues.py`

> `venues.py` exposes `VENUE_SPECS` (the 6 Tier-A venues with quote/tier) and
> `build_connector(venue, symbol)` returning a ready connector with the real
> transport wired (Coinbase/OKX/Bybit via `SnapshotDeltaConnector` + their
> `_default_connect`; Kraken/Binance/Bitstamp via their existing connectors).
> The real `_default_connect` for the 3 snapshot+delta venues includes the
> venue's subscribe + app-level ping; these are live-smoke-only. The unit test
> only checks the registry wiring (no network).

- [ ] **Step 1: Write the failing test — create `tests/unit/test_venues.py`:**

```python
# tests/unit/test_venues.py
import pytest

from pavilos.core.models import Quote, Tier
from pavilos.connectors.venues import VENUE_SPECS, build_connector


def test_venue_specs_cover_six_tier_a():
    names = {s.exchange for s in VENUE_SPECS}
    assert names == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp"}
    assert all(s.tier is Tier.A for s in VENUE_SPECS)
    quotes = {s.exchange: s.quote for s in VENUE_SPECS}
    assert quotes["coinbase"] is Quote.USD and quotes["okx"] is Quote.USDT


def test_build_connector_returns_runnable_for_each_venue():
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd"}
    for venue, sym in symbols.items():
        conn = build_connector(venue, sym)
        assert conn.exchange == venue
        assert hasattr(conn, "run") and hasattr(conn, "health")


def test_build_connector_unknown_raises():
    with pytest.raises(KeyError):
        build_connector("ftx", "BTCUSDT")
```

- [ ] **Step 2: Run** `python -m pytest tests/unit/test_venues.py -v` — expect FAIL.

- [ ] **Step 3: Implement — create `src/pavilos/connectors/venues.py`:**

```python
# src/pavilos/connectors/venues.py
"""Venue registry: the six Tier-A venues + a factory that builds a ready
connector (real transport wired) for each. The real WS subscribes + app-level
pings here are live-smoke-only; unit tests check wiring, not connectivity."""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import websockets

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.connectors.kraken_connector import KrakenConnector
from pavilos.connectors.binance_connector import BinanceConnector
from pavilos.connectors.bitstamp_connector import BitstampConnector
from pavilos.connectors.snapshot_delta_connector import SnapshotDeltaConnector
from pavilos.connectors.coinbase import CoinbaseFeed
from pavilos.connectors.okx import OKXFeed
from pavilos.connectors.bybit import BybitFeed

VENUE_SPECS: tuple[VenueSpec, ...] = (
    VenueSpec("kraken", Quote.USD, Tier.A),
    VenueSpec("binance", Quote.USDT, Tier.A),
    VenueSpec("coinbase", Quote.USD, Tier.A),
    VenueSpec("okx", Quote.USDT, Tier.A),
    VenueSpec("bybit", Quote.USDT, Tier.A),
    VenueSpec("bitstamp", Quote.USD, Tier.A),
)


def _coinbase_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://advanced-trade-ws.coinbase.com")
        await ws.send(json.dumps({"type": "subscribe", "product_ids": [symbol], "channel": "level2"}))
        await ws.send(json.dumps({"type": "subscribe", "product_ids": [symbol], "channel": "heartbeats"}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def _okx_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://ws.okx.com:8443/ws/v5/public")
        await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    if raw == "pong":
                        continue
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def _bybit_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://stream.bybit.com/v5/public/spot")
        await ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.200.{symbol}"]}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def build_connector(venue: str, symbol: str):
    """Return a ready-to-run connector for ``venue`` (real transport wired)."""
    if venue == "kraken":
        return KrakenConnector(symbol, depth=25)
    if venue == "binance":
        return BinanceConnector(symbol)
    if venue == "bitstamp":
        return BitstampConnector(symbol)
    if venue == "coinbase":
        return SnapshotDeltaConnector("coinbase", CoinbaseFeed, connect=_coinbase_connect(symbol))
    if venue == "okx":
        return SnapshotDeltaConnector("okx", OKXFeed, connect=_okx_connect(symbol))
    if venue == "bybit":
        return SnapshotDeltaConnector("bybit", BybitFeed, connect=_bybit_connect(symbol))
    raise KeyError(f"unknown venue {venue!r}")
```

- [ ] **Step 4: Run** `python -m pytest tests/unit/test_venues.py -v` — expect PASS (3 passed).

- [ ] **Step 5: Update `scripts/live_smoke.py`** to use the registry for all 6 venues. Replace the `specs`/`connectors` construction in `main` with:

```python
    from pavilos.connectors.venues import VENUE_SPECS, build_connector
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd"}
    specs = list(VENUE_SPECS)
    agg = Aggregator(specs, PegProvider(), bin_bps=5.0, window_bps=50.0, staleness_s=15.0)
    connectors = [build_connector(v, symbols[v]) for v in symbols]
```
(Remove the now-unused direct `KrakenConnector`/`BinanceConnector` imports if they are no longer referenced; keep `Aggregator`/`PegProvider`/`Engine`/`VenueSpec` imports as needed. Run the import check `python -c "import scripts.live_smoke; print('ok')"`.)

- [ ] **Step 6: Full suite** `python -m pytest` — expect 85 passed (82 + 3) and the live_smoke import check prints `ok`.

- [ ] **Step 7: Commit**

```bash
git add src/pavilos/connectors/venues.py tests/unit/test_venues.py scripts/live_smoke.py
git commit -m "feat(connectors): add venue registry + wire all 6 venues into live smoke"
```

---

## Task 8: Full suite green + close-out

**Files:** none (verification only)

- [ ] **Step 1:** `python -m pytest -v` — expect ALL pass (85: 60 prior + 25 new — snapshot_delta 2, coinbase 4, okx 5, bybit 5, bitstamp 4, bitstamp_connector 2, venues 3).
- [ ] **Step 2:** `git status` — clean.
- [ ] **Step 3:** `git tag m1d-native-connectors && git log --oneline -10`.

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.1 — remaining venues):**
- Coinbase full-snapshot + sequence_num → Task 2 ✅; OKX books seqId/prevSeqId (CRC deprecation handled by using seqId primary) → Task 3 ✅; Bybit u-continuity + reset-on-snapshot/u==1 → Task 4 ✅; Bitstamp REST-seed + microtimestamp → Tasks 5/6 ✅.
- Reconnect/backoff (stop-aware) + logging + health → Task 1 (generic) + Task 6 (Bitstamp) ✅.
- All emit `pavilos.core.models.BookUpdate` that `Aggregator.apply` consumes; removal semantics (`"0"`) preserved for BookState ✅.
- Wired into Engine via `venues.build_connector` + live smoke covers all 6 ✅.
- *Deferred to M1e (correctly out of scope):* ccxt long-tail wrapper; live peg/FX updater (USDT/USD + KRW/JPY for Tier-B); per-venue heartbeat/ping TUNING in the real transports (basic ping omitted from the snapshot+delta `_default_connect` — OKX/Bybit idle-disconnect after 30s/20s; the live smoke runs <30s so it's tolerable, but M1e must add app-level ping tasks for long-lived runs); EEA endpoint switch for OKX (`wseea`) if region routing is wanted; replay-fixture regression tests from `capture.py`; migrating Kraken/Binance onto the generic connector.

**Placeholder scan:** none; every step has full runnable code. The real `_default_connect` transports are live-smoke-only (no unit test), consistent with M1c.

**Type consistency:** every feed exposes `process(msg, *, ts) -> BookUpdate | None` (snapshot+delta) or `seed`/`apply` (Bitstamp); `SnapshotDeltaConnector(exchange, make_feed, *, connect, now, sleep, max_backoff)`; all connectors expose `exchange` + `async run(out_q, stop)` + `health() -> ConnectorHealth` (Engine-compatible); `BookUpdate(exchange, ts, bids, asks, is_snapshot, seq)` used uniformly.

**Adversarial-test note (the THIRD barrier per task):** each feed task ships tests for the nasty cases — gap→resync, duplicate/out-of-order→ignore, reset (u==1 / seqId<prevSeqId / mid-stream snapshot)→reset, removal (`"0"`), and the skip path (acks/heartbeats/pong). The per-task adversarial reviewer should additionally try: a frame missing the seq field; a snapshot with prevSeqId != -1 (OKX); interleaved venues' frames; an empty `data`; a delta before any snapshot; and confirm no `BookUpdate` is emitted that would corrupt `BookState` (e.g. negative size).

**Ping caveat (live):** the snapshot+delta `_default_connect` functions in `venues.py` do NOT yet send app-level pings; OKX closes idle sockets after 30s and Bybit after 20s. The live smoke (default 15s) tolerates this, but **M1e must add a keepalive task** before any long-lived run. Documented here so it is not a silent gap.
