# PAVILOS M1b: Connector Logic (Kraken + Binance, network-free) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure, network-free logic that turns raw exchange order-book frames into validated `BookUpdate`s for the two reference exchanges — Kraken (full-snapshot WS + CRC32 checksum) and Binance (REST-seed + diff stream with U/u sequence continuity) — fully unit-tested with synthetic fixtures and a documented Kraken checksum vector. No sockets, no HTTP.

**Architecture:** Connectors split into two layers. This plan is the **logic** layer: pure functions/classes that accept already-received frames (parsed dicts) and produce `BookUpdate`s + integrity verdicts, with zero I/O. The **transport** layer (async websockets/aiohttp clients, reconnection, the live capture/smoke harness, wiring into `Aggregator.run`) is the next plan (M1c) and depends on this one. Keeping the protocol logic pure makes the trickiest correctness rules — Kraken's CRC32 and Binance's sequence continuity — deterministically testable before any flaky network is involved.

**Tech Stack:** Python 3.13, stdlib only (`zlib`, `decimal`, `dataclasses`, `enum`). No new runtime dependencies (websockets/aiohttp are added in M1c). `pytest` with the existing `pythonpath=["src"]` config. Builds on the merged M1-core engine (`pavilos.core.models.BookUpdate`, `pavilos.aggregator.book_state.BookState`).

---

## Protocol facts this plan encodes (verified 2026-06-06, official docs)

**Kraken Spot WS v2 `book` channel:**
- Messages: `{channel:"book", type:"snapshot"|"update", data:[{symbol, bids:[{price,qty}], asks:[{price,qty}], checksum:int, timestamp}]}`. `price`/`qty` are JSON numbers at FULL precision — must be read without float rounding (we read them as strings/Decimal).
- `qty == 0` removes a level. After each update, truncate each side to the subscribed depth.
- **CRC32:** over the **top 10 asks (price low→high) then top 10 bids (price high→low)**. Per level: remove the decimal point from `price`, strip leading zeros; same for `qty`; append `price+qty`. CRC32 over the ASCII bytes, cast unsigned 32-bit (`& 0xFFFFFFFF`).
- **Documented test vector:** the combined string below → checksum `3310070434`.

**Binance Spot diff. depth stream (`<symbol>@depth@100ms`):**
- Event: `{e:"depthUpdate", E, s, U (first update id), u (final update id), b:[[price,qty]], a:[[price,qty]]}` — strings, absolute qty, `"0"` removes. **No `pu` field on spot** (that is futures-only).
- REST seed: `GET /api/v3/depth?symbol=&limit=5000` → `{lastUpdateId, bids, asks}`.
- Procedure: buffer events → fetch snapshot → drop events with `u <= lastUpdateId` → first applied event must straddle (`U <= lastUpdateId+1 <= u`) → set `localUpdateId = lastUpdateId` → apply (qty 0 removes), then `localUpdateId = u`.
- **Continuity (spot):** `event.U == previous.u + 1`. **Gap → resync** when `event.U > localUpdateId + 1`. Stale (`event.u <= localUpdateId`) → ignore.

---

## File Structure

```
PAVILOS/
├── src/pavilos/connectors/
│   ├── __init__.py
│   ├── base.py            # ResyncRequired exception + ConnectorHealth dataclass
│   ├── kraken.py          # _fmt, _crc32, book_checksum, parse_kraken_message
│   └── binance.py         # BinanceDepthFeed (seed + apply: continuity, gap, removals)
└── tests/unit/
    ├── test_connectors_base.py
    ├── test_kraken.py
    └── test_binance.py
```

**Responsibility per file:**
- `base.py` — shared connector vocabulary: the `ResyncRequired` signal (book out of sync → caller must re-seed) and a `ConnectorHealth` snapshot dataclass. No logic.
- `kraken.py` — Kraken-specific PURE functions: checksum formatting + CRC32 (validated against Kraken's documented vector) and a frame→`BookUpdate` parser. No state, no I/O.
- `binance.py` — `BinanceDepthFeed`: stateful (tracks only `last_update_id`) sequencer that turns a REST snapshot + diff events into `BookUpdate`s, enforcing the documented continuity/gap rules. No I/O — the transport feeds it already-received dicts.

---

## Task 1: Connector base (ResyncRequired + ConnectorHealth)

**Files:**
- Create: `src/pavilos/connectors/__init__.py` (empty)
- Create: `src/pavilos/connectors/base.py`
- Test: `tests/unit/test_connectors_base.py`

- [ ] **Step 1: Create `src/pavilos/connectors/__init__.py`** as an empty file.

- [ ] **Step 2: Write the failing test — create `tests/unit/test_connectors_base.py`:**

```python
# tests/unit/test_connectors_base.py
import pytest

from pavilos.connectors.base import ResyncRequired, ConnectorHealth


def test_resync_required_is_exception_with_message():
    with pytest.raises(ResyncRequired) as exc:
        raise ResyncRequired("gap at seq 5")
    assert "gap at seq 5" in str(exc.value)


def test_connector_health_fields():
    h = ConnectorHealth(exchange="kraken", connected=True, last_update_ts=12.5, resyncs=1, errors=0)
    assert h.exchange == "kraken"
    assert h.connected is True
    assert h.last_update_ts == 12.5
    assert h.resyncs == 1
    assert h.errors == 0
```

- [ ] **Step 3: Run `python -m pytest tests/unit/test_connectors_base.py -v`** — expect FAIL (`ModuleNotFoundError: No module named 'pavilos.connectors.base'`).

- [ ] **Step 4: Implement — create `src/pavilos/connectors/base.py`:**

```python
# src/pavilos/connectors/base.py
"""Shared connector vocabulary: resync signal and health snapshot. No logic."""
from __future__ import annotations

from dataclasses import dataclass


class ResyncRequired(Exception):
    """Raised when a connector's local book is out of sync and the caller must
    discard it and re-seed (e.g. a Binance sequence gap, or a Kraken checksum
    mismatch). Transport code catches this and re-subscribes / re-fetches."""


@dataclass(slots=True, frozen=True)
class ConnectorHealth:
    """Point-in-time health of one connector, surfaced to monitoring/dashboard."""

    exchange: str
    connected: bool
    last_update_ts: float
    resyncs: int
    errors: int
```

- [ ] **Step 5: Run `python -m pytest tests/unit/test_connectors_base.py -v`** — expect PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add src/pavilos/connectors/__init__.py src/pavilos/connectors/base.py tests/unit/test_connectors_base.py
git commit -m "feat(connectors): add ResyncRequired and ConnectorHealth base types"
```

---

## Task 2: Kraken checksum (formatting + CRC32)

**Files:**
- Create: `src/pavilos/connectors/kraken.py`
- Test: `tests/unit/test_kraken.py`

- [ ] **Step 1: Write the failing test — create `tests/unit/test_kraken.py`:**

```python
# tests/unit/test_kraken.py
import zlib

from pavilos.connectors.kraken import _fmt, _crc32, book_checksum

# Kraken's official worked example (docs.kraken.com spot-ws-book-v2):
# the concatenated top-10 asks (low->high) + top-10 bids (high->low) string
# CRC32s (unsigned 32-bit) to this value.
DOC_COMBINED = (
    "45285210000045286415457195345286615457110945289615456091145290215890660"
    "452918154553491452947445474945296135380000452975994554245299518772827"
    "452835100000004528341545820154528211000000045281010000000452803154592586"
    "452790799000045277633101034527753000000045277315460273745276615445238"
)
DOC_CHECKSUM = 3310070434


def test_fmt_removes_dot_and_strips_leading_zeros():
    assert _fmt("45283.5") == "452835"
    assert _fmt("0.00100000") == "100000"
    assert _fmt("0.5666") == "5666"
    assert _fmt("100.00") == "10000"


def test_crc32_matches_kraken_documented_vector():
    assert _crc32(DOC_COMBINED) == DOC_CHECKSUM


def test_book_checksum_assembles_asks_then_bids_top10_formatted():
    # asks pre-sorted low->high, bids pre-sorted high->low; (price, qty) strings.
    asks = [("100.5", "2.0"), ("101.0", "0.5")]
    bids = [("100.0", "1.5"), ("99.5", "3.0")]
    # expected string: for each ask then each bid, _fmt(price)+_fmt(qty)
    #   100.5/2.0 -> "1005"+"20"="100520"; 101.0/0.5 -> "1010"+"5"="10105"
    #   100.0/1.5 -> "1000"+"15"="100015"; 99.5/3.0 -> "995"+"30"="99530"
    expected_str = "100520" + "10105" + "100015" + "99530"
    expected = zlib.crc32(expected_str.encode("ascii")) & 0xFFFFFFFF
    assert book_checksum(asks, bids) == expected


def test_book_checksum_uses_only_top_10_each_side():
    # 12 asks and 12 bids; only the first 10 of each (already sorted) must count.
    asks = [(f"{100 + i}.0", "1.0") for i in range(12)]
    bids = [(f"{99 - i}.0", "1.0") for i in range(12)]
    full = book_checksum(asks, bids)
    trimmed = book_checksum(asks[:10], bids[:10])
    assert full == trimmed
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_kraken.py -v`** — expect FAIL (`ModuleNotFoundError: No module named 'pavilos.connectors.kraken'`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/kraken.py`:**

```python
# src/pavilos/connectors/kraken.py
"""Kraken Spot WS v2 `book` channel: pure checksum + frame parsing. No I/O."""
from __future__ import annotations

import zlib


def _fmt(value: str) -> str:
    """Kraken checksum formatting for one price or qty string: remove the decimal
    point and strip leading zeros (e.g. '0.00100000' -> '100000'). Returns '0'
    for an all-zero result (defensive; removed levels never reach here)."""
    return value.replace(".", "").lstrip("0") or "0"


def _crc32(s: str) -> int:
    """CRC32 of the ASCII bytes of ``s``, cast to unsigned 32-bit (Kraken's cast)."""
    return zlib.crc32(s.encode("ascii")) & 0xFFFFFFFF


def book_checksum(asks: list[tuple[str, str]], bids: list[tuple[str, str]]) -> int:
    """Kraken v2 book CRC32 over the top-10 asks (price low->high) then top-10
    bids (price high->low). Each side must already be sorted in that order;
    only the first 10 of each are used. ``asks``/``bids`` are (price, qty)
    strings at full wire precision."""
    parts: list[str] = []
    for price, qty in asks[:10]:
        parts.append(_fmt(price) + _fmt(qty))
    for price, qty in bids[:10]:
        parts.append(_fmt(price) + _fmt(qty))
    return _crc32("".join(parts))
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_kraken.py -v`** — expect PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/connectors/kraken.py tests/unit/test_kraken.py
git commit -m "feat(connectors): add Kraken v2 book CRC32 checksum (verified vs docs vector)"
```

---

## Task 3: Kraken frame parser → BookUpdate

**Files:**
- Modify: `src/pavilos/connectors/kraken.py` (add `parse_kraken_message`)
- Test: `tests/unit/test_kraken.py` (add tests)

> The parser turns a decoded Kraken `book` message dict into a `BookUpdate`
> (prices/qtys as floats for the aggregator). `type:"snapshot"` →
> `is_snapshot=True`; `type:"update"` → `is_snapshot=False`. The Kraken book
> channel carries no per-message sequence number, so `seq=None` (integrity is
> the CRC32, handled by the transport layer in M1c). A receive timestamp ``ts``
> is injected by the caller (the wire `timestamp` is the exchange clock; the
> transport passes local receive time).

- [ ] **Step 1: Add the failing tests to `tests/unit/test_kraken.py`:**

```python
from pavilos.core.models import BookUpdate
from pavilos.connectors.kraken import parse_kraken_message


def _kraken_msg(mtype, bids, asks, checksum=0):
    return {
        "channel": "book",
        "type": mtype,
        "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": p, "qty": q} for p, q in bids],
            "asks": [{"price": p, "qty": q} for p, q in asks],
            "checksum": checksum,
            "timestamp": "2023-10-06T17:35:55.440295Z",
        }],
    }


def test_parse_snapshot_message():
    msg = _kraken_msg("snapshot", bids=[(100.0, 1.0), (99.0, 2.0)], asks=[(101.0, 1.5)])
    u = parse_kraken_message(msg, ts=5.0)
    assert isinstance(u, BookUpdate)
    assert u.exchange == "kraken"
    assert u.is_snapshot is True
    assert u.ts == 5.0
    assert u.seq is None
    assert u.bids == ((100.0, 1.0), (99.0, 2.0))
    assert u.asks == ((101.0, 1.5),)


def test_parse_update_message_with_removal():
    msg = _kraken_msg("update", bids=[(100.0, 0.0)], asks=[(101.5, 2.0)])
    u = parse_kraken_message(msg, ts=6.0)
    assert u.is_snapshot is False
    assert u.bids == ((100.0, 0.0),)   # qty 0 preserved; BookState removes on apply
    assert u.asks == ((101.5, 2.0),)
```

- [ ] **Step 2: Run the two new tests** — `python -m pytest tests/unit/test_kraken.py -k parse -v` — expect FAIL (`cannot import name 'parse_kraken_message'`).

- [ ] **Step 3: Add to `src/pavilos/connectors/kraken.py`** (new import + function):

At the top, update imports to:
```python
from __future__ import annotations

import zlib

from pavilos.core.models import BookUpdate
```

Append the function:
```python
def parse_kraken_message(msg: dict, *, ts: float, exchange: str = "kraken") -> BookUpdate:
    """Convert a decoded Kraken v2 ``book`` message into a ``BookUpdate``.

    ``type:"snapshot"`` -> ``is_snapshot=True``; ``"update"`` -> ``False``.
    Levels are taken from ``data[0]`` and converted to float (price, qty) tuples;
    ``qty == 0`` levels are preserved verbatim (``BookState`` removes them on
    apply). The book channel has no sequence number, so ``seq`` is ``None`` —
    integrity is verified separately via the CRC32 checksum."""
    data = msg["data"][0]
    bids = tuple((float(lvl["price"]), float(lvl["qty"])) for lvl in data["bids"])
    asks = tuple((float(lvl["price"]), float(lvl["qty"])) for lvl in data["asks"])
    return BookUpdate(
        exchange=exchange,
        ts=ts,
        bids=bids,
        asks=asks,
        is_snapshot=(msg["type"] == "snapshot"),
        seq=None,
    )
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_kraken.py -v`** — expect PASS (6 passed total).

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/connectors/kraken.py tests/unit/test_kraken.py
git commit -m "feat(connectors): add Kraken book message -> BookUpdate parser"
```

---

## Task 4: Binance depth feed (REST seed + diff continuity/gap)

**Files:**
- Create: `src/pavilos/connectors/binance.py`
- Test: `tests/unit/test_binance.py`

> `BinanceDepthFeed` enforces the documented spot procedure with no I/O: the
> transport supplies a REST snapshot dict and decoded diff events; the feed
> emits `BookUpdate`s and signals resync on a gap. It tracks only
> `last_update_id` — the aggregator's `BookState` holds the actual book.

- [ ] **Step 1: Write the failing test — create `tests/unit/test_binance.py`:**

```python
# tests/unit/test_binance.py
import pytest

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired
from pavilos.connectors.binance import BinanceDepthFeed


def _snapshot(last_update_id, bids, asks):
    return {"lastUpdateId": last_update_id, "bids": bids, "asks": asks}


def _event(U, u, bids, asks, E=1_000):
    return {"e": "depthUpdate", "E": E, "s": "BTCUSDT", "U": U, "u": u, "b": bids, "a": asks}


def test_seed_emits_snapshot_bookupdate():
    feed = BinanceDepthFeed("BTCUSDT")
    snap = feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    assert isinstance(snap, BookUpdate)
    assert snap.exchange == "binance"
    assert snap.is_snapshot is True
    assert snap.ts == 5.0
    assert snap.seq == 100
    assert snap.bids == ((100.0, 1.0),)
    assert snap.asks == ((101.0, 2.0),)


def test_apply_contiguous_event_emits_update():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # first event must straddle lastUpdateId+1 = 101
    u = feed.apply(_event(U=101, u=105, bids=[["100.0", "1.5"]], asks=[["101.0", "0"]], E=6_000))
    assert u is not None
    assert u.is_snapshot is False
    assert u.seq == 105
    assert u.ts == 6.0
    assert u.bids == ((100.0, 1.5),)
    assert u.asks == ((101.0, 0.0),)   # removal preserved
    # next event must be contiguous: U == previous u + 1 == 106
    u2 = feed.apply(_event(U=106, u=108, bids=[["99.5", "4.0"]], asks=[]))
    assert u2 is not None
    assert u2.seq == 108


def test_stale_event_is_ignored():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # u <= lastUpdateId -> stale -> ignored (returns None), state unchanged
    assert feed.apply(_event(U=90, u=99, bids=[["1.0", "1.0"]], asks=[])) is None
    # a contiguous event after the stale one still applies from lastUpdateId=100
    u = feed.apply(_event(U=101, u=102, bids=[["100.0", "2.0"]], asks=[]))
    assert u is not None and u.seq == 102


def test_gap_raises_resync_required():
    feed = BinanceDepthFeed("BTCUSDT")
    feed.seed(_snapshot(100, [["100.0", "1.0"]], [["101.0", "2.0"]]), ts=5.0)
    # U (103) > lastUpdateId+1 (101) -> missed events -> resync
    with pytest.raises(ResyncRequired):
        feed.apply(_event(U=103, u=104, bids=[["100.0", "1.0"]], asks=[]))


def test_apply_before_seed_raises():
    feed = BinanceDepthFeed("BTCUSDT")
    with pytest.raises(ResyncRequired):
        feed.apply(_event(U=1, u=2, bids=[], asks=[]))
```

- [ ] **Step 2: Run `python -m pytest tests/unit/test_binance.py -v`** — expect FAIL (`ModuleNotFoundError: No module named 'pavilos.connectors.binance'`).

- [ ] **Step 3: Implement — create `src/pavilos/connectors/binance.py`:**

```python
# src/pavilos/connectors/binance.py
"""Binance Spot diff. depth sequencer: REST snapshot + diff events -> BookUpdates.

No I/O. The transport supplies already-decoded dicts; this class enforces the
documented spot continuity rules and tracks only ``last_update_id`` (the
aggregator's BookState holds the actual book)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(raw: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(price), float(qty)) for price, qty in raw)


class BinanceDepthFeed:
    """Sequences Binance spot depth: ``seed`` from a REST snapshot, then ``apply``
    each diff event. Emits ``BookUpdate``s; raises ``ResyncRequired`` on a gap or
    if used before seeding. Spot continuity rule: ``event.U == prev.u + 1`` (no
    ``pu`` field on spot)."""

    def __init__(self, symbol: str, *, exchange: str = "binance") -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._last_update_id: int | None = None

    def seed(self, snapshot: dict, *, ts: float) -> BookUpdate:
        """Seed from a ``GET /api/v3/depth`` response. Returns a snapshot BookUpdate
        carrying the full book; sets ``last_update_id = lastUpdateId``."""
        self._last_update_id = int(snapshot["lastUpdateId"])
        return BookUpdate(
            exchange=self.exchange,
            ts=ts,
            bids=_levels(snapshot["bids"]),
            asks=_levels(snapshot["asks"]),
            is_snapshot=True,
            seq=self._last_update_id,
        )

    def apply(self, event: dict) -> BookUpdate | None:
        """Apply one ``depthUpdate`` event.

        Returns an update ``BookUpdate``, or ``None`` if the event is stale
        (``u <= last_update_id``). Raises ``ResyncRequired`` if not seeded or on a
        gap (``U > last_update_id + 1``). Absolute sizes; ``qty == "0"`` removals
        are passed through verbatim (BookState removes them on apply)."""
        if self._last_update_id is None:
            raise ResyncRequired("binance: apply before seed")
        first_id = int(event["U"])
        final_id = int(event["u"])
        if final_id <= self._last_update_id:
            return None  # stale / already applied
        if first_id > self._last_update_id + 1:
            raise ResyncRequired(
                f"binance: gap (event U={first_id} > last_update_id+1={self._last_update_id + 1})"
            )
        self._last_update_id = final_id
        return BookUpdate(
            exchange=self.exchange,
            ts=float(event["E"]) / 1000.0,
            bids=_levels(event["b"]),
            asks=_levels(event["a"]),
            is_snapshot=False,
            seq=final_id,
        )
```

- [ ] **Step 4: Run `python -m pytest tests/unit/test_binance.py -v`** — expect PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/connectors/binance.py tests/unit/test_binance.py
git commit -m "feat(connectors): add Binance spot depth sequencer (seed/apply/continuity/gap)"
```

---

## Task 5: Full suite green + close-out

**Files:** none (verification only)

- [ ] **Step 1: Run the entire suite**

Run: `python -m pytest -v`
Expected: ALL pass (26 prior M1-core + 2 base + 6 kraken + 5 binance = 39 tests).

- [ ] **Step 2: Confirm clean tree**

Run: `git status`
Expected: working tree clean.

- [ ] **Step 3: Tag**

```bash
git tag m1b-connectors-logic
git log --oneline -6
```

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.1 connectors — logic portion):**
- Two book models represented: Kraken full-snapshot parse (Task 3) + Binance REST-seed + diff sequencing (Task 4) ✅
- Integrity by checksum (Kraken CRC32, Task 2, verified vs documented vector) and by sequence continuity (Binance U/u, Task 4) ✅
- `qty == 0` removal semantics preserved into `BookUpdate` for both (BookState removes on apply) ✅
- Resync signalling for out-of-sync books (`ResyncRequired`, Tasks 1 & 4) ✅
- Normalized output: every path yields `pavilos.core.models.BookUpdate`, which `Aggregator.apply` already consumes ✅
- *Deferred to M1c (correctly out of scope):* async websockets/aiohttp transport, reconnection/backoff, proxy support, the live capture script + skippable live smoke, Kraken's stateful apply-and-verify-checksum loop (uses Task 2's primitive), wiring connectors into `Aggregator.run`, the remaining 4 native connectors (Coinbase/OKX/Bybit/Bitstamp) + ccxt long tail, live peg/FX updater.

**Placeholder scan:** No TBD/TODO; every code step has complete runnable code. ✅

**Type consistency:** `BookUpdate(exchange, ts, bids, asks, is_snapshot, seq)` used identically by `parse_kraken_message` and `BinanceDepthFeed`; `ResyncRequired`/`ConnectorHealth` from `base` imported consistently; `book_checksum(asks, bids)` and `_fmt`/`_crc32` signatures match their tests. ✅

**Note on Decimal precision (deferred, intentional):** `parse_kraken_message` converts prices/qtys to float for the aggregator (binning tolerates float). Kraken's CRC32 requires FULL wire precision, so the stateful checksum-verify loop in M1c must read the raw `price`/`qty` as Decimal/strings (decode with `parse_float=Decimal`) and feed `book_checksum` the original strings — NOT round-tripped floats. `book_checksum` already takes strings precisely for this reason. This is flagged here so M1c preserves precision at the checksum boundary.
