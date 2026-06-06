# PAVILOS M1-core: Aggregation Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure, network-free core that turns per-exchange L2 order-book updates into a combined, USD-normalized, binned depth snapshot with per-venue composition — fully unit-tested and runnable via a deterministic replay harness.

**Architecture:** A monolith-async design (see spec). This plan covers only the *pure logic* layer: data models, per-exchange `BookState` maintenance, quote→USD normalization (Tier A only mixed into the level map; Tier B kept as context), price binning + combination, and an `Aggregator` that ties them together. No sockets, no exchanges — updates arrive as `BookUpdate` objects, so everything is deterministic and testable. Real exchange connectors and the dashboard are separate follow-on plans that feed `BookUpdate`s into this engine.

**Tech Stack:** Python 3.13, stdlib only for the core (`dataclasses`, `enum`, `math`, `statistics`, `asyncio`), `pytest` for tests. `src/` layout with `pythonpath` configured in pytest (no editable install needed).

---

## File Structure

```
PAVILOS/
├── pyproject.toml                         # project metadata + pytest config
├── src/pavilos/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   └── models.py                      # Side, Quote, Tier, BookUpdate, VenueSpec, DepthBin, CombinedDepthSnapshot
│   └── aggregator/
│       ├── __init__.py
│       ├── book_state.py                  # BookState (per-exchange L2 maintenance)
│       ├── normalize.py                   # PegProvider (quote -> USD)
│       ├── combine.py                     # bin_index + build_combined (pure function)
│       └── aggregator.py                  # Aggregator (holds states, staleness, snapshot, async run)
├── scripts/
│   └── replay.py                          # deterministic replay harness (JSONL -> combined book)
└── tests/
    ├── fixtures/
    │   └── replay_two_venues.jsonl        # synthetic two-venue update stream
    └── unit/
        ├── test_models.py
        ├── test_book_state.py
        ├── test_normalize.py
        ├── test_combine.py
        ├── test_aggregator.py
        └── test_replay.py
```

**Responsibility per file:**
- `core/models.py` — all shared immutable data types and enums. No logic.
- `aggregator/book_state.py` — maintain ONE exchange's book from absolute-size updates and snapshots. Knows nothing about USD, bins, or other venues.
- `aggregator/normalize.py` — convert a price in some quote currency to USD using live-updatable rates. Pure arithmetic.
- `aggregator/combine.py` — given several venues' books + their specs + a peg provider, produce one binned `CombinedDepthSnapshot`. Pure function, no state.
- `aggregator/aggregator.py` — own the per-exchange `BookState`s, track staleness, and produce snapshots on demand or on a clock-driven async loop.
- `scripts/replay.py` — feed a recorded JSONL stream of `BookUpdate`s through the `Aggregator` and print snapshots; proves the engine end-to-end without a network.

---

## Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/pavilos/__init__.py`, `src/pavilos/core/__init__.py`, `src/pavilos/aggregator/__init__.py`
- Create: `tests/__init__.py`, `tests/unit/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "pavilos"
version = "0.1.0"
description = "Multi-exchange BTC order-book aggregation and support/resistance trading engine"
requires-python = ">=3.13"
dependencies = []

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
addopts = "-q"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 2: Create the empty package marker files**

Create each of these as an empty file:
- `src/pavilos/__init__.py`
- `src/pavilos/core/__init__.py`
- `src/pavilos/aggregator/__init__.py`
- `tests/__init__.py`
- `tests/unit/__init__.py`

- [ ] **Step 3: Install dev dependencies and verify pytest runs**

Run: `python -m pip install -e ".[dev]"`
Then run: `python -m pytest`
Expected: pytest collects 0 items and exits 0 (`no tests ran`).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml src/ tests/
git commit -m "chore: scaffold pavilos package and pytest config"
```

---

## Task 2: Core data models

**Files:**
- Create: `src/pavilos/core/models.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_models.py
import dataclasses
import pytest

from pavilos.core.models import (
    Side, Quote, Tier, BookUpdate, VenueSpec, DepthBin, CombinedDepthSnapshot,
    TIER_A_QUOTES,
)


def test_side_and_quote_values():
    assert Side.BID.value == "bid"
    assert Side.ASK.value == "ask"
    assert Quote.USD.value == "USD"
    assert {Quote.USD, Quote.USDT, Quote.USDC} == TIER_A_QUOTES


def test_bookupdate_is_immutable_and_holds_levels():
    u = BookUpdate(
        exchange="kraken",
        ts=1.0,
        bids=((100.0, 1.0), (99.0, 2.0)),
        asks=((101.0, 1.5),),
        is_snapshot=True,
        seq=None,
    )
    assert u.bids[0] == (100.0, 1.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        u.exchange = "binance"  # type: ignore[misc]


def test_venuespec_pairs_quote_and_tier():
    spec = VenueSpec(exchange="upbit", quote=Quote.KRW, tier=Tier.B)
    assert spec.tier is Tier.B
    assert spec.quote is Quote.KRW


def test_depthbin_and_snapshot_construct():
    b = DepthBin(price=100.0, size=1.5, composition={"kraken": 1.0, "coinbase": 0.5})
    snap = CombinedDepthSnapshot(
        ts=1.0, mid=100.5, bids=(b,), asks=(), venues_active=("kraken", "coinbase"), venues_total=2
    )
    assert snap.bids[0].composition["coinbase"] == 0.5
    assert snap.venues_total == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pavilos.core.models'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pavilos/core/models.py
"""Shared immutable data types for PAVILOS. No logic lives here."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Side(str, Enum):
    BID = "bid"
    ASK = "ask"


class Quote(str, Enum):
    USD = "USD"
    USDT = "USDT"
    USDC = "USDC"
    EUR = "EUR"
    KRW = "KRW"
    JPY = "JPY"


class Tier(str, Enum):
    A = "A"  # price-comparable core (USD/USDT/USDC) -> mixed into the USD level map
    B = "B"  # context/breadth (KRW/JPY/EUR) -> kept separate, not in the level map


#: Quotes that are price-comparable in USD and may be mixed into the combined book.
TIER_A_QUOTES: frozenset[Quote] = frozenset({Quote.USD, Quote.USDT, Quote.USDC})


@dataclass(slots=True, frozen=True)
class BookUpdate:
    """A normalized L2 update from one exchange.

    Sizes are ABSOLUTE base-asset (BTC) quantities at each price level; a size
    of 0.0 removes that level. ``is_snapshot=True`` means the receiver should
    reset its book to exactly these levels before applying. Prices are in the
    exchange's own quote currency — USD normalization happens downstream.
    ``seq`` is the feed's sequence number when available (else None).
    """

    exchange: str
    ts: float
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    is_snapshot: bool
    seq: int | None = None


@dataclass(slots=True, frozen=True)
class VenueSpec:
    """Static metadata about one venue used by the aggregator."""

    exchange: str
    quote: Quote
    tier: Tier


@dataclass(slots=True, frozen=True)
class DepthBin:
    """One binned price level of the combined book.

    ``price`` is the bin's representative (center) USD price, ``size`` is the
    total base size across contributing venues, and ``composition`` maps each
    contributing exchange to the size it added.
    """

    price: float
    size: float
    composition: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CombinedDepthSnapshot:
    """The combined, USD-normalized, binned book at one instant.

    ``bids`` are sorted by price descending (best bid first); ``asks`` ascending
    (best ask first). ``venues_active`` are the Tier-A venues that contributed;
    ``venues_total`` is the count of configured Tier-A venues.
    """

    ts: float
    mid: float
    bids: tuple[DepthBin, ...]
    asks: tuple[DepthBin, ...]
    venues_active: tuple[str, ...]
    venues_total: int
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_models.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/core/models.py tests/unit/test_models.py
git commit -m "feat(core): add shared order-book data models"
```

---

## Task 3: BookState (per-exchange L2 maintenance)

**Files:**
- Create: `src/pavilos/aggregator/book_state.py`
- Test: `tests/unit/test_book_state.py`

> Note: sequence *contiguity* validation (e.g. Binance U/u rules, snapshot re-seed on gap) is the responsibility of the exchange *connectors* in a later plan, because the rule is feed-specific. `BookState` only enforces snapshot-reset, absolute-size apply, removal on 0, and dropping stale/duplicate updates by monotonic `seq`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_book_state.py
from pavilos.core.models import BookUpdate
from pavilos.aggregator.book_state import BookState


def _snap(exchange="kraken", ts=1.0, bids=(), asks=(), seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=True, seq=seq)


def _upd(exchange="kraken", ts=2.0, bids=(), asks=(), seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=False, seq=seq)


def test_snapshot_sets_book_and_drops_zero_sizes():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0), (99.0, 0.0)], asks=[(101.0, 2.0)]))
    assert bs.bids() == {100.0: 1.0}        # the 0-size level is dropped
    assert bs.asks() == {101.0: 2.0}
    assert bs.best_bid() == 100.0
    assert bs.best_ask() == 101.0
    assert bs.mid() == 100.5
    assert bs.last_ts == 1.0


def test_update_adds_updates_and_removes_levels():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
    bs.apply(_upd(ts=2.0, bids=[(100.0, 3.0), (99.5, 1.0)], asks=[(101.0, 0.0)]))
    assert bs.bids() == {100.0: 3.0, 99.5: 1.0}   # 100.0 updated, 99.5 added
    assert bs.asks() == {}                          # 101.0 removed by 0-size
    assert bs.best_ask() is None
    assert bs.mid() is None                          # no ask side -> no mid
    assert bs.last_ts == 2.0


def test_snapshot_resets_previous_state():
    bs = BookState("kraken")
    bs.apply(_snap(bids=[(100.0, 1.0), (99.0, 5.0)], asks=[(101.0, 2.0)]))
    bs.apply(_snap(ts=3.0, bids=[(98.0, 1.0)], asks=[(102.0, 1.0)]))
    assert bs.bids() == {98.0: 1.0}                  # old levels gone
    assert bs.asks() == {102.0: 1.0}


def test_stale_or_duplicate_seq_is_ignored():
    bs = BookState("bybit", track_seq=True)
    bs.apply(_snap(exchange="bybit", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], seq=10))
    bs.apply(_upd(exchange="bybit", ts=2.0, bids=[(100.0, 2.0)], seq=9))   # stale
    assert bs.bids() == {100.0: 1.0}                 # ignored
    bs.apply(_upd(exchange="bybit", ts=3.0, bids=[(100.0, 2.0)], seq=11))  # fresh
    assert bs.bids() == {100.0: 2.0}
    assert bs.last_seq == 11
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_book_state.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pavilos.aggregator.book_state'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pavilos/aggregator/book_state.py
"""Maintain ONE exchange's L2 order book from absolute-size updates."""
from __future__ import annotations

from collections.abc import Iterable

from pavilos.core.models import BookUpdate


class BookState:
    """Per-exchange L2 book held as ``price -> size`` maps.

    Sizes are absolute; a size <= 0 removes the level. Snapshots reset the
    book. When ``track_seq`` is set and updates carry ``seq``, updates whose
    ``seq`` is not strictly greater than the last seen ``seq`` are dropped
    (stale/duplicate). Prices remain in the exchange's quote currency.
    """

    def __init__(self, exchange: str, *, track_seq: bool = False) -> None:
        self.exchange = exchange
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self.last_ts: float = 0.0
        self.last_seq: int | None = None
        self._track_seq = track_seq

    def apply(self, u: BookUpdate) -> None:
        if not u.is_snapshot and self._track_seq and u.seq is not None and self.last_seq is not None:
            if u.seq <= self.last_seq:
                return  # stale / duplicate
        if u.is_snapshot:
            self._bids = {p: s for p, s in u.bids if s > 0}
            self._asks = {p: s for p, s in u.asks if s > 0}
        else:
            self._apply_side(self._bids, u.bids)
            self._apply_side(self._asks, u.asks)
        if u.seq is not None:
            self.last_seq = u.seq
        self.last_ts = u.ts

    @staticmethod
    def _apply_side(book: dict[float, float], levels: Iterable[tuple[float, float]]) -> None:
        for price, size in levels:
            if size <= 0:
                book.pop(price, None)
            else:
                book[price] = size

    def bids(self) -> dict[float, float]:
        return self._bids

    def asks(self) -> dict[float, float]:
        return self._asks

    def best_bid(self) -> float | None:
        return max(self._bids) if self._bids else None

    def best_ask(self) -> float | None:
        return min(self._asks) if self._asks else None

    def mid(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_book_state.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/aggregator/book_state.py tests/unit/test_book_state.py
git commit -m "feat(aggregator): add per-exchange BookState L2 maintenance"
```

---

## Task 4: Quote → USD normalization (PegProvider)

**Files:**
- Create: `src/pavilos/aggregator/normalize.py`
- Test: `tests/unit/test_normalize.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_normalize.py
import pytest

from pavilos.core.models import Quote
from pavilos.aggregator.normalize import PegProvider


def test_usd_pegged_default_to_one():
    peg = PegProvider()
    assert peg.to_usd(100.0, Quote.USD) == 100.0
    assert peg.to_usd(100.0, Quote.USDT) == 100.0
    assert peg.to_usd(100.0, Quote.USDC) == 100.0


def test_set_rate_applies_to_conversion():
    peg = PegProvider()
    peg.set_rate(Quote.USDT, 0.999)            # USDT trading slightly below peg
    assert peg.to_usd(100_000.0, Quote.USDT) == pytest.approx(99_900.0)


def test_fx_quotes_require_explicit_rate():
    peg = PegProvider()
    with pytest.raises(ValueError):
        peg.to_usd(140_000_000.0, Quote.KRW)   # no KRW rate set
    peg.set_rate(Quote.KRW, 0.00072)
    assert peg.to_usd(140_000_000.0, Quote.KRW) == pytest.approx(100_800.0)


def test_rate_must_be_positive():
    peg = PegProvider()
    with pytest.raises(ValueError):
        peg.set_rate(Quote.JPY, 0.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_normalize.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pavilos.aggregator.normalize'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pavilos/aggregator/normalize.py
"""Convert prices from a quote currency to USD using live-updatable rates."""
from __future__ import annotations

from pavilos.core.models import Quote


class PegProvider:
    """Holds ``quote -> USD`` multipliers.

    USD-pegged stablecoins default to 1.0 (overridable with live peg readings,
    e.g. from a USDT/USD market). FX quotes (KRW/JPY/EUR) have no default and
    must be set explicitly via :meth:`set_rate` before conversion, otherwise
    :meth:`to_usd` raises ``ValueError``.
    """

    def __init__(self, rates: dict[Quote, float] | None = None) -> None:
        self._rates: dict[Quote, float] = {
            Quote.USD: 1.0,
            Quote.USDT: 1.0,
            Quote.USDC: 1.0,
        }
        if rates:
            for quote, rate in rates.items():
                self.set_rate(quote, rate)

    def set_rate(self, quote: Quote, rate: float) -> None:
        if rate <= 0:
            raise ValueError(f"rate for {quote} must be positive, got {rate}")
        self._rates[quote] = rate

    def to_usd(self, price: float, quote: Quote) -> float:
        try:
            return price * self._rates[quote]
        except KeyError:
            raise ValueError(f"no USD conversion rate set for quote {quote}") from None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_normalize.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/aggregator/normalize.py tests/unit/test_normalize.py
git commit -m "feat(aggregator): add PegProvider quote->USD normalization"
```

---

## Task 5: Binning + combination (build_combined)

**Files:**
- Create: `src/pavilos/aggregator/combine.py`
- Test: `tests/unit/test_combine.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_combine.py
import math
import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.combine import bin_index, build_combined


def test_bin_index_sign_and_magnitude():
    mid = 100.5
    # 100.0 is ~ -49.75 bps from mid -> floor(-0.4975) = -1
    assert bin_index(100.0, mid, bin_bps=100.0) == -1
    # 99.0 is ~ -149.25 bps -> floor(-1.4925) = -2
    assert bin_index(99.0, mid, bin_bps=100.0) == -2
    # 101.0 is ~ +49.75 bps -> floor(0.4975) = 0
    assert bin_index(101.0, mid, bin_bps=100.0) == 0
    # 102.0 is ~ +149.25 bps -> floor(1.4925) = 1
    assert bin_index(102.0, mid, bin_bps=100.0) == 1


def _book(exchange, bids, asks):
    bs = BookState(exchange)
    bs.apply(BookUpdate(exchange=exchange, ts=1.0, bids=tuple(bids), asks=tuple(asks),
                        is_snapshot=True, seq=None))
    return bs


def test_build_combined_sums_sizes_and_tracks_composition():
    books = {
        "kraken": _book("kraken", [(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0), (102.0, 3.0)]),
        "coinbase": _book("coinbase", [(100.0, 0.5)], [(101.0, 0.5)]),
    }
    specs = {
        "kraken": VenueSpec("kraken", Quote.USD, Tier.A),
        "coinbase": VenueSpec("coinbase", Quote.USD, Tier.A),
    }
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken", "coinbase"})
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    assert snap.venues_total == 2
    assert set(snap.venues_active) == {"kraken", "coinbase"}

    # bids: bin -1 (the 100.0 level from both venues) and bin -2 (kraken 99.0)
    bids_by_size = sorted((b.size for b in snap.bids), reverse=True)
    assert bids_by_size == pytest.approx([2.0, 1.5])
    top_bid = max(snap.bids, key=lambda b: b.price)        # nearest to mid = bin -1
    assert top_bid.size == pytest.approx(1.5)
    assert top_bid.composition == {"kraken": pytest.approx(1.0), "coinbase": pytest.approx(0.5)}
    assert top_bid.price == pytest.approx(100.5 * (1 + (-0.5) * 100.0 / 1e4))  # ~99.9975

    # asks: bin 0 (101.0 from both) and bin 1 (kraken 102.0)
    asks_by_size = sorted((a.size for a in snap.asks))
    assert asks_by_size == pytest.approx([1.5, 3.0])
    best_ask = min(snap.asks, key=lambda a: a.price)        # nearest to mid = bin 0
    assert best_ask.composition == {"kraken": pytest.approx(1.0), "coinbase": pytest.approx(0.5)}


def test_build_combined_excludes_tier_b_and_inactive():
    books = {
        "kraken": _book("kraken", [(100.0, 1.0)], [(101.0, 1.0)]),
        "upbit": _book("upbit", [(100.0, 9.0)], [(101.0, 9.0)]),    # Tier B -> excluded
        "okx": _book("okx", [(100.0, 7.0)], [(101.0, 7.0)]),        # inactive -> excluded
    }
    specs = {
        "kraken": VenueSpec("kraken", Quote.USD, Tier.A),
        "upbit": VenueSpec("upbit", Quote.KRW, Tier.B),
        "okx": VenueSpec("okx", Quote.USDT, Tier.A),
    }
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active={"kraken"})
    assert snap is not None
    # only kraken contributes -> top bid size 1.0, composition only kraken
    top_bid = max(snap.bids, key=lambda b: b.price)
    assert top_bid.size == pytest.approx(1.0)
    assert top_bid.composition == {"kraken": pytest.approx(1.0)}
    assert snap.venues_total == 2          # kraken + okx are Tier A
    assert snap.venues_active == ("kraken",)


def test_build_combined_returns_none_without_active_tier_a():
    books = {"upbit": _book("upbit", [(100.0, 1.0)], [(101.0, 1.0)])}
    specs = {"upbit": VenueSpec("upbit", Quote.KRW, Tier.B)}
    snap = build_combined(books, specs, PegProvider(), bin_bps=100.0, window_bps=200.0,
                          ts=5.0, active=set())
    assert snap is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_combine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pavilos.aggregator.combine'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pavilos/aggregator/combine.py
"""Combine several venues' books into one binned, USD-normalized snapshot."""
from __future__ import annotations

import math
from statistics import median

from pavilos.core.models import (
    CombinedDepthSnapshot, DepthBin, Tier, VenueSpec,
)
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.normalize import PegProvider


def bin_index(price: float, mid: float, bin_bps: float) -> int:
    """Signed bin index of ``price`` relative to ``mid``, in units of ``bin_bps``.

    Negative for prices below mid (bids), >= 0 for prices above mid (asks).
    """
    bps = (price - mid) / mid * 1e4
    return int(math.floor(bps / bin_bps))


def _bin_center_price(idx: int, mid: float, bin_bps: float) -> float:
    """Representative USD price for a bin index (its center)."""
    return mid * (1.0 + (idx + 0.5) * bin_bps / 1e4)


def build_combined(
    books: dict[str, BookState],
    specs: dict[str, VenueSpec],
    peg: PegProvider,
    *,
    bin_bps: float,
    window_bps: float,
    ts: float,
    active: set[str],
) -> CombinedDepthSnapshot | None:
    """Build a combined depth snapshot from the active Tier-A venues.

    Only venues that are (a) Tier A, (b) in ``active``, and (c) have a valid mid
    contribute. Prices are converted to USD via ``peg``, binned in ``bin_bps``
    buckets around the combined mid, and summed across venues. Levels beyond
    ``window_bps`` from mid are ignored. Returns ``None`` if no Tier-A venue is
    active.
    """
    contributors: list[tuple[str, BookState, float]] = []  # (exchange, book, usd_mid)
    for exchange, book in books.items():
        spec = specs.get(exchange)
        if spec is None or spec.tier is not Tier.A or exchange not in active:
            continue
        m = book.mid()
        if m is None:
            continue
        contributors.append((exchange, book, peg.to_usd(m, spec.quote)))

    if not contributors:
        return None

    mid = float(median(usd_mid for _, _, usd_mid in contributors))
    half_window = mid * window_bps / 1e4

    bid_size: dict[int, float] = {}
    bid_comp: dict[int, dict[str, float]] = {}
    ask_size: dict[int, float] = {}
    ask_comp: dict[int, dict[str, float]] = {}

    for exchange, book, _ in contributors:
        quote = specs[exchange].quote
        for price, size in book.bids().items():
            usd = peg.to_usd(price, quote)
            if usd < mid - half_window or usd >= mid:
                continue
            idx = bin_index(usd, mid, bin_bps)
            bid_size[idx] = bid_size.get(idx, 0.0) + size
            bid_comp.setdefault(idx, {})[exchange] = bid_comp.setdefault(idx, {}).get(exchange, 0.0) + size
        for price, size in book.asks().items():
            usd = peg.to_usd(price, quote)
            if usd > mid + half_window or usd <= mid:
                continue
            idx = bin_index(usd, mid, bin_bps)
            ask_size[idx] = ask_size.get(idx, 0.0) + size
            ask_comp.setdefault(idx, {})[exchange] = ask_comp.setdefault(idx, {}).get(exchange, 0.0) + size

    bids = tuple(
        DepthBin(price=_bin_center_price(idx, mid, bin_bps), size=bid_size[idx], composition=bid_comp[idx])
        for idx in sorted(bid_size, reverse=True)
    )
    asks = tuple(
        DepthBin(price=_bin_center_price(idx, mid, bin_bps), size=ask_size[idx], composition=ask_comp[idx])
        for idx in sorted(ask_size)
    )
    venues_total = sum(1 for s in specs.values() if s.tier is Tier.A)
    venues_active = tuple(sorted(ex for ex, _, _ in contributors))
    return CombinedDepthSnapshot(
        ts=ts, mid=mid, bids=bids, asks=asks,
        venues_active=venues_active, venues_total=venues_total,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_combine.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/aggregator/combine.py tests/unit/test_combine.py
git commit -m "feat(aggregator): add binning and combined-book builder"
```

---

## Task 6: Aggregator (state ownership, staleness, snapshot)

**Files:**
- Create: `src/pavilos/aggregator/aggregator.py`
- Test: `tests/unit/test_aggregator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_aggregator.py
import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator


def _snap(exchange, ts, bids, asks, seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=True, seq=seq)


def _specs():
    return [
        VenueSpec("kraken", Quote.USD, Tier.A),
        VenueSpec("coinbase", Quote.USD, Tier.A),
    ]


def test_aggregator_routes_updates_and_builds_snapshot():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    agg.apply(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    agg.apply(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    snap = agg.snapshot(now=2.0)
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    assert set(snap.venues_active) == {"kraken", "coinbase"}


def test_aggregator_excludes_stale_venue():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    agg.apply(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    agg.apply(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    # 'now' is 10s later; staleness_s is 5 -> both feeds are stale -> no snapshot
    assert agg.snapshot(now=11.0) is None
    # a fresh coinbase update revives only coinbase
    agg.apply(_snap("coinbase", 11.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    snap = agg.snapshot(now=12.0)
    assert snap is not None
    assert snap.venues_active == ("coinbase",)


def test_aggregator_rejects_unknown_exchange():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    with pytest.raises(KeyError):
        agg.apply(_snap("ftx", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_aggregator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pavilos.aggregator.aggregator'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pavilos/aggregator/aggregator.py
"""Owns per-exchange BookStates and produces combined snapshots."""
from __future__ import annotations

from collections.abc import Sequence

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot, VenueSpec
from pavilos.aggregator.book_state import BookState
from pavilos.aggregator.combine import build_combined
from pavilos.aggregator.normalize import PegProvider


class Aggregator:
    """Routes ``BookUpdate``s to per-exchange ``BookState``s and, on demand,
    builds a combined snapshot from the venues that are fresh (not stale)."""

    def __init__(
        self,
        specs: Sequence[VenueSpec],
        peg: PegProvider,
        *,
        bin_bps: float,
        window_bps: float,
        staleness_s: float,
    ) -> None:
        self._specs = {s.exchange: s for s in specs}
        self._books = {s.exchange: BookState(s.exchange) for s in specs}
        self._peg = peg
        self._bin_bps = bin_bps
        self._window_bps = window_bps
        self._staleness_s = staleness_s

    def apply(self, u: BookUpdate) -> None:
        self._books[u.exchange].apply(u)  # KeyError on unknown exchange (by design)

    def active(self, now: float) -> set[str]:
        return {
            ex
            for ex, book in self._books.items()
            if book.last_ts > 0.0 and (now - book.last_ts) <= self._staleness_s and book.mid() is not None
        }

    def snapshot(self, now: float) -> CombinedDepthSnapshot | None:
        return build_combined(
            self._books,
            self._specs,
            self._peg,
            bin_bps=self._bin_bps,
            window_bps=self._window_bps,
            ts=now,
            active=self.active(now),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_aggregator.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/aggregator/aggregator.py tests/unit/test_aggregator.py
git commit -m "feat(aggregator): add Aggregator with staleness-aware snapshots"
```

---

## Task 7: Async run loop (clock-driven snapshot emission)

**Files:**
- Modify: `src/pavilos/aggregator/aggregator.py` (add `run` method)
- Test: `tests/unit/test_aggregator.py` (add async test)

> Adds a thin asyncio loop that drains an input queue of `BookUpdate`s and emits snapshots to an output queue at a fixed interval, using an injected ``now`` callable so the test is deterministic (no wall-clock sleeps tied to real time beyond a tiny yield).

- [ ] **Step 1: Write the failing test (append to `tests/unit/test_aggregator.py`)**

```python
import asyncio


def test_run_emits_snapshot_then_stops():
    async def scenario():
        agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        await in_q.put(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        await in_q.put(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))

        clock = {"t": 2.0}
        stop = asyncio.Event()

        task = asyncio.create_task(
            agg.run(in_q, out_q, interval_s=0.0, now=lambda: clock["t"], stop=stop)
        )
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap

    snap = asyncio.run(scenario())
    assert snap is not None
    assert set(snap.venues_active) == {"kraken", "coinbase"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_aggregator.py::test_run_emits_snapshot_then_stops -v`
Expected: FAIL with `AttributeError: 'Aggregator' object has no attribute 'run'`

- [ ] **Step 3: Add the `run` method to `Aggregator`**

Add these imports at the top of `aggregator.py` (alongside existing imports):

```python
import asyncio
from collections.abc import Callable
```

Add this method to the `Aggregator` class:

```python
    async def run(
        self,
        in_q: "asyncio.Queue[BookUpdate]",
        out_q: "asyncio.Queue[CombinedDepthSnapshot]",
        *,
        interval_s: float,
        now: Callable[[], float],
        stop: "asyncio.Event",
    ) -> None:
        """Drain ``in_q`` into the books and emit a snapshot every ``interval_s``.

        Drains all immediately-available updates each tick, then emits one
        combined snapshot (if any Tier-A venue is fresh). Exits when ``stop``
        is set. ``now`` is injected for deterministic testing.
        """
        while not stop.is_set():
            # Drain everything currently queued without blocking.
            while True:
                try:
                    self.apply(in_q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            snap = self.snapshot(now())
            if snap is not None:
                await out_q.put(snap)
            if interval_s > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(0)  # yield control
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_aggregator.py -v`
Expected: PASS (4 passed — the 3 prior + the new async test)

- [ ] **Step 5: Commit**

```bash
git add src/pavilos/aggregator/aggregator.py tests/unit/test_aggregator.py
git commit -m "feat(aggregator): add async run loop with injected clock"
```

---

## Task 8: Replay harness + end-to-end test

> **Correction applied during implementation (2026-06-06):** the original draft of this task used fixture timestamps `ts=1.0/2.0` with the test snapshotting at `now=100.0` and the CLI `main` at `now=1e12`. That contradicts the shipped `Aggregator.active(now)` staleness gate (`staleness_s=60.0`): every venue would be stale → `snapshot()` returns `None`, making the asserted outcomes impossible. **Resolution (implemented & merged):** fixture timestamps shifted to `40.0/40.0/41.0` (within 60 s of the test's `now=100.0`), and `main` snapshots at `now = max(u.ts for u in updates)` — the stream's own final clock, which is the semantically correct choice for a batch replay. The engine was NOT changed; the staleness gate is correct.

**Files:**
- Create: `scripts/replay.py`
- Create: `tests/fixtures/replay_two_venues.jsonl`
- Test: `tests/unit/test_replay.py`

> The harness reads a JSONL stream where each line is a `BookUpdate` (fields:
> `exchange, ts, bids, asks, is_snapshot, seq`), feeds them through an
> `Aggregator`, and returns the final combined snapshot. This proves the whole
> engine deterministically with no network.

- [ ] **Step 1: Create the fixture `tests/fixtures/replay_two_venues.jsonl`**

```
{"exchange": "kraken", "ts": 1.0, "bids": [[100.0, 1.0], [99.0, 2.0]], "asks": [[101.0, 1.0], [102.0, 3.0]], "is_snapshot": true, "seq": null}
{"exchange": "coinbase", "ts": 1.0, "bids": [[100.0, 0.5]], "asks": [[101.0, 0.5]], "is_snapshot": true, "seq": null}
{"exchange": "kraken", "ts": 2.0, "bids": [[100.0, 1.5]], "asks": [], "is_snapshot": false, "seq": null}
```

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_replay.py
from pathlib import Path

import pytest

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from scripts.replay import load_updates, replay

FIXTURE = Path(__file__).parent.parent / "fixtures" / "replay_two_venues.jsonl"


def _agg():
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A)]
    return Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)


def test_load_updates_parses_jsonl():
    updates = load_updates(FIXTURE)
    assert len(updates) == 3
    assert updates[0].exchange == "kraken"
    assert updates[0].is_snapshot is True
    assert updates[0].bids[0] == (100.0, 1.0)
    assert updates[2].is_snapshot is False


def test_replay_produces_expected_final_snapshot():
    snap = replay(_agg(), load_updates(FIXTURE), now=100.0)
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    # kraken bid 100.0 was updated to 1.5; coinbase 100.0 still 0.5 -> bin -1 size 2.0
    top_bid = max(snap.bids, key=lambda b: b.price)
    assert top_bid.size == pytest.approx(2.0)
    assert top_bid.composition == {"kraken": pytest.approx(1.5), "coinbase": pytest.approx(0.5)}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_replay.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.replay'`

- [ ] **Step 4: Create `scripts/__init__.py` (empty) and write `scripts/replay.py`**

```python
# scripts/replay.py
"""Deterministic replay harness: feed a JSONL BookUpdate stream through the
Aggregator and print/return the resulting combined snapshot. No network."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.models import CombinedDepthSnapshot


def load_updates(path: Path) -> list[BookUpdate]:
    updates: list[BookUpdate] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        updates.append(
            BookUpdate(
                exchange=d["exchange"],
                ts=float(d["ts"]),
                bids=tuple((float(p), float(s)) for p, s in d["bids"]),
                asks=tuple((float(p), float(s)) for p, s in d["asks"]),
                is_snapshot=bool(d["is_snapshot"]),
                seq=d.get("seq"),
            )
        )
    return updates


def replay(agg: Aggregator, updates: list[BookUpdate], *, now: float) -> CombinedDepthSnapshot | None:
    for u in updates:
        agg.apply(u)
    return agg.snapshot(now=now)


def _default_aggregator() -> Aggregator:
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A)]
    return Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: python -m scripts.replay <updates.jsonl>", file=sys.stderr)
        return 2
    snap = replay(_default_aggregator(), load_updates(Path(argv[1])), now=1e12)
    if snap is None:
        print("no snapshot (no active Tier-A venue)")
        return 0
    print(f"mid={snap.mid:.2f}  venues={snap.venues_active}/{snap.venues_total}")
    print("  bids (best first):")
    for b in snap.bids[:10]:
        print(f"    {b.price:.2f}  size={b.size:.4f}  {b.composition}")
    print("  asks (best first):")
    for a in snap.asks[:10]:
        print(f"    {a.price:.2f}  size={a.size:.4f}  {a.composition}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/unit/test_replay.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Run the harness manually to confirm human-readable output**

Run: `python -m scripts.replay tests/fixtures/replay_two_venues.jsonl`
Expected output (approximately):
```
mid=100.50  venues=('coinbase', 'kraken')/2
  bids (best first):
    99.9975  size=2.0000  {'kraken': 1.5, 'coinbase': 0.5}
    98.9925  size=2.0000  {'kraken': 2.0}
  asks (best first):
    101.0025  size=1.5000  {'kraken': 1.0, 'coinbase': 0.5}
    102.0075  size=3.0000  {'kraken': 3.0}
```

- [ ] **Step 7: Commit**

```bash
git add scripts/__init__.py scripts/replay.py tests/fixtures/replay_two_venues.jsonl tests/unit/test_replay.py
git commit -m "feat(scripts): add deterministic replay harness + e2e test"
```

---

## Task 9: Full suite green + plan close-out

**Files:** none (verification only)

- [ ] **Step 1: Run the entire test suite**

Run: `python -m pytest -v`
Expected: ALL pass (test_models 4, test_book_state 4, test_normalize 4, test_combine 4, test_aggregator 4, test_replay 2 = 22 tests).

- [ ] **Step 2: Confirm no stray files / clean tree**

Run: `git status`
Expected: working tree clean.

- [ ] **Step 3: Tag the milestone-core completion (lightweight)**

```bash
git tag m1-core-aggregation
git log --oneline -9
```

---

## Self-Review (performed by plan author)

**Spec coverage (relevant slice of the spec — §5.2 aggregator):**
- Per-exchange L2 maintenance (snapshot reset, absolute-size apply, removal on 0) → Task 3 ✅
- Quote normalization USD/USDT/USDC with live-updatable peg; FX requires explicit rate → Task 4 ✅
- Tier A mixed into level map, Tier B excluded → Task 5 (`test_build_combined_excludes_tier_b_and_inactive`) ✅
- Binning around combined mid + per-venue composition → Task 5 ✅
- Combined mid from active venues; window filter → Task 5 ✅
- Staleness exclusion + graceful degradation (`venues_active`/`venues_total`) → Task 6 ✅
- Cadence-driven emission (async loop) → Task 7 ✅
- Deterministic end-to-end proof → Task 8 ✅
- *Deferred to later plans (correctly out of scope here):* real connectors, sequence-contiguity validation, live peg/FX wiring, detection, signals, paper broker, dashboard.

**Placeholder scan:** No TBD/TODO; every code step contains full runnable code. ✅

**Type consistency:** `BookUpdate(exchange, ts, bids, asks, is_snapshot, seq)`, `DepthBin(price, size, composition)`, `CombinedDepthSnapshot(ts, mid, bids, asks, venues_active, venues_total)`, `VenueSpec(exchange, quote, tier)`, `build_combined(books, specs, peg, *, bin_bps, window_bps, ts, active)`, `Aggregator(specs, peg, *, bin_bps, window_bps, staleness_s)` with `apply`/`active`/`snapshot`/`run` — all consistent across Tasks 2–8. ✅

**Note on live peg:** Task 4 ships `PegProvider` with a `set_rate` hook but no live updater (USDT/USD, KRW/JPY FX). Wiring a live updater is a small task in the connectors plan; for this engine the peg is injectable and defaulted to 1.0 for USD-pegged stablecoins, which is correct and honest for the network-free core.
