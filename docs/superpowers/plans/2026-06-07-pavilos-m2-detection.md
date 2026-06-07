# PAVILOS M2: Support/Resistance Detection (network-free) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification (try to break the detector: a frame sequence that produces a phantom support, a wall that should be flagged spoof but isn't, a confidence that exceeds 1.0 or goes negative, a zone-identity mismatch across snapshots). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn the combined depth stream into ranked support/resistance **zones** with a confidence score — detecting liquidity walls + clusters, tracking each zone's lifecycle across snapshots (persistence, growing/shrinking, pulled), and applying book-only anti-spoofing. Pure, deterministic, network-free; fed by `CombinedDepthSnapshot` (from M1's Aggregator/Engine).

**Architecture:** A pipeline of pure units under `src/pavilos/detection/`: `walls` (find bins that stand out vs local depth) → `clusters` (group adjacent heavy bins into zones) → `ZoneTracker` (match zones across snapshots, accumulate persistence, flag pulled) → `confidence` (score a zone 0..1) → `Detector` (orchestrates: `update(snapshot) -> DepthAnalysis`). The Detector is stateful only through the ZoneTracker; everything else is pure functions. Detection runs on the already-USD-normalized, binned `CombinedDepthSnapshot`, so it never touches a venue or a quote currency directly.

**Tech Stack:** Python 3.13, stdlib only (`statistics`, `dataclasses`), `pytest`. Builds on merged M1 (`CombinedDepthSnapshot`, `DepthBin` from `pavilos.core.models`).

---

## Scope decision (READ FIRST)

**Book-only anti-spoofing.** The spec's "consumed (real) vs pulled (spoof)" distinction requires **trade prints** (did a wall vanish because trades hit it, or because it was cancelled?). The M1 connectors emit order-book updates only — no trades. So M2 implements the anti-spoofing that IS possible from the book alone:
- **Persistence gate:** a zone must persist ≥ `min_persistence_s` to be "operable".
- **Pulled detection:** a tracked zone that disappears (size collapses) **while price never reached it** is flagged `pulled` (spoof-like) and its confidence is penalized.
- **Venue corroboration:** a zone backed by more venues scores higher (a single-venue wall is weaker / easier to spoof).

**Deferred to a future trade-feed milestone (M3-trades):** the trade-confirmed *consumed-vs-pulled* classification (watching trade prints in a zone). Noted in `confidence.py` and tracked in memory. This is a refinement, not a blocker — book-only persistence + pulled-detection already filters most spoofing.

Detection **parameters** (thresholds/weights) are configurable with sensible defaults; their VALUES need calibration against real data in M3 — the defaults here are reasonable starting points, the tests pin the *logic*, not the calibration.

---

## File Structure

```
PAVILOS/
├── src/pavilos/detection/
│   ├── __init__.py
│   ├── models.py        # Zone, DepthAnalysis [NEW]
│   ├── walls.py         # detect_walls(bins, ...) -> list[WallBin] [NEW]
│   ├── clusters.py      # cluster_bins(walls, ...) -> list[RawZone] [NEW]
│   ├── lifecycle.py     # ZoneTracker (match across snapshots, persistence, pulled) [NEW]
│   ├── confidence.py    # score_zone(...) -> float [NEW]
│   └── detector.py      # Detector.update(snapshot) -> DepthAnalysis [NEW]
└── tests/unit/
    ├── test_detection_models.py
    ├── test_walls.py
    ├── test_clusters.py
    ├── test_lifecycle.py
    ├── test_confidence.py
    └── test_detector.py
```

---

## Task 1: Detection models

**Files:** Create `src/pavilos/detection/__init__.py` (empty), `src/pavilos/detection/models.py`; Test `tests/unit/test_detection_models.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_detection_models.py`:**

```python
# tests/unit/test_detection_models.py
import dataclasses
import pytest

from pavilos.detection.models import Side, Zone, DepthAnalysis


def test_side_values():
    assert Side.SUPPORT.value == "support"
    assert Side.RESISTANCE.value == "resistance"


def test_zone_is_immutable_and_holds_fields():
    z = Zone(side=Side.SUPPORT, price=100.0, low=99.5, high=100.5, strength=12.0,
             venues=("kraken", "binance"), persistence_s=4.0, pulled=False, confidence=0.7)
    assert z.price == 100.0 and z.strength == 12.0
    assert z.venues == ("kraken", "binance")
    with pytest.raises(dataclasses.FrozenInstanceError):
        z.price = 1.0  # type: ignore[misc]


def test_depth_analysis_holds_sorted_zones():
    s = Zone(Side.SUPPORT, 100.0, 99.5, 100.5, 12.0, ("kraken",), 4.0, False, 0.7)
    r = Zone(Side.RESISTANCE, 101.0, 100.8, 101.2, 9.0, ("binance",), 2.0, False, 0.5)
    a = DepthAnalysis(ts=5.0, mid=100.5, supports=(s,), resistances=(r,))
    assert a.supports[0].confidence == 0.7
    assert a.mid == 100.5
```

- [ ] **Step 2:** `python -m pytest tests/unit/test_detection_models.py -v` → FAIL (ModuleNotFoundError).

- [ ] **Step 3: Implement — `src/pavilos/detection/models.py`:**

```python
# src/pavilos/detection/models.py
"""Immutable detection result types. No logic."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    SUPPORT = "support"        # below mid (a buy wall)
    RESISTANCE = "resistance"  # above mid (a sell wall)


@dataclass(slots=True, frozen=True)
class Zone:
    """A detected support/resistance zone in the combined book.

    ``price`` is the strength-weighted representative USD price; ``low``/``high``
    bound the zone; ``strength`` is the total base size (BTC) in the zone;
    ``venues`` are the exchanges contributing; ``persistence_s`` is how long the
    zone has existed; ``pulled`` flags a zone observed to vanish without price
    reaching it (spoof-like); ``confidence`` is 0..1."""

    side: Side
    price: float
    low: float
    high: float
    strength: float
    venues: tuple[str, ...]
    persistence_s: float
    pulled: bool
    confidence: float


@dataclass(slots=True, frozen=True)
class DepthAnalysis:
    """Detector output for one snapshot: ranked supports (below mid) and
    resistances (above mid), each sorted by confidence descending."""

    ts: float
    mid: float
    supports: tuple[Zone, ...]
    resistances: tuple[Zone, ...]
```

- [ ] **Step 4:** `python -m pytest tests/unit/test_detection_models.py -v` → 3 passed.
- [ ] **Step 5:** full suite (`python -m pytest`) → 99 passed (96 + 3).
- [ ] **Step 6:** Commit `feat(detection): add Zone + DepthAnalysis result models`.

---

## Task 2: Wall detection

**Files:** Create `src/pavilos/detection/walls.py`; Test `tests/unit/test_walls.py`.

> A "wall" is a bin whose size stands out vs the typical depth on its side. We use
> a **median-multiple** rule (robust to the wall itself): a bin is a wall if its
> size >= `size_multiple` × median(side sizes) AND size >= `min_size` (absolute
> floor so thin books don't produce noise). Returns the qualifying bins with a
> `prominence` = size / median.

- [ ] **Step 1: Failing test — `tests/unit/test_walls.py`:**

```python
# tests/unit/test_walls.py
from pavilos.core.models import DepthBin
from pavilos.detection.walls import detect_walls, WallBin


def _bin(price, size):
    return DepthBin(price=price, size=size, composition={"kraken": size})


def test_detects_bin_above_median_multiple():
    bins = [_bin(100.0, 1.0), _bin(99.0, 1.0), _bin(98.0, 10.0), _bin(97.0, 1.0)]
    walls = detect_walls(bins, size_multiple=3.0, min_size=0.0)
    assert len(walls) == 1
    assert isinstance(walls[0], WallBin)
    assert walls[0].bin.price == 98.0
    assert walls[0].prominence == 10.0  # 10.0 / median(1,1,10,1)=1.0


def test_min_size_floor_filters_thin_books():
    bins = [_bin(100.0, 0.001), _bin(99.0, 0.001), _bin(98.0, 0.005)]
    # 0.005 is 5x the median (0.001) but below the absolute floor -> not a wall
    assert detect_walls(bins, size_multiple=3.0, min_size=0.01) == []


def test_empty_or_uniform_book_has_no_walls():
    assert detect_walls([], size_multiple=3.0, min_size=0.0) == []
    uniform = [_bin(100.0, 5.0), _bin(99.0, 5.0), _bin(98.0, 5.0)]
    assert detect_walls(uniform, size_multiple=3.0, min_size=0.0) == []  # none exceeds 3x median
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/walls.py`:**

```python
# src/pavilos/detection/walls.py
"""Detect liquidity walls: bins that stand out vs the side's typical depth."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import median

from pavilos.core.models import DepthBin


@dataclass(slots=True, frozen=True)
class WallBin:
    """A bin flagged as a wall, with its prominence (size / side median)."""

    bin: DepthBin
    prominence: float


def detect_walls(bins, *, size_multiple: float, min_size: float) -> list[WallBin]:
    """Return the bins whose size is >= ``size_multiple`` x the median size of all
    ``bins`` AND >= ``min_size``. ``prominence`` = size / median. Empty/uniform
    books yield no walls. ``bins`` is one side (bids or asks) of a snapshot."""
    sizes = [b.size for b in bins]
    if not sizes:
        return []
    med = median(sizes)
    if med <= 0:
        return []
    threshold = size_multiple * med
    walls: list[WallBin] = []
    for b in bins:
        if b.size >= threshold and b.size >= min_size:
            walls.append(WallBin(bin=b, prominence=b.size / med))
    return walls
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite → 102 passed. **Step 6:** Commit `feat(detection): add median-multiple wall detection`.

---

## Task 3: Cluster walls into zones

**Files:** Create `src/pavilos/detection/clusters.py`; Test `tests/unit/test_clusters.py`.

> Adjacent walls (within `max_gap_bps` of each other) form one zone (a "wide
> wall"). A `RawZone` aggregates the cluster: `low`/`high` price bounds,
> strength-weighted `price`, total `strength`, and merged `venues`.

- [ ] **Step 1: Failing test — `tests/unit/test_clusters.py`:**

```python
# tests/unit/test_clusters.py
from pavilos.core.models import DepthBin
from pavilos.detection.walls import WallBin
from pavilos.detection.clusters import cluster_walls, RawZone


def _wall(price, size, venues=("kraken",)):
    comp = {v: size / len(venues) for v in venues}
    return WallBin(bin=DepthBin(price=price, size=size, composition=comp), prominence=size)


def test_isolated_wall_is_its_own_zone():
    zones = cluster_walls([_wall(100.0, 5.0)], mid=101.0, max_gap_bps=50.0)
    assert len(zones) == 1
    z = zones[0]
    assert isinstance(z, RawZone)
    assert z.low == 100.0 and z.high == 100.0 and z.strength == 5.0
    assert z.price == 100.0 and z.venues == ("kraken",)


def test_adjacent_walls_merge_into_one_zone_strength_weighted_price():
    # two walls $0.5 apart at ~100k-scale... use small prices: gap in bps from mid=100
    walls = [_wall(100.0, 2.0, ("kraken",)), _wall(99.95, 6.0, ("binance",))]
    zones = cluster_walls(walls, mid=101.0, max_gap_bps=20.0)  # ~ (0.05/101)*1e4 ~ 4.95 bps gap < 20
    assert len(zones) == 1
    z = zones[0]
    assert z.low == 99.95 and z.high == 100.0
    assert z.strength == 8.0
    # strength-weighted price = (100.0*2 + 99.95*6)/8
    assert abs(z.price - (100.0 * 2.0 + 99.95 * 6.0) / 8.0) < 1e-9
    assert set(z.venues) == {"kraken", "binance"}


def test_far_apart_walls_stay_separate():
    walls = [_wall(100.0, 5.0), _wall(95.0, 5.0)]
    zones = cluster_walls(walls, mid=101.0, max_gap_bps=20.0)  # 5.0 apart -> ~495 bps >> 20
    assert len(zones) == 2
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/clusters.py`:**

```python
# src/pavilos/detection/clusters.py
"""Group adjacent wall bins into zones."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.detection.walls import WallBin


@dataclass(slots=True, frozen=True)
class RawZone:
    """A clustered zone before lifecycle/confidence: price bounds, strength-
    weighted representative price, total strength, and contributing venues."""

    low: float
    high: float
    price: float
    strength: float
    venues: tuple[str, ...]


def cluster_walls(walls, *, mid: float, max_gap_bps: float) -> list[RawZone]:
    """Cluster walls whose adjacent price gap is within ``max_gap_bps`` (relative
    to ``mid``) into single zones. Returns zones sorted by price descending."""
    if not walls:
        return []
    ordered = sorted(walls, key=lambda w: w.bin.price, reverse=True)
    max_gap = mid * max_gap_bps / 1e4
    groups: list[list[WallBin]] = [[ordered[0]]]
    for w in ordered[1:]:
        if groups[-1][-1].bin.price - w.bin.price <= max_gap:
            groups[-1].append(w)
        else:
            groups.append([w])
    zones: list[RawZone] = []
    for group in groups:
        strength = sum(w.bin.size for w in group)
        low = min(w.bin.price for w in group)
        high = max(w.bin.price for w in group)
        price = sum(w.bin.price * w.bin.size for w in group) / strength if strength else high
        venues: dict[str, None] = {}
        for w in group:
            for v in w.bin.composition:
                venues[v] = None
        zones.append(RawZone(low=low, high=high, price=price, strength=strength, venues=tuple(venues)))
    return zones
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite → 105 passed. **Step 6:** Commit `feat(detection): cluster adjacent walls into zones`.

---

## Task 4: Lifecycle tracker (persistence + pulled)

**Files:** Create `src/pavilos/detection/lifecycle.py`; Test `tests/unit/test_lifecycle.py`.

> `ZoneTracker` keeps zone identity across snapshots. On each `update(raw_zones,
> mid, ts)` it MATCHES new zones to tracked ones by price-range overlap; a match
> accumulates `persistence_s = ts - first_seen`. A tracked zone NOT matched this
> round is "gone" — if price never entered its range while it existed, it's
> flagged `pulled` (spoof-like) and reported once before being dropped. Returns
> `TrackedZone`s (raw zone + first_seen/persistence_s + pulled).

- [ ] **Step 1: Failing test — `tests/unit/test_lifecycle.py`:**

```python
# tests/unit/test_lifecycle.py
from pavilos.detection.clusters import RawZone
from pavilos.detection.lifecycle import ZoneTracker, TrackedZone


def _z(low, high, strength=5.0, venues=("kraken",)):
    return RawZone(low=low, high=high, price=(low + high) / 2, strength=strength, venues=venues)


def test_persistence_accumulates_across_matched_snapshots():
    t = ZoneTracker(match_overlap_bps=10.0)
    out1 = t.update([_z(99.5, 100.5)], mid=101.0, ts=1.0)
    assert out1[0].persistence_s == 0.0 and out1[0].pulled is False
    out2 = t.update([_z(99.6, 100.6)], mid=101.0, ts=3.0)  # overlaps -> same zone
    assert len(out2) == 1
    assert out2[0].persistence_s == 2.0  # 3.0 - 1.0
    assert out2[0].pulled is False


def test_disappeared_zone_with_price_away_is_flagged_pulled():
    t = ZoneTracker(match_overlap_bps=10.0)
    t.update([_z(99.5, 100.5)], mid=101.0, ts=1.0)   # support well below mid 101
    # next snapshot: zone gone, price (mid) still above it -> pulled
    out = t.update([], mid=101.0, ts=2.0)
    pulled = [z for z in out if z.pulled]
    assert len(pulled) == 1 and pulled[0].low == 99.5
    # it's reported once then forgotten
    out2 = t.update([], mid=101.0, ts=3.0)
    assert out2 == []


def test_disappeared_zone_after_price_reached_it_is_not_pulled():
    t = ZoneTracker(match_overlap_bps=10.0)
    t.update([_z(100.0, 100.6)], mid=101.0, ts=1.0)
    # price drops into the zone (mid now 100.3, inside [100.0,100.6]) then zone gone next tick
    t.update([_z(100.0, 100.6)], mid=100.3, ts=2.0)   # price reached it
    out = t.update([], mid=100.3, ts=3.0)             # now gone, but it WAS reached
    assert all(not z.pulled for z in out)             # consumed, not pulled -> no pulled flag
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/lifecycle.py`:**

```python
# src/pavilos/detection/lifecycle.py
"""Track zone identity across snapshots: persistence + pulled (spoof) detection."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.detection.clusters import RawZone


@dataclass(slots=True, frozen=True)
class TrackedZone:
    """A RawZone enriched with lifecycle: how long it has existed and whether it
    was pulled (vanished without price ever entering its range)."""

    zone: RawZone
    first_seen: float
    persistence_s: float
    pulled: bool


class _Live:
    __slots__ = ("zone", "first_seen", "reached")

    def __init__(self, zone: RawZone, first_seen: float) -> None:
        self.zone = zone
        self.first_seen = first_seen
        self.reached = False  # has price ever entered this zone's range?


class ZoneTracker:
    """Matches incoming zones to live ones by price-range overlap (within
    ``match_overlap_bps`` of mid). Accumulates persistence; flags a vanished zone
    as ``pulled`` iff price never reached it while it was alive."""

    def __init__(self, *, match_overlap_bps: float = 10.0) -> None:
        self._match_bps = match_overlap_bps
        self._live: list[_Live] = []

    def update(self, raw_zones, mid: float, ts: float) -> list[TrackedZone]:
        tol = mid * self._match_bps / 1e4
        live = self._live
        matched: list[bool] = [False] * len(live)
        out: list[TrackedZone] = []
        new_live: list[_Live] = []

        for rz in raw_zones:
            idx = _best_match(rz, live, matched, tol)
            if idx is not None:
                matched[idx] = True
                cur = live[idx]
                cur.zone = rz
                if rz.low <= mid <= rz.high:
                    cur.reached = True
                out.append(TrackedZone(rz, cur.first_seen, ts - cur.first_seen, pulled=False))
                new_live.append(cur)
            else:
                fresh = _Live(rz, ts)
                if rz.low <= mid <= rz.high:
                    fresh.reached = True
                out.append(TrackedZone(rz, ts, 0.0, pulled=False))
                new_live.append(fresh)

        # zones that vanished this round: pulled iff never reached by price
        for was_matched, cur in zip(matched, live):
            if not was_matched and not cur.reached:
                out.append(TrackedZone(cur.zone, cur.first_seen, ts - cur.first_seen, pulled=True))

        self._live = new_live
        return out


def _best_match(rz: RawZone, live: list[_Live], matched: list[bool], tol: float) -> int | None:
    for i, cur in enumerate(live):
        if matched[i]:
            continue
        # overlap if ranges touch within tolerance
        if rz.low - tol <= cur.zone.high and rz.high + tol >= cur.zone.low:
            return i
    return None
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite → 108 passed. **Step 6:** Commit `feat(detection): add ZoneTracker (persistence + pulled detection)`.

---

## Task 5: Confidence scoring

**Files:** Create `src/pavilos/detection/confidence.py`; Test `tests/unit/test_confidence.py`.

> `score_zone(tracked, *, mid, window_bps, persistence_target_s, venues_target,
> strength_target)` returns a 0..1 confidence as the product of four factors —
> persistence, venue corroboration, strength, and proximity-to-mid — with a hard
> override: a `pulled` zone scores 0.0 (spoof). All factors clamp to [0, 1].

- [ ] **Step 1: Failing test — `tests/unit/test_confidence.py`:**

```python
# tests/unit/test_confidence.py
import pytest

from pavilos.detection.clusters import RawZone
from pavilos.detection.lifecycle import TrackedZone
from pavilos.detection.confidence import score_zone


def _tracked(persistence_s=10.0, venues=("kraken", "binance", "coinbase"), strength=10.0,
             low=99.5, high=100.5, pulled=False):
    rz = RawZone(low=low, high=high, price=(low + high) / 2, strength=strength, venues=venues)
    return TrackedZone(rz, first_seen=0.0, persistence_s=persistence_s, pulled=pulled)


_PARAMS = dict(window_bps=200.0, persistence_target_s=10.0, venues_target=3.0, strength_target=10.0)


def test_strong_zone_scores_high():
    s = score_zone(_tracked(), mid=100.5, **_PARAMS)
    assert 0.0 < s <= 1.0
    assert s > 0.8  # at/above all targets, near mid


def test_pulled_zone_scores_zero():
    assert score_zone(_tracked(pulled=True), mid=100.5, **_PARAMS) == 0.0


def test_confidence_in_unit_interval_and_monotone_in_persistence():
    low = score_zone(_tracked(persistence_s=1.0), mid=100.5, **_PARAMS)
    high = score_zone(_tracked(persistence_s=10.0), mid=100.5, **_PARAMS)
    assert 0.0 <= low <= high <= 1.0


def test_single_venue_scores_lower_than_multi_venue():
    one = score_zone(_tracked(venues=("kraken",)), mid=100.5, **_PARAMS)
    many = score_zone(_tracked(venues=("kraken", "binance", "coinbase")), mid=100.5, **_PARAMS)
    assert one < many
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/confidence.py`:**

```python
# src/pavilos/detection/confidence.py
"""Confidence score (0..1) for a tracked zone. Pure."""
from __future__ import annotations

from pavilos.detection.lifecycle import TrackedZone


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def score_zone(
    tracked: TrackedZone,
    *,
    mid: float,
    window_bps: float,
    persistence_target_s: float,
    venues_target: float,
    strength_target: float,
) -> float:
    """Confidence = persistence x venues x strength x proximity, each clamped to
    [0,1]. A pulled (spoof-like) zone scores 0.0 regardless of the rest."""
    if tracked.pulled:
        return 0.0
    z = tracked.zone
    persistence = _clamp01(tracked.persistence_s / persistence_target_s) if persistence_target_s > 0 else 1.0
    venues = _clamp01(len(z.venues) / venues_target) if venues_target > 0 else 1.0
    strength = _clamp01(z.strength / strength_target) if strength_target > 0 else 1.0
    # proximity: 1.0 at mid, decaying to 0.0 at window_bps away
    half_window = mid * window_bps / 1e4
    distance = abs(z.price - mid)
    proximity = _clamp01(1.0 - distance / half_window) if half_window > 0 else 1.0
    return _clamp01(persistence * venues * strength * proximity)
```

- [ ] **Step 4:** run → 4 passed. **Step 5:** full suite → 112 passed. **Step 6:** Commit `feat(detection): add zone confidence scoring (pulled -> 0)`.

---

## Task 6: Detector (orchestration)

**Files:** Create `src/pavilos/detection/detector.py`; Test `tests/unit/test_detector.py`.

> `Detector.update(snapshot) -> DepthAnalysis` runs the pipeline: detect walls on
> bids (→ supports) and asks (→ resistances), cluster each, track lifecycle
> (one tracker per side), score, build `Zone`s, and return them sorted by
> confidence descending. Bids/asks are tracked separately so a support and a
> resistance never match each other.

- [ ] **Step 1: Failing test — `tests/unit/test_detector.py`:**

```python
# tests/unit/test_detector.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.models import Side
from pavilos.detection.detector import Detector


def _bin(price, size, venues=("kraken", "binance")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("kraken", "binance"), venues_total=2)


def _detector():
    return Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, match_overlap_bps=10.0,
                    window_bps=500.0, persistence_target_s=5.0, venues_target=2.0, strength_target=5.0)


def test_detects_support_and_resistance_walls():
    d = _detector()
    bids = [_bin(100.0, 1.0), _bin(99.5, 10.0), _bin(99.0, 1.0)]   # 99.5 is the support wall
    asks = [_bin(100.5, 1.0), _bin(101.0, 12.0), _bin(101.5, 1.0)]  # 101.0 is the resistance wall
    a = d.update(_snap(1.0, 100.25, bids, asks))
    assert len(a.supports) == 1 and a.supports[0].side is Side.SUPPORT
    assert abs(a.supports[0].price - 99.5) < 1e-9
    assert len(a.resistances) == 1 and a.resistances[0].side is Side.RESISTANCE
    assert abs(a.resistances[0].price - 101.0) < 1e-9


def test_persistence_raises_confidence_over_two_snapshots():
    d = _detector()
    bids = [_bin(100.0, 1.0), _bin(99.5, 10.0), _bin(99.0, 1.0)]
    asks = [_bin(100.5, 1.0)]
    a1 = d.update(_snap(1.0, 100.25, bids, asks))
    a2 = d.update(_snap(6.0, 100.25, bids, asks))   # +5s, same support persists
    assert a2.supports[0].confidence >= a1.supports[0].confidence
    assert a2.supports[0].persistence_s == 5.0


def test_supports_sorted_by_confidence_desc():
    d = _detector()
    # two support walls; the multi-venue, bigger one should rank first
    bids = [_bin(100.0, 12.0, ("kraken", "binance")), _bin(95.0, 6.0, ("kraken",)), _bin(99.0, 1.0)]
    asks = [_bin(101.0, 1.0)]
    a = d.update(_snap(1.0, 100.5, bids, asks))
    confs = [z.confidence for z in a.supports]
    assert confs == sorted(confs, reverse=True)
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/detector.py`:**

```python
# src/pavilos/detection/detector.py
"""Detector: combined depth snapshot -> ranked support/resistance zones."""
from __future__ import annotations

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.detection.walls import detect_walls
from pavilos.detection.clusters import cluster_walls
from pavilos.detection.lifecycle import ZoneTracker, TrackedZone
from pavilos.detection.confidence import score_zone


class Detector:
    """Stateful across snapshots only via two ZoneTrackers (bids/asks)."""

    def __init__(
        self,
        *,
        size_multiple: float,
        min_size: float,
        max_gap_bps: float,
        match_overlap_bps: float,
        window_bps: float,
        persistence_target_s: float,
        venues_target: float,
        strength_target: float,
    ) -> None:
        self._p = dict(size_multiple=size_multiple, min_size=min_size, max_gap_bps=max_gap_bps,
                       window_bps=window_bps, persistence_target_s=persistence_target_s,
                       venues_target=venues_target, strength_target=strength_target)
        self._support_tracker = ZoneTracker(match_overlap_bps=match_overlap_bps)
        self._resist_tracker = ZoneTracker(match_overlap_bps=match_overlap_bps)

    def update(self, snapshot: CombinedDepthSnapshot) -> DepthAnalysis:
        mid = snapshot.mid
        supports = self._side(snapshot.bids, mid, snapshot.ts, Side.SUPPORT, self._support_tracker)
        resistances = self._side(snapshot.asks, mid, snapshot.ts, Side.RESISTANCE, self._resist_tracker)
        return DepthAnalysis(ts=snapshot.ts, mid=mid, supports=supports, resistances=resistances)

    def _side(self, bins, mid, ts, side, tracker) -> tuple[Zone, ...]:
        walls = detect_walls(bins, size_multiple=self._p["size_multiple"], min_size=self._p["min_size"])
        raw = cluster_walls(walls, mid=mid, max_gap_bps=self._p["max_gap_bps"])
        tracked = tracker.update(raw, mid=mid, ts=ts)
        zones = [self._to_zone(t, mid, side) for t in tracked]
        zones.sort(key=lambda z: z.confidence, reverse=True)
        return tuple(zones)

    def _to_zone(self, t: TrackedZone, mid: float, side: Side) -> Zone:
        conf = score_zone(t, mid=mid, window_bps=self._p["window_bps"],
                          persistence_target_s=self._p["persistence_target_s"],
                          venues_target=self._p["venues_target"], strength_target=self._p["strength_target"])
        z = t.zone
        return Zone(side=side, price=z.price, low=z.low, high=z.high, strength=z.strength,
                    venues=z.venues, persistence_s=t.persistence_s, pulled=t.pulled, confidence=conf)
```

- [ ] **Step 4:** run → 3 passed. **Step 5:** full suite → 115 passed. **Step 6:** Commit `feat(detection): add Detector orchestrating walls->clusters->lifecycle->confidence`.

---

## Task 7: Full suite green + close-out

- [ ] **Step 1:** `python -m pytest -v` → ALL pass (115: 96 prior + 19 new — models 3, walls 3, clusters 3, lifecycle 3, confidence 4, detector 3).
- [ ] **Step 2:** `git status` clean.
- [ ] **Step 3:** `git tag m2-detection && git log --oneline -8`.

---

## Self-Review (performed by plan author)

**Spec coverage (spec §5.3 detection):**
- Walls (size ≫ local depth) → Task 2 ✅; clusters (contiguous heavy bins → zones) → Task 3 ✅; lifecycle (appeared/persistence/pulled) → Task 4 ✅; confidence (persistence, venues, strength, distance) → Task 5 ✅; ranked SupportZone/ResistanceZone output with composition → Tasks 1, 6 ✅; operates on the combined USD-normalized snapshot → Task 6 ✅.
- **Book-only anti-spoofing** (persistence gate + pulled detection + venue corroboration) → Tasks 4, 5 ✅. *Deferred (correctly): trade-confirmed consumed-vs-pulled (needs a trade-feed milestone).*
- *Deferred to M2-wire / M3:* wiring the Detector into the Engine (consume the snapshot queue → emit DepthAnalysis); parameter calibration against real data; signal generation (M3) and dashboard/Telegram (M4).

**Placeholder scan:** none; every step has full runnable code.

**Type consistency:** `Zone(side, price, low, high, strength, venues, persistence_s, pulled, confidence)`, `DepthAnalysis(ts, mid, supports, resistances)`, `WallBin(bin, prominence)`, `RawZone(low, high, price, strength, venues)`, `TrackedZone(zone, first_seen, persistence_s, pulled)`, `detect_walls(bins, *, size_multiple, min_size)`, `cluster_walls(walls, *, mid, max_gap_bps)`, `ZoneTracker.update(raw_zones, mid, ts)`, `score_zone(tracked, *, mid, window_bps, persistence_target_s, venues_target, strength_target)`, `Detector.update(snapshot)` — consistent across Tasks 1–6.

**Calibration note:** thresholds/targets (`size_multiple`, `min_size`, `max_gap_bps`, `match_overlap_bps`, persistence/venues/strength targets, `window_bps`) are constructor params with reasonable defaults in tests; their production VALUES need calibration against live data in M3. The tests pin the LOGIC (a wall is detected, a pulled zone scores 0, persistence raises confidence, multi-venue beats single), not specific calibrated numbers.

**Adversarial focus (3rd barrier):** each task's adversarial pass should try — a wall that inflates its own median (does it still get detected?); a zone that flickers (matched/unmatched alternating — does persistence reset wrongly or pulled mis-fire?); price exactly at a zone boundary (reached or not?); confidence > 1 or < 0 from extreme inputs; two zones merging/splitting across snapshots (identity confusion); empty snapshot / single-bin side; NaN already guarded upstream but confirm no division-by-zero (median 0, strength 0, half_window 0 are all guarded).
