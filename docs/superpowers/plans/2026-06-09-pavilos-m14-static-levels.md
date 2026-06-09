# PAVILOS M14: Static Level Tracker + Approach Validation Study (Milestone A-static)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Detect **static** support/resistance levels (multi-venue walls at FIXED absolute prices that price moves AWAY from), and validate the bounce thesis with a forward-return study keyed on price **approaches** to those levels — fixing the M13 finding that detected "supports" were the near-touch bid trailing price. Validation FIRST; no trading. (Milestone A-static of the static-levels spec.)

**Architecture:** `static_levels.py` — a stateful, causal `StaticLevelTracker` accrues wall presence at absolute price buckets over time (with staleness pruning), records each level's `max_away_bps` (max distance from mid during its life — the near-touch discriminator), and exposes active static supports/resistances (strong + multi-venue + price has been away). `static_study.py` replays the lake, runs the tracker, samples ONE forward observation per price-**approach episode** (mid returns to within `entry_zone_bps` of an active static level after being away), measures MFE/MAE in R, buckets by level strength vs baseline, and reports the slice's realized volatility + episode N. A CLI prints the table. Causal score, forward-only outcome (signal validation, not a trade leak).

**Tech Stack:** Python 3.13, reuses merged M1–M13 (`CombinedDepthSnapshot`, M11 `replay_snapshots`, M13 study patterns: R-math, summarize). `pytest`.

---

## Correctness foundation (study-first verified concept)
- A diagnostic over the lake CONFIRMED static multi-venue levels exist (e.g. a 9-venue wall 37bps below mid present 83% of 20 min, at a FIXED price), distinct from the near-touch (always ~2-7bps, trailing). The flat 20-min slice had 0 approaches to far levels → bounce setups need price MOVEMENT; the study must run over volatile windows + report volatility.
- **Near-touch discriminator:** track each level's `max_away_bps` = max over its life of |level − mid|/mid·1e4. A STATIC level has been ≥ `min_away_bps` from price (price left it); the near-touch never gets far from mid → excluded. This works in moving markets (the near-touch smears across buckets; a static level accrues at one fixed bucket).
- **Causal:** presence accrues only from past snapshots; pruning is causal. The study's level strength at T uses only data ≤ T; the forward MFE/MAE uses only data > T.
- **Independent episodes:** ONE observation per price-approach to a static level (mid enters `entry_zone_bps` after being outside it), with hysteresis — fixing M13's autocorrelation/1-episode collapse, because the level is at a FIXED price and price genuinely moves away/back.

---

## File Structure
```
src/pavilos/detection/static_levels.py    # StaticLevelTracker + StaticLevel + StaticLevelConfig [NEW]
src/pavilos/backtest/static_study.py        # approach-episode forward-return study              [NEW]
scripts/analyze.py                          # + static-study mode                                 [MODIFY]
tests/unit/test_static_levels.py
tests/unit/test_static_study.py
tests/unit/test_analyze_cli.py              # + static-study row [MODIFY]
```

---

## Task 1: StaticLevelTracker

**Files:** Create `src/pavilos/detection/static_levels.py`; Test `tests/unit/test_static_levels.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_static_levels.py`:**
```python
# tests/unit/test_static_levels.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.static_levels import StaticLevelTracker, StaticLevelConfig


def _bin(price, size, venues):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks=()):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=14)


def _cfg(**o):
    kw = dict(level_bucket_usd=25.0, size_multiple=3.0, stale_s=30.0, min_venues=6,
              level_threshold=0.0, min_away_bps=25.0, max_reach_bps=400.0,
              venues_target=8.0, duration_target_s=10.0)
    kw.update(o)
    return StaticLevelConfig(**kw)


def test_accrues_presence_at_a_fixed_level_and_unions_venues():
    trk = StaticLevelTracker(_cfg(min_venues=1))
    big = ("k", "b", "o", "x", "g", "m", "h")    # 7 venues
    for ts in range(0, 10):                       # a wall sits at 62700 for 10 ticks while mid=63000
        trk.update(_snap(float(ts), 63000.0,
                         [_bin(62700.0, 50.0, big), _bin(62995.0, 1.0, ("k",))]))
    sup = trk.active_supports(mid=63000.0, now=9.0)
    assert any(abs(s.price - 62700.0) <= 25.0 for s in sup)
    s = next(s for s in sup if abs(s.price - 62700.0) <= 25.0)
    assert s.n_venues == 7 and s.presence >= 10
    # this level is ~48bps below mid -> max_away_bps >= min_away (a real static level)
    assert s.max_away_bps >= 25.0


def test_near_touch_excluded_by_min_away():
    trk = StaticLevelTracker(_cfg(min_venues=1))
    big = ("k", "b", "o", "x", "g", "m", "h")
    # a big wall always ~2bps below mid (the near-touch); mid drifts but wall stays ~2bps away
    for i in range(20):
        mid = 63000.0 + i
        trk.update(_snap(float(i), mid, [_bin(mid - 12.0, 50.0, big)]))   # ~2bps below, trailing
    # near-touch never got >= min_away_bps from mid -> not an active static support
    sup = trk.active_supports(mid=63019.0, now=19.0)
    assert all(s.max_away_bps < 25.0 for s in sup) or sup == []


def test_prunes_stale_levels():
    trk = StaticLevelTracker(_cfg(min_venues=1, stale_s=5.0))
    big = ("k", "b", "o", "x", "g", "m", "h")
    trk.update(_snap(0.0, 63000.0, [_bin(62700.0, 50.0, big)]))
    # advance far past stale_s with no wall at 62700
    trk.update(_snap(20.0, 63000.0, [_bin(62800.0, 50.0, big)]))
    sup = trk.active_supports(mid=63000.0, now=20.0)
    assert all(abs(s.price - 62700.0) > 25.0 for s in sup)   # 62700 pruned (stale)


def test_strength_rises_with_venues_and_duration():
    big = ("k", "b", "o", "x", "g", "m", "h", "i")
    weak_trk = StaticLevelTracker(_cfg(min_venues=1))
    weak_trk.update(_snap(0.0, 63000.0, [_bin(62700.0, 50.0, ("k",))]))
    strong_trk = StaticLevelTracker(_cfg(min_venues=1))
    for ts in range(0, 30):
        strong_trk.update(_snap(float(ts), 63000.0, [_bin(62700.0, 50.0, big)]))
    w = weak_trk.active_supports(63000.0, 0.0)
    s = strong_trk.active_supports(63000.0, 29.0)
    assert s and (not w or s[0].strength > w[0].strength)
```
  NOTE to implementer: READ `src/pavilos/core/models.py` for `DepthBin`/`CombinedDepthSnapshot` and `src/pavilos/detection/models.py` for `Side` (SUPPORT/RESISTANCE enum). `StaticLevel.side` reuses `Side`. Adapt builders to real fields if needed (do not change production types).

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/static_levels.py`:**
  - `@dataclass(frozen=True) StaticLevelConfig(level_bucket_usd, size_multiple, stale_s, min_venues, level_threshold, min_away_bps, max_reach_bps, venues_target, duration_target_s)`.
  - `@dataclass(frozen=True) StaticLevel(price, side, strength, venues: tuple, n_venues, presence, duration_s, max_away_bps, last_seen_ts)`.
  - Internal mutable `_LS` per bucket: `first_seen, last_seen, presence(int), venues(set), size_sum(float), max_away_bps(float)`.
  - `update(snapshot)`:
    1. `now = snapshot.ts`; `med = median(bin.size for bin in bids+asks)` (guard empty/0).
    2. For each wall bin (`size > size_multiple*med`) in bids+asks: `L = round(price/level_bucket_usd)*level_bucket_usd`; get/create `_LS[L]`; `presence += 1`, `last_seen = now`, `venues |= comp.keys()`, `size_sum += size`, `first_seen` set once; update `max_away_bps = max(max_away_bps, abs(L - mid)/mid*1e4)`.
    3. Prune: drop `_LS[L]` where `now - last_seen > stale_s`.
  - `strength(ls, now)` (0..1): `clamp(0.5*min(n_venues/venues_target,1) + 0.3*min(duration_s/duration_target_s,1) + 0.2*min(presence/ (duration_s* refresh_hz or presence_cap),1), 0,1)` — venue-dominant + persistence-duration + defense consistency. (Keep interpretable; exact form in code.)
  - `active_supports(mid, now)`: levels with `L < mid`, `min_away_bps <= (mid-L)/mid*1e4 <= max_reach_bps`, `strength >= level_threshold`, `n_venues >= min_venues`, `max_away_bps >= min_away_bps` (price has been away → static, not near-touch); return `StaticLevel`s sorted by strength desc. `active_resistances` mirrored (L > mid).

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(detection): add StaticLevelTracker (fixed-price multi-venue levels, near-touch-excluded)`.

---

## Task 2: Approach-episode forward-return study

**Files:** Create `src/pavilos/backtest/static_study.py`; Test `tests/unit/test_static_study.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_static_study.py`** (synthetic snapshots; build a static support that price moves away from then RETURNS to, with a subsequent bounce; assert ONE approach episode + a positive R obs; a control where price breaks through → negative; price oscillating in/out with hysteresis does not double-count):
  Key surface: `study_static_approaches(snapshots, cfg) -> list[Obs]` (one per approach episode, with `level_strength`, `n_venues`, `mfe_r`, `mae_r`, `bounced`, `decided`); `summarize_static(obs, buckets) -> list[dict]` (per strength-bucket n / bounce_rate / expectancy_r / mean MFE,MAE + baseline row); `realized_vol_bps(snapshots)` helper for the slice-volatility report. Reuse M13's `Obs`/R-math/`summarize_study` where possible (import or mirror).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3: Implement — `static_study.py`:** `StaticStudyConfig(static: StaticLevelConfig, horizon_s, target_r, stop_offset_bps, atr_stop_mult, entry_zone_bps, episode_gap_s, buckets)`. `study_static_approaches`: feed snapshots to a `StaticLevelTracker` + an `ATR(window)` over mids; per snapshot, get `active_supports(mid, ts)`; a tradeable approach = an active static support with `(mid - L)/mid*1e4 <= entry_zone_bps` (price has returned to within the zone). **Episode:** opens when an approach to a level (by band) first occurs after being outside `entry_zone_bps`; ONE Obs at onset (entry=mid, stop=ATR-floored below L, R=entry−stop, scan forward to horizon for MFE/MAE/bounced/decided); the episode for that level closes when mid leaves `entry_zone_bps` for > `episode_gap_s` (hysteresis) → a later re-approach is a new episode. Causal score (level strength at onset), forward-only MFE/MAE. `summarize_static` buckets by level strength + baseline + expectancy_r (decided-only).
- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add static-level approach forward-return study`.

---

## Task 3: CLI static-study + close-out
- [ ] **Step 1:** Add `format_static_row` + `static-study [horizon_s] [t0 t1]` mode to `scripts/analyze.py` (replay → study_static_approaches → summarize_static → print bucket table + baseline + episode N + **realized-vol of the slice** + small-N caveat) + a CLI format test.
- [ ] **Step 2:** Run tests + `python -c "import scripts.analyze"`. **Step 3:** full suite (≈275 prior + ~10 new). **Step 4:** `git tag m14-static-levels`. **Step 5:** Commit `feat(scripts): add static-study CLI`.
- [ ] **Step 6 (operator):** find a VOLATILE slice (high realized-vol) in the lake and run `python -m scripts.analyze D:\pavilos_book_data static-study 120 <t0> <t1>` → does bounce-rate / expectancy_r RISE with level strength AND beat baseline, with adequate episode N? The verdict (over moving-price windows).

---

## Self-Review (plan author)
**Coverage:** tracker (T1, the new fixed-price detector with the near-touch discriminator) → approach study (T2) → CLI + verdict (T3). Directly fixes the M13 finding; validation-first.
**Correctness/honesty:** causal accrual + pruning; `max_away_bps` excludes the near-touch (the core fix); ONE obs per genuine approach episode (independent — level is fixed, price moves away/back); R-multiple expectancy + baseline; realized-vol + N reported (the flat-slice lesson). Matches the spec's Milestone A-static + anti-inflation discipline.
**Type consistency:** `StaticLevelTracker.update(snapshot)`, `active_supports/active_resistances(mid, now) -> list[StaticLevel]`; `StaticLevel(price, side, strength, venues, n_venues, presence, duration_s, max_away_bps, last_seen_ts)`; `study_static_approaches(snapshots, cfg) -> list[Obs]`; `summarize_static(obs, buckets) -> list[dict]`. Reuses M11 replay + M13 R-math/summarize + `Side` enum + `DepthBin`/`CombinedDepthSnapshot` (implementer verifies real fields).
**Adversarial focus (3rd barrier):** (1) **near-touch exclusion** — a wall trailing ~2bps below a drifting mid must NEVER become an active static support (max_away_bps stays < min_away_bps); a fixed wall price LEFT + returns to MUST (max_away_bps ≥ min_away_bps). This is the whole point — prove both. (2) **causal/no-leak** — level strength + max_away at T use only data ≤ T; forward MFE/MAE only > T. (3) **approach-episode independence** — price entering the zone once = ONE obs; oscillating in/out within `episode_gap_s` does NOT double-count (hysteresis); leaving for > gap then returning = TWO. (4) **pruning** — a level not refreshed for > stale_s is dropped (a wall that vanished is not "static"). (5) **R-math** (reused from M13) — bounce/stop/decided correct; gap-through-stop → mae_r ≤ −1, bounced False. (6) flat-slice → few/zero episodes (documented, not a bug); volatile-slice → episodes appear. Items (1) near-touch-exclusion and (3) episode-independence are the headline — they are the difference between detecting real static levels vs re-detecting the near-touch and inflating N.
