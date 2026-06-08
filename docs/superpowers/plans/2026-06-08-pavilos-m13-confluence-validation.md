# PAVILOS M13: Confluence Analyzer + Forward-Return Validation Study

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Build the **ConfluenceAnalyzer** (multi-venue support confluence) and a **forward-return validation study** that measures, over the recorded lake, whether high-confluence supports actually precede bounces — using MANY independent samples (one per cluster *episode*), sidestepping the few-trades problem. This validates (or refutes) the strategy thesis BEFORE building any entry/exit. (Milestone A of the confluence-strategy spec.)

**Architecture:** `confluence.py` turns a `DepthAnalysis` into `ConfluenceCluster`s (support zones merged within `confluence_band_bps`, scored by venue-consensus + stacking). `confluence_study.py` replays the lake (M11), runs the analyzer per snapshot, samples ONE forward observation per cluster **episode** (dedup consecutive same-band clusters), measures the forward path in **R-multiples** (MFE/MAE vs the stop risk) over a horizon, and aggregates **bounce-rate + expectancy by confluence-score bucket vs a baseline**. A CLI prints the table. Pure, faithful (reuses the verified M11 replay + Detector); the SETUP score is causal, the forward path is the measured outcome (legitimate signal validation, not a trading leak).

**Tech Stack:** Python 3.13, reuses merged M1–M12 (`Detector`, `DepthAnalysis`/`Zone`, M11 `replay_snapshots`). `pytest`.

---

## Correctness / honesty foundation
- **Causal setup, measured future:** the confluence score at time T uses only data ≤ T (the snapshot's zones). The forward return uses data > T — that is the *outcome being measured*, NOT a trading look-ahead. (We are testing a signal's predictive power, the standard way; we never trade on the future.)
- **Independent samples:** consecutive snapshots show the SAME persisting cluster → autocorrelated. We sample ONE observation per cluster **episode** (a tradeable cluster appearing near price, deduped while its band persists within `episode_gap_s`). Report episode-level N; never inflate with per-snapshot counts.
- **R-multiples:** measure MFE/MAE in units of the stop risk R = (entry − stop). "Bounce success" = MFE ≥ `target_R`·R reached BEFORE MAE ≤ −1R. Expectancy in R is the honest, scale-free verdict.
- **Baseline:** compare high-confluence buckets against low-confluence and against an all-snapshots baseline — confluence must BEAT baseline, not just be positive.
- Report N per bucket loudly; a great rate on tiny N is noise.

## Scope decisions
1. **Confluence = (a) stacking + (b) venue consensus** in the analyzer (stateless, pure, the novel core). **(c) historical-touches** is added as a SEPARATE causal `LevelHistory` factor the study reports alongside (so we see if it adds) — built here too, per "include all three", but kept out of the core analyzer so the analyzer stays pure/testable.
2. **No trading in M13** — pure measurement. Entry/exit/trailing are Milestone B.
3. **Reuse M11 replay verbatim** (faithful). The study is read-only over the lake.

**Deferred:** the paper strategy (entry + the two trailing modes), Sharpe, the dashboard surface.

---

## File Structure
```
src/pavilos/detection/confluence.py        # ConfluenceCluster + analyze_confluence(analysis, cfg) [NEW]
src/pavilos/detection/level_history.py      # LevelHistory: causal past-support memory -> touches  [NEW]
src/pavilos/backtest/confluence_study.py    # forward-return episode study over the lake           [NEW]
scripts/analyze.py                          # + confluence-study mode                                [MODIFY]
tests/unit/test_confluence.py
tests/unit/test_level_history.py
tests/unit/test_confluence_study.py
tests/unit/test_analyze_cli.py              # + study row format [MODIFY]
```

---

## Task 1: ConfluenceAnalyzer

**Files:** Create `src/pavilos/detection/confluence.py`; Test `tests/unit/test_confluence.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_confluence.py`:**
```python
# tests/unit/test_confluence.py
from pavilos.detection.models import Zone, Side, DepthAnalysis
from pavilos.detection.confluence import analyze_confluence, ConfluenceConfig

# REAL model (verified src/pavilos/detection/models.py):
#   Side = Enum SUPPORT="support" | RESISTANCE="resistance"
#   Zone(side, price, low, high, strength, venues, persistence_s, pulled, confidence)
#   DepthAnalysis(ts, mid, supports, resistances)   # NO atr field


def _z(side, low, high, conf, venues, persistence_s=30.0):
    return Zone(side=side, price=(low + high) / 2, low=low, high=high, strength=1.0,
                venues=tuple(venues), persistence_s=persistence_s, pulled=False, confidence=conf)


def _analysis(mid, supports, resistances=()):
    return DepthAnalysis(ts=1.0, mid=mid, supports=tuple(supports), resistances=tuple(resistances))


def test_merges_supports_within_band_and_unions_venues():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0,
                           threshold=0.0, min_venues=1)
    # two supports ~5bps apart at 63000 -> one cluster; venues union {k,b,o,x}
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.7, ("k", "b")),
                            _z(Side.SUPPORT, 62975, 62980, 0.6, ("o", "x"))])
    clusters = analyze_confluence(a, cfg)
    sup = [c for c in clusters if c.side == Side.SUPPORT]
    assert len(sup) == 1
    c = sup[0]
    assert c.n_zones == 2 and set(c.venues) == {"k", "b", "o", "x"} and c.n_venues == 4
    assert c.price_lo == 62975 and c.price_hi == 62985
    assert 0.0 <= c.score <= 1.0


def test_distant_supports_are_separate_clusters():
    cfg = ConfluenceConfig(confluence_band_bps=5.0, venues_target=8.0, threshold=0.0, min_venues=1)
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62900, 62905, 0.7, ("k",)),
                            _z(Side.SUPPORT, 62000, 62005, 0.7, ("b",))])  # ~140bps apart
    assert len([c for c in analyze_confluence(a, cfg) if c.side == Side.SUPPORT]) == 2


def test_score_rises_with_venues_and_stacking():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0, threshold=0.0, min_venues=1)
    weak = analyze_confluence(_analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.6, ("k",))]), cfg)[0]
    strong = analyze_confluence(_analysis(63000.0, [
        _z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b", "o", "x", "g")),
        _z(Side.SUPPORT, 62976, 62981, 0.8, ("m", "h"))]), cfg)[0]
    assert strong.score > weak.score


def test_tradeable_gate_filters_by_threshold_and_venues():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0, threshold=0.6, min_venues=6)
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b", "o", "x", "g", "m", "h"))])
    clusters = analyze_confluence(a, cfg)
    assert clusters[0].tradeable is True
    a2 = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b"))])  # only 2 venues
    assert analyze_confluence(a2, cfg)[0].tradeable is False
```
  NOTE to implementer: the test builders above already match the VERIFIED model in `src/pavilos/detection/models.py` — `Side.SUPPORT`/`Side.RESISTANCE` (enum), `Zone(side, price, low, high, strength, venues, persistence_s, pulled, confidence)`, `DepthAnalysis(ts, mid, supports, resistances)` (no `atr`). `ConfluenceCluster.side` should reuse `Side.SUPPORT`/`Side.RESISTANCE`. ATR (needed by the study, not the analyzer) is computed separately via the `ATR` class over mids, as `run_backtest` does — `DepthAnalysis` carries no atr.

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/detection/confluence.py`:** `@dataclass(frozen=True) ConfluenceConfig(confluence_band_bps, venues_target, threshold, min_venues, min_persistence_s=0.0)`; `@dataclass(frozen=True) ConfluenceCluster(side, price_lo, price_hi, n_zones, venues: tuple, n_venues, max_confidence, max_persistence_s, score, tradeable)`; `analyze_confluence(analysis, cfg) -> list[ConfluenceCluster]`:
  - For each side (supports, resistances): sort zones by price; greedily merge zones whose band is within `confluence_band_bps` (relative to mid) of the running cluster band into one cluster (union venues, max conf/persistence, count zones, extend lo/hi).
  - score = clamp(`0.6*min(n_venues/venues_target,1) + 0.4*min(n_zones,3)/3 * max_confidence`, 0, 1) — venue-consensus-dominant + stacking×quality. (Exact weights in the plan; keep interpretable.)
  - tradeable = score >= threshold AND n_venues >= min_venues AND max_persistence_s >= min_persistence_s.

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(detection): add ConfluenceAnalyzer (multi-venue support confluence)`.

---

## Task 2: LevelHistory (causal historical-touch factor)

**Files:** Create `src/pavilos/detection/level_history.py`; Test `tests/unit/test_level_history.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_level_history.py`:**
```python
from pavilos.detection.level_history import LevelHistory


def test_counts_only_past_distinct_episodes_within_band():
    h = LevelHistory(band_bps=20.0, episode_gap_s=60.0)
    # episode 1 at ~63000, ts 0..10
    for ts in (0.0, 5.0, 10.0):
        h.observe(price_level=63000.0, ts=ts)
    # touches at 63000 BEFORE a new episode: counts past distinct episodes (=1 so far, the current is ongoing)
    assert h.touches(63000.0, now=10.0) >= 0
    # a gap > episode_gap_s, then episode 2 -> now touches() sees 1 prior distinct episode
    h.observe(price_level=63010.0, ts=200.0)
    assert h.touches(63010.0, now=200.0) == 1          # one prior episode (~63000) within band
    # a far level has no history
    assert h.touches(61000.0, now=200.0) == 0


def test_touches_is_causal_ignores_future():
    h = LevelHistory(band_bps=20.0, episode_gap_s=60.0)
    h.observe(63000.0, ts=100.0)
    assert h.touches(63000.0, now=50.0) == 0           # nothing observed before t=50
```
  NOTE: `observe(price_level, ts)` records a support touch at a level/time; episodes are runs of touches at ~the same band separated by `> episode_gap_s`. `touches(level, now)` = count of DISTINCT past episodes (ended before `now`) whose level is within `band_bps` of `level`. Causal: ignores observations with ts >= now. Adapt assertions to a clean, documented semantics if the exact counts differ — the invariants that matter: (a) only PAST episodes counted, (b) distinct episodes (gap-separated) not every touch, (c) band-matched.

- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement `LevelHistory` (a list of episodes per band; `observe` extends/creates an episode; `touches` counts past distinct band-matching episodes ended before `now`). **Step 4:** pass. **Step 5:** full suite. **Step 6:** Commit `feat(detection): add causal LevelHistory (historical-touch factor)`.

---

## Task 3: Forward-return validation study

**Files:** Create `src/pavilos/backtest/confluence_study.py`; Test `tests/unit/test_confluence_study.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_confluence_study.py`** (synthetic snapshots, no lake): build a snapshot list where a strong confluence support sits below price and price then RISES (a bounce), assert the study records a positive forward observation for that episode and buckets it; build a control where price falls through, assert negative. Key asserts: `study_observations(snapshots, cfg, horizon_s, ...)` returns one obs per episode (NOT per snapshot — a 10-snapshot persisting cluster yields 1 obs), each obs has `confluence_score`, `mfe_r`, `mae_r`, `fwd_return_bps`, `bounced` (bool); and `summarize_study(observations)` returns per-bucket `{n, bounce_rate, mean_fwd_return_bps, mean_mfe_r, mean_mae_r, expectancy_r}`.
```python
# tests/unit/test_confluence_study.py  (sketch — implementer writes concrete synthetic snapshots)
from pavilos.backtest.confluence_study import study_observations, summarize_study, StudyConfig
# ... build snapshots with a deep multi-venue bid wall ~mid-10bps, price rising over the next snapshots ...
# obs = study_observations(snaps, StudyConfig(...)); assert len(obs) == 1 (one episode)
# assert obs[0].confluence_score > 0 and obs[0].mfe_r > 0 and obs[0].bounced in (True, False)
# s = summarize_study(obs); assert s buckets have n>=1 and expectancy_r is finite
```
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3: Implement — `confluence_study.py`:**
  - `@dataclass StudyConfig(confluence: ConfluenceConfig, horizon_s, target_r, stop_offset_bps, atr_stop_mult, entry_zone_bps, episode_gap_s, buckets)`.
  - `study_observations(snapshots, cfg) -> list[Obs]`: run a `Detector`? No — snapshots already feed `analyze_confluence` via a Detector. Actually: feed snapshots to a `Detector` to get `DepthAnalysis`, run `analyze_confluence`, find tradeable support clusters within `entry_zone_bps` of price. **Episode dedup:** track the currently-open episode per side+band; emit ONE Obs at episode onset; close it when the cluster lapses for > `episode_gap_s` or the band shifts. For each Obs at onset index i: entry = mid_i; stop = ATR-floored below the cluster; R = entry−stop; scan forward snapshots (i+1 .. up to horizon_s) computing MFE = max(mid−entry), MAE = min(mid−entry); `mfe_r = MFE/R`, `mae_r = MAE/R`, `fwd_return_bps` at horizon, `bounced = (mfe_r reaches target_r before mae_r reaches -1)`. Record `confluence_score`, components.
  - `summarize_study(observations, buckets) -> list[dict]`: bucket by confluence_score; per bucket compute N, bounce_rate, mean_fwd_return_bps, mean_mfe_r, mean_mae_r, expectancy_r (= mean of per-obs R outcome: +target_r if bounced else −1, or the realized mfe/mae-capped R). Plus an ALL/baseline row.
- [ ] **Step 4:** pass. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add confluence forward-return validation study`.

---

## Task 4: CLI confluence-study + close-out

**Files:** Modify `scripts/analyze.py`, `tests/unit/test_analyze_cli.py`.

- [ ] **Step 1:** Add `format_study_row(bucket_row)` (readable per-bucket line: score range, N, bounce%, expectancy R, mean fwd bps) + test (assert key fields in the string).
- [ ] **Step 2:** Add `confluence-study [horizon_s] [t0 t1]` mode to `main()`: replay at the configured window → `study_observations` → `summarize_study` → print per-bucket table + the baseline row + total episode N + the `< 1h` / small-N caveat.
- [ ] **Step 3:** Run tests + `python -c "import scripts.analyze"`. **Step 4:** full suite (≈255 prior + ~10 new). **Step 5:** `git tag m13-confluence-validation`. **Step 6:** Commit `feat(scripts): add confluence-study CLI`.
- [ ] **Step 7 (operator):** `python -m scripts.analyze D:\pavilos_book_data confluence-study 60 <t0> <t1>` on a slice → read the bucket table: does bounce-rate / expectancy_r RISE with confluence score, and BEAT the baseline, with adequate episode N? That is the theory verdict.

---

## Self-Review (plan author)
**Coverage:** analyzer (T1) → causal history (T2) → forward-return episode study (T3) → CLI + verdict (T4). Validates the confluence-bounce thesis with many independent samples before any strategy build.
**Honesty/correctness:** causal score, measured-future outcome (not a trade leak), per-EPISODE sampling (no autocorrelation inflation), R-multiple expectancy, baseline comparison, loud N. Matches the spec's Milestone A + the anti-inflation discipline.
**Type consistency:** `analyze_confluence(analysis, cfg) -> list[ConfluenceCluster]`; `ConfluenceCluster(side, price_lo, price_hi, n_zones, venues, n_venues, max_confidence, max_persistence_s, score, tradeable)`; `LevelHistory.observe/touches`; `study_observations(snapshots, cfg) -> list[Obs]`; `summarize_study(obs, buckets) -> list[dict]`. Reuses `Detector`, M11 `replay_snapshots`, `Zone`/`DepthAnalysis` (implementer verifies real field names first).
**Adversarial focus (3rd barrier):** (1) **causal score / no leak** — the confluence score + LevelHistory.touches at T use only data ≤ T; the forward window only data > T; prove they never cross. (2) **episode dedup** — a cluster persisting across K snapshots yields exactly ONE observation, not K (autocorrelation guard); a cluster that lapses + reforms after `episode_gap_s` yields two. (3) **MFE/MAE/R correctness** — on a hand-built rising path, mfe_r/mae_r/bounced match hand calc; a path that gaps straight to stop → mae_r ≤ −1, bounced False. (4) **bucketing + baseline** — buckets partition [0,1]; baseline row aggregates all; expectancy_r formula is the honest per-obs R. (5) **merge correctness** — zones exactly `confluence_band_bps` apart (boundary), single-zone clusters, empty analysis. (6) **horizon clipping** — an episode near the end of the snapshot list with < horizon forward data is handled (shorter window or dropped, documented). Items (1) no-leak and (2) episode-dedup are the headline — they are the difference between an honest validation and a misleading one.
