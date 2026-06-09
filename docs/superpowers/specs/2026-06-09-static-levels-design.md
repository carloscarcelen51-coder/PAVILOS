# PAVILOS — Static Support/Resistance Levels (Design Spec)

**Status:** DESIGN (awaiting approval before plan/build). **Date:** 2026-06-09.
**Supersedes** the confluence-near-price approach for the entry signal, after the M13 finding.

## 1. Why (the M13 finding)
The current Detector flags the **near-touch** bid/ask liquidity as "supports" — always ~2bps from mid, **trailing price**. That is not a level to bounce off (it is always there). A diagnostic over the lake confirmed: 100% of snapshots have a "support" ≤7bps below price; meanwhile genuine **static** multi-venue walls (e.g. $63,150 with 9 venues present 83% of the time, at a FIXED price 37bps below) sit there persistently but the current detector/strategy never trades them as such. Decision: detect **static levels** — fixed-price, multi-venue walls that price moves AWAY from and later RETURNS to.

## 2. What a static level IS (vs near-touch)
A **StaticLevel** is an absolute USD price `L` where a significant **multi-venue** wall has **persisted at that fixed price across time** (accumulated presence), and which price has **moved away from** (so it is *behind* price, not the trailing near-touch). It is the consensus level many exchanges defend — the unique multi-venue signal.

- **Tracked in absolute price space** (not relative to mid): a level at $63,150 stays at $63,150 as mid moves.
- **Persistence at a fixed price** is the core: presence accrues each snapshot a multi-venue wall sits at `L`; it **decays** when the wall is gone (eaten / pulled).
- **Strength** = f(presence-duration, max venues, size-time). High strength = a level confirmed by many venues over a long time.

## 3. Detection: StaticLevelTracker (stateful, causal)
Consumes combined snapshots in time order. Maintains `level -> LevelState` keyed on price buckets (`level_bucket_usd`, e.g. $25, or bps-based):
- Per snapshot: find **walls** in the combined book (bin size > `size_multiple`×median, same as the Detector). For each wall at absolute price `p`, bucket to `L`; bump `LevelState[L]`: `presence += 1`, `venues |= wall.composition.keys()`, `size_time += size`, `last_seen_ts = ts`.
- **Decay:** each snapshot, levels not refreshed lose presence (e.g. `presence *= decay` or drop after `stale_s` without a wall) — a static level must keep being defended.
- A level's **strength score** (0..1): venue-consensus-dominant + persistence + size, like the confluence score but over the level's **accumulated time-presence** (not a single snapshot).
- **Active static support** = strength ≥ `level_threshold` AND `n_venues ≥ min_venues` AND it sits **below** current mid by ≥ `min_away_bps` (excludes the near-touch) and ≤ `max_reach_bps` (within reach). Mirrored: active static resistance above.

This is the key change: levels persist at fixed prices + the `min_away_bps` gate removes the trailing near-touch.

## 4. Bounce setup + entry (reuses M12 reversion + M-static levels)
- **Setup:** price, having been away, **falls back toward** an active static support (mid approaching `L` from above, within `entry_zone_bps` of the level). That is a genuine, independent bounce test (the level is fixed; each approach after moving away is a new event).
- **Entry:** market LONG at the approach (M12 `enter_market`); stop below `L` (ATR-floored). Mirrored SHORT at a static resistance.
- **Exit:** the two trailing modes from the confluence spec — `chandelier` and `support` — compared head-to-head. Ride up.
- **Frequency:** intrinsically LOW (only when price swings to a static level) — matches the "few but quality" philosophy. Paper trades only; no alerts.

## 5. Validation FIRST (Milestone A-static) — the honest gate
Before any entry/exit, a **forward-return study keyed on price-APPROACHES to static levels** (reusing the M13 study machinery, now with genuine independent episodes because the level is at a fixed price and price moves away/back):
- Replay the lake; run the StaticLevelTracker; detect each **approach episode** (mid enters `entry_zone_bps` of an active static support after being outside `min_away_bps`); record the forward MFE/MAE in R; bucket by the level's strength + n_venues vs baseline.
- **Crucially, run over windows WITH price movement** (the 20-min flat slice had 0 approaches to far levels). Use longer / higher-volatility slices; report the realized-volatility of the slice + episode N.
- **Verdict:** do bounces off strong static multi-venue levels show a positive R-expectancy that beats baseline, with adequate episode N? Honest small-N caveat as always; this is low-frequency so N accrues slowly (more recording + longer replays).

## 6. Architecture
```
src/pavilos/detection/static_levels.py   # StaticLevelTracker + StaticLevel (stateful, causal, decay) [NEW]
src/pavilos/backtest/static_study.py      # approach-episode forward-return study                       [NEW]
src/pavilos/signals/engine.py             # entry_mode="static_reversion": approach-to-static entry     [MODIFY, Milestone B]
scripts/analyze.py                        # static-study mode                                            [MODIFY]
```

## 7. Phasing
- **Milestone A-static (FIRST):** StaticLevelTracker + approach-episode forward-return study + CLI. Validate the static-level bounce thesis over moving-price windows. (No trading.)
- **Milestone B-static (if validated):** `static_reversion` entry + the two trailing modes (chandelier vs support) → paper trades + trade backtest.

## 8. Open decisions
1. **Level bucketing:** fixed $-bucket (e.g. $25) vs bps-relative (e.g. 4bps). Fixed-$ is simpler + truly static; bps-relative adapts to price scale. (Recommend fixed-$ for true static levels, configurable.)
2. **Decay model:** exponential presence decay vs hard drop after `stale_s`. (Recommend exponential — graceful.)
3. **`min_away_bps`** (how far a level must be from price to count as "static, not near-touch"): start ~20-30bps (excludes the ~2-7bps near-touch). Sweepable.

## 9. Honesty / correctness notes
- StaticLevelTracker is **causal** (presence accrues only from past snapshots; decay is causal).
- The study's level-strength at time T uses only data ≤ T; the forward return uses data > T (measured outcome, not a trade leak).
- Approach-episodes are genuinely independent (fixed level + price away→back), fixing the M13 autocorrelation/1-episode problem — but only when price MOVES, so windows must have volatility (reported).
- Low frequency is inherent + acknowledged; validation accrues slowly.
