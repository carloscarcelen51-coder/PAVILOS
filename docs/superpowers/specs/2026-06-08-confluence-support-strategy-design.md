# PAVILOS — Confluence-Support Reversion Strategy (Design Spec)

**Status:** DESIGN (awaiting approval before plan/build).
**Date:** 2026-06-08.

## 1. Intent (the user's thesis)
Trade ONLY high-quality support setups, accept low frequency. Position at a support where **multiple support zones / many venues confluence**, wait for the bounce, and **ride the move up with a trailing stop** (let winners run). Mirror for resistances (short). This plays to PAVILOS's unique strength: real **multi-exchange confluence** that single-venue traders cannot see.

**For now: PAPER trades only — no alerts, no live trading.** The strategy makes paper trades on confluence setups (via the existing PaperBroker), so the whole thing is **testable** end-to-end (live paper + offline backtest). Alerts and live execution are explicitly out of scope here.

## 2. Honest framing (must be acknowledged up front)
"Few but high-quality trades" trades away fast statistical validation. With few trades you **cannot prove edge** with a backtest number (3-6 trades = noise, as M12 showed). So this is a **conviction + risk-managed** strategy: the bet rests on (a) the *logic* — multi-venue confluence = real liquidity = a support likely to hold — and (b) *risk management* — defined stop + trailing ride. The backtest's role shifts to **sanity-check (not bleeding) + R-multiple tracking**, and the edge verdict **accumulates slowly** (weeks/months of recording + periodic walk-forward). This is a legitimate, disciplined choice, taken with eyes open.

## 3. Core concept: the Confluence score
The Detector already yields support/resistance `Zone`s (each: confidence, persistence_s, venues set, price band). **Confluence** = agreement/stacking at a level, scored from three readings (the user's):

- **(a) Stacking** — number of distinct support zones whose bands fall within `confluence_band_bps` of each other (a thick floor of several walls).
- **(b) Venue consensus** — distinct venues across the stacked zones (the core edge: many of the 14 exchanges agreeing at one level).
- **(c) Historical defense** *(phase 2)* — how many times this price band previously acted as support and held (from the recorded lake / rolling history). A level repeatedly defended is stronger.

A **ConfluenceCluster** merges support zones within `confluence_band_bps` into one super-level with: `price_lo/price_hi`, `n_zones`, `venues` (union), `n_venues`, `max_confidence`, `max_persistence_s`, `historical_touches` (phase 2, else 0).

**Score (0..1, interpretable, tunable):**
```
score = clamp(
    0.45 * min(n_venues / confluence_venues_target, 1.0)   # multi-venue consensus  (core)
  + 0.25 * min(n_zones, 3) / 3                              # stacking
  + 0.20 * max_confidence                                  # base zone quality
  + 0.10 * min(historical_touches, hist_cap) / hist_cap    # repeated defense (0 in phase 1)
  , 0, 1)
```
A cluster is **tradeable/alertable** iff `score >= confluence_threshold` AND `n_venues >= min_confluence_venues` AND `max_persistence_s >= min_persistence_s`. Defaults (strict, quality-first): `confluence_venues_target=8`, `confluence_threshold=0.6`, `min_confluence_venues=6`, `confluence_band_bps=15`. All tunable + sweepable offline with the M11 replay.

## 4. Entry (reversion at confluence)
- Only consider entry when a **tradeable confluence cluster** sits within `entry_zone_bps` of price, on the correct side (a support cluster just below price → LONG; resistance cluster just above → SHORT).
- Enter **market** at the current price (reuses M12 `broker.enter_market`).
- Initial **stop** beyond the cluster band, ATR-floored (`min(cluster_lo*(1-stop_offset), price - atr*atr_stop_mult)` for LONG; mirrored).
- One position at a time (unchanged).

## 5. Exit — ride up with a trailing stop (NOT a fixed TP) — TWO variants, compared
"Subir con ellos": let winners run. We build **two trailing modes** (config `trail_mode`) and backtest them head-to-head (data decides which rides best), exactly as momentum-vs-reversion was compared:
- **`trail_mode="chandelier"`** — `peak` = highest price since entry (LONG); trailing = `peak - atr * trail_mult`. Each tick raise the broker stop to `max(current_stop, trailing)` (never lower). Robust; trails even when no new structure forms. Mirrored SHORT (lowest price + atr*trail_mult).
- **`trail_mode="support"`** — trail the stop up to just below the **highest confident support zone** that sits below price (`max(current_stop, highest_support.low*(1-stop_offset_bps))`). Rides the *structure* — each new support that forms during the rise becomes the new stop. If no new support forms, the stop does not advance (more give-back; a backstop ATR floor optional). Mirrored SHORT (lowest resistance above).
- **Structural exit (both modes):** if the confluence cluster we leaned on dissolves (no tradeable cluster overlaps the entry band anymore) → close. The floor we bet on is gone.
- No fixed take-profit. Risk is bounded by the trailing stop; reward is open-ended.

## 6. Paper trading + testing (NO alerts for now)
The strategy makes **paper trades** on confluence setups via the existing `PaperBroker` — instead of alerting or trading live — precisely so it is **fully testable**:
- **Live paper:** the running app positions automatically on a tradeable confluence cluster; the trade + R-multiple is recorded to the paper trade log (M5a) and visible on the dashboard.
- **Offline backtest:** the same logic runs through the M6/M11 harness on the recorded lake (window sweep + walk-forward), so we measure it without waiting for live setups.
- The dashboard MAY surface the active confluence clusters (read-only, informational) — useful to see setups forming — but there is **no alert/notification system** (no Telegram, no push). Alerts are deferred entirely.

## 7. Backtest / validation
- New `entry_mode = "confluence"` (config). Backtestable via the verified M6 `run_backtest` / M11 replay (the confluence analysis is causal, no look-ahead).
- Report per run: n_trades, **R-multiple distribution** (each trade's profit/risk), win-rate, mean R, max drawdown, and the (slowly-accumulating) walk-forward OOS.
- Sweep `confluence_threshold`, `confluence_band_bps`, `trail_mult`, `atr_stop_mult` offline on the recorded lake. Honest caveat printed: low trade count → preliminary.

## 8. Architecture / components (for the eventual plan)
```
src/pavilos/detection/confluence.py   # ConfluenceCluster + analyze_confluence(DepthAnalysis, cfg) -> clusters   [NEW]
src/pavilos/signals/engine.py         # entry_mode="confluence": confluence-gated entry + chandelier trail exit  [MODIFY]
src/pavilos/core/runtime.py           # config (confluence_* , trail_mult)                                       [MODIFY]
src/pavilos/backtest/runner.py        # pass entry_mode; confluence analysis in the backtest path                [MODIFY]
scripts/analyze.py                    # confluence sweep                                                          [MODIFY]
src/pavilos/web/state.py              # (optional) surface active confluence clusters, read-only/informational   [MODIFY]
(phase 2) src/pavilos/detection/history.py  # rolling level-history -> historical_touches
```
No alerts/notifier module — paper trades + backtest are the only outputs.

## 9. Phasing
- **Phase 1 (core):** ConfluenceAnalyzer (stacking + venues, no history) → confluence-gated reversion entry → chandelier trailing exit + structural exit → **paper trades** + backtest/sweep. Delivers the full testable strategy minus history.
- **Phase 2:** historical-touches confluence component (uses the lake). (Alerts/Telegram remain out of scope unless reintroduced later.)

## 10. Decisions (made with the user)
1. **Confluence components:** include ALL THREE — (a) stacking, (b) venues, (c) historical-touches — built so each is **individually measurable** (ablatable), to learn which actually predicts. "Best for validating the theory, effort no object."
2. **Trailing method:** build BOTH — `trail_mode="chandelier"` (ATR) AND `trail_mode="support"` (trail below higher supports) — config-selectable, **backtested head-to-head** (data decides). (Strategy milestone, not the validation step.)
3. **Sequencing:** **validation FIRST** (analyzer + study) before building the live paper entry/exit.
4. **Alerts:** none — paper trades only.

## 11. Milestone A (FIRST): Confluence forward-return validation study
**The key reframe for validating the theory with MANY samples, not few trades.** To test "do multi-venue confluence supports predict bounces?" we do NOT need to trade — we measure, over the recorded lake, what price did AFTER every confluence cluster formed:
- **ConfluenceAnalyzer** (a+b+c) over the M11-replayed snapshot stream → confluence clusters with scores + component sub-scores.
- For each support cluster near price, record a **forward-return observation**: the price move over the next `H` seconds (and the max-favorable-excursion / max-adverse-excursion = did it bounce, and how far before it broke). This is a *measurement of the future* (legitimate for signal validation — we are measuring predictive power, NOT trading on look-ahead).
- **Aggregate by confluence-score bucket** (and by each component a/b/c, and vs a low-confluence/baseline control): bounce-rate, mean forward return, MFE/MAE distribution, sample size.
- **Verdict:** if high-confluence supports show a materially higher bounce-rate / forward return than baseline (with adequate samples), the theory holds → build the strategy (Milestone B). If not, the theory is refuted cheaply — before building any entry/exit.
- Honest stats: report N per bucket; a high bounce-rate on tiny N is noise. This study has FAR more samples than trades (every cluster, not just traded ones), so it can actually reach significance.

## 12. Milestone B (after validation): the paper strategy — TWO trailing variants
Only if Milestone A validates: confluence-gated reversion entry (§4) + structural exit, built with **both** trailing modes (§5) — `trail_mode="chandelier"` and `trail_mode="support"` — config-selectable. **Paper trades** + the M6/M11 trade-level backtest run BOTH variants head-to-head (sanity + R-multiples + the slowly-accumulating walk-forward OOS), so the data picks the better trailing. Built on the validated analyzer.

(Alerts: resolved — none, paper trades only.)

## 11. Self-review
- **Coverage:** confluence score (a/b/c) → entry → ride-up exit → alerts → backtest, matching the user's idea exactly (position at confluence supports, ride up, few-but-quality, alerts).
- **Honesty:** §2 states the slow-validation tradeoff explicitly; backtest reframed as sanity + R-tracking, not edge-proof.
- **Plays to strength:** multi-venue confluence is the unique signal; uses the 14-venue book + the recorded lake.
- **Reuses verified machinery:** Detector zones, M12 reversion `enter_market`, M6/M11 backtest, M10 lake; new code is the confluence analyzer + trailing exit + notifier.
- **Risk-managed:** defined initial stop + trailing; structural exit when the floor dissolves; one position at a time.
- **Ambiguity resolved:** confluence is concretely defined (merge within band, score formula, tradeable gate); trailing is concrete (chandelier); alerts debounced.
