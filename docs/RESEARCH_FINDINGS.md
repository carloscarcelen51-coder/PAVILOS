# PAVILOS — Strategy R&D Findings (honest log)

This records what we tested for a tradeable edge in the multi-venue BTC/USD order book,
and the honest verdicts. The infrastructure (14-venue ingest, raw-L2 lake, faithful replay,
level detection, forward-return studies, lossless compaction) is solid and reusable; this
log is about the SIGNAL.

## Theses tested and REFUTED

| Thesis | Verdict |
|---|---|
| **Momentum entry** off detected walls | No edge; ~3-6 OOS trades, noise. |
| **Mean-reversion entry** at supports | No edge; same structural low frequency. |
| **Confluence near-price** (M13) | The detected "supports" were the **near-touch bid** (~2bps, trailing price), not static levels. Bounce thesis ill-posed. |
| **Static-level BOUNCE** (M14) | A single volatile window showed +0.44R/72%, but **OOS over 5 windows (~191 decided) = pooled 33% bounce / −0.33R, NEGATIVE.** Variance, not edge. Price breaks through static multi-venue levels more than it bounces. |
| **Static-level BREAKDOWN** (fade/short the support) | Survived a naive drift control (+0.31R) but that was an **R-scale artifact** (sub-bp ATR baseline). With an R-matched baseline the support-specific edge collapses to **+0.07R (noise)**, and after fees (10bps round-trip ≈ 0.60R at the ~17bps R) the absolute short is **net −0.27R**. Refuted. |
| **Order-flow / buying-pressure LEADS** the move | Near-touch (±10bps) book imbalance **does lead** at 5–60s (spread +2.6–3.5bps, concordance 0.59–0.67, consistent across 5 windows) — but it is **~3bps vs ~10bps fees** (HFT-scale, fee-killed), **decays to noise by 300s**, and wider bands (±30/100bps) show no signal. Real micro-info, no accessible tradeable edge. |

## The one lead still alive (UNDERPOWERED, not validated)

**Pressure-piloted dynamic-stop "ride"** (`scripts/_pressure_pilot.py`): instead of scalping
the 3bps micro-signal, use it as a continuous pilot — enter on strong pressure, RIDE while
pressure persists, exit on flip. Most parametrizations bleed (fee drag from chop), BUT the
**selective + loose-ride** config (`enter_T=0.3, exit_T=−0.2`) returned **+3.62 bps/trade net
of 10bps fees, +62bps total** across 5 windows — the **first net-positive-after-fees result
in the project**.

**BUT it is not validated:** only **17 trades**, wildly inconsistent across windows
(−9/+0/+29/+5/+60), within ~1σ of zero. It is a **trend-following / fat-tailed** profile —
its return lives in rare big rides, so distinguishing edge from luck needs **hundreds of
trades** = weeks of data spanning trending/volatile regimes (the ~12h tested was mostly
low-vol). The current data cannot settle it.

### Re-test plan (when ~2 weeks of varied data have accrued)
1. Keep the recorder (`python -m pavilos`) running (it accumulates passively).
2. Pick top volatile **pre-gap** windows (respect `D:\pavilos_book_data\_gaps.json` — never
   replay across a recording gap), aiming for **300+ trades** for the `e0.3/−0.2` config.
3. Re-run `python -m scripts._pressure_pilot <t0> <t1>` per window; pool the net/trade.
4. Verdict: net/trade **clearly > 0 over 300+ varied-regime trades** ⇒ real edge ⇒ build the
   strategy (selective pressure entry + dynamic-stop ride). Else ⇒ it was luck; close.

### Pair note (reasoned, untested)
For a flow-piloted ride, **BTC is likely the WORST fit** (highest cap absorbs flow → weakest
pressure→price, most arbitraged). Lower-cap-but-liquid alts (**ETH, then SOL**) should give a
stronger flow signal + bigger rides — at the cost of wider spreads and more manipulable book
imbalance. Testing there requires recording those pairs (multi-pair build + weeks of data).

## Structural conclusion
The multi-venue book holds **real micro-information** (near-touch imbalance leads ~3bps over
seconds) but **no tradeable post-fee edge** at any horizon a non-colocated, fee-paying setup
can access. The fee wall (~10bps round-trip) dominates the ~17bps moves at this granularity;
only **larger/longer moves** (where fees are a small fraction) could change the arithmetic —
which is exactly what the pressure-piloted ride bets on, and what the re-test must settle.

## Known data caveats
The **ccxt connectors (gemini, kucoin, …) over-record**: they write the FULL book as a
snapshot on every update (gemini ~670M rows/hour, kucoin ~231M, vs kraken ~1.5M deltas) — 95.7%
of the lake. Analyses exclude `gemini`/`kucoin` from the replay to stay memory-bounded; the
finding is robust to their exclusion. **Fixed by M17 (2026-10-06)** for data recorded from its
deploy on: the recorder stores differences, losslessly (see the M17 spec).

### Data quality, audited 2026-10-06 (read before picking re-test windows)
- **Contaminated span — never analyse it:** 2026-09-28 15:46:31Z → 2026-09-29 16:24:03Z. The
  lake itself filled D: (the recorder ran 14 days without a restart and the 7-day prune only
  ran at startup): 47,297 failed writes across all 14 venues (~1 in 5 batches lost), and the
  native venues' replayed books stay wrong until the restart that ended it. Listed in
  `D:\pavilos_book_data\_gaps.json` together with every restart gap.
- **Partial native books before M17:** kraken, binance, okx, bybit and bitstamp send a full
  book only when they connect, so a window replayed from mid-session without a long warm-up
  rebuilds their books from the window's deltas only (deep levels that never changed inside
  the window are missing). From M17 on, a full checkpoint of every active venue is recorded
  every 60 s and `replay_snapshots` warms up 120 s before `t0`.
- **Mixed clock (before M17):** binance deltas carried the exchange's event time; every other
  venue the PC's receive time. On 2026-10-06 the PC clock was ~323 ms behind binance and
  Windows Time sync was not running, so pre-M17 binance is shifted against the other venues by
  a few hundred ms (and the offset drifts). From M17 on, binance deltas carry the receive time
  too, and the recorder never lets a venue's ts go backwards.
- **Replay defaults changed with M17:** `replay_snapshots` now warms up 120 s before `t0`
  (pass `warmup_s=0` to reproduce a pre-M17 run, which differs only where a recorder restart
  marked in `_runs.jsonl` falls inside the window: the books are reset there), reads only the
  hours it needs and skips unreadable files with a warning. The research
  scripts (`_oos_window`, `_pressure_leads`, `_pressure_pilot`) use it instead of their own
  readers.
- **Self-check:** `python -m scripts.check_lake <data_dir> <t0> <t1>` verifies, for M17 data,
  that the book rebuilt from the lake equals every recorded checkpoint (expected: 0 mismatches,
  0 reordered; losses the recorder flagged show up as mismatches and in the `losses` column).
