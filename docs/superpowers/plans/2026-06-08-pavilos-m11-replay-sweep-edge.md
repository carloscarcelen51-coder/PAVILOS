# PAVILOS M11: Offline Replay + Window Sweep + Edge Walk-Forward Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Two analyses on the recorded raw-L2 lake (M10): **(#2) a window sweep** — does `window_bps=300` detect the most robust supports vs 200/500/1000? — and **(#1) an edge verdict** — walk-forward backtest of the strategy on faithfully-reconstructed combined snapshots. Both sit on one **replay engine** that re-aggregates the lake through the REAL `Aggregator`, so the offline pipeline is faithful to live **by construction**.

**Architecture:** `replay.py` streams lake rows (DuckDB, ordered by `ts, exchange, seq_no`), groups consecutive rows sharing `(ts, exchange, seq_no)` into `BookUpdate`s, feeds them into a real `Aggregator(window_bps, bin_bps)` in ts order, and emits a `CombinedDepthSnapshot` at each cadence boundary (every `interval_s` of recorded time, after applying all updates with `ts <= boundary`). This is **proven** to reproduce the live snapshot stream (cadence snapshots are invariant to intra-tick venue ordering — validated empirically before this plan). `analysis.py` runs the window sweep (replay × each window → detection profile + backtest) and the edge walk-forward reuses M6's verified `walk_forward`. A CLI drives both over the lake.

**Tech Stack:** Python 3.13, `duckdb`, reuses merged M1–M10 (`Aggregator`, `build_combined`, `Detector`, `ATR`, `SignalEngine`, `PaperBroker`, M6 `run_backtest`/`grid_search`/`walk_forward`, M10 lake). `pytest`.

---

## Correctness foundation (already validated, must be preserved)
- **Faithfulness anchor:** the replay reuses the REAL `Aggregator` + `build_combined`. It must NOT reimplement aggregation. Snapshots are emitted at **cadence boundaries** (after draining all updates `ts <= boundary`), exactly like `Aggregator.run`. This was proven: feeding the SAME updates in live-arrival order vs lake order (`ORDER BY ts, exchange`) yields **identical cadence snapshots** (intra-tick venue order is irrelevant at the cadence level).
- **Causality / no look-ahead:** each snapshot is built only from updates with `ts <= boundary`. The walk-forward optimizes on fold k (in-sample) and scores on fold k+1 (out-of-sample) — M6's verified method. The replay is strictly causal, so no future data leaks into a snapshot.
- **Reconstruction:** lake rows for one `BookUpdate` share `(ts, exchange, seq_no)`; `is_snapshot` + per-level `(side, price, size)` (size 0 = remove on a delta) reproduce the original `BookUpdate`, which `BookState` applies identically to live.

---

## Scope decisions
1. **Both analyses share `replay.py`.** #1 = replay at `window_bps=300` → `walk_forward`. #2 = replay at each window in a grid → detection profile + `run_backtest`.
2. **Stream, do not materialise all rows.** Use a DuckDB cursor (`fetchmany`) to stream rows in `(ts, exchange, seq_no)` order; hold only the current Aggregator state + the (bounded) snapshot list. Snapshots at 5Hz are tiny (1h ≈ 18k); raw rows are huge (millions) → never load all rows.
3. **Sweep sets BOTH `window_bps` (aggregator, in replay) and `det_window_bps` (detector)** to the swept value — they are coupled (the detector's proximity scale matches the aggregate window).
4. **Honest data-sufficiency:** the edge verdict needs HOURS/DAYS. The CLI runs on whatever is recorded and PRINTS the covered span + #trades; a great in-sample number on minutes of data is noise. Recording continues; re-run as data grows.
5. **Read-only:** these analyses never write to the lake; they only read it.

**Deferred:** parallel sweep, caching re-aggregated snapshots, Sharpe/risk-adjusted ranking, sweeping `bin_bps` jointly with `window_bps` (grid supports it but default sweeps window only), a web view of results.

---

## File Structure
```
PAVILOS/
├── src/pavilos/backtest/
│   ├── replay.py             # lake -> real Aggregator -> CombinedDepthSnapshot stream [NEW]
│   └── analysis.py           # detection_profile + window_sweep [NEW]
├── scripts/analyze.py        # CLI: window-sweep | walkforward [NEW]
└── tests/unit/
    ├── test_replay.py
    ├── test_analysis.py
    └── test_analyze_cli.py
```

---

## Task 1: Replay engine (the faithful foundation)

**Files:** Create `src/pavilos/backtest/replay.py`; Test `tests/unit/test_replay.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_replay.py`** (round-trip faithfulness — VALIDATED before this plan with a gap+delta sequence, n=5, match=True):
```python
# tests/unit/test_replay.py
from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.backtest.replay import replay_snapshots


_SPECS = (VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A))


def _write_lake(base, updates):
    """Persist updates exactly like BookRecorder (seq_no per exchange, one row/level)."""
    sink = ParquetSink(base)
    seq: dict = {}
    by_ex: dict = {}
    for u in updates:
        s = seq.get(u.exchange, 0); seq[u.exchange] = s + 1
        rows = by_ex.setdefault(u.exchange, [])
        for p, sz in u.bids:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "bid", "price": p, "size": sz})
        for p, sz in u.asks:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "ask", "price": p, "size": sz})
    for ex, rows in by_ex.items():
        sink.write(ex, rows)


def _cadence(updates, *, interval_s, window_bps=300.0):
    """Reference = the SAME cadence algorithm replay uses, run on the ORIGINAL updates.
    Comparing replay(lake) to this isolates lake round-trip faithfulness from any
    boundary-convention question (both sides share the convention)."""
    agg = Aggregator(list(_SPECS), PegProvider(), bin_bps=5.0, window_bps=window_bps, staleness_s=100.0)
    out = []; nb = None
    for u in sorted(updates, key=lambda x: x.ts):   # stable -> intra-ts keeps input order (irrelevant at cadence)
        if nb is None:
            nb = u.ts
        while nb < u.ts:
            s = agg.snapshot(nb)
            if s is not None:
                out.append(s)
            nb += interval_s
        agg.apply(u)
    if nb is not None:
        s = agg.snapshot(nb)
        if s is not None:
            out.append(s)
    return out


def _norm(s):
    return None if s is None else (
        round(s.mid, 9),
        tuple((round(x.price, 9), round(x.size, 9)) for x in s.bids),
        tuple((round(x.price, 9), round(x.size, 9)) for x in s.asks),
        s.venues_active)


def test_replay_roundtrip_matches_cadence_aggregation(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))   # use test specs
    updates = [   # snapshots + a delta (size-0 remove) + a GAP (2.3 -> 4.7)
        BookUpdate("kraken", 1.0, ((100.0, 1.0), (99.5, 2.0)), ((100.5, 1.5), (101.0, 0.5)), True, 1),
        BookUpdate("coinbase", 1.0, ((100.1, 3.0),), ((100.6, 2.0),), True, 1),
        BookUpdate("kraken", 2.3, ((99.5, 0.0), (99.0, 4.0)), ((100.5, 2.5),), False, 2),
        BookUpdate("coinbase", 4.7, ((100.2, 5.0),), ((100.7, 3.0),), True, 2),
    ]
    _write_lake(str(tmp_path), updates)
    got = replay_snapshots(str(tmp_path), 0.0, 100.0, window_bps=300.0, bin_bps=5.0,
                           interval_s=1.0, staleness_s=100.0)
    want = _cadence(updates, interval_s=1.0)
    assert [_norm(s) for s in got] == [_norm(s) for s in want]
    assert len(got) == 5                                   # cadence boundaries 1,2,3,4,5
    assert any(round(x.price) == 99 for x in got[-1].bids)  # delta applied: 99.5 removed, 99.0 present


def test_replay_empty_range_returns_empty(tmp_path):
    assert replay_snapshots(str(tmp_path), 0.0, 1.0, window_bps=300.0, bin_bps=5.0,
                            interval_s=1.0, staleness_s=15.0) == []
```
  NOTE to implementer: expose a module-level `_SPECS_FN = lambda: list(VENUE_SPECS)` that `replay_snapshots` calls, so the test can monkeypatch it to its 2-venue set (production uses the real 14 `VENUE_SPECS`).

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/backtest/replay.py`:**
```python
# src/pavilos/backtest/replay.py
"""Re-aggregate the recorded raw-L2 lake (M10) into the combined-snapshot stream,
through the REAL Aggregator, so it is faithful to live by construction.

Lake rows for one BookUpdate share (ts, exchange, seq_no). We stream them in
(ts, exchange, seq_no) order, group into BookUpdates, apply them to a real
Aggregator, and emit a snapshot at each cadence boundary (every interval_s of
recorded time, after applying all updates with ts <= boundary) -- exactly like
Aggregator.run. Cadence snapshots are invariant to intra-tick venue ordering, so
this reproduces the live snapshot stream."""
from __future__ import annotations

import duckdb

from pavilos.core.models import BookUpdate, CombinedDepthSnapshot
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.connectors.venues import VENUE_SPECS

_SPECS_FN = lambda: list(VENUE_SPECS)   # indirection so tests can inject a venue subset
_BATCH = 50_000


def _iter_updates(base_dir: str, t0: float, t1: float):
    """Yield reconstructed BookUpdates in (ts, exchange, seq_no) order, streamed."""
    con = duckdb.connect()
    try:
        res = con.execute(
            f"SELECT ts, exchange, seq_no, is_snapshot, side, price, size "
            f"FROM '{base_dir}/**/*.parquet' WHERE ts >= ? AND ts <= ? "
            f"ORDER BY ts, exchange, seq_no", [float(t0), float(t1)])
    except duckdb.IOException:
        return                              # no parquet files under base_dir
    key = None
    ts = ex = snap = None
    bids: list = []
    asks: list = []
    while True:
        rows = res.fetchmany(_BATCH)
        if not rows:
            break
        for r_ts, r_ex, r_seq, r_snap, side, price, size in rows:
            k = (r_ts, r_ex, r_seq)
            if k != key:
                if key is not None:
                    yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks),
                                     is_snapshot=snap, seq=None)
                key = k; ts = r_ts; ex = r_ex; snap = r_snap; bids = []; asks = []
            (bids if side == "bid" else asks).append((price, size))
    if key is not None:
        yield BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks),
                         is_snapshot=snap, seq=None)


def replay_snapshots(base_dir: str, t0: float, t1: float, *, window_bps: float,
                     bin_bps: float, interval_s: float, staleness_s: float) -> list[CombinedDepthSnapshot]:
    """Reconstruct the combined-snapshot stream for [t0, t1] at the given aggregate
    config. Faithful to live (reuses the real Aggregator + cadence emission)."""
    agg = Aggregator(_SPECS_FN(), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=staleness_s)
    out: list[CombinedDepthSnapshot] = []
    next_b: float | None = None
    for u in _iter_updates(base_dir, t0, t1):
        if next_b is None:
            next_b = u.ts                      # first cadence boundary AT the first update's ts
        while next_b < u.ts:                   # emit boundaries fully covered by applied updates
            s = agg.snapshot(next_b)           # state = all updates with ts <= next_b (u not yet applied)
            if s is not None:
                out.append(s)
            next_b += interval_s
        agg.apply(u)
    if next_b is not None:                      # final boundary: everything applied
        s = agg.snapshot(next_b)
        if s is not None:
            out.append(s)
    return out
```
  CADENCE SEMANTICS (validated empirically before this plan, the test pins it): `snapshot(b)` reflects exactly the updates with `ts <= b`. Boundaries are `first_ts, first_ts+interval_s, ...`. Because updates stream in ts order, when we reach an update `u` with `u.ts > next_b`, every update with `ts <= next_b` has already been applied and `u` has not — so `snapshot(next_b)` is correct. `while next_b < u.ts` is STRICT (an update exactly at `next_b` is applied before that boundary is emitted, so it is included). The final boundary after the loop captures the tail. This is invariant to intra-tick venue order (proven), so it equals the live stream. Do NOT change to `>=`/`u.ts + interval` — that was a rejected off-by-one. The test's `_cadence` reference uses this exact loop, so they agree on round-trip; if they ever diverge, the lake round-trip is broken — report BLOCKED, do not fudge.

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add lake replay engine (faithful re-aggregation via the real Aggregator)`.

---

## Task 2: Window sweep + detection profile

**Files:** Create `src/pavilos/backtest/analysis.py`; Test `tests/unit/test_analysis.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_analysis.py`:**
```python
# tests/unit/test_analysis.py
import dataclasses
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.analysis import detection_profile


def _bin(p, s):
    return DepthBin(price=p, size=s, composition={"k": s / 2, "b": s / 2})


def _snap(ts, mid):
    bids = (_bin(mid - 1, 1.0), _bin(mid - 5, 30.0), _bin(mid - 9, 1.0))   # a wall ~mid-5
    asks = (_bin(mid + 1, 1.0), _bin(mid + 6, 1.0))
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=bids, asks=asks,
                                 venues_active=("k", "b"), venues_total=2)


def test_detection_profile_reports_zone_stats():
    cfg = dataclasses.replace(RuntimeConfig(), min_persistence_s=0.0, venues_target=2.0,
                              strength_target=5.0, persistence_target_s=1.0)
    snaps = [_snap(float(i), 100.0) for i in range(40)]
    prof = detection_profile(snaps, cfg)
    assert prof["n_snapshots"] == 40
    assert prof["avg_zones_per_snapshot"] >= 0.0
    assert 0.0 <= prof["avg_confidence"] <= 1.0
    assert "frac_snaps_with_strong_zone" in prof


def test_detection_profile_empty():
    prof = detection_profile([], RuntimeConfig())
    assert prof["n_snapshots"] == 0 and prof["avg_zones_per_snapshot"] == 0.0
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/backtest/analysis.py`:**
```python
# src/pavilos/backtest/analysis.py
"""Window sweep: for each candidate window_bps, re-aggregate the lake and measure
detection quality (zones surfaced) + downstream backtest P&L. Answers 'is 300 the
right window?' empirically. detection_profile() is pure over a snapshot list."""
from __future__ import annotations

import dataclasses
from statistics import mean

from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.backtest.runner import run_backtest
from pavilos.backtest.replay import replay_snapshots


def _detector(c: RuntimeConfig) -> Detector:
    return Detector(size_multiple=c.size_multiple, min_size=c.min_size, max_gap_bps=c.max_gap_bps,
                    max_zone_width_bps=c.max_zone_width_bps, match_overlap_bps=c.match_overlap_bps,
                    grace_s=c.grace_s, window_bps=c.det_window_bps, persistence_target_s=c.persistence_target_s,
                    venues_target=c.venues_target, strength_target=c.strength_target)


def detection_profile(snapshots, config: RuntimeConfig) -> dict:
    """Run the detector over ``snapshots`` and summarise zone quality."""
    if not snapshots:
        return {"n_snapshots": 0, "avg_zones_per_snapshot": 0.0, "avg_confidence": 0.0,
                "avg_venues_per_zone": 0.0, "frac_snaps_with_strong_zone": 0.0}
    detector = _detector(config)
    atr = ATR(window=config.atr_window)
    counts: list[int] = []
    confs: list[float] = []
    venues: list[int] = []
    strong = 0
    for s in snapshots:
        analysis = detector.update(s)
        atr.update(s.mid)
        zones = list(analysis.supports) + list(analysis.resistances)
        counts.append(len(zones))
        confs.extend(z.confidence for z in zones)
        venues.extend(len(z.venues) for z in zones)
        if any(z.confidence >= config.entry_threshold and len(z.venues) >= config.min_venues for z in zones):
            strong += 1
    return {
        "n_snapshots": len(snapshots),
        "avg_zones_per_snapshot": mean(counts),
        "avg_confidence": mean(confs) if confs else 0.0,
        "avg_venues_per_zone": mean(venues) if venues else 0.0,
        "frac_snaps_with_strong_zone": strong / len(snapshots),
    }


def window_sweep(base_dir: str, t0: float, t1: float, windows, *, base_config: RuntimeConfig,
                 starting_equity: float) -> list[dict]:
    """For each window_bps: re-aggregate the lake, profile detection, and backtest.
    Sets BOTH window_bps (aggregator) and det_window_bps (detector) to the swept value."""
    out: list[dict] = []
    for w in windows:
        snaps = replay_snapshots(base_dir, t0, t1, window_bps=w, bin_bps=base_config.bin_bps,
                                 interval_s=base_config.snapshot_interval_s, staleness_s=base_config.staleness_s)
        cfg = dataclasses.replace(base_config, window_bps=w, det_window_bps=w)
        out.append({
            "window_bps": w,
            "n_snapshots": len(snaps),
            "detection": detection_profile(snaps, cfg),
            "backtest": run_backtest(snaps, config=cfg, starting_equity=starting_equity),
        })
    return out
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add window sweep + detection profile`.

---

## Task 3: CLI (window-sweep + edge walk-forward)

**Files:** Create `scripts/analyze.py`; Test `tests/unit/test_analyze_cli.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_analyze_cli.py`:**
```python
from scripts.analyze import format_sweep_row, _lake_span
from pavilos.backtest.runner import BacktestResult


def test_format_sweep_row_readable():
    row = {"window_bps": 300.0, "n_snapshots": 1000,
           "detection": {"avg_zones_per_snapshot": 4.2, "avg_confidence": 0.55,
                         "avg_venues_per_zone": 3.1, "frac_snaps_with_strong_zone": 0.8},
           "backtest": BacktestResult(1000, 5, 3, 2, 60.0, 12.0, 4.0, 0.12, 10012.0, 8.0, 0.08)}
    s = format_sweep_row(row)
    assert "win=300" in s and "zones/snap=4.2" in s and "strong=80%" in s and "ret=" in s


def test_lake_span_missing_dir_returns_none(tmp_path):
    assert _lake_span(str(tmp_path / "nope")) is None
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `scripts/analyze.py`:**
```python
# scripts/analyze.py
"""Analyse the recorded raw-L2 lake. Usage:

    python -m scripts.analyze <data_dir> window-sweep [t0 t1]
    python -m scripts.analyze <data_dir> walkforward [n_splits] [t0 t1]

window-sweep: re-aggregate at 200/300/500/1000 bps -> detection profile + backtest.
walkforward : re-aggregate at the configured window -> in-sample-optimise /
              out-of-sample-score the strategy params (OOS return is the verdict).
"""
from __future__ import annotations

import sys

import duckdb

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import BacktestResult
from pavilos.backtest.replay import replay_snapshots
from pavilos.backtest.analysis import window_sweep
from pavilos.backtest.sweep import walk_forward

_WINDOWS = [200.0, 300.0, 500.0, 1000.0]
_WF_GRID = {
    "entry_threshold": [0.4, 0.55, 0.7],
    "opposing_distance_bps": [5.0, 10.0, 20.0],
    "entry_zone_bps": [15.0, 30.0, 60.0],
    "atr_stop_mult": [2.0, 3.0, 5.0],
}


def _lake_span(base_dir: str):
    try:
        r = duckdb.sql(f"SELECT min(ts), max(ts), count(*) FROM '{base_dir}/**/*.parquet'").fetchone()
    except Exception:
        return None
    if r is None or r[0] is None:
        return None
    return float(r[0]), float(r[1]), int(r[2])


def format_sweep_row(row: dict) -> str:
    d = row["detection"]; b = row["backtest"]
    return (f"win={row['window_bps']:.0f}  snaps={row['n_snapshots']}  "
            f"zones/snap={d['avg_zones_per_snapshot']:.1f}  conf={d['avg_confidence']:.2f}  "
            f"venues/zone={d['avg_venues_per_zone']:.1f}  strong={d['frac_snaps_with_strong_zone']*100:.0f}%  "
            f"| trades={b.n_trades} ret={b.return_pct:+.2f}% win={b.win_rate:.0f}%")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__); return
    base, mode = sys.argv[1], sys.argv[2]
    span = _lake_span(base)
    if span is None:
        print(f"no data under {base}"); return
    t0, t1, n = span
    base_cfg = RuntimeConfig()
    print(f"lake: {n:,} rows, span {t1 - t0:.0f}s ({(t1-t0)/3600:.2f}h)")
    if (t1 - t0) < 3600:
        print("WARNING: < 1h of data -- results are PRELIMINARY (noise). Keep recording for a real verdict.")

    if mode == "window-sweep":
        a = float(sys.argv[3]) if len(sys.argv) > 3 else t0
        b = float(sys.argv[4]) if len(sys.argv) > 4 else t1
        print(f"=== window sweep over [{a:.0f},{b:.0f}] ===")
        for row in window_sweep(base, a, b, _WINDOWS, base_config=base_cfg, starting_equity=base_cfg.starting_equity):
            print("  " + format_sweep_row(row))
    elif mode == "walkforward":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        print(f"=== walk-forward, {n_splits} folds, {len(snaps)} snapshots @ window={base_cfg.window_bps} ===")
        folds = walk_forward(snaps, base_config=base_cfg, grid=_WF_GRID, n_splits=n_splits,
                             starting_equity=base_cfg.starting_equity)
        for i, f in enumerate(folds):
            isr, oos = f["is_result"], f["oos_result"]
            print(f"  fold {i}: IS ret={isr.return_pct:+.2f}% ({isr.n_trades} tr) -> "
                  f"OOS ret={oos.return_pct:+.2f}% ({oos.n_trades} tr)")
        if folds:
            avg = sum(f["oos_result"].return_pct for f in folds) / len(folds)
            tot = sum(f["oos_result"].n_trades for f in folds)
            print(f"  >>> mean OOS return = {avg:+.2f}%  over {tot} OOS trades  (the verdict)")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4:** Run test → pass; `python -c "import scripts.analyze; print('import OK')"`. **Step 5:** full suite. **Step 6:** Commit `feat(scripts): add analyze CLI (window-sweep + edge walk-forward over the lake)`.

---

## Task 4: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (228 prior + ~6 new).
- [ ] **Step 2:** Confirm imports clean: `python -c "import pavilos.backtest.replay, pavilos.backtest.analysis, scripts.analyze; print('import OK')"`.
- [ ] **Step 3:** `git status` clean; `git tag m11-replay-sweep-edge`.
- [ ] **Step 4 (operator, reads the live lake):** `python -m scripts.analyze D:\pavilos_book_data window-sweep` and `... walkforward 4` — report results with the data-span caveat.

---

## Self-Review (plan author)
**Coverage:** replay engine (T1, the faithful foundation) → window sweep + detection profile (T2, answers #2) → CLI with edge walk-forward (T3, answers #1) → suite + tag + operator run (T4). Both analyses reuse verified components (real Aggregator, M6 run_backtest/walk_forward).
**Faithfulness (the crux):** replay reuses the REAL Aggregator + emits at cadence boundaries — proven (validated before the plan) to equal the live snapshot stream, invariant to intra-tick venue order. The T1 test pins it (lake round-trip == direct cadence aggregation). NO reimplementation of aggregation.
**No look-ahead:** snapshots are strictly causal (only updates `ts <= boundary`); walk_forward optimises IS / scores OOS (M6, verified). The CLI prints the data span + #trades and warns on < 1h (small-data honesty — the Brujita walk-forward-leak lesson).
**Type consistency:** `replay_snapshots(base_dir, t0, t1, *, window_bps, bin_bps, interval_s, staleness_s) -> list[CombinedDepthSnapshot]`; `detection_profile(snapshots, config) -> dict`; `window_sweep(base_dir, t0, t1, windows, *, base_config, starting_equity) -> list[dict]`; reuses `run_backtest`/`walk_forward` (M6) and `Detector`/`ATR` signatures (verified). Sweep sets BOTH `window_bps` + `det_window_bps`.
**Adversarial focus (3rd barrier):** (1) **faithfulness** — re-derive that replay == direct cadence aggregation over a richer synthetic sequence (multiple venues, snapshots+deltas, size-0 removes, staleness drop-out, updates straddling a cadence boundary); confirm intra-tick order invariance. (2) **streaming memory** — replay must NOT load all rows (fetchmany cursor); a multi-million-row range must not OOM (snapshots bounded). (3) **boundary/cadence correctness** — the FIRST snapshot covers updates up to `first_ts+interval_s`; no snapshot before the first update; a gap with no updates still advances boundaries without emitting bogus snapshots. (4) **empty/missing lake** — DuckDB IOException on no files -> empty, no crash. (5) **no look-ahead** — a snapshot at boundary b contains no level whose update ts > b. (6) **sweep couples window_bps + det_window_bps**; (7) **CLI small-data warning** fires < 1h. (8) reconstruction: a BookUpdate spanning a fetchmany batch boundary is still grouped correctly (the group key persists across batches). Item (1) faithfulness + (8) batch-boundary grouping are the headline; if either is wrong the analyses are worthless — prove them or report BROKEN.
