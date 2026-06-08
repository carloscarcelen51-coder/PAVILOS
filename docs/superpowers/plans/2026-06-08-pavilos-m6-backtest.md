# PAVILOS M6: Backtest / Optimization Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Find (or disprove) strategy edge **systematically and offline**, not by live trial-and-error: record the combined-book snapshot stream to disk, replay it through the real Detector→SignalEngine→PaperBroker for any config, measure performance (P&L, win-rate, #trades, **max drawdown**, fees), grid-search the parameter space, and **walk-forward validate** (optimize in-sample, score out-of-sample) so we don't overfit to one market slice — the exact trap Brujita hit.

**Architecture:** Pure, deterministic units under `src/pavilos/backtest/`. `io.py` (de)serializes `CombinedDepthSnapshot` ↔ JSONL. `runner.py` replays a snapshot list through fresh Detector/ATR/SignalEngine/PaperBroker for a given `RuntimeConfig` and returns a `BacktestResult` (metrics + equity curve). `sweep.py` does the cartesian grid-search and the walk-forward split. A live `scripts/record_book.py` captures data; `scripts/backtest.py` is the CLI. Everything except the recorder is network-free and unit-tested with synthetic snapshot sequences.

**Tech Stack:** Python 3.13, stdlib (`dataclasses`, `json`, `itertools`), `pytest`. Reuses merged M1–M5 (`CombinedDepthSnapshot`, `Detector`, `ATR`, `SignalEngine`, `PaperBroker`, `RuntimeConfig`).

---

## Scope decisions
1. **Backtest replays RECORDED combined snapshots.** The aggregator window/bin (`window_bps`/`bin_bps`) are baked into the recording, so the sweep tunes the **detector + signal** params (size_multiple, entry_threshold, entry_zone_bps, opposing_distance_bps, atr_stop_mult, risk_pct, …), which is where the edge lives. Re-aggregating from raw per-venue books (to sweep the window too) is a deferred extension.
2. **Determinism = faithful backtest.** Detection/signals/broker are deterministic given the snapshot stream, so replaying the recording reproduces exactly what live would have done with that config. Fills use the combined mid (same as live), so backtest P&L == live paper P&L for the same data+config.
3. **Honesty first.** The headline metric is **out-of-sample (walk-forward) return**, never in-sample. The report prints #trades alongside every result (a great IS return on 3 trades is noise). A recording of minutes is still one regime — the harness is only as good as the data; the operator must record hours/days for a trustworthy verdict (logged, not hidden).
4. **Trade.pnl is net-of-fees, funding-excluded** (as in M5a); drawdown is computed on the mark-to-market equity curve.

**Deferred:** re-aggregation sweep (window/bin), Sharpe/Sortino, multi-position, slippage, parallelized sweep, a web view of backtest results.

---

## File Structure
```
PAVILOS/
├── src/pavilos/backtest/
│   ├── __init__.py            # [NEW]
│   ├── io.py                  # snapshot <-> dict/JSONL [NEW]
│   ├── runner.py              # BacktestResult + run_backtest(snapshots, config) [NEW]
│   └── sweep.py               # grid_search + walk_forward [NEW]
├── scripts/
│   ├── record_book.py         # live recorder -> JSONL [NEW]
│   └── backtest.py            # CLI: single / sweep / walk-forward [NEW]
└── tests/unit/
    ├── test_backtest_io.py
    ├── test_backtest_runner.py
    └── test_backtest_sweep.py
```

---

## Task 1: Snapshot (de)serialization

**Files:** Create `src/pavilos/backtest/__init__.py` (empty), `src/pavilos/backtest/io.py`; Test `tests/unit/test_backtest_io.py`.

- [ ] **Step 1: Create `src/pavilos/backtest/__init__.py` empty.**

- [ ] **Step 2: Failing test — `tests/unit/test_backtest_io.py`:**
```python
# tests/unit/test_backtest_io.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.backtest.io import snapshot_to_dict, snapshot_from_dict, dumps_snapshot, loads_snapshot


def _snap():
    return CombinedDepthSnapshot(
        ts=5.0, mid=100.0,
        bids=(DepthBin(price=99.0, size=10.0, composition={"kraken": 6.0, "binance": 4.0}),),
        asks=(DepthBin(price=101.0, size=2.0, composition={"okx": 2.0}),),
        venues_active=("kraken", "binance", "okx"), venues_total=6)


def test_dict_roundtrip_preserves_all_fields():
    s = _snap()
    d = snapshot_to_dict(s)
    assert d["ts"] == 5.0 and d["mid"] == 100.0 and d["venues_total"] == 6
    assert d["bids"][0] == [99.0, 10.0, {"kraken": 6.0, "binance": 4.0}]
    r = snapshot_from_dict(d)
    assert r.ts == s.ts and r.mid == s.mid and r.venues_total == s.venues_total
    assert r.venues_active == ("kraken", "binance", "okx")
    assert r.bids[0].price == 99.0 and r.bids[0].size == 10.0
    assert r.bids[0].composition == {"kraken": 6.0, "binance": 4.0}
    assert r.asks[0].price == 101.0


def test_jsonl_line_roundtrip():
    s = _snap()
    line = dumps_snapshot(s)
    assert "\n" not in line
    r = loads_snapshot(line)
    assert r.mid == 100.0 and r.bids[0].composition == {"kraken": 6.0, "binance": 4.0}
```

- [ ] **Step 3:** run → FAIL.

- [ ] **Step 4: Implement — `src/pavilos/backtest/io.py`:**
```python
# src/pavilos/backtest/io.py
"""Serialize CombinedDepthSnapshot <-> dict / JSONL line for recording + replay."""
from __future__ import annotations

import json

from pavilos.core.models import DepthBin, CombinedDepthSnapshot


def snapshot_to_dict(s: CombinedDepthSnapshot) -> dict:
    return {
        "ts": s.ts, "mid": s.mid,
        "bids": [[b.price, b.size, b.composition] for b in s.bids],
        "asks": [[b.price, b.size, b.composition] for b in s.asks],
        "venues_active": list(s.venues_active), "venues_total": s.venues_total,
    }


def snapshot_from_dict(d: dict) -> CombinedDepthSnapshot:
    return CombinedDepthSnapshot(
        ts=d["ts"], mid=d["mid"],
        bids=tuple(DepthBin(price=p, size=sz, composition=dict(c)) for p, sz, c in d["bids"]),
        asks=tuple(DepthBin(price=p, size=sz, composition=dict(c)) for p, sz, c in d["asks"]),
        venues_active=tuple(d["venues_active"]), venues_total=d["venues_total"],
    )


def dumps_snapshot(s: CombinedDepthSnapshot) -> str:
    return json.dumps(snapshot_to_dict(s))


def loads_snapshot(line: str) -> CombinedDepthSnapshot:
    return snapshot_from_dict(json.loads(line))


def load_snapshots(path: str) -> list[CombinedDepthSnapshot]:
    out: list[CombinedDepthSnapshot] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(loads_snapshot(line))
                except Exception:
                    continue  # skip a corrupt/partial line
    return out
```

- [ ] **Step 5:** run → 2 passed. **Step 6:** full suite. **Step 7:** Commit `feat(backtest): add snapshot <-> JSONL (de)serialization`.

---

## Task 2: Backtester + metrics

**Files:** Create `src/pavilos/backtest/runner.py`; Test `tests/unit/test_backtest_runner.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_backtest_runner.py`:**
```python
# tests/unit/test_backtest_runner.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import run_backtest, BacktestResult


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=2)


def test_empty_snapshots_gives_flat_result():
    r = run_backtest([], config=RuntimeConfig(), starting_equity=10_000.0)
    assert isinstance(r, BacktestResult)
    assert r.n_snapshots == 0 and r.n_trades == 0 and r.final_equity == 10_000.0
    assert r.realized_pnl == 0.0 and r.win_rate == 0.0 and r.max_drawdown == 0.0


def test_backtest_runs_pipeline_and_reports_trades():
    # config that arms eagerly so the synthetic series produces at least one trade
    cfg = RuntimeConfig(entry_threshold=0.3, min_persistence_s=0.0, venues_target=2.0,
                        strength_target=5.0, persistence_target_s=1.0, entry_zone_bps=200.0,
                        opposing_distance_bps=50.0, det_window_bps=500.0)
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]  # support wall ~99
    asks = [_bin(105.0, 1.0)]
    # warm up persistence, then drive price up (fill) then down (stop) to close a trade
    snaps = [_snap(float(i), 99.5, bids, asks) for i in range(3)]
    snaps += [_snap(3.0, 103.0, bids, asks), _snap(4.0, 90.0, bids, asks)]
    r = run_backtest(snaps, config=cfg, starting_equity=10_000.0)
    assert r.n_snapshots == 5
    assert r.n_trades >= 1
    assert r.wins + r.losses == r.n_trades
    assert r.max_drawdown >= 0.0
    # final equity == starting + realized (flat at end: backtest force-closes any open position)
    assert abs(r.final_equity - (10_000.0 + r.realized_pnl)) < 1e-6
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/backtest/runner.py`:**
```python
# src/pavilos/backtest/runner.py
"""Replay a recorded combined-snapshot stream through the real detection->signals
->paper-broker pipeline for one config, and report performance metrics. Pure."""
from __future__ import annotations

from dataclasses import dataclass

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.execution.broker import PaperBroker


@dataclass(slots=True, frozen=True)
class BacktestResult:
    n_snapshots: int
    n_trades: int
    wins: int
    losses: int
    win_rate: float
    realized_pnl: float
    fees: float
    return_pct: float
    final_equity: float
    max_drawdown: float
    max_drawdown_pct: float


def _detector(c: RuntimeConfig) -> Detector:
    return Detector(size_multiple=c.size_multiple, min_size=c.min_size, max_gap_bps=c.max_gap_bps,
                    max_zone_width_bps=c.max_zone_width_bps, match_overlap_bps=c.match_overlap_bps,
                    grace_s=c.grace_s, window_bps=c.det_window_bps, persistence_target_s=c.persistence_target_s,
                    venues_target=c.venues_target, strength_target=c.strength_target)


def _signal(c: RuntimeConfig) -> SignalEngine:
    return SignalEngine(entry_threshold=c.entry_threshold, trail_threshold=c.trail_threshold,
                        opposing_threshold=c.opposing_threshold, min_persistence_s=c.min_persistence_s,
                        min_venues=c.min_venues, entry_offset_bps=c.entry_offset_bps,
                        stop_offset_bps=c.stop_offset_bps, atr_stop_mult=c.atr_stop_mult,
                        opposing_distance_bps=c.opposing_distance_bps, risk_pct=c.risk_pct,
                        max_leverage=c.max_leverage, entry_zone_bps=c.entry_zone_bps,
                        pending_timeout_s=c.pending_timeout_s)


def run_backtest(snapshots, *, config: RuntimeConfig, starting_equity: float) -> BacktestResult:
    detector = _detector(config)
    atr = ATR(window=config.atr_window)
    signal = _signal(config)
    broker = PaperBroker(starting_equity=starting_equity)
    last_mid = None
    peak = starting_equity
    max_dd = 0.0
    n = 0
    for snap in snapshots:
        n += 1
        last_mid = snap.mid
        analysis = detector.update(snap)
        atr.update(snap.mid)
        signal.update(analysis, atr.value(), broker)
        eq = broker.equity(snap.mid)
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
    if broker.position() is not None and last_mid is not None:
        broker.close(ts=snapshots[-1].ts)
    trades = broker.trades()
    realized = sum(t.pnl for t in trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = sum(1 for t in trades if t.pnl < 0)
    return BacktestResult(
        n_snapshots=n, n_trades=len(trades), wins=wins, losses=losses,
        win_rate=(wins / len(trades) * 100.0) if trades else 0.0,
        realized_pnl=realized, fees=sum(t.fee for t in trades),
        return_pct=(realized / starting_equity * 100.0) if starting_equity else 0.0,
        final_equity=broker.equity(last_mid) if last_mid is not None else starting_equity,
        max_drawdown=max_dd,
        max_drawdown_pct=(max_dd / peak * 100.0) if peak else 0.0,
    )
```

- [ ] **Step 4:** run → 2 passed. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add run_backtest + BacktestResult metrics (incl. max drawdown)`.

---

## Task 3: Grid-search + walk-forward

**Files:** Create `src/pavilos/backtest/sweep.py`; Test `tests/unit/test_backtest_sweep.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_backtest_sweep.py`:**
```python
# tests/unit/test_backtest_sweep.py
import dataclasses
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.sweep import grid_search, walk_forward


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snaps(n):
    bids = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]
    asks = [_bin(105.0, 1.0)]
    out = [CombinedDepthSnapshot(ts=float(i), mid=99.5 if i < 3 else (103.0 if i % 2 else 92.0),
                                 bids=tuple(bids), asks=tuple(asks), venues_active=("k", "b"), venues_total=2)
           for i in range(n)]
    return out


_BASE = RuntimeConfig(min_persistence_s=0.0, venues_target=2.0, strength_target=5.0,
                      persistence_target_s=1.0, entry_zone_bps=200.0, det_window_bps=500.0)


def test_grid_search_runs_every_combo_and_ranks():
    grid = {"entry_threshold": [0.3, 0.9], "opposing_distance_bps": [8.0, 50.0]}
    results = grid_search(_snaps(12), base_config=_BASE, grid=grid, starting_equity=10_000.0)
    assert len(results) == 4                       # 2 x 2 cartesian
    cfgs = [dataclasses.asdict(c)["entry_threshold"] for c, _ in results]
    assert set(cfgs) == {0.3, 0.9}
    # sorted best-first by return_pct (descending)
    rets = [r.return_pct for _, r in results]
    assert rets == sorted(rets, reverse=True)


def test_walk_forward_reports_in_and_out_of_sample():
    grid = {"entry_threshold": [0.3, 0.9]}
    folds = walk_forward(_snaps(20), base_config=_BASE, grid=grid, n_splits=2, starting_equity=10_000.0)
    assert len(folds) == 1                          # 2 splits -> 1 IS->OOS transition
    f = folds[0]
    assert "is_result" in f and "oos_result" in f and "config" in f
    assert f["is_result"].n_snapshots > 0 and f["oos_result"].n_snapshots > 0
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/backtest/sweep.py`:**
```python
# src/pavilos/backtest/sweep.py
"""Grid-search over config params + walk-forward validation (optimize in-sample,
score out-of-sample) to avoid overfitting to one slice. Pure."""
from __future__ import annotations

import dataclasses
import itertools

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.runner import run_backtest, BacktestResult


def _configs(base_config: RuntimeConfig, grid: dict) -> list[RuntimeConfig]:
    if not grid:
        return [base_config]
    keys = list(grid)
    combos = itertools.product(*(grid[k] for k in keys))
    return [dataclasses.replace(base_config, **dict(zip(keys, combo))) for combo in combos]


def grid_search(snapshots, *, base_config: RuntimeConfig, grid: dict,
                starting_equity: float) -> list[tuple[RuntimeConfig, BacktestResult]]:
    """Run a backtest for every grid combo; return (config, result) sorted by
    return_pct descending (best first)."""
    out = [(c, run_backtest(snapshots, config=c, starting_equity=starting_equity))
           for c in _configs(base_config, grid)]
    out.sort(key=lambda cr: cr[1].return_pct, reverse=True)
    return out


def walk_forward(snapshots, *, base_config: RuntimeConfig, grid: dict, n_splits: int,
                 starting_equity: float) -> list[dict]:
    """Split snapshots into ``n_splits`` contiguous folds. For each adjacent pair,
    grid-search on fold k (in-sample), then score the winning config on fold k+1
    (out-of-sample). The OOS result is the honest performance estimate."""
    if n_splits < 2:
        raise ValueError("n_splits must be >= 2")
    size = len(snapshots) // n_splits
    if size == 0:
        return []
    folds = [snapshots[i * size:(i + 1) * size] for i in range(n_splits)]
    out: list[dict] = []
    for k in range(n_splits - 1):
        ranked = grid_search(folds[k], base_config=base_config, grid=grid, starting_equity=starting_equity)
        best_cfg, is_result = ranked[0]
        oos_result = run_backtest(folds[k + 1], config=best_cfg, starting_equity=starting_equity)
        out.append({"config": best_cfg, "is_result": is_result, "oos_result": oos_result})
    return out
```

- [ ] **Step 4:** run → 2 passed. **Step 5:** full suite. **Step 6:** Commit `feat(backtest): add grid_search + walk_forward`.

---

## Task 4: Recorder script

**Files:** Create `scripts/record_book.py`. (Live, network; smoke-only — no unit test, mirrors `scripts/live_smoke.py`/`calibration_probe.py`.)

- [ ] **Step 1: Implement — `scripts/record_book.py`:**
```python
# scripts/record_book.py
"""Record the live combined-book snapshot stream to JSONL for offline backtesting.
Network; run from a residential host. Usage:

    python -m scripts.record_book [seconds] [out_path] [window_bps] [bin_bps]

Record HOURS for a trustworthy backtest — minutes is one regime slice.
"""
from __future__ import annotations

import asyncio
import sys
import time

from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.core.engine import Engine
from pavilos.connectors.venues import VENUE_SPECS, build_connector
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.io import dumps_snapshot


async def main() -> None:
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 3600.0
    out_path = sys.argv[2] if len(sys.argv) > 2 else "book_recording.jsonl"
    cfg = RuntimeConfig()
    window_bps = float(sys.argv[3]) if len(sys.argv) > 3 else cfg.window_bps
    bin_bps = float(sys.argv[4]) if len(sys.argv) > 4 else cfg.bin_bps
    connectors = [build_connector(v, cfg.symbols[v]) for v in cfg.symbols]
    agg = Aggregator(list(VENUE_SPECS), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=cfg.staleness_s)
    engine = Engine(connectors, agg)
    await engine.start()
    deadline = time.time() + seconds
    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        try:
            while time.time() < deadline:
                try:
                    snap = await asyncio.wait_for(engine.snapshots.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue
                f.write(dumps_snapshot(snap) + "\n")
                n += 1
                if n % 500 == 0:
                    f.flush()
                    print(f"recorded {n} snapshots...", flush=True)
        finally:
            await engine.stop()
    print(f"done: {n} snapshots -> {out_path} (window_bps={window_bps} bin_bps={bin_bps})")


if __name__ == "__main__":
    asyncio.run(main())
```
- [ ] **Step 2:** Confirm import-clean (no network at import): `python -c "import scripts.record_book; print('import OK')"` → `import OK`.
- [ ] **Step 3:** Commit `feat(scripts): add live book recorder for backtesting`.

---

## Task 5: Backtest CLI

**Files:** Create `scripts/backtest.py`. Test: extend `tests/unit/test_backtest_sweep.py` with a tiny format helper test OR keep CLI thin (no unit test; it just wires io+sweep+printing). Provide a `_format_result` pure helper that IS unit-tested.

- [ ] **Step 1: Failing test — append to `tests/unit/test_backtest_sweep.py`:**
```python
def test_format_result_line_is_readable():
    from pavilos.backtest.runner import BacktestResult
    from scripts.backtest import format_result
    r = BacktestResult(n_snapshots=1000, n_trades=12, wins=7, losses=5, win_rate=58.33,
                       realized_pnl=123.45, fees=20.0, return_pct=1.2345, final_equity=10123.45,
                       max_drawdown=80.0, max_drawdown_pct=0.79)
    s = format_result(r)
    assert "trades=12" in s and "win=58.3%" in s and "ret=+1.23%" in s and "maxDD=" in s
```

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: Implement — `scripts/backtest.py`:**
```python
# scripts/backtest.py
"""Offline backtest CLI. Usage:

    python -m scripts.backtest <recording.jsonl> single
    python -m scripts.backtest <recording.jsonl> sweep
    python -m scripts.backtest <recording.jsonl> walkforward [n_splits]

'sweep'/'walkforward' use a small built-in grid over the most impactful params.
"""
from __future__ import annotations

import sys

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.io import load_snapshots
from pavilos.backtest.runner import run_backtest, BacktestResult
from pavilos.backtest.sweep import grid_search, walk_forward

_GRID = {
    "entry_threshold": [0.4, 0.55, 0.7],
    "opposing_distance_bps": [5.0, 10.0, 20.0],
    "entry_zone_bps": [15.0, 30.0, 60.0],
    "atr_stop_mult": [2.0, 3.0, 5.0],
}


def format_result(r: BacktestResult) -> str:
    return (f"trades={r.n_trades} win={r.win_rate:.1f}% ret={r.return_pct:+.2f}% "
            f"pnl={r.realized_pnl:+.2f} fees={r.fees:.2f} maxDD={r.max_drawdown:.2f}"
            f"({r.max_drawdown_pct:.2f}%) eq={r.final_equity:.2f} n={r.n_snapshots}")


def _short(cfg: RuntimeConfig) -> str:
    return (f"entryTh={cfg.entry_threshold} oppBps={cfg.opposing_distance_bps} "
            f"zoneBps={cfg.entry_zone_bps} atrMult={cfg.atr_stop_mult}")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        return
    path, mode = sys.argv[1], sys.argv[2]
    snaps = load_snapshots(path)
    base = RuntimeConfig()
    eq = base.starting_equity
    print(f"loaded {len(snaps)} snapshots from {path}")
    if mode == "single":
        print("single:", format_result(run_backtest(snaps, config=base, starting_equity=eq)))
    elif mode == "sweep":
        ranked = grid_search(snaps, base_config=base, grid=_GRID, starting_equity=eq)
        print(f"=== top 10 of {len(ranked)} configs (by return%) ===")
        for cfg, r in ranked[:10]:
            print(f"  {format_result(r)}   [{_short(cfg)}]")
    elif mode == "walkforward":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        folds = walk_forward(snaps, base_config=base, grid=_GRID, n_splits=n_splits, starting_equity=eq)
        print(f"=== walk-forward, {n_splits} folds (OOS is the honest number) ===")
        for i, f in enumerate(folds):
            print(f"  fold {i}: IS {format_result(f['is_result'])}")
            print(f"          OOS {format_result(f['oos_result'])}   [{_short(f['config'])}]")
        if folds:
            avg_oos = sum(f["oos_result"].return_pct for f in folds) / len(folds)
            print(f"  >>> mean OOS return = {avg_oos:+.2f}%  (the number that matters)")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4:** run `python -m pytest tests/unit/test_backtest_sweep.py -v` → pass; `python -c "import scripts.backtest; print('import OK')"` → `import OK`. **Step 5:** full suite. **Step 6:** Commit `feat(scripts): add backtest CLI (single/sweep/walkforward)`.

---

## Task 6: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (195 prior + ~8 new). Existing suites stay green.
- [ ] **Step 2:** `.gitignore` add `book_recording.jsonl` (and `*.recording.jsonl`) — recordings are runtime data.
- [ ] **Step 3:** `git status` clean; `git tag m6-backtest`.
- [ ] **Step 4 (operator):** record data + run it:
  ```
  python -m scripts.record_book 3600 book_recording.jsonl     # 1h (more = better)
  python -m scripts.backtest book_recording.jsonl walkforward 6
  ```
  The **mean OOS return** is the honest verdict on edge.

---

## Self-Review (plan author)
**Coverage:** record (T4) → (de)serialize (T1) → backtest one config with full metrics incl. drawdown (T2) → grid-search + walk-forward (T3) → CLI report (T5) → gitignore + tag (T6). Directly answers "find edge systematically, not by live tuning."
**Honesty:** walk-forward OOS is the headline; #trades shown on every line; recorder docstring warns minutes = one regime. Mirrors the Brujita lesson ([[project-brujita-walkforward-leak]]).
**Determinism:** backtest reuses the exact Detector/ATR/SignalEngine/PaperBroker, so backtest P&L == live paper P&L for the same data+config.
**Type consistency:** `snapshot_to_dict/from_dict/dumps/loads/load_snapshots`; `run_backtest(snapshots, *, config: RuntimeConfig, starting_equity) -> BacktestResult(n_snapshots,n_trades,wins,losses,win_rate,realized_pnl,fees,return_pct,final_equity,max_drawdown,max_drawdown_pct)`; `grid_search(snapshots,*,base_config,grid,starting_equity)`; `walk_forward(...,n_splits,...)`; `format_result(BacktestResult)`. Uses `dataclasses.replace(RuntimeConfig, **combo)` (RuntimeConfig is a frozen dataclass — replace works). The detector/signal param wiring matches the merged M2/M3 signatures (verified incl. M3 entry_zone_bps/pending_timeout_s).
**Adversarial focus (3rd barrier):** empty snapshots → flat result (no div-by-zero on win_rate/drawdown); single snapshot; an always-open position force-closed at the last mid (final_equity == start+realized); `dataclasses.replace` with a bad grid key → clear error; grid value lists of length 1 / empty grid → base only; walk_forward n_splits<2 → ValueError, size 0 → []; backtest determinism (same data+config twice → identical result); drawdown never negative; return_pct/​win_rate with starting_equity or trades 0; corrupt JSONL line skipped on load; composition dict round-trips exactly; very large recording memory (note, acceptable).
