# PAVILOS M15: Lossless Parquet Lake Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Merge the lake's many tiny Parquet files (52k+ and growing → DuckDB queries pathologically slow) into one file per closed partition — **losslessly and verified**, so queries/replays/studies become 10-100x faster while producing **byte-identical results**. Data integrity is paramount: verify the merged file equals the originals BEFORE deleting anything, and NEVER touch the live (current-hour) partition the recorder is writing.

**Architecture:** `compaction.py` — `compact_partition(part_dir)` reads all `*.parquet` in a partition, concatenates the rows, writes ONE zstd file, **reads it back and verifies its row multiset equals the originals**, and only THEN deletes the originals (atomic: merged written + verified before any delete → no window of data loss). `compact_lake(base_dir, *, now_ts)` iterates `exchange=*/date=*/HH/` partitions and compacts only those whose hour is FULLY PAST (strictly before the current wall-clock hour), skipping the live partition. A CLI runs it on D: with `--dry-run`. Results are identical because the data is unchanged (same rows) and all queries `ORDER BY` (physical layout irrelevant).

**Tech Stack:** Python 3.13, `pyarrow` (read/concat/write), `pytest`. Read-modify on the M10 lake.

---

## Safety foundation (data integrity — non-negotiable)
- **Lossless:** `pa.concat_tables` keeps every row; rewriting to zstd is exact (no recompute). Results are byte-identical because queries re-`ORDER BY` (physical order/file-count irrelevant).
- **Verify BEFORE delete:** after writing the merged file, READ IT BACK and assert its row **multiset** equals the union of the originals (count + sorted-content equality). Only on success delete the originals. On any failure: keep originals, remove the temp merged → zero data loss.
- **Only CLOSED partitions:** never compact the current wall-clock hour (the recorder is appending to it). `compact_lake` compacts only partitions whose hour ended before `floor(now_ts to hour)`.
- **Idempotent:** a partition with ≤1 parquet file is skipped (already compact).
- **Crash-safe ordering:** write merged to a distinct temp name → verify → delete originals → rename temp to final. A crash before delete leaves originals intact (+ a harmless stray temp); after delete the temp holds all rows.

---

## File Structure
```
src/pavilos/persistence/compaction.py   # compact_partition + compact_lake [NEW]
scripts/compact_lake.py                  # CLI (dry-run + run, closed partitions only) [NEW]
tests/unit/test_compaction.py
```

---

## Task 1: compaction core

**Files:** Create `src/pavilos/persistence/compaction.py`; Test `tests/unit/test_compaction.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_compaction.py`:**
```python
# tests/unit/test_compaction.py
import os
import duckdb
import pyarrow.parquet as pq
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.compaction import compact_partition, compact_lake


def _seed_partition(base, exchange, date, hour, n_files, rows_per):
    """Write n_files small parquet files into one exchange/date/HH partition."""
    sink = ParquetSink(base)
    seq = 0
    for _ in range(n_files):
        rows = []
        for _ in range(rows_per):
            rows.append({"seq_no": seq, "ts": _hour_ts(date, hour) + (seq % 50),
                         "exchange": exchange, "is_snapshot": True, "side": "bid",
                         "price": 63000.0 + seq, "size": 1.0 + (seq % 7)})
            seq += 1
        sink.write(exchange, rows)  # ts in the same hour -> same partition


def _hour_ts(date, hour):
    import time
    return time.mktime(time.strptime(f"{date} {hour}", "%Y-%m-%d %H")) - time.timezone  # UTC epoch


def _part_dir(base, exchange, date, hour):
    return os.path.join(base, f"exchange={exchange}", f"date={date}", hour)


def _rowset(paths):
    import pyarrow as pa
    t = pa.concat_tables([pq.read_table(p) for p in paths])
    return sorted(tuple(r.values()) for r in t.to_pylist())


def test_compact_partition_is_lossless_and_reduces_files(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "10", n_files=8, rows_per=20)
    part = _part_dir(base, "kraken", "2026-06-01", "10")
    before = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    before_rows = _rowset([os.path.join(part, f) for f in before])
    res = compact_partition(part)
    after = [p for p in os.listdir(part) if p.endswith(".parquet")]
    assert res["compacted"] is True and res["files_before"] == 8
    assert len(after) == 1                                   # 8 -> 1
    after_rows = _rowset([os.path.join(part, after[0])])
    assert after_rows == before_rows                         # EXACT same rows (multiset)
    # DuckDB sees identical data
    n = duckdb.sql(f"SELECT count(*), sum(size) FROM '{part}/*.parquet'").fetchone()
    assert n[0] == 160


def test_compact_partition_idempotent_single_file(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "okx", "2026-06-01", "09", n_files=1, rows_per=5)
    part = _part_dir(base, "okx", "2026-06-01", "09")
    res = compact_partition(part)
    assert res.get("skipped") is True
    assert len([p for p in os.listdir(part) if p.endswith(".parquet")]) == 1


def test_compact_partition_keeps_originals_if_verify_fails(tmp_path, monkeypatch):
    base = str(tmp_path)
    _seed_partition(base, "gate", "2026-06-01", "08", n_files=4, rows_per=10)
    part = _part_dir(base, "gate", "2026-06-01", "08")
    before = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    import pavilos.persistence.compaction as comp
    monkeypatch.setattr(comp, "_rows_equal", lambda *a, **k: False)   # force verify failure
    try:
        compact_partition(part)
    except Exception:
        pass
    after = sorted(p for p in os.listdir(part) if p.endswith(".parquet"))
    assert after == before                                   # originals intact, NO data loss
    assert not any(f.startswith("_compacted") for f in os.listdir(part))  # temp cleaned up


def test_compact_lake_skips_the_live_current_hour(tmp_path):
    base = str(tmp_path)
    _seed_partition(base, "kraken", "2026-06-01", "10", n_files=5, rows_per=10)  # past
    _seed_partition(base, "kraken", "2026-06-01", "12", n_files=5, rows_per=10)  # the "current" hour
    now = _hour_ts("2026-06-01", "12") + 1800   # we are in hour 12
    summary = compact_lake(base, now_ts=now)
    past = _part_dir(base, "kraken", "2026-06-01", "10")
    live = _part_dir(base, "kraken", "2026-06-01", "12")
    assert len([p for p in os.listdir(past) if p.endswith(".parquet")]) == 1   # past compacted
    assert len([p for p in os.listdir(live) if p.endswith(".parquet")]) == 5   # live untouched
    assert summary["partitions_compacted"] >= 1
```
  NOTE to implementer: adapt `_hour_ts` if the sink buckets by a different time basis — READ `src/pavilos/persistence/parquet_sink.py` first to confirm the partition layout (`exchange=X/date=YYYY-MM-DD/HH/NNNNNN.parquet`, hour via `time.gmtime`) and make the test seed land rows in the intended hour partition. The invariants that MUST hold regardless: lossless multiset equality, 8→1 files, idempotent single-file, originals kept on verify-fail, live hour skipped.

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/persistence/compaction.py`:**
```python
# src/pavilos/persistence/compaction.py
"""Losslessly compact the lake's many tiny Parquet files into one per closed
partition. Verify the merged file equals the originals BEFORE deleting anything;
never touch the current (live) hour the recorder is writing."""
from __future__ import annotations

import logging
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

_log = logging.getLogger(__name__)


def _rows_equal(merged_path: str, orig_paths: list[str]) -> bool:
    """True iff the merged file's rows are the SAME MULTISET as the originals
    (order-independent; queries re-ORDER BY). Reads the merged file back from disk
    so a write/zstd bug is caught."""
    m = pq.read_table(merged_path)
    o = pa.concat_tables([pq.read_table(p) for p in orig_paths])
    if m.num_rows != o.num_rows:
        return False
    return sorted(map(tuple, (r.values() for r in m.to_pylist()))) == \
           sorted(map(tuple, (r.values() for r in o.to_pylist())))


def compact_partition(part_dir: str) -> dict:
    """Merge all *.parquet in ``part_dir`` into one, verified-lossless. Originals
    are deleted ONLY after the merged file is written + verified equal to them."""
    files = sorted(f for f in os.listdir(part_dir) if f.endswith(".parquet"))
    if len(files) <= 1:
        return {"skipped": True, "files": len(files)}
    paths = [os.path.join(part_dir, f) for f in files]
    merged = pa.concat_tables([pq.read_table(p) for p in paths])
    tmp = os.path.join(part_dir, f"_compacted_{os.getpid()}.parquet")
    pq.write_table(merged, tmp, compression="zstd")
    try:
        if not _rows_equal(tmp, paths):
            raise ValueError(f"compaction verify FAILED for {part_dir}; keeping originals")
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    for p in paths:               # safe: merged written + verified before any delete
        os.remove(p)
    final = os.path.join(part_dir, "compacted.parquet")
    os.replace(tmp, final)
    return {"compacted": True, "files_before": len(files), "rows": merged.num_rows}


def _partition_hour_epoch(date: str, hour: str) -> float:
    return float(int(time.mktime(time.strptime(f"{date} {hour}", "%Y-%m-%d %H"))) - time.timezone)


def compact_lake(base_dir: str, *, now_ts: float | None = None) -> dict:
    """Compact every CLOSED partition (hour strictly before the current hour).
    Skips the live partition the recorder is appending to."""
    if not os.path.isdir(base_dir):
        return {"partitions_compacted": 0, "partitions_skipped": 0, "files_removed": 0}
    now = time.time() if now_ts is None else now_ts
    cur_hour = now - (now % 3600)            # start of the current hour (UTC epoch)
    compacted = skipped = files_removed = 0
    for ex in sorted(os.listdir(base_dir)):
        if not ex.startswith("exchange="):
            continue
        ex_dir = os.path.join(base_dir, ex)
        for d in sorted(os.listdir(ex_dir)):
            if not d.startswith("date="):
                continue
            d_dir = os.path.join(ex_dir, d)
            for hh in sorted(os.listdir(d_dir)):
                part = os.path.join(d_dir, hh)
                if not os.path.isdir(part):
                    continue
                try:
                    hour_epoch = _partition_hour_epoch(d[len("date="):], hh)
                except ValueError:
                    continue
                if hour_epoch + 3600 > cur_hour:   # this hour is the live/current one -> skip
                    skipped += 1
                    continue
                n_before = len([f for f in os.listdir(part) if f.endswith(".parquet")])
                res = compact_partition(part)
                if res.get("compacted"):
                    compacted += 1
                    files_removed += n_before - 1
                else:
                    skipped += 1
    return {"partitions_compacted": compacted, "partitions_skipped": skipped,
            "files_removed": files_removed}
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(persistence): add lossless verified lake compaction`.

---

## Task 2: CLI + close-out

**Files:** Create `scripts/compact_lake.py`.

- [ ] **Step 1: Implement — `scripts/compact_lake.py`:**
```python
# scripts/compact_lake.py
"""Compact the raw-L2 lake (merge tiny Parquet files per closed partition).
Lossless + verified; never touches the current hour. Usage:

    python -m scripts.compact_lake <data_dir> [--dry-run]
"""
from __future__ import annotations

import os
import sys
import time

from pavilos.persistence.compaction import compact_lake


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__); return
    base = sys.argv[1]
    if "--dry-run" in sys.argv:
        # count files per closed partition without changing anything
        total = parts = 0
        now = time.time(); cur = now - (now % 3600)
        for root, _dirs, files in os.walk(base):
            pq = [f for f in files if f.endswith(".parquet")]
            if len(pq) > 1:
                parts += 1; total += len(pq)
        print(f"dry-run: {parts} multi-file partitions, {total} parquet files (closed ones would merge to ~{parts})")
        return
    print(f"compacting {base} (closed partitions only)...")
    s = compact_lake(base)
    print(f"done: {s['partitions_compacted']} partitions compacted, "
          f"{s['files_removed']} files removed, {s['partitions_skipped']} skipped")


if __name__ == "__main__":
    main()
```
- [ ] **Step 2:** `python -c "import scripts.compact_lake, pavilos.persistence.compaction; print('OK')"`. **Step 3:** full suite. **Step 4:** `git tag m15-compaction`. **Step 5:** Commit `feat(scripts): add compact_lake CLI`.
- [ ] **Step 6 (operator):** `python -m scripts.compact_lake D:\pavilos_book_data --dry-run` then (after review) without `--dry-run`. Then re-run a confluence/static study to confirm IDENTICAL results, faster.

---

## Self-Review (plan author)
**Coverage:** compaction core with verify-before-delete (T1) → CLI + operator run (T2). Makes the lake queryable fast while preserving every row.
**Data integrity (the user's worry, answered in code):** lossless concat; merged file read-back-verified as the SAME multiset as originals BEFORE any delete; originals kept on verify-fail; live hour never touched; idempotent. Results are byte-identical (data unchanged, queries re-ORDER BY).
**Type consistency:** `compact_partition(part_dir) -> dict{compacted|skipped, files_before, rows}`; `compact_lake(base_dir, *, now_ts) -> dict{partitions_compacted, partitions_skipped, files_removed}`; `_rows_equal(merged, origs) -> bool`. Reuses ParquetSink layout (verified first).
**Adversarial focus (3rd barrier):** (1) **NO DATA LOSS** — prove a forced verify-failure (monkeypatch _rows_equal→False) leaves ALL originals intact + removes the temp; prove a partition compacts to the EXACT same row multiset (count + content) as before; randomized rows survive a round-trip. (2) **live hour skipped** — compact_lake with now_ts inside hour H must NOT touch hour H partitions; only strictly-past hours. (3) **idempotent** — running twice = no change after the first; single-file partition skipped. (4) **lossless under DuckDB** — a query (count, sum, ORDER BY) over the partition is identical before/after. (5) crash-safety ordering — write+verify precede any delete. (6) empty/missing dir, a non-parquet file in the dir, a malformed parquet (read error) → fail safe (keep originals, surface error), never partial-delete. Item (1) no-data-loss is the headline — a single dropped row would corrupt the research lake irreversibly.
