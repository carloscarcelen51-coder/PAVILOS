# PAVILOS M10: Per-Exchange Raw L2 Data Layer (Parquet + DuckDB) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Persist the **raw L2 order-book stream of every venue** to compressed, partitioned **Parquet** (queryable with **DuckDB**, no server), so we can re-aggregate offline, analyse per venue, and feed the backtest — with **mandatory retention** so the disk never fills (raw L2 = tens of GB/day).

**Architecture:** A `BookRecorder` taps every `BookUpdate` as the Aggregator drains it. `record(update)` does ONE O(1) thread-safe queue put (never touches the event loop beyond that). A dedicated **writer thread** drains the queue, expands each update into per-level rows (assigning a monotonic `seq_no` per exchange so an update's levels stay groupable for replay), batches them, and writes Parquet partitioned `exchange=X/date=Y/HH/<n>.parquet` (zstd). Backpressure = drop-and-count (never block ingest). A retention pass deletes/moves date-partitions older than N days. A DuckDB helper + CLI query the lake. Recording is OFF unless `book_data_dir` is set (so it's an explicit opt-in to the disk cost).

**Tech Stack:** Python 3.13, `pyarrow` (Parquet), `duckdb` (query), `threading`, `queue`, `pytest`. Builds on merged M1–M8 (M9 reverted).

---

## Scope decisions
1. **Raw L2, full fidelity.** One row per level per update: `seq_no, ts, exchange, is_snapshot, side, price, size`. `is_snapshot` + `size==0`-means-remove preserve enough to REPLAY each venue's book (start at a snapshot, apply deltas). Ordering for replay: `(ts, seq_no)`.
2. **All heavy work off the event loop.** `record()` = one `queue.put_nowait`. The writer thread does row expansion + Parquet I/O. The event loop only ever does the O(1) put.
3. **Backpressure = drop, not block.** Bounded `queue.Queue`; if the writer falls behind, drop the update and count it (`dropped`). Blocking would defeat the isolation and stall ingest.
4. **Partition `exchange=X/date=Y/HH/`** (Hive layout) → DuckDB prunes by partition; a file per flush per (exchange, hour).
5. **Retention is mandatory + opt-in recording.** `book_data_dir=None` ⇒ no recording (default). When set (recommend a path on D: given the volume), record + prune partitions older than `retention_days` (default 7).
6. **DuckDB for query** (embedded, SQL, reads the Parquet glob) — no server, matches PAVILOS's lightweight design.

**Deferred:** compaction of many small files, columnar nested schema (list<struct> per update), live de-pegging, automatic cold-tier move beyond simple delete/move, full book-replay-to-combined-snapshot re-aggregation (this layer is its foundation; the sweep itself is later).

---

## File Structure
```
PAVILOS/
├── pyproject.toml                       # + pyarrow, duckdb [MODIFY]
├── src/pavilos/persistence/
│   ├── __init__.py                      # [NEW]
│   ├── parquet_sink.py                  # ParquetSink: rows -> partitioned Parquet [NEW]
│   ├── recorder.py                      # BookRecorder: queue + writer thread [NEW]
│   ├── retention.py                     # prune old date-partitions [NEW]
│   └── query.py                         # DuckDB helpers (load_range, reconstruct_book) [NEW]
├── src/pavilos/core/
│   ├── engine.py                        # pass on_update -> aggregator.run [MODIFY]
│   └── runtime.py                       # build recorder when book_data_dir set; start/stop; retention [MODIFY]
├── src/pavilos/aggregator/aggregator.py # run(..., on_update=None) [MODIFY]
├── scripts/book_query.py                # CLI over the lake [NEW]
└── tests/unit/
    ├── test_parquet_sink.py
    ├── test_recorder.py
    ├── test_retention.py
    ├── test_book_query.py
    ├── test_aggregator.py               # on_update called per update [MODIFY]
    └── test_deps_importable.py          # + pyarrow, duckdb [MODIFY]
```

---

## Task 1: dependencies

**Files:** Modify `pyproject.toml`, `tests/unit/test_deps_importable.py`.

- [ ] **Step 1:** Add `"pyarrow>=16"` and `"duckdb>=1.0"` to `[project] dependencies` (already pip-installed; do NOT install).
- [ ] **Step 2:** Append to `tests/unit/test_deps_importable.py`:
```python
def test_parquet_duckdb_importable():
    import pyarrow, pyarrow.parquet, duckdb  # noqa: F401
    assert hasattr(pyarrow.parquet, "write_table")
```
- [ ] **Step 3:** Run → pass. **Step 4:** Commit `chore(deps): add pyarrow + duckdb for the data layer`.

---

## Task 2: ParquetSink

**Files:** Create `src/pavilos/persistence/__init__.py` (empty), `src/pavilos/persistence/parquet_sink.py`; Test `tests/unit/test_parquet_sink.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_parquet_sink.py`:**
```python
# tests/unit/test_parquet_sink.py
import duckdb
from pavilos.persistence.parquet_sink import ParquetSink, ROW_FIELDS


def test_writes_partitioned_parquet_readable_by_duckdb(tmp_path):
    sink = ParquetSink(str(tmp_path))
    # two updates' worth of rows, same exchange, ts within one hour (epoch 1_700_000_000 ~ 2023-11)
    rows = [
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.5},
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "ask", "price": 101.0, "size": 2.0},
        {"seq_no": 1, "ts": 1_700_000_001.0, "exchange": "kraken", "is_snapshot": False, "side": "bid", "price": 100.0, "size": 0.0},
    ]
    sink.write("kraken", rows)
    files = list(tmp_path.rglob("*.parquet"))
    assert files, "a parquet file was written"
    # partition path includes exchange + date
    assert any("exchange=kraken" in str(f) for f in files)
    got = duckdb.sql(f"SELECT count(*) c, sum(size) s FROM '{tmp_path}/**/*.parquet'").fetchone()
    assert got[0] == 3 and abs(got[1] - 3.5) < 1e-9
    # schema columns present
    cols = set(duckdb.sql(f"SELECT * FROM '{tmp_path}/**/*.parquet' LIMIT 0").columns)
    assert set(ROW_FIELDS).issubset(cols)


def test_two_writes_same_partition_do_not_overwrite(tmp_path):
    sink = ParquetSink(str(tmp_path))
    r = [{"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "okx", "is_snapshot": True, "side": "bid", "price": 1.0, "size": 1.0}]
    sink.write("okx", r)
    sink.write("okx", [{**r[0], "seq_no": 1, "price": 2.0}])
    n = duckdb.sql(f"SELECT count(*) FROM '{tmp_path}/**/*.parquet'").fetchone()[0]
    assert n == 2   # second write must not clobber the first
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/persistence/parquet_sink.py`:**
```python
# src/pavilos/persistence/parquet_sink.py
"""Write batches of L2 rows to Hive-partitioned Parquet (exchange/date/hour),
zstd-compressed. One file per write() per (date, hour) partition; file names are
unique per partition so concurrent/repeated writes never clobber."""
from __future__ import annotations

import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

ROW_FIELDS = ("seq_no", "ts", "exchange", "is_snapshot", "side", "price", "size")

_SCHEMA = pa.schema([
    ("seq_no", pa.int64()), ("ts", pa.float64()), ("exchange", pa.string()),
    ("is_snapshot", pa.bool_()), ("side", pa.string()),
    ("price", pa.float64()), ("size", pa.float64()),
])


class ParquetSink:
    def __init__(self, base_dir: str, *, compression: str = "zstd") -> None:
        self._base = base_dir
        self._compression = compression
        self._counter: dict[tuple, int] = {}

    def write(self, exchange: str, rows: list[dict]) -> int:
        """Write ``rows`` (already expanded, dicts with ROW_FIELDS) for ``exchange``,
        grouped into the exchange/date/hour partition derived from each row's ts.
        Returns the number of files written."""
        if not rows:
            return 0
        groups: dict[tuple[str, str], list[dict]] = {}
        for r in rows:
            d, h = _date_hour(r["ts"])
            groups.setdefault((d, h), []).append(r)
        written = 0
        for (date, hour), grp in groups.items():
            part = os.path.join(self._base, f"exchange={exchange}", f"date={date}", hour)
            os.makedirs(part, exist_ok=True)
            key = (exchange, date, hour)
            idx = self._counter.get(key, 0)
            self._counter[key] = idx + 1
            path = os.path.join(part, f"{idx:06d}.parquet")
            table = pa.Table.from_pylist(grp, schema=_SCHEMA)
            pq.write_table(table, path, compression=self._compression)
            written += 1
        return written


def _date_hour(ts: float) -> tuple[str, str]:
    lt = time.gmtime(ts)
    return time.strftime("%Y-%m-%d", lt), time.strftime("%H", lt)
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(persistence): add ParquetSink (partitioned zstd Parquet)`.

---

## Task 3: BookRecorder

**Files:** Create `src/pavilos/persistence/recorder.py`; Test `tests/unit/test_recorder.py`.

- [ ] **Step 1: Failing test — `tests/unit/test_recorder.py`:**
```python
# tests/unit/test_recorder.py
import threading
import time

from pavilos.core.models import BookUpdate
from pavilos.persistence.recorder import BookRecorder


class _FakeSink:
    def __init__(self):
        self.rows_by_ex: dict[str, list] = {}
        self._lock = threading.Lock()
    def write(self, exchange, rows):
        with self._lock:
            self.rows_by_ex.setdefault(exchange, []).extend(rows)
        return 1


def _u(ex, ts, bids, asks, snap=True, seq=1):
    return BookUpdate(exchange=ex, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=snap, seq=seq)


def test_record_expands_levels_and_flushes_via_writer_thread():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=0.02)
    rec.start()
    try:
        rec.record(_u("kraken", 1.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
        rec.record(_u("kraken", 2.0, [(100.0, 0.0)], [], snap=False))
        # wait for the writer thread to flush
        deadline = time.time() + 2.0
        while time.time() < deadline and len(sink.rows_by_ex.get("kraken", [])) < 4:
            time.sleep(0.01)
    finally:
        rec.stop()
    rows = sink.rows_by_ex["kraken"]
    assert len(rows) == 4   # 2+1 levels from update 1, 1 from update 2
    # seq_no monotonic per exchange, groups an update's levels
    seqs = sorted({r["seq_no"] for r in rows})
    assert seqs == [0, 1]
    assert {r["side"] for r in rows} == {"bid", "ask"}
    bid_remove = [r for r in rows if r["seq_no"] == 1]
    assert bid_remove[0]["size"] == 0.0 and bid_remove[0]["is_snapshot"] is False


def test_record_is_nonblocking_and_drops_when_queue_full():
    sink = _FakeSink()
    rec = BookRecorder(sink, flush_interval_s=100.0, max_queue=3)  # writer effectively idle
    for i in range(10):
        rec.record(_u("okx", float(i), [(1.0, 1.0)], []))
    assert rec.dropped >= 1     # overflow dropped, never blocked
    # stop flushes whatever made it into the queue without hanging
    rec.start(); rec.stop()
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement — `src/pavilos/persistence/recorder.py`:**
```python
# src/pavilos/persistence/recorder.py
"""Tap the BookUpdate stream and persist it as raw L2 rows via a ParquetSink.

record() does ONE O(1) queue put (safe to call from the event loop). A dedicated
writer thread drains the queue, expands each update into per-level rows (assigning a
monotonic seq_no per exchange so an update's levels stay groupable for replay), and
hands batches to the sink. Backpressure = drop-and-count (never blocks ingest)."""
from __future__ import annotations

import logging
import queue
import threading

from pavilos.core.models import BookUpdate

_log = logging.getLogger(__name__)


class BookRecorder:
    def __init__(self, sink, *, flush_interval_s: float = 5.0, max_queue: int = 200_000) -> None:
        self._sink = sink
        self._flush_interval_s = flush_interval_s
        self._q: "queue.Queue[BookUpdate]" = queue.Queue(maxsize=max_queue)
        self._seq: dict[str, int] = {}
        self.dropped = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def record(self, update: BookUpdate) -> None:
        try:
            self._q.put_nowait(update)          # O(1); safe from the event loop
        except queue.Full:
            self.dropped += 1                   # writer behind -> drop (never block ingest)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="book-recorder", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        while not self._stop.is_set():
            batch = self._drain(self._flush_interval_s)
            if batch:
                self._flush(batch)
        rest = self._drain(0.0)                  # final flush on shutdown
        if rest:
            self._flush(rest)

    def _drain(self, wait_s: float) -> list[BookUpdate]:
        out: list[BookUpdate] = []
        try:
            out.append(self._q.get(timeout=wait_s) if wait_s > 0 else self._q.get_nowait())
        except queue.Empty:
            return out
        while True:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out

    def _flush(self, updates: list[BookUpdate]) -> None:
        rows_by_ex: dict[str, list[dict]] = {}
        for u in updates:
            seq = self._seq.get(u.exchange, 0)
            self._seq[u.exchange] = seq + 1
            rows = rows_by_ex.setdefault(u.exchange, [])
            for price, size in u.bids:
                rows.append(_row(seq, u, "bid", price, size))
            for price, size in u.asks:
                rows.append(_row(seq, u, "ask", price, size))
        for exchange, rows in rows_by_ex.items():
            try:
                self._sink.write(exchange, rows)
            except Exception:
                _log.exception("book recorder failed to write %s rows", exchange)


def _row(seq: int, u: BookUpdate, side: str, price: float, size: float) -> dict:
    return {"seq_no": seq, "ts": u.ts, "exchange": u.exchange,
            "is_snapshot": u.is_snapshot, "side": side, "price": float(price), "size": float(size)}
```

- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(persistence): add BookRecorder (queue + writer thread, drop-on-full)`.

---

## Task 4: wire into Aggregator/Engine/Runtime

**Files:** Modify `src/pavilos/aggregator/aggregator.py`, `src/pavilos/core/engine.py`, `src/pavilos/core/runtime.py`; Test `tests/unit/test_aggregator.py`.

- [ ] **Step 1: Failing test — add to `tests/unit/test_aggregator.py`:**
```python
def test_run_calls_on_update_for_each_update():
    import asyncio
    from pavilos.aggregator.aggregator import Aggregator
    from pavilos.aggregator.normalize import PegProvider
    from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier

    seen = []
    agg = Aggregator([VenueSpec("kraken", Quote.USD, Tier.A)], PegProvider(),
                     bin_bps=5.0, window_bps=300.0, staleness_s=15.0)

    async def scenario():
        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await in_q.put(BookUpdate(exchange="kraken", ts=1.0, bids=((100.0, 1.0),),
                                  asks=((101.0, 1.0),), is_snapshot=True, seq=1))
        task = asyncio.create_task(agg.run(in_q, out_q, interval_s=0.01, now=lambda: 1.0,
                                           stop=stop, on_update=seen.append))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())
    assert len(seen) == 1 and seen[0].exchange == "kraken"
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Modify `Aggregator.run`** — add `on_update: Callable[[BookUpdate], None] | None = None` (keyword) and, in the drain loop, after `self.apply(...)`, call it:
```python
            while True:
                try:
                    u = in_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self.apply(u)
                if on_update is not None:
                    on_update(u)
```
  (Import `Callable` already present.)

- [ ] **Step 4: Modify `Engine`** — add `on_update=None` to `__init__`, store it, and pass it in `start()`:
```python
        self._tasks.append(asyncio.create_task(
            self._aggregator.run(self._updates, self.snapshots, interval_s=self._interval_s,
                                 now=self._now, stop=self._stop, on_update=self._on_update)))
```

- [ ] **Step 5: Modify `RuntimeConfig` + `Runtime`:**
  (a) `RuntimeConfig`: add `book_data_dir: str | None = None`, `book_flush_interval_s: float = 5.0`, `book_retention_days: int = 7`.
  (b) In `Runtime.build`, after building connectors, if `config.book_data_dir`:
```python
        recorder = None
        if config.book_data_dir:
            from pavilos.persistence.parquet_sink import ParquetSink
            from pavilos.persistence.recorder import BookRecorder
            recorder = BookRecorder(ParquetSink(config.book_data_dir),
                                    flush_interval_s=config.book_flush_interval_s)
        engine = Engine(connectors, agg, interval_s=config.snapshot_interval_s,
                        on_update=(recorder.record if recorder else None))
```
  store `recorder` on the Runtime (add to `__init__` + the returned `cls(...)`).
  (c) In `run_app`: before `engine.start()`, if `self.recorder`: run retention once + `self.recorder.start()`; in the `finally`, `self.recorder.stop()` (before/after engine.stop is fine — stop recorder after engine so the final updates are captured). Retention call:
```python
        if self.recorder is not None:
            from pavilos.persistence.retention import prune_old_partitions
            prune_old_partitions(self.config.book_data_dir, self.config.book_retention_days)
            self.recorder.start()
```

- [ ] **Step 6:** Run aggregator test → pass; full suite (existing engine/runtime tests must still pass — Engine/Runtime new params are optional/defaulted). **Step 7:** Commit `feat(runtime): wire BookRecorder via aggregator on_update when book_data_dir set`.

---

## Task 5: retention + DuckDB query + CLI

**Files:** Create `src/pavilos/persistence/retention.py`, `src/pavilos/persistence/query.py`, `scripts/book_query.py`; Tests `tests/unit/test_retention.py`, `tests/unit/test_book_query.py`.

- [ ] **Step 1: Failing tests.**
  `tests/unit/test_retention.py`:
```python
import os
from pavilos.persistence.retention import prune_old_partitions


def _mk(base, exchange, date):
    p = os.path.join(base, f"exchange={exchange}", f"date={date}", "00")
    os.makedirs(p, exist_ok=True)
    open(os.path.join(p, "000000.parquet"), "w").close()


def test_prune_deletes_date_partitions_older_than_retention(tmp_path):
    base = str(tmp_path)
    _mk(base, "kraken", "2026-06-01")   # old
    _mk(base, "kraken", "2026-06-08")   # fresh
    # 'now' = 2026-06-08 -> retention 3 days keeps >= 2026-06-05
    removed = prune_old_partitions(base, retention_days=3, now_date="2026-06-08")
    dates = {d for d in os.listdir(os.path.join(base, "exchange=kraken"))}
    assert "date=2026-06-01" not in dates and "date=2026-06-08" in dates
    assert removed == 1


def test_prune_move_to_cold(tmp_path):
    base = str(tmp_path / "hot"); cold = str(tmp_path / "cold")
    _mk(base, "okx", "2026-05-01")
    prune_old_partitions(base, retention_days=1, now_date="2026-06-08", move_to=cold)
    assert os.path.exists(os.path.join(cold, "exchange=okx", "date=2026-05-01"))
    assert not os.path.exists(os.path.join(base, "exchange=okx", "date=2026-05-01"))
```
  `tests/unit/test_book_query.py`:
```python
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.query import load_range, reconstruct_book


def _seed(base):
    sink = ParquetSink(base)
    # snapshot then a delta that removes a bid and adds an ask
    sink.write("kraken", [
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "bid", "price": 100.0, "size": 1.0},
        {"seq_no": 0, "ts": 1_700_000_000.0, "exchange": "kraken", "is_snapshot": True, "side": "ask", "price": 101.0, "size": 1.0},
    ])
    sink.write("kraken", [
        {"seq_no": 1, "ts": 1_700_000_005.0, "exchange": "kraken", "is_snapshot": False, "side": "bid", "price": 100.0, "size": 0.0},
        {"seq_no": 1, "ts": 1_700_000_005.0, "exchange": "kraken", "is_snapshot": False, "side": "ask", "price": 102.0, "size": 2.0},
    ])


def test_load_range_counts_rows(tmp_path):
    _seed(str(tmp_path))
    rows = load_range(str(tmp_path), "kraken", 1_700_000_000.0, 1_700_000_010.0)
    assert len(rows) == 4


def test_reconstruct_book_replays_snapshot_then_delta(tmp_path):
    _seed(str(tmp_path))
    bids, asks = reconstruct_book(str(tmp_path), "kraken", at_ts=1_700_000_006.0)
    assert 100.0 not in bids            # removed by the delta (size 0)
    assert asks.get(101.0) == 1.0 and asks.get(102.0) == 2.0
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3: Implement `src/pavilos/persistence/retention.py`:**
```python
# src/pavilos/persistence/retention.py
"""Delete (or move to cold storage) date-partitions older than a retention window.
Raw L2 is tens of GB/day, so this MUST run or the disk fills."""
from __future__ import annotations

import os
import shutil
import time


def prune_old_partitions(base_dir: str, retention_days: int, *, now_date: str | None = None,
                         move_to: str | None = None) -> int:
    """Remove ``date=YYYY-MM-DD`` partitions older than ``retention_days`` under every
    ``exchange=*`` dir. If ``move_to`` is set, move instead of delete. ``now_date``
    (YYYY-MM-DD) is injectable for tests. Returns the number of partitions handled."""
    if not os.path.isdir(base_dir):
        return 0
    cutoff = _epoch_day(now_date or time.strftime("%Y-%m-%d", time.gmtime())) - retention_days
    handled = 0
    for ex_dir in os.listdir(base_dir):
        if not ex_dir.startswith("exchange="):
            continue
        ex_path = os.path.join(base_dir, ex_dir)
        for date_dir in os.listdir(ex_path):
            if not date_dir.startswith("date="):
                continue
            if _epoch_day(date_dir[len("date="):]) < cutoff:
                src = os.path.join(ex_path, date_dir)
                if move_to:
                    dst = os.path.join(move_to, ex_dir, date_dir)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.move(src, dst)
                else:
                    shutil.rmtree(src, ignore_errors=True)
                handled += 1
    return handled


def _epoch_day(date_str: str) -> int:
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")) // 86400)
```

- [ ] **Step 4: Implement `src/pavilos/persistence/query.py`:**
```python
# src/pavilos/persistence/query.py
"""DuckDB helpers over the partitioned Parquet lake."""
from __future__ import annotations

import duckdb


def _glob(base_dir: str) -> str:
    return f"{base_dir}/**/*.parquet"


def load_range(base_dir: str, exchange: str, t0: float, t1: float) -> list[dict]:
    """All raw rows for ``exchange`` with ts in [t0, t1], ordered for replay."""
    rel = duckdb.sql(
        f"SELECT seq_no, ts, exchange, is_snapshot, side, price, size FROM '{_glob(base_dir)}' "
        f"WHERE exchange = '{exchange}' AND ts >= {float(t0)} AND ts <= {float(t1)} "
        f"ORDER BY ts, seq_no"
    )
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


def reconstruct_book(base_dir: str, exchange: str, at_ts: float) -> tuple[dict, dict]:
    """Replay ``exchange`` up to ``at_ts`` -> (bids, asks) as {price: size}."""
    rel = duckdb.sql(
        f"SELECT seq_no, ts, is_snapshot, side, price, size FROM '{_glob(base_dir)}' "
        f"WHERE exchange = '{exchange}' AND ts <= {float(at_ts)} ORDER BY ts, seq_no"
    )
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    cur = None
    for seq_no, ts, is_snapshot, side, price, size in rel.fetchall():
        if is_snapshot and seq_no != cur:      # a new snapshot update resets the book
            bids, asks = {}, {}
            cur = seq_no
        book = bids if side == "bid" else asks
        if size == 0.0:
            book.pop(price, None)
        else:
            book[price] = size
    return bids, asks
```

- [ ] **Step 5: Implement `scripts/book_query.py`:**
```python
# scripts/book_query.py
"""Query the raw-L2 Parquet lake. Usage:

    python -m scripts.book_query <data_dir> summary
    python -m scripts.book_query <data_dir> book <exchange> <ts>
"""
from __future__ import annotations

import sys

import duckdb

from pavilos.persistence.query import reconstruct_book


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__); return
    base, mode = sys.argv[1], sys.argv[2]
    glob = f"{base}/**/*.parquet"
    if mode == "summary":
        print(duckdb.sql(
            f"SELECT exchange, count(*) rows, min(ts) t0, max(ts) t1 "
            f"FROM '{glob}' GROUP BY exchange ORDER BY rows DESC").to_string())
    elif mode == "book":
        exchange, ts = sys.argv[3], float(sys.argv[4])
        bids, asks = reconstruct_book(base, exchange, ts)
        top_bid = max(bids) if bids else None
        top_ask = min(asks) if asks else None
        print(f"{exchange} @ {ts}: {len(bids)} bids, {len(asks)} asks; "
              f"best bid={top_bid} best ask={top_ask}")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6:** Run tests → pass; `python -c "import scripts.book_query; print('import OK')"`. **Step 7:** full suite. **Step 8:** Commit `feat(persistence): add retention + DuckDB query helpers + book_query CLI`.

---

## Task 6: Close-out
- [ ] **Step 1:** `python -m pytest -v` → all pass (207 prior + ~9 new).
- [ ] **Step 2:** `.gitignore` add `data/` and `*.parquet` (recordings are runtime data, never committed).
- [ ] **Step 3:** Confirm imports clean: `python -c "import pavilos.persistence.recorder, pavilos.persistence.parquet_sink, pavilos.persistence.retention, pavilos.persistence.query, scripts.book_query; print('import OK')"`.
- [ ] **Step 4:** `git status` clean; `git tag m10-data-layer`.
- [ ] **Step 5 (operator, live):** set `book_data_dir` to a path with space (recommend **D:** given raw L2 ≈ tens of GB/day), run `python -m pavilos` ~60s, then `python -m scripts.book_query <dir> summary` → confirm all 12 venues have rows; spot-check `book <exchange> <ts>`. Confirm `recorder.dropped` stays ~0 (writer keeps up); confirm retention removes an aged partition.

---

## Self-Review (plan author)
**Coverage:** deps (T1) → ParquetSink (T2) → BookRecorder (T3) → Aggregator/Engine/Runtime wiring + config (T4) → retention + DuckDB query + CLI (T5) → suite/gitignore/tag/live (T6). Delivers per-exchange raw-L2 persistence with mandatory retention + queryability.
**Event-loop safety:** `record()` is one `queue.put_nowait` (O(1)); ALL expansion + Parquet I/O is on the writer thread. The aggregator hot loop only does that O(1) put when recording is on; with `book_data_dir=None` it's not even wired.
**Disk safety:** recording is opt-in (`book_data_dir`); retention runs at startup; the live step recommends D: + watches `dropped`. zstd keeps files ~10x smaller than JSON.
**Type consistency:** `ParquetSink.write(exchange, rows)`; rows carry `ROW_FIELDS`; `BookRecorder(sink, flush_interval_s, max_queue)` with `record/start/stop/dropped`; `Aggregator.run(..., on_update=None)`; `Engine(..., on_update=None)`; `RuntimeConfig.book_data_dir/book_flush_interval_s/book_retention_days`; `prune_old_partitions(base, retention_days, *, now_date, move_to)`; `load_range/reconstruct_book(base, exchange, ...)`. Schema fields match across sink/recorder/query. `BookUpdate.bids/asks` are (price,size) tuples (verified).
**Adversarial focus (3rd barrier):** (1) **record() never blocks the event loop** — only put_nowait; prove drop-on-full + non-blocking under a stalled writer. (2) **disk fill** — retention deletes/moves aged partitions (test with injected now_date); recording opt-in. (3) **replay correctness** — reconstruct_book replays snapshot-reset + delta size-0-removal in (ts,seq_no) order; prove vs a known sequence. (4) **partition file-name collisions** — two writes to one (exchange,date,hour) must not clobber (counter); test. (5) **shutdown flushes the tail** — stop() drains the queue before joining; bounded join. (6) **schema/type** — is_snapshot bool, seq_no int64; pyarrow from_pylist with explicit schema rejects mismatches. (7) **DuckDB glob on an empty/missing dir** — returns nothing, no crash. (8) hour-boundary rows land in the right partition (grouped by each row's ts). (9) a sink write exception is logged, not fatal to the recorder thread.
