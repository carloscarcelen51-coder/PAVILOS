# src/pavilos/persistence/query.py
"""DuckDB helpers over the partitioned Parquet lake."""
from __future__ import annotations

import glob as _glob_mod
import os

import duckdb


def _glob(base_dir: str) -> str:
    return f"{base_dir}/**/*.parquet"


def _has_files(base_dir: str) -> bool:
    """True if at least one parquet file exists under ``base_dir``. DuckDB 1.5.x
    raises IOException on a glob that matches nothing, so callers short-circuit."""
    return any(_glob_mod.iglob(os.path.join(base_dir, "**", "*.parquet"), recursive=True))


def summary(base_dir: str) -> list[dict]:
    """Per-exchange row count + ts range over the whole lake (empty list if no data).
    ``n`` is the alias (NOT ``rows`` — that is a reserved keyword in DuckDB)."""
    if not _has_files(base_dir):
        return []
    rel = duckdb.sql(
        f"SELECT exchange, count(*) AS n, min(ts) AS t0, max(ts) AS t1 "
        f"FROM '{_glob(base_dir)}' GROUP BY exchange ORDER BY n DESC"
    )
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


def load_range(base_dir: str, exchange: str, t0: float, t1: float) -> list[dict]:
    """All raw rows for ``exchange`` with ts in [t0, t1], ordered for replay."""
    if not _has_files(base_dir):
        return []
    rel = duckdb.sql(
        f"SELECT seq_no, ts, exchange, is_snapshot, side, price, size FROM '{_glob(base_dir)}' "
        f"WHERE exchange = ? AND ts >= ? AND ts <= ? "
        f"ORDER BY ts, seq_no",
        params=[exchange, float(t0), float(t1)],
    )
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


def reconstruct_book(base_dir: str, exchange: str, at_ts: float) -> tuple[dict, dict]:
    """Replay ``exchange`` up to ``at_ts`` -> (bids, asks) as {price: size}."""
    if not _has_files(base_dir):
        return {}, {}
    rel = duckdb.sql(
        f"SELECT seq_no, ts, is_snapshot, side, price, size FROM '{_glob(base_dir)}' "
        f"WHERE exchange = ? AND ts <= ? ORDER BY ts, seq_no",
        params=[exchange, float(at_ts)],
    )
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    prev: tuple[bool, int] | None = None     # (is_snapshot, seq_no) of the previous row
    for seq_no, ts, is_snapshot, side, price, size in rel.fetchall():
        # A snapshot row that begins a NEW update resets the book. We detect a new
        # update by a change in (is_snapshot, seq_no) vs the previous row, so a
        # post-restart snapshot reusing seq_no=0 still resets (the prior row was a
        # delta/snapshot with a different tuple). Relying on seq_no alone would miss
        # this because BookRecorder._seq resets to 0 on every process restart.
        if is_snapshot and (prev is None or prev != (is_snapshot, seq_no)):
            bids, asks = {}, {}
        prev = (is_snapshot, seq_no)
        book = bids if side == "bid" else asks
        if size == 0.0:
            book.pop(price, None)
        else:
            book[price] = size
    return bids, asks
