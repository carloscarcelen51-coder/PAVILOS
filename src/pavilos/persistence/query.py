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
