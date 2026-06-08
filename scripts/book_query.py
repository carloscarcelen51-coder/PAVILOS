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
