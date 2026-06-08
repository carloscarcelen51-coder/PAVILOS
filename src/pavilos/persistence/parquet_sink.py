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
