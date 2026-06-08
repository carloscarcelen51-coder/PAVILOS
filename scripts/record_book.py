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
