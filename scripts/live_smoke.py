# scripts/live_smoke.py
"""MANUAL live smoke (uses the network): run all six Tier-A venues through the
Engine for a few seconds and print the combined book + health. Not a pytest test.

Usage: python -m scripts.live_smoke [seconds]
"""
from __future__ import annotations

import asyncio
import sys

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.engine import Engine


async def main(seconds: float) -> int:
    from pavilos.connectors.venues import VENUE_SPECS, build_connector
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd"}
    specs = list(VENUE_SPECS)
    agg = Aggregator(specs, PegProvider(), bin_bps=5.0, window_bps=50.0, staleness_s=15.0)
    connectors = [build_connector(v, symbols[v]) for v in symbols]
    engine = Engine(connectors, agg, interval_s=1.0)
    await engine.start()
    try:
        deadline = asyncio.get_event_loop().time() + seconds
        last = None
        while asyncio.get_event_loop().time() < deadline:
            try:
                last = await asyncio.wait_for(engine.snapshots.get(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        if last is None:
            print("NO SNAPSHOT — check connectivity")
            return 1
        print(f"mid={last.mid:.2f} venues={last.venues_active}/{last.venues_total} "
              f"bids={len(last.bids)} asks={len(last.asks)}")
        for b in last.bids[:5]:
            print(f"  BID {b.price:.2f} size={b.size:.4f} {b.composition}")
        for h in engine.health():
            print(f"  health {h.exchange}: connected={h.connected} resyncs={h.resyncs} errors={h.errors}")
        return 0
    finally:
        await engine.stop()


if __name__ == "__main__":
    secs = float(sys.argv[1]) if len(sys.argv) > 1 else 15.0
    raise SystemExit(asyncio.run(main(secs)))
