# scripts/capture.py
"""MANUAL frame capture (uses the network): record raw decoded frames from one
exchange WS to a JSONL file for future regression fixtures. Not a pytest test.

Usage: python -m scripts.capture kraken|binance <out.jsonl> [count]
"""
from __future__ import annotations

import asyncio
import json
import sys

from pavilos.connectors.kraken_connector import KrakenConnector
from pavilos.connectors.binance_connector import BinanceConnector


async def main(exchange: str, out_path: str, count: int) -> int:
    if exchange == "kraken":
        stream = await KrakenConnector("BTC/USD", depth=25)._default_connect()
    elif exchange == "binance":
        stream = await BinanceConnector("BTCUSDT")._default_connect()
    else:
        print("exchange must be kraken|binance", file=sys.stderr)
        return 2
    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        async for msg in stream:
            f.write(json.dumps(msg, default=str) + "\n")
            n += 1
            if n >= count:
                break
    print(f"captured {n} frames to {out_path}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python -m scripts.capture kraken|binance <out.jsonl> [count]", file=sys.stderr)
        raise SystemExit(2)
    ex, out = sys.argv[1], sys.argv[2]
    cnt = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    raise SystemExit(asyncio.run(main(ex, out, cnt)))
