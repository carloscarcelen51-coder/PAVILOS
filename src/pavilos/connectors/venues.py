# src/pavilos/connectors/venues.py
"""Venue registry: the six Tier-A venues + a factory that builds a ready
connector (real transport wired) for each. The real WS subscribes + app-level
pings here are live-smoke-only; unit tests check wiring, not connectivity."""
from __future__ import annotations

import json
from collections.abc import AsyncIterator

import websockets

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.connectors.kraken_connector import KrakenConnector
from pavilos.connectors.binance_connector import BinanceConnector
from pavilos.connectors.bitstamp_connector import BitstampConnector
from pavilos.connectors.snapshot_delta_connector import SnapshotDeltaConnector
from pavilos.connectors.coinbase import CoinbaseFeed
from pavilos.connectors.okx import OKXFeed
from pavilos.connectors.bybit import BybitFeed

VENUE_SPECS: tuple[VenueSpec, ...] = (
    VenueSpec("kraken", Quote.USD, Tier.A),
    VenueSpec("binance", Quote.USDT, Tier.A),
    VenueSpec("coinbase", Quote.USD, Tier.A),
    VenueSpec("okx", Quote.USDT, Tier.A),
    VenueSpec("bybit", Quote.USDT, Tier.A),
    VenueSpec("bitstamp", Quote.USD, Tier.A),
)


def _coinbase_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://advanced-trade-ws.coinbase.com", max_size=None)
        await ws.send(json.dumps({"type": "subscribe", "product_ids": [symbol], "channel": "level2"}))
        await ws.send(json.dumps({"type": "subscribe", "product_ids": [symbol], "channel": "heartbeats"}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def _okx_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://ws.okx.com:8443/ws/v5/public", max_size=None)
        await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    if raw == "pong":
                        continue
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def _bybit_connect(symbol: str):
    async def connect() -> AsyncIterator[dict]:
        ws = await websockets.connect("wss://stream.bybit.com/v5/public/spot", max_size=None)
        await ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.200.{symbol}"]}))

        async def gen() -> AsyncIterator[dict]:
            try:
                async for raw in ws:
                    yield json.loads(raw)
            finally:
                await ws.close()
        return gen()
    return connect


def build_connector(venue: str, symbol: str):
    """Return a ready-to-run connector for ``venue`` (real transport wired)."""
    if venue == "kraken":
        return KrakenConnector(symbol, depth=25)
    if venue == "binance":
        return BinanceConnector(symbol)
    if venue == "bitstamp":
        return BitstampConnector(symbol)
    if venue == "coinbase":
        return SnapshotDeltaConnector("coinbase", CoinbaseFeed, connect=_coinbase_connect(symbol))
    if venue == "okx":
        return SnapshotDeltaConnector("okx", OKXFeed, connect=_okx_connect(symbol))
    if venue == "bybit":
        return SnapshotDeltaConnector("bybit", BybitFeed, connect=_bybit_connect(symbol))
    raise KeyError(f"unknown venue {venue!r}")
