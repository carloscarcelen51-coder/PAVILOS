# src/pavilos/connectors/venues.py
"""Venue registry: the 12 Tier-A venues + a factory that builds a ready
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
from pavilos.connectors.ccxt_connector import CcxtConnector

VENUE_SPECS: tuple[VenueSpec, ...] = (
    VenueSpec("kraken", Quote.USD, Tier.A),
    VenueSpec("binance", Quote.USDT, Tier.A),
    VenueSpec("coinbase", Quote.USD, Tier.A),
    VenueSpec("okx", Quote.USDT, Tier.A),
    VenueSpec("bybit", Quote.USDT, Tier.A),
    VenueSpec("bitstamp", Quote.USD, Tier.A),
    VenueSpec("gate", Quote.USDT, Tier.A),
    VenueSpec("mexc", Quote.USDT, Tier.A),
    VenueSpec("cryptocom", Quote.USDT, Tier.A),
    VenueSpec("bitget", Quote.USDT, Tier.A),
    VenueSpec("kucoin", Quote.USDT, Tier.A),
    VenueSpec("htx", Quote.USDT, Tier.A),
)

NATIVE_VENUES = ("kraken", "binance", "coinbase", "okx", "bybit", "bitstamp")
CCXT_VENUES = ("gate", "mexc", "cryptocom", "bitget", "kucoin", "htx")

# Staggered startup (seconds) for the ccxt venues so they don't all run
# load_markets + open a websocket at the same instant (which storms the host and
# times out / rate-limits the slower ones). Fast venues first, slow ones later.
_CCXT_STAGGER = {"gate": 0.0, "mexc": 1.0, "htx": 2.0, "cryptocom": 3.0, "bitget": 4.0, "kucoin": 5.0}

# Per-venue ccxt config overrides. Bitget's local-checksum verification hits a ccxt
# bug (handle_check_sum_error -> UnsubscribeError) that kills its book on the 2nd
# update and reconnect-storms (starving the other ccxt venues); disable it.
_CCXT_OPTIONS = {"bitget": {"options": {"watchOrderBook": {"checksum": False}}}}


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
        return KrakenConnector(symbol, depth=1000)   # Kraken's max book depth — fills the ±300bps
        # aggregate window (25 levels only covered the touch). CRC stays top-10; max_size=None handles frames.
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
    if venue in _CCXT_STAGGER:
        # stagger the ccxt connects: all 6 doing load_markets + WS at once storms the
        # host (slow venues time out / get rate-limited); spread them over a few seconds.
        return CcxtConnector(venue, symbol, startup_delay=_CCXT_STAGGER[venue],
                             ccxt_options=_CCXT_OPTIONS.get(venue))
    raise KeyError(f"unknown venue {venue!r}")
