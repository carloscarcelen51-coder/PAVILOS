# tests/unit/test_venues.py
import pytest

from pavilos.core.models import Quote, Tier
from pavilos.connectors.venues import VENUE_SPECS, build_connector


def test_venue_specs_cover_all_tier_a():
    names = {s.exchange for s in VENUE_SPECS}
    assert names == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp",
                     "gate", "mexc", "cryptocom", "bitget", "kucoin", "htx",
                     "bitfinex", "gemini"}
    assert all(s.tier is Tier.A for s in VENUE_SPECS)
    quotes = {s.exchange: s.quote for s in VENUE_SPECS}
    assert quotes["coinbase"] is Quote.USD and quotes["okx"] is Quote.USDT
    assert quotes["gate"] is Quote.USDT and quotes["htx"] is Quote.USDT
    assert quotes["bitfinex"] is Quote.USD and quotes["gemini"] is Quote.USD


def test_build_connector_returns_runnable_for_each_venue():
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd",
               "gate": "BTC/USDT", "mexc": "BTC/USDT", "cryptocom": "BTC/USDT",
               "bitget": "BTC/USDT", "kucoin": "BTC/USDT", "htx": "BTC/USDT",
               "bitfinex": "BTC/USD", "gemini": "BTC/USD"}
    for venue, sym in symbols.items():
        conn = build_connector(venue, sym)
        assert conn.exchange == venue
        assert hasattr(conn, "run") and hasattr(conn, "health")


def test_build_connector_unknown_raises():
    with pytest.raises(KeyError):
        build_connector("ftx", "BTCUSDT")


def test_okx_connects_on_port_443(monkeypatch):
    # OKX shuts the :8443 WebSocket endpoints down on 2026-10-31.
    import asyncio
    import pavilos.connectors.venues as venues
    seen = []

    class _WS:
        async def send(self, _msg):
            pass

    async def fake_connect(url, **_kw):
        seen.append(url)
        return _WS()

    monkeypatch.setattr(venues.websockets, "connect", fake_connect)
    conn = build_connector("okx", "BTC-USDT")
    asyncio.run(conn._connect())
    assert seen == ["wss://ws.okx.com/ws/v5/public"]


def test_htx_loads_spot_markets_only():
    # Its derivatives host (api.hbdm.vn) is intercepted at times by this ISP and made
    # load_markets fail; spot is all the recorder needs.
    conn = build_connector("htx", "BTC/USDT")
    types = conn._ccxt_options["options"]["fetchMarkets"]["types"]
    assert types == {"spot": True, "linear": False, "inverse": False}
