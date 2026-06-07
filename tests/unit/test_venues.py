# tests/unit/test_venues.py
import pytest

from pavilos.core.models import Quote, Tier
from pavilos.connectors.venues import VENUE_SPECS, build_connector


def test_venue_specs_cover_six_tier_a():
    names = {s.exchange for s in VENUE_SPECS}
    assert names == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp"}
    assert all(s.tier is Tier.A for s in VENUE_SPECS)
    quotes = {s.exchange: s.quote for s in VENUE_SPECS}
    assert quotes["coinbase"] is Quote.USD and quotes["okx"] is Quote.USDT


def test_build_connector_returns_runnable_for_each_venue():
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd"}
    for venue, sym in symbols.items():
        conn = build_connector(venue, sym)
        assert conn.exchange == venue
        assert hasattr(conn, "run") and hasattr(conn, "health")


def test_build_connector_unknown_raises():
    with pytest.raises(KeyError):
        build_connector("ftx", "BTCUSDT")
