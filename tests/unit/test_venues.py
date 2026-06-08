# tests/unit/test_venues.py
import pytest

from pavilos.core.models import Quote, Tier
from pavilos.connectors.venues import VENUE_SPECS, build_connector


def test_venue_specs_cover_all_tier_a():
    names = {s.exchange for s in VENUE_SPECS}
    assert names == {"kraken", "binance", "coinbase", "okx", "bybit", "bitstamp",
                     "gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"}
    assert all(s.tier is Tier.A for s in VENUE_SPECS)
    quotes = {s.exchange: s.quote for s in VENUE_SPECS}
    assert quotes["coinbase"] is Quote.USD and quotes["okx"] is Quote.USDT
    assert quotes["gate"] is Quote.USDT and quotes["htx"] is Quote.USDT


def test_build_connector_returns_runnable_for_each_venue():
    symbols = {"kraken": "BTC/USD", "binance": "BTCUSDT", "coinbase": "BTC-USD",
               "okx": "BTC-USDT", "bybit": "BTCUSDT", "bitstamp": "btcusd",
               "gate": "BTC/USDT", "mexc": "BTC/USDT", "cryptocom": "BTC/USDT",
               "bitget": "BTC/USDT", "kucoin": "BTC/USDT", "htx": "BTC/USDT"}
    for venue, sym in symbols.items():
        conn = build_connector(venue, sym)
        assert conn.exchange == venue
        assert hasattr(conn, "run") and hasattr(conn, "health")


def test_build_connector_unknown_raises():
    with pytest.raises(KeyError):
        build_connector("ftx", "BTCUSDT")


def test_native_and_ccxt_venue_groups_partition_specs():
    from pavilos.connectors.venues import NATIVE_VENUES, CCXT_VENUES, VENUE_SPECS
    allnames = {s.exchange for s in VENUE_SPECS}
    assert set(NATIVE_VENUES) | set(CCXT_VENUES) == allnames
    assert set(NATIVE_VENUES) & set(CCXT_VENUES) == set()
    assert set(CCXT_VENUES) == {"gate", "mexc", "cryptocom", "bitget", "kucoin", "htx"}
