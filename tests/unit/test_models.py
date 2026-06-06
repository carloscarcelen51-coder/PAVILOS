# tests/unit/test_models.py
import dataclasses
import pytest

from pavilos.core.models import (
    Side, Quote, Tier, BookUpdate, VenueSpec, DepthBin, CombinedDepthSnapshot,
    TIER_A_QUOTES,
)


def test_side_and_quote_values():
    assert Side.BID.value == "bid"
    assert Side.ASK.value == "ask"
    assert Quote.USD.value == "USD"
    assert {Quote.USD, Quote.USDT, Quote.USDC} == TIER_A_QUOTES


def test_bookupdate_is_immutable_and_holds_levels():
    u = BookUpdate(
        exchange="kraken",
        ts=1.0,
        bids=((100.0, 1.0), (99.0, 2.0)),
        asks=((101.0, 1.5),),
        is_snapshot=True,
        seq=None,
    )
    assert u.bids[0] == (100.0, 1.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        u.exchange = "binance"  # type: ignore[misc]


def test_venuespec_pairs_quote_and_tier():
    spec = VenueSpec(exchange="upbit", quote=Quote.KRW, tier=Tier.B)
    assert spec.tier is Tier.B
    assert spec.quote is Quote.KRW


def test_depthbin_and_snapshot_construct():
    b = DepthBin(price=100.0, size=1.5, composition={"kraken": 1.0, "coinbase": 0.5})
    snap = CombinedDepthSnapshot(
        ts=1.0, mid=100.5, bids=(b,), asks=(), venues_active=("kraken", "coinbase"), venues_total=2
    )
    assert snap.bids[0].composition["coinbase"] == 0.5
    assert snap.venues_total == 2
