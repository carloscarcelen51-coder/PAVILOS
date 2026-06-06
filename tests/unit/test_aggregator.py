# tests/unit/test_aggregator.py
import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator


def _snap(exchange, ts, bids, asks, seq=None):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks),
                      is_snapshot=True, seq=seq)


def _specs():
    return [
        VenueSpec("kraken", Quote.USD, Tier.A),
        VenueSpec("coinbase", Quote.USD, Tier.A),
    ]


def test_aggregator_routes_updates_and_builds_snapshot():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    agg.apply(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    agg.apply(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    snap = agg.snapshot(now=2.0)
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    assert set(snap.venues_active) == {"kraken", "coinbase"}


def test_aggregator_excludes_stale_venue():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    agg.apply(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    agg.apply(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    # 'now' is 10s later; staleness_s is 5 -> both feeds are stale -> no snapshot
    assert agg.snapshot(now=11.0) is None
    # a fresh coinbase update revives only coinbase
    agg.apply(_snap("coinbase", 11.0, [(100.0, 0.5)], [(101.0, 0.5)]))
    snap = agg.snapshot(now=12.0)
    assert snap is not None
    assert snap.venues_active == ("coinbase",)


def test_aggregator_rejects_unknown_exchange():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
    with pytest.raises(KeyError):
        agg.apply(_snap("ftx", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
