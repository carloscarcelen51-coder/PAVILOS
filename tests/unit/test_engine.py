# tests/unit/test_engine.py
import asyncio

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.engine import Engine


class _FakeConnector:
    """Emits a fixed list of BookUpdates onto out_q, then idles until stop."""
    def __init__(self, exchange, updates):
        self.exchange = exchange
        self._updates = updates

    async def run(self, out_q, stop):
        for u in self._updates:
            await out_q.put(u)
        await stop.wait()


def _snap(exchange, ts, bids, asks):
    return BookUpdate(exchange=exchange, ts=ts, bids=tuple(bids), asks=tuple(asks), is_snapshot=True, seq=None)


def test_engine_produces_combined_snapshot_from_connectors():
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("binance", Quote.USDT, Tier.A)]
    connectors = [
        _FakeConnector("kraken", [_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)])]),
        _FakeConnector("binance", [_snap("binance", 1.0, [(100.0, 0.5)], [(101.0, 0.5)])]),
    ]

    async def scenario():
        agg = Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)
        engine = Engine(connectors, agg, interval_s=0.0, now=lambda: 2.0)
        await engine.start()
        snap = await asyncio.wait_for(engine.snapshots.get(), timeout=1.0)
        await engine.stop()
        return snap

    snap = asyncio.run(scenario())
    assert snap is not None
    assert set(snap.venues_active) == {"kraken", "binance"}
    assert snap.mid == 100.5


def test_engine_stop_cancels_a_wedged_connector():
    # A connector that never observes stop must NOT hang Engine.stop().
    from pavilos.core.models import VenueSpec, Quote, Tier
    from pavilos.aggregator.normalize import PegProvider
    from pavilos.aggregator.aggregator import Aggregator

    class _HangConnector:
        exchange = "hang"
        async def run(self, out_q, stop):
            await asyncio.Event().wait()  # never returns; ignores stop

    async def scenario():
        specs = [VenueSpec("kraken", Quote.USD, Tier.A)]
        agg = Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)
        engine = Engine([_HangConnector()], agg, interval_s=0.0, now=lambda: 1.0)
        await engine.start()
        await asyncio.wait_for(engine.stop(grace=0.05), timeout=2.0)  # must NOT hang
        return True

    assert asyncio.run(scenario()) is True
