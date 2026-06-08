# tests/unit/test_aggregator.py
import asyncio

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
    with pytest.raises(KeyError) as exc_info:
        agg.apply(_snap("ftx", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    msg = str(exc_info.value)
    assert "ftx" in msg
    assert "kraken" in msg and "coinbase" in msg   # configured venues listed for diagnosis


def test_run_emits_snapshot_then_stops():
    async def scenario():
        agg = Aggregator(_specs(), PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=5.0)
        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        await in_q.put(_snap("kraken", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
        await in_q.put(_snap("coinbase", 1.0, [(100.0, 0.5)], [(101.0, 0.5)]))

        clock = {"t": 2.0}
        stop = asyncio.Event()

        task = asyncio.create_task(
            agg.run(in_q, out_q, interval_s=0.0, now=lambda: clock["t"], stop=stop)
        )
        snap = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return snap

    snap = asyncio.run(scenario())
    assert snap is not None
    assert set(snap.venues_active) == {"kraken", "coinbase"}


def test_run_calls_on_update_for_each_update():
    import asyncio
    from pavilos.aggregator.aggregator import Aggregator
    from pavilos.aggregator.normalize import PegProvider
    from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier

    seen = []
    agg = Aggregator([VenueSpec("kraken", Quote.USD, Tier.A)], PegProvider(),
                     bin_bps=5.0, window_bps=300.0, staleness_s=15.0)

    async def scenario():
        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await in_q.put(BookUpdate(exchange="kraken", ts=1.0, bids=((100.0, 1.0),),
                                  asks=((101.0, 1.0),), is_snapshot=True, seq=1))
        task = asyncio.create_task(agg.run(in_q, out_q, interval_s=0.01, now=lambda: 1.0,
                                           stop=stop, on_update=seen.append))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())
    assert len(seen) == 1 and seen[0].exchange == "kraken"
