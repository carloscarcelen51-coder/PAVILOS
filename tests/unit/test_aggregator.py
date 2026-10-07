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


def test_checkpoint_updates_copy_active_books_only_and_do_not_touch_them():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=10.0)
    agg.apply(_snap("kraken", 5.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
    agg.apply(_snap("coinbase", 1.0, [(100.0, 1.0)], [(101.0, 1.0)]))   # stale at t=20
    cks = agg.checkpoint_updates(now=12.0)
    assert [c.exchange for c in cks] == ["kraken"]                     # coinbase stale -> excluded
    ck = cks[0]
    assert ck.checkpoint is True and ck.is_snapshot is True
    assert ck.ts == 5.0                                                 # = book.last_ts, NOT now
    assert dict(ck.bids) == {100.0: 1.0, 99.0: 2.0} and dict(ck.asks) == {101.0: 3.0}
    assert agg._books["kraken"].last_ts == 5.0                         # never applied


def test_run_emits_periodic_checkpoints_to_on_update_without_applying_them():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=100.0)
    clock = {"t": 0.0}
    seen: list = []

    async def scenario():
        in_q: asyncio.Queue = asyncio.Queue(); out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await in_q.put(_snap("kraken", 0.5, [(100.0, 1.0)], [(101.0, 1.0)]))
        task = asyncio.create_task(agg.run(in_q, out_q, interval_s=0.01, now=lambda: clock["t"],
                                           stop=stop, on_update=seen.append, checkpoint_every_s=60.0))
        await asyncio.sleep(0.05)                 # first pass schedules the first checkpoint
        clock["t"] = 59.0; await asyncio.sleep(0.05)
        assert not any(u.checkpoint for u in seen)
        clock["t"] = 61.0; await asyncio.sleep(0.05)
        stop.set(); await task

    asyncio.run(scenario())
    cks = [u for u in seen if u.checkpoint]
    assert len(cks) == 1 and cks[0].exchange == "kraken" and cks[0].ts == 0.5
    assert seen.index(cks[0]) > 0                 # emitted AFTER the update it reflects
    assert agg._books["kraken"].last_ts == 0.5


def test_run_without_checkpoint_setting_emits_none():
    agg = Aggregator(_specs(), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=100.0)
    seen: list = []

    async def scenario():
        in_q: asyncio.Queue = asyncio.Queue(); out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await in_q.put(_snap("kraken", 0.5, [(100.0, 1.0)], [(101.0, 1.0)]))
        task = asyncio.create_task(agg.run(in_q, out_q, interval_s=0.01, now=lambda: 10_000.0,
                                           stop=stop, on_update=seen.append))
        await asyncio.sleep(0.05); stop.set(); await task

    asyncio.run(scenario())
    assert seen and not any(u.checkpoint for u in seen)


def test_checkpoint_schedule_survives_a_backwards_clock_jump():
    import asyncio
    agg = Aggregator(_specs(), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=1e9)
    agg.apply(BookUpdate("kraken", 1.0, ((100.0, 1.0),), ((101.0, 1.0),), True, None))
    seen = []
    clock = {"t": 0.0, "mark": None}

    def now():
        clock["t"] += 0.5
        if clock["mark"] is None and any(u.checkpoint for u in seen):
            clock["t"] -= 3600.0                         # the PC clock steps back one hour
            clock["mark"] = len(seen)
        return clock["t"]

    async def scenario():
        stop = asyncio.Event()
        task = asyncio.create_task(agg.run(asyncio.Queue(), asyncio.Queue(), interval_s=0.0, now=now,
                                           stop=stop, on_update=seen.append, checkpoint_every_s=2.0))
        for _ in range(400):
            await asyncio.sleep(0)
        stop.set()
        await task

    asyncio.run(scenario())
    assert clock["mark"] is not None
    assert len(seen) - clock["mark"] >= 5               # checkpoints keep coming after the jump


def test_invalid_checkpoint_periods_are_rejected():
    import asyncio
    import pytest
    from pavilos.core.engine import Engine
    agg = Aggregator(_specs(), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=15.0)
    for bad in (0.0, -1.0, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            Engine([], agg, checkpoint_every_s=bad)
        with pytest.raises(ValueError):
            asyncio.run(agg.run(asyncio.Queue(), asyncio.Queue(), interval_s=0.0, now=lambda: 0.0,
                                stop=asyncio.Event(), on_update=lambda u: None, checkpoint_every_s=bad))
    Engine([], agg, checkpoint_every_s=None)             # None = off
    Engine([], agg, checkpoint_every_s=60.0)
