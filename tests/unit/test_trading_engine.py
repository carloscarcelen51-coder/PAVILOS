import asyncio

from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.execution.broker import PaperBroker
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.core.trading_engine import TradingEngine


def _bin(price, size, venues=("k", "b")):
    return DepthBin(price=price, size=size, composition={v: size / len(venues) for v in venues})


def _snap(ts, mid, bids, asks):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=tuple(bids), asks=tuple(asks),
                                 venues_active=("k", "b"), venues_total=2)


def _components():
    detector = Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                        match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                        persistence_target_s=1.0, venues_target=2.0, strength_target=5.0)
    signal = SignalEngine(entry_threshold=0.3, trail_threshold=0.3, opposing_threshold=0.7,
                          min_persistence_s=0.0, min_venues=2, entry_offset_bps=2.0,
                          stop_offset_bps=2.0, atr_stop_mult=3.0, opposing_distance_bps=30.0,
                          risk_pct=0.01, max_leverage=10.0)
    broker = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0, funding_rate_hourly=0.0)
    return TradingEngine(detector, signal, broker, ATR(window=10))


_BIDS = [_bin(100.0, 1.0), _bin(99.0, 10.0), _bin(98.0, 1.0)]   # support wall ~99 (below mid 99.5)
_ASKS = [_bin(100.5, 1.0)]


def test_process_runs_pipeline_and_arms_then_fills():
    te = _components()
    te.process(_snap(1.0, 99.5, _BIDS, _ASKS))   # first sighting: persistence 0 -> conf 0, no arm
    te.process(_snap(2.0, 99.5, _BIDS, _ASKS))   # support persisted -> conf clears threshold -> arm
    assert te.signal.state == "PENDING_ENTRY"
    te.process(_snap(3.0, 102.0, _BIDS, _ASKS))  # mid rises through the buy-stop -> fill
    assert te.signal.state == "IN_POSITION" and te.broker.position() is not None


def test_run_consumes_queue_until_stop():
    te = _components()

    async def scenario():
        q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        await q.put(_snap(1.0, 99.5, _BIDS, _ASKS))
        await q.put(_snap(2.0, 99.5, _BIDS, _ASKS))
        await q.put(_snap(3.0, 102.0, _BIDS, _ASKS))
        task = asyncio.create_task(te.run(q, stop))
        for _ in range(200):
            if te.broker.position() is not None:
                break
            await asyncio.sleep(0)
        stop.set()
        await q.put(_snap(4.0, 102.0, _BIDS, _ASKS))  # unblock the queue.get so run() observes stop
        await asyncio.wait_for(task, timeout=1.0)
        return te.broker.position()

    assert asyncio.run(scenario()) is not None
