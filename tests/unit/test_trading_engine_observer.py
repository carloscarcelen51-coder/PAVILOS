# tests/unit/test_trading_engine_observer.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.execution.broker import PaperBroker
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine
from pavilos.core.trading_engine import TradingEngine


def _snap(ts, mid):
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=(), asks=(),
                                 venues_active=("k",), venues_total=1)


def _te(observer):
    d = Detector(size_multiple=3.0, min_size=0.0, max_gap_bps=20.0, max_zone_width_bps=50.0,
                 match_overlap_bps=10.0, grace_s=0.0, window_bps=500.0,
                 persistence_target_s=1.0, venues_target=2.0, strength_target=5.0)
    s = SignalEngine(entry_threshold=0.3, trail_threshold=0.3, opposing_threshold=0.7,
                     min_persistence_s=0.0, min_venues=2, entry_offset_bps=2.0, stop_offset_bps=2.0,
                     atr_stop_mult=3.0, opposing_distance_bps=30.0, risk_pct=0.01, max_leverage=10.0)
    b = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    return TradingEngine(d, ATR(window=10), s, b, observer=observer)


def test_observer_called_with_analysis_and_broker():
    seen = []
    te = _te(lambda snap, analysis, broker: seen.append((analysis.mid, broker)))
    te.process(_snap(1.0, 100.0))
    assert len(seen) == 1 and seen[0][0] == 100.0


def test_observer_optional_default_none():
    te = _te(None)
    te.process(_snap(1.0, 100.0))  # must not raise
