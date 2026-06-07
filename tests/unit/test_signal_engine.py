# tests/unit/test_signal_engine.py
import pytest

from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.execution.broker import PaperBroker
from pavilos.signals.engine import SignalEngine


def _zone(side, price, low, high, conf=0.9, persistence_s=100.0, venues=("k", "b", "c")):
    return Zone(side=side, price=price, low=low, high=high, strength=20.0,
                venues=venues, persistence_s=persistence_s, pulled=False, confidence=conf)


def _analysis(ts, mid, supports=(), resistances=()):
    return DepthAnalysis(ts=ts, mid=mid, supports=tuple(supports), resistances=tuple(resistances))


def _engine():
    return SignalEngine(entry_threshold=0.6, trail_threshold=0.6, opposing_threshold=0.7,
                        min_persistence_s=5.0, min_venues=2, entry_offset_bps=2.0,
                        stop_offset_bps=2.0, atr_stop_mult=3.0, opposing_distance_bps=30.0,
                        risk_pct=0.01, max_leverage=10.0)


def _bk():
    return PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0, funding_rate_hourly=0.0)


def test_arms_buy_stop_above_price_with_support_as_stop():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)   # support below price
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    assert e.state == "PENDING_ENTRY"
    pend = bk.pending_entry()
    assert pend["side"] == "LONG"
    assert abs(pend["trigger"] - 100.0 * (1 + 2.0 / 1e4)) < 1e-9   # buy-stop just above price
    assert abs(pend["stop"] - 98.8 * (1 - 2.0 / 1e4)) < 1e-9       # just below the support (the floor)


def test_cancels_pending_when_thesis_support_vanishes():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    assert e.state == "PENDING_ENTRY"
    # next snapshot: price did not rise to the buy-stop trigger, and the support vanished
    e.update(_analysis(2.0, mid=100.0, supports=[]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.pending_entry() is None and bk.position() is None


def test_fill_transitions_to_in_position():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)   # arm buy-stop ~100.02
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)   # price rises through -> fill
    assert e.state == "IN_POSITION" and bk.position().side == "LONG"


def test_trails_stop_up_as_higher_supports_form_but_not_inside_atr_floor():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill, stop ~98.78
    stop0 = bk.position().stop
    higher = _zone(Side.SUPPORT, price=104.0, low=103.8, high=104.2)
    # price 110, atr 1, atr_floor=110-3=107; support_stop=103.8*(1-2bps)=~103.78 < 107 -> stop->~103.78
    e.update(_analysis(3.0, mid=110.0, supports=[higher]), atr=1.0, broker=bk)
    assert bk.position().stop > stop0
    assert abs(bk.position().stop - 103.8 * (1 - 2.0 / 1e4)) < 1e-6


def test_exits_on_near_opposing_resistance():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill
    assert e.state == "IN_POSITION"
    res = _zone(Side.RESISTANCE, price=101.2, low=101.1, high=101.3, conf=0.9)  # ~10bps above 101
    e.update(_analysis(3.0, mid=101.0, supports=[sup], resistances=[res]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.position() is None


def test_stop_out_returns_to_idle():
    e, bk = _engine(), _bk()
    sup = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2)
    e.update(_analysis(1.0, mid=100.0, supports=[sup]), atr=1.0, broker=bk)
    e.update(_analysis(2.0, mid=101.0, supports=[sup]), atr=1.0, broker=bk)  # fill, stop ~98.78
    e.update(_analysis(3.0, mid=98.0, supports=[sup]), atr=1.0, broker=bk)   # below stop -> stop-out
    assert e.state == "IDLE" and bk.position() is None


def test_ignores_non_operable_zone():
    e, bk = _engine(), _bk()
    weak = _zone(Side.SUPPORT, price=99.0, low=98.8, high=99.2, conf=0.5)  # below entry_threshold
    e.update(_analysis(1.0, mid=100.0, supports=[weak]), atr=1.0, broker=bk)
    assert e.state == "IDLE" and bk.pending_entry() is None
