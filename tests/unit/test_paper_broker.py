# tests/unit/test_paper_broker.py
from pavilos.execution.broker import PaperBroker, Position


def _bk(**kw):
    return PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002,
                       funding_rate_hourly=0.0, **kw)


def test_long_entry_fills_on_trigger_and_charges_fee():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(99.0, ts=0.0)              # below trigger -> no fill
    assert bk.position() is None and bk.pending_entry() is not None
    bk.on_price(100.0, ts=1.0)            # touches trigger -> fill
    pos = bk.position()
    assert isinstance(pos, Position) and pos.side == "LONG" and pos.size == 2.0
    assert pos.entry == 100.0 and pos.stop == 98.0
    assert bk.pending_entry() is None
    # entry fee = size*entry*taker = 2*100*0.0005 = 0.10
    assert abs(bk.equity() - (10_000.0 - 0.10)) < 1e-9


def test_long_stop_out_realizes_loss_and_clears_position():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)            # fill
    bk.on_price(97.0, ts=2.0)            # below stop -> fills at the breaching price 97
    assert bk.position() is None
    # pnl = 2*(97-100) = -6.0 ; entry fee 0.10 ; exit fee 2*97*0.0005=0.097
    assert abs(bk.equity() - (10_000.0 - 6.0 - 0.10 - 0.097)) < 1e-9


def test_short_entry_and_stop_are_mirrored():
    bk = _bk()
    bk.place_entry("SHORT", trigger=100.0, stop=102.0, size=1.0)
    bk.on_price(101.0, ts=0.0)           # above trigger -> no short fill
    assert bk.position() is None
    bk.on_price(100.0, ts=1.0)           # touches trigger -> short fills
    assert bk.position().side == "SHORT"
    bk.on_price(103.0, ts=2.0)           # above stop -> fills at the breaching price 103
    assert bk.position() is None
    # pnl = 1*(100-103) = -3.0 ; fees 1*100*.0005 + 1*103*.0005
    assert abs(bk.equity() - (10_000.0 - 3.0 - 0.05 - 0.0515)) < 1e-9


def test_cancel_entry_clears_pending():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.cancel_entry()
    bk.on_price(100.0, ts=1.0)
    assert bk.pending_entry() is None and bk.position() is None
    assert bk.equity() == 10_000.0


def test_modify_stop_and_close_take_profit():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)
    bk.on_price(110.0, ts=2.0)           # price rose; equity reflects unrealized
    assert abs(bk.equity() - (10_000.0 - 0.10 + 2.0 * (110.0 - 100.0))) < 1e-9
    bk.modify_stop(105.0)                 # trail up
    assert bk.position().stop == 105.0
    bk.close(ts=3.0)                      # market close at last price 110
    assert bk.position() is None
    # realized pnl = 2*(110-100)=20 ; exit fee 2*110*.0005=0.11
    assert abs(bk.equity() - (10_000.0 - 0.10 + 20.0 - 0.11)) < 1e-9


def test_funding_charged_hourly_to_longs():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0,
                     funding_rate_hourly=0.0001)
    bk.place_entry("LONG", trigger=100.0, stop=90.0, size=1.0)
    bk.on_price(100.0, ts=0.0)           # fill at t=0
    bk.on_price(100.0, ts=3600.0)        # +1h -> funding = notional*rate = 100*0.0001 = 0.01
    assert abs(bk.equity() - (10_000.0 - 0.01)) < 1e-9


def test_stop_fills_at_gap_price_not_optimistically():
    bk = _bk()
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)            # fill @100
    bk.on_price(90.0, ts=2.0)            # GAP through the stop -> fills at 90, not 98
    assert bk.position() is None
    # pnl = 2*(90-100) = -20.0 ; entry fee 0.10 ; exit fee 2*90*0.0005=0.09
    assert abs(bk.equity() - (10_000.0 - 20.0 - 0.10 - 0.09)) < 1e-9


def test_place_entry_rejects_stop_on_wrong_side():
    import pytest
    bk = _bk()
    with pytest.raises(ValueError):
        bk.place_entry("LONG", trigger=100.0, stop=101.0, size=1.0)   # stop above trigger
    with pytest.raises(ValueError):
        bk.place_entry("SHORT", trigger=100.0, stop=99.0, size=1.0)   # stop below trigger


def test_place_entry_rejects_non_finite_levels():
    import pytest
    bk = _bk()
    with pytest.raises(ValueError):
        bk.place_entry("LONG", trigger=100.0, stop=float("nan"), size=1.0)


def test_enter_market_opens_immediately_at_last_price():
    from pavilos.execution.broker import PaperBroker
    b = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005)
    b.on_price(100.0, ts=1.0)                       # establishes last price
    b.enter_market("LONG", stop=98.0, size=2.0, ts=1.0)
    pos = b.position()
    assert pos is not None and pos.side == "LONG" and pos.entry == 100.0 and pos.stop == 98.0
    # taker fee charged on entry notional
    assert abs(b.equity(100.0) - (10_000.0 - 2.0 * 100.0 * 0.0005)) < 1e-9
    # a LONG stop below the entry still fills via on_price
    b.on_price(97.0, ts=2.0)
    assert b.position() is None and b.trades()[-1].reason == "stop"


def test_enter_market_validates_stop_side_and_state():
    import pytest
    from pavilos.execution.broker import PaperBroker
    b = PaperBroker(starting_equity=10_000.0)
    b.on_price(100.0, ts=1.0)
    with pytest.raises(ValueError):                  # LONG stop must be below price
        b.enter_market("LONG", stop=101.0, size=1.0, ts=1.0)
    with pytest.raises(ValueError):
        b.enter_market("SHORT", stop=99.0, size=1.0, ts=1.0)   # SHORT stop must be above
    b.enter_market("LONG", stop=98.0, size=1.0, ts=1.0)
    with pytest.raises(RuntimeError):                # already in a position
        b.enter_market("LONG", stop=98.0, size=1.0, ts=1.0)
