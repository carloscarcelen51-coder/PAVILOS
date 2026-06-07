# tests/unit/test_paper_broker_trades.py
from pavilos.execution.broker import PaperBroker, Trade


def test_close_records_a_net_pnl_trade_and_calls_callback():
    seen = []
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002, on_trade=seen.append)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=2.0)
    bk.on_price(100.0, ts=1.0)      # entry fill; entry fee = 2*100*0.0005 = 0.10
    bk.on_price(110.0, ts=2.0)      # mark up
    bk.close(ts=3.0)                # close at 110; exit fee = 2*110*0.0005 = 0.11
    assert len(bk.trades()) == 1 and len(seen) == 1
    t = bk.trades()[0]
    assert isinstance(t, Trade) and t.side == "LONG" and t.size == 2.0
    assert t.entry == 100.0 and t.exit == 110.0 and t.reason == "close"
    assert t.entry_ts == 1.0 and t.exit_ts == 3.0
    # gross = 2*(110-100)=20 ; fees = 0.10+0.11=0.21 ; net = 19.79
    assert abs(t.fee - 0.21) < 1e-9
    assert abs(t.pnl - 19.79) < 1e-9
    assert abs(t.return_pct - (19.79 / (100.0 * 2.0) * 100.0)) < 1e-9
    # net pnl reconciles with equity change vs starting (no funding)
    assert abs(bk.equity() - (10_000.0 + 19.79)) < 1e-9


def test_stop_out_records_loss_trade_with_reason_stop():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0005, maker_fee=0.0002)
    bk.place_entry("SHORT", trigger=100.0, stop=102.0, size=1.0)
    bk.on_price(100.0, ts=1.0)      # short fill; entry fee 0.05
    bk.on_price(103.0, ts=2.0)      # >= stop -> stop fills at breaching price 103; exit fee 1*103*0.0005=0.0515
    assert bk.position() is None
    t = bk.trades()[0]
    # gross = 1*(100-103) = -3 ; fees 0.05+0.0515 ; net = -3.1015
    assert t.reason == "stop" and t.side == "SHORT"
    assert abs(t.pnl - (-3.0 - 0.05 - 0.0515)) < 1e-9
    assert t.exit == 103.0


def test_no_trade_recorded_without_a_close():
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=1.0)      # only an entry, still open
    assert bk.trades() == []
