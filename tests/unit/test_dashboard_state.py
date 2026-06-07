# tests/unit/test_dashboard_state.py
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.detection.models import Side, Zone, DepthAnalysis
from pavilos.connectors.base import ConnectorHealth
from pavilos.execution.broker import PaperBroker
from pavilos.web.state import DashboardState


def _zone(side, price, conf):
    return Zone(side=side, price=price, low=price - 1, high=price + 1, strength=12.0,
                venues=("kraken", "binance"), persistence_s=8.0, pulled=False, confidence=conf)


def _analysis():
    return DepthAnalysis(ts=10.0, mid=100.0,
                         supports=(_zone(Side.SUPPORT, 99.0, 0.7),),
                         resistances=(_zone(Side.RESISTANCE, 101.0, 0.5),))


def test_initial_snapshot_is_empty_but_shaped():
    s = DashboardState().snapshot()
    assert s["mid"] is None and s["supports"] == [] and s["position"] is None
    assert s["venues"] == [] and s["state"] == "IDLE"


def test_update_serializes_domain_objects():
    st = DashboardState()
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    bk.place_entry("LONG", trigger=100.0, stop=98.0, size=1.0)
    bk.on_price(100.0, ts=10.0)  # fill -> position open
    health = [ConnectorHealth("kraken", True, 10.0, 0, 0)]
    st.update(_analysis(), bk, health, engine_state="IN_POSITION", now=10.0)
    snap = st.snapshot()
    assert snap["mid"] == 100.0 and snap["state"] == "IN_POSITION"
    assert snap["supports"][0]["price"] == 99.0 and snap["supports"][0]["confidence"] == 0.7
    assert snap["supports"][0]["pulled"] is False
    assert snap["resistances"][0]["side"] == "resistance"
    assert snap["position"]["side"] == "LONG" and snap["position"]["size"] == 1.0
    assert snap["equity"] == 10_000.0  # unrealized 0 at mark 100
    assert snap["venues"][0]["exchange"] == "kraken" and snap["venues"][0]["connected"] is True


def test_stale_flag_true_when_wall_clock_exceeds_feed_ts():
    # now is a real wall clock, distinct from the feed ts; when feed lag exceeds
    # staleness_s the served snapshot must report stale=True (the only path that
    # surfaces a frozen/wedged feed while the server is still alive).
    st = DashboardState()
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    health = [ConnectorHealth("kraken", True, 10.0, 0, 0)]
    # analysis.ts == 10.0; now == 30.0 -> 20s lag > 15s staleness
    st.update(_analysis(), bk, health, engine_state="IDLE", now=30.0, staleness_s=15.0)
    assert st.snapshot()["stale"] is True


def test_update_includes_trades_and_summary():
    from pavilos.execution.broker import Trade
    from pavilos.execution.trade_log import summarize
    st = DashboardState()
    bk = PaperBroker(starting_equity=10_000.0, taker_fee=0.0, maker_fee=0.0)
    trades = [Trade("LONG", 1.0, 100.0, 105.0, 1.0, 2.0, 5.0, 0.0, 5.0, "close")]
    st.update(_analysis(), bk, [], engine_state="IDLE", now=10.0,
              trades=trades, summary=summarize(trades, base_equity=10_000.0))
    snap = st.snapshot()
    assert snap["trades"][0]["pnl"] == 5.0 and snap["trades"][0]["reason"] == "close"
    assert snap["summary"]["n_trades"] == 1 and snap["summary"]["wins"] == 1


def test_initial_snapshot_has_empty_trades_and_summary():
    snap = DashboardState().snapshot()
    assert snap["trades"] == [] and snap["summary"] == {}
