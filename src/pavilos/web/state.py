# src/pavilos/web/state.py
"""Latest-state holder bridging the async trading loop (single writer) and the
web layer (many readers). update() serializes domain objects to a JSON-able dict;
snapshot() returns the latest by atomic reference read (no lock needed: CPython
attribute assignment is atomic, and readers always get a complete prior dict)."""
from __future__ import annotations

from pavilos.detection.models import DepthAnalysis
from pavilos.execution.broker import PaperBroker

_EMPTY: dict = {
    "ts": None, "mid": None, "state": "IDLE", "supports": [], "resistances": [],
    "position": None, "pending": None, "equity": None, "realized_equity": None,
    "fills": [], "venues": [], "stale": False,
}


def _zone(z) -> dict:
    return {"side": z.side.value, "price": z.price, "low": z.low, "high": z.high,
            "strength": z.strength, "venues": list(z.venues),
            "persistence_s": z.persistence_s, "confidence": z.confidence}


class DashboardState:
    def __init__(self) -> None:
        self._snap: dict = dict(_EMPTY)

    def snapshot(self) -> dict:
        return self._snap

    def update(self, analysis: DepthAnalysis, broker: PaperBroker, health,
               *, engine_state: str, now: float, staleness_s: float = 15.0) -> None:
        pos = broker.position()
        pend = broker.pending_entry()
        fills = broker.fills()[-12:]
        snap = {
            "ts": analysis.ts,
            "mid": analysis.mid,
            "state": engine_state,
            "supports": [_zone(z) for z in analysis.supports],
            "resistances": [_zone(z) for z in analysis.resistances],
            "position": None if pos is None else {
                "side": pos.side, "size": pos.size, "entry": pos.entry, "stop": pos.stop},
            "pending": None if pend is None else {
                "side": pend["side"], "trigger": pend["trigger"], "stop": pend["stop"], "size": pend["size"]},
            "equity": broker.equity(analysis.mid),
            "realized_equity": broker.equity(analysis.mid) if pos is None else broker.equity(analysis.mid) - (
                pos.size * (analysis.mid - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - analysis.mid)),
            "fills": [{"ts": f.ts, "side": f.side, "price": f.price, "size": f.size,
                       "fee": f.fee, "kind": f.kind} for f in fills],
            "venues": [{"exchange": h.exchange, "connected": h.connected,
                        "last_update_ts": h.last_update_ts, "resyncs": h.resyncs, "errors": h.errors}
                       for h in health],
            "stale": (now - analysis.ts) > staleness_s,
        }
        self._snap = snap  # atomic swap
