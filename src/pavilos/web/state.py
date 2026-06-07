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
    "fills": [], "venues": [], "stale": False, "trades": [], "summary": {},
}


def _zone(z) -> dict:
    return {"side": z.side.value, "price": z.price, "low": z.low, "high": z.high,
            "strength": z.strength, "venues": list(z.venues),
            "persistence_s": z.persistence_s, "pulled": z.pulled, "confidence": z.confidence}


def _trade(t) -> dict:
    return {"side": t.side, "size": t.size, "entry": t.entry, "exit": t.exit,
            "entry_ts": t.entry_ts, "exit_ts": t.exit_ts, "pnl": t.pnl,
            "fee": t.fee, "return_pct": t.return_pct, "reason": t.reason}


class DashboardState:
    def __init__(self) -> None:
        self._snap: dict = dict(_EMPTY)

    def snapshot(self) -> dict:
        return self._snap

    def update(self, analysis: DepthAnalysis, broker: PaperBroker, health,
               *, engine_state: str, now: float, staleness_s: float = 15.0,
               trades=(), summary=None) -> None:
        pos = broker.position()
        pend = broker.pending_entry()
        fills = broker.fills()[-12:]
        mark_equity = broker.equity(analysis.mid)  # mark-to-market once; reuse below
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
            "equity": mark_equity,
            "realized_equity": mark_equity if pos is None else mark_equity - (
                pos.size * (analysis.mid - pos.entry) if pos.side == "LONG" else pos.size * (pos.entry - analysis.mid)),
            "fills": [{"ts": f.ts, "side": f.side, "price": f.price, "size": f.size,
                       "fee": f.fee, "kind": f.kind} for f in fills],
            "venues": [{"exchange": h.exchange, "connected": h.connected,
                        "last_update_ts": h.last_update_ts, "resyncs": h.resyncs, "errors": h.errors}
                       for h in health],
            "stale": (now - analysis.ts) > staleness_s,
            "trades": [_trade(t) for t in trades],
            "summary": dict(summary) if summary else {},
        }
        self._snap = snap  # atomic swap
