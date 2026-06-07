# src/pavilos/connectors/okx.py
"""OKX v5 ``books`` channel sequencer (pure). Integrity is seqId/prevSeqId
continuity (CRC32 is being deprecated to 0 on 2026-06-23, so seqId is primary)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(rows: list[list[str]]) -> tuple[tuple[float, float], ...]:
    # each row is [price, size, deprecated, num_orders]; size "0" removes (abs)
    return tuple((float(r[0]), float(r[1])) for r in rows)


class OKXFeed:
    """Turns OKX ``books`` frames into ``BookUpdate``s. A snapshot (action
    'snapshot', prevSeqId == -1) resets; an update is valid iff its prevSeqId
    equals the last seqId. A gap (prevSeqId mismatch) or a reset (seqId <
    prevSeqId) raises ResyncRequired. seqId == prevSeqId is a benign no-op."""

    def __init__(self, exchange: str = "okx") -> None:
        self.exchange = exchange
        self._last_seq: int | None = None

    def process(self, msg: dict, *, ts: float) -> BookUpdate | None:
        if msg.get("arg", {}).get("channel") != "books" or "action" not in msg or not msg.get("data"):
            return None  # subscribe ack / event / other channel
        action = msg["action"]
        data = msg["data"][0]
        seq_id = data.get("seqId")
        prev = data.get("prevSeqId")
        is_snapshot = action == "snapshot"
        if not is_snapshot:
            if seq_id is not None and prev is not None and seq_id < prev:
                raise ResyncRequired(f"okx seqId reset: seqId={seq_id} < prevSeqId={prev}")
            if self._last_seq is not None and prev is not None and prev != -1 and prev != self._last_seq:
                raise ResyncRequired(f"okx seqId gap: prevSeqId={prev} != last={self._last_seq}")
        if seq_id is not None:
            self._last_seq = seq_id
        return BookUpdate(exchange=self.exchange, ts=ts, bids=_levels(data.get("bids", [])),
                          asks=_levels(data.get("asks", [])), is_snapshot=is_snapshot, seq=seq_id)
