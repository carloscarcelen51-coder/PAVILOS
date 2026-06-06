# src/pavilos/connectors/binance.py
"""Binance Spot diff. depth sequencer: REST snapshot + diff events -> BookUpdates.

No I/O. The transport supplies already-decoded dicts; this class enforces the
documented spot continuity rules and tracks only ``last_update_id`` (the
aggregator's BookState holds the actual book)."""
from __future__ import annotations

from pavilos.core.models import BookUpdate
from pavilos.connectors.base import ResyncRequired


def _levels(raw: list[list[str]]) -> tuple[tuple[float, float], ...]:
    return tuple((float(price), float(qty)) for price, qty in raw)


class BinanceDepthFeed:
    """Sequences Binance spot depth: ``seed`` from a REST snapshot, then ``apply``
    each diff event. Emits ``BookUpdate``s; raises ``ResyncRequired`` on a gap or
    if used before seeding. Spot continuity rule: ``event.U == prev.u + 1`` (no
    ``pu`` field on spot)."""

    def __init__(self, symbol: str, *, exchange: str = "binance") -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._last_update_id: int | None = None

    def seed(self, snapshot: dict, *, ts: float) -> BookUpdate:
        """Seed from a ``GET /api/v3/depth`` response. Returns a snapshot BookUpdate
        carrying the full book; sets ``last_update_id = lastUpdateId``."""
        self._last_update_id = int(snapshot["lastUpdateId"])
        return BookUpdate(
            exchange=self.exchange,
            ts=ts,
            bids=_levels(snapshot["bids"]),
            asks=_levels(snapshot["asks"]),
            is_snapshot=True,
            seq=self._last_update_id,
        )

    def apply(self, event: dict) -> BookUpdate | None:
        """Apply one ``depthUpdate`` event.

        Returns an update ``BookUpdate``, or ``None`` if the event is stale
        (``u <= last_update_id``). Raises ``ResyncRequired`` if not seeded or on a
        gap (``U > last_update_id + 1``). Absolute sizes; ``qty == "0"`` removals
        are passed through verbatim (BookState removes them on apply)."""
        if self._last_update_id is None:
            raise ResyncRequired("binance: apply before seed")
        first_id = int(event["U"])
        final_id = int(event["u"])
        if final_id <= self._last_update_id:
            return None  # stale / already applied
        if first_id > self._last_update_id + 1:
            raise ResyncRequired(
                f"binance: gap (event U={first_id} > last_update_id+1={self._last_update_id + 1})"
            )
        self._last_update_id = final_id
        return BookUpdate(
            exchange=self.exchange,
            ts=float(event["E"]) / 1000.0,
            bids=_levels(event["b"]),
            asks=_levels(event["a"]),
            is_snapshot=False,
            seq=final_id,
        )
