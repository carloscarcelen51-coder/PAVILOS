# src/pavilos/connectors/base.py
"""Shared connector vocabulary: resync signal and health snapshot. No logic."""
from __future__ import annotations

from dataclasses import dataclass


class ResyncRequired(Exception):
    """Raised when a connector's local book is out of sync and the caller must
    discard it and re-seed (e.g. a Binance sequence gap, or a Kraken checksum
    mismatch). Transport code catches this and re-subscribes / re-fetches."""


@dataclass(slots=True, frozen=True)
class ConnectorHealth:
    """Point-in-time health of one connector, surfaced to monitoring/dashboard."""

    exchange: str
    connected: bool
    last_update_ts: float
    resyncs: int
    errors: int
