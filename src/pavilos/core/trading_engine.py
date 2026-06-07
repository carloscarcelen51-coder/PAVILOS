# src/pavilos/core/trading_engine.py
"""TradingEngine: wire the combined-book snapshot stream through detection ->
signals -> paper broker. Network-free; the snapshot source is injected."""
from __future__ import annotations

import asyncio

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine


class TradingEngine:
    def __init__(self, detector: Detector, signal: SignalEngine, broker, atr: ATR) -> None:
        self.detector = detector
        self.signal = signal
        self.broker = broker
        self.atr = atr

    def process(self, snapshot: CombinedDepthSnapshot) -> None:
        """One snapshot through the full pipeline (sync, deterministic)."""
        analysis = self.detector.update(snapshot)
        self.atr.update(snapshot.mid)
        self.signal.update(analysis, self.atr.value(), self.broker)

    async def run(self, snapshots: "asyncio.Queue[CombinedDepthSnapshot]",
                  stop: "asyncio.Event") -> None:
        """Consume snapshots until stop is set."""
        while not stop.is_set():
            snap = await snapshots.get()
            if stop.is_set():
                break
            self.process(snap)
