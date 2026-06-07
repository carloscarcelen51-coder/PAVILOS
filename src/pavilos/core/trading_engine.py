# src/pavilos/core/trading_engine.py
"""TradingEngine: wire the combined-book snapshot stream through detection ->
signals -> paper broker. Network-free; the snapshot source is injected."""
from __future__ import annotations

import asyncio

from pavilos.core.models import CombinedDepthSnapshot
from pavilos.detection.detector import Detector
from pavilos.execution.broker import PaperBroker
from pavilos.signals.atr import ATR
from pavilos.signals.engine import SignalEngine


class TradingEngine:
    """Wires the snapshot stream through detection -> ATR -> signals; the broker
    is injected and the whole pipeline is network-free.

    Pipeline order mirrors the constructor argument order: ``detect -> atr ->
    signal``. ``run`` is a crash-loud consume loop (a per-tick exception
    propagates out and stops the loop; the caller/supervisor must await the task
    to observe it), and its shutdown is bounded — ``stop`` is honoured even when
    the queue is idle.
    """

    def __init__(self, detector: Detector, atr: ATR, signal: SignalEngine,
                 broker: PaperBroker) -> None:
        self.detector = detector
        self.atr = atr
        self.signal = signal
        self.broker = broker

    def process(self, snapshot: CombinedDepthSnapshot) -> None:
        """One snapshot through the full pipeline (sync, deterministic)."""
        analysis = self.detector.update(snapshot)
        self.atr.update(snapshot.mid)
        self.signal.update(analysis, self.atr.value(), self.broker)

    async def run(self, snapshots: "asyncio.Queue[CombinedDepthSnapshot]",
                  stop: "asyncio.Event") -> None:
        """Consume snapshots until ``stop`` is set.

        Shutdown is bounded regardless of item arrival: the get is raced against
        ``stop.wait()`` so an idle (empty) queue still wakes the loop when
        ``stop`` fires — mirroring ``Aggregator.run``'s bounded-shutdown
        contract instead of wedging until the next snapshot.

        Crash-loud: an exception anywhere in :meth:`process` propagates out of
        this coroutine and terminates the loop (consistent with the project's
        other connector/aggregator loops). A supervisor that launches this task
        MUST await it to surface such failures; otherwise the strategy goes
        offline silently.
        """
        while not stop.is_set():
            getter = asyncio.ensure_future(snapshots.get())
            stopper = asyncio.ensure_future(stop.wait())
            done, _ = await asyncio.wait(
                {getter, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if stopper in done:
                if getter in done:
                    # stop and an arrival raced: process the already-dequeued
                    # snapshot rather than dropping it, then exit.
                    self.process(getter.result())
                else:
                    getter.cancel()  # nothing dequeued; cancel the pending get
                break
            stopper.cancel()  # got an item before stop; tidy the stop-waiter
            self.process(getter.result())
