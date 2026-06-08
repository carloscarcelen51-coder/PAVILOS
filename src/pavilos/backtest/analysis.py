# src/pavilos/backtest/analysis.py
"""Window sweep: for each candidate window_bps, re-aggregate the lake and measure
detection quality (zones surfaced) + downstream backtest P&L. Answers 'is 300 the
right window?' empirically. detection_profile() is pure over a snapshot list."""
from __future__ import annotations

import dataclasses
from statistics import mean

from pavilos.core.runtime import RuntimeConfig
from pavilos.signals.atr import ATR
from pavilos.backtest.runner import run_backtest, _detector
from pavilos.backtest.replay import replay_snapshots


def detection_profile(snapshots, config: RuntimeConfig) -> dict:
    """Run the detector over ``snapshots`` and summarise zone quality."""
    if not snapshots:
        return {"n_snapshots": 0, "avg_zones_per_snapshot": 0.0, "avg_confidence": 0.0,
                "avg_venues_per_zone": 0.0, "frac_snaps_with_strong_zone": 0.0}
    detector = _detector(config)
    atr = ATR(window=config.atr_window)
    counts: list[int] = []
    confs: list[float] = []
    venues: list[int] = []
    strong = 0
    for s in snapshots:
        analysis = detector.update(s)
        atr.update(s.mid)
        zones = list(analysis.supports) + list(analysis.resistances)
        counts.append(len(zones))
        confs.extend(z.confidence for z in zones)
        venues.extend(len(z.venues) for z in zones)
        if any(z.confidence >= config.entry_threshold and len(z.venues) >= config.min_venues for z in zones):
            strong += 1
    return {
        "n_snapshots": len(snapshots),
        "avg_zones_per_snapshot": mean(counts),
        "avg_confidence": mean(confs) if confs else 0.0,
        "avg_venues_per_zone": mean(venues) if venues else 0.0,
        "frac_snaps_with_strong_zone": strong / len(snapshots),
    }


def window_sweep(base_dir: str, t0: float, t1: float, windows, *, base_config: RuntimeConfig,
                 starting_equity: float) -> list[dict]:
    """For each window_bps: re-aggregate the lake, profile detection, and backtest.
    Sets BOTH window_bps (aggregator) and det_window_bps (detector) to the swept value."""
    out: list[dict] = []
    for w in windows:
        snaps = replay_snapshots(base_dir, t0, t1, window_bps=w, bin_bps=base_config.bin_bps,
                                 interval_s=base_config.snapshot_interval_s, staleness_s=base_config.staleness_s)
        cfg = dataclasses.replace(base_config, window_bps=w, det_window_bps=w)
        out.append({
            "window_bps": w,
            "n_snapshots": len(snaps),
            "detection": detection_profile(snaps, cfg),
            "backtest": run_backtest(snaps, config=cfg, starting_equity=starting_equity),
        })
    return out
