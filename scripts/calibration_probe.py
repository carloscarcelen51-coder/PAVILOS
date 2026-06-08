# scripts/calibration_probe.py
"""Live calibration probe: run the real 6-venue Engine + Aggregator for N seconds
and capture, per combined snapshot, the binned-depth distribution and what the
Detector would surface — so detection/signal thresholds are tuned from DATA, not
guessed. Network; run from a residential host. Usage:

    python -m scripts.calibration_probe [seconds] [window_bps] [bin_bps]

Prints aggregate stats and writes per-snapshot rows to calibration_probe.jsonl.
"""
from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time

from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.core.engine import Engine
from pavilos.connectors.venues import VENUE_SPECS, build_connector
from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.detector import Detector


def _side_stats(bins, mid: float) -> dict:
    sizes = [b.size for b in bins]
    if not sizes:
        return {"n": 0, "median": 0.0, "max": 0.0, "prom": 0.0, "n_ge2": 0, "n_ge3": 0, "n_ge5": 0,
                "max_dist_bps": 0.0}
    med = statistics.median(sizes)
    mx = max(sizes)
    prom = (mx / med) if med > 0 else 0.0
    n_ge = lambda k: sum(1 for s in sizes if med > 0 and s >= k * med)
    dists = [abs(b.price - mid) / mid * 1e4 for b in bins]
    return {"n": len(sizes), "median": med, "max": mx, "prom": prom,
            "n_ge2": n_ge(2), "n_ge3": n_ge(3), "n_ge5": n_ge(5), "max_dist_bps": max(dists)}


async def main() -> None:
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 120.0
    window_bps = float(sys.argv[2]) if len(sys.argv) > 2 else 300.0
    bin_bps = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0
    cfg = RuntimeConfig()
    connectors = [build_connector(v, cfg.symbols[v]) for v in cfg.symbols]
    agg = Aggregator(list(VENUE_SPECS), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=cfg.staleness_s)
    engine = Engine(connectors, agg)
    # Detector with the wider det window so proximity isn't the limiter while we probe.
    detector = Detector(size_multiple=cfg.size_multiple, min_size=cfg.min_size, max_gap_bps=cfg.max_gap_bps,
                        max_zone_width_bps=cfg.max_zone_width_bps, match_overlap_bps=cfg.match_overlap_bps,
                        grace_s=cfg.grace_s, window_bps=window_bps, persistence_target_s=cfg.persistence_target_s,
                        venues_target=cfg.venues_target, strength_target=cfg.strength_target)
    await engine.start()
    out = open("calibration_probe.jsonl", "w", encoding="utf-8")
    deadline = time.time() + seconds
    n_snap = 0
    proms_bid, proms_ask = [], []
    zone_confs, zone_persist, zone_venues, zone_strength = [], [], [], []
    any_zone_snaps = 0
    max_conf_seen = 0.0
    venues_seen_max = 0
    try:
        while time.time() < deadline:
            try:
                snap = await asyncio.wait_for(engine.snapshots.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            n_snap += 1
            a = detector.update(snap)
            bs = _side_stats(snap.bids, snap.mid)
            as_ = _side_stats(snap.asks, snap.mid)
            proms_bid.append(bs["prom"]); proms_ask.append(as_["prom"])
            venues_seen_max = max(venues_seen_max, len(snap.venues_active))
            zones = list(a.supports) + list(a.resistances)
            if zones:
                any_zone_snaps += 1
                for z in zones:
                    zone_confs.append(z.confidence); zone_persist.append(z.persistence_s)
                    zone_venues.append(len(z.venues)); zone_strength.append(z.strength)
                    max_conf_seen = max(max_conf_seen, z.confidence)
            out.write(json.dumps({"ts": snap.ts, "mid": snap.mid,
                                  "venues_active": len(snap.venues_active),
                                  "bid": bs, "ask": as_,
                                  "n_sup": len(a.supports), "n_res": len(a.resistances),
                                  "top_conf": max((z.confidence for z in zones), default=0.0)}) + "\n")
    finally:
        await engine.stop()
        out.close()

    def pct(xs, p):
        if not xs:
            return 0.0
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))]

    print("=== CALIBRATION PROBE RESULTS ===")
    print(f"window_bps={window_bps} bin_bps={bin_bps} seconds={seconds}")
    print(f"snapshots={n_snap}  venues_active_max={venues_seen_max}")
    print(f"bid prominence (max/median bin): p50={pct(proms_bid,50):.2f} p90={pct(proms_bid,90):.2f} max={max(proms_bid or [0]):.2f}")
    print(f"ask prominence (max/median bin): p50={pct(proms_ask,50):.2f} p90={pct(proms_ask,90):.2f} max={max(proms_ask or [0]):.2f}")
    print(f"snapshots with >=1 detected zone: {any_zone_snaps}/{n_snap}")
    print(f"zones detected total={len(zone_confs)}  max_confidence_seen={max_conf_seen:.3f}")
    if zone_confs:
        print(f"zone confidence: p50={pct(zone_confs,50):.3f} p90={pct(zone_confs,90):.3f}")
        print(f"zone persistence_s: p50={pct(zone_persist,50):.1f} p90={pct(zone_persist,90):.1f} max={max(zone_persist):.1f}")
        print(f"zone venues: p50={pct(zone_venues,50)} max={max(zone_venues)}")
        print(f"zone strength: p50={pct(zone_strength,50):.3f} p90={pct(zone_strength,90):.3f}")


if __name__ == "__main__":
    asyncio.run(main())
