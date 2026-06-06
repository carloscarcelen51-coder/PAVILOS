# scripts/replay.py
"""Deterministic replay harness: feed a JSONL BookUpdate stream through the
Aggregator and print/return the resulting combined snapshot. No network."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from pavilos.core.models import CombinedDepthSnapshot


def load_updates(path: Path) -> list[BookUpdate]:
    updates: list[BookUpdate] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        updates.append(
            BookUpdate(
                exchange=d["exchange"],
                ts=float(d["ts"]),
                bids=tuple((float(p), float(s)) for p, s in d["bids"]),
                asks=tuple((float(p), float(s)) for p, s in d["asks"]),
                is_snapshot=bool(d["is_snapshot"]),
                seq=d.get("seq"),
            )
        )
    return updates


def replay(agg: Aggregator, updates: list[BookUpdate], *, now: float) -> CombinedDepthSnapshot | None:
    for u in updates:
        agg.apply(u)
    return agg.snapshot(now=now)


def _default_aggregator() -> Aggregator:
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A)]
    return Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: python -m scripts.replay <updates.jsonl>", file=sys.stderr)
        return 2
    updates = load_updates(Path(argv[1]))
    # Snapshot relative to the stream's own clock (the freshest update's ts), not
    # wall-clock infinity, so the Aggregator's staleness gate sees the venues as
    # fresh. A hardcoded far-future `now` would make every venue stale -> no snapshot.
    now = max((u.ts for u in updates), default=0.0)
    snap = replay(_default_aggregator(), updates, now=now)
    if snap is None:
        print("no snapshot (no active Tier-A venue)")
        return 0
    print(f"mid={snap.mid:.2f}  venues={snap.venues_active}/{snap.venues_total}")
    print("  bids (best first):")
    for b in snap.bids[:10]:
        print(f"    {b.price:.2f}  size={b.size:.4f}  {b.composition}")
    print("  asks (best first):")
    for a in snap.asks[:10]:
        print(f"    {a.price:.2f}  size={a.size:.4f}  {a.composition}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
