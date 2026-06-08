# src/pavilos/backtest/io.py
"""Serialize CombinedDepthSnapshot <-> dict / JSONL line for recording + replay."""
from __future__ import annotations

import json

from pavilos.core.models import DepthBin, CombinedDepthSnapshot


def snapshot_to_dict(s: CombinedDepthSnapshot) -> dict:
    return {
        "ts": s.ts, "mid": s.mid,
        "bids": [[b.price, b.size, b.composition] for b in s.bids],
        "asks": [[b.price, b.size, b.composition] for b in s.asks],
        "venues_active": list(s.venues_active), "venues_total": s.venues_total,
    }


def snapshot_from_dict(d: dict) -> CombinedDepthSnapshot:
    return CombinedDepthSnapshot(
        ts=d["ts"], mid=d["mid"],
        bids=tuple(DepthBin(price=p, size=sz, composition=dict(c)) for p, sz, c in d["bids"]),
        asks=tuple(DepthBin(price=p, size=sz, composition=dict(c)) for p, sz, c in d["asks"]),
        venues_active=tuple(d["venues_active"]), venues_total=d["venues_total"],
    )


def dumps_snapshot(s: CombinedDepthSnapshot) -> str:
    return json.dumps(snapshot_to_dict(s))


def loads_snapshot(line: str) -> CombinedDepthSnapshot:
    return snapshot_from_dict(json.loads(line))


def load_snapshots(path: str) -> list[CombinedDepthSnapshot]:
    out: list[CombinedDepthSnapshot] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(loads_snapshot(line))
                except Exception:
                    continue  # skip a corrupt/partial line
    return out
