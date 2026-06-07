# src/pavilos/execution/trade_log.py
"""Durable JSONL trade log (one closed Trade per line) + summary stats. File I/O
is isolated here so the PaperBroker stays pure/unit-testable."""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path

from pavilos.execution.broker import Trade


class TradeLog:
    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def append(self, trade: Trade) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(trade)) + "\n")

    def load(self) -> list[Trade]:
        if not self._path.exists():
            return []
        out: list[Trade] = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(Trade(**json.loads(line)))
            except Exception:
                continue  # skip corrupt / partially-written lines
        return out


def summarize(trades, *, base_equity: float) -> dict:
    n = len(trades)
    realized = sum(t.pnl for t in trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = sum(1 for t in trades if t.pnl < 0)
    return {
        "n_trades": n,
        "realized_pnl": realized,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / n * 100.0) if n else 0.0,
        "return_pct": (realized / base_equity * 100.0) if base_equity else 0.0,
        "gross_win": sum(t.pnl for t in trades if t.pnl > 0),
        "gross_loss": sum(t.pnl for t in trades if t.pnl < 0),
    }
