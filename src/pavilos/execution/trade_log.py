# src/pavilos/execution/trade_log.py
"""Durable JSONL trade log (one closed Trade per line) + summary stats. File I/O
is isolated here so the PaperBroker stays pure/unit-testable."""
from __future__ import annotations

import dataclasses
import json
import math
from pathlib import Path

from pavilos.execution.broker import Trade


def _reject_non_finite(_token: str):
    # json.loads accepts NaN/Infinity/-Infinity by default; reject them so a
    # poisoned line can't produce a non-finite Trade (which would later make the
    # summary NaN -> FastAPI JSONResponse HTTP 500 -> dashboard "feed lost").
    raise ValueError("non-finite numeric literal")


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
                out.append(Trade(**json.loads(line, parse_constant=_reject_non_finite)))
            except Exception:
                continue  # skip corrupt / partially-written / non-finite lines
        return out


def summarize(trades, *, base_equity: float) -> dict:
    # Consider only finite-pnl trades: a stray non-finite pnl would otherwise make
    # realized_pnl/return_pct NaN (-> JSON 500) and silently mis-count win_rate
    # (NaN>0 and NaN<0 are both False, so wins+losses != n_trades).
    finite = [t for t in trades if math.isfinite(t.pnl)]
    n = len(finite)
    realized = sum(t.pnl for t in finite)
    wins = sum(1 for t in finite if t.pnl > 0)
    losses = sum(1 for t in finite if t.pnl < 0)
    return {
        "n_trades": n,
        "realized_pnl": realized,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / n * 100.0) if n else 0.0,
        "return_pct": (realized / base_equity * 100.0) if base_equity else 0.0,
    }
