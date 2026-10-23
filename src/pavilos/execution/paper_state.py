# src/pavilos/execution/paper_state.py
"""The paper account across restarts: realized cash, the open position (with its current
stop) and the measurement period, saved as one JSON file (``paper_state.json`` next to the
trade log) after every entry and exit, after stop moves (at most once a second) and every
``heartbeat`` seconds.

Until 2026-10-23 each process start began again at ``starting_equity`` with no position:
a position open when the PC shut down vanished without a record, and the all-time P&L
summed one fresh account per restart. The file is written to a temporary name and then
renamed over the old one, so a crash mid-write never leaves a half-written state; a file
that cannot be read is kept aside (renamed, never deleted) and the account starts fresh.

``period`` = {"since": epoch s, "equity0": cash at that moment, "history": [earlier periods]}:
the dashboards count trades from ``since`` by default. Starting a new period resets the
cash to the starting equity and deletes nothing. ``last_alive`` (saved on every write)
tells the next start how long the program was down."""
from __future__ import annotations

import json
import logging
import math
import os
import time
from pathlib import Path

_log = logging.getLogger(__name__)

VERSION = 2


def _reject_non_finite(token: str):
    raise ValueError(f"non-finite number {token!r} in the paper state")


def _finite_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


class PaperState:
    def __init__(self, path: str, *, now=time.time) -> None:
        self._path = Path(path)
        self._now = now
        self.period: dict | None = None        # set by the runtime after load()

    @property
    def path(self) -> Path:
        return self._path

    def save(self, state: dict) -> None:
        """``state`` = PaperBroker.export_state(): {"equity": float, "position": dict | None}."""
        now = self._now()
        doc = {"version": VERSION, "saved_ts": now, "last_alive": now, "equity": state["equity"],
               "position": state.get("position"), "period": self.period}
        text = json.dumps(doc, allow_nan=False)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)

    def load(self) -> dict | None:
        """The saved state, or None if there is none or it cannot be used (then the file is
        renamed to ``*.unreadable-<epoch>`` and kept for inspection)."""
        if not self._path.exists():
            return None
        try:
            doc = json.loads(self._path.read_text(encoding="utf-8"), parse_constant=_reject_non_finite)
            equity = doc["equity"]
            if not _finite_number(equity):
                raise ValueError(f"bad equity {equity!r}")
            pos = doc.get("position")
            if pos is not None:
                missing = {"side", "size", "entry", "stop", "entry_ts"} - set(pos)
                if missing:
                    raise ValueError(f"position without {sorted(missing)}")
            period = doc.get("period")
            if period is not None and not (_finite_number(period.get("since"))
                                           and _finite_number(period.get("equity0"))):
                raise ValueError(f"bad period {period!r}")
            alive = doc.get("last_alive", doc.get("saved_ts"))
            return {"equity": float(equity), "position": pos, "period": period,
                    "saved_ts": doc.get("saved_ts"),
                    "last_alive": float(alive) if _finite_number(alive) else None}
        except Exception as e:
            aside = self._path.with_name(f"{self._path.name}.unreadable-{int(self._now())}")
            try:
                os.replace(self._path, aside)
            except OSError:
                aside = self._path
            _log.error("paper state %s unusable (%s); kept as %s, the account starts fresh",
                       self._path, e, aside)
            return None
