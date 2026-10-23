# src/pavilos/web/history.py
"""Facts about the paper-trading history that the trade log itself does not carry:
when each process started (every start reset the paper account to 10,000 until
2026-10-23) and from when the live setup was the final one."""
from __future__ import annotations

import json
import math
from pathlib import Path

# Process starts before the recorder supervisor existed (it logs every start since
# 2026-06-30). Deduced from the trade log: a start resets the paper account to 10,000, so a
# trade whose notional exceeds 10x the account rebuilt since the previous start (the
# leverage cap) proves a restart before it. scripts/audit_trades.py reproduces them.
PRE_SUPERVISOR_RUN_STARTS = (
    1780899518.493,    # 2026-06-08 06:18:38Z  first trade of the log
    1781011259.48,     # 2026-06-09 13:20:59Z
    1781283429.039,    # 2026-06-12 16:57:09Z
)

# 2026-06-09 13:20:59Z: first start with the final live setup. The strategy logic was final
# from 2026-06-08 06:09Z (the log starts at 06:18Z), but until this restart venues were being
# added (6 -> 12 -> 14) and the snapshot rate changed (10 -> 5 Hz): 758 trades.
STABLE_SETUP_SINCE = 1781011259.48


def run_starts(runs_file: str | None) -> list[dict]:
    """Every known process start, oldest first: the deduced ones above plus the lake's
    ``_runs.jsonl`` (one line per recorder start; earlier lines were rebuilt from the
    supervisor log). Unreadable lines are skipped."""
    starts = {ts: "deducido" for ts in PRE_SUPERVISOR_RUN_STARTS}
    if runs_file and Path(runs_file).exists():
        for line in Path(runs_file).read_text(encoding="utf-8").splitlines():
            try:
                ts = float(json.loads(line)["start_ts"])
            except Exception:
                continue
            if math.isfinite(ts):
                starts.setdefault(ts, "registro")
    return [{"start": ts, "source": src} for ts, src in sorted(starts.items())]
