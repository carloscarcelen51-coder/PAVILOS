# src/pavilos/persistence/retention.py
"""Delete (or move to cold storage) date-partitions older than a retention window.
Raw L2 is tens of GB/day, so this MUST run or the disk fills."""
from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone

_log = logging.getLogger(__name__)


def prune_old_partitions(base_dir: str, retention_days: int, *, now_date: str | None = None,
                         move_to: str | None = None) -> int:
    """Remove ``date=YYYY-MM-DD`` partitions older than ``retention_days`` under every
    ``exchange=*`` dir. If ``move_to`` is set, move instead of delete. ``now_date``
    (YYYY-MM-DD) is injectable for tests. Returns the number of partitions handled.

    Robust to junk: a single malformed/foreign child (a stray non-date dir, a
    ``_SUCCESS`` marker, a file, or an unparseable ``date=`` name left by an
    interrupted shutil.move) is skipped (and warned) so it never aborts the whole
    prune pass — otherwise one bad entry would silently let the disk fill."""
    if not os.path.isdir(base_dir):
        return 0
    cutoff = _epoch_day(now_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")) - retention_days
    handled = 0
    for ex_dir in os.listdir(base_dir):
        if not ex_dir.startswith("exchange="):
            continue
        ex_path = os.path.join(base_dir, ex_dir)
        for date_dir in os.listdir(ex_path):
            if not date_dir.startswith("date="):
                continue
            src = os.path.join(ex_path, date_dir)
            if not os.path.isdir(src):
                _log.warning("retention: skipping non-directory partition entry %s", src)
                continue
            try:
                day = _epoch_day(date_dir[len("date="):])
            except ValueError:
                _log.warning("retention: skipping malformed date partition %s", src)
                continue
            if day < cutoff:
                if move_to:
                    dst = os.path.join(move_to, ex_dir, date_dir)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.move(src, dst)
                else:
                    shutil.rmtree(src, ignore_errors=True)
                handled += 1
    return handled


def _epoch_day(date_str: str) -> int:
    # Calendar-based UTC day index so the boundary matches the sink's time.gmtime
    # (UTC) partitioning exactly and is immune to local-time DST quirks.
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() // 86400)
