# src/pavilos/persistence/retention.py
"""Retention of the raw-L2 lake.

Default policy (M17): retention is ETERNAL -- nothing is ever deleted automatically.
The drive is shared, so a disk FLOOR protects it instead: while the drive has less
than ``min_free_gb`` free the recorder is PAUSED (loudly logged) and it resumes by
itself once space is back. ``RetentionJanitor`` checks this at startup and every
``every_s`` (not only at startup: on 2026-09-28 the lake filled D: after 14 days
without a restart). Deleting by age (``prune_old_partitions``) is opt-in only."""
from __future__ import annotations

import logging
import os
import shutil
import threading
from collections.abc import Callable
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


_GB = 1024 ** 3     # "GB" as Windows Explorer and df -h report them (GiB)


def free_gb(path: str, *, disk_usage=shutil.disk_usage) -> float:
    """Free space (GiB) of the drive holding ``path`` -- measured on its nearest existing
    ancestor, so a lake directory that does not exist yet is still measurable."""
    p = os.path.abspath(path)
    while not os.path.exists(p):
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return disk_usage(p).free / _GB


class RetentionJanitor:
    """Applies the retention policy: ``run_once()`` (call it at startup, before the
    recorder starts) and then every ``every_s`` in a daemon thread after ``start()``.

    Each pass: the optional age limit (``retention_days``; None = never delete), then
    the disk floor -- ``on_floor(True|False)`` reports whether the drive has at least
    ``min_free_gb`` free (None/0 = no floor). It is not called when the free space
    cannot be measured, so a transient stat error never flips the recorder."""

    def __init__(self, base_dir: str, *, retention_days: int | None, min_free_gb: float | None,
                 every_s: float = 600.0, on_floor: Callable[[bool], None] | None = None,
                 disk_usage=shutil.disk_usage) -> None:
        self._base = base_dir
        self._retention_days = retention_days
        self._min_free_gb = min_free_gb
        self._every_s = every_s
        self._on_floor = on_floor
        self._disk_usage = disk_usage
        self._floor_ok: bool | None = None
        self._passes = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> dict:
        """One pass; never raises. Returns {"aged_out", "free_gb", "floor_ok"}."""
        res: dict = {"aged_out": 0, "free_gb": None, "floor_ok": None}
        self._passes += 1
        if self._retention_days is not None:
            try:
                res["aged_out"] = prune_old_partitions(self._base, self._retention_days)
            except Exception:
                _log.exception("retention: age prune failed (%s, %d days)", self._base, self._retention_days)
            if res["aged_out"]:
                _log.warning("retention: deleted %d date partitions older than %d days",
                             res["aged_out"], self._retention_days)
        if not self._min_free_gb:
            return res
        try:
            free = free_gb(self._base, disk_usage=self._disk_usage)
        except Exception as e:
            _log.warning("retention: cannot measure free space for %s (%s); floor state unchanged",
                         self._base, e)
            return res
        ok = free >= self._min_free_gb
        res["free_gb"], res["floor_ok"] = free, ok
        if not ok:
            _log.error("retention: only %.1f GB free on the lake's drive (floor %.0f GB) -- "
                       "recording PAUSED, nothing is deleted; free space to resume", free, self._min_free_gb)
        elif self._floor_ok is False:
            _log.warning("retention: %.1f GB free again (floor %.0f GB) -- recording resumes",
                         free, self._min_free_gb)
        elif self._passes == 1:
            _log.info("retention: %.1f GB free on the lake's drive (floor %.0f GB); age limit: %s",
                      free, self._min_free_gb,
                      "none (eternal)" if self._retention_days is None else f"{self._retention_days} days")
        self._floor_ok = ok
        if self._on_floor is not None:
            try:
                self._on_floor(ok)
            except Exception:
                _log.exception("retention: on_floor callback failed")
        return res

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="lake-janitor", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self) -> None:
        while not self._stop.wait(self._every_s):
            try:
                self.run_once()
            except Exception:                         # run_once guards itself; belt and braces
                _log.exception("retention: janitor pass failed")
