# src/pavilos/persistence/lake_files.py
"""Finding and vetting the lake's Parquet files (``exchange=X/date=YYYY-MM-DD/HH/*``).

Rows are partitioned by their OWN ts, so the hour dirs that overlap a time span hold
every row of that span -- readers prune to them instead of globbing the whole lake.
A file that is not a complete Parquet file (a writer killed mid-write before writes
became atomic, a damaged footer) must never break a reader: ``check_files`` tells
the readable ones from the rest."""
from __future__ import annotations

import calendar
import os
import time

import pyarrow.parquet as pq


def hour_dirs(base_dir: str, t0: float, t1: float, exclude=()) -> list[str]:
    """Existing ``exchange=*/date=*/HH`` dirs whose hour overlaps [t0, t1] and that hold
    at least one ``*.parquet``. Walks only the partitions that exist."""
    out: list[str] = []
    if not os.path.isdir(base_dir):
        return out
    for ex in sorted(os.listdir(base_dir)):
        if not ex.startswith("exchange=") or ex[len("exchange="):] in exclude:
            continue
        ex_dir = os.path.join(base_dir, ex)
        if not os.path.isdir(ex_dir):
            continue
        for dd in sorted(os.listdir(ex_dir)):
            day = _day_start(dd)
            if day is None or day + 86400 <= t0 or day > t1:
                continue
            d_dir = os.path.join(ex_dir, dd)
            if not os.path.isdir(d_dir):
                continue
            for hh in sorted(os.listdir(d_dir)):
                if not _is_hour(hh):
                    continue
                start = day + int(hh) * 3600
                if start + 3600 <= t0 or start > t1:
                    continue
                part = os.path.join(d_dir, hh)
                if os.path.isdir(part) and has_parquet(part):
                    out.append(part)
    return out


def newest_hour_dirs(base_dir: str) -> list[str]:
    """Per exchange, the most recent hour dir holding a ``*.parquet``."""
    out: list[str] = []
    if not os.path.isdir(base_dir):
        return out
    for ex in sorted(os.listdir(base_dir)):
        ex_dir = os.path.join(base_dir, ex)
        if not ex.startswith("exchange=") or not os.path.isdir(ex_dir):
            continue
        found = None
        for dd in sorted((d for d in os.listdir(ex_dir) if _day_start(d) is not None), reverse=True):
            d_dir = os.path.join(ex_dir, dd)
            if not os.path.isdir(d_dir):
                continue
            for hh in sorted((h for h in os.listdir(d_dir) if _is_hour(h)), reverse=True):
                part = os.path.join(d_dir, hh)
                if os.path.isdir(part) and has_parquet(part):
                    found = part
                    break
            if found:
                break
        if found:
            out.append(found)
    return out


def has_parquet(d: str) -> bool:
    with os.scandir(d) as it:
        return any(e.name.endswith(".parquet") and e.is_file() for e in it)


def parquet_files(d: str) -> list[str]:
    return sorted(os.path.join(d, n) for n in os.listdir(d)
                  if n.endswith(".parquet") and os.path.isfile(os.path.join(d, n)))


def check_files(paths: list[str]) -> tuple[list[str], list[str]]:
    """(readable, unreadable): a readable file starts and ends with b"PAR1" and its
    footer parses."""
    good: list[str] = []
    bad: list[str] = []
    for p in paths:
        (good if _readable(p) else bad).append(p)
    return good, bad


def _readable(p: str) -> bool:
    try:
        with open(p, "rb") as fh:
            if fh.read(4) != b"PAR1":
                return False
            fh.seek(0, os.SEEK_END)
            if fh.tell() < 12:
                return False
            fh.seek(-4, os.SEEK_END)
            if fh.read(4) != b"PAR1":
                return False
        pq.read_metadata(p)
        return True
    except Exception:
        return False


def _day_start(dd: str) -> float | None:
    if not dd.startswith("date="):
        return None
    try:
        return float(calendar.timegm(time.strptime(dd[len("date="):], "%Y-%m-%d")))
    except ValueError:
        return None


def _is_hour(hh: str) -> bool:
    return len(hh) == 2 and hh.isascii() and hh.isdigit() and int(hh) < 24
