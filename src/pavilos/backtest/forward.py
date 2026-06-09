# src/pavilos/backtest/forward.py
"""Shared forward-measurement + decided-only aggregation helpers for the
forward-return validation studies (M13 ``confluence_study`` + M14
``static_study``).

These are the single source of truth for the R-multiple path math and the
honest decided-only aggregation, so a future correction (a tie-break, a
horizon-boundary fix, the decided-only expectancy convention) is applied ONCE
and the two studies can never silently diverge. The signatures are config-
agnostic — callers pass ``horizon_s``/``target_r`` (both studies expose them
identically) and an iterable of objects exposing ``.outcome_r`` — so neither
study's config or ``Obs`` dataclass is imported here.

R-multiples
-----------
R = entry - stop (the stop risk). MFE = max(mid - entry) over the forward
window, MAE = min(mid - entry). ``mfe_r = MFE / R``, ``mae_r = MAE / R``.
``bounced`` is True iff ``mfe_r`` reaches ``target_r`` BEFORE ``mae_r`` reaches
-1 (target before stop), scanning forward snapshots in time order. ``decided``
is True iff the window resolved one of those thresholds; an undecided /
horizon-clipped window (incl. an empty forward window) is ``decided=False,
bounced=False`` and is EXCLUDED from expectancy rather than charged as a -1R
stop-out.
"""
from __future__ import annotations

from statistics import mean
from typing import Iterable, Protocol


class _HasOutcomeR(Protocol):
    outcome_r: float | None


def measure_forward(snapshots, i: int, *, entry: float, risk: float,
                    horizon_s: float, target_r: float
                    ) -> tuple[float, float, float, bool, bool, int]:
    """Scan ``snapshots`` strictly AFTER onset index ``i`` within ``horizon_s``
    and return ``(mfe_r, mae_r, fwd_return_bps, bounced, decided, horizon_snaps)``.

    ``decided`` is True iff +``target_r`` or -1R was resolved before the horizon
    expired; a window reaching neither (incl. an empty forward window) is
    ``decided=False, bounced=False`` and must NOT be scored as a stop-out.
    Forward-only: never touches snapshots at or before ``i`` (the onset)."""
    entry_ts = snapshots[i].ts
    mfe = 0.0   # best favourable excursion (mid - entry), >= 0
    mae = 0.0   # worst adverse excursion (mid - entry), <= 0
    last_mid = entry
    bounced = False
    decided = False
    n = 0
    for j in range(i + 1, len(snapshots)):
        s = snapshots[j]
        if s.ts - entry_ts > horizon_s:
            break
        n += 1
        delta = s.mid - entry
        if delta > mfe:
            mfe = delta
        if delta < mae:
            mae = delta
        last_mid = s.mid
        # Decide bounce in TIME order: whichever threshold is reached FIRST wins.
        if not decided and risk > 0.0:
            if delta >= target_r * risk:
                bounced, decided = True, True
            elif delta <= -risk:
                bounced, decided = False, True
    mfe_r = mfe / risk if risk > 0.0 else 0.0
    mae_r = mae / risk if risk > 0.0 else 0.0
    fwd_return_bps = ((last_mid - entry) / entry * 1e4) if entry else 0.0
    return mfe_r, mae_r, fwd_return_bps, bounced, decided, n


def bucket_label(lo: float, hi: float) -> str:
    return f"[{lo:.2f},{hi:.2f})"


def expectancy(members: Iterable[_HasOutcomeR]) -> float:
    """Mean per-obs R outcome over DECIDED observations only (the honest,
    scale-free verdict). ``outcome_r`` was decided at sample time so it can never
    desync from ``bounced``. Undecided/horizon-clipped obs are excluded — never
    charged as a -1R stop-out. No decided obs -> finite zero (not a loss)."""
    decided = [o.outcome_r for o in members if o.outcome_r is not None]
    return mean(decided) if decided else 0.0
