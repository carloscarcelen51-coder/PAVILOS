# src/pavilos/backtest/static_study.py
"""Approach-episode forward-return validation study (M14 Milestone A-static).

Replays a combined-snapshot stream through the causal ``StaticLevelTracker``,
samples ONE forward observation per price-**approach episode** to an active
static support (mid returns to within ``entry_zone_bps`` of a FIXED-price
multi-venue level that price had been AWAY from), measures the forward path in
R-multiples (MFE/MAE vs the stop risk) over a horizon, and aggregates bounce-rate
+ expectancy by level-strength bucket against a baseline. A ``realized_vol_bps``
helper reports the slice volatility so a flat slice (few/zero approaches) is
distinguished from a genuine negative verdict.

This fixes the M13 finding: the detected "supports" there were the near-touch bid
trailing a few bps below a drifting mid. Here a tradeable approach can only open
on a level whose ``max_away_bps >= min_away_bps`` (price genuinely left it), so
the near-touch — which never gets far from mid — never opens an episode.

Causality
---------
The level STRENGTH at an episode onset T uses ONLY data <= T (the tracker is fed
snapshots in order; ``active_supports`` at T sees only accrual up to T). The
forward MFE/MAE uses snapshots strictly AFTER T (data > T): that is the OUTCOME
being measured, not a trading look-ahead.

Independent episodes
--------------------
A level is keyed by its FIXED bucket price. An episode for a level OPENS when mid
first enters ``entry_zone_bps`` of that active static support after being outside
it; we emit exactly ONE observation at onset. The episode stays open while mid
keeps returning to the zone; it CLOSES once mid has been OUTSIDE the zone for
longer than ``episode_gap_s`` (hysteresis) — so a brief pop just outside the zone
does NOT double-count, while leaving for > the gap and re-approaching yields TWO
observations.

R-multiples (reused from M13 ``confluence_study``)
--------------------------------------------------
R = entry - stop (the stop risk). MFE = max(mid - entry) over the forward window,
MAE = min(mid - entry). ``mfe_r = MFE / R``, ``mae_r = MAE / R``. ``bounced`` is
True iff ``mfe_r`` reaches ``target_r`` BEFORE ``mae_r`` reaches -1 (target before
stop), scanning forward snapshots in time order. ``decided`` is True iff the
window resolved one of those thresholds; an undecided/horizon-clipped window is
EXCLUDED from expectancy rather than charged as a -1R stop-out.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from pavilos.detection.static_levels import (
    StaticLevel,
    StaticLevelConfig,
    StaticLevelTracker,
)
from pavilos.signals.atr import ATR


@dataclass(slots=True, frozen=True)
class StaticStudyConfig:
    """Tuning for :func:`study_static_approaches`.

    ``static`` — the tracker config. ``horizon_s`` — forward measurement window
    (recorded-time seconds). ``target_r`` — R-multiple that counts as a "bounce"
    success. ``stop_offset_bps`` — stop placed this far below the static level.
    ``atr_stop_mult`` — the stop is never tighter than this x ATR from entry
    (anti-whipsaw floor; 0 disables the floor so the level-offset stop wins).
    ``entry_zone_bps`` — an approach is only sampled when mid is within this of
    the level (price has returned). ``episode_gap_s`` — mid outside the zone for
    longer than this closes the episode (hysteresis / dedup boundary).
    ``buckets`` — level-strength bucket edges for :func:`summarize_static`.
    ``atr_window`` — ATR proxy window over mids."""

    static: StaticLevelConfig
    horizon_s: float
    target_r: float
    stop_offset_bps: float
    atr_stop_mult: float
    entry_zone_bps: float
    episode_gap_s: float
    buckets: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0)
    atr_window: int = 14


@dataclass(slots=True, frozen=True)
class Obs:
    """One forward observation, sampled once at an approach episode's onset.

    ``level_strength`` is the causal setup score (data <= onset); ``n_venues`` is
    the level's venue breadth; ``max_away_bps`` is how far price had been from the
    level over its life (the near-touch discriminator). ``entry``/``stop``/``risk``
    define R. ``mfe_r``/``mae_r`` are the forward MFE/MAE in R-multiples,
    ``fwd_return_bps`` the horizon return. ``decided`` is True iff the forward
    window resolved +target_r or -1R; ``bounced`` is True iff it resolved to the
    target first. ``outcome_r`` is the per-obs R outcome decided AT SAMPLE TIME
    (+target_r if bounced, -1.0 if stopped, ``None`` if undecided) — carried here
    so expectancy can never desync from the bounce decision. ``horizon_snaps`` is
    how many forward snapshots were measured (loud small-window flag;
    horizon_snaps==0 => no forward data => undecided)."""

    ts: float
    level_price: float
    level_strength: float
    n_venues: int
    max_away_bps: float
    entry: float
    stop: float
    risk: float
    mfe_r: float
    mae_r: float
    fwd_return_bps: float
    bounced: bool
    decided: bool
    outcome_r: float | None
    horizon_snaps: int


def realized_vol_bps(snapshots) -> float:
    """Realized volatility of the slice: mean absolute tick-to-tick mid return in
    bps. Reports the slice's price MOVEMENT so a flat slice (few/zero approaches,
    the M13 lesson) is distinguished from a real negative verdict. Empty / single
    snapshot -> 0.0. Non-positive mids are skipped."""
    snaps = list(snapshots)
    diffs: list[float] = []
    prev: float | None = None
    for s in snaps:
        m = s.mid
        if m <= 0.0:
            prev = None
            continue
        if prev is not None and prev > 0.0:
            diffs.append(abs(m - prev) / prev * 1e4)
        prev = m
    return mean(diffs) if diffs else 0.0


def _best_near_support(supports: list[StaticLevel], mid: float,
                       entry_zone_bps: float) -> StaticLevel | None:
    """Highest-strength active static support sitting within ``entry_zone_bps``
    BELOW ``mid`` (price has RETURNED to the level). Pure; uses only the supports
    the tracker reports at this snapshot."""
    best: StaticLevel | None = None
    for s in supports:
        if s.price >= mid:
            continue
        if (mid - s.price) / mid * 1e4 <= entry_zone_bps:
            if best is None or s.strength > best.strength:
                best = s
    return best


def _measure_forward(snapshots, i: int, entry: float, risk: float,
                     cfg: StaticStudyConfig) -> tuple[float, float, float, bool, bool, int]:
    """Scan snapshots strictly AFTER onset index ``i`` within ``horizon_s`` and
    return (mfe_r, mae_r, fwd_return_bps, bounced, decided, horizon_snaps).
    Mirrors M13's R-math: ``decided`` is True iff +target_r or -1R was resolved
    before the horizon expired; a window reaching neither (incl. an empty forward
    window) is ``decided=False, bounced=False`` and must NOT be scored as a
    stop-out. Forward-only: never touches snapshots at or before ``i``."""
    entry_ts = snapshots[i].ts
    mfe = 0.0   # best favourable excursion (mid - entry), >= 0
    mae = 0.0   # worst adverse excursion (mid - entry), <= 0
    last_mid = entry
    bounced = False
    decided = False
    n = 0
    for j in range(i + 1, len(snapshots)):
        s = snapshots[j]
        if s.ts - entry_ts > cfg.horizon_s:
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
            if delta >= cfg.target_r * risk:
                bounced, decided = True, True
            elif delta <= -risk:
                bounced, decided = False, True
    mfe_r = mfe / risk if risk > 0.0 else 0.0
    mae_r = mae / risk if risk > 0.0 else 0.0
    fwd_return_bps = ((last_mid - entry) / entry * 1e4) if entry else 0.0
    return mfe_r, mae_r, fwd_return_bps, bounced, decided, n


def study_static_approaches(snapshots, cfg: StaticStudyConfig) -> list[Obs]:
    """Replay ``snapshots`` through a causal ``StaticLevelTracker``, sampling ONE
    forward observation per price-approach episode to an active static support.

    The strength is causal (data <= onset); the forward path is the measured
    outcome (data > onset). An approach to a level (keyed by its fixed bucket
    price) opens an episode the first time mid enters ``entry_zone_bps`` of it
    after being outside; the episode stays open while mid keeps returning, and
    closes once mid has been outside the zone for longer than ``episode_gap_s``
    (hysteresis) so a brief pop does not double-count and a genuine re-approach
    after the gap is a new episode."""
    snaps = list(snapshots)
    if not snaps:
        return []
    tracker = StaticLevelTracker(cfg.static)
    atr = ATR(window=cfg.atr_window)

    out: list[Obs] = []
    open_level: float | None = None       # bucket price of the currently-open episode
    last_in_zone_ts: float | None = None  # last ts mid was within the zone of open_level

    for i, snap in enumerate(snaps):
        tracker.update(snap)
        atr.update(snap.mid)
        supports = tracker.active_supports(snap.mid, snap.ts)
        cand = _best_near_support(supports, snap.mid, cfg.entry_zone_bps)

        # Close the open episode if mid has been OUTSIDE its zone beyond the gap.
        if open_level is not None and last_in_zone_ts is not None:
            still_in_zone = (
                cand is not None and cand.price == open_level
            )
            if still_in_zone:
                last_in_zone_ts = snap.ts
            elif snap.ts - last_in_zone_ts > cfg.episode_gap_s:
                open_level = None

        if cand is None:
            continue

        # An open episode for THIS level persists: dedup (no new observation),
        # and refresh the in-zone timestamp so hysteresis spans the whole episode.
        if open_level is not None and cand.price == open_level:
            last_in_zone_ts = snap.ts
            continue

        # New approach episode onset: emit exactly ONE observation, measured
        # strictly forward. Entry = mid; stop = ATR-floored below the level.
        entry = snap.mid
        stop = min(cand.price * (1 - cfg.stop_offset_bps / 1e4),
                   entry - atr.value() * cfg.atr_stop_mult)
        risk = entry - stop
        if risk <= 0.0:
            # Degenerate stop (>= entry): cannot define R. Open the episode so we
            # do not re-sample the same persisting approach, but record no obs.
            open_level, last_in_zone_ts = cand.price, snap.ts
            continue
        mfe_r, mae_r, fwd_bps, bounced, decided, n = _measure_forward(snaps, i, entry, risk, cfg)
        outcome_r = (cfg.target_r if bounced else -1.0) if decided else None
        out.append(Obs(
            ts=snap.ts, level_price=cand.price, level_strength=cand.strength,
            n_venues=cand.n_venues, max_away_bps=cand.max_away_bps,
            entry=entry, stop=stop, risk=risk, mfe_r=mfe_r, mae_r=mae_r,
            fwd_return_bps=fwd_bps, bounced=bounced, decided=decided,
            outcome_r=outcome_r, horizon_snaps=n))
        open_level, last_in_zone_ts = cand.price, snap.ts

    return out


def _bucket_label(lo: float, hi: float) -> str:
    return f"[{lo:.2f},{hi:.2f})"


def _expectancy(members: list[Obs]) -> float:
    """Mean per-obs R outcome over DECIDED observations only (the honest,
    scale-free verdict). ``outcome_r`` was decided at sample time so it can never
    desync from ``bounced``. Undecided/horizon-clipped obs are excluded — never
    charged as a -1R stop-out. No decided obs -> finite zero (not a loss)."""
    decided = [o.outcome_r for o in members if o.outcome_r is not None]
    return mean(decided) if decided else 0.0


def _summary_row(bucket: str, members: list[Obs]) -> dict:
    """Aggregate one strength bucket. ``expectancy_r`` is the mean DECIDED per-obs
    R outcome (+target_r if bounced, -1 if stopped); undecided/horizon-clipped
    episodes are EXCLUDED and counted in ``n_undecided`` so the verdict is not
    silently diluted. ``bounce_rate`` is over decided obs. Empty buckets report
    zeros (finite)."""
    n = len(members)
    n_decided = sum(1 for o in members if o.decided)
    n_undecided = n - n_decided
    if n == 0:
        return {"bucket": bucket, "n": 0, "n_decided": 0, "n_undecided": 0,
                "bounce_rate": 0.0, "mean_fwd_return_bps": 0.0, "mean_mfe_r": 0.0,
                "mean_mae_r": 0.0, "expectancy_r": 0.0}
    bounce_rate = (sum(1 for o in members if o.bounced) / n_decided) if n_decided else 0.0
    return {
        "bucket": bucket,
        "n": n,
        "n_decided": n_decided,
        "n_undecided": n_undecided,
        "bounce_rate": bounce_rate,
        "mean_fwd_return_bps": mean(o.fwd_return_bps for o in members),
        "mean_mfe_r": mean(o.mfe_r for o in members),
        "mean_mae_r": mean(o.mae_r for o in members),
        "expectancy_r": _expectancy(members),
    }


def summarize_static(observations, buckets: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0)) -> list[dict]:
    """Bucket observations by LEVEL STRENGTH and aggregate bounce-rate +
    expectancy_r per bucket, then append an ALL/baseline row over every
    observation. Buckets partition ``[edges[0], edges[-1]]``; the final bucket is
    closed on the right so a perfect strength of 1.0 lands in the top bucket. N is
    reported per bucket loudly — a great rate on tiny N is noise.

    ``expectancy_r`` is the mean R outcome over DECIDED episodes only (each obs
    carries the outcome decided at sample time); undecided / horizon-clipped
    episodes are EXCLUDED (never a -1R loss) and surfaced as ``n_undecided``."""
    obs = list(observations)
    edges = sorted(set(buckets))
    rows: list[dict] = []
    if len(edges) >= 2:
        n_buckets = len(edges) - 1
        for b in range(n_buckets):
            lo, hi = edges[b], edges[b + 1]
            top = b == n_buckets - 1   # last bucket includes its right edge
            members = [
                o for o in obs
                if o.level_strength >= lo and (o.level_strength <= hi if top
                                               else o.level_strength < hi)
            ]
            rows.append(_summary_row(_bucket_label(lo, hi), members))
    rows.append(_summary_row("ALL", obs))
    return rows
