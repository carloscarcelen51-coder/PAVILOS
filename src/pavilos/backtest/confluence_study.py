# src/pavilos/backtest/confluence_study.py
"""Forward-return validation study (M13 Milestone A).

Replays a combined-snapshot stream through the REAL ``Detector`` +
``analyze_confluence``, samples ONE forward observation per support-cluster
*episode* (deduping the same persisting cluster so autocorrelated snapshots do
not inflate N), measures the forward path in R-multiples (MFE/MAE vs the stop
risk) over a horizon, and aggregates bounce-rate + expectancy by
confluence-score bucket against a baseline.

Causality
---------
The confluence SETUP score at the episode onset T uses ONLY that snapshot's
zones (data <= T) — a causal score. The forward path uses snapshots strictly
after T (data > T): that is the OUTCOME BEING MEASURED, not a trading
look-ahead. We test a signal's predictive power; we never trade on the future.

Independent samples
-------------------
Consecutive snapshots show the SAME persisting cluster (autocorrelated). We open
an episode at the first snapshot whose tradeable support cluster sits within
``entry_zone_bps`` of price, emit exactly ONE observation there, and keep it open
while a band-overlapping cluster keeps appearing. The episode CLOSES when no
matching cluster appears for longer than ``episode_gap_s`` (or the band shifts to
a non-overlapping cluster), so a cluster that lapses and reforms yields TWO
observations. N is reported per EPISODE.

R-multiples
-----------
R = entry - stop (the stop risk). MFE = max(mid - entry) over the forward
window, MAE = min(mid - entry). ``mfe_r = MFE / R``, ``mae_r = MAE / R``.
``bounced`` is True iff ``mfe_r`` reaches ``target_r`` BEFORE ``mae_r`` reaches
-1 (target hit before the stop), scanning the forward snapshots in time order.
``fwd_return_bps`` is the close-to-close return at the horizon, in bps.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.confluence import ConfluenceCluster, ConfluenceConfig, analyze_confluence
from pavilos.detection.models import Side
from pavilos.signals.atr import ATR
from pavilos.backtest.runner import _detector


@dataclass(slots=True, frozen=True)
class StudyConfig:
    """Tuning for :func:`study_observations`.

    ``confluence`` — the analyzer config. ``horizon_s`` — forward measurement
    window (recorded-time seconds). ``target_r`` — R-multiple that counts as a
    "bounce" success. ``stop_offset_bps`` — stop placed this far below the
    cluster low. ``atr_stop_mult`` — the stop is never tighter than this x ATR
    from entry (anti-whipsaw floor). ``entry_zone_bps`` — only sample a support
    cluster when price is within this of it. ``episode_gap_s`` — a cluster
    absent for longer than this closes the episode (dedup boundary). ``buckets``
    — confluence-score bucket edges for :func:`summarize_study`."""

    confluence: ConfluenceConfig
    horizon_s: float
    target_r: float
    stop_offset_bps: float
    atr_stop_mult: float
    entry_zone_bps: float
    episode_gap_s: float
    buckets: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0)


@dataclass(slots=True, frozen=True)
class Obs:
    """One forward observation, sampled once at an episode's onset.

    ``confluence_score`` is the causal setup score; ``n_venues``/``n_zones`` are
    its components. ``entry``/``stop``/``risk`` define R. ``mfe_r``/``mae_r`` are
    the forward MFE/MAE in R-multiples, ``fwd_return_bps`` the horizon return,
    ``bounced`` whether target_r was reached before -1R. ``horizon_snaps`` is
    how many forward snapshots were measured (loud small-window flag)."""

    ts: float
    confluence_score: float
    n_venues: int
    n_zones: int
    entry: float
    stop: float
    risk: float
    mfe_r: float
    mae_r: float
    fwd_return_bps: float
    bounced: bool
    horizon_snaps: int


def _best_near_support(clusters: list[ConfluenceCluster], mid: float,
                       entry_zone_bps: float) -> ConfluenceCluster | None:
    """Highest-score TRADEABLE support cluster sitting just BELOW and within
    ``entry_zone_bps`` of ``mid``. Pure; uses only this snapshot's clusters."""
    tol = mid * entry_zone_bps / 1e4
    best: ConfluenceCluster | None = None
    for c in clusters:
        if c.side is not Side.SUPPORT or not c.tradeable:
            continue
        if c.price_hi < mid and (mid - c.price_hi) <= tol:
            if best is None or c.score > best.score:
                best = c
    return best


def _bands_overlap(a: ConfluenceCluster, b: ConfluenceCluster) -> bool:
    """True if two clusters' price bands overlap — same persisting episode."""
    return a.price_lo <= b.price_hi and a.price_hi >= b.price_lo


def _measure_forward(snapshots, i: int, entry: float, risk: float,
                     cfg: StudyConfig) -> tuple[float, float, float, bool, int]:
    """Scan snapshots strictly AFTER onset index ``i`` within ``horizon_s`` and
    return (mfe_r, mae_r, fwd_return_bps, bounced, horizon_snaps). Forward-only:
    never touches snapshots at or before ``i`` (the onset)."""
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
    return mfe_r, mae_r, fwd_return_bps, bounced, n


def study_observations(snapshots, cfg: StudyConfig, *,
                       runtime: RuntimeConfig | None = None) -> list[Obs]:
    """Replay ``snapshots`` through a ``Detector`` + ``analyze_confluence``,
    sampling ONE forward observation per support-cluster episode.

    Faithful: the detector + ATR are the same ones ``run_backtest`` uses (built
    from ``runtime``). The score is causal (data <= onset); the forward path is
    the measured outcome (data > onset)."""
    snaps = list(snapshots)
    if not snaps:
        return []
    runtime = runtime or RuntimeConfig()
    detector = _detector(runtime)
    atr = ATR(window=runtime.atr_window)

    out: list[Obs] = []
    open_cluster: ConfluenceCluster | None = None   # band of the currently-open episode
    last_seen_ts: float | None = None               # last ts a matching cluster appeared

    for i, snap in enumerate(snaps):
        analysis = detector.update(snap)
        atr.update(snap.mid)
        clusters = analyze_confluence(analysis, cfg.confluence)
        cand = _best_near_support(clusters, snap.mid, cfg.entry_zone_bps)

        # Close the open episode if its cluster has lapsed beyond episode_gap_s.
        if open_cluster is not None and last_seen_ts is not None:
            if snap.ts - last_seen_ts > cfg.episode_gap_s:
                open_cluster = None

        if cand is None:
            continue

        # An open, band-overlapping episode persists: dedup (no new observation).
        if open_cluster is not None and _bands_overlap(open_cluster, cand):
            last_seen_ts = snap.ts
            continue

        # New episode onset (first appearance, after a gap, or band shifted):
        # emit exactly ONE observation here, measured strictly forward.
        entry = snap.mid
        stop = min(cand.price_lo * (1 - cfg.stop_offset_bps / 1e4),
                   entry - atr.value() * cfg.atr_stop_mult)
        risk = entry - stop
        if risk <= 0.0:
            # Degenerate stop (>= entry): cannot define R. Open the episode so we
            # do not re-sample the same persisting cluster, but record no obs.
            open_cluster, last_seen_ts = cand, snap.ts
            continue
        mfe_r, mae_r, fwd_bps, bounced, n = _measure_forward(snaps, i, entry, risk, cfg)
        out.append(Obs(
            ts=snap.ts, confluence_score=cand.score, n_venues=cand.n_venues,
            n_zones=cand.n_zones, entry=entry, stop=stop, risk=risk,
            mfe_r=mfe_r, mae_r=mae_r, fwd_return_bps=fwd_bps, bounced=bounced,
            horizon_snaps=n))
        open_cluster, last_seen_ts = cand, snap.ts

    return out


def _bucket_label(lo: float, hi: float) -> str:
    return f"[{lo:.2f},{hi:.2f})"


def _summary_row(bucket: str, members: list[Obs], target_r: float) -> dict:
    """Aggregate one bucket. ``expectancy_r`` is the mean per-obs R outcome:
    +target_r if bounced else -1 (the honest, scale-free verdict). Empty buckets
    report zeros (finite)."""
    n = len(members)
    if n == 0:
        return {"bucket": bucket, "n": 0, "bounce_rate": 0.0,
                "mean_fwd_return_bps": 0.0, "mean_mfe_r": 0.0, "mean_mae_r": 0.0,
                "expectancy_r": 0.0}
    bounce_rate = sum(1 for o in members if o.bounced) / n
    expectancy_r = mean(target_r if o.bounced else -1.0 for o in members)
    return {
        "bucket": bucket,
        "n": n,
        "bounce_rate": bounce_rate,
        "mean_fwd_return_bps": mean(o.fwd_return_bps for o in members),
        "mean_mfe_r": mean(o.mfe_r for o in members),
        "mean_mae_r": mean(o.mae_r for o in members),
        "expectancy_r": expectancy_r,
    }


def summarize_study(observations, buckets: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0),
                    *, target_r: float = 2.0) -> list[dict]:
    """Bucket observations by confluence score and aggregate bounce-rate +
    expectancy_r per bucket, then append an ALL/baseline row over every
    observation. Buckets partition ``[edges[0], edges[-1]]``; the final bucket is
    closed on the right so a perfect score of 1.0 lands in the top bucket. N is
    reported per bucket loudly — a great rate on tiny N is noise."""
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
                if o.confluence_score >= lo and (o.confluence_score <= hi if top
                                                 else o.confluence_score < hi)
            ]
            rows.append(_summary_row(_bucket_label(lo, hi), members, target_r))
    rows.append(_summary_row("ALL", obs, target_r))
    return rows
