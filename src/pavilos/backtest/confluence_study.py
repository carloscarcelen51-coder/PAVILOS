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
while a band-overlapping cluster keeps appearing. The overlap test is padded by a
price tolerance (``confluence_band_bps`` of mid) so a single-bin wall whose
representative price merely *jitters* a couple of bps still reads as the same
persisting episode — without that pad, zero-width point-bands never overlap and
EVERY snapshot would open a new episode, inflating N by orders of magnitude on
real (jittery) data. The episode CLOSES when no matching cluster appears for
longer than ``episode_gap_s`` (or the band shifts to a genuinely different,
beyond-tolerance level), so a cluster that lapses and reforms — or jumps to a far
level — yields TWO observations. N is reported per EPISODE.

R-multiples
-----------
R = entry - stop (the stop risk). MFE = max(mid - entry) over the forward
window, MAE = min(mid - entry). ``mfe_r = MFE / R``, ``mae_r = MAE / R``.
``bounced`` is True iff ``mfe_r`` reaches ``target_r`` BEFORE ``mae_r`` reaches
-1 (target hit before the stop), scanning the forward snapshots in time order.
``decided`` is True iff the forward window resolved one of those two thresholds;
an episode whose horizon expires (or has too few/zero forward snaps) with NEITHER
threshold reached is ``decided=False`` and is EXCLUDED from expectancy_r rather
than charged as a -1R stop-out — undecided/horizon-clipped episodes are reported
separately so the verdict is not silently diluted. ``fwd_return_bps`` is the
close-to-close return at the horizon, in bps.

Historical-touch factor
------------------------
A causal ``LevelHistory`` runs alongside: every snapshot's tradeable support is
``observe``d, and at each episode onset we record ``n_touches`` = the number of
prior distinct episodes that respected this level (data < onset only).
``summarize_study`` breaks expectancy down by touch-presence so the study can
show whether historical touches ADD predictive power over raw confluence score.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from pavilos.core.runtime import RuntimeConfig
from pavilos.detection.confluence import ConfluenceCluster, ConfluenceConfig, analyze_confluence
from pavilos.detection.level_history import LevelHistory
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
    its components; ``n_touches`` is the causal LevelHistory count of prior
    distinct episodes that respected this level (data < onset). ``entry``/
    ``stop``/``risk`` define R. ``mfe_r``/``mae_r`` are the forward MFE/MAE in
    R-multiples, ``fwd_return_bps`` the horizon return. ``decided`` is True iff
    the forward window resolved +target_r or -1R; ``bounced`` is True iff it
    resolved to the target first. ``outcome_r`` is the per-obs R outcome decided
    AT SAMPLE TIME (+target_r if bounced, -1.0 if stopped, ``None`` if
    undecided) — carried here so expectancy can never desync from the bounce
    decision. ``horizon_snaps`` is how many forward snapshots were measured
    (loud small-window flag; horizon_snaps==0 => no forward data => undecided)."""

    ts: float
    confluence_score: float
    n_venues: int
    n_zones: int
    n_touches: int
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


def _bands_overlap(a: ConfluenceCluster, b: ConfluenceCluster, tol: float = 0.0) -> bool:
    """True if two clusters' price bands overlap within ``tol`` — same persisting
    episode. ``tol`` pads the comparison so a single-bin (zero-width) wall whose
    representative price merely jitters a couple of bps still reads as the same
    episode; a band shift beyond ``tol`` opens a genuinely new episode."""
    return a.price_lo - tol <= b.price_hi and a.price_hi + tol >= b.price_lo


def _measure_forward(snapshots, i: int, entry: float, risk: float,
                     cfg: StudyConfig) -> tuple[float, float, float, bool, bool, int]:
    """Scan snapshots strictly AFTER onset index ``i`` within ``horizon_s`` and
    return (mfe_r, mae_r, fwd_return_bps, bounced, decided, horizon_snaps).
    ``decided`` is True iff +target_r or -1R was resolved before the horizon
    expired; a window that reaches neither (incl. an empty forward window) is
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
    # Causal historical-touch memory reported alongside (Scope decision 1): every
    # tradeable support is observed; touches() at onset sees only data < onset.
    history = LevelHistory(band_bps=cfg.confluence.confluence_band_bps,
                           episode_gap_s=cfg.episode_gap_s)

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

        # Tolerance pads the overlap test so a jittery single-bin wall stays one
        # episode; a beyond-tolerance band shift opens a new one.
        tol = snap.mid * cfg.confluence.confluence_band_bps / 1e4

        # An open, band-overlapping episode persists: dedup (no new observation).
        if open_cluster is not None and _bands_overlap(open_cluster, cand, tol):
            last_seen_ts = snap.ts
            # Still feed the persisting touch into the causal memory.
            history.observe(cand.price_lo, snap.ts)
            continue

        # New episode onset (first appearance, after a gap, or band shifted):
        # count PRIOR distinct episodes at this level (data < onset only) BEFORE
        # recording this touch, so the current episode never counts itself.
        n_touches = history.touches(cand.price_lo, now=snap.ts)
        history.observe(cand.price_lo, snap.ts)

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
        mfe_r, mae_r, fwd_bps, bounced, decided, n = _measure_forward(snaps, i, entry, risk, cfg)
        # Outcome R decided at sample time: it can never desync from `bounced`.
        outcome_r = (cfg.target_r if bounced else -1.0) if decided else None
        out.append(Obs(
            ts=snap.ts, confluence_score=cand.score, n_venues=cand.n_venues,
            n_zones=cand.n_zones, n_touches=n_touches, entry=entry, stop=stop, risk=risk,
            mfe_r=mfe_r, mae_r=mae_r, fwd_return_bps=fwd_bps, bounced=bounced,
            decided=decided, outcome_r=outcome_r, horizon_snaps=n))
        open_cluster, last_seen_ts = cand, snap.ts

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


def _summary_row(bucket: str, members: list[Obs], target_r: float) -> dict:
    """Aggregate one bucket. ``expectancy_r`` is the mean DECIDED per-obs R
    outcome (+target_r if bounced, -1 if stopped); undecided/horizon-clipped
    episodes are EXCLUDED and counted in ``n_undecided`` so the verdict is not
    silently diluted. ``expectancy_r_with_touches`` / ``_no_touches`` split the
    decided expectancy by whether the level had prior historical touches, so the
    study can show whether the LevelHistory factor ADDS lift. Empty buckets
    report zeros (finite). ``target_r`` is unused in the math now (outcome_r is
    pre-baked) but kept for signature stability and the empty-bucket contract."""
    n = len(members)
    n_decided = sum(1 for o in members if o.decided)
    n_undecided = n - n_decided
    if n == 0:
        return {"bucket": bucket, "n": 0, "n_decided": 0, "n_undecided": 0,
                "bounce_rate": 0.0, "mean_fwd_return_bps": 0.0, "mean_mfe_r": 0.0,
                "mean_mae_r": 0.0, "expectancy_r": 0.0,
                "expectancy_r_with_touches": 0.0, "expectancy_r_no_touches": 0.0}
    # bounce_rate is over DECIDED obs (an undecided window neither bounced nor
    # stopped, so it is not a "no-bounce"); zero decided -> 0.0.
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
        "expectancy_r_with_touches": _expectancy([o for o in members if o.n_touches > 0]),
        "expectancy_r_no_touches": _expectancy([o for o in members if o.n_touches == 0]),
    }


def summarize_study(observations, buckets: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0),
                    *, target_r: float = 2.0) -> list[dict]:
    """Bucket observations by confluence score and aggregate bounce-rate +
    expectancy_r per bucket, then append an ALL/baseline row over every
    observation. Buckets partition ``[edges[0], edges[-1]]``; the final bucket is
    closed on the right so a perfect score of 1.0 lands in the top bucket. N is
    reported per bucket loudly — a great rate on tiny N is noise.

    Conventions: ``expectancy_r`` is the mean R outcome over DECIDED episodes
    only (each obs carries the outcome decided at sample time); undecided /
    horizon-clipped episodes are EXCLUDED (never a -1R loss) and surfaced as
    ``n_undecided``. Each row also reports the decided expectancy split by
    historical-touch presence (``expectancy_r_with_touches`` /
    ``expectancy_r_no_touches``) so the LevelHistory factor's lift is visible.
    ``target_r`` is retained for signature stability (the outcome is pre-baked
    per obs)."""
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
