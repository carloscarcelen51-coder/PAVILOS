# scripts/analyze.py
"""Analyse the recorded raw-L2 lake. Usage:

    python -m scripts.analyze <data_dir> window-sweep [t0 t1]
    python -m scripts.analyze <data_dir> walkforward [n_splits] [t0 t1]
    python -m scripts.analyze <data_dir> mode-compare [n_splits] [t0 t1]
    python -m scripts.analyze <data_dir> confluence-study [horizon_s] [t0 t1]
    python -m scripts.analyze <data_dir> static-study [horizon_s] [t0 t1]

window-sweep: re-aggregate at 200/300/500/1000 bps -> detection profile + backtest.
walkforward : re-aggregate at the configured window -> in-sample-optimise /
              out-of-sample-score the strategy params (OOS return is the verdict).
mode-compare: walk-forward EACH entry mode (momentum vs reversion) on the same
              slice and print their OOS returns + #trades side by side.
confluence-study: replay -> analyze_confluence -> ONE forward observation per
              support-cluster episode; print bounce-rate + expectancy_r by
              confluence-score bucket vs the baseline (the theory verdict).
static-study: replay -> StaticLevelTracker -> ONE forward observation per
              price-APPROACH episode to a FIXED-price multi-venue static support
              (near-touch excluded); print bounce-rate + expectancy_r by
              level-strength bucket vs baseline, plus the slice's realized
              volatility + episode N (the static-bounce verdict over moving price).
"""
from __future__ import annotations

import sys

import duckdb

from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.replay import replay_snapshots
from pavilos.backtest.analysis import window_sweep
from pavilos.backtest.sweep import walk_forward
from pavilos.detection.confluence import ConfluenceConfig
from pavilos.backtest.confluence_study import StudyConfig, study_observations, summarize_study
from pavilos.detection.static_levels import StaticLevelConfig
from pavilos.backtest.static_study import (
    StaticStudyConfig,
    study_static_approaches,
    summarize_static,
    realized_vol_bps,
)

_WINDOWS = [200.0, 300.0, 500.0, 1000.0]
_WF_GRID = {
    "entry_threshold": [0.4, 0.55, 0.7],
    "opposing_distance_bps": [5.0, 10.0, 20.0],
    "entry_zone_bps": [15.0, 30.0, 60.0],
    "atr_stop_mult": [2.0, 3.0, 5.0],
}
_MOM_GRID = {"entry_threshold": [0.4, 0.55, 0.7], "opposing_distance_bps": [5.0, 10.0, 20.0],
             "entry_zone_bps": [15.0, 30.0, 60.0], "atr_stop_mult": [2.0, 3.0, 5.0]}
_REV_GRID = {"entry_threshold": [0.4, 0.55, 0.7], "entry_zone_bps": [15.0, 30.0, 60.0],
             "atr_stop_mult": [2.0, 3.0, 5.0], "tp_mult": [1.5, 2.0, 3.0]}


def _lake_span(base_dir: str):
    try:
        r = duckdb.sql(f"SELECT min(ts), max(ts), count(*) FROM '{base_dir}/**/*.parquet'").fetchone()
    except Exception:
        return None
    if r is None or r[0] is None:
        return None
    return float(r[0]), float(r[1]), int(r[2])


def format_sweep_row(row: dict) -> str:
    d = row["detection"]; b = row["backtest"]
    return (f"win={row['window_bps']:.0f}  snaps={row['n_snapshots']}  "
            f"zones/snap={d['avg_zones_per_snapshot']:.1f}  conf={d['avg_confidence']:.2f}  "
            f"venues/zone={d['avg_venues_per_zone']:.1f}  strong={d['frac_snaps_with_strong_zone']*100:.0f}%  "
            f"| trades={b.n_trades} ret={b.return_pct:+.2f}% win={b.win_rate:.0f}%")


def format_mode_row(mode: str, folds: list) -> str:
    if not folds:
        return f"  {mode:<10} (no folds)"
    oos = sum(f["oos_result"].return_pct for f in folds) / len(folds)
    tr = sum(f["oos_result"].n_trades for f in folds)
    is_ret = sum(f["is_result"].return_pct for f in folds) / len(folds)
    return f"  {mode:<10} mean IS={is_ret:+.2f}%  ->  mean OOS={oos:+.2f}%  over OOS trades={tr}"


def format_study_row(row: dict) -> str:
    """One readable per-bucket line: confluence-score range (the bucket label),
    episode N (with undecided count), bounce%, expectancy in R (decided-only),
    the historical-touch expectancy split, and mean forward return in bps."""
    und = row.get("n_undecided", 0)
    und_str = f"(und={und}) " if und else ""
    touch = ""
    if "expectancy_r_with_touches" in row:
        touch = (f"  expR[touch={row['expectancy_r_with_touches']:+.2f} "
                 f"none={row['expectancy_r_no_touches']:+.2f}]")
    return (f"  {row['bucket']:<14} n={row['n']:<4} {und_str}"
            f"bounce={row['bounce_rate']*100:.0f}%  "
            f"exp_R={row['expectancy_r']:+.2f}  "
            f"mfe_R={row['mean_mfe_r']:+.2f} mae_R={row['mean_mae_r']:+.2f}  "
            f"fwd={row['mean_fwd_return_bps']:+.1f}bps{touch}")


def format_static_row(row: dict) -> str:
    """One readable per-bucket line for the static-level approach study:
    level-strength range (the bucket label), episode N (with undecided count when
    any), bounce%, expectancy in R (decided-only), MFE/MAE in R, and mean forward
    return in bps. Mirrors :func:`format_study_row` but on the static-study row
    schema (no historical-touch split)."""
    und = row.get("n_undecided", 0)
    und_str = f"(und={und}) " if und else ""
    return (f"  {row['bucket']:<14} n={row['n']:<4} {und_str}"
            f"bounce={row['bounce_rate']*100:.0f}%  "
            f"exp_R={row['expectancy_r']:+.2f}  "
            f"mfe_R={row['mean_mfe_r']:+.2f} mae_R={row['mean_mae_r']:+.2f}  "
            f"fwd={row['mean_fwd_return_bps']:+.1f}bps")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__); return
    base, mode = sys.argv[1], sys.argv[2]
    span = _lake_span(base)
    if span is None:
        print(f"no data under {base}"); return
    t0, t1, n = span
    base_cfg = RuntimeConfig()
    print(f"lake: {n:,} rows, span {t1 - t0:.0f}s ({(t1-t0)/3600:.2f}h)")
    if (t1 - t0) < 3600:
        print("WARNING: < 1h of data -- results are PRELIMINARY (noise). Keep recording for a real verdict.")

    if mode == "window-sweep":
        a = float(sys.argv[3]) if len(sys.argv) > 3 else t0
        b = float(sys.argv[4]) if len(sys.argv) > 4 else t1
        print(f"=== window sweep over [{a:.0f},{b:.0f}] ===")
        for row in window_sweep(base, a, b, _WINDOWS, base_config=base_cfg, starting_equity=base_cfg.starting_equity):
            print("  " + format_sweep_row(row))
    elif mode == "walkforward":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        print(f"=== walk-forward, {n_splits} folds, {len(snaps)} snapshots @ window={base_cfg.window_bps} ===")
        folds = walk_forward(snaps, base_config=base_cfg, grid=_WF_GRID, n_splits=n_splits,
                             starting_equity=base_cfg.starting_equity)
        for i, f in enumerate(folds):
            isr, oos = f["is_result"], f["oos_result"]
            print(f"  fold {i}: IS ret={isr.return_pct:+.2f}% ({isr.n_trades} tr) -> "
                  f"OOS ret={oos.return_pct:+.2f}% ({oos.n_trades} tr)")
        if folds:
            avg = sum(f["oos_result"].return_pct for f in folds) / len(folds)
            tot = sum(f["oos_result"].n_trades for f in folds)
            print(f"  >>> mean OOS return = {avg:+.2f}%  over {tot} OOS trades  (the verdict)")
    elif mode == "mode-compare":
        n_splits = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        print(f"=== mode compare, {n_splits} folds, {len(snaps)} snapshots ===")
        import dataclasses as _dc
        for m, grid in (("momentum", _MOM_GRID), ("reversion", _REV_GRID)):
            cfg = _dc.replace(base_cfg, entry_mode=m)
            folds = walk_forward(snaps, base_config=cfg, grid=grid, n_splits=n_splits,
                                 starting_equity=base_cfg.starting_equity)
            print(format_mode_row(m, folds))
    elif mode == "confluence-study":
        horizon_s = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        conf_cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=base_cfg.venues_target,
                                    threshold=0.5, min_venues=base_cfg.min_venues,
                                    min_persistence_s=base_cfg.min_persistence_s)
        study_cfg = StudyConfig(confluence=conf_cfg, horizon_s=horizon_s, target_r=base_cfg.tp_mult,
                                stop_offset_bps=base_cfg.stop_offset_bps, atr_stop_mult=base_cfg.atr_stop_mult,
                                entry_zone_bps=base_cfg.entry_zone_bps, episode_gap_s=base_cfg.grace_s)
        obs = study_observations(snaps, study_cfg, runtime=base_cfg)
        rows = summarize_study(obs, study_cfg.buckets, target_r=study_cfg.target_r)
        n_episodes = len(obs)
        n_undecided = sum(1 for o in obs if not o.decided)
        n_decided = n_episodes - n_undecided
        print(f"=== confluence forward-return study, horizon={horizon_s:.0f}s, "
              f"{len(snaps)} snapshots @ window={base_cfg.window_bps} ===")
        print(f"    target_R={study_cfg.target_r}  entry_zone={study_cfg.entry_zone_bps:.0f}bps  "
              f"episode_gap={study_cfg.episode_gap_s:.0f}s  -> {n_episodes} independent episodes "
              f"({n_decided} decided, {n_undecided} undecided/horizon-clipped EXCLUDED from exp_R)")
        for row in rows:
            if row["bucket"] == "ALL":
                print("  --- baseline ---")
            print(format_study_row(row))
        print(f"  >>> total episode N = {n_episodes} ({n_decided} decided)  "
              f"(does bounce-rate / expectancy_r RISE with score AND beat baseline? "
              f"does expR[touch] beat expR[none]?)")
        if n_decided < 30:
            print(f"  WARNING: small decided N (< 30 of {n_episodes} episodes) -- bucket rates "
                  f"are NOISE; keep recording for a real verdict.")
        if obs and min(o.horizon_snaps for o in obs) == 0:
            thin = sum(1 for o in obs if o.horizon_snaps == 0)
            print(f"  NOTE: {thin} episode(s) had ZERO forward snapshots (end-of-slice); "
                  f"counted as undecided, not as losses.")
    elif mode == "static-study":
        horizon_s = float(sys.argv[3]) if len(sys.argv) > 3 else 120.0
        a = float(sys.argv[4]) if len(sys.argv) > 4 else t0
        b = float(sys.argv[5]) if len(sys.argv) > 5 else t1
        snaps = replay_snapshots(base, a, b, window_bps=base_cfg.window_bps, bin_bps=base_cfg.bin_bps,
                                 interval_s=base_cfg.snapshot_interval_s, staleness_s=base_cfg.staleness_s)
        static_cfg = StaticLevelConfig(
            level_bucket_usd=25.0, size_multiple=base_cfg.size_multiple, stale_s=base_cfg.staleness_s,
            min_venues=base_cfg.min_venues, level_threshold=0.0, min_away_bps=25.0,
            max_reach_bps=400.0, venues_target=base_cfg.venues_target,
            duration_target_s=base_cfg.persistence_target_s)
        study_cfg = StaticStudyConfig(
            static=static_cfg, horizon_s=horizon_s, target_r=base_cfg.tp_mult,
            stop_offset_bps=base_cfg.stop_offset_bps, atr_stop_mult=base_cfg.atr_stop_mult,
            entry_zone_bps=base_cfg.entry_zone_bps, episode_gap_s=base_cfg.grace_s,
            atr_window=base_cfg.atr_window)
        obs = study_static_approaches(snaps, study_cfg)
        rows = summarize_static(obs, study_cfg.buckets)
        vol_bps = realized_vol_bps(snaps)
        n_episodes = len(obs)
        n_undecided = sum(1 for o in obs if not o.decided)
        n_decided = n_episodes - n_undecided
        print(f"=== static-level approach forward-return study, horizon={horizon_s:.0f}s, "
              f"{len(snaps)} snapshots @ window={base_cfg.window_bps} ===")
        print(f"    target_R={study_cfg.target_r}  entry_zone={study_cfg.entry_zone_bps:.0f}bps  "
              f"min_away={static_cfg.min_away_bps:.0f}bps  episode_gap={study_cfg.episode_gap_s:.0f}s  "
              f"-> {n_episodes} approach episodes ({n_decided} decided, "
              f"{n_undecided} undecided/horizon-clipped EXCLUDED from exp_R)")
        print(f"    realized volatility of slice = {vol_bps:.2f} bps/tick "
              f"(price MOVEMENT — a flat slice yields few/zero approaches, not a verdict)")
        for row in rows:
            if row["bucket"] == "ALL":
                print("  --- baseline ---")
            print(format_static_row(row))
        print(f"  >>> total episode N = {n_episodes} ({n_decided} decided)  "
              f"(does bounce-rate / expectancy_r RISE with level strength AND beat baseline?)")
        if n_decided < 30:
            print(f"  WARNING: small decided N (< 30 of {n_episodes} episodes) -- bucket rates "
                  f"are NOISE; find a MORE VOLATILE slice / keep recording for a real verdict.")
        if obs and min(o.horizon_snaps for o in obs) == 0:
            thin = sum(1 for o in obs if o.horizon_snaps == 0)
            print(f"  NOTE: {thin} episode(s) had ZERO forward snapshots (end-of-slice); "
                  f"counted as undecided, not as losses.")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
