# tests/unit/test_analysis.py
import dataclasses
from pavilos.core.models import DepthBin, CombinedDepthSnapshot
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.analysis import detection_profile


def _bin(p, s):
    return DepthBin(price=p, size=s, composition={"k": s / 2, "b": s / 2})


def _snap(ts, mid):
    bids = (_bin(mid - 1, 1.0), _bin(mid - 5, 30.0), _bin(mid - 9, 1.0))   # a wall ~mid-5
    asks = (_bin(mid + 1, 1.0), _bin(mid + 6, 1.0))
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=bids, asks=asks,
                                 venues_active=("k", "b"), venues_total=2)


def test_detection_profile_reports_zone_stats():
    cfg = dataclasses.replace(RuntimeConfig(), min_persistence_s=0.0, venues_target=2.0,
                              strength_target=5.0, persistence_target_s=1.0)
    snaps = [_snap(float(i), 100.0) for i in range(40)]
    prof = detection_profile(snaps, cfg)
    assert prof["n_snapshots"] == 40
    assert prof["avg_zones_per_snapshot"] >= 0.0
    assert 0.0 <= prof["avg_confidence"] <= 1.0
    assert "frac_snaps_with_strong_zone" in prof


def test_detection_profile_empty():
    prof = detection_profile([], RuntimeConfig())
    assert prof["n_snapshots"] == 0 and prof["avg_zones_per_snapshot"] == 0.0
