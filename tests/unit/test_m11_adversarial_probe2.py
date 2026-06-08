# tests/unit/test_m11_adversarial_probe2.py
"""ADVERSARIAL probes part 2: look-ahead, gap carry-forward, streaming, empty,
window_sweep det_window_bps application."""
import ast
import dataclasses
import os

import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier, DepthBin, CombinedDepthSnapshot
from pavilos.persistence.parquet_sink import ParquetSink
import pavilos.backtest.replay as replay_mod
from pavilos.backtest.replay import replay_snapshots, _iter_updates
from pavilos.core.runtime import RuntimeConfig
from pavilos.backtest.analysis import window_sweep, detection_profile

_SPECS = (
    VenueSpec("kraken", Quote.USD, Tier.A),
    VenueSpec("binance", Quote.USDT, Tier.A),
    VenueSpec("coinbase", Quote.USD, Tier.A),
)


def _write_lake(base, updates):
    sink = ParquetSink(base)
    seq = {}; by_ex = {}
    for u in updates:
        s = seq.get(u.exchange, 0); seq[u.exchange] = s + 1
        rows = by_ex.setdefault(u.exchange, [])
        for p, sz in u.bids:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "bid", "price": float(p), "size": float(sz)})
        for p, sz in u.asks:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "ask", "price": float(p), "size": float(sz)})
    for ex, rows in by_ex.items():
        sink.write(ex, rows)


def test_no_lookahead(tmp_path, monkeypatch):
    """No snapshot at boundary b may contain a level whose SOURCE update had ts > b.
    We track, per (exchange, USD-price-ish), the earliest ts a level value appeared,
    and assert every composition entry in a snapshot at ts=b came from an update<=b.

    Simpler, exact test: a NEW level introduced strictly after a boundary must not be
    present in that boundary's snapshot. Introduce a distinctive far level only at a
    late ts and assert earlier snapshots never carry it."""
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    updates = [
        BookUpdate("kraken", 1.0, ((100.0, 1.0), (99.5, 2.0)), ((100.5, 1.5),), True, 1),
        BookUpdate("coinbase", 1.0, ((100.0, 1.0),), ((100.5, 1.0),), True, 1),
        # A distinctive bid wall introduced ONLY at t=3.6 (between boundaries 3 and 4).
        BookUpdate("kraken", 3.6, ((98.0, 999.0),), (), False, 2),
    ]
    _write_lake(str(tmp_path), updates)
    got = replay_snapshots(str(tmp_path), 0.0, 100.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=100.0)
    for s in got:
        has_wall = any(abs(b.size - 999.0) < 1e-6 or b.composition.get("kraken", 0.0) >= 999.0 for b in s.bids)
        if s.ts < 3.6:
            assert not has_wall, f"look-ahead: boundary {s.ts} shows level from update at ts=3.6"
        if s.ts >= 4.0:
            assert has_wall, f"boundary {s.ts} should carry the wall introduced at 3.6"


def test_gap_carry_forward_no_dup_ts(tmp_path, monkeypatch):
    """A long gap with no updates must advance boundaries and emit carry-forward
    snapshots — with strictly increasing, non-duplicate ts, no bogus ts."""
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    updates = [
        # distinct prices off mid so bids/asks survive the strict lo<=usd<mid / mid<usd<=hi filter
        BookUpdate("kraken", 1.0, ((99.5, 1.0),), ((100.5, 1.0),), True, 1),
        BookUpdate("coinbase", 1.0, ((99.5, 1.0),), ((100.5, 1.0),), True, 1),
        BookUpdate("kraken", 10.0, ((99.5, 2.0),), ((100.5, 2.0),), False, 2),  # big gap 1->10
        BookUpdate("coinbase", 10.0, ((99.5, 2.0),), ((100.5, 2.0),), False, 2),
    ]
    _write_lake(str(tmp_path), updates)
    got = replay_snapshots(str(tmp_path), 0.0, 100.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=100.0)
    tss = [round(s.ts, 9) for s in got]
    # Boundaries: 1,2,...,10 (first at first ts=1.0; final at last ts=10.0).
    assert tss == [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0], tss
    assert len(tss) == len(set(tss)), "duplicate boundary ts emitted"
    assert tss == sorted(tss), "non-monotonic ts"
    # Carry-forward: snapshots in [1,10) reflect the t=1 state. Combined bid size is
    # kraken(1.0)+coinbase(1.0)=2.0 (two venues sum into the same bin), not t=10's 4.0.
    mids_before_10 = [s for s in got if s.ts < 10.0]
    assert mids_before_10, "no carry-forward snapshots emitted in the gap"
    assert all(any(abs(b.size - 2.0) < 1e-9 for b in s.bids) for s in mids_before_10)
    # The final boundary (10) reflects the post-gap update (combined bid size 4.0).
    last = got[-1]
    assert any(abs(b.size - 4.0) < 1e-9 for b in last.bids)


def test_empty_and_missing_lake(tmp_path, monkeypatch):
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    # missing dir entirely
    assert replay_snapshots(str(tmp_path / "does_not_exist"), 0.0, 1.0,
                            window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0) == []
    # dir exists but no parquet
    (tmp_path / "empty").mkdir()
    assert replay_snapshots(str(tmp_path / "empty"), 0.0, 1.0,
                            window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0) == []
    # range that selects zero rows (data exists outside [t0,t1])
    _write_lake(str(tmp_path / "data"), [
        BookUpdate("kraken", 50.0, ((100.0, 1.0),), ((100.5, 1.0),), True, 1),
        BookUpdate("coinbase", 50.0, ((100.0, 1.0),), ((100.5, 1.0),), True, 1),
    ])
    assert replay_snapshots(str(tmp_path / "data"), 0.0, 10.0,
                            window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0) == []


def test_uses_fetchmany_streaming_not_fetchall():
    """Source-level proof: _iter_updates uses res.fetchmany(...) and never fetchall()."""
    src = open(os.path.join(os.path.dirname(replay_mod.__file__), "replay.py")).read()
    tree = ast.parse(src)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    methods = {c.func.attr for c in calls}
    assert "fetchmany" in methods, "replay must stream via fetchmany"
    assert "fetchall" not in methods, "replay must NOT buffer the whole lake with fetchall"


def _wall_snap(ts, mid, wall_bps, wall_size):
    """A snapshot whose support wall sits ~wall_bps below mid, others ~1.0."""
    wall_price = mid * (1.0 - wall_bps / 1e4)
    bids = (DepthBin(mid * (1 - 0.0002), 1.0, {"k": 0.5, "b": 0.5}),
            DepthBin(wall_price, wall_size, {"k": wall_size / 2, "b": wall_size / 2}),
            DepthBin(mid * (1 - 0.0009), 1.0, {"k": 0.5, "b": 0.5}))
    asks = (DepthBin(mid * (1 + 0.0001), 1.0, {"k": 0.5, "b": 0.5}),)
    return CombinedDepthSnapshot(ts=ts, mid=mid, bids=bids, asks=asks,
                                 venues_active=("k", "b"), venues_total=2)


def test_det_window_bps_actually_applied():
    """A zone at ~200bps from mid should score >0 confidence at window=300 but be
    proximity-killed (conf 0) at window=200 (distance == half_window -> proximity 0).
    Proves det_window_bps flows into the detector's proximity scoring."""
    cfg = dataclasses.replace(RuntimeConfig(), min_persistence_s=0.0, venues_target=2.0,
                              strength_target=5.0, persistence_target_s=1.0, size_multiple=3.0)
    # zone at 200bps below mid
    snaps = [_wall_snap(float(i), 100.0, wall_bps=200.0, wall_size=30.0) for i in range(40)]

    cfg300 = dataclasses.replace(cfg, det_window_bps=300.0)
    cfg200 = dataclasses.replace(cfg, det_window_bps=200.0)
    prof300 = detection_profile(snaps, cfg300)
    prof200 = detection_profile(snaps, cfg200)
    # At window 300, the 200bps zone has positive proximity -> positive confidence.
    assert prof300["avg_confidence"] > 0.0
    # At window 200, distance == half_window so proximity clamps to 0 -> conf 0.
    assert prof200["avg_confidence"] == 0.0, prof200
    assert prof300["avg_confidence"] > prof200["avg_confidence"]


def test_window_sweep_threads_det_window_into_aggregator_and_detector(tmp_path, monkeypatch):
    """window_sweep must set BOTH window_bps (aggregator) and det_window_bps (detector)
    to each swept value. A far zone (~±250bps) surfaces at window 300 but is EXCLUDED
    by the aggregator window at 200 (no level beyond ±200bps -> no wall -> no zone)."""
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    mid = 100.0
    # Many small bid levels spread 10..150bps below mid (so the side median is ~1.0),
    # plus one BIG wall at ~250bps below mid. At window 300 the wall is binned and stands
    # out (prominence >> size_multiple); at window 200 it's beyond the aggregate window
    # and never enters the book, so no wall -> fewer zones.
    small_bids = tuple((mid * (1 - bps / 1e4), 1.0) for bps in range(10, 160, 10))  # 10..150bps
    far = mid * (1 - 250 / 1e4)
    bids = small_bids + ((far, 80.0),)
    asks = ((mid * (1 + 5 / 1e4), 1.0),)
    updates = []
    for i in range(60):
        ts = float(i)
        updates.append(BookUpdate("kraken", ts, bids, asks, True, i + 1))
        updates.append(BookUpdate("coinbase", ts, bids, asks, True, i + 1))
    _write_lake(str(tmp_path), updates)
    base = dataclasses.replace(RuntimeConfig(), min_persistence_s=0.0, venues_target=2.0,
                               strength_target=5.0, persistence_target_s=1.0,
                               snapshot_interval_s=1.0, staleness_s=100.0, bin_bps=5.0,
                               entry_threshold=0.4, min_venues=1)
    rows = window_sweep(str(tmp_path), 0.0, 1000.0, [200.0, 300.0],
                        base_config=base, starting_equity=10_000.0)
    by_w = {r["window_bps"]: r for r in rows}
    # At window 200, the 250bps wall is BEYOND the aggregate window -> never binned ->
    # the snapshot has no far wall -> fewer/zero zones than at 300.
    z200 = by_w[200.0]["detection"]["avg_zones_per_snapshot"]
    z300 = by_w[300.0]["detection"]["avg_zones_per_snapshot"]
    assert z300 > z200, f"det_window_bps/window_bps not applied: z200={z200} z300={z300}"
