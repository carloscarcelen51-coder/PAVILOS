# tests/unit/test_replay.py
from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.backtest.replay import replay_snapshots


_SPECS = (VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A))


def _write_lake(base, updates):
    """Persist updates exactly like BookRecorder (seq_no per exchange, one row/level)."""
    sink = ParquetSink(base)
    seq: dict = {}
    by_ex: dict = {}
    for u in updates:
        s = seq.get(u.exchange, 0); seq[u.exchange] = s + 1
        rows = by_ex.setdefault(u.exchange, [])
        for p, sz in u.bids:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "bid", "price": p, "size": sz})
        for p, sz in u.asks:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange, "is_snapshot": u.is_snapshot, "side": "ask", "price": p, "size": sz})
    for ex, rows in by_ex.items():
        sink.write(ex, rows)


def _cadence(updates, *, interval_s, window_bps=300.0):
    """Reference = the SAME cadence algorithm replay uses, run on the ORIGINAL updates.
    Comparing replay(lake) to this isolates lake round-trip faithfulness from any
    boundary-convention question (both sides share the convention)."""
    agg = Aggregator(list(_SPECS), PegProvider(), bin_bps=5.0, window_bps=window_bps, staleness_s=100.0)
    out = []; nb = None
    for u in sorted(updates, key=lambda x: x.ts):   # stable -> intra-ts keeps input order (irrelevant at cadence)
        if nb is None:
            nb = u.ts
        while nb < u.ts:
            s = agg.snapshot(nb)
            if s is not None:
                out.append(s)
            nb += interval_s
        agg.apply(u)
    if nb is not None:
        s = agg.snapshot(nb)
        if s is not None:
            out.append(s)
    return out


def _norm(s):
    return None if s is None else (
        round(s.mid, 9),
        tuple((round(x.price, 9), round(x.size, 9)) for x in s.bids),
        tuple((round(x.price, 9), round(x.size, 9)) for x in s.asks),
        s.venues_active)


def test_replay_roundtrip_matches_cadence_aggregation(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))   # use test specs
    updates = [   # snapshots + a delta (size-0 remove) + a GAP (2.3 -> 4.7)
        BookUpdate("kraken", 1.0, ((100.0, 1.0), (99.5, 2.0)), ((100.5, 1.5), (101.0, 0.5)), True, 1),
        BookUpdate("coinbase", 1.0, ((100.1, 3.0),), ((100.6, 2.0),), True, 1),
        BookUpdate("kraken", 2.3, ((99.5, 0.0), (99.0, 4.0)), ((100.5, 2.5),), False, 2),
        BookUpdate("coinbase", 4.7, ((100.2, 5.0),), ((100.7, 3.0),), True, 2),
    ]
    _write_lake(str(tmp_path), updates)
    got = replay_snapshots(str(tmp_path), 0.0, 100.0, window_bps=300.0, bin_bps=5.0,
                           interval_s=1.0, staleness_s=100.0)
    want = _cadence(updates, interval_s=1.0)
    assert [_norm(s) for s in got] == [_norm(s) for s in want]
    assert len(got) == 5                                   # cadence boundaries 1,2,3,4,5
    assert any(round(x.price) == 99 for x in got[-1].bids)  # delta applied: 99.5 removed, 99.0 present


def test_replay_empty_range_returns_empty(tmp_path):
    assert replay_snapshots(str(tmp_path), 0.0, 1.0, window_bps=300.0, bin_bps=5.0,
                            interval_s=1.0, staleness_s=15.0) == []
