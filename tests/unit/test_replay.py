# tests/unit/test_replay.py
import pathlib

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


def _cadence_ref(updates, *, interval_s, staleness_s):
    """Live reference: the aggregator applies every update EXCEPT checkpoints (those are
    recorder-only copies of its own state), at the replay's cadence convention."""
    agg = Aggregator(list(_SPECS), PegProvider(), bin_bps=5.0, window_bps=300.0, staleness_s=staleness_s)
    out = []; nb = None
    for u in updates:
        if nb is None:
            nb = u.ts
        while nb < u.ts:
            s = agg.snapshot(nb)
            if s is not None:
                out.append(s)
            nb += interval_s
        if not u.checkpoint:
            agg.apply(u)
    if nb is not None:
        s = agg.snapshot(nb)
        if s is not None:
            out.append(s)
    return out


def test_recorder_encoded_lake_replays_identically_to_live(tmp_path, monkeypatch):
    """Full chain on real Parquet: BookRecorder(+BookEncoder) -> ParquetSink -> replay.
    kraken behaves like a native feed (one snapshot, then deltas); coinbase like a ccxt
    feed (the WHOLE book every message, often unchanged -> heartbeats). Short staleness
    so venue freshness (last_ts parity) is exercised, plus live-state checkpoints."""
    import random
    import pavilos.backtest.replay as replay_mod
    from pavilos.aggregator.book_state import BookState
    from pavilos.persistence.recorder import BookRecorder
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    rng = random.Random(11)
    live = {"kraken": BookState("kraken"), "coinbase": BookState("coinbase")}
    cb = {100.0 + i * 0.1: 1.0 for i in range(10)}
    updates = [BookUpdate("kraken", 1.0, tuple((99.0 - i * 0.5, 1.0 + i) for i in range(8)),
                          tuple((101.0 + i * 0.5, 1.0 + i) for i in range(8)), True, None)]
    live["kraken"].apply(updates[0])
    ts = 1.0
    for step in range(400):
        ts += rng.choice([0.05, 0.3, 0.9, 2.5])           # 2.5 s gaps > staleness: venues go stale
        r = rng.random()
        if r < 0.45:
            lv = [(round(99.0 - rng.randint(0, 12) * 0.5, 2), rng.choice([0.0, 0.5, 2.0, 3.0]))
                  for _ in range(rng.randint(1, 3))]
            av = [(round(101.0 + rng.randint(0, 12) * 0.5, 2), rng.choice([0.0, 0.5, 2.0, 3.0]))
                  for _ in range(rng.randint(0, 2))]
            u = BookUpdate("kraken", ts, tuple(lv), tuple(av), False, None)
        elif r < 0.9:
            if rng.random() < 0.5:                         # change one coinbase level
                p = 100.0 + rng.randint(0, 12) * 0.1
                cb[round(p, 1)] = rng.choice([0.5, 1.0, 4.0])
            u = BookUpdate("coinbase", ts, tuple(sorted(cb.items(), reverse=True)),
                           ((102.0, 1.0),), True, None)
        else:
            ex = rng.choice(["kraken", "coinbase"])
            b = live[ex]
            if b.mid() is None:
                continue
            u = BookUpdate(ex, b.last_ts, tuple(b.bids().items()), tuple(b.asks().items()),
                           True, None, checkpoint=True)
        if not u.checkpoint:
            live[u.exchange].apply(u)
        updates.append(u)
    rec = BookRecorder(ParquetSink(str(tmp_path)), flush_interval_s=0.05)
    rec.start()
    for u in updates:
        rec.record(u)
    rec.stop()
    assert rec.dropped == 0
    for stale in (100.0, 1.5):
        got = replay_snapshots(str(tmp_path), 0.0, ts + 1.0, window_bps=300.0, bin_bps=5.0,
                               interval_s=1.0, staleness_s=stale)
        want = _cadence_ref(updates, interval_s=1.0, staleness_s=stale)
        assert len(got) == len(want) > 50
        assert [_norm(s) for s in got] == [_norm(s) for s in want]


def _warm_lake(tmp_path, monkeypatch):
    """kraken as a native feed seen mid-session: a full checkpoint 60 s before the
    window, then only deltas inside it (one bid change, one ask change, one removal)."""
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    full_bids = tuple((99.0 - i * 0.5, 2.0) for i in range(10))
    full_asks = tuple((101.0 + i * 0.5, 2.0) for i in range(10))
    updates = [
        BookUpdate("kraken", 940.0, full_bids, full_asks, True, None, checkpoint=True),
        BookUpdate("kraken", 1001.0, ((99.0, 3.0),), ((101.0, 1.0),), False, None),
        BookUpdate("kraken", 1003.0, ((98.5, 0.0),), (), False, None),
        BookUpdate("kraken", 1004.5, (), ((101.5, 5.0),), False, None),
    ]
    _write_lake(str(tmp_path), updates)
    return updates


_KW = dict(window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=1e9)


def test_warmup_equals_full_history_replay_restricted_to_the_window(tmp_path, monkeypatch):
    """With warm-up the window starts from the complete book: it must equal a replay of
    the WHOLE history (same integer boundary grid) restricted to the window, and emit
    nothing before t0."""
    _warm_lake(tmp_path, monkeypatch)
    warm = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=120.0, **_KW)
    full = replay_snapshots(str(tmp_path), 900.0, 1010.0, warmup_s=0.0, **_KW)
    assert [s.ts for s in warm] == [1001.0, 1002.0, 1003.0, 1004.0, 1005.0]
    assert [_norm(s) for s in warm] == [_norm(s) for s in full if s.ts >= 1001.0]
    # a window starting exactly ON an update anchors on it, as without warm-up
    at = replay_snapshots(str(tmp_path), 1001.0, 1010.0, warmup_s=120.0, **_KW)
    assert [_norm(s) for s in at] == [_norm(s) for s in warm]
    depth = lambda s: round(sum(b.size for b in s.bids) + sum(a.size for a in s.asks), 9)
    # +-300 bps of mid 100: bids 99(3)+98.5..97(4x2) = 11, asks 101(1)+101.5..103(4x2) = 9
    assert depth(warm[0]) == 20.0                      # untouched levels are there


def test_warmup_zero_or_too_short_keeps_the_previous_behaviour(tmp_path, monkeypatch):
    """warmup_s=0 == the pre-M17 replay when no recorder restart is marked inside the window
    (window updates only, anchored on the first);
    a warm-up that does not reach the checkpoint reads nothing more."""
    updates = _warm_lake(tmp_path, monkeypatch)
    cold = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=0.0, **_KW)
    short = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=59.0, **_KW)
    want = _cadence([u for u in updates if u.ts >= 1000.0], interval_s=1.0, window_bps=300.0)
    assert [s.ts for s in cold] == [1001.0, 1002.0, 1003.0, 1004.0, 1005.0]
    assert [_norm(s) for s in cold] == [_norm(s) for s in want] == [_norm(s) for s in short]
    depth = lambda s: round(sum(b.size for b in s.bids) + sum(a.size for a in s.asks), 9)
    assert depth(cold[0]) == 4.0                       # only the in-window levels
    # the checkpoint sits exactly at t0 - 60: a 60 s warm-up includes it (closed bound)
    edge = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=60.0, **_KW)
    warm = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=120.0, **_KW)
    assert [_norm(s) for s in edge] == [_norm(s) for s in warm]


def test_warmup_alone_emits_nothing(tmp_path, monkeypatch):
    _warm_lake(tmp_path, monkeypatch)
    assert replay_snapshots(str(tmp_path), 960.0, 1000.0, warmup_s=120.0, **_KW) == []


def test_default_warmup_reaches_a_checkpoint_one_minute_before(tmp_path, monkeypatch):
    _warm_lake(tmp_path, monkeypatch)                       # checkpoint at t0 - 60
    default = replay_snapshots(str(tmp_path), 1000.0, 1010.0, **_KW)
    warm = replay_snapshots(str(tmp_path), 1000.0, 1010.0, warmup_s=120.0, **_KW)
    assert [_norm(s) for s in default] == [_norm(s) for s in warm]


def test_window_starting_after_a_venue_outage_sees_its_whole_book(tmp_path, monkeypatch):
    """kraken goes silent for 300 s and reconnects with a fresh snapshot. A window that
    starts 5 s after the reconnect must see its WHOLE book: the recorder writes the first
    update after a silence whole, so the warm-up finds a base (no checkpoint here)."""
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence.recorder import BookRecorder
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    book1 = (tuple((99.0 - i * 0.1, 1.0) for i in range(20)), tuple((101.0 + i * 0.1, 1.0) for i in range(20)))
    book2 = (tuple((99.0 - i * 0.1, 2.0) for i in range(20)), tuple((101.0 + i * 0.1, 2.0) for i in range(20)))
    updates = [
        BookUpdate("kraken", 1000.0, book1[0], book1[1], True, None),
        BookUpdate("kraken", 1004.0, ((99.0, 3.0),), (), False, None),
        BookUpdate("kraken", 1310.0, book2[0], book2[1], True, None),     # reconnect after 306 s
        BookUpdate("kraken", 1312.0, ((98.9, 5.0),), (), False, None),
        BookUpdate("kraken", 1316.0, (), ((101.0, 4.0),), False, None),
        BookUpdate("kraken", 1320.0, ((98.8, 0.0),), (), False, None),
    ]
    rec = BookRecorder(ParquetSink(str(tmp_path)), flush_interval_s=0.05, rebase_after_s=15.0)
    rec.start()
    for u in updates:
        rec.record(u)
    rec.stop()
    got = replay_snapshots(str(tmp_path), 1315.0, 1325.0, window_bps=300.0, bin_bps=5.0,
                           interval_s=1.0, staleness_s=15.0)
    want = [s for s in _cadence_ref(updates, interval_s=1.0, staleness_s=15.0) if s.ts >= 1316.0]
    assert got and [_norm(s) for s in got] == [_norm(s) for s in want]


def test_replay_reads_only_the_hours_it_needs(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    book = (((99.0, 1.0),), ((101.0, 1.0),))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 3600.0 * h + 10.0, *book, True, None)
                                for h in (1, 5, 9)])
    seen = []
    real = replay_mod._query
    monkeypatch.setattr(replay_mod, "_query", lambda con, src, a, b: seen.extend(src) or real(con, src, a, b))
    got = replay_snapshots(str(tmp_path), 5 * 3600.0, 5 * 3600.0 + 30.0, warmup_s=0.0, **_KW)
    assert got and len(seen) == 1 and "/05/" in seen[0].replace("\\", "/")


def test_a_broken_parquet_file_is_skipped_not_fatal(tmp_path, monkeypatch, caplog):
    import logging
    import os
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _warm_lake(tmp_path, monkeypatch)
    good = replay_snapshots(str(tmp_path), 1000.0, 1010.0, **_KW)          # before the damage
    part = os.path.join(str(tmp_path), "exchange=kraken", "date=1970-01-01", "00")
    with open(os.path.join(part, "truncated.parquet"), "wb") as fh:
        fh.write(b"PAR1 then the process was killed")
    open(os.path.join(part, "empty.parquet"), "wb").close()
    with caplog.at_level(logging.WARNING, logger="pavilos.backtest.replay"):
        got = replay_snapshots(str(tmp_path), 1000.0, 1010.0, **_KW)
    assert good and [_norm(s) for s in got] == [_norm(s) for s in good]
    assert any("skipping 2 unreadable" in r.getMessage() for r in caplog.records)


def test_a_recorder_restart_resets_the_books(tmp_path, monkeypatch):
    """Process 1 recorded kraken AND coinbase; process 2 (marked start 998) only has
    kraken so far. Right after the restart the replay must not keep coinbase's old book
    alive -- the new live process started with empty books."""
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence.runs import mark_run_start
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    p1 = [BookUpdate("kraken", 990.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
          BookUpdate("coinbase", 995.0, ((99.5, 7.0),), ((100.5, 7.0),), True, None)]
    p2 = [BookUpdate("kraken", 1000.0, ((99.0, 2.0),), ((101.0, 2.0),), True, None),
          BookUpdate("kraken", 1003.0, ((98.0, 1.0),), (), False, None)]
    _write_lake(str(tmp_path), p1)                           # process 1
    mark_run_start(str(tmp_path), ts=998.0)
    _write_lake(str(tmp_path), p2)                           # process 2: seq_no from 0 again
    kw = dict(window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0)
    got = replay_snapshots(str(tmp_path), 1000.0, 1005.0, **kw)
    assert got and all(s.venues_active == ("kraken",) for s in got)


def test_excluded_venues_are_not_read(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [
        BookUpdate("kraken", 1000.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
        BookUpdate("coinbase", 1000.5, ((99.5, 7.0),), ((100.5, 7.0),), True, None)])
    kw = dict(window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0)
    got = replay_snapshots(str(tmp_path), 1000.0, 1002.0, exclude=("coinbase",), **kw)
    assert got and all(s.venues_active == ("kraken",) for s in got)


def test_check_files_tells_readable_from_broken(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pavilos.persistence.lake_files import check_files
    d = tmp_path / "h"
    d.mkdir()
    pq.write_table(pa.table({"ts": [1.0]}), str(d / "ok.parquet"))
    (d / "head.parquet").write_bytes(b"XXXX" + b"\x00" * 8 + b"PAR1")    # damaged head
    (d / "tail.parquet").write_bytes(b"PAR1" + b"\x00" * 8 + b"XXXX")    # truncated tail
    (d / "tiny.parquet").write_bytes(b"PAR1PAR1")                        # too short to be one
    (d / "footer.parquet").write_bytes(b"PAR1" + b"\x00" * 40 + b"PAR1")  # framed, footer garbage
    (d / "empty.parquet").write_bytes(b"")
    names = ["ok", "head", "tail", "tiny", "footer", "empty"]
    good, bad = check_files([str(d / f"{n}.parquet") for n in names])
    assert [pathlib.Path(p).stem for p in good] == ["ok"]
    assert sorted(pathlib.Path(p).stem for p in bad) == ["empty", "footer", "head", "tail", "tiny"]


def test_a_venue_the_new_run_wrote_first_survives_the_restart(tmp_path, monkeypatch):
    """coinbase was down at the end of run 1; in run 2 it writes BEFORE kraken does. The
    reset happens at the marked start, so coinbase's new book is kept (a seq_no-0
    heuristic used to wipe it when kraken's seq_no 0 arrived)."""
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence.runs import mark_run_start
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 990.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None)])
    mark_run_start(str(tmp_path), ts=998.0)
    _write_lake(str(tmp_path), [
        BookUpdate("coinbase", 999.0, ((99.5, 7.0),), ((100.5, 7.0),), True, None),
        BookUpdate("kraken", 1000.0, ((99.0, 2.0),), ((101.0, 2.0),), True, None),
        BookUpdate("kraken", 1003.0, ((98.0, 1.0),), (), False, None)])
    kw = dict(window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=15.0)
    got = replay_snapshots(str(tmp_path), 1000.0, 1005.0, **kw)
    assert got and all(s.venues_active == ("coinbase", "kraken") for s in got)


def test_boundaries_before_a_restart_keep_the_old_books(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence.runs import mark_run_start
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 1000.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
                                BookUpdate("kraken", 1001.0, ((98.0, 1.0),), (), False, None)])
    mark_run_start(str(tmp_path), ts=1003.5)
    _write_lake(str(tmp_path), [BookUpdate("kraken", 1005.0, ((90.0, 9.0),), ((110.0, 9.0),), True, None)])
    kw = dict(window_bps=3000.0, bin_bps=5.0, interval_s=1.0, staleness_s=1e9)
    got = {s.ts: s for s in replay_snapshots(str(tmp_path), 1000.0, 1010.0, **kw)}
    assert {1000.0, 1001.0, 1002.0, 1003.0}.issubset(got) and 1004.0 not in got   # empty after the reset
    assert got[1003.0].mid == 100.0 and got[1005.0].mid == 100.0
    assert sum(b.size for b in got[1003.0].bids) == 2.0 and sum(b.size for b in got[1005.0].bids) == 9.0


def test_a_row_exactly_at_t1_on_an_hour_edge_is_read(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 7190.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
                                BookUpdate("kraken", 7200.0, ((98.0, 4.0),), (), False, None)])
    kw = dict(window_bps=300.0, bin_bps=5.0, interval_s=5.0, staleness_s=1e9)
    got = replay_snapshots(str(tmp_path), 7190.0, 7200.0, warmup_s=0.0, **kw)
    assert got[-1].ts == 7200.0 and sum(b.size for b in got[-1].bids) == 5.0


def test_replay_uses_the_capped_duckdb_connection(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence import duck
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 1000.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None)])
    calls = []
    monkeypatch.setattr(replay_mod, "connect", lambda: calls.append(1) or duck.connect())
    assert replay_snapshots(str(tmp_path), 1000.0, 1001.0, **_KW)
    assert calls == [1]


def test_a_restart_after_the_last_row_closes_the_final_boundary(tmp_path, monkeypatch):
    import pavilos.backtest.replay as replay_mod
    from pavilos.persistence.runs import mark_run_start
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    _write_lake(str(tmp_path), [BookUpdate("kraken", 1000.0, ((99.0, 1.0),), ((101.0, 1.0),), True, None),
                                BookUpdate("kraken", 1002.5, ((98.0, 1.0),), (), False, None)])
    mark_run_start(str(tmp_path), ts=1002.8)                 # the process died; a new one began
    kw = dict(window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=1e9)
    got = replay_snapshots(str(tmp_path), 1000.0, 1010.0, **kw)
    assert [s.ts for s in got] == [1000.0, 1001.0, 1002.0]  # no 1003 with the dead run's books
