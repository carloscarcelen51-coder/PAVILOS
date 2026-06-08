# tests/unit/test_m11_adversarial_probe.py
"""ADVERSARIAL throwaway probes for M11 replay/analysis.
Real ParquetSink + real Aggregator. No network, no D:."""
import dataclasses
import math

import pytest

from pavilos.core.models import BookUpdate, VenueSpec, Quote, Tier
from pavilos.aggregator.aggregator import Aggregator
from pavilos.aggregator.normalize import PegProvider
from pavilos.persistence.parquet_sink import ParquetSink
from pavilos.persistence.recorder import BookRecorder
import pavilos.backtest.replay as replay_mod
from pavilos.backtest.replay import replay_snapshots


# 3 venues incl. a USDT venue (peg path) so build_combined runs peg.to_usd != 1 path
_SPECS = (
    VenueSpec("kraken", Quote.USD, Tier.A),
    VenueSpec("binance", Quote.USDT, Tier.A),   # USDT -> peg path
    VenueSpec("coinbase", Quote.USD, Tier.A),
)


def _write_lake_like_recorder(base, updates):
    """Persist EXACTLY like BookRecorder._flush: monotonic seq_no per exchange in
    arrival order, one row per level, side strings 'bid'/'ask'."""
    sink = ParquetSink(base)
    seq: dict = {}
    by_ex: dict = {}
    for u in updates:
        s = seq.get(u.exchange, 0); seq[u.exchange] = s + 1
        rows = by_ex.setdefault(u.exchange, [])
        for p, sz in u.bids:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange,
                         "is_snapshot": u.is_snapshot, "side": "bid", "price": float(p), "size": float(sz)})
        for p, sz in u.asks:
            rows.append({"seq_no": s, "ts": u.ts, "exchange": u.exchange,
                         "is_snapshot": u.is_snapshot, "side": "ask", "price": float(p), "size": float(sz)})
    # Write per-exchange in chunks to exercise multi-file partitions too.
    for ex, rows in by_ex.items():
        sink.write(ex, rows)


def _cadence(updates, *, interval_s, window_bps, bin_bps, staleness_s, specs):
    """Reference cadence algorithm on ORIGINAL updates (the contract replay claims)."""
    agg = Aggregator(list(specs), PegProvider(), bin_bps=bin_bps,
                     window_bps=window_bps, staleness_s=staleness_s)
    out = []; nb = None
    for u in sorted(updates, key=lambda x: x.ts):   # stable sort
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
        round(s.ts, 9),
        round(s.mid, 9),
        tuple((round(x.price, 9), round(x.size, 9), tuple(sorted((k, round(v, 9)) for k, v in x.composition.items())))
              for x in s.bids),
        tuple((round(x.price, 9), round(x.size, 9), tuple(sorted((k, round(v, 9)) for k, v in x.composition.items())))
              for x in s.asks),
        s.venues_active,
        s.venues_total)


def _rich_updates():
    """A RICH sequence: 3 venues incl USDT, snapshots+deltas, size-0 removes, a venue
    going STALE and dropping out, two venues at the EXACT same ts, updates straddling
    cadence boundaries, two updates from same venue same ts (intra-tick order)."""
    return [
        # t=1.0: kraken + binance + coinbase snapshots (binance prices ~ same USD via peg=1)
        BookUpdate("kraken",   1.0, ((100.0, 1.0), (99.5, 2.0), (99.0, 0.7)), ((100.5, 1.5), (101.0, 0.5)), True, 1),
        BookUpdate("binance",  1.0, ((100.1, 3.0), (99.6, 1.2)),              ((100.6, 2.0), (101.2, 0.9)), True, 1),
        BookUpdate("coinbase", 1.0, ((100.05, 0.8),),                          ((100.55, 1.1),),            True, 1),
        # t=2.3: kraken delta with a size-0 remove (99.5 removed) + add 98.8
        BookUpdate("kraken",   2.3, ((99.5, 0.0), (98.8, 4.0)),                ((100.5, 2.5),),             False, 2),
        # t=2.3: binance delta at the EXACT same ts as kraken's above (two venues same ts)
        BookUpdate("binance",  2.3, ((100.1, 0.0), (99.7, 2.5)),               ((100.6, 1.0),),             False, 2),
        # t=2.3: a SECOND kraken update at the same ts (intra-tick same-venue ordering)
        BookUpdate("kraken",   2.3, ((98.8, 5.0),),                            (),                          False, 3),
        # t=4.7: coinbase update only (kraken last_ts=2.3, binance last_ts=2.3).
        BookUpdate("coinbase", 4.7, ((100.2, 5.0),),                           ((100.7, 3.0),),             True, 2),
        # t=20.0: kraken update — by now coinbase(4.7) & binance(2.3) are STALE vs
        # staleness_s=10 at boundaries >14.7/>12.3. Forces a venue to drop out.
        BookUpdate("kraken",   20.0, ((100.0, 2.0), (99.0, 3.0)),              ((100.5, 1.0),),             False, 4),
    ]


@pytest.mark.parametrize("window_bps,bin_bps,interval_s,staleness_s", [
    (300.0, 5.0, 1.0, 10.0),
    (300.0, 5.0, 0.5, 10.0),    # finer cadence -> straddle boundaries differently
    (500.0, 7.0, 1.0, 10.0),
])
def test_faithfulness_rich_sequence(tmp_path, monkeypatch, window_bps, bin_bps, interval_s, staleness_s):
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    updates = _rich_updates()
    _write_lake_like_recorder(str(tmp_path), updates)
    got = replay_snapshots(str(tmp_path), 0.0, 1000.0, window_bps=window_bps, bin_bps=bin_bps,
                           interval_s=interval_s, staleness_s=staleness_s)
    want = _cadence(updates, interval_s=interval_s, window_bps=window_bps, bin_bps=bin_bps,
                    staleness_s=staleness_s, specs=_SPECS)
    assert [_norm(s) for s in got] == [_norm(s) for s in want]
    # Sanity: a USDT venue actually contributed somewhere (peg path exercised)
    assert any("binance" in s.venues_active for s in got)
    # Sanity: at least one snapshot dropped a stale venue (coinbase gone by t=20 area)
    assert any("coinbase" not in s.venues_active and "kraken" in s.venues_active for s in got)


def test_faithfulness_intratick_order_invariant(tmp_path, monkeypatch):
    """Reordering the intra-tick (same-ts) venue rows in the lake must NOT change the
    cadence snapshots. replay sorts by (ts, exchange, seq_no); cadence is invariant to
    intra-tick venue ordering. We compare two lakes with the same-ts updates permuted."""
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    base_updates = _rich_updates()

    a = tmp_path / "a"; b = tmp_path / "b"
    _write_lake_like_recorder(str(a), base_updates)

    # Permute ONLY the t=2.3 same-ts cluster across venues (kraken/binance interleave).
    # seq_no per exchange is still monotonic within each exchange (recorder invariant),
    # so the per-venue ordering is preserved; only inter-venue interleave changes.
    perm = [base_updates[i] for i in (2, 1, 0, 4, 3, 5, 6, 7)]
    _write_lake_like_recorder(str(b), perm)

    ga = replay_snapshots(str(a), 0.0, 1000.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=10.0)
    gb = replay_snapshots(str(b), 0.0, 1000.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=10.0)
    assert [_norm(s) for s in ga] == [_norm(s) for s in gb]


@pytest.mark.parametrize("batch", [1, 2, 3, 7])
def test_batch_boundary_grouping(tmp_path, monkeypatch, batch):
    """HEADLINE: a single BookUpdate's rows must reconstruct WHOLE even when they span a
    fetchmany batch boundary. Compare tiny _BATCH vs a huge batch; snapshots identical."""
    monkeypatch.setattr(replay_mod, "_SPECS_FN", lambda: list(_SPECS))
    updates = _rich_updates()
    _write_lake_like_recorder(str(tmp_path), updates)

    # Reference: huge batch (no boundary splits within an update).
    monkeypatch.setattr(replay_mod, "_BATCH", 1_000_000)
    big = replay_snapshots(str(tmp_path), 0.0, 1000.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=10.0)

    # Attack: tiny batch so multi-row updates straddle batch boundaries.
    monkeypatch.setattr(replay_mod, "_BATCH", batch)
    small = replay_snapshots(str(tmp_path), 0.0, 1000.0, window_bps=300.0, bin_bps=5.0, interval_s=1.0, staleness_s=10.0)

    assert [_norm(s) for s in small] == [_norm(s) for s in big], f"batch={batch} split an update"
    # And both match the cadence contract.
    want = _cadence(updates, interval_s=1.0, window_bps=300.0, bin_bps=5.0, staleness_s=10.0, specs=_SPECS)
    assert [_norm(s) for s in big] == [_norm(s) for s in want]
