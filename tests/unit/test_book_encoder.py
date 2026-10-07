# tests/unit/test_book_encoder.py
"""The recorder's BookEncoder turns the received BookUpdate stream into a compact
write stream. Invariant under test: applying the WRITTEN updates to a fresh
BookState reproduces the LIVE BookState (the one the aggregator holds) after
every single update -- levels always, last_ts whenever the book has a mid."""
import math
import random

from pavilos.aggregator.book_state import BookState
from pavilos.core.models import BookUpdate
from pavilos.persistence.encoder import BookEncoder


def _snap(ts, bids, asks, ex="x", checkpoint=False):
    return BookUpdate(ex, ts, tuple(bids), tuple(asks), True, None, checkpoint=checkpoint)


def _delta(ts, bids, asks, ex="x"):
    return BookUpdate(ex, ts, tuple(bids), tuple(asks), False, None)


def test_first_snapshot_is_written_in_full():
    w = BookEncoder().encode(_snap(1.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
    assert w.is_snapshot is True
    assert dict(w.bids) == {100.0: 1.0, 99.0: 2.0} and dict(w.asks) == {101.0: 3.0}


def test_repeated_snapshot_becomes_a_diff_with_changes_and_removals():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
    w = enc.encode(_snap(2.0, [(100.0, 1.5), (98.0, 4.0)], [(101.0, 3.0)]))
    assert w.is_snapshot is False
    assert dict(w.bids) == {100.0: 1.5, 98.0: 4.0, 99.0: 0.0}   # changed, new, removed
    assert w.asks == ()                                          # unchanged side: nothing
    assert w.ts == 2.0


def test_identical_snapshot_writes_a_one_level_noop_heartbeat():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
    w = enc.encode(_snap(2.0, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
    assert w.is_snapshot is False and w.ts == 2.0
    assert w.bids == ((100.0, 1.0),) and w.asks == ()          # best bid at its CURRENT size


def test_checkpoint_is_always_written_in_full_even_with_a_mirror():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    w = enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)], checkpoint=True))
    assert w.is_snapshot is True and dict(w.bids) == {100.0: 1.0}


def test_delta_is_normalized_last_wins_and_non_finite_dropped():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    w = enc.encode(_delta(2.0, [(99.0, 5.0), (99.0, 0.0), (float("nan"), 1.0)], [(102.0, -1.0)]))
    assert w.is_snapshot is False
    assert w.bids == ((99.0, 0.0),)                              # last write wins (removal)
    assert w.asks == ((102.0, 0.0),)                             # negative = removal, normalized to 0.0


def test_empty_snapshot_after_a_book_writes_removals_of_every_level():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    w = enc.encode(_snap(2.0, [], []))
    assert w.is_snapshot is False
    assert dict(w.bids) == {100.0: 0.0} and dict(w.asks) == {101.0: 0.0}


def test_nothing_to_write_for_an_empty_book_returns_none():
    enc = BookEncoder()
    assert enc.encode(_snap(1.0, [], [])) is None
    assert enc.encode(_delta(2.0, [], [])) is None


def test_venues_are_independent():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)], ex="a"))
    w = enc.encode(_snap(2.0, [(100.0, 1.0)], [(101.0, 1.0)], ex="b"))
    assert w.is_snapshot is True                                 # b has no mirror yet -> full


# --------------------------------------------------------------------------- property

def _rand_levels(rng, base, n_max, *, allow_bad=True):
    out = []
    for _ in range(rng.randint(0, n_max)):
        p = base + rng.randint(-20, 20) * 0.5
        s = rng.choice([0.0, -1.0, 0.25, 1.0, 2.5, rng.random() * 3])
        if allow_bad and rng.random() < 0.03:
            p = rng.choice([float("nan"), float("inf")])
        if allow_bad and rng.random() < 0.03:
            s = float("nan")
        out.append((p, s))
        if rng.random() < 0.05:
            out.append((p, rng.choice([0.0, 1.0, 2.0])))          # duplicate price in one update
    return out


def _check(live, replay):
    assert replay.bids() == live.bids()
    assert replay.asks() == live.asks()
    if live.mid() is not None:
        assert replay.last_ts == live.last_ts


def test_property_replay_of_written_stream_equals_live_book():
    for seed in range(400):
        rng = random.Random(seed)
        live, replay, enc = BookState("x"), BookState("x"), BookEncoder()
        ts = 0.0
        for _ in range(60):
            ts += rng.choice([0.001, 0.2, 1.0, 5.0])
            kind = rng.random()
            if kind < 0.12 and live.mid() is not None:
                # the aggregator checkpoints ACTIVE venues with ts=last_ts; it is NOT applied live
                u = BookUpdate("x", live.last_ts, tuple(live.bids().items()),
                               tuple(live.asks().items()), True, None, checkpoint=True)
            elif kind < 0.45:
                u = _snap(ts, _rand_levels(rng, 100.0, 12), _rand_levels(rng, 111.0, 12))
                live.apply(u)
            else:
                u = _delta(ts, _rand_levels(rng, 100.0, 4), _rand_levels(rng, 111.0, 4))
                live.apply(u)
            w = enc.encode(u)
            if w is not None:
                replay.apply(w)
            _check(live, replay)


def test_checkpoint_heals_a_dropped_update():
    live, replay, enc = BookState("x"), BookState("x"), BookEncoder()
    u0 = _snap(1.0, [(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)])
    live.apply(u0); replay.apply(enc.encode(u0))
    dropped = _delta(2.0, [(99.0, 0.0), (98.0, 3.0)], [])      # the recorder never sees this one
    live.apply(dropped)
    assert replay.bids() != live.bids()                          # diverged
    ck = BookUpdate("x", live.last_ts, tuple(live.bids().items()), tuple(live.asks().items()),
                    True, None, checkpoint=True)
    replay.apply(enc.encode(ck))
    _check(live, replay)                                         # healed by the checkpoint
    nxt = _delta(3.0, [(98.0, 4.0)], [])
    live.apply(nxt); replay.apply(enc.encode(nxt))
    _check(live, replay)                                         # and stays consistent after


def test_written_levels_are_unique_per_side_so_row_order_cannot_matter():
    # Rows of one update come back from the lake in NO guaranteed order; uniqueness per
    # side makes the replay independent of that order.
    rng = random.Random(3)
    enc = BookEncoder()
    for i in range(300):
        u = (_snap if rng.random() < 0.4 else _delta)(float(i), _rand_levels(rng, 100.0, 10),
                                                      _rand_levels(rng, 111.0, 10))
        w = enc.encode(u)
        if w is None:
            continue
        for side in (w.bids, w.asks):
            prices = [p for p, _ in side]
            assert len(prices) == len(set(prices))
            assert all(math.isfinite(p) and math.isfinite(s) for p, s in side)


def test_empty_checkpoint_after_a_book_writes_removals():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 2.0)]))
    ck = BookUpdate("x", 2.0, (), (), True, None, checkpoint=True)
    w = enc.encode(ck)
    assert w == BookUpdate("x", 2.0, ((100.0, 0.0),), ((101.0, 0.0),), False, None)
    assert enc.encode(BookUpdate("x", 3.0, (), (), True, None, checkpoint=True)) is None


# --- order and base rules (barriers 1+2) -------------------------------------------

def test_written_ts_never_goes_backwards_within_a_venue():
    enc = BookEncoder()
    w1 = enc.encode(_snap(10.0, [(100.0, 1.0)], [(101.0, 1.0)]))     # seed on the PC clock
    w2 = enc.encode(_delta(9.7, [(99.0, 2.0)], []))                   # buffered delta, other clock
    w3 = enc.encode(_delta(10.2, [(98.0, 1.0)], []))
    assert [w1.ts, w2.ts, w3.ts] == [10.0, 10.0, 10.2]
    assert enc.encode(_snap(5.0, [(1.0, 1.0)], [(2.0, 1.0)], ex="y")).ts == 5.0   # per venue


def test_first_write_and_write_after_silence_are_the_whole_book():
    enc = BookEncoder(rebase_after_s=15.0)
    first = enc.encode(_delta(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))  # no snapshot seen yet
    assert first.is_snapshot and dict(first.bids) == {100.0: 1.0}
    enc.encode(_delta(2.0, [(99.0, 2.0)], []))
    near = enc.encode(_delta(16.9, [(98.0, 1.0)], []))               # 14.9 s later: a delta
    assert not near.is_snapshot and near.bids == ((98.0, 1.0),)
    after = enc.encode(_delta(32.0, [(97.0, 1.0)], []))              # 15.1 s of silence: rebase
    assert after.is_snapshot and after.ts == 32.0
    assert dict(after.bids) == {100.0: 1.0, 99.0: 2.0, 98.0: 1.0, 97.0: 1.0}
    assert dict(after.asks) == {101.0: 1.0}
    back = enc.encode(_snap(60.0, [(100.0, 1.0)], [(101.0, 1.0)]))   # reconnect after silence
    assert back.is_snapshot and dict(back.bids) == {100.0: 1.0}      # the whole book, not a diff


def test_rebase_never_turns_removals_into_an_empty_snapshot():
    enc = BookEncoder(rebase_after_s=15.0)
    enc.encode(_snap(1.0, [(100.0, 1.0)], []))
    w = enc.encode(_snap(30.0, [], []))                              # emptied after a silence
    assert w == BookUpdate("x", 30.0, ((100.0, 0.0),), (), False, None)


def test_property_with_clock_steps_back_and_silences():
    for seed in range(300):
        rng = random.Random(1000 + seed)
        live, replay, enc = BookState("x"), BookState("x"), BookEncoder(rebase_after_s=15.0)
        ts, hi = 0.0, None
        for _ in range(60):
            ts += rng.choice([0.001, 0.2, 1.0, 5.0, 20.0, -0.3, -2.0])
            kind = rng.random()
            if kind < 0.12 and live.mid() is not None:
                u = BookUpdate("x", live.last_ts, tuple(live.bids().items()),
                               tuple(live.asks().items()), True, None, checkpoint=True)
            elif kind < 0.45:
                u = _snap(ts, _rand_levels(rng, 100.0, 12), _rand_levels(rng, 111.0, 12))
                live.apply(u)
            else:
                u = _delta(ts, _rand_levels(rng, 100.0, 4), _rand_levels(rng, 111.0, 4))
                live.apply(u)
            w = enc.encode(u)
            if w is not None:
                hi = u.ts if hi is None else max(hi, u.ts)
                assert w.ts == hi                                    # never backwards
                replay.apply(w)
            assert replay.bids() == live.bids() and replay.asks() == live.asks()
            if live.mid() is not None:
                assert replay.last_ts == hi


def test_explicit_rebase_makes_the_next_write_whole_once():
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    enc.encode(_snap(1.0, [(1.0, 1.0)], [(2.0, 1.0)], ex="y"))
    enc.rebase("x")                                         # its last rows never reached the lake
    w = enc.encode(_delta(2.0, [(99.0, 2.0)], []))
    assert w.is_snapshot and _finite(w.bids) == {100.0: 1.0, 99.0: 2.0} and _marked(w)
    assert not enc.encode(_delta(3.0, [(98.0, 1.0)], [])).is_snapshot     # only once
    assert not enc.encode(_delta(2.0, [(1.5, 1.0)], [], ex="y")).is_snapshot  # other venue untouched
    enc.rebase()                                            # every venue
    assert enc.encode(_delta(4.0, [(97.0, 1.0)], [])).is_snapshot
    assert enc.encode(_delta(3.0, [(1.4, 1.0)], [], ex="y")).is_snapshot


def _finite(levels):
    return {p: s for p, s in levels if math.isfinite(p)}


def _marked(w):
    return any(not math.isfinite(p) for p, _ in w.bids)


def test_loss_rebase_carries_the_mark_and_silence_rebase_does_not():
    enc = BookEncoder(rebase_after_s=15.0)
    enc.encode(_snap(1.0, [(100.0, 1.0)], [(101.0, 1.0)]))
    after_silence = enc.encode(_delta(30.0, [(99.0, 1.0)], []))
    assert after_silence.is_snapshot and not _marked(after_silence)
    enc.rebase("x")
    after_loss = enc.encode(_delta(31.0, [(98.0, 1.0)], []))
    assert after_loss.is_snapshot and _marked(after_loss)
    assert _finite(after_loss.bids) == {100.0: 1.0, 99.0: 1.0, 98.0: 1.0}
    replay = BookState("x")
    replay.apply(after_loss)
    assert replay.bids() == {100.0: 1.0, 99.0: 1.0, 98.0: 1.0}   # the mark is not a level


def test_an_empty_book_after_a_loss_is_written_as_the_mark_alone():
    """A lost write may have removed levels the lake still holds: an EMPTY book after a
    loss must reach the lake as an explicit empty book, not as nothing / partial removals."""
    enc = BookEncoder()
    enc.encode(_snap(1.0, [(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)]))
    enc.encode(_delta(2.0, [(99.0, 0.0)], []))       # imagine this write was lost...
    enc.rebase("x")
    w = enc.encode(_snap(3.0, [], []))               # ...and now the book is empty
    assert w.is_snapshot and _marked(w) and _finite(w.bids) == {} and w.asks == ()
    replay = BookState("x")
    replay.apply(_snap(1.0, [(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)]))   # lake: lost the delta
    replay.apply(w)
    assert replay.bids() == {} and replay.asks() == {}
    assert enc.encode(_snap(4.0, [], [])) is None    # marked once; nothing more to say


def test_floor_keeps_every_write_at_or_after_the_run_start():
    enc = BookEncoder()
    enc.set_floor(100.0)
    assert enc.encode(_snap(50.0, [(1.0, 1.0)], [(2.0, 1.0)])).ts == 100.0
    assert enc.encode(_delta(101.0, [(1.5, 1.0)], [])).ts == 101.0


def test_a_clock_step_back_is_warned_once_per_episode(caplog):
    import logging
    enc = BookEncoder()
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.encoder"):
        enc.encode(_snap(100.0, [(1.0, 1.0)], [(2.0, 1.0)]))
        enc.encode(_delta(90.0, [(1.5, 1.0)], []))   # 10 s back
        enc.encode(_delta(91.0, [(1.6, 1.0)], []))   # still behind: no second warning
        enc.encode(_delta(101.0, [(1.7, 1.0)], []))  # caught up
        enc.encode(_delta(50.0, [(1.8, 1.0)], []))   # a new episode
    warns = [r for r in caplog.records if "behind what is already written" in r.getMessage()]
    assert len(warns) == 2


def test_a_clock_more_than_a_minute_behind_is_critical(caplog):
    import logging
    enc = BookEncoder()
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.encoder"):
        enc.encode(_snap(1000.0, [(1.0, 1.0)], [(2.0, 1.0)]))
        enc.encode(_delta(990.0, [(1.5, 1.0)], []))   # 10 s back: WARNING
        enc.encode(_delta(900.0, [(1.6, 1.0)], []))   # 100 s back in the same episode: CRITICAL
        enc.encode(_delta(899.0, [(1.7, 1.0)], []))   # still: nothing new
    levels = [r.levelno for r in caplog.records if "behind what is already written" in r.getMessage()]
    assert levels == [logging.WARNING, logging.CRITICAL]
    assert enc.clamp_lead_s == 101.0
