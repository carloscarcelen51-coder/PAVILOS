# src/pavilos/backtest/lake_check.py
"""Self-consistency check of the recorded lake (M17).

Every checkpoint in the lake is a full copy of the aggregator's LIVE book, so when a
venue's rows are replayed in lake order, the book rebuilt just BEFORE a checkpoint
must be IDENTICAL to it: every level, and the ts of the last update applied equal to
the checkpoint's ts. An unexplained ``mismatch`` means rows went missing between two
checkpoints -- a queue-full drop (logged by the recorder) or a bug. The checkpoint
re-bases the replay, so every check is independent of the previous one.

When the recorder KNOWS rows were lost (a failed write or encode, a failed batch, a
pause), the whole book it writes next carries a LOSS MARK (one non-finite level the
replay ignores): that group is always compared, and a difference there is counted as
``loss_mismatches`` -- expected, already reported by the recorder -- not as an
unexplained mismatch. Other full groups that are not checkpoints are counted as
``bases``, not checked: the first one of a venue in the window or after a recorder
start marked in ``_runs.jsonl`` (all books are reset there, as in the replay; a
``seq_no`` back to 0 counts as a restart too, for unmarked lakes), and a venue's first
write after more than ``rebase_after_s`` of silence (written whole; the book may
legitimately have changed meanwhile). Limitation: queue-full drops lasting longer than
``rebase_after_s`` that end at a checkpoint look like a silence and count as a base --
the recorder's drop counter and log report them. Deltas before a venue's first base
are ``unbased`` and skipped. Rows are read in the replay's order (ts, exchange,
seq_no); within a run any backwards ``seq_no`` means the replay applies that venue's
updates in a different order than they were written -- counted as ``reordered``
(expected 0: the recorder never lets a venue's ts go backwards).

Meaningful only for data written by the M17 recorder: before it, ccxt venues stored
every message as a full book, so each of those would be "checked" against a book that
legitimately changed since the previous message."""
from __future__ import annotations

import math

from pavilos.aggregator.book_state import BookState
from pavilos.backtest.replay import iter_lake
from pavilos.persistence.runs import run_starts

_MAX_EXAMPLES = 5


def _venue() -> dict:
    return {"checks": 0, "mismatches": 0, "loss_mismatches": 0, "bases": 0, "unbased": 0,
            "reordered": 0, "loss_marks": 0, "updates": 0, "examples": []}


def _differing(a: dict, b: dict) -> int:
    return sum(1 for p in set(a) | set(b) if a.get(p) != b.get(p))


def check_checkpoints(base_dir: str, t0: float, t1: float, *,
                      rebase_after_s: float = 15.0) -> dict[str, dict]:
    """Per venue: ``checks`` (full groups compared), ``mismatches`` (unexplained),
    ``loss_mismatches`` (at loss-marked groups), ``bases``, ``unbased`` deltas,
    ``reordered`` groups, ``loss_marks``, ``updates`` (all groups) and up to 5 examples
    of unexplained mismatches (ts, levels differing, last ts replayed vs checkpoint ts).
    ``rebase_after_s`` must match the recorder's (the aggregator's staleness_s)."""
    out: dict[str, dict] = {}
    books: dict[str, BookState | None] = {}
    last_seq: dict[str, int] = {}
    starts = run_starts(base_dir, t0, t1)
    si = 0
    for u in iter_lake(base_dir, t0, t1):
        while si < len(starts) and starts[si] <= u.ts:   # a new recorder run: books start empty
            books.clear()
            last_seq.clear()
            si += 1
        seq = u.seq
        v = out.setdefault(u.exchange, _venue())
        v["updates"] += 1
        prev = last_seq.get(u.exchange)
        restarted = prev is not None and seq == 0
        if prev is not None and seq <= prev and not restarted:
            v["reordered"] += 1
        last_seq[u.exchange] = seq
        book = None if restarted else books.get(u.exchange)
        if u.is_snapshot:
            target = BookState(u.exchange)
            target.apply(u)                       # the book as the replay sees it (mark dropped)
            lost = any(not math.isfinite(p) for p, _ in u.bids)
            v["loss_marks"] += lost
            if book is None or (not lost and u.ts - book.last_ts > rebase_after_s):
                v["bases"] += 1
            else:
                v["checks"] += 1
                diff = _differing(book.bids(), target.bids()) + _differing(book.asks(), target.asks())
                if diff or book.last_ts != u.ts:
                    if lost:
                        v["loss_mismatches"] += 1
                    else:
                        v["mismatches"] += 1
                        if len(v["examples"]) < _MAX_EXAMPLES:
                            v["examples"].append({"ts": u.ts, "levels_differing": diff,
                                                  "replayed_last_ts": book.last_ts})
            book = target
        elif book is None:
            v["unbased"] += 1
        else:
            book.apply(u)
        books[u.exchange] = book
    return out
