# tests/unit/test_replay.py
from pathlib import Path

import pytest

from pavilos.core.models import VenueSpec, Quote, Tier
from pavilos.aggregator.normalize import PegProvider
from pavilos.aggregator.aggregator import Aggregator
from scripts.replay import load_updates, replay

FIXTURE = Path(__file__).parent.parent / "fixtures" / "replay_two_venues.jsonl"


def _agg():
    specs = [VenueSpec("kraken", Quote.USD, Tier.A), VenueSpec("coinbase", Quote.USD, Tier.A)]
    return Aggregator(specs, PegProvider(), bin_bps=100.0, window_bps=200.0, staleness_s=60.0)


def test_load_updates_parses_jsonl():
    updates = load_updates(FIXTURE)
    assert len(updates) == 3
    assert updates[0].exchange == "kraken"
    assert updates[0].is_snapshot is True
    assert updates[0].bids[0] == (100.0, 1.0)
    assert updates[2].is_snapshot is False


def test_replay_produces_expected_final_snapshot():
    snap = replay(_agg(), load_updates(FIXTURE), now=100.0)
    assert snap is not None
    assert snap.mid == pytest.approx(100.5)
    # kraken bid 100.0 was updated to 1.5; coinbase 100.0 still 0.5 -> bin -1 size 2.0
    top_bid = max(snap.bids, key=lambda b: b.price)
    assert top_bid.size == pytest.approx(2.0)
    assert top_bid.composition == {"kraken": pytest.approx(1.5), "coinbase": pytest.approx(0.5)}
