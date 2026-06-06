# tests/unit/test_kraken_connector.py
import asyncio

from pavilos.connectors.kraken import book_checksum
from pavilos.connectors.kraken_book import KrakenRawBook
from pavilos.connectors.kraken_connector import KrakenConnector


def _frame(mtype, bids, asks):
    """A standalone frame whose checksum is computed over exactly the listed
    levels. Correct for a SNAPSHOT (its levels ARE the whole book); for an
    update use ``_FrameBuilder`` (Kraken's CRC covers the cumulative book)."""
    cs = book_checksum(
        sorted(asks, key=lambda x: float(x[0])),
        sorted(bids, key=lambda x: float(x[0]), reverse=True),
    )
    return {"channel": "book", "type": mtype,
            "data": [{"symbol": "BTC/USD",
                      "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks],
                      "checksum": cs}]}


class _FrameBuilder:
    """Builds a sequence of Kraken ``book`` frames whose checksum reflects the
    FULL cumulative top-10 book after each frame (real Kraken v2 semantics: the
    CRC32 is over the whole book, not just the delta levels in the message). A
    correct connector verifies these and emits the matching BookUpdates."""

    def __init__(self, symbol: str = "BTC/USD", depth: int = 10) -> None:
        self._ref = KrakenRawBook(symbol, depth=depth)
        self._symbol = symbol

    def make(self, mtype, bids, asks):
        msg = {"channel": "book", "type": mtype,
               "data": [{"symbol": self._symbol,
                         "bids": [{"price": p, "qty": q} for p, q in bids],
                         "asks": [{"price": p, "qty": q} for p, q in asks],
                         "checksum": 0}]}
        self._ref.apply(msg)
        msg["data"][0]["checksum"] = self._ref.checksum()
        return msg


def _run(coro):
    return asyncio.run(coro)


def test_emits_bookupdates_and_skips_non_book_frames():
    fb = _FrameBuilder()
    frames = [
        {"channel": "status", "type": "update", "data": []},   # must be skipped
        fb.make("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")]),
        # update: bid 100.0 -> 1.5; the ask is retained, so the cumulative-book
        # checksum (what the connector recomputes) covers both sides.
        fb.make("update", bids=[("100.0", "1.5")], asks=[]),
    ]

    async def fake_connect():
        async def gen():
            for f in frames:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = KrakenConnector("BTC/USD", connect=fake_connect, now=lambda: 1.0,
                               sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u1 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        u2 = await asyncio.wait_for(out_q.get(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u1, u2

    u1, u2 = _run(scenario())
    assert u1.exchange == "kraken" and u1.is_snapshot is True
    assert u1.bids == ((100.0, 1.0),)
    assert u2.is_snapshot is False and u2.bids == ((100.0, 1.5),)


def test_checksum_mismatch_triggers_reconnect():
    bad = _frame("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")])
    bad["data"][0]["checksum"] = 1  # deliberately wrong
    good = _frame("snapshot", bids=[("100.0", "1.0")], asks=[("101.0", "2.0")])
    sessions = [[bad], [good]]   # first session bad -> reconnect -> second good
    calls = {"n": 0}

    async def fake_connect():
        i = calls["n"]
        calls["n"] += 1
        session = sessions[min(i, len(sessions) - 1)]

        async def gen():
            for f in session:
                yield f
        return gen()

    async def scenario():
        out_q: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        conn = KrakenConnector("BTC/USD", connect=fake_connect, now=lambda: 1.0,
                               sleep=lambda d: asyncio.sleep(0), max_backoff=0.0)
        task = asyncio.create_task(conn.run(out_q, stop))
        u = await asyncio.wait_for(out_q.get(), timeout=1.0)  # from the GOOD session
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        return u, calls["n"]

    u, n = _run(scenario())
    assert u.is_snapshot is True
    assert n >= 2   # reconnected at least once after the bad checksum
