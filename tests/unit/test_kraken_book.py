# tests/unit/test_kraken_book.py
from decimal import Decimal

from pavilos.connectors.kraken_book import KrakenRawBook
from pavilos.connectors.kraken import book_checksum


def _snap(bids, asks, checksum=0):
    return {"channel": "book", "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks], "checksum": checksum}]}


def _upd(bids, asks, checksum=0):
    return {"channel": "book", "type": "update",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": p, "qty": q} for p, q in bids],
                      "asks": [{"price": p, "qty": q} for p, q in asks], "checksum": checksum}]}


def test_snapshot_checksum_matches_book_checksum():
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[("100.0", "1.5"), ("99.5", "3.0")], asks=[("100.5", "2.0"), ("101.0", "0.5")]))
    expected = book_checksum([("100.5", "2.0"), ("101.0", "0.5")], [("100.0", "1.5"), ("99.5", "3.0")])
    assert book.checksum() == expected
    assert book.verify(expected) is True
    assert book.verify(expected ^ 0xFF) is False


def test_update_applies_and_removes_then_rechecksums():
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")]))
    book.apply(_upd(bids=[("100.0", "0")], asks=[("101.0", "2.5"), ("101.5", "1.0")]))
    expected = book_checksum([("101.0", "2.5"), ("101.5", "1.0")], [])
    assert book.checksum() == expected


def test_truncates_each_side_to_depth():
    book = KrakenRawBook("BTC/USD", depth=2)
    book.apply(_snap(
        bids=[("100.0", "1"), ("99.0", "1"), ("98.0", "1")],
        asks=[("101.0", "1"), ("102.0", "1"), ("103.0", "1")],
    ))
    expected = book_checksum([("101.0", "1"), ("102.0", "1")], [("100.0", "1"), ("99.0", "1")])
    assert book.checksum() == expected


def test_preserves_full_precision_unlike_float():
    # A trailing-zero price/qty that float() would collapse -- this is WHY
    # KrakenRawBook keeps strings, not floats. The checksum over the exact wire
    # strings must match book_checksum AND must differ from the float-collapsed one.
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[("0.00100000", "1.50000000")], asks=[("0.00200000", "2.00000000")]))
    exact = book_checksum([("0.00200000", "2.00000000")], [("0.00100000", "1.50000000")])
    assert book.checksum() == exact
    collapsed = book_checksum(
        [(str(float("0.00200000")), str(float("2.00000000")))],
        [(str(float("0.00100000")), str(float("1.50000000")))],
    )
    assert exact != collapsed   # float would mangle the digits -> different checksum


def _ref_checksum(bids: dict, asks: dict) -> int:
    # Reference = the pre-optimization logic: exact Decimal full-sort, top 10.
    a = sorted(asks.items(), key=lambda kv: Decimal(kv[0]))[:10]
    b = sorted(bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[:10]
    return book_checksum([(p, q) for p, q in a], [(p, q) for p, q in b])


def test_checksum_equiv_to_decimal_reference_over_random_deep_book():
    # Prove the heapq/float-ordered + early-return-bound optimization yields the SAME
    # CRC as the original full-Decimal-sort implementation, over a deep (depth=1000)
    # book driven by many random deltas (adds, replaces, removals, growth past depth).
    import random
    rng = random.Random(20260608)
    book = KrakenRawBook("BTC/USD", depth=1000)
    bids: dict[str, str] = {}
    asks: dict[str, str] = {}

    def fmt(x: float) -> str:
        return f"{x:.2f}"

    base = 63000.0
    snap_b = [(fmt(base - i * 0.25), "1.50000000") for i in range(1000)]
    snap_a = [(fmt(base + 0.25 + i * 0.25), "1.50000000") for i in range(1000)]
    book.apply(_snap(snap_b, snap_a))
    for p, q in snap_b:
        bids[p] = q
    for p, q in snap_a:
        asks[p] = q
    assert book.checksum() == _ref_checksum(bids, asks)

    for _ in range(300):
        ub, ua = [], []
        for _ in range(rng.randint(1, 8)):
            # touch a price near the top of book (where the checksum lives) or deeper
            b_price = fmt(base - rng.randint(0, 1200) * 0.25)
            a_price = fmt(base + 0.25 + rng.randint(0, 1200) * 0.25)
            b_qty = "0" if rng.random() < 0.3 else f"{rng.uniform(0.1, 9):.8f}"
            a_qty = "0" if rng.random() < 0.3 else f"{rng.uniform(0.1, 9):.8f}"
            ub.append((b_price, b_qty))
            ua.append((a_price, a_qty))
        book.apply(_upd(ub, ua))
        # mirror into the reference dicts (last write wins; qty 0 removes)
        for p, q in ub:
            bids.pop(p, None) if Decimal(q) == 0 else bids.__setitem__(p, q)
        for p, q in ua:
            asks.pop(p, None) if Decimal(q) == 0 else asks.__setitem__(p, q)
        # the reference is bounded to top-`depth` the same way before comparing top-10
        bids = dict(sorted(bids.items(), key=lambda kv: Decimal(kv[0]), reverse=True)[:1000])
        asks = dict(sorted(asks.items(), key=lambda kv: Decimal(kv[0]))[:1000])
        assert book.checksum() == _ref_checksum(bids, asks)


def test_accepts_decimal_values_from_real_decode():
    # Real frames decode with parse_float=Decimal, so price/qty arrive as Decimal.
    book = KrakenRawBook("BTC/USD", depth=10)
    book.apply(_snap(bids=[(Decimal("100.0"), Decimal("1.5"))], asks=[(Decimal("101.0"), Decimal("2.0"))]))
    expected = book_checksum([("101.0", "2.0")], [("100.0", "1.5")])
    assert book.checksum() == expected
