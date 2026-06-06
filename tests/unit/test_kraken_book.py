# tests/unit/test_kraken_book.py
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
