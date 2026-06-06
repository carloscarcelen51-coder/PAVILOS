# tests/unit/test_kraken.py
import zlib

from pavilos.connectors.kraken import _fmt, _crc32, book_checksum

# Kraken's official worked example (docs.kraken.com spot-ws-book-v2):
# the concatenated top-10 asks (low->high) + top-10 bids (high->low) string
# CRC32s (unsigned 32-bit) to this value.
DOC_COMBINED = (
    "45285210000045286415457195345286615457110945289615456091145290215890660"
    "452918154553491452947445474945296135380000452975994554245299518772827"
    "452835100000004528341545820154528211000000045281010000000452803154592586"
    "452790799000045277633101034527753000000045277315460273745276615445238"
)
DOC_CHECKSUM = 3310070434


def test_fmt_removes_dot_and_strips_leading_zeros():
    assert _fmt("45283.5") == "452835"
    assert _fmt("0.00100000") == "100000"
    assert _fmt("0.5666") == "5666"
    assert _fmt("100.00") == "10000"


def test_crc32_matches_kraken_documented_vector():
    assert _crc32(DOC_COMBINED) == DOC_CHECKSUM


def test_book_checksum_assembles_asks_then_bids_top10_formatted():
    # asks pre-sorted low->high, bids pre-sorted high->low; (price, qty) strings.
    asks = [("100.5", "2.0"), ("101.0", "0.5")]
    bids = [("100.0", "1.5"), ("99.5", "3.0")]
    # expected string: for each ask then each bid, _fmt(price)+_fmt(qty)
    #   100.5/2.0 -> "1005"+"20"="100520"; 101.0/0.5 -> "1010"+"5"="10105"
    #   100.0/1.5 -> "1000"+"15"="100015"; 99.5/3.0 -> "995"+"30"="99530"
    expected_str = "100520" + "10105" + "100015" + "99530"
    expected = zlib.crc32(expected_str.encode("ascii")) & 0xFFFFFFFF
    assert book_checksum(asks, bids) == expected


def test_book_checksum_uses_only_top_10_each_side():
    # 12 asks and 12 bids; only the first 10 of each (already sorted) must count.
    asks = [(f"{100 + i}.0", "1.0") for i in range(12)]
    bids = [(f"{99 - i}.0", "1.0") for i in range(12)]
    full = book_checksum(asks, bids)
    trimmed = book_checksum(asks[:10], bids[:10])
    assert full == trimmed
