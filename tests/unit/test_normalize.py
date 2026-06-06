# tests/unit/test_normalize.py
import pytest

from pavilos.core.models import Quote
from pavilos.aggregator.normalize import PegProvider


def test_usd_pegged_default_to_one():
    peg = PegProvider()
    assert peg.to_usd(100.0, Quote.USD) == 100.0
    assert peg.to_usd(100.0, Quote.USDT) == 100.0
    assert peg.to_usd(100.0, Quote.USDC) == 100.0


def test_set_rate_applies_to_conversion():
    peg = PegProvider()
    peg.set_rate(Quote.USDT, 0.999)            # USDT trading slightly below peg
    assert peg.to_usd(100_000.0, Quote.USDT) == pytest.approx(99_900.0)


def test_fx_quotes_require_explicit_rate():
    peg = PegProvider()
    with pytest.raises(ValueError):
        peg.to_usd(140_000_000.0, Quote.KRW)   # no KRW rate set
    peg.set_rate(Quote.KRW, 0.00072)
    assert peg.to_usd(140_000_000.0, Quote.KRW) == pytest.approx(100_800.0)


def test_rate_must_be_positive():
    peg = PegProvider()
    with pytest.raises(ValueError):
        peg.set_rate(Quote.JPY, 0.0)


def test_rate_must_be_finite():
    peg = PegProvider()
    with pytest.raises(ValueError):
        peg.set_rate(Quote.USDT, float("nan"))
    with pytest.raises(ValueError):
        peg.set_rate(Quote.USDT, float("inf"))
