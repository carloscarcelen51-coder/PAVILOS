# src/pavilos/aggregator/normalize.py
"""Convert prices from a quote currency to USD using live-updatable rates."""
from __future__ import annotations

import math

from pavilos.core.models import Quote


class PegProvider:
    """Holds ``quote -> USD`` multipliers.

    USD-pegged stablecoins default to 1.0 (overridable with live peg readings,
    e.g. from a USDT/USD market). FX quotes (KRW/JPY/EUR) have no default and
    must be set explicitly via :meth:`set_rate` before conversion, otherwise
    :meth:`to_usd` raises ``ValueError``.
    """

    def __init__(self, rates: dict[Quote, float] | None = None) -> None:
        self._rates: dict[Quote, float] = {
            Quote.USD: 1.0,
            Quote.USDT: 1.0,
            Quote.USDC: 1.0,
        }
        if rates:
            for quote, rate in rates.items():
                self.set_rate(quote, rate)

    def set_rate(self, quote: Quote, rate: float) -> None:
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError(f"rate for {quote} must be a positive finite number, got {rate}")
        self._rates[quote] = rate

    def to_usd(self, price: float, quote: Quote) -> float:
        try:
            return price * self._rates[quote]
        except KeyError:
            raise ValueError(f"no USD conversion rate set for quote {quote}") from None
