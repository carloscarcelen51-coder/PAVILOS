# src/pavilos/connectors/peg_feed.py
"""Live USD value of USDT and USDC for the aggregator.

Nine of the 14 venues quote BTC in USDT, and the PegProvider fixed USDT = 1.0 USD.
USDT trades below the dollar (-12.1 bp on 2026-10-23; between about -21 and +16 bp in two
years), so the USDT books sat ~12 bp above the USD ones in the combined book: a band of
artificial depth right around the price. This feed reads Kraken's spot USDT/USD and
USDC/USD tickers (public REST) every ``interval_s`` and pushes the mids into the
PegProvider, which every combined-book build reads. Each reading is also appended to
``<lake>/_pegs.jsonl`` so a replay of the raw (unconverted) lake can apply the same
rates. A failed or implausible reading keeps the last good rate."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections.abc import Awaitable, Callable

import aiohttp

from pavilos.aggregator.normalize import PegProvider
from pavilos.core.models import Quote

_log = logging.getLogger(__name__)

KRAKEN_TICKER = "https://api.kraken.com/0/public/Ticker?pair=USDTUSD,USDCUSD"
PEGS_FILE = "_pegs.jsonl"
BOUNDS = (0.95, 1.05)          # outside this a stablecoin reading is treated as bad data
_LOG_MOVE_BPS = 5.0            # log a rate again only after it moved this much


def parse_kraken_ticker(body: dict) -> dict[str, float]:
    """{"USDT": mid, "USDC": mid} from a Kraken /0/public/Ticker response (keys like
    USDTZUSD / USDCUSD; mid = (best ask + best bid) / 2)."""
    if body.get("error"):
        raise RuntimeError(f"kraken ticker error: {body['error']}")
    out: dict[str, float] = {}
    for key, t in (body.get("result") or {}).items():
        mid = (float(t["a"][0]) + float(t["b"][0])) / 2.0
        if key.startswith("USDT"):
            out["USDT"] = mid
        elif key.startswith("USDC"):
            out["USDC"] = mid
    return out


async def fetch_kraken() -> dict[str, float]:
    # aiodns is unreliable on this host: stdlib ThreadedResolver, as the other connectors.
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as s:
        async with s.get(KRAKEN_TICKER) as r:
            r.raise_for_status()
            return parse_kraken_ticker(await r.json(content_type=None))


class PegFeed:
    def __init__(self, peg: PegProvider, *, lake_dir: str | None, interval_s: float = 30.0,
                 fetch: Callable[[], Awaitable[dict[str, float]]] = fetch_kraken,
                 now: Callable[[], float] = time.time) -> None:
        if not interval_s > 0:
            raise ValueError("interval_s must be positive")
        self._peg = peg
        self._lake_dir = lake_dir
        self._interval = interval_s
        self._fetch = fetch
        self._now = now
        self._fails = 0
        self._logged: dict[str, float] = {}
        self.last: dict | None = None        # {"ts", "USDT", "USDC"} of the last applied reading

    async def refresh_once(self) -> bool:
        """One reading; True if at least one rate was applied. Never raises."""
        try:
            rates = await self._fetch()
        except Exception as e:
            self._fails += 1
            if self._fails == 1 or self._fails % 20 == 0:
                _log.warning("peg: cannot read USDT/USDC from Kraken (%s; %d in a row); keeping %s",
                             e, self._fails, self._describe())
            return False
        applied: dict[str, float] = {}
        for name, quote in (("USDT", Quote.USDT), ("USDC", Quote.USDC)):
            r = rates.get(name)
            if r is None:
                continue
            if isinstance(r, bool) or not (isinstance(r, (int, float)) and math.isfinite(r)
                                           and BOUNDS[0] <= r <= BOUNDS[1]):
                _log.warning("peg: implausible %s/USD reading %r ignored; keeping %s", name, r, self._describe())
                continue
            self._peg.set_rate(quote, r)
            applied[name] = r
        if not applied:
            return False
        ts = self._now()
        if self._fails:
            _log.warning("peg: Kraken readings back after %d failures", self._fails)
        self._fails = 0
        self.last = {"ts": ts, **applied}
        moved = [n for n, r in applied.items()
                 if n not in self._logged or abs(r - self._logged[n]) * 1e4 >= _LOG_MOVE_BPS]
        if moved:
            _log.info("peg: %s", ", ".join(f"{n} {applied[n]:.6f} ({(applied[n] - 1) * 1e4:+.1f} bp)" for n in moved))
            self._logged.update({n: applied[n] for n in moved})
        self._append(ts, applied)
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.refresh_once()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def _describe(self) -> str:
        if self.last is None:
            return "the previous rates (1.0 until a first reading)"
        return "the last reading from " + time.strftime("%H:%M:%SZ", time.gmtime(self.last["ts"]))

    def _append(self, ts: float, applied: dict[str, float]) -> None:
        if not self._lake_dir:
            return
        try:
            os.makedirs(self._lake_dir, exist_ok=True)
            with open(os.path.join(self._lake_dir, PEGS_FILE), "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": round(ts, 3), **applied, "src": "kraken"}) + "\n")
        except OSError as e:
            _log.warning("peg: cannot append to %s (%s)", PEGS_FILE, e)
