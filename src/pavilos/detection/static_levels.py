# src/pavilos/detection/static_levels.py
"""StaticLevelTracker: causal detection of FIXED-price multi-venue walls.

The M13 finding was that detected "supports" were really the *near-touch* — the
best bid trailing a few bps below a drifting mid. A genuine **static** level sits
at a FIXED absolute price that price has moved AWAY from (and may return to). This
tracker accrues wall presence per absolute price bucket over time, records each
bucket's ``max_away_bps`` (the max distance from mid over its life — the
near-touch discriminator), prunes stale buckets causally, and exposes the active
static supports/resistances: strong, multi-venue, price-has-been-away, and within
the ``[min_away_bps, max_reach_bps]`` band of mid.

Semantics
---------
- **Causal accrual:** presence accrues only from past ``update`` calls; pruning
  drops buckets not refreshed within ``stale_s``. Querying at ``now`` uses only
  data fed up to ``now``.
- **Near-touch discriminator:** a bucket's ``max_away_bps`` is the max over its
  life of ``|L - mid| / mid * 1e4``. A static level has been ``>= min_away_bps``
  from price (price left it); the near-touch never gets that far → excluded. In a
  moving market the near-touch smears across many buckets while a static level
  accrues at one fixed bucket.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from pavilos.detection.models import Side
from pavilos.detection.walls import detect_walls

#: Defense-consistency normaliser for the presence term of ``strength`` — how many
#: refresh ticks count as "fully defended". Keeps strength interpretable without a
#: refresh-rate estimate (presence is per-update, so this is a tick budget).
_PRESENCE_CAP: float = 30.0


@dataclass(frozen=True)
class StaticLevelConfig:
    """Tuning for :class:`StaticLevelTracker`.

    ``level_bucket_usd`` is the absolute-price bucket width; ``size_multiple`` is
    the wall threshold vs the snapshot's median bin size; ``stale_s`` prunes
    buckets not refreshed within it; ``min_venues`` / ``level_threshold`` gate an
    active level by breadth / strength; ``min_away_bps`` is the near-touch
    discriminator (a level must have been at least this far from mid); a level is
    only active within ``[min_away_bps, max_reach_bps]`` of the current mid;
    ``venues_target`` / ``duration_target_s`` normalise the strength score.
    """

    level_bucket_usd: float
    size_multiple: float
    stale_s: float
    min_venues: int
    level_threshold: float
    min_away_bps: float
    max_reach_bps: float
    venues_target: float
    duration_target_s: float


@dataclass(frozen=True)
class StaticLevel:
    """A detected static support/resistance at a fixed absolute price.

    ``side`` reuses the detection :class:`Side` enum (SUPPORT below mid /
    RESISTANCE above mid); ``strength`` is 0..1; ``venues`` is the union of
    contributing exchanges over the level's life; ``presence`` is the number of
    snapshots the wall was seen in; ``duration_s`` is ``last_seen - first_seen``;
    ``max_away_bps`` is the max distance from mid over the level's life (the
    near-touch discriminator)."""

    price: float
    side: Side
    strength: float
    venues: tuple[str, ...]
    n_venues: int
    presence: int
    duration_s: float
    max_away_bps: float
    last_seen_ts: float


@dataclass(slots=True)
class _LS:
    """Mutable per-bucket accumulator. Internal to the tracker."""

    first_seen: float
    last_seen: float
    presence: int
    venues: set[str]
    size_sum: float
    max_away_bps: float


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


@dataclass(slots=True)
class StaticLevelTracker:
    """Stateful, causal tracker of fixed-price multi-venue walls."""

    cfg: StaticLevelConfig
    _levels: dict[float, _LS] = field(default_factory=dict)

    def update(self, snapshot) -> None:
        """Accrue wall presence from one combined-book snapshot (causal).

        Flags bins whose size exceeds ``size_multiple`` x the snapshot's median
        bin size, buckets each by absolute price, and accrues presence, the union
        of contributing venues, total size, and ``max_away_bps``. Then prunes any
        bucket not refreshed within ``stale_s`` of ``snapshot.ts``.
        """
        cfg = self.cfg
        now = snapshot.ts
        mid = snapshot.mid

        if mid > 0.0:
            # Use the SAME wall definition as the Detector (detect_walls: size >=
            # size_multiple x the side's median), per side, so a "static level" is
            # built from exactly the walls the rest of the system sees.
            walls = (detect_walls(snapshot.bids, size_multiple=cfg.size_multiple, min_size=0.0)
                     + detect_walls(snapshot.asks, size_multiple=cfg.size_multiple, min_size=0.0))
            for w in walls:
                b = w.bin
                L = round(b.price / cfg.level_bucket_usd) * cfg.level_bucket_usd
                away = abs(L - mid) / mid * 1e4
                ls = self._levels.get(L)
                if ls is None:
                    ls = _LS(first_seen=now, last_seen=now, presence=0,
                             venues=set(), size_sum=0.0, max_away_bps=0.0)
                    self._levels[L] = ls
                ls.presence += 1
                ls.last_seen = now
                ls.venues |= set(b.composition.keys())
                ls.size_sum += b.size
                if away > ls.max_away_bps:
                    ls.max_away_bps = away

        # Causal pruning: drop buckets not refreshed within stale_s.
        stale = [L for L, ls in self._levels.items() if now - ls.last_seen > cfg.stale_s]
        for L in stale:
            del self._levels[L]

    def _strength(self, ls: _LS) -> float:
        """Score a bucket 0..1: venue-dominant breadth + persistence-duration +
        defense consistency (presence). Interpretable, monotone in each term."""
        cfg = self.cfg
        n_venues = len(ls.venues)
        duration_s = ls.last_seen - ls.first_seen
        venue_term = min(n_venues / cfg.venues_target, 1.0) if cfg.venues_target > 0 else 0.0
        dur_term = min(duration_s / cfg.duration_target_s, 1.0) if cfg.duration_target_s > 0 else 0.0
        presence_term = min(ls.presence / _PRESENCE_CAP, 1.0)
        return _clamp01(0.5 * venue_term + 0.3 * dur_term + 0.2 * presence_term)

    def _to_level(self, L: float, ls: _LS, side: Side) -> StaticLevel:
        return StaticLevel(
            price=L,
            side=side,
            strength=self._strength(ls),
            venues=tuple(sorted(ls.venues)),
            n_venues=len(ls.venues),
            presence=ls.presence,
            duration_s=ls.last_seen - ls.first_seen,
            max_away_bps=ls.max_away_bps,
            last_seen_ts=ls.last_seen,
        )

    def _active(self, mid: float, now: float, *, below: bool) -> list[StaticLevel]:
        cfg = self.cfg
        if mid <= 0.0:
            return []
        out: list[StaticLevel] = []
        side = Side.SUPPORT if below else Side.RESISTANCE
        for L, ls in self._levels.items():
            if below and not (L < mid):
                continue
            if (not below) and not (L > mid):
                continue
            away = abs(mid - L) / mid * 1e4
            if away > cfg.max_reach_bps:        # too far to be reachable
                continue
            if len(ls.venues) < cfg.min_venues:
                continue
            # near-touch discriminator: the level must have been >= min_away_bps from
            # price at SOME point in its life (a static level price LEFT), not the bid
            # that always trails ~2bps. We do NOT bound the CURRENT distance below —
            # the bounce setup is exactly when price RETURNS close to the static level.
            if ls.max_away_bps < cfg.min_away_bps:
                continue
            level = self._to_level(L, ls, side)
            if level.strength < cfg.level_threshold:
                continue
            out.append(level)
        out.sort(key=lambda s: s.strength, reverse=True)
        return out

    def active_supports(self, mid: float, now: float) -> list[StaticLevel]:
        """Active static supports (buckets below ``mid``) sorted by strength desc.

        Gated by strength ``>= level_threshold``, ``n_venues >= min_venues``,
        ``max_away_bps >= min_away_bps`` (price has been away over the level's life
        → static, not the near-touch), and current distance ``<= max_reach_bps``.
        The current distance is NOT bounded below: the bounce setup is exactly when
        price RETURNS close to the static level.
        """
        return self._active(mid, now, below=True)

    def active_resistances(self, mid: float, now: float) -> list[StaticLevel]:
        """Active static resistances (buckets above ``mid``); mirror of
        :meth:`active_supports`."""
        return self._active(mid, now, below=False)
