# tests/unit/test_clusters.py
from pavilos.core.models import DepthBin
from pavilos.detection.walls import WallBin
from pavilos.detection.clusters import cluster_walls, RawZone


def _wall(price, size, venues=("kraken",)):
    comp = {v: size / len(venues) for v in venues}
    return WallBin(bin=DepthBin(price=price, size=size, composition=comp), prominence=size)


def test_isolated_wall_is_its_own_zone():
    zones = cluster_walls([_wall(100.0, 5.0)], mid=101.0, max_gap_bps=50.0)
    assert len(zones) == 1
    z = zones[0]
    assert isinstance(z, RawZone)
    assert z.low == 100.0 and z.high == 100.0 and z.strength == 5.0
    assert z.price == 100.0 and z.venues == ("kraken",)


def test_adjacent_walls_merge_into_one_zone_strength_weighted_price():
    # two walls $0.05 apart; gap in bps from mid=101 ~ (0.05/101)*1e4 ~ 4.95 bps < 20
    walls = [_wall(100.0, 2.0, ("kraken",)), _wall(99.95, 6.0, ("binance",))]
    zones = cluster_walls(walls, mid=101.0, max_gap_bps=20.0)
    assert len(zones) == 1
    z = zones[0]
    assert z.low == 99.95 and z.high == 100.0
    assert z.strength == 8.0
    # strength-weighted price = (100.0*2 + 99.95*6)/8
    assert abs(z.price - (100.0 * 2.0 + 99.95 * 6.0) / 8.0) < 1e-9
    assert set(z.venues) == {"kraken", "binance"}


def test_far_apart_walls_stay_separate():
    walls = [_wall(100.0, 5.0), _wall(95.0, 5.0)]
    zones = cluster_walls(walls, mid=101.0, max_gap_bps=20.0)  # ~495 bps apart >> 20
    assert len(zones) == 2
