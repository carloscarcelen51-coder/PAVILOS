# tests/unit/test_walls.py
from pavilos.core.models import DepthBin
from pavilos.detection.walls import detect_walls, WallBin


def _bin(price, size):
    return DepthBin(price=price, size=size, composition={"kraken": size})


def test_detects_bin_above_median_multiple():
    bins = [_bin(100.0, 1.0), _bin(99.0, 1.0), _bin(98.0, 10.0), _bin(97.0, 1.0)]
    walls = detect_walls(bins, size_multiple=3.0, min_size=0.0)
    assert len(walls) == 1
    assert isinstance(walls[0], WallBin)
    assert walls[0].bin.price == 98.0
    assert walls[0].prominence == 10.0  # 10.0 / median(1,1,10,1)=1.0


def test_min_size_floor_filters_thin_books():
    bins = [_bin(100.0, 0.001), _bin(99.0, 0.001), _bin(98.0, 0.005)]
    # 0.005 is 5x the median (0.001) but below the absolute floor -> not a wall
    assert detect_walls(bins, size_multiple=3.0, min_size=0.01) == []


def test_empty_or_uniform_book_has_no_walls():
    assert detect_walls([], size_multiple=3.0, min_size=0.0) == []
    uniform = [_bin(100.0, 5.0), _bin(99.0, 5.0), _bin(98.0, 5.0)]
    assert detect_walls(uniform, size_multiple=3.0, min_size=0.0) == []  # none exceeds 3x median
