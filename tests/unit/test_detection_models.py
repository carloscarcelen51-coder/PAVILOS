# tests/unit/test_detection_models.py
import dataclasses
import pytest

from pavilos.detection.models import Side, Zone, DepthAnalysis


def test_side_values():
    assert Side.SUPPORT.value == "support"
    assert Side.RESISTANCE.value == "resistance"


def test_zone_is_immutable_and_holds_fields():
    z = Zone(side=Side.SUPPORT, price=100.0, low=99.5, high=100.5, strength=12.0,
             venues=("kraken", "binance"), persistence_s=4.0, pulled=False, confidence=0.7)
    assert z.price == 100.0 and z.strength == 12.0
    assert z.venues == ("kraken", "binance")
    with pytest.raises(dataclasses.FrozenInstanceError):
        z.price = 1.0  # type: ignore[misc]


def test_depth_analysis_holds_sorted_zones():
    s = Zone(Side.SUPPORT, 100.0, 99.5, 100.5, 12.0, ("kraken",), 4.0, False, 0.7)
    r = Zone(Side.RESISTANCE, 101.0, 100.8, 101.2, 9.0, ("binance",), 2.0, False, 0.5)
    a = DepthAnalysis(ts=5.0, mid=100.5, supports=(s,), resistances=(r,))
    assert a.supports[0].confidence == 0.7
    assert a.mid == 100.5
