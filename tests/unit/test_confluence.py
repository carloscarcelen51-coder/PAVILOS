# tests/unit/test_confluence.py
from pavilos.detection.models import Zone, Side, DepthAnalysis
from pavilos.detection.confluence import analyze_confluence, ConfluenceConfig

# REAL model (verified src/pavilos/detection/models.py):
#   Side = Enum SUPPORT="support" | RESISTANCE="resistance"
#   Zone(side, price, low, high, strength, venues, persistence_s, pulled, confidence)
#   DepthAnalysis(ts, mid, supports, resistances)   # NO atr field


def _z(side, low, high, conf, venues, persistence_s=30.0):
    return Zone(side=side, price=(low + high) / 2, low=low, high=high, strength=1.0,
                venues=tuple(venues), persistence_s=persistence_s, pulled=False, confidence=conf)


def _analysis(mid, supports, resistances=()):
    return DepthAnalysis(ts=1.0, mid=mid, supports=tuple(supports), resistances=tuple(resistances))


def test_merges_supports_within_band_and_unions_venues():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0,
                           threshold=0.0, min_venues=1)
    # two supports ~5bps apart at 63000 -> one cluster; venues union {k,b,o,x}
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.7, ("k", "b")),
                            _z(Side.SUPPORT, 62975, 62980, 0.6, ("o", "x"))])
    clusters = analyze_confluence(a, cfg)
    sup = [c for c in clusters if c.side == Side.SUPPORT]
    assert len(sup) == 1
    c = sup[0]
    assert c.n_zones == 2 and set(c.venues) == {"k", "b", "o", "x"} and c.n_venues == 4
    assert c.price_lo == 62975 and c.price_hi == 62985
    assert 0.0 <= c.score <= 1.0


def test_distant_supports_are_separate_clusters():
    cfg = ConfluenceConfig(confluence_band_bps=5.0, venues_target=8.0, threshold=0.0, min_venues=1)
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62900, 62905, 0.7, ("k",)),
                            _z(Side.SUPPORT, 62000, 62005, 0.7, ("b",))])  # ~140bps apart
    assert len([c for c in analyze_confluence(a, cfg) if c.side == Side.SUPPORT]) == 2


def test_score_rises_with_venues_and_stacking():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0, threshold=0.0, min_venues=1)
    weak = analyze_confluence(_analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.6, ("k",))]), cfg)[0]
    strong = analyze_confluence(_analysis(63000.0, [
        _z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b", "o", "x", "g")),
        _z(Side.SUPPORT, 62976, 62981, 0.8, ("m", "h"))]), cfg)[0]
    assert strong.score > weak.score


def test_tradeable_gate_filters_by_threshold_and_venues():
    cfg = ConfluenceConfig(confluence_band_bps=15.0, venues_target=8.0, threshold=0.6, min_venues=6)
    a = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b", "o", "x", "g", "m", "h"))])
    clusters = analyze_confluence(a, cfg)
    assert clusters[0].tradeable is True
    a2 = _analysis(63000.0, [_z(Side.SUPPORT, 62980, 62985, 0.9, ("k", "b"))])  # only 2 venues
    assert analyze_confluence(a2, cfg)[0].tradeable is False
