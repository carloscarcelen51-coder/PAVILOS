# tests/unit/test_connectors_base.py
import pytest

from pavilos.connectors.base import ResyncRequired, ConnectorHealth


def test_resync_required_is_exception_with_message():
    with pytest.raises(ResyncRequired) as exc:
        raise ResyncRequired("gap at seq 5")
    assert "gap at seq 5" in str(exc.value)


def test_connector_health_fields():
    h = ConnectorHealth(exchange="kraken", connected=True, last_update_ts=12.5, resyncs=1, errors=0)
    assert h.exchange == "kraken"
    assert h.connected is True
    assert h.last_update_ts == 12.5
    assert h.resyncs == 1
    assert h.errors == 0
