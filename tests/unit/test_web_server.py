# tests/unit/test_web_server.py
from fastapi.testclient import TestClient

from pavilos.web.state import DashboardState
from pavilos.web.server import create_app


def test_api_state_returns_current_snapshot():
    state = DashboardState()
    client = TestClient(create_app(state))
    r = client.get("/api/state")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "IDLE" and body["supports"] == []


def test_root_serves_dashboard_html():
    client = TestClient(create_app(DashboardState()))
    r = client.get("/")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert "PAVILOS" in r.text


def test_api_state_reflects_updates():
    state = DashboardState()
    client = TestClient(create_app(state))
    # mutate the holder directly (simulates the trading loop writing)
    state._snap = {**state.snapshot(), "mid": 104231.5, "state": "IN_POSITION"}
    body = client.get("/api/state").json()
    assert body["mid"] == 104231.5 and body["state"] == "IN_POSITION"
