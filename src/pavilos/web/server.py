# src/pavilos/web/server.py
"""FastAPI app serving the dashboard JSON + static page. Read-only over a
DashboardState. No business logic here."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from pavilos.web.state import DashboardState

_STATIC = Path(__file__).parent / "static"


def create_app(state: DashboardState) -> FastAPI:
    app = FastAPI(title="PAVILOS", docs_url=None, redoc_url=None)

    @app.get("/api/state")
    def api_state() -> JSONResponse:
        return JSONResponse(state.snapshot())

    @app.get("/api/health")
    def api_health() -> JSONResponse:
        snap = state.snapshot()
        return JSONResponse({"venues": snap["venues"], "stale": snap["stale"], "ts": snap["ts"]})

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse((_STATIC / "index.html").read_text(encoding="utf-8"))

    return app
