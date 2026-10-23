# src/pavilos/web/server.py
"""FastAPI app serving the dashboard JSON + static pages. Read-only over a
DashboardState and the all-time trade list. No business logic here: the
/trades page computes its filters and metrics client-side (trades_metrics.js)."""
from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Callable, Sequence

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from pavilos.web.history import STABLE_SETUP_SINCE, run_starts
from pavilos.web.state import DashboardState

_STATIC = Path(__file__).parent / "static"

# Column order of /api/trades rows (one array per trade keeps ~23k trades at ~2.5 MB).
TRADE_FIELDS = ("id", "symbol", "side", "size", "entry", "exit", "entry_ts", "exit_ts",
                "pnl", "fee", "return_pct", "reason", "context")


def _finite(x):
    # JSONResponse refuses NaN/Infinity (HTTP 500): a non-finite value goes out as null.
    return x if isinstance(x, (int, float)) and math.isfinite(x) else None


def _clean(v):
    """A JSON-safe copy of the entry context (non-finite numbers -> null)."""
    if isinstance(v, dict):
        return {str(k): _clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_clean(x) for x in v]
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    return v


def _trade_row(i: int, t) -> list:
    return [i, t.symbol, t.side, _finite(t.size), _finite(t.entry), _finite(t.exit),
            _finite(t.entry_ts), _finite(t.exit_ts), _finite(t.pnl), _finite(t.fee),
            _finite(t.return_pct), t.reason, _clean(getattr(t, "context", None))]


def create_app(state: DashboardState, *, trades: Callable[[], Sequence] | None = None,
               starting_equity: float = 10_000.0, ref_notional: float = 10_000.0,
               runs_file: str | None = None, paper: Callable[[], dict] | None = None,
               new_period: Callable[[str], dict] | None = None,
               restart: Callable[[], None] | None = None) -> FastAPI:
    """``trades`` returns the live all-time trade list (append-only, oldest first): a
    trade's id is its index there, so ``/api/trades?since=N`` serves only the new ones.
    ``boot`` changes on every process start; the page then reloads everything.
    ``ref_notional`` is the size every trade is normalized to on /trades; ``runs_file``
    the lake's ``_runs.jsonl`` (process starts). ``paper`` describes the paper account and
    its measurement period, ``new_period`` starts a new one (cash back to the starting
    equity; nothing deleted) and ``restart`` asks for a clean restart. The two actions are
    POST with the header ``X-Pavilos: 1``: a page of another site cannot send it (a custom
    header needs a CORS permission this server never gives)."""
    app = FastAPI(title="PAVILOS", docs_url=None, redoc_url=None)

    def _from_dashboard(request: Request) -> bool:
        return request.headers.get("x-pavilos") == "1"
    boot = f"{time.time():.6f}"

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

    @app.get("/api/trades")
    def api_trades(since: int = 0) -> JSONResponse:
        # Sync handler -> runs in the threadpool, off the recorder's event loop. list()
        # copies under the GIL, so a concurrent append can't tear the snapshot.
        snap = list(trades()) if trades is not None else []
        total = len(snap)
        if since < 0 or since > total:
            since = 0           # unknown position (e.g. other process): send everything
        return JSONResponse({
            "boot": boot, "total": total, "since": since,
            "starting_equity": starting_equity, "ref_notional": ref_notional,
            "stable_since": STABLE_SETUP_SINCE, "fields": list(TRADE_FIELDS),
            "rows": [_trade_row(i, snap[i]) for i in range(since, total)],
        })

    @app.get("/api/runs")
    def api_runs() -> JSONResponse:
        """Process starts: until 2026-10-23 each one reset the paper account to 10,000."""
        return JSONResponse({"runs": run_starts(runs_file), "stable_since": STABLE_SETUP_SINCE})

    # async: these run on the event loop, the same thread as the trading -> no races
    @app.get("/api/paper")
    async def api_paper() -> JSONResponse:
        if paper is None:
            return JSONResponse({"available": False})
        return JSONResponse(_clean({"available": True, **paper()}))

    @app.post("/api/paper/new-period")
    async def api_new_period(request: Request) -> JSONResponse:
        if not _from_dashboard(request):
            return JSONResponse({"ok": False, "error": "falta la cabecera X-Pavilos: 1"}, status_code=403)
        if new_period is None:
            return JSONResponse({"ok": False, "error": "no disponible"}, status_code=503)
        try:
            body = await request.json()
        except Exception:
            body = {}
        note = str((body or {}).get("note", ""))[:200] if isinstance(body, dict) else ""
        try:
            return JSONResponse(_clean({"ok": True, **new_period(note)}))
        except RuntimeError as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=409)

    @app.post("/api/admin/restart")
    async def api_restart(request: Request) -> JSONResponse:
        if not _from_dashboard(request):
            return JSONResponse({"ok": False, "error": "falta la cabecera X-Pavilos: 1"}, status_code=403)
        if restart is None:
            return JSONResponse({"ok": False, "error": "no disponible"}, status_code=503)
        restart()
        return JSONResponse({"ok": True, "detail": "parando de forma ordenada (grabador vaciado, cuenta "
                             "guardada); el supervisor arranca un proceso nuevo en unos segundos"},
                            status_code=202)

    @app.get("/trades", response_class=HTMLResponse)
    def trades_page() -> HTMLResponse:
        return HTMLResponse((_STATIC / "trades.html").read_text(encoding="utf-8"),
                            headers={"Cache-Control": "no-cache"})

    @app.get("/static/trades_metrics.js")
    def trades_metrics_js() -> Response:
        return Response((_STATIC / "trades_metrics.js").read_text(encoding="utf-8"),
                        media_type="application/javascript; charset=utf-8",
                        headers={"Cache-Control": "no-cache"})

    return app
