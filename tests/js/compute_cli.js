// Usado por tests/unit/test_trades_page.py: calcula las métricas de /trades sobre la respuesta
// de /api/trades y las imprime en JSON para contrastarlas con una implementación en Python.
// Uso: node compute_cli.js <entrada.json>   con {"fields", "rows", "start", "tz"}
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const M = require(path.join(__dirname, "..", "..", "src", "pavilos", "web", "static", "trades_metrics.js"));

const inp = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const e = M.enrich(M.fromRows(inp.fields, inp.rows));
const r = M.compute(e.trades, inp.start, inp.tz);
delete r.equity;
r.dropped = e.dropped;
r.symbols = Array.from(new Set(e.trades.map((t) => t.symbol))).sort();
r.months = M.groupPeriods(e.trades, "month", inp.tz, inp.start).map((g) => [g.key, g.n, g.net]);
for (const k of Object.keys(r)) if (r[k] === Infinity) r[k] = "inf";   // JSON no tiene Infinity
process.stdout.write(JSON.stringify(r));
