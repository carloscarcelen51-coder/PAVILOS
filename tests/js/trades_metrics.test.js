// Tests de las funciones puras de /trades (node --test). Los lanza tests/unit/test_trades_page.py.
"use strict";
// Zona fija para que el caso del cambio de hora (25-10-2026) sea el mismo en cualquier equipo.
// Se pone aquí y no en el entorno: Git Bash no pasa TZ a los programas nativos de Windows.
process.env.TZ = "Europe/Madrid";
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const M = require(path.join(__dirname, "..", "..", "src", "pavilos", "web", "static", "trades_metrics.js"));

const U = (y, mo, d, h = 0, mi = 0, s = 0) => Date.UTC(y, mo - 1, d, h, mi, s) / 1000;
const close = (a, b, eps = 1e-9) => assert.ok(Math.abs(a - b) <= eps * Math.max(1, Math.abs(b)), `${a} != ${b}`);

function raw(id, pnl, exitTs, extra = {}) {
  return Object.assign({ id, symbol: "BTC/USD", side: "LONG", size: 0.1, entry: 50000, exit: 50000 + pnl * 10,
                         entry_ts: exitTs - 10, exit_ts: exitTs, pnl, fee: 1, return_pct: pnl / 5000 * 100,
                         reason: "close" }, extra);
}

test("formato español: miles con punto, coma decimal, sin -0", () => {
  assert.equal(M.fmtNum(1234.5, 2), "1.234,50");
  assert.equal(M.fmtNum(1234567.891, 3), "1.234.567,891");
  assert.equal(M.fmtNum(999, 0), "999");
  assert.equal(M.fmtNum(-0.001, 2), "0,00");
  assert.equal(M.fmtNum(NaN), "—");
  assert.equal(M.fmtNum(null), "—");
  assert.equal(M.fmtSigned(5, 2), "+5,00");
  assert.equal(M.fmtSigned(-5, 2), "-5,00");
  assert.equal(M.fmtSigned(0, 2), "0,00");
  assert.equal(M.fmtEur(-174130.27, 2, true), "-174.130,27 €");
  assert.equal(M.fmtPct(12.345, 2, true), "+12,35 %");
  assert.equal(M.fmtPct(Infinity), "∞");
  assert.equal(M.fmtRatio(Infinity), "∞");
});

test("duraciones legibles", () => {
  assert.equal(M.fmtDuration(0.27), "0,27 s");
  assert.equal(M.fmtDuration(9.66), "9,66 s");
  assert.equal(M.fmtDuration(23.86), "23,9 s");
  assert.equal(M.fmtDuration(77.3), "1 min 17 s");
  assert.equal(M.fmtDuration(3725), "1 h 02 min");
  assert.equal(M.fmtDuration(90061), "1 d 01 h");
  assert.equal(M.fmtDuration(undefined), "—");
});

test("fechas en UTC y semana ISO", () => {
  const ts = U(2026, 10, 21, 17, 5, 9);
  assert.equal(M.fmtDate(ts, "utc", true), "21/10/2026 17:05:09");
  assert.equal(M.dayKey(ts, "utc"), "2026-10-21");
  assert.equal(M.monthKey(ts, "utc"), "2026-10");
  assert.equal(M.yearKey(ts, "utc"), "2026");
  assert.equal(M.weekKey(U(2026, 1, 1), "utc"), "2026-W01");      // jueves
  assert.equal(M.weekKey(U(2027, 1, 1), "utc"), "2026-W53");      // viernes: semana 53 de 2026
  assert.equal(M.weekKey(U(2026, 10, 19), "utc"), "2026-W43");    // lunes
  assert.equal(M.weekKey(U(2026, 10, 25, 23, 59), "utc"), "2026-W43");   // domingo
  assert.equal(M.fmtDayKey("2026-10-21"), "21/10/2026");
  assert.equal(M.fmtDayKey("2026-W43"), "2026-W43");
  assert.equal(M.isoTime(ts + 0.25, "utc"), "2026-10-21 17:05:09.250");
});

test("atajos de periodo (UTC)", () => {
  const now = U(2026, 10, 21, 17);     // miércoles
  assert.deepEqual(M.periodBounds("todo", now, "utc"), { from: null, to: null });
  assert.deepEqual(M.periodBounds("hoy", now, "utc"), { from: U(2026, 10, 21), to: null });
  assert.deepEqual(M.periodBounds("ayer", now, "utc"), { from: U(2026, 10, 20), to: U(2026, 10, 21) - 0.001 });
  assert.deepEqual(M.periodBounds("7d", now, "utc"), { from: now - 7 * 86400, to: null });
  assert.deepEqual(M.periodBounds("semana", now, "utc"), { from: U(2026, 10, 19), to: null });
  assert.deepEqual(M.periodBounds("mes", now, "utc"), { from: U(2026, 10, 1), to: null });
  assert.deepEqual(M.periodBounds("mes_pasado", now, "utc"), { from: U(2026, 9, 1), to: U(2026, 10, 1) - 0.001 });
  assert.deepEqual(M.periodBounds("ano", now, "utc"), { from: U(2026, 1, 1), to: null });
  assert.deepEqual(M.periodBounds("ano_pasado", now, "utc"), { from: U(2025, 1, 1), to: U(2026, 1, 1) - 0.001 });
  // enero: el mes pasado es diciembre del año anterior
  assert.deepEqual(M.periodBounds("mes_pasado", U(2026, 1, 15), "utc"), { from: U(2025, 12, 1), to: U(2026, 1, 1) - 0.001 });
});

test("búsqueda por #id exacto", () => {
  const many = M.enrich([raw(1, 1, 100), raw(10, 1, 200), raw(100, 1, 300)]).trades;
  assert.deepEqual(M.applyFilters(many, { text: "#1" }).map((t) => t.id), [1]);
  assert.deepEqual(M.applyFilters(many, { text: " #100 " }).map((t) => t.id), [100]);
  assert.deepEqual(M.applyFilters(many, { text: "1" }).map((t) => t.id), [1, 10, 100]);
});

test("rango de un periodo y lunes de semana ISO", () => {
  assert.deepEqual(M.isoWeekMonday(2026, 1), { y: 2025, m: 11, d: 29 });
  assert.deepEqual(M.isoWeekMonday(2026, 43), { y: 2026, m: 9, d: 19 });
  assert.deepEqual(M.isoWeekMonday(2026, 53), { y: 2026, m: 11, d: 28 });
  assert.deepEqual(M.periodRange("day", "2026-10-21", "utc"), [U(2026, 10, 21), U(2026, 10, 22) - 0.001]);
  assert.deepEqual(M.periodRange("week", "2026-W43", "utc"), [U(2026, 10, 19), U(2026, 10, 26) - 0.001]);
  assert.deepEqual(M.periodRange("month", "2026-12", "utc"), [U(2026, 12, 1), U(2027, 1, 1) - 0.001]);
  assert.deepEqual(M.periodRange("year", "2026", "utc"), [U(2026, 1, 1), U(2027, 1, 1) - 0.001]);
  assert.deepEqual(M.periodRange("day", "basura", "utc"), [null, null]);
  // la clave de semana que da weekKey vuelve a su propio rango
  for (const ts of [U(2026, 1, 1), U(2027, 1, 1), U(2026, 10, 25, 23)]) {
    const [a, b] = M.periodRange("week", M.weekKey(ts, "utc"), "utc");
    assert.ok(a <= ts && ts <= b, M.weekKey(ts, "utc"));
  }
});

test("números y duraciones escritos a mano", () => {
  assert.equal(M.parseNum("1,5"), 1.5);
  assert.equal(M.parseNum("1.5"), 1.5);
  assert.equal(M.parseNum("1.234,5"), 1234.5);
  assert.equal(M.parseNum("-3 €"), -3);
  assert.equal(M.parseNum("12 %"), 12);
  assert.equal(M.parseNum("1e-4"), 0.0001);
  assert.equal(M.parseNum(""), null);
  assert.equal(M.parseNum("   "), null);
  assert.ok(Number.isNaN(M.parseNum("abc")));
  assert.ok(Number.isNaN(M.parseNum("1,2,3")));
  assert.equal(M.parseDur("90"), 90);
  assert.equal(M.parseDur("90s"), 90);
  assert.equal(M.parseDur("1,5m"), 90);
  assert.equal(M.parseDur("1,5 min"), 90);
  assert.equal(M.parseDur("2h"), 7200);
  assert.equal(M.parseDur("1d"), 86400);
  assert.equal(M.parseDur("250ms"), 0.25);
  assert.equal(M.parseDur(""), null);
  assert.ok(Number.isNaN(M.parseDur("10 años")));
});

test("entrada de fecha ↔ epoch en la zona elegida", () => {
  assert.equal(M.parseDateInput("2026-10-21T17:05", "utc"), U(2026, 10, 21, 17, 5));
  assert.equal(M.parseDateInput("2026-10-21", "utc"), U(2026, 10, 21));
  assert.equal(M.parseDateInput("", "utc"), null);
  assert.equal(M.parseDateInput("basura", "utc"), null);
  assert.equal(M.toDateInput(U(2026, 10, 21, 17, 5, 59), "utc"), "2026-10-21T17:05");
  assert.equal(M.toDateInput(null, "utc"), "");
});

test("filas columnares → objetos; enrich descarta lo no finito y deriva campos", () => {
  const objs = M.fromRows(["id", "pnl", "side"], [[0, 1.5, "LONG"], [1, -2, "SHORT"]]);
  assert.deepEqual(objs, [{ id: 0, pnl: 1.5, side: "LONG" }, { id: 1, pnl: -2, side: "SHORT" }]);
  const { trades, dropped } = M.enrich([
    raw(0, 5, 1000),
    raw(1, -3, 2000, { side: "SHORT", entry: 100, exit: 99, size: 2, fee: 0.5 }),
    raw(2, null, 3000),
    raw(3, 1, 4000, { entry_ts: null }),
    raw(4, 0, 5000, { symbol: undefined, reason: undefined, fee: null }),
  ]);
  assert.equal(dropped, 2);
  assert.equal(trades.length, 3);
  const s = trades[1];
  close(s.move_bps, 100);                  // un short que baja de 100 a 99 gana 100 pb
  assert.equal(s.notional, 200);
  assert.equal(s.gross, -2.5);
  assert.equal(s.duration, 10);
  assert.equal(s.result, "loss");
  assert.equal(trades[0].result, "win");
  assert.equal(trades[2].result, "flat");
  assert.equal(trades[2].symbol, "BTC/USD");
  assert.equal(trades[2].reason, "");
  assert.equal(trades[2].fee, 0);
});

test("filtros", () => {
  // salidas a y media: la entrada (10 s antes) cae en la misma hora
  const base = M.enrich([
    raw(0, 10, U(2026, 10, 19, 8, 30), { reason: "stop" }),              // lunes 08h
    raw(1, -5, U(2026, 10, 20, 23, 30), { side: "SHORT" }),              // martes 23h
    raw(2, -1, U(2026, 10, 21, 2, 30), { symbol: "ETH/USD", size: 2 }),  // miércoles 02h
    raw(3, 0, U(2026, 10, 22, 12, 30)),                                  // jueves 12h
  ]).trades;
  const ids = (f) => M.applyFilters(base, Object.assign({ tz: "utc" }, f)).map((t) => t.id);
  assert.deepEqual(ids({}), [0, 1, 2, 3]);
  assert.deepEqual(ids({ symbol: "ETH/USD" }), [2]);
  assert.deepEqual(ids({ symbol: "ALL" }), [0, 1, 2, 3]);
  assert.deepEqual(ids({ side: "SHORT" }), [1]);
  assert.deepEqual(ids({ result: "win" }), [0]);
  assert.deepEqual(ids({ result: "loss" }), [1, 2]);
  assert.deepEqual(ids({ result: "flat" }), [3]);
  assert.deepEqual(ids({ reasons: ["stop"] }), [0]);
  assert.deepEqual(ids({ from: U(2026, 10, 20), to: U(2026, 10, 21, 23, 59) }), [1, 2]);
  assert.deepEqual(ids({ pnlMin: -5, pnlMax: 0 }), [1, 2, 3]);
  assert.deepEqual(ids({ sizeMin: 1 }), [2]);
  assert.deepEqual(ids({ notionalMax: 5000 }), [0, 1, 3]);
  assert.deepEqual(ids({ hourFrom: 22, hourTo: 3 }), [1, 2]);       // cruza medianoche
  assert.deepEqual(ids({ hourFrom: 8, hourTo: 12 }), [0, 3]);
  assert.deepEqual(ids({ weekdays: [0, 3] }), [0, 3]);               // lunes y jueves
  assert.deepEqual(ids({ weekdays: [] }), []);                       // ningún día marcado
  assert.deepEqual(ids({ weekdays: [0, 1, 2, 3, 4, 5, 6] }), [0, 1, 2, 3]);
  assert.deepEqual(ids({ text: "#2" }), [2]);
  assert.deepEqual(ids({ text: "short" }), [1]);
  assert.deepEqual(ids({ text: "eth" }), [2]);
  assert.deepEqual(ids({ reasons: [] }), []);                        // ningún motivo marcado
  assert.deepEqual(ids({ reasons: null }), [0, 1, 2, 3]);
  assert.deepEqual(ids({ durMin: 11 }), []);
  // por hora de ENTRADA (10 s antes de la salida): el trade 0 entró a las 08:29:50
  assert.deepEqual(ids({ timeField: "entry", from: U(2026, 10, 19, 8, 29, 55) }), [1, 2, 3]);
  assert.deepEqual(ids({ timeField: "exit", from: U(2026, 10, 19, 8, 29, 55) }), [0, 1, 2, 3]);
  assert.equal(M.hourMatches(5, null, null), true);
  assert.equal(M.hourMatches(5, 6, null), false);
  assert.equal(M.hourMatches(5, null, 5), true);
});

test("métricas sobre un caso calculado a mano", () => {
  // balance 1000 → 1010, 1005, 1000, 1020, 990 (salidas en 3 días distintos, UTC)
  const t1 = U(2026, 10, 1, 10), t2 = U(2026, 10, 1, 11), t3 = U(2026, 10, 2, 9), t4 = U(2026, 10, 2, 10), t5 = U(2026, 10, 4, 12);
  const tr = M.enrich([raw(0, 10, t1), raw(1, -5, t2), raw(2, -5, t3), raw(3, 20, t4), raw(4, -30, t5)]).trades;
  const r = M.compute(tr.slice().reverse(), 1000, "utc");    // el orden de entrada no importa
  assert.equal(r.n, 5);
  close(r.net, -10);
  close(r.gross, -5);                       // 5 trades × 1 € de comisión
  close(r.fees, 5);
  close(r.final_balance, 990);
  close(r.roi_pct, -1);
  assert.equal(r.wins, 2); assert.equal(r.losses, 3); assert.equal(r.flats, 0);
  close(r.win_rate, 40);
  close(r.profit_factor, 30 / 40);
  close(r.expectancy, -2);
  close(r.avg_win, 15);
  close(r.avg_loss, -40 / 3);
  close(r.payoff, 15 / (40 / 3));
  close(r.breakeven_wr, (40 / 3) / (15 + 40 / 3) * 100);
  assert.equal(r.largest_win, 20); assert.equal(r.largest_win_id, 3);
  assert.equal(r.largest_loss, -30); assert.equal(r.largest_loss_id, 4);
  close(r.max_dd, 30);
  close(r.max_dd_pct, 30 / 1020 * 100);
  assert.equal(r.max_dd_peak_ts, t4); assert.equal(r.max_dd_trough_ts, t5);
  assert.equal(r.max_dd_duration, Math.max(t4 - t1, t5 - t4));
  close(r.current_dd, 30);
  close(r.max_runup, 20);
  close(r.recovery_factor, -10 / 30);
  assert.equal(r.max_win_streak, 1); assert.equal(r.max_loss_streak, 2); assert.equal(r.current_streak, -1);
  assert.equal(r.days_traded, 3);
  assert.equal(r.calendar_days, 4);         // 1, 2, 3 (sin trades) y 4 de octubre
  assert.equal(r.best_day_key, "2026-10-02"); close(r.best_day, 15);
  assert.equal(r.worst_day_key, "2026-10-04"); close(r.worst_day, -30);
  close(r.positive_days_pct, 2 / 3 * 100);
  const daily = [5, 15, 0, -30];
  const md = M.mean(daily), sd = M.std(daily);
  close(r.sharpe, md / sd * Math.sqrt(365));
  close(r.sortino, md / Math.sqrt(900 / 4) * Math.sqrt(365));
  close(r.vol_annual, sd * Math.sqrt(365));
  assert.equal(r.annual_return_pct, null);  // menos de 30 días: no se anualiza
  assert.equal(r.cagr_pct, null);
  assert.equal(r.calmar, null);
  close(r.avg_duration, 10);
  close(r.exposure_pct, 50 / (t5 - (t1 - 10)) * 100);
  assert.equal(r.equity.length, 6);
  assert.deepEqual(r.equity.map((p) => p.bal), [1000, 1010, 1005, 1000, 1020, 990]);
  assert.equal(r.long.n, 5); assert.equal(r.short.n, 0);
});

test("métricas: vacío, todo ganador y anualización con más de 30 días", () => {
  const e = M.compute([], 1000, "utc");
  assert.equal(e.n, 0); assert.equal(e.final_balance, 1000); assert.equal(e.win_rate, null);
  const tr = M.enrich([raw(0, 10, U(2026, 1, 1)), raw(1, 10, U(2026, 3, 2))]).trades;
  const r = M.compute(tr, 1000, "utc");
  assert.equal(r.profit_factor, Infinity);
  assert.equal(r.payoff, null);
  assert.equal(r.max_dd, 0);
  assert.equal(r.recovery_factor, null);
  assert.ok(r.period_days > 30);
  close(r.annual_return_pct, 2 * 365 / r.period_days);
  close(r.cagr_pct, (Math.pow(1020 / 1000, 365 / r.period_days) - 1) * 100);
  // balance que acaba en negativo: CAGR no existe, el resto sí
  const neg = M.compute(M.enrich([raw(0, -1500, U(2026, 1, 1)), raw(1, -10, U(2026, 3, 2))]).trades, 1000, "utc");
  assert.equal(neg.cagr_pct, null);
  close(neg.final_balance, -510);
  close(neg.max_dd_pct, 151);
  assert.ok(neg.calmar < 0);
  // capital inválido → 10.000
  assert.equal(M.compute(tr, 0, "utc").start_equity, 10000);
});

test("agrupaciones por periodo, hora, día y duración", () => {
  const tr = M.enrich([
    raw(0, 10, U(2026, 10, 1, 10)), raw(1, -25, U(2026, 10, 1, 11)), raw(2, 5, U(2026, 10, 2, 9)),
  ]).trades;
  const days = M.groupPeriods(tr, "day", "utc", 1000);
  assert.deepEqual(days.map((d) => d.key), ["2026-10-01", "2026-10-02"]);
  close(days[0].net, -15); close(days[0].bal_start, 1000); close(days[0].bal_end, 985);
  close(days[0].max_dd, 25);                // pico 1010 → 985 dentro del día
  close(days[0].roi_pct, -1.5);
  close(days[1].bal_start, 985); close(days[1].max_dd, 0);
  const months = M.groupPeriods(tr, "month", "utc", 1000);
  assert.equal(months.length, 1); assert.equal(months[0].n, 3);
  const hours = M.byHour(tr, "utc");
  assert.equal(hours.length, 24);
  assert.equal(hours[9].n, 1); assert.equal(hours[10].n, 1); assert.equal(hours[0].n, 0);
  assert.equal(hours[9].label, "09h");
  const wd = M.byWeekday(tr, "utc");
  assert.equal(wd.length, 7);
  assert.equal(wd[3].n, 2); assert.equal(wd[3].label, "jue");     // 1-10-2026 es jueves
  assert.equal(M.durationBucket(0.5), "< 1 s");
  assert.equal(M.durationBucket(9.7), "5-15 s");
  assert.equal(M.durationBucket(60), "1-5 min");
  assert.equal(M.durationBucket(1e6), "> 30 min");
  const dur = M.byDuration(tr);
  assert.deepEqual(dur.map((d) => d.key), ["5-15 s"]);
  const g = M.summarizeGroup("x", tr);
  close(g.profit_factor, 15 / 25);
  close(g.win_rate, 200 / 3);
});

test("histograma, orden, balances, punto más cercano", () => {
  const vals = []; for (let i = 0; i < 1000; i++) vals.push(i);
  vals.push(1e9);                           // outlier
  const h = M.histogram(vals, 10);
  assert.equal(h.counts.reduce((a, b) => a + b, 0), 1001);
  assert.equal(h.edges.length, 11);
  assert.ok(h.hi < 1e9);
  assert.ok(h.clippedHi >= 1);
  assert.deepEqual(M.histogram([], 5).counts, []);
  const one = M.histogram([3, 3, 3], 4);
  assert.equal(one.counts.reduce((a, b) => a + b, 0), 3);

  const rows = [{ id: 0, v: 2 }, { id: 1, v: null }, { id: 2, v: 1 }, { id: 3, v: 2 }];
  assert.deepEqual(M.sortTrades(rows, "v", "asc").map((x) => x.id), [2, 0, 3, 1]);
  assert.deepEqual(M.sortTrades(rows, "v", "desc").map((x) => x.id), [0, 3, 2, 1]);

  const tr = M.enrich([raw(0, 10, 200), raw(1, -4, 100)]).trades;
  const b = M.runningBalances(tr, 1000);
  close(b[1].bal, 996); close(b[0].bal, 1006); close(b[0].cum, 6);

  const pts = [{ ts: 0 }, { ts: 10 }, { ts: 20 }];
  assert.equal(M.nearestIndex(pts, 14), 1);
  assert.equal(M.nearestIndex(pts, 16), 2);
  assert.equal(M.nearestIndex(pts, -5), 0);
  assert.equal(M.nearestIndex(pts, 99), 2);
  assert.equal(M.nearestIndex([], 1), -1);
});

test("CSV en formato español y estándar", () => {
  const tr = M.enrich([raw(7, -1.5, U(2026, 10, 21, 17, 5, 9))]).trades;
  const b = M.runningBalances(tr, 1000);
  const es = M.toCSV(tr, b, "utc", "es").split("\r\n");
  assert.equal(es[0].split(";")[0], "id");
  assert.equal(es[0].split(";").length, 18);
  const row = es[1].split(";");
  assert.equal(row[0], "7");
  assert.equal(row[3], "2026-10-21 17:04:59.000");
  assert.equal(row[13], "-1,500000");
  assert.equal(row[16], "998,500000");
  assert.equal(es[2], "");                  // termina en salto de línea
  const std = M.toCSV(tr, b, "utc", "std").split("\r\n");
  assert.equal(std[1].split(",")[13], "-1.500000");
});

test("zona local: el cambio de hora no salta ni repite días (TZ del proceso)", () => {
  // Con TZ=Europe/Madrid, el 25-10-2026 tiene 25 horas.
  const loc = (y, mo, d, h) => new Date(y, mo - 1, d, h).getTime() / 1000;
  const tr = M.enrich([raw(0, 1, loc(2026, 10, 24, 12)), raw(1, 1, loc(2026, 10, 26, 12))]).trades;
  const r = M.compute(tr, 1000, "local");
  assert.equal(r.calendar_days, 3);
  assert.equal(M.dayKey(loc(2026, 10, 25, 23), "local"), "2026-10-25");
  const pb = M.periodBounds("hoy", loc(2026, 10, 25, 20), "local");
  assert.equal(pb.from, loc(2026, 10, 25, 0));
});
