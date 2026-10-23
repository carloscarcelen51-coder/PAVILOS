/* PAVILOS · métricas del historial de trades.
   Funciones puras (sin DOM), compartidas por la página /trades y por los tests de Node.

   Convenciones (las de /api/trades):
   - pnl es NETO de comisiones; fee es la comisión total (entrada + salida); funding no incluido.
   - return_pct = pnl / nocional * 100; tiempos en epoch segundos.
   - Las métricas se calculan sobre el conjunto filtrado, en orden de SALIDA, como si la
     cuenta solo hubiera hecho esas operaciones partiendo del capital base. El balance
     puede quedar negativo (el paper no corta), así que los ratios de riesgo (Sharpe,
     Sortino, volatilidad, Calmar) se calculan sobre el P&L diario en euros, que no depende
     del balance; ROI y rentabilidades van siempre sobre el capital base. */
(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.TradesMetrics = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var DAY = 86400;
  var WEEKDAYS = ["lun", "mar", "mié", "jue", "vie", "sáb", "dom"];
  var MIN_ANNUAL_DAYS = 30;      // por debajo, anualizar no tiene sentido: se deja en null

  function isNum(x) { return typeof x === "number" && isFinite(x); }
  function pad(n) { return (n < 10 ? "0" : "") + n; }

  // ── tiempo: zona "utc" o "local" (la del navegador) ──────────────────────
  function parts(ts, tz) {
    var d = new Date(ts * 1000);
    if (tz === "utc") {
      return { y: d.getUTCFullYear(), m: d.getUTCMonth(), d: d.getUTCDate(), hour: d.getUTCHours(),
               min: d.getUTCMinutes(), sec: d.getUTCSeconds(), wd: (d.getUTCDay() + 6) % 7 };
    }
    return { y: d.getFullYear(), m: d.getMonth(), d: d.getDate(), hour: d.getHours(),
             min: d.getMinutes(), sec: d.getSeconds(), wd: (d.getDay() + 6) % 7 };
  }
  // epoch (s) de una fecha de calendario en la zona elegida; admite desbordes (d = 0, m = -1…)
  function mkTs(y, m, d, hh, mm, tz) {
    return (tz === "utc" ? Date.UTC(y, m, d, hh || 0, mm || 0) : new Date(y, m, d, hh || 0, mm || 0).getTime()) / 1000;
  }
  function dayKey(ts, tz) { var p = parts(ts, tz); return p.y + "-" + pad(p.m + 1) + "-" + pad(p.d); }
  function monthKey(ts, tz) { var p = parts(ts, tz); return p.y + "-" + pad(p.m + 1); }
  function yearKey(ts, tz) { return String(parts(ts, tz).y); }
  function weekKey(ts, tz) {               // semana ISO 8601: AAAA-Wss (lunes a domingo)
    var p = parts(ts, tz);
    var d = new Date(Date.UTC(p.y, p.m, p.d));
    var dayNum = d.getUTCDay() || 7;        // lunes = 1 … domingo = 7
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);   // el jueves de esa semana fija el año ISO
    var yearStart = Date.UTC(d.getUTCFullYear(), 0, 1);
    var week = Math.ceil(((d.getTime() - yearStart) / 864e5 + 1) / 7);
    return d.getUTCFullYear() + "-W" + pad(week);
  }
  var PERIOD_KEY = { day: dayKey, week: weekKey, month: monthKey, year: yearKey };

  // Atajos de periodo → {from, to} en epoch s (null = sin límite). Los "N días" son móviles.
  var PRESETS = ["todo", "hoy", "ayer", "7d", "30d", "90d", "semana", "mes", "mes_pasado", "ano", "ano_pasado"];
  function periodBounds(preset, nowTs, tz) {
    var p = parts(nowTs, tz);
    var today = mkTs(p.y, p.m, p.d, 0, 0, tz);
    switch (preset) {
      case "hoy": return { from: today, to: null };
      case "ayer": return { from: mkTs(p.y, p.m, p.d - 1, 0, 0, tz), to: today - 0.001 };
      case "7d": return { from: nowTs - 7 * DAY, to: null };
      case "30d": return { from: nowTs - 30 * DAY, to: null };
      case "90d": return { from: nowTs - 90 * DAY, to: null };
      case "semana": return { from: mkTs(p.y, p.m, p.d - p.wd, 0, 0, tz), to: null };
      case "mes": return { from: mkTs(p.y, p.m, 1, 0, 0, tz), to: null };
      case "mes_pasado": return { from: mkTs(p.y, p.m - 1, 1, 0, 0, tz), to: mkTs(p.y, p.m, 1, 0, 0, tz) - 0.001 };
      case "ano": return { from: mkTs(p.y, 0, 1, 0, 0, tz), to: null };
      case "ano_pasado": return { from: mkTs(p.y - 1, 0, 1, 0, 0, tz), to: mkTs(p.y, 0, 1, 0, 0, tz) - 0.001 };
      default: return { from: null, to: null };
    }
  }

  // Lunes de una semana ISO como fecha de calendario {y, m (0-11), d}.
  function isoWeekMonday(year, week) {
    var jan4 = new Date(Date.UTC(year, 0, 4));
    var d = new Date(Date.UTC(year, 0, 4 - (jan4.getUTCDay() + 6) % 7 + (week - 1) * 7));
    return { y: d.getUTCFullYear(), m: d.getUTCMonth(), d: d.getUTCDate() };
  }

  // Clave de periodo (la de groupPeriods) → [from, to] en epoch s, en la zona elegida.
  function periodRange(period, key, tz) {
    var m;
    if (period === "day" && (m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(key))) {
      return [mkTs(+m[1], +m[2] - 1, +m[3], 0, 0, tz), mkTs(+m[1], +m[2] - 1, +m[3] + 1, 0, 0, tz) - 0.001];
    }
    if (period === "week" && (m = /^(\d{4})-W(\d{2})$/.exec(key))) {
      var mon = isoWeekMonday(+m[1], +m[2]);
      return [mkTs(mon.y, mon.m, mon.d, 0, 0, tz), mkTs(mon.y, mon.m, mon.d + 7, 0, 0, tz) - 0.001];
    }
    if (period === "month" && (m = /^(\d{4})-(\d{2})$/.exec(key))) {
      return [mkTs(+m[1], +m[2] - 1, 1, 0, 0, tz), mkTs(+m[1], +m[2], 1, 0, 0, tz) - 0.001];
    }
    if (period === "year" && /^\d{4}$/.test(key)) {
      return [mkTs(+key, 0, 1, 0, 0, tz), mkTs(+key + 1, 0, 1, 0, 0, tz) - 0.001];
    }
    return [null, null];
  }

  // Números escritos a mano: "1,5", "1.5", "1.234,5", "-3 €", "12 %". "" → null; basura → NaN.
  function parseNum(str) {
    var s = String(str == null ? "" : str).replace(/[\s€%]/g, "");
    if (!s) return null;
    if (s.indexOf(",") >= 0 && s.indexOf(".") >= 0) s = s.replace(/\./g, "").replace(",", ".");
    else s = s.replace(",", ".");
    if (!/^[-+]?(\d+\.?\d*|\.\d+)(e[-+]?\d+)?$/i.test(s)) return NaN;
    return Number(s);
  }
  // Duraciones: "90", "90s", "1,5m", "1,5 min", "2h", "1d", "250ms" → segundos.
  function parseDur(str) {
    var s = String(str == null ? "" : str).trim().toLowerCase().replace(/\s/g, "");
    if (!s) return null;
    var m = /^([-+]?[\d.,]+)(ms|s|seg|m|min|h|d)?$/.exec(s);
    if (!m) return NaN;
    var v = parseNum(m[1]);
    if (!isNum(v)) return NaN;
    var mult = { ms: 0.001, s: 1, seg: 1, m: 60, min: 60, h: 3600, d: DAY }[m[2] || "s"];
    return v * mult;
  }

  // <input type="datetime-local"> ↔ epoch s, leyendo la hora en la zona elegida
  function parseDateInput(str, tz) {
    var m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/.exec(str || "");
    if (!m) return null;
    return mkTs(+m[1], +m[2] - 1, +m[3], m[4] ? +m[4] : 0, m[5] ? +m[5] : 0, tz);
  }
  function toDateInput(ts, tz) {
    if (!isNum(ts)) return "";
    var p = parts(ts, tz);
    return p.y + "-" + pad(p.m + 1) + "-" + pad(p.d) + "T" + pad(p.hour) + ":" + pad(p.min);
  }

  // ── datos: filas de /api/trades → objetos con campos derivados ───────────
  function fromRows(fields, rows) {
    var out = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var o = {}, r = rows[i];
      for (var k = 0; k < fields.length; k++) o[fields[k]] = r[k];
      out[i] = o;
    }
    return out;
  }

  // Descarta (y cuenta) lo que no sea utilizable: sin pnl, precios, tamaño u horas finitos.
  function enrich(raw) {
    var out = [], dropped = 0;
    for (var i = 0; i < raw.length; i++) {
      var t = raw[i];
      if (!t || !isNum(t.pnl) || !isNum(t.entry) || !isNum(t.exit) ||
          !isNum(t.entry_ts) || !isNum(t.exit_ts) || !isNum(t.size)) { dropped++; continue; }
      var fee = isNum(t.fee) ? t.fee : 0;
      var dir = t.side === "SHORT" ? -1 : 1;
      var ctx = t.context && typeof t.context === "object" ? t.context : null;
      var cnum = function (k) { return ctx && isNum(ctx[k]) ? ctx[k] : null; };
      var notional = t.entry * t.size;
      out.push({
        id: t.id, symbol: t.symbol || "BTC/USD", side: t.side, reason: t.reason || "",
        size: t.size, entry: t.entry, exit: t.exit, entry_ts: t.entry_ts, exit_ts: t.exit_ts,
        pnl: t.pnl, fee: fee, return_pct: isNum(t.return_pct) ? t.return_pct : 0,
        gross: t.pnl + fee,                                   // antes de comisiones (sin funding)
        notional: notional,
        // importes tal como se registraron (normalize() los conserva aquí)
        pnl_real: t.pnl, fee_real: fee, gross_real: t.pnl + fee, notional_real: notional, size_real: t.size,
        move_bps: t.entry ? dir * (t.exit - t.entry) / t.entry * 1e4 : 0,   // a favor del lado
        duration: Math.max(0, t.exit_ts - t.entry_ts),
        result: t.pnl > 0 ? "win" : (t.pnl < 0 ? "loss" : "flat"),
        ctx: ctx,                                             // contexto de entrada (desde el 23-10)
        stop_bps: cnum("stop_bps"), room_bps: cnum("room_bps"), zone_conf: cnum("zone_conf"),
      });
    }
    return { trades: out, dropped: dropped };
  }

  // Cada trade como si hubiera sido de `notional` (el P&L en pb no cambia). Hasta el 23-10 el
  // paper dimensionaba con el 1 % del capital de cada sesión, que se iba gastando: tamaños de
  // 4 € a 96.000 € que no se pueden sumar ni comparar. Devuelve copias; los importes
  // registrados siguen en *_real.
  function normalize(trades, notional) {
    var out = new Array(trades.length);
    for (var i = 0; i < trades.length; i++) {
      var t = trades[i], f = t.notional_real > 0 ? notional / t.notional_real : 0;
      var c = {};
      for (var k in t) if (Object.prototype.hasOwnProperty.call(t, k)) c[k] = t[k];
      c.pnl = t.pnl_real * f; c.fee = t.fee_real * f; c.gross = t.gross_real * f;
      c.notional = t.notional_real * f; c.size = t.size_real * f;
      out[i] = c;
    }
    return out;
  }

  // Índice del último arranque <= ts en una lista ordenada de arranques (-1 si es anterior).
  function runIndex(starts, ts) {
    var lo = 0, hi = starts.length - 1, ans = -1;
    while (lo <= hi) {
      var mid = (lo + hi) >> 1;
      if (starts[mid] <= ts) { ans = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return ans;
  }

  // Resumen por sesión (arranque del programa). Además de lo habitual: el P&L y el nocional
  // REGISTRADOS al principio y al final de la sesión, que muestran cómo se iba gastando.
  function groupRuns(trades, starts) {
    var by = {}, keys = [];
    var t = trades.slice().sort(byExit);
    for (var i = 0; i < t.length; i++) {
      var k = runIndex(starts, t[i].entry_ts);
      if (!by[k]) { by[k] = []; keys.push(k); }
      by[k].push(t[i]);
    }
    keys.sort(function (a, b) { return a - b; });
    return keys.map(function (k) {
      var a = by[k], g = summarizeGroup(k, a), m = Math.min(10, a.length), first = 0, last = 0, real = 0;
      for (var i = 0; i < a.length; i++) real += a[i].pnl_real;
      for (i = 0; i < m; i++) { first += a[i].notional_real; last += a[a.length - 1 - i].notional_real; }
      g.run = k; g.start = k >= 0 ? starts[k] : null;
      g.first_ts = a[0].entry_ts; g.last_ts = a[a.length - 1].exit_ts;
      g.pnl_real = real; g.notional_first = first / m; g.notional_last = last / m;
      return g;
    });
  }

  // ── filtros ──────────────────────────────────────────────────────────────
  function inRange(v, lo, hi) { return (lo == null || v >= lo) && (hi == null || v <= hi); }

  function hourMatches(h, from, to) {
    if (from == null && to == null) return true;
    if (from == null) return h <= to;
    if (to == null) return h >= from;
    return from <= to ? (h >= from && h <= to) : (h >= from || h <= to);   // cruza medianoche
  }

  // f: { symbol, side, result, reasons[], from, to (epoch s), timeField "exit"|"entry",
  //      pnlMin/Max, retMin/Max, sizeMin/Max, notionalMin/Max, entryMin/Max, durMin/Max (s),
  //      feeMin/Max, moveMin/Max (pb), stopMin/Max, roomMin/Max (pb, contexto de entrada),
  //      hourFrom, hourTo, weekdays[] (0 = lun), tz, text, notBefore (entrada >= este ts) }
  // Hora y día de la semana se miran en la ENTRADA del trade. reasons/weekdays: null = todos,
  // [] = ninguno. text "#123" busca ese id exacto; cualquier otro texto, por contenido.
  function applyFilters(trades, f) {
    f = f || {};
    var tz = f.tz || "local";
    var field = f.timeField === "entry" ? "entry_ts" : "exit_ts";
    var text = f.text ? String(f.text).trim().toLowerCase() : "";
    var exactId = /^#\d+$/.test(text) ? +text.slice(1) : null;
    var reasons = Array.isArray(f.reasons) ? f.reasons : null;
    var weekdays = Array.isArray(f.weekdays) && f.weekdays.length < 7 ? f.weekdays : null;
    var needClock = f.hourFrom != null || f.hourTo != null || weekdays;
    var out = [];
    for (var i = 0; i < trades.length; i++) {
      var t = trades[i];
      if (f.symbol && f.symbol !== "ALL" && t.symbol !== f.symbol) continue;
      if (f.side && f.side !== "ALL" && t.side !== f.side) continue;
      if (f.result && f.result !== "ALL" && t.result !== f.result) continue;
      if (reasons && reasons.indexOf(t.reason) < 0) continue;
      if (f.notBefore != null && t.entry_ts < f.notBefore) continue;
      var ts = t[field];
      if (f.from != null && ts < f.from) continue;
      if (f.to != null && ts > f.to) continue;
      if (!inRange(t.pnl, f.pnlMin, f.pnlMax)) continue;
      if (!inRange(t.return_pct, f.retMin, f.retMax)) continue;
      if (!inRange(t.size, f.sizeMin, f.sizeMax)) continue;
      if (!inRange(t.notional, f.notionalMin, f.notionalMax)) continue;
      if (!inRange(t.entry, f.entryMin, f.entryMax)) continue;
      if (!inRange(t.duration, f.durMin, f.durMax)) continue;
      if (!inRange(t.fee, f.feeMin, f.feeMax)) continue;
      if (!inRange(t.move_bps, f.moveMin, f.moveMax)) continue;
      // contexto de entrada: con un límite puesto, un trade sin contexto (anterior al 23-10) no entra
      if ((f.stopMin != null || f.stopMax != null) && (t.stop_bps == null || !inRange(t.stop_bps, f.stopMin, f.stopMax))) continue;
      if ((f.roomMin != null || f.roomMax != null) && (t.room_bps == null || !inRange(t.room_bps, f.roomMin, f.roomMax))) continue;
      if (needClock) {
        var p = parts(t.entry_ts, tz);
        if (!hourMatches(p.hour, f.hourFrom, f.hourTo)) continue;
        if (weekdays && weekdays.indexOf(p.wd) < 0) continue;
      }
      if (exactId !== null) {
        if (t.id !== exactId) continue;
      } else if (text) {
        var hay = ("#" + t.id + " " + t.symbol + " " + t.side + " " + t.reason).toLowerCase();
        if (hay.indexOf(text) < 0) continue;
      }
      out.push(t);
    }
    return out;
  }

  // ── estadística básica ───────────────────────────────────────────────────
  function mean(a) { if (!a.length) return null; var s = 0; for (var i = 0; i < a.length; i++) s += a[i]; return s / a.length; }
  function std(a) {                         // desviación típica muestral
    if (a.length < 2) return null;
    var m = mean(a), s = 0;
    for (var i = 0; i < a.length; i++) s += (a[i] - m) * (a[i] - m);
    return Math.sqrt(s / (a.length - 1));
  }
  function median(a) {
    if (!a.length) return null;
    var s = a.slice().sort(function (x, y) { return x - y; });
    var k = s.length >> 1;
    return s.length % 2 ? s[k] : (s[k - 1] + s[k]) / 2;
  }
  function byExit(a, b) { return a.exit_ts - b.exit_ts || a.entry_ts - b.entry_ts || a.id - b.id; }

  // ── métricas completas ───────────────────────────────────────────────────
  // opts.activeDays: claves de día (dayKey) en que el programa estuvo en marcha. Si se da, la
  // serie diaria de Sharpe/Sortino/volatilidad solo cuenta esos días (un día con el programa
  // parado no es un día a 0 €); sin ella, cuenta todos los días naturales del periodo.
  function compute(trades, startEquity, tz, opts) {
    tz = tz || "local";
    var activeDays = opts && opts.activeDays ? opts.activeDays : null;
    var start = isNum(startEquity) && startEquity > 0 ? startEquity : 10000;
    var t = trades.slice().sort(byExit);
    var n = t.length;
    var r = {
      n: n, start_equity: start, final_balance: start, net: 0, gross: 0, fees: 0,
      sum_win: 0, sum_loss: 0, wins: 0, losses: 0, flats: 0, win_rate: null, roi_pct: 0,
      profit_factor: null, expectancy: null, expectancy_pct: null, avg_win: null, avg_loss: null,
      avg_win_pct: null, avg_loss_pct: null, payoff: null, breakeven_wr: null,
      largest_win: null, largest_win_id: null, largest_loss: null, largest_loss_id: null,
      median_pnl: null, std_pnl: null, sqn: null, kelly_pct: null,
      max_dd: 0, max_dd_pct: 0, max_dd_peak_ts: null, max_dd_trough_ts: null,
      max_dd_duration: 0, current_dd: 0, current_dd_pct: 0, recovery_factor: null, ulcer_index: null,
      max_runup: 0,
      max_win_streak: 0, max_loss_streak: 0, current_streak: 0,
      avg_duration: null, median_duration: null, avg_duration_win: null, avg_duration_loss: null,
      first_ts: null, last_ts: null, period_days: 0, trades_per_day: null, exposure_pct: null,
      days_traded: 0, calendar_days: 0, positive_days_pct: null, best_day: null, best_day_key: null,
      worst_day: null, worst_day_key: null, avg_day: null,
      sharpe: null, sortino: null, vol_annual: null, vol_annual_pct: null,
      annual_return_pct: null, cagr_pct: null, calmar: null,
      total_notional: 0, avg_notional: null, avg_move_bps: null, gross_bps: null, fee_bps: null, net_bps: null,
      fee_drag_pct: null, fees_pct_capital: 0,
      long: sideStats([]), short: sideStats([]),
      equity: [],
    };
    if (!n) return r;

    var bal = start, peak = start, peakTs = t[0].entry_ts, trough = start;
    var durs = [], dursW = [], dursL = [], pnls = [], rets = [], retsW = [], retsL = [], moves = [];
    var curW = 0, curL = 0, dd2 = 0;
    var ddOpenTs = null;                    // inicio del drawdown en curso (hora del pico)
    r.equity.push({ ts: t[0].entry_ts, bal: start, dd: 0, dd_pct: 0, id: null });
    for (var i = 0; i < n; i++) {
      var x = t[i];
      r.net += x.pnl; r.gross += x.gross; r.fees += x.fee; r.total_notional += x.notional;
      pnls.push(x.pnl); rets.push(x.return_pct); durs.push(x.duration); moves.push(x.move_bps);
      if (x.result === "win") {
        r.wins++; r.sum_win += x.pnl; dursW.push(x.duration); retsW.push(x.return_pct); curW++; curL = 0;
        if (r.largest_win === null || x.pnl > r.largest_win) { r.largest_win = x.pnl; r.largest_win_id = x.id; }
      } else if (x.result === "loss") {
        r.losses++; r.sum_loss += x.pnl; dursL.push(x.duration); retsL.push(x.return_pct); curL++; curW = 0;
        if (r.largest_loss === null || x.pnl < r.largest_loss) { r.largest_loss = x.pnl; r.largest_loss_id = x.id; }
      } else {
        r.flats++;                          // una neutra no rompe ni alarga rachas
      }
      if (curW > r.max_win_streak) r.max_win_streak = curW;
      if (curL > r.max_loss_streak) r.max_loss_streak = curL;

      bal += x.pnl;
      if (bal >= peak) {
        if (ddOpenTs !== null) {            // recuperado: cierra el episodio
          r.max_dd_duration = Math.max(r.max_dd_duration, x.exit_ts - ddOpenTs);
          ddOpenTs = null;
        }
        peak = bal; peakTs = x.exit_ts;
      } else if (ddOpenTs === null) {
        ddOpenTs = peakTs;
      }
      if (bal < trough) trough = bal;
      if (bal - trough > r.max_runup) r.max_runup = bal - trough;
      var dd = peak - bal, ddPct = dd / peak * 100;    // peak >= start > 0
      if (dd > r.max_dd) { r.max_dd = dd; r.max_dd_peak_ts = peakTs; r.max_dd_trough_ts = x.exit_ts; }
      if (ddPct > r.max_dd_pct) r.max_dd_pct = ddPct;
      dd2 += ddPct * ddPct;
      r.equity.push({ ts: x.exit_ts, bal: bal, dd: dd, dd_pct: ddPct, id: x.id });
    }
    var lastTs = t[n - 1].exit_ts;
    if (ddOpenTs !== null) r.max_dd_duration = Math.max(r.max_dd_duration, lastTs - ddOpenTs);
    r.current_streak = curW > 0 ? curW : (curL > 0 ? -curL : 0);
    r.final_balance = bal;
    r.current_dd = peak - bal;
    r.current_dd_pct = (peak - bal) / peak * 100;
    r.roi_pct = r.net / start * 100;
    r.win_rate = r.wins / n * 100;
    r.profit_factor = r.sum_loss < 0 ? r.sum_win / -r.sum_loss : (r.sum_win > 0 ? Infinity : null);
    r.expectancy = r.net / n;
    r.expectancy_pct = mean(rets);
    r.avg_win = r.wins ? r.sum_win / r.wins : null;
    r.avg_loss = r.losses ? r.sum_loss / r.losses : null;
    r.avg_win_pct = mean(retsW);
    r.avg_loss_pct = mean(retsL);
    r.payoff = r.avg_win !== null && r.avg_loss !== null && r.avg_loss < 0 ? r.avg_win / -r.avg_loss : null;
    if (r.avg_win !== null && r.avg_loss !== null) {        // acierto que haría falta para quedar a 0
      r.breakeven_wr = -r.avg_loss / (r.avg_win - r.avg_loss) * 100;
    }
    r.median_pnl = median(pnls);
    r.std_pnl = std(pnls);
    r.sqn = r.std_pnl ? Math.sqrt(Math.min(n, 100)) * (r.net / n) / r.std_pnl : null;
    var decided = r.wins + r.losses;
    if (decided && r.payoff) {
      var w = r.wins / decided;
      r.kelly_pct = (w - (1 - w) / r.payoff) * 100;
    }
    r.recovery_factor = r.max_dd > 0 ? r.net / r.max_dd : null;
    r.ulcer_index = Math.sqrt(dd2 / n);
    r.avg_duration = mean(durs); r.median_duration = median(durs);
    r.avg_duration_win = mean(dursW); r.avg_duration_loss = mean(dursL);
    r.avg_notional = r.total_notional / n;
    r.avg_move_bps = mean(moves);
    if (r.total_notional > 0) {             // ponderado por nocional: ida y vuelta en pb
      r.gross_bps = r.gross / r.total_notional * 1e4;
      r.fee_bps = r.fees / r.total_notional * 1e4;
      r.net_bps = r.net / r.total_notional * 1e4;
    }
    r.fee_drag_pct = r.gross > 0 ? r.fees / r.gross * 100 : null;
    r.fees_pct_capital = r.fees / start * 100;

    // periodo y exposición (unión de los intervalos en posición)
    var firstTs = Infinity;
    for (i = 0; i < n; i++) if (t[i].entry_ts < firstTs) firstTs = t[i].entry_ts;
    r.first_ts = firstTs; r.last_ts = lastTs;
    var span = Math.max(lastTs - firstTs, 1);
    r.period_days = span / DAY;
    r.trades_per_day = n / Math.max(r.period_days, 1);
    var iv = t.map(function (x) { return [x.entry_ts, x.exit_ts]; }).sort(function (a, b) { return a[0] - b[0]; });
    var covered = 0, cs = iv[0][0], ce = iv[0][1];
    for (i = 1; i < iv.length; i++) {
      if (iv[i][0] <= ce) { if (iv[i][1] > ce) ce = iv[i][1]; }
      else { covered += ce - cs; cs = iv[i][0]; ce = iv[i][1]; }
    }
    covered += ce - cs;
    r.exposure_pct = Math.min(100, covered / span * 100);

    // días naturales (en la zona elegida) con P&L por día de SALIDA
    var days = groupPeriods(t, "day", tz, start);
    r.days_traded = days.length;
    var pos = 0;
    for (i = 0; i < days.length; i++) {
      var g = days[i];
      if (g.net > 0) pos++;
      if (r.best_day === null || g.net > r.best_day) { r.best_day = g.net; r.best_day_key = g.key; }
      if (r.worst_day === null || g.net < r.worst_day) { r.worst_day = g.net; r.worst_day_key = g.key; }
    }
    r.positive_days_pct = days.length ? pos / days.length * 100 : null;
    r.avg_day = days.length ? r.net / days.length : null;

    // serie diaria CONTINUA (los días sin operaciones cuentan como 0 €; con activeDays, solo
    // los días con el programa en marcha) para los ratios
    var dailyPnl = calendarDailyPnl(days, firstTs, lastTs, tz, activeDays);
    r.calendar_days = dailyPnl.length;
    r.days_basis = activeDays ? "active" : "calendar";
    var md = mean(dailyPnl), sd = std(dailyPnl);
    if (md !== null && sd) {
      r.sharpe = md / sd * Math.sqrt(365);
      r.vol_annual = sd * Math.sqrt(365);
      r.vol_annual_pct = r.vol_annual / start * 100;
    }
    if (md !== null && dailyPnl.length > 1) {
      var down = 0;
      for (i = 0; i < dailyPnl.length; i++) if (dailyPnl[i] < 0) down += dailyPnl[i] * dailyPnl[i];
      var dsd = Math.sqrt(down / dailyPnl.length);
      if (dsd > 0) r.sortino = md / dsd * Math.sqrt(365);
    }
    if (r.period_days >= MIN_ANNUAL_DAYS) {
      var perYear = 365 / r.period_days;
      r.annual_return_pct = r.roi_pct * perYear;
      if (bal > 0) r.cagr_pct = (Math.pow(bal / start, perYear) - 1) * 100;
      if (r.max_dd > 0) r.calmar = r.net * perYear / r.max_dd;
    }

    r.long = sideStats(t.filter(function (x) { return x.side === "LONG"; }));
    r.short = sideStats(t.filter(function (x) { return x.side === "SHORT"; }));
    return r;
  }

  // P&L de cada día natural entre la primera entrada y la última salida (0 si no hubo cierres).
  // activeDays (Set o array de claves de día, opcional): solo esos días.
  function calendarDailyPnl(days, firstTs, lastTs, tz, activeDays) {
    var byKey = {}, out = [], seen = {}, act = null;
    if (activeDays) {
      act = {};
      (Array.isArray(activeDays) ? activeDays : Array.from(activeDays)).forEach(function (k) { act[k] = 1; });
    }
    for (var i = 0; i < days.length; i++) byKey[days[i].key] = days[i].net;
    var cursor = firstTs, lastKey = dayKey(lastTs, tz);
    for (var guard = 0; guard < 200000; guard++) {
      var k = dayKey(cursor, tz);
      if (!seen[k]) { seen[k] = 1; if (!act || act[k] || byKey[k] != null) out.push(byKey[k] || 0); }
      if (k === lastKey) break;
      cursor += DAY / 2;                    // medio día: robusto ante cambios de hora
    }
    return out;
  }

  function sideStats(a) {
    var s = { n: a.length, wins: 0, net: 0, fees: 0, win_rate: null, avg: null };
    for (var i = 0; i < a.length; i++) { s.net += a[i].pnl; s.fees += a[i].fee; if (a[i].pnl > 0) s.wins++; }
    if (a.length) { s.win_rate = s.wins / a.length * 100; s.avg = s.net / a.length; }
    return s;
  }

  // ── agrupaciones ─────────────────────────────────────────────────────────
  function summarizeGroup(key, a) {
    var g = { key: key, n: a.length, wins: 0, losses: 0, net: 0, gross: 0, fees: 0, notional: 0,
              sum_win: 0, sum_loss: 0, win_rate: null, avg: null, profit_factor: null, avg_duration: null,
              net_bps: null };
    var dur = 0;
    for (var i = 0; i < a.length; i++) {
      var x = a[i];
      g.net += x.pnl; g.gross += x.gross; g.fees += x.fee; g.notional += x.notional; dur += x.duration;
      if (x.pnl > 0) { g.wins++; g.sum_win += x.pnl; } else if (x.pnl < 0) { g.losses++; g.sum_loss += x.pnl; }
    }
    if (a.length) { g.win_rate = g.wins / a.length * 100; g.avg = g.net / a.length; g.avg_duration = dur / a.length; }
    if (g.notional > 0) g.net_bps = g.net / g.notional * 1e4;
    g.profit_factor = g.sum_loss < 0 ? g.sum_win / -g.sum_loss : (g.sum_win > 0 ? Infinity : null);
    return g;
  }

  // order: lista fija de claves (las vacías se omiten); sin ella, orden natural de la clave.
  function groupBy(trades, keyFn, order) {
    var map = {}, keys = [];
    for (var i = 0; i < trades.length; i++) {
      var k = keyFn(trades[i]);
      if (!map[k]) { map[k] = []; keys.push(k); }
      map[k].push(trades[i]);
    }
    if (order) keys = order.filter(function (k) { return map[k]; });
    else keys.sort(function (a, b) { return a < b ? -1 : (a > b ? 1 : 0); });
    return keys.map(function (k) { return summarizeGroup(k, map[k]); });
  }

  // Por periodo (cronológico, por hora de SALIDA): balance al empezar y al cerrar el periodo,
  // ROI del periodo sobre el capital base y peor drawdown DENTRO del periodo (desde su propio pico).
  function groupPeriods(trades, period, tz, startEquity) {
    var keyOf = PERIOD_KEY[period] || dayKey;
    var t = trades.slice().sort(byExit);
    var groups = [], cur = null, bal = startEquity, peak = startEquity;
    for (var i = 0; i < t.length; i++) {
      var k = keyOf(t[i].exit_ts, tz);
      if (!cur || cur.key !== k) {
        cur = { key: k, items: [], bal_start: bal, max_dd: 0 };
        groups.push(cur);
        peak = bal;
      }
      cur.items.push(t[i]);
      bal += t[i].pnl;
      if (bal > peak) peak = bal;
      if (peak - bal > cur.max_dd) cur.max_dd = peak - bal;
    }
    return groups.map(function (gr) {
      var s = summarizeGroup(gr.key, gr.items);
      s.bal_start = gr.bal_start;
      s.bal_end = gr.bal_start + s.net;
      s.roi_pct = s.net / startEquity * 100;
      s.max_dd = gr.max_dd;
      return s;
    });
  }

  function fullRange(groups, size, labelOf) {
    var full = [];
    for (var k = 0; k < size; k++) {
      var found = null;
      for (var i = 0; i < groups.length; i++) if (+groups[i].key === k) found = groups[i];
      var s = found || summarizeGroup(k, []);
      s.key = k;
      s.label = labelOf(k);
      full.push(s);
    }
    return full;
  }
  function byHour(trades, tz) {
    return fullRange(groupBy(trades, function (x) { return parts(x.entry_ts, tz).hour; }), 24,
                     function (h) { return pad(h) + "h"; });
  }
  function byWeekday(trades, tz) {
    return fullRange(groupBy(trades, function (x) { return parts(x.entry_ts, tz).wd; }), 7,
                     function (d) { return WEEKDAYS[d]; });
  }

  // Tramos calibrados con el log real (mediana 9,7 s; p95 77 s; máx. 13 min) y con margen
  // para estrategias más lentas.
  var DURATION_BUCKETS = [
    { key: "< 1 s", max: 1 }, { key: "1-5 s", max: 5 }, { key: "5-15 s", max: 15 },
    { key: "15-60 s", max: 60 }, { key: "1-5 min", max: 300 }, { key: "5-30 min", max: 1800 },
    { key: "> 30 min", max: Infinity },
  ];
  function durationBucket(s) {
    for (var i = 0; i < DURATION_BUCKETS.length; i++) if (s < DURATION_BUCKETS[i].max) return DURATION_BUCKETS[i].key;
    return DURATION_BUCKETS[DURATION_BUCKETS.length - 1].key;
  }
  function byDuration(trades) {
    return groupBy(trades, function (x) { return durationBucket(x.duration); },
                   DURATION_BUCKETS.map(function (b) { return b.key; }));
  }

  // Histograma: los extremos (fuera de p1-p99) se suman a la primera y la última barra para
  // que un outlier no aplaste el resto; lo dicen clippedLo/clippedHi.
  function histogram(values, bins) {
    bins = bins || 40;
    var a = values.filter(isNum).slice().sort(function (x, y) { return x - y; });
    if (!a.length) return { edges: [], counts: [], lo: null, hi: null, clippedLo: 0, clippedHi: 0 };
    var lo = a[Math.floor((a.length - 1) * 0.01)], hi = a[Math.ceil((a.length - 1) * 0.99)];
    if (hi <= lo) { lo = a[0]; hi = a[a.length - 1]; }
    if (hi <= lo) { lo -= 0.5; hi += 0.5; }
    var w = (hi - lo) / bins, counts = [], edges = [], cLo = 0, cHi = 0;
    for (var i = 0; i < bins; i++) { counts.push(0); edges.push(lo + i * w); }
    edges.push(hi);
    for (i = 0; i < a.length; i++) {
      if (a[i] < lo) cLo++;
      if (a[i] > hi) cHi++;
      var k = Math.floor((a[i] - lo) / w);
      if (k < 0) k = 0;
      if (k >= bins) k = bins - 1;
      counts[k]++;
    }
    return { edges: edges, counts: counts, lo: lo, hi: hi, clippedLo: cLo, clippedHi: cHi };
  }

  // Ordenación estable por cualquier campo; los null/undefined siempre al final.
  function sortTrades(trades, key, dir) {
    var s = dir === "desc" ? -1 : 1;
    return trades.map(function (x, i) { return [x, i]; }).sort(function (A, B) {
      var a = A[0][key], b = B[0][key];
      if (a === b) return A[1] - B[1];
      if (a === undefined || a === null) return 1;
      if (b === undefined || b === null) return -1;
      return (a < b ? -1 : 1) * s;
    }).map(function (p) { return p[0]; });
  }

  // P&L acumulado y balance tras cada trade (orden de salida), por id.
  function runningBalances(trades, startEquity) {
    var t = trades.slice().sort(byExit), out = {}, bal = startEquity, cum = 0;
    for (var i = 0; i < t.length; i++) { cum += t[i].pnl; bal += t[i].pnl; out[t[i].id] = { cum: cum, bal: bal }; }
    return out;
  }

  // Punto de la curva de balance más cercano a ts (búsqueda binaria; la curva va ordenada).
  function nearestIndex(points, ts) {
    if (!points.length) return -1;
    var lo = 0, hi = points.length - 1;
    while (hi - lo > 1) {
      var mid = (lo + hi) >> 1;
      if (points[mid].ts < ts) lo = mid; else hi = mid;
    }
    return Math.abs(points[lo].ts - ts) <= Math.abs(points[hi].ts - ts) ? lo : hi;
  }

  // ── formato (español: miles con punto, decimales con coma) ───────────────
  var EM = "—";
  function fmtNum(x, d) {
    if (!isNum(x)) return EM;
    d = d == null ? 2 : d;
    var s = Math.abs(x).toFixed(d), dot = s.indexOf(".");
    var ip = dot < 0 ? s : s.slice(0, dot), fp = dot < 0 ? "" : s.slice(dot + 1);
    ip = ip.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
    var out = fp ? ip + "," + fp : ip;
    return x < 0 && /[1-9]/.test(out) ? "-" + out : out;      // nunca "-0,00"
  }
  function fmtSigned(x, d) {
    var s = fmtNum(x, d);
    return s !== EM && x > 0 && /[1-9]/.test(s) ? "+" + s : s;
  }
  function fmtEur(x, d, signed) {
    if (!isNum(x)) return EM;
    return (signed ? fmtSigned(x, d == null ? 2 : d) : fmtNum(x, d == null ? 2 : d)) + " €";
  }
  function fmtPct(x, d, signed) {
    if (x === Infinity) return "∞";
    if (!isNum(x)) return EM;
    return (signed ? fmtSigned(x, d == null ? 2 : d) : fmtNum(x, d == null ? 2 : d)) + " %";
  }
  function fmtRatio(x, d) {
    if (x === Infinity) return "∞";
    return fmtNum(x, d == null ? 2 : d);
  }
  function fmtDuration(s) {
    if (!isNum(s)) return EM;
    if (s < 60) return fmtNum(s, s < 10 ? 2 : 1) + " s";
    var m = Math.floor(s / 60), sec = Math.floor(s - m * 60);
    if (s < 3600) return m + " min " + pad(sec) + " s";
    var h = Math.floor(s / 3600), mm = Math.floor((s - h * 3600) / 60);
    if (s < DAY) return h + " h " + pad(mm) + " min";
    var dd = Math.floor(s / DAY), hh = Math.floor((s - dd * DAY) / 3600);
    return dd + " d " + pad(hh) + " h";
  }
  function fmtDate(ts, tz, withSeconds) {
    if (!isNum(ts)) return EM;
    var p = parts(ts, tz);
    return pad(p.d) + "/" + pad(p.m + 1) + "/" + p.y + " " + pad(p.hour) + ":" + pad(p.min) +
           (withSeconds ? ":" + pad(p.sec) : "");
  }
  function fmtDayKey(key) {              // "2026-10-21" → "21/10/2026"; otras claves, tal cual
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(key);
    return m ? m[3] + "/" + m[2] + "/" + m[1] : key;
  }

  // ── CSV: "es" (separador ; y coma decimal, abre bien en Excel en español) o
  //    "std" (separador , y punto decimal, para pandas, R, etc.) ────────────
  function isoTime(ts, tz) {
    var p = parts(ts, tz), ms = Math.floor((ts - Math.floor(ts)) * 1000 + 1e-6);
    return p.y + "-" + pad(p.m + 1) + "-" + pad(p.d) + " " + pad(p.hour) + ":" + pad(p.min) + ":" +
           pad(p.sec) + "." + (ms < 10 ? "00" : ms < 100 ? "0" : "") + ms;
  }
  function toCSV(trades, balances, tz, format) {
    var es = format !== "std", sep = es ? ";" : ",";
    function num(x, d) {
      if (!isNum(x)) return "";
      var s = d == null ? String(x) : x.toFixed(d);
      return es ? s.replace(".", ",") : s;
    }
    var head = ["id", "par", "lado", "entrada_" + tz, "salida_" + tz, "duracion_s", "precio_entrada",
                "precio_salida", "movimiento_pb", "tamano", "nocional", "pnl_bruto", "comision",
                "pnl_neto", "rentabilidad_pct", "pnl_acumulado", "balance", "motivo",
                "pnl_neto_registrado", "nocional_registrado", "stop_pb", "espacio_pb", "conf_zona"];
    var lines = [head.join(sep)];
    for (var i = 0; i < trades.length; i++) {
      var x = trades[i], b = balances[x.id] || {};
      lines.push([x.id, x.symbol, x.side, isoTime(x.entry_ts, tz), isoTime(x.exit_ts, tz),
                  num(x.duration, 3), num(x.entry), num(x.exit), num(x.move_bps, 3), num(x.size),
                  num(x.notional, 2), num(x.gross, 6), num(x.fee, 6), num(x.pnl, 6), num(x.return_pct, 6),
                  num(b.cum, 6), num(b.bal, 6), x.reason,
                  num(x.pnl_real != null ? x.pnl_real : x.pnl, 6), num(x.notional_real != null ? x.notional_real : x.notional, 2),
                  num(x.stop_bps), num(x.room_bps), num(x.zone_conf)].join(sep));
    }
    return lines.join("\r\n") + "\r\n";
  }

  return {
    DAY: DAY, WEEKDAYS: WEEKDAYS, PRESETS: PRESETS, DURATION_BUCKETS: DURATION_BUCKETS,
    MIN_ANNUAL_DAYS: MIN_ANNUAL_DAYS,
    parts: parts, mkTs: mkTs, dayKey: dayKey, weekKey: weekKey, monthKey: monthKey, yearKey: yearKey,
    periodBounds: periodBounds, parseDateInput: parseDateInput, toDateInput: toDateInput,
    isoWeekMonday: isoWeekMonday, periodRange: periodRange, parseNum: parseNum, parseDur: parseDur,
    fromRows: fromRows, enrich: enrich, applyFilters: applyFilters, hourMatches: hourMatches,
    normalize: normalize, runIndex: runIndex, groupRuns: groupRuns,
    compute: compute, calendarDailyPnl: calendarDailyPnl, sideStats: sideStats,
    summarizeGroup: summarizeGroup, groupBy: groupBy, groupPeriods: groupPeriods,
    byHour: byHour, byWeekday: byWeekday, byDuration: byDuration, durationBucket: durationBucket,
    histogram: histogram, sortTrades: sortTrades, runningBalances: runningBalances,
    nearestIndex: nearestIndex, toCSV: toCSV, isoTime: isoTime,
    mean: mean, std: std, median: median,
    fmtNum: fmtNum, fmtSigned: fmtSigned, fmtEur: fmtEur, fmtPct: fmtPct, fmtRatio: fmtRatio,
    fmtDuration: fmtDuration, fmtDate: fmtDate, fmtDayKey: fmtDayKey,
  };
});
