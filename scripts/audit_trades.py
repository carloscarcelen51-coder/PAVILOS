"""Audit of the paper trade log -- read only, it never modifies the log.

    python -m scripts.audit_trades                       # report
    python -m scripts.audit_trades --csv annotated.csv   # + one annotated row per trade

Until 2026-10-23 every process start reset the paper account to 10,000 and lost any open
position, and trades were sized at 1 % of what was left of the session's account. So the
log's euro totals sum one account per restart, and sizes go from a few euros to ~96,000.
This script rebuilds the sessions (process starts) and reports the strategy in basis points
(independent of size). The annotated CSV is the "clean" view of the log:
- session, account before the trade, the stop distance deduced from the size;
- whether the leverage cap bound, and whether the trade is from the setup period;
- the P&L at a fixed 10,000 notional.
Nothing is dropped: flags mark trades, they do not remove them.

Sessions: the lake's ``_runs.jsonl`` (one line per start since 2026-06-30) plus the June starts,
deduced here the same way as ``pavilos.web.history``. A trade whose notional exceeds 10x the
account rebuilt since the previous start (the leverage cap) proves a restart before it."""
from __future__ import annotations

import argparse
import csv
import json
import statistics as st
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

START_EQUITY = 10_000.0
REF_NOTIONAL = 10_000.0
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pavilos.web.history import STABLE_SETUP_SINCE  # noqa: E402


def utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, timezone.utc)


def load_trades(path: Path) -> list[dict]:
    out = []
    with path.open(encoding="utf-8") as f:
        for i, line in enumerate(f):
            try:
                d = json.loads(line)
            except ValueError:
                continue
            d["id"] = i
            d["notional"] = d["entry"] * d["size"]
            if d["notional"] <= 0:
                continue
            sgn = -1 if d["side"] == "SHORT" else 1
            d["gross"] = d["pnl"] + d["fee"]
            d["dur"] = d["exit_ts"] - d["entry_ts"]
            d["move_bps"] = sgn * (d["exit"] - d["entry"]) / d["entry"] * 1e4
            d["net_bps"] = d["pnl"] / d["notional"] * 1e4
            out.append(d)
    return out


def deduce_starts(trades: list[dict], first_known: float | None) -> list[float]:
    pre = [t for t in trades if first_known is None or t["entry_ts"] < first_known]
    if not pre:
        return []
    starts = [round(pre[0]["entry_ts"] - 1, 3)]
    eq, prev_exit = START_EQUITY, None
    for t in pre:
        gap = (t["entry_ts"] - prev_exit) if prev_exit else 0.0
        if t["notional"] > 10 * eq * 1.005 or (gap > 1800 and t["notional"] > 10 * eq * 0.5 and eq < START_EQUITY * 0.6):
            starts.append(round(t["entry_ts"] - 1, 3))
            eq = START_EQUITY
        eq += t["pnl"]
        prev_exit = t["exit_ts"]
    return starts


def annotate(trades: list[dict], starts: list[float]) -> None:
    starts = sorted(starts)
    j, eq, prev_run = -1, START_EQUITY, None
    for t in trades:
        while j + 1 < len(starts) and starts[j + 1] <= t["entry_ts"]:
            j += 1
        if j != prev_run:
            eq, prev_run = START_EQUITY, j
        t["run"] = j
        t["equity_before"] = eq
        t["capped"] = t["size"] >= 10 * eq / t["entry"] * 0.995 if eq > 0 else False
        t["stop_bps_deduced"] = ((eq * 0.01 / t["size"]) / t["entry"] * 1e4
                                 if (not t["capped"] and eq > 0 and t["size"] > 0 and not t.get("context")) else None)
        t["setup_period"] = t["entry_ts"] < STABLE_SETUP_SINCE
        t["pnl_at_ref"] = t["net_bps"] * REF_NOTIONAL / 1e4
        eq += t["pnl"]


def bps(sub: list[dict]) -> str:
    if not sub:
        return "—"
    notl = sum(t["notional"] for t in sub)
    g = sum(t["gross"] for t in sub) / notl * 1e4
    n = sum(t["pnl"] for t in sub) / notl * 1e4
    wr = sum(1 for t in sub if t["pnl"] > 0) / len(sub) * 100
    return f"n={len(sub):6d}  bruto {g:+6.2f} pb  neto {n:+7.2f} pb  WR {wr:5.1f} %"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log", default=str(ROOT / "paper_trades.jsonl"))
    ap.add_argument("--runs", default=r"D:\pavilos_book_data\_runs.jsonl")
    ap.add_argument("--csv", help="write one annotated row per trade here")
    a = ap.parse_args()

    trades = load_trades(Path(a.log))
    known = []
    if Path(a.runs).exists():
        for line in Path(a.runs).read_text(encoding="utf-8").splitlines():
            try:
                known.append(float(json.loads(line)["start_ts"]))
            except (ValueError, KeyError):
                continue
    starts = sorted(set(deduce_starts(trades, min(known) if known else None) + known))
    annotate(trades, starts)
    stable = [t for t in trades if not t["setup_period"]]

    print(f"trades: {len(trades)}  ({utc(trades[0]['entry_ts']):%Y-%m-%d} -> {utc(trades[-1]['exit_ts']):%Y-%m-%d})")
    print(f"sesiones (arranques): {len(starts)} · con trades: {len({t['run'] for t in trades})}")
    print(f"P&L registrado: {sum(t['pnl'] for t in trades):,.2f} · a {REF_NOTIONAL:,.0f} por trade: "
          f"{sum(t['pnl_at_ref'] for t in trades):,.2f}")
    print(f"puesta en marcha (antes de {utc(STABLE_SETUP_SINCE):%Y-%m-%d %H:%MZ}): "
          f"{sum(1 for t in trades if t['setup_period'])} trades")
    print(f"trades con < 1.000 de nocional (cuenta de la sesión casi gastada): "
          f"{sum(1 for t in trades if t['notional'] < 1000)}")
    print("\nTOTAL (sin la puesta en marcha):", bps(stable))
    print("\nPOR DURACIÓN")
    for lo, hi, lab in [(0, 1, "< 1 s"), (1, 10, "1-10 s"), (10, 60, "10-60 s"), (60, 1e9, ">= 1 min")]:
        print(f"  {lab:10}", bps([t for t in stable if lo <= t["dur"] < hi]))
    print("\nPOR MES")
    for m in sorted({utc(t["entry_ts"]).strftime("%Y-%m") for t in stable}):
        print(f"  {m:10}", bps([t for t in stable if utc(t["entry_ts"]).strftime("%Y-%m") == m]))
    print("\nPOR SESIÓN (P&L registrado · nocional medio de los 10 primeros -> 10 últimos)")
    by = defaultdict(list)
    for t in trades:
        by[t["run"]].append(t)
    for k in sorted(by):
        sub = by[k]
        m = min(10, len(sub))
        first = st.mean(t["notional"] for t in sub[:m])
        last = st.mean(t["notional"] for t in sub[-m:])
        print(f"  S{k + 1:<3} {utc(starts[k]):%Y-%m-%d %H:%MZ}  {bps(sub)}  registrado {sum(t['pnl'] for t in sub):11,.2f}"
              f"  nocional {first:9,.0f} -> {last:9,.0f}")
    notl = sum(t["notional"] for t in stable)
    g = sum(t["gross"] for t in stable) / notl * 1e4
    print("\nCON OTRAS COMISIONES (mismos trades)")
    for lab, fee in [("taker+taker 10 pb", 10), ("maker+taker 7 pb", 7), ("maker+maker 4 pb", 4)]:
        print(f"  {lab:20} neto ~ {g - fee:+.2f} pb/trade")

    if a.csv:
        cols = ["id", "run", "setup_period", "side", "reason", "entry_ts", "exit_ts", "dur", "entry", "exit",
                "size", "notional", "equity_before", "capped", "stop_bps_deduced", "move_bps", "gross", "fee",
                "pnl", "net_bps", "pnl_at_ref"]
        with open(a.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for t in trades:
                w.writerow([t.get(c) for c in cols])
        print(f"\nCSV anotado: {a.csv} ({len(trades)} filas)")


if __name__ == "__main__":
    main()
