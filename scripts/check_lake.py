# scripts/check_lake.py
"""Self-consistency of the recorded lake (M17): at every checkpoint, the book rebuilt
from the lake just before it must equal it. Read-only. Usage:

    python -m scripts.check_lake <data_dir> <t0> <t1>      (epoch seconds, UTC)
"""
from __future__ import annotations

import sys
import time

from pavilos.backtest.lake_check import check_checkpoints
from pavilos.core.runtime import RuntimeConfig


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__)
        return 2
    base, t0, t1 = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    fmt = lambda t: time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(t))
    print(f"lake {base}  window {fmt(t0)} -> {fmt(t1)}")
    res = check_checkpoints(base, t0, t1, rebase_after_s=RuntimeConfig().staleness_s)
    print(f"{'venue':<10} {'updates':>9} {'checks':>7} {'mismatch':>8} {'reorder':>7} {'losses':>6} "
          f"{'loss_mis':>8} {'bases':>6} {'unbased':>8}")
    bad = 0
    for ex in sorted(res):
        v = res[ex]
        bad += v["mismatches"] + v["reordered"]
        print(f"{ex:<10} {v['updates']:>9} {v['checks']:>7} {v['mismatches']:>8} {v['reordered']:>7} "
              f"{v['loss_marks']:>6} {v['loss_mismatches']:>8} {v['bases']:>6} {v['unbased']:>8}")
        for e in v["examples"]:
            print(f"    mismatch at {fmt(e['ts'])} ({e['ts']:.6f}): {e['levels_differing']} levels differ, "
                  f"replayed last ts {e['replayed_last_ts']:.6f}")
    losses = sum(v["loss_marks"] for v in res.values())
    print("RESULT:", "OK (0 unexplained mismatches, 0 reordered)" if bad == 0
          else f"{bad} UNEXPLAINED MISMATCHES/REORDERED",
          f"| {losses} loss(es) flagged by the recorder" if losses else "")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
