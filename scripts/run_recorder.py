"""Auto-restart supervisor for the PAVILOS recorder: relaunch `python -m pavilos`
within seconds of ANY process death (crash, OOM, exit-4) so unattended multi-week
recording has near-zero gaps. Crash-loop-safe: exponential backoff on consecutive
short-lived runs (capped), reset after a healthy run. Stop with Ctrl-C.

    python -m scripts.run_recorder
"""
from __future__ import annotations

import logging
import subprocess
import sys
import time

_log = logging.getLogger("pavilos.supervisor")


def supervise(cmd, *, backoff_s: float = 3.0, max_backoff_s: float = 60.0,
              healthy_run_s: float = 60.0, _run=subprocess.call, _sleep=time.sleep,
              _now=time.monotonic, should_continue=lambda: True) -> int:
    """Run ``cmd`` repeatedly until ``should_continue()`` is False, returning the
    number of launches.

    Invariants:
      (a) relaunch after every non-clean exit while ``should_continue()`` holds;
      (b) exponential backoff on consecutive SHORT (< ``healthy_run_s``) runs,
          capped at ``max_backoff_s`` — a child that dies instantly must NOT
          hot-spin (CPU/log flood);
      (c) backoff RESETS to ``backoff_s`` after a run lasting >= ``healthy_run_s``,
          so an occasional ~daily crash relaunches FAST (small gap);
      (d) a clean child exit (code == 0) stops the loop;
      (e) never raises on a child non-zero exit; KeyboardInterrupt stops cleanly.
    """
    launches = 0
    backoff = backoff_s
    while should_continue():
        start = _now()
        launches += 1
        try:
            code = _run(cmd)
        except KeyboardInterrupt:
            break
        ran = _now() - start
        _log.warning("recorder exited (code=%s) after %.0fs; restart #%d", code, ran, launches)
        if code == 0:
            break  # clean stop requested by the child
        # Reset backoff after a healthy run, then sleep the current backoff and
        # escalate only on consecutive short (crash-loop) runs.
        if ran >= healthy_run_s:
            backoff = backoff_s
        _sleep(backoff)
        if ran < healthy_run_s:
            backoff = min(backoff * 2, max_backoff_s)
    return launches


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cmd = [sys.executable, "-m", "pavilos"]
    _log.info("supervising: %s (Ctrl-C to stop)", " ".join(cmd))
    try:
        supervise(cmd)
    except KeyboardInterrupt:
        _log.info("supervisor stopped")


if __name__ == "__main__":
    main()
