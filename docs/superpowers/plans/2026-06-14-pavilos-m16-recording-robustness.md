# PAVILOS M16: Recording Robustness (unattended-survival) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development, with a THIRD barrier per task — adversarial verification. Steps use checkbox (`- [ ]`).

**Goal:** Make the live recorder survive ANY single failure unattended, so multi-week
recording has near-zero gaps. Two layers: (1) a **global asyncio exception handler** so a
flaky venue's internal ccxt WS callback exception (e.g. bitfinex `amount=None` TypeError in
`Client.receive_loop()`) is LOGGED and the loop keeps running instead of escalating; (2) an
**outer auto-restart supervisor** that relaunches `python -m pavilos` within seconds of ANY
process death (crash, OOM, exit-4) with crash-loop-safe backoff. The recorder already
self-reconnects per connector; this closes the whole-process-death gap that caused the two
observed multi-hour recording stops.

**Architecture:** A pure `_loop_exception_handler(loop, context)` installed at the top of
`Runtime.run_app` via `set_exception_handler`. A standalone `scripts/run_recorder.py`
supervisor: loop → run the child → on exit, log + back off (reset backoff after a healthy
run, escalate on a tight crash-loop) → relaunch, until interrupted. Both fully unit-testable
via injection (no real server, no network).

**Tech Stack:** Python 3.13, stdlib `asyncio`/`subprocess`. `pytest`.

---

## Design constraints
- The handler LOGS loudly (error level, with traceback) and CONTINUES — it must NOT mask a
  clean shutdown (ignore `CancelledError`/`SystemExit`) and must never raise.
- The supervisor must NOT hot-loop: if the child dies almost immediately (crash-loop), back
  off exponentially up to a cap; if the child ran healthily (e.g. ≥ `healthy_run_s`), reset
  backoff so an occasional ~daily crash relaunches FAST (small gap).
- The supervisor must stop cleanly on KeyboardInterrupt/SIGTERM (operator stop), and support
  an injected runner + clock + stop predicate for tests (never spawn the real server in tests).
- Nothing changes the recorded DATA or the connectors' logic; this is purely supervision.

## File Structure
```
src/pavilos/core/runtime.py     # + _loop_exception_handler, installed in run_app  [MODIFY]
scripts/run_recorder.py          # auto-restart supervisor CLI                       [NEW]
tests/unit/test_runtime_exception_handler.py
tests/unit/test_run_recorder.py
```

---

## Task 1: Global asyncio loop exception handler

**Files:** Modify `src/pavilos/core/runtime.py`; Test `tests/unit/test_runtime_exception_handler.py`.

- [ ] **Step 1: Failing test:**
```python
# tests/unit/test_runtime_exception_handler.py
import asyncio
import logging
from pavilos.core.runtime import _loop_exception_handler


def test_logs_callback_exception_and_does_not_raise(caplog):
    loop = asyncio.new_event_loop()
    try:
        with caplog.at_level(logging.ERROR):
            # must NOT raise, regardless of the context shape
            _loop_exception_handler(loop, {"message": "Exception in callback Client.receive_loop()",
                                           "exception": TypeError("'<' not supported between ... NoneType")})
        assert any("Client.receive_loop" in r.message or "loop exception" in r.message.lower()
                   for r in caplog.records)
    finally:
        loop.close()


def test_ignores_cancellation_and_handles_missing_exception(caplog):
    loop = asyncio.new_event_loop()
    try:
        _loop_exception_handler(loop, {"exception": asyncio.CancelledError()})   # benign
        _loop_exception_handler(loop, {"message": "no exception object here"})    # must not raise
    finally:
        loop.close()
```
- [ ] **Step 2:** Run → FAIL (no `_loop_exception_handler`).
- [ ] **Step 3: Implement in `runtime.py`** (module-level function + install it):
```python
def _loop_exception_handler(loop, context: dict) -> None:
    """Log an unhandled exception that surfaced in an asyncio callback/task (e.g. a
    flaky venue's internal ccxt WS callback) and let the loop KEEP RUNNING — one bad
    venue must never silently kill 24/7 recording. Never raises; ignores benign
    cancellation/shutdown. The outer supervisor handles whole-process death."""
    exc = context.get("exception")
    if isinstance(exc, (asyncio.CancelledError, SystemExit, KeyboardInterrupt)):
        return
    msg = context.get("message") or "unhandled asyncio exception"
    _log.error("asyncio loop exception (logged, recorder continues): %s", msg, exc_info=exc)
```
  And at the START of `run_app` (after `stop = stop or asyncio.Event()`):
```python
        asyncio.get_running_loop().set_exception_handler(_loop_exception_handler)
```
- [ ] **Step 4:** Run → pass. **Step 5:** full suite. **Step 6:** Commit `feat(runtime): global asyncio exception handler so a flaky venue callback never kills the recorder`.

---

## Task 2: Auto-restart supervisor

**Files:** Create `scripts/run_recorder.py`; Test `tests/unit/test_run_recorder.py`.

- [ ] **Step 1: Failing test:**
```python
# tests/unit/test_run_recorder.py
from scripts.run_recorder import supervise


def test_relaunches_on_crash_until_stopped():
    runs = []
    calls = {"n": 0}
    def fake_run(cmd):
        calls["n"] += 1
        runs.append(cmd)
        return 4 if calls["n"] < 3 else 0      # crash twice, then "clean"
    stop_after = {"n": 0}
    def keep_going():
        stop_after["n"] += 1
        return stop_after["n"] <= 3            # allow up to 3 launches
    slept = []
    n = supervise(["x"], _run=fake_run, _sleep=slept.append, _now=lambda: 0.0,
                  should_continue=keep_going, backoff_s=2.0)
    assert calls["n"] == 3                      # relaunched after each crash
    assert n == 3


def test_backoff_escalates_on_tight_crashloop_then_caps():
    calls = {"n": 0}
    def fake_run(cmd):
        calls["n"] += 1
        return 1                                 # always crashes immediately
    t = {"v": 0.0}
    def now():                                    # child "runs" 0s each time (tight loop)
        return t["v"]
    slept = []
    supervise(["x"], _run=fake_run, _sleep=lambda s: slept.append(s), _now=now,
              should_continue=lambda: calls["n"] < 5, backoff_s=1.0, max_backoff_s=8.0)
    # backoff doubles on consecutive instant crashes, capped at max
    assert slept and slept[0] == 1.0 and max(slept) <= 8.0 and slept == sorted(slept)[:len(slept)]


def test_backoff_resets_after_a_healthy_run():
    calls = {"n": 0}
    def fake_run(cmd):
        calls["n"] += 1
        return 1
    times = iter([0.0, 600.0,    # 1st child ran 600s (healthy) -> reset
                  600.0, 600.5]) # 2nd child ran 0.5s (crash-loop)
    def now():
        return next(times)
    slept = []
    supervise(["x"], _run=fake_run, _sleep=lambda s: slept.append(s), _now=now,
              should_continue=lambda: calls["n"] < 2, backoff_s=3.0, healthy_run_s=60.0)
    assert slept[0] == 3.0      # after a healthy run, backoff is the base (fast relaunch)
```
  NOTE to implementer: adapt the exact `_now`/`should_continue` call counts to your loop
  structure — the INVARIANTS that must hold: (a) relaunch after every non-clean exit while
  `should_continue()`; (b) exponential backoff on consecutive SHORT (< `healthy_run_s`) runs,
  capped at `max_backoff_s`; (c) backoff RESETS to `backoff_s` after a run ≥ `healthy_run_s`;
  (d) returns the number of launches; (e) never raises on child non-zero exit.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3: Implement `scripts/run_recorder.py`:**
```python
# scripts/run_recorder.py
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
    """Run ``cmd`` repeatedly until ``should_continue()`` is False. Returns launch count."""
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
        if not should_continue():
            break
        backoff = backoff_s if ran >= healthy_run_s else min(backoff * 2, max_backoff_s)
        # (first backoff after a short run is backoff_s; doubles only on the NEXT short run)
        _sleep(backoff if ran < healthy_run_s else backoff_s)
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
```
  NOTE: ensure the backoff logic matches the tests' invariants exactly (reset after healthy
  run; escalate on consecutive short runs; first sleep after a short run = `backoff_s`). Tune
  the implementation so all three tests pass; keep the semantics documented above.
- [ ] **Step 4:** Run → pass. **Step 5:** `python -c "import scripts.run_recorder"` + full suite. **Step 6:** Commit `feat(scripts): auto-restart supervisor for unattended recording`.

---

## Task 3: Close-out
- [ ] **Step 1:** Full suite green (≈308 prior + ~5 new). **Step 2:** `python -c "import pavilos.core.runtime, scripts.run_recorder; print('OK')"`. **Step 3:** `git tag m16-recording-robustness`. **Step 4:** Commit if any close-out fixes.
- [ ] **Step 5 (operator, NOT in workflow):** stop the direct `python -m pavilos`, relaunch via `python -m scripts.run_recorder`; optionally add a Windows startup entry so the supervisor itself survives a machine reboot (the two observed stops were machine-level).

---

## Self-Review (plan author)
**Coverage:** global handler (T1, stops a venue callback from escalating) + supervisor (T2, relaunches on any whole-process death) + closeout/deploy (T3). Together = near-zero-gap unattended recording.
**Correctness/safety:** handler logs loudly + continues, ignores cancellation, never raises; supervisor is crash-loop-safe (backoff + reset), stop-clean, fully injection-tested (no real server/network). No change to recorded data or connector logic.
**Type consistency:** `_loop_exception_handler(loop, context: dict) -> None`; `supervise(cmd, *, backoff_s, max_backoff_s, healthy_run_s, _run, _sleep, _now, should_continue) -> int`.
**Adversarial focus (3rd barrier):** (1) **crash-loop safety** — a child that dies instantly must NOT hot-spin (CPU/log flood): assert backoff escalates + caps. (2) **fast recovery** — after a healthy multi-hour run, a crash relaunches at the BASE backoff (small gap), not the escalated one (reset works). (3) **clean stop** — Ctrl-C / should_continue False exits the loop promptly, no extra launch. (4) **handler never masks shutdown** — CancelledError/SystemExit ignored; a real fatal (e.g. KeyboardInterrupt in the child) still stops. (5) **handler never raises** — malformed context (no exception key) is safe. Item (1) crash-loop safety + (3) clean stop are the headline (a supervisor that hot-loops or won't stop is worse than none).
