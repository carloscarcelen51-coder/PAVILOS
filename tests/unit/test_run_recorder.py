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
