import os
from pavilos.persistence.retention import prune_old_partitions


def _mk(base, exchange, date):
    p = os.path.join(base, f"exchange={exchange}", f"date={date}", "00")
    os.makedirs(p, exist_ok=True)
    open(os.path.join(p, "000000.parquet"), "w").close()


def test_prune_deletes_date_partitions_older_than_retention(tmp_path):
    base = str(tmp_path)
    _mk(base, "kraken", "2026-06-01")   # old
    _mk(base, "kraken", "2026-06-08")   # fresh
    # 'now' = 2026-06-08 -> retention 3 days keeps >= 2026-06-05
    removed = prune_old_partitions(base, retention_days=3, now_date="2026-06-08")
    dates = {d for d in os.listdir(os.path.join(base, "exchange=kraken"))}
    assert "date=2026-06-01" not in dates and "date=2026-06-08" in dates
    assert removed == 1


def test_prune_move_to_cold(tmp_path):
    base = str(tmp_path / "hot"); cold = str(tmp_path / "cold")
    _mk(base, "okx", "2026-05-01")
    prune_old_partitions(base, retention_days=1, now_date="2026-06-08", move_to=cold)
    assert os.path.exists(os.path.join(cold, "exchange=okx", "date=2026-05-01"))
    assert not os.path.exists(os.path.join(base, "exchange=okx", "date=2026-05-01"))


def test_prune_skips_malformed_date_entries_and_still_prunes_valid(tmp_path):
    base = str(tmp_path)
    _mk(base, "kraken", "2026-06-01")   # old, valid -> should be pruned
    ex = os.path.join(base, "exchange=kraken")
    # a stray non-date name + a malformed date= name + a file (not a dir)
    os.makedirs(os.path.join(ex, "date=2026-06-08x.tmp"), exist_ok=True)
    os.makedirs(os.path.join(ex, "date=garbage"), exist_ok=True)
    open(os.path.join(ex, "date=stray.parquet"), "w").close()
    # must NOT raise; valid old partition is still pruned
    removed = prune_old_partitions(base, retention_days=3, now_date="2026-06-08")
    dates = set(os.listdir(ex))
    assert "date=2026-06-01" not in dates       # valid old one pruned
    assert "date=garbage" in dates              # malformed entries left untouched
    assert removed == 1


# --- M17: eternal retention + disk floor (pause, never delete) -------------------

import logging
import time
from types import SimpleNamespace

from pavilos.persistence.retention import RetentionJanitor, free_gb

_GB = 1024 ** 3


def _disk(free_gb_value, calls=None):
    def usage(path):
        if calls is not None:
            calls.append(path)
        return SimpleNamespace(total=1000 * _GB, used=0, free=free_gb_value * _GB)
    return usage


def test_free_gb_measures_the_nearest_existing_ancestor(tmp_path):
    calls = []
    missing = os.path.join(str(tmp_path), "lake", "not", "yet")
    assert free_gb(missing, disk_usage=_disk(42.0, calls)) == 42.0
    assert calls == [os.path.abspath(str(tmp_path))]


def test_default_policy_never_deletes_anything_however_old(tmp_path):
    base = str(tmp_path)
    _mk(base, "kraken", "2001-01-01")
    seen = []
    j = RetentionJanitor(base, retention_days=None, min_free_gb=100.0,
                         on_floor=seen.append, disk_usage=_disk(500.0))
    res = j.run_once()
    assert os.path.exists(os.path.join(base, "exchange=kraken", "date=2001-01-01"))
    assert res == {"aged_out": 0, "free_gb": 500.0, "floor_ok": True}
    assert seen == [True]


def test_below_the_floor_reports_pause_and_deletes_nothing(tmp_path, caplog):
    base = str(tmp_path)
    _mk(base, "kraken", "2001-01-01")
    seen = []
    j = RetentionJanitor(base, retention_days=None, min_free_gb=100.0,
                         on_floor=seen.append, disk_usage=_disk(99.9))
    with caplog.at_level(logging.ERROR, logger="pavilos.persistence.retention"):
        res = j.run_once()
    assert res["floor_ok"] is False and seen == [False]
    assert os.path.exists(os.path.join(base, "exchange=kraken", "date=2001-01-01"))
    assert any("PAUSED" in r.getMessage() for r in caplog.records)


def test_floor_transitions_are_reported_each_pass(tmp_path, caplog):
    free = {"v": 50.0}
    seen = []
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0, on_floor=seen.append,
                         disk_usage=lambda p: SimpleNamespace(free=free["v"] * _GB))
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.retention"):
        j.run_once()                     # below -> pause
        free["v"] = 150.0
        j.run_once()                     # back above -> resume (logged)
        j.run_once()                     # still fine
        free["v"] = 100.0
        j.run_once()                     # exactly AT the floor = enough
    assert seen == [False, True, True, True]
    assert any("resumes" in r.getMessage() for r in caplog.records)


def test_unmeasurable_disk_does_not_flip_the_recorder(tmp_path, caplog):
    def boom(path):
        raise OSError("device not ready")
    seen = []
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0,
                         on_floor=seen.append, disk_usage=boom)
    with caplog.at_level(logging.WARNING, logger="pavilos.persistence.retention"):
        res = j.run_once()
    assert seen == [] and res["floor_ok"] is None
    assert any("cannot measure" in r.getMessage() for r in caplog.records)


def test_no_floor_configured_never_calls_back(tmp_path):
    seen = []
    for floor in (None, 0.0):
        RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=floor,
                         on_floor=seen.append, disk_usage=_disk(1.0)).run_once()
    assert seen == []


def test_age_limit_is_opt_in(tmp_path):
    base = str(tmp_path)
    _mk(base, "kraken", "2001-01-01")
    j = RetentionJanitor(base, retention_days=3, min_free_gb=None, disk_usage=_disk(500.0))
    assert j.run_once()["aged_out"] == 1
    assert not os.path.exists(os.path.join(base, "exchange=kraken", "date=2001-01-01"))


def test_a_failing_callback_never_escapes_the_pass(tmp_path):
    def bad(ok):
        raise RuntimeError("boom")
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0,
                         on_floor=bad, disk_usage=_disk(1.0))
    assert j.run_once()["floor_ok"] is False


def test_janitor_thread_repeats_the_pass_and_stops_promptly(tmp_path):
    calls = []
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0,
                         every_s=0.02, disk_usage=_disk(500.0, calls))
    j.start()
    j.start()                            # idempotent: still one thread
    deadline = time.monotonic() + 5.0
    while len(calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    t0 = time.monotonic()
    j.stop(timeout=5.0)
    assert len(calls) >= 3
    assert time.monotonic() - t0 < 1.0
    n = len(calls)
    time.sleep(0.1)
    assert len(calls) == n               # no pass after stop()


def test_every_pass_below_the_floor_logs_an_error(tmp_path, caplog):
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0, disk_usage=_disk(10.0))
    with caplog.at_level(logging.ERROR, logger="pavilos.persistence.retention"):
        j.run_once()
        j.run_once()
        j.run_once()
    assert sum("PAUSED" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR) == 3


def test_janitor_thread_survives_a_failing_pass(tmp_path, monkeypatch):
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0,
                         every_s=0.02, disk_usage=_disk(500.0))
    n = {"c": 0}

    def boom():
        n["c"] += 1
        raise RuntimeError("bug")

    monkeypatch.setattr(j, "run_once", boom)
    j.start()
    deadline = time.monotonic() + 5.0
    while n["c"] < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    j.stop(timeout=5.0)
    assert n["c"] >= 3                                   # the thread kept going


def test_any_measurement_error_leaves_the_floor_state_alone(tmp_path):
    def bad(path):
        raise ValueError("weird driver answer")
    seen = []
    j = RetentionJanitor(str(tmp_path), retention_days=None, min_free_gb=100.0,
                         on_floor=seen.append, disk_usage=bad)
    assert j.run_once()["floor_ok"] is None and seen == []
