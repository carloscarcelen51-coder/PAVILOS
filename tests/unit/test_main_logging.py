# tests/unit/test_main_logging.py
import logging
import pathlib
import time

import pytest

from pavilos.__main__ import setup_logging


@pytest.fixture
def _restore_root_logging():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    for h in root.handlers:
        if h not in handlers:
            h.close()
    root.handlers[:] = handlers
    root.setLevel(level)


def test_info_goes_to_the_rotating_file_and_only_critical_to_stderr(tmp_path, capsys, _restore_root_logging):
    from logging.handlers import RotatingFileHandler
    path = setup_logging(tmp_path / "logs")
    log = logging.getLogger("pavilos.test")
    log.info("routine line")
    log.error("connector traceback")
    log.critical("fatal line")
    for h in logging.getLogger().handlers:
        h.flush()
    text = path.read_text(encoding="utf-8")
    assert "routine line" in text and "connector traceback" in text and "fatal line" in text
    err = capsys.readouterr().err
    assert "fatal line" in err
    assert "routine line" not in err and "connector traceback" not in err
    rot = [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]
    assert len(rot) == 1 and rot[0].maxBytes == 20 * 1024 * 1024 and rot[0].backupCount == 5


def test_rotation_keeps_the_log_bounded(tmp_path, _restore_root_logging, monkeypatch):
    import re
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 2_000)
    setup_logging(tmp_path / "logs")
    log = logging.getLogger("pavilos.test")
    for i in range(2_000):
        log.info("line %05d %s", i, "x" * 40)
        if i % 100 == 0:
            time.sleep(0.002)                     # distinct millisecond stamps
    for h in logging.getLogger().handlers:
        h.flush()
    files = sorted(p.name for p in (tmp_path / "logs").iterdir())
    rotated = [f for f in files if f != "pavilos.log"]
    assert "pavilos.log" in files and len(rotated) == 5
    assert all(re.fullmatch(r"pavilos\.\d{8}-\d{6}\.\d{6}\.log", f) for f in rotated)
    assert all((tmp_path / "logs" / f).stat().st_size <= 2_200 for f in files)
    assert "line 01999" in (tmp_path / "logs" / "pavilos.log").read_text(encoding="utf-8")


def test_main_sets_up_logging_first(monkeypatch, tmp_path):
    import pavilos.__main__ as m
    order = []
    monkeypatch.setattr(m, "setup_logging", lambda: order.append("logging") or tmp_path / "x.log")

    class _Rt:
        async def run_app(self):
            order.append("run_app")

    monkeypatch.setattr(m.Runtime, "build", classmethod(lambda cls, cfg: order.append("build") or _Rt()))
    m.main()
    assert order == ["logging", "build", "run_app"]


def test_logging_never_stops_the_recorder_from_starting(tmp_path, capsys, _restore_root_logging):
    blocker = tmp_path / "logs"
    blocker.write_text("a FILE where the logs dir should be")
    assert setup_logging(blocker) is None                     # no file logging...
    logging.getLogger("pavilos.test").warning("still logging")
    err = capsys.readouterr().err
    assert "cannot log to" in err and "still logging" in err  # ...but WARNING+ on stderr


def test_a_blocked_rotation_keeps_logging_into_the_current_file(tmp_path, capsys,
                                                                _restore_root_logging, monkeypatch):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 300)
    path = setup_logging(tmp_path / "logs")
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, m._RotatingFile))
    real_rename = m.os.rename

    def locked(src, dst):
        if src == handler.baseFilename:
            raise PermissionError(32, "another process has the file open")
        return real_rename(src, dst)

    monkeypatch.setattr(m.os, "rename", locked)
    log = logging.getLogger("pavilos.test")
    for i in range(50):
        log.info("line %02d %s", i, "x" * 30)
    for h in logging.getLogger().handlers:
        h.flush()
    text = path.read_text(encoding="utf-8")
    assert all(f"line {i:02d}" in text for i in range(50))     # nothing lost
    assert "Logging error" not in capsys.readouterr().err      # no traceback flood on stderr


def test_failed_rotations_never_destroy_backups(tmp_path, _restore_root_logging, monkeypatch):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 300)
    monkeypatch.setattr(m._RotatingFile, "_RETRY_S", 0.0)      # retry on every record
    logs = tmp_path / "logs"
    logs.mkdir()
    olds = [logs / f"pavilos.2026100{i}-000000.000000.log" for i in range(1, 6)]
    for i, p in enumerate(olds, 1):
        p.write_text(f"BACKUP-{i}", encoding="utf-8")
    path = setup_logging(logs)
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, m._RotatingFile))
    real_rename = m.os.rename

    def locked(src, dst):
        if src == handler.baseFilename:
            raise PermissionError(32, "another process has the file open")
        return real_rename(src, dst)

    monkeypatch.setattr(m.os, "rename", locked)
    log = logging.getLogger("pavilos.test")
    for i in range(60):
        log.info("line %02d %s", i, "x" * 30)
    for h in logging.getLogger().handlers:
        h.flush()
    for i, p in enumerate(olds, 1):
        assert p.read_text(encoding="utf-8") == f"BACKUP-{i}"
    assert "line 59" in path.read_text(encoding="utf-8")


def test_a_free_rotation_still_rotates(tmp_path, _restore_root_logging, monkeypatch):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 300)
    path = setup_logging(tmp_path / "logs")
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, m._RotatingFile))
    log = logging.getLogger("pavilos.test")
    for i in range(30):
        log.info("line %02d %s", i, "x" * 30)
        time.sleep(0.002)
    for h in logging.getLogger().handlers:
        h.flush()
    assert handler.rotated()
    assert "line 29" in path.read_text(encoding="utf-8")


def test_an_old_log_held_open_is_kept_and_deleted_later(tmp_path, _restore_root_logging, monkeypatch):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 300)
    logs = tmp_path / "logs"
    logs.mkdir()
    olds = [logs / f"pavilos.2026100{i}-000000.000000.log" for i in range(1, 6)]
    for p in olds:
        p.write_text("old", encoding="utf-8")
    setup_logging(logs)
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, m._RotatingFile))
    real_remove = m.os.remove
    held = {str(olds[0])}

    def remove(p):
        if str(p) in held:
            raise PermissionError(32, "a reader holds it")
        return real_remove(p)

    monkeypatch.setattr(m.os, "remove", remove)
    log = logging.getLogger("pavilos.test")
    log.info("x" * 400)                       # first rotation: the oldest is held -> kept
    log.info("y" * 400)
    assert olds[0].exists() and len(handler.rotated()) == 6       # nothing else lost
    held.clear()                              # the reader is gone
    time.sleep(0.002)
    log.info("z" * 400)                       # next rotation deletes the oldest ones
    assert not olds[0].exists() and len(handler.rotated()) == 5


def test_rotations_in_the_same_microsecond_get_distinct_names(tmp_path, _restore_root_logging,
                                                              monkeypatch):
    import pavilos.__main__ as m
    monkeypatch.setattr(m, "_LOG_MAX_BYTES", 300)
    path = setup_logging(tmp_path / "logs")
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, m._RotatingFile))
    monkeypatch.setattr(m.time, "time", lambda: 1_791_000_000.5)   # a frozen clock
    log = logging.getLogger("pavilos.test")
    for i in range(12):
        log.info("line %02d %s", i, "x" * 60)
    for h in logging.getLogger().handlers:
        h.flush()
    names = [pathlib.Path(p).name for p in handler.rotated()]
    assert len(names) >= 3 and len(set(names)) == len(names)
    assert names[:2] == ["pavilos.20261003-040000.500000.log", "pavilos.20261003-040000.500001.log"]
    text = "".join(pathlib.Path(p).read_text(encoding="utf-8") for p in handler.rotated())
    text += path.read_text(encoding="utf-8")
    assert all(f"line {i:02d}" in text for i in range(12))            # nothing lost
