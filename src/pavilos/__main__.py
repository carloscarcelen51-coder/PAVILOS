# src/pavilos/__main__.py
"""`python -m pavilos` — run the live paper-trading app + dashboard."""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pavilos.connectors.venues import VENUE_SPECS
from pavilos.core.runtime import Runtime, RuntimeConfig

_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"   # <project>/logs
_LOG_MAX_BYTES = 20 * 1024 * 1024
_LOG_BACKUPS = 5
_STAMP = re.compile(r"\d{8}-\d{6}\.\d{6}")


class _RotatingFile(RotatingFileHandler):
    """Size-rotated log that never loses history it should keep. The live file is
    RENAMED to ``pavilos.<UTC yyyymmdd-hhmmss.micros>.log`` -- a fresh name, so nothing is
    ever overwritten and there is no chain of renames -- then the oldest rotated files
    beyond ``backupCount`` are deleted. If the rename is impossible (Windows: another
    process holds the live file open without FILE_SHARE_DELETE) nothing is touched,
    logging continues in the live file and the rotation is retried a minute later; an
    old file held open simply stays until a later rotation can delete it. (The stdlib
    shifts .1->.2->... in place: a held file made every attempt drop a backup, and a
    failing rotation turned every record into a traceback on stderr, which the
    supervisor appends to its log without limit.)"""

    _RETRY_S = 60.0
    _failed_at: float | None = None

    def shouldRollover(self, record) -> bool:
        if self._failed_at is not None and time.monotonic() - self._failed_at < self._RETRY_S:
            return False
        return super().shouldRollover(record)

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None                  # reopened (append) on the next emit
        root, ext = os.path.splitext(self.baseFilename)
        us = int(time.time() * 1_000_000)
        while True:                             # a free name that still sorts chronologically
            dst = (f"{root}.{time.strftime('%Y%m%d-%H%M%S', time.gmtime(us // 1_000_000))}"
                   f".{us % 1_000_000:06d}{ext}")
            if not os.path.exists(dst):
                break
            us += 1
        try:
            os.rename(self.baseFilename, dst)   # never overwrites
        except OSError:
            self._failed_at = time.monotonic()
            return
        self._failed_at = None
        for old in self.rotated()[:-self.backupCount] if self.backupCount else self.rotated():
            try:
                os.remove(old)
            except OSError:
                pass                            # held open: deleted at a later rotation

    def rotated(self) -> list[str]:
        """The rotated files, oldest first (the stamp sorts chronologically)."""
        root, ext = os.path.splitext(self.baseFilename)
        folder, name = os.path.split(root)
        return sorted(os.path.join(folder, f) for f in os.listdir(folder)
                      if f.startswith(name + ".") and f.endswith(ext)
                      and _STAMP.fullmatch(f[len(name) + 1:len(f) - len(ext)]))


def setup_logging(log_dir: Path = _LOG_DIR) -> Path | None:
    """INFO+ to a ROTATING file (``logs/pavilos.log`` + 5 stamped rotations of 20 MB); only
    CRITICAL to stderr, which the supervisor appends to ``recorder_supervisor.log``
    (it had grown to 97 MB with every connector traceback). A fatal crash still
    reaches that log: an uncaught exception is printed by the interpreter itself.
    If the file cannot be used, logging falls back to WARNING+ on stderr: logging
    must never stop the recorder from starting."""
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    to_stderr = logging.StreamHandler()
    to_stderr.setFormatter(fmt)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / "pavilos.log"
        to_file = _RotatingFile(path, maxBytes=_LOG_MAX_BYTES, backupCount=_LOG_BACKUPS,
                                encoding="utf-8")
    except OSError as e:
        to_stderr.setLevel(logging.WARNING)
        root.handlers[:] = [to_stderr]
        logging.getLogger("pavilos").critical("cannot log to %s (%s); WARNING+ to stderr only", log_dir, e)
        return None
    to_file.setFormatter(fmt)
    to_file.setLevel(logging.INFO)
    to_stderr.setLevel(logging.CRITICAL)
    root.handlers[:] = [to_file, to_stderr]
    return path


def main() -> None:
    log_path = setup_logging()
    cfg = RuntimeConfig(paper_state_path="paper_state.json")   # next to paper_trades.jsonl
    rt = Runtime.build(cfg)
    _log = logging.getLogger("pavilos")
    _log.info("PAVILOS paper dashboard on http://%s:%d (PAPER mode, %d venues); log: %s",
              cfg.host, cfg.port, len(VENUE_SPECS), log_path)
    try:
        asyncio.run(rt.run_app())
    except KeyboardInterrupt:
        _log.info("shutting down")


if __name__ == "__main__":
    main()
