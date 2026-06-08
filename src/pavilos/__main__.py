# src/pavilos/__main__.py
"""`python -m pavilos` — run the live paper-trading app + dashboard."""
from __future__ import annotations

import asyncio
import logging

from pavilos.connectors.venues import VENUE_SPECS
from pavilos.core.runtime import Runtime, RuntimeConfig


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = RuntimeConfig()
    rt = Runtime.build(cfg)
    _log = logging.getLogger("pavilos")
    _log.info("PAVILOS paper dashboard on http://%s:%d (PAPER mode, %d venues)", cfg.host, cfg.port, len(VENUE_SPECS))
    try:
        asyncio.run(rt.run_app())
    except KeyboardInterrupt:
        _log.info("shutting down")


if __name__ == "__main__":
    main()
