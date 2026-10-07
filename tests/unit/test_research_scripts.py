# tests/unit/test_research_scripts.py
import importlib

import pytest


@pytest.mark.parametrize("name", ["scripts._oos_window", "scripts._pressure_leads",
                                  "scripts._pressure_pilot", "scripts.check_lake",
                                  "scripts.compact_lake"])
def test_research_scripts_import_and_use_the_shared_replay(name):
    mod = importlib.import_module(name)
    if name.startswith("scripts._"):
        assert mod.replay_snapshots.__module__ == "pavilos.backtest.replay"
        assert not hasattr(mod, "_iter_updates")              # no private copy of the reader
