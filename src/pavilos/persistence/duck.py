# src/pavilos/persistence/duck.py
"""DuckDB connections for the lake: memory capped, spilling to a KNOWN temp dir.

Without the cap, merging/sorting a multi-million-row deep-book partition built large
hash tables / buffers and spiked to many GB (a run OOM'd and segfaulted); with it
DuckDB spills to disk instead. Without ``temp_directory``, an in-memory connection
spills into ``./.tmp`` of the CURRENT directory -- the repo -- where a killed process
left 17 GB of spill files (2026-09-06)."""
from __future__ import annotations

import os
import tempfile

import duckdb

SPILL_DIR = os.path.join(tempfile.gettempdir(), "pavilos_duckdb_spill")


def connect(memory_limit: str = "3GB") -> "duckdb.DuckDBPyConnection":
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{memory_limit}'")
    con.sql(f"SET temp_directory='{SPILL_DIR.replace(chr(92), '/')}'")
    return con
