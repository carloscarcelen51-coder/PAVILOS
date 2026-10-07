# tests/unit/test_duck.py
import os

from pavilos.persistence.duck import SPILL_DIR, connect


def test_connections_are_memory_capped_and_spill_outside_the_repo():
    con = connect("1GB")
    try:
        mem = con.sql("SELECT current_setting('memory_limit')").fetchone()[0]
        tmp = con.sql("SELECT current_setting('temp_directory')").fetchone()[0]
    finally:
        con.close()
    assert "GiB" in mem or "MiB" in mem or "GB" in mem or "MB" in mem
    assert os.path.normcase(os.path.normpath(tmp)) == os.path.normcase(os.path.normpath(SPILL_DIR))
    assert os.path.isdir(SPILL_DIR)
