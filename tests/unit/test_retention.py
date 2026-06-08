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
