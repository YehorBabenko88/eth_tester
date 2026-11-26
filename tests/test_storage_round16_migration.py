"""Round16 B forward-only, fail-closed storage regression tests."""
from importlib.resources import files

from eth_tester.migrations import MIGRATIONS


def test_label_repair_preserves_original_migrations_and_proofs():
    root = files("eth_tester").joinpath("migrations")
    assert MIGRATIONS[-2:] == (
        "0008_provisional_cost_observations.sql",
        "0009_disambiguate_paper_label_candle.sql",
    )
    old = root.joinpath("0007_bounded_live_and_archive_bins.sql").read_text()
    new = root.joinpath("0009_disambiguate_paper_label_candle.sql").read_text()
    assert "kind text;" in old
    assert "WHERE symbol=original.symbol AND kind='trade' AND tf='1'" in old
    assert "CREATE OR REPLACE FUNCTION eth_round6_guard_full_future_label_grid()" in new
    assert "structural_side := original.reason->>'side';" in new
    assert "WHERE c.symbol=original.symbol AND c.kind='trade' AND c.tf='1'" in new
    assert "CREATE TRIGGER eth_round6_full_future_label_gate" not in new
    assert "Missing verified full future candle grid" in new
    assert "Structural label outcome disagrees with stored future OHLC" in new
    assert "Observed-only live paper entry cannot supply historical ML label" in new


def test_backup_and_cleanup_remain_fail_closed():
    storage = files("eth_tester").joinpath("storage")
    backup = storage.joinpath("secure_backup.py").read_text()
    cleanup = storage.joinpath("maintenance.py").read_text()
    assert "ETH_TESTER_PG_BIN_DIR" in backup
    assert "No executable chosen from PATH" in backup
    assert "No verified atomic owner directory handle" in cleanup
    assert "Owned temp scan budget exceeded; no files deleted" in cleanup
