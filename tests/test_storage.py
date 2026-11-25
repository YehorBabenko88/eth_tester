"""Unit checks for destructive operation guards and migration inventory."""
from pathlib import Path
from unittest.mock import patch

import pytest

from eth_tester.migrations import MIGRATIONS
from eth_tester.retention import disk_guard
from eth_tester.storage import drop_owned_database, validate_db_name, verify_backup


def test_dedicated_database_names():
    assert validate_db_name("postgresql://localhost/eth_tester") == "eth_tester"
    assert validate_db_name("postgresql://localhost/eth_tester_lab") == "eth_tester_lab"
    with pytest.raises(ValueError):
        validate_db_name("postgresql://localhost/postgres")


def test_drop_requires_explicit_backup_and_phrase():
    url = "postgresql://localhost/eth_tester"
    with pytest.raises(ValueError):
        drop_owned_database(url, confirmation="yes")
    with pytest.raises(ValueError):
        drop_owned_database(url, confirmation="DELETE ETH TESTER eth_tester")


def test_migration_inventory():
    assert MIGRATIONS == ("0001_partitioned_archive.sql", "0002_quality_and_provenance.sql", "0003_retention_indexes.sql", "0004_trade_identity_guard.sql")


def test_disk_guard_threshold(tmp_path):
    result = disk_guard(tmp_path, minimum_free_gb=0)
    assert result["total"] >= result["free"] >= 0
    with pytest.raises(RuntimeError):
        disk_guard(tmp_path, minimum_free_gb=10**12)


def test_backup_must_exist(tmp_path):
    with patch("eth_tester.storage.shutil.which", return_value=None):
        with pytest.raises(RuntimeError):
            verify_backup(Path(tmp_path) / "missing.dump")
