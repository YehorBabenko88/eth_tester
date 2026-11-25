"""Round-4 cross-owner contracts: strict fail-closed QA (no real network/trading).

These are release gates, not mocks that certify market-data completeness.
Production paths remain owned by B/C/D/E/F/G, not QA.
"""
import inspect
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from eth_tester.simulation.identity import (
    require_new_strategy_identity, rule_identity,
)
from eth_tester.strategies import Parameters


ROOT = Path(__file__).resolve().parents[2]


def _reason():
    version, metadata = rule_identity()
    return {
        "strategy_version": version,
        "rules_version": Parameters().version,
        **metadata,
    }


def test_d_e_full_hashed_identity_interoperability():
    """D's exact v3:SHA12 identity must be accepted, not only its prefix."""
    valid = _reason()
    assert valid["strategy_version"] == Parameters().engine_identity()
    assert require_new_strategy_identity(valid) == valid


@pytest.mark.parametrize("alteration", [
    {"strategy_version": "eth_rules_v3"},
    {"strategy_version": "eth_rules_v2"},
    {"strategy_version": "unversioned"},
    {"strategy_version": ""},
    {"rules_version": "eth_rules_v2"},
    {"parameter_sha256": "0" * 64},
    {"parameters": {}},
])
def test_d_e_reject_unknown_tampered_or_mismatched_identity(alteration):
    with pytest.raises(ValueError, match="(?i)strategy|version|parameter"):
        require_new_strategy_identity({**_reason(), **alteration})


def test_c_producer_must_use_b_writer_for_certified_snapshots():
    from eth_tester.snapshot_producer import certify_snapshot

    source = inspect.getsource(certify_snapshot)
    assert "write_feature_snapshot(" in source
    assert not re.search(r"INSERT\s+INTO\s+feature_snapshots", source, re.I)


def test_b_c_d_f_share_exact_v2_provenance_contract():
    from eth_tester.storage.snapshots import write_feature_snapshot
    from eth_tester.orderflow import _certified_snapshot
    from eth_tester.ml.extract import from_snapshot_records

    writer = inspect.getsource(write_feature_snapshot)
    d_reader = inspect.getsource(_certified_snapshot)
    f_reader = inspect.getsource(from_snapshot_records)
    for key in (
        "eth_feature_snapshot_v2",
        "feature_available_ms",
        "feature_sources",
        "source_receipt_ms",
        "source_checked_at_ms",
        "source_coverage_verified",
    ):
        assert key in writer, f"B writer missing {key}"
    for reader in (d_reader, f_reader):
        assert "eth_feature_snapshot_v2" in reader
        assert "feature_sources" in reader
        assert "source_receipt_ms" in reader


def test_e_persists_exact_snapshot_identity_and_decision_time():
    from eth_tester.paper import open_paper

    source = inspect.getsource(open_paper)
    for field in ("ml_snapshot_key", "ml_source_checksum"):
        assert field in source
    assert "immutable_feature_snapshot" in source
    assert "signal_ts" in source


def test_g_supervisor_uses_read_only_schema_verification():
    from eth_tester.supervisor import Supervisor

    source = inspect.getsource(Supervisor.run)
    assert "verify_existing_schema(settings)" in source
    assert "init_database(settings)" not in source


def test_installer_has_one_registration_and_nonempty_owner_id_validation():
    source = (ROOT / "install" / "install.ps1").read_text(encoding="utf-8")
    assert source.count("Register-ScheduledTask") == 1
    assert "ETH_TESTER_TG_USER_ID" in source
    assert not re.search(r"-notmatch\s+'\^\[0-9\]\+\s*(?:\r?\n|$)", source)
    assert "DROP DATABASE" not in source.upper()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell is not installed")
def test_windows_install_and_uninstall_parse_without_execution():
    paths = [ROOT / "install" / name for name in ("install.ps1", "uninstall.ps1")]
    literal_paths = ",".join("'" + str(path).replace("'", "''") + "'" for path in paths)
    script = (
        "$files=@(" + literal_paths + ");"
        "foreach($file in $files){"
        "$tok=$null;$errors=$null;"
        "[System.Management.Automation.Language.Parser]::ParseFile("
        "$file,[ref]$tok,[ref]$errors)|Out-Null;"
        "if($errors.Count -gt 0){throw ($file+': '+($errors -join '; '))}}"
    )
    result = subprocess.run(
        ["pwsh", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True, capture_output=True, timeout=30, check=False,
    )
    assert result.returncode == 0, result.stderr
