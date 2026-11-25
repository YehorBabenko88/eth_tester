"""Non-destructive Windows install/uninstall safety contract.

Static assertions complement (but do not replace) a Windows VM lifecycle test.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
INSTALL = ROOT / "install" / "install.ps1"
UNINSTALL = ROOT / "install" / "uninstall.ps1"


def test_install_requires_admin_identity_and_credential_acl():
    source = INSTALL.read_text(encoding="utf-8")
    assert "WindowsBuiltInRole]::Administrator" in source
    assert "LOCAL SERVICE" in source
    assert "Get-Acl" in source
    assert ".env" in source
    assert ".eth-tester-install.json" in source


def test_uninstall_refuses_unknown_ownership_and_mismatched_task():
    source = UNINSTALL.read_text(encoding="utf-8")
    assert "Ownership manifest missing" in source
    assert "Ownership mismatch" in source
    assert "Task action does not match manifest" in source
    assert "ETHTester-Owned-Supervisor" in source
    assert "Unregister-ScheduledTask" in source


def test_uninstall_cannot_broadly_erase_user_data():
    source = UNINSTALL.read_text(encoding="utf-8")
    assert not re.search(r"Remove-Item[^\n]*-Recurse", source, flags=re.I)
    assert not re.search(r"DROP\s+DATABASE|TRUNCATE\s+TABLE", source, flags=re.I)
    assert not re.search(r"Remove-Item[^\n]*\\(?:data|archives|postgres)", source, flags=re.I)
    assert "Remove-Item -LiteralPath $manifestPath" in source
