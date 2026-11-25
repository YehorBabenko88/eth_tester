"""QA H R14: root-pinned POSIX temp cleanup and accounting fault contracts."""
import os

import pytest


@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor cleanup only")
def test_scan_budget_preflight_prevents_unreported_partial_deletion(tmp_path):
    from eth_tester.storage.maintenance import cleanup_owned_temp

    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    (root / ".eth-tester-owned-temp").write_text(
        "eth_tester:temporary:v1\n", encoding="utf-8"
    )
    eligible = root / ("eth-tester-stage-" + "a" * 32 + ".part")
    eligible.write_bytes(b"QA owned temporary only")
    os.utime(eligible, (1, 1))
    protected = root / "operator.backup"
    protected.write_bytes(b"not eligible")
    for index in range(10001):
        (root / f"qa-padding-{index:05}.log").touch()
    with pytest.raises(RuntimeError, match="(?i)(budget|scan)"):
        cleanup_owned_temp(root, now=7200, older_than_seconds=3600)
    assert eligible.read_bytes() == b"QA owned temporary only"
    assert protected.read_bytes() == b"not eligible"


@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor cleanup only")
def test_normal_bounded_sweep_only_deletes_expired_owned_file(tmp_path):
    from eth_tester.storage.maintenance import cleanup_owned_temp

    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    (root / ".eth-tester-owned-temp").write_text(
        "eth_tester:temporary:v1\n", encoding="utf-8"
    )
    old = root / ("eth-tester-stage-" + "b" * 32 + ".part")
    new = root / ("eth-tester-stage-" + "c" * 32 + ".part")
    foreign = root / "operator.backup"
    old.write_bytes(b"old")
    new.write_bytes(b"fresh")
    foreign.write_bytes(b"protected")
    os.utime(old, (1, 1))
    os.utime(new, (7199, 7199))
    report = cleanup_owned_temp(root, now=7200, older_than_seconds=3600)
    assert report["status"] == "ok"
    assert report["deleted_owned_temp_files"] == 1
    assert report["raw_market_data_deleted"] == 0
    assert not old.exists()
    assert new.read_bytes() == b"fresh"
    assert foreign.read_bytes() == b"protected"


def test_unsupported_windows_cleanup_must_fail_closed(monkeypatch):
    from eth_tester.storage import maintenance

    monkeypatch.setattr(maintenance.os, "name", "nt")
    with pytest.raises(RuntimeError, match="(?i)(owner|atomic|handle)"):
        maintenance.cleanup_owned_temp("C:\\qa\\temporary")
