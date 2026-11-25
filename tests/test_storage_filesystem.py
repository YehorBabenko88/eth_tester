"""No PostgreSQL required: scoped file lifecycle never deletes strangers."""
from pathlib import Path

import pytest

from eth_tester.storage.filesystem import (
    deletion_manifest, initialize_owned_root, register_owned_file,
    remove_owned_root,
)


def test_owned_files_only_and_exact_confirmation(tmp_path):
    root = tmp_path / "eth_owned"
    first = initialize_owned_root(root)
    assert first["project"] == "eth_tester"
    log = root / "diagnostics" / "sample.log"
    log.parent.mkdir()
    log.write_text("ETH Tester only", encoding="utf-8")
    register_owned_file(root, "diagnostics/sample.log")
    manifest = deletion_manifest(root)
    assert manifest["files"] == ["diagnostics/sample.log"]
    with pytest.raises(RuntimeError, match="writers"):
        remove_owned_root(
            root, confirmation=manifest["confirmation_phrase"],
            manifest_sha256=manifest["manifest_sha256"],
        )
    with pytest.raises(ValueError, match="confirmation"):
        remove_owned_root(
            root, confirmation="DELETE EVERYTHING",
            manifest_sha256=manifest["manifest_sha256"], workers_stopped=True,
        )
    assert remove_owned_root(
        root, confirmation=manifest["confirmation_phrase"],
        manifest_sha256=manifest["manifest_sha256"], workers_stopped=True,
    ) == 1
    assert not root.exists()


def test_unknown_file_fails_before_any_delete(tmp_path):
    root = tmp_path / "eth_owned"
    initialize_owned_root(root)
    log = root / "owned.txt"
    log.write_text("keep if something unknown appears", encoding="utf-8")
    register_owned_file(root, "owned.txt")
    stranger = root / "not_registered.txt"
    stranger.write_text("unrelated", encoding="utf-8")
    manifest = deletion_manifest(root)
    with pytest.raises(RuntimeError, match="Unregistered"):
        remove_owned_root(
            root, confirmation=manifest["confirmation_phrase"],
            manifest_sha256=manifest["manifest_sha256"], workers_stopped=True,
        )
    assert log.exists() and stranger.exists()


def test_unknown_directory_is_protected(tmp_path):
    root = tmp_path / "eth_owned"
    initialize_owned_root(root)
    (root / "unregistered_empty").mkdir()
    manifest = deletion_manifest(root)
    with pytest.raises(RuntimeError, match="directories"):
        remove_owned_root(
            root, confirmation=manifest["confirmation_phrase"],
            manifest_sha256=manifest["manifest_sha256"], workers_stopped=True,
        )
    assert root.exists()


def test_refuse_foreign_root_and_manifest_change(tmp_path):
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "personal.txt").write_text("untouched", encoding="utf-8")
    with pytest.raises(RuntimeError, match="marker"):
        initialize_owned_root(foreign)
    assert (foreign / "personal.txt").exists()

    root = tmp_path / "owned"
    initialize_owned_root(root)
    manifest = deletion_manifest(root)
    log = root / "new.log"
    log.write_text("changed manifest", encoding="utf-8")
    register_owned_file(root, "new.log")
    with pytest.raises(RuntimeError, match="changed"):
        remove_owned_root(
            root, confirmation=manifest["confirmation_phrase"],
            manifest_sha256=manifest["manifest_sha256"], workers_stopped=True,
        )
    with pytest.raises(ValueError):
        register_owned_file(root, Path(".eth_tester_owned.json"))
