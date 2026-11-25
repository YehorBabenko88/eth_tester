"""Manifest-gated deletion of only files explicitly owned by ETH Tester.

Never recursively remove an arbitrary configured directory.
"""
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

MARKER = ".eth_tester_owned.json"


def _safe_root(root):
    path = Path(root).expanduser()
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("Owned storage root must be absolute and not a symlink")
    path = path.resolve()
    for protected in (Path.home().resolve(), Path.cwd().resolve()):
        if protected == path or protected.is_relative_to(path):
            raise ValueError("Cannot own a home, working or ancestor directory")
    if path == Path(path.anchor):
        raise ValueError("Refusing filesystem root")
    return path


def _write_marker(root, manifest):
    temporary = root / (MARKER + ".tmp")
    if temporary.exists():
        raise RuntimeError("Uncommitted manifest temp file; manual inspection needed")
    payload = json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, root / MARKER)


def _load(root):
    marker = root / MARKER
    if marker.is_symlink() or not marker.is_file():
        raise RuntimeError("Missing/invalid ETH Tester file ownership marker")
    contents = marker.read_bytes()
    manifest = json.loads(contents)
    if (manifest.get("project") != "eth_tester"
            or manifest.get("version") != 1
            or manifest.get("root") != str(root)
            or not manifest.get("nonce")
            or not isinstance(manifest.get("files"), dict)):
        raise RuntimeError("Owned directory manifest contract mismatch")
    return manifest, hashlib.sha256(contents).hexdigest()


def initialize_owned_root(root):
    path = _safe_root(root)
    if path.exists():
        if not path.is_dir():
            raise RuntimeError("Owned root is not a directory")
        return _load(path)[0]
    path.mkdir(mode=0o700)
    manifest = {
        "project": "eth_tester", "version": 1, "root": str(path),
        "nonce": uuid4().hex, "files": {},
    }
    _write_marker(path, manifest)
    return manifest


def register_owned_file(root, filename):
    path = _safe_root(root)
    manifest, _ = _load(path)
    relative = Path(filename)
    if (relative.is_absolute() or not relative.parts or ".." in relative.parts
            or relative.name in (MARKER, MARKER + ".tmp")):
        raise ValueError("Owned file path must be contained and relative")
    candidate = path / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise RuntimeError("Only existing, regular non-symlink files can be registered")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_relative_to(path):
        raise ValueError("Refusing path escaping owned root")
    for ancestor in candidate.parents:
        if ancestor == path:
            break
        if ancestor.is_symlink():
            raise ValueError("Owned path crosses a symbolic link")
    stat = candidate.stat()
    manifest["files"][relative.as_posix()] = {
        "device": stat.st_dev, "inode": stat.st_ino,
    }
    _write_marker(path, manifest)
    return _load(path)[1]


def deletion_manifest(root):
    path = _safe_root(root)
    manifest, digest = _load(path)
    return {
        "root": str(path), "nonce": manifest["nonce"],
        "files": sorted(manifest["files"]),
        "manifest_sha256": digest,
        "confirmation_phrase": f"DELETE ETH TESTER FILES {manifest['nonce']}",
    }


def remove_owned_root(root, *, confirmation, manifest_sha256, workers_stopped=False):
    """Delete only registered files, after full preflight and human confirmation."""
    if not workers_stopped:
        raise RuntimeError("All ETH Tester file writers must first be stopped")
    path = _safe_root(root)
    manifest, digest = _load(path)
    if digest != manifest_sha256:
        raise RuntimeError("Ownership manifest changed since preflight")
    if confirmation != f"DELETE ETH TESTER FILES {manifest['nonce']}":
        raise ValueError("Exact ETH Tester file deletion confirmation required")

    known = set(manifest["files"])
    observed = set()
    for current in path.rglob("*"):
        if current.is_symlink():
            raise RuntimeError("Refusing deletion through symbolic link")
        if current.is_file():
            relative = current.relative_to(path).as_posix()
            if relative != MARKER:
                observed.add(relative)
    known_dirs = {
        str(Path(relative).parent.as_posix())
        for relative in known
        if Path(relative).parent != Path(".")
    }
    allowed_dirs = set()
    for parent in known_dirs:
        item = Path(parent)
        while item != Path("."):
            allowed_dirs.add(item.as_posix())
            item = item.parent
    actual_dirs = {
        p.relative_to(path).as_posix()
        for p in path.rglob("*") if p.is_dir()
    }
    if actual_dirs != allowed_dirs:
        raise RuntimeError("Unknown or missing directories prevent deletion")
    if observed != known:
        raise RuntimeError(
            f"Unregistered or missing files prevent deletion: {sorted(observed ^ known)}"
        )
    for relative, identity in manifest["files"].items():
        candidate = path / relative
        status = candidate.stat()
        if status.st_dev != identity["device"] or status.st_ino != identity["inode"]:
            raise RuntimeError(f"Owned file identity changed: {relative}")

    # No deletion happens until every path and inode passes preflight.
    for relative in sorted(known):
        (path / relative).unlink()
    for current in sorted(
        (p for p in path.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts), reverse=True,
    ):
        current.rmdir()
    (path / MARKER).unlink()
    path.rmdir()
    return len(known)
