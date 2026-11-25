"""POSIX-only owned archive staging and trusted PostgreSQL CLI boundary.

No PATH fallback, user-provided file paths to pg_restore, or unchecked
pathname unlink. Other OSes fail closed until native handle/ACL QA exists.
"""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import stat
import subprocess
import tempfile

_READ = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)


def _posix():
    if (os.name != "posix" or not _NOFOLLOW or not _DIRECTORY
            or not os.path.isdir("/proc/self/fd")):
        raise RuntimeError("Secure PostgreSQL archive handles unavailable on this OS")


def _safe_parents(path):
    """No mutable PATH components, except root-owned sticky directories."""
    _posix()
    actual = Path(path)
    if not actual.is_absolute():
        raise RuntimeError("An absolute PostgreSQL path is required")
    for part in (actual, *actual.parents):
        info = os.lstat(part)
        if not stat.S_ISDIR(info.st_mode):
            raise RuntimeError("Symlink or non-directory component in PostgreSQL path")
        if info.st_uid not in (0, os.geteuid()):
            raise RuntimeError("Untrusted directory owner")
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            if not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
                raise RuntimeError("Writable PostgreSQL path component")


def trusted_tool(name):
    """No executable chosen from PATH. Administrator must pin absolute bin dir."""
    _posix()
    if name not in ("pg_dump", "pg_restore"):
        raise ValueError("Unapproved PostgreSQL executable")
    root = os.environ.get("ETH_TESTER_PG_BIN_DIR", "")
    if not root or not os.path.isabs(root):
        raise RuntimeError("ETH_TESTER_PG_BIN_DIR must be an absolute trusted directory")
    _safe_parents(root)
    executable = Path(root) / name
    info = os.lstat(executable)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.geteuid())
            or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or not (info.st_mode & stat.S_IXUSR)):
        raise RuntimeError("Untrusted PostgreSQL executable")
    return str(executable)


def secure_parent(path):
    """Return a pinned, owned, non-writable-by-other directory fd."""
    _posix()
    parent = Path(path).absolute().parent
    _safe_parents(parent)
    fd = os.open(parent, _READ | _NOFOLLOW | _DIRECTORY)
    info = os.fstat(fd)
    if (info.st_uid != os.geteuid()
            or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)):
        os.close(fd)
        raise RuntimeError("Archive parent must be process-owned, non-shared")
    return fd


def _fd_sha(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(fd, 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _unchanged(fd, original):
    new = os.fstat(fd)
    if ((new.st_dev, new.st_ino, new.st_size, new.st_mtime_ns, new.st_ctime_ns)
            != original):
        raise RuntimeError("Archive changed during verification/restore")


@contextmanager
def _snapshot(source):
    """Copy from an opened, nofollow file to private archive; keep fd pinned."""
    _posix()
    src_fd = os.open(source, _READ | _NOFOLLOW)
    try:
        before = os.fstat(src_fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
                or before.st_uid not in (0, os.geteuid()) or before.st_size <= 0):
            raise RuntimeError("Untrusted backup input")
        identity = (before.st_dev, before.st_ino, before.st_size,
                    before.st_mtime_ns, before.st_ctime_ns)
        with tempfile.TemporaryDirectory(prefix="eth-tester-verify-") as folder:
            os.chmod(folder, 0o700)
            staged = Path(folder) / "pinned.dump"
            stage_fd = os.open(staged, os.O_CREAT | os.O_EXCL | os.O_RDWR | _NOFOLLOW, 0o600)
            try:
                os.lseek(src_fd, 0, os.SEEK_SET)
                while chunk := os.read(src_fd, 1024 * 1024):
                    view = memoryview(chunk)
                    while view:
                        view = view[os.write(stage_fd, view):]
                _unchanged(src_fd, identity)
                os.fsync(stage_fd)
                os.fchmod(stage_fd, 0o400)
                reference = (os.fstat(stage_fd).st_dev, os.fstat(stage_fd).st_ino,
                             os.fstat(stage_fd).st_size, os.fstat(stage_fd).st_mtime_ns,
                             os.fstat(stage_fd).st_ctime_ns)
                yield (stage_fd, f"/proc/self/fd/{stage_fd}", _fd_sha(stage_fd), reference)
                _unchanged(stage_fd, reference)
            finally:
                os.close(stage_fd)
    finally:
        os.close(src_fd)


@contextmanager
def verified_archive(path, *, expected_sha256=None, expected_database=None):
    """Ensure TOC and restore consume the same private pinned archive fd."""
    binary = trusted_tool("pg_restore")
    with _snapshot(path) as (fd, alias, checksum, snapshot_stat):
        if expected_sha256 is not None and checksum != expected_sha256:
            raise RuntimeError("PostgreSQL backup checksum mismatch")
        result = subprocess.run([binary, "--list", alias], pass_fds=(fd,),
                                capture_output=True, text=True, check=True, timeout=120)
        if "eth_storage_identity" not in result.stdout:
            raise RuntimeError("ETH Tester ownership marker absent from backup")
        if expected_database is not None:
            dbnames = [line.split("dbname:", 1)[1].strip()
                       for line in result.stdout.splitlines()
                       if line.lstrip().startswith(";") and "dbname:" in line]
            if dbnames != [expected_database]:
                raise RuntimeError("Backup source database does not match target")
        _unchanged(fd, snapshot_stat)
        if _fd_sha(fd) != checksum:
            raise RuntimeError("Pinned backup bytes changed after TOC verification")
        try:
            yield (binary, fd, alias, checksum)
        finally:
            _unchanged(fd, snapshot_stat)
            if _fd_sha(fd) != checksum:
                raise RuntimeError("Pinned backup bytes changed during restore")


def dump_privately(url, destination, *, cli_dsn, env, expected_database):
    """Never create final filename until dump and TOC have both succeeded."""
    tool = trusted_tool("pg_dump")
    target = Path(destination).absolute()
    if target.name in ("", ".", ".."):
        raise ValueError("Invalid backup filename")
    # Only a trusted owned destination directory; no untrusted mkdir here.
    parent_fd = secure_parent(target)
    temp_name = None
    fd = None
    try:
        if os.path.lexists(target):
            raise FileExistsError(target)
        temp_name = ".eth-tester-backup-" + os.urandom(16).hex() + ".part"
        fd = os.open(temp_name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | _NOFOLLOW,
                     0o600, dir_fd=parent_fd)
        own = os.fstat(fd)
        try:
            with os.fdopen(os.dup(fd), "wb") as output:
                subprocess.run([tool, "--format=custom", "--no-password",
                                "--dbname", cli_dsn], env=env, stdout=output,
                               check=True, timeout=7200)
                output.flush()
                os.fsync(output.fileno())
            if os.fstat(fd).st_size <= 0:
                raise RuntimeError("Empty PostgreSQL dump")
            # TOC against pinned temp filename inside a verified private parent.
            with verified_archive(target.parent / temp_name,
                                  expected_database=expected_database):
                pass
            # Revalidate the exact staged inode before publishing; the temp
            # pathname is never trusted merely because its TOC once passed.
            checked = os.stat(temp_name, dir_fd=parent_fd, follow_symlinks=False)
            if (checked.st_dev, checked.st_ino) != (own.st_dev, own.st_ino):
                raise RuntimeError("Staged backup file replaced before publication")
            # O_EXCL-equivalent publication, cannot overwrite existing path.
            os.link(temp_name, target.name, src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd, follow_symlinks=False)
            published = os.stat(target.name, dir_fd=parent_fd,
                                follow_symlinks=False)
            if (published.st_dev, published.st_ino) != (own.st_dev, own.st_ino):
                raise RuntimeError("Published backup identity differs from staged archive")
            os.fsync(parent_fd)
            return target
        finally:
            # If temp was swapped, never unlink a different inode.
            try:
                current = os.stat(temp_name, dir_fd=parent_fd,
                                  follow_symlinks=False)
                if (current.st_dev, current.st_ino) == (own.st_dev, own.st_ino):
                    os.unlink(temp_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
    finally:
        if fd is not None:
            os.close(fd)
        os.close(parent_fd)
