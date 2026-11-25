"""Fail-closed, bounded ETH Tester storage operating checks and owned-temp sweep.

Never use the local filesystem's free bytes to certify a remote PostgreSQL
server; no market-data rows or backups are deleted by this module.
"""
from datetime import datetime, timezone
import os
import re
import stat
from shutil import disk_usage

_MARKER = ".eth-tester-owned-temp"
_MARKER_TEXT = "eth_tester:temporary:v1\n"
_TEMP_PATTERN = re.compile(r"eth-tester-stage-[0-9a-f]{32}\.part\Z")


def assess_pressure(*, local_free_bytes, minimum_local_free_bytes,
                    db_bytes, max_db_bytes=None, remote_pg_disk_free_bytes=None,
                    min_remote_pg_free_bytes=None, wal_on_disk_bytes=None,
                    max_wal_on_disk_bytes=None, database_in_recovery=False):
    """Classify measured disk pressure; unknown != healthy.

    WAL directory size is CLUSTER-wide, not attributable to ETH Tester alone.
    The result must not authorize deleting records or disabling paper controls.
    """
    checks = {}
    fields = (
        ("local_free", local_free_bytes, minimum_local_free_bytes, "minimum"),
        ("database_bytes", db_bytes, max_db_bytes, "maximum"),
        ("postgres_free", remote_pg_disk_free_bytes, min_remote_pg_free_bytes, "minimum"),
        ("cluster_wal_on_disk", wal_on_disk_bytes, max_wal_on_disk_bytes, "maximum"),
    )
    for name, observed, threshold, direction in fields:
        if threshold is None:
            checks[name] = "not_configured"
            continue
        if type(threshold) is not int or threshold < 0:
            raise ValueError("Storage thresholds must be nonnegative integers")
        if observed is None:
            checks[name] = "unknown"
        elif type(observed) is not int or observed < 0:
            raise ValueError("Storage measurements must be nonnegative integers")
        else:
            passed = observed >= threshold if direction == "minimum" else observed <= threshold
            checks[name] = "ok" if passed else "blocked"
    if database_in_recovery:
        checks["database_role"] = "blocked"
    else:
        checks["database_role"] = "ok"
    status = ("blocked" if "blocked" in checks.values() else
              "unknown" if "unknown" in checks.values() else "ok")
    return {"status": status, "checks": checks,
            "may_delete_raw_data": False, "paper_only": True}


def local_temp_preflight(path, *, minimum_free_gb=20):
    """Measure the filesystem of THIS process, not the PostgreSQL host."""
    if isinstance(minimum_free_gb, bool) or not isinstance(minimum_free_gb, (int, float)):
        raise ValueError("Invalid local minimum free GB")
    if not 0 <= minimum_free_gb <= 100000:
        raise ValueError("Invalid local free GB threshold")
    usage = disk_usage(path)
    minimum = int(minimum_free_gb * 1024**3)
    result = assess_pressure(local_free_bytes=usage.free,
                             minimum_local_free_bytes=minimum,
                             db_bytes=0)
    return {**result, "free_bytes": usage.free,
            "total_bytes": usage.total,
            "filesystem_scope": "local_process_only"}


def cleanup_owned_temp(root, *, now=None, older_than_seconds=3600, max_files=100):
    """POSIX handle-pinned temp sweep; on Windows fail closed without native handles.

    Descriptor-based directory identity stops a symlink substitution after
    owner validation. Windows must use reparse-safe native handles and
    must not fall back to path-based unlink.
    """
    if (type(older_than_seconds) is not int or older_than_seconds < 60 or
            type(max_files) is not int or not 1 <= max_files <= 1000):
        raise ValueError('Bounded positive temp cleanup limits required')
    if os.name != 'posix' or not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_DIRECTORY'):
        raise RuntimeError('No verified atomic owner directory handle')
    instant = datetime.now(timezone.utc).timestamp() if now is None else now
    if isinstance(instant, bool) or not isinstance(instant, (int, float)):
        raise ValueError('Invalid cleanup time')
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | getattr(os, 'O_CLOEXEC', 0)
    root_fd = os.open(root, flags)
    try:
        st = os.fstat(root_fd)
        if (not stat.S_ISDIR(st.st_mode) or st.st_uid != os.geteuid()
                or st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)):
            raise RuntimeError('Owned temp directory permissions not secure')
        marker_fd = os.open(_MARKER, os.O_RDONLY | os.O_NOFOLLOW |
                            getattr(os, 'O_CLOEXEC', 0), dir_fd=root_fd)
        try:
            marker_st = os.fstat(marker_fd)
            marker_bytes = os.read(marker_fd, len(_MARKER_TEXT.encode()) + 1)
            if (not stat.S_ISREG(marker_st.st_mode) or marker_st.st_nlink != 1
                    or marker_st.st_uid != st.st_uid
                    or marker_bytes != _MARKER_TEXT.encode()):
                raise RuntimeError('Exact owned-temp marker required')
        finally:
            os.close(marker_fd)
        deleted = 0
        scanned = 0
        visited = 0
        # scandir(fd) streams, unlike sorted(Path.iterdir()); no unbounded list.
        with os.scandir(root_fd) as entries:
            for item in entries:
                visited += 1
                if visited > 10000:
                    raise RuntimeError('Owned temp scan budget exceeded')
                if deleted >= max_files:
                    break
                name = item.name
                if not _TEMP_PATTERN.fullmatch(name):
                    continue
                scanned += 1
                try:
                    info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
                    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_mtime > instant - older_than_seconds):
                        continue
                    os.unlink(name, dir_fd=root_fd)
                    deleted += 1
                except FileNotFoundError:
                    continue
        return {'status': 'ok', 'deleted_owned_temp_files': deleted,
                'matched_candidates_scanned': scanned,
                'raw_market_data_deleted': 0, 'backups_deleted': 0,
                'bounded': deleted == max_files}
    finally:
        os.close(root_fd)

