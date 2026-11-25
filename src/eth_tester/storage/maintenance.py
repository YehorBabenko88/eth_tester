"""Fail-closed, bounded ETH Tester storage operating checks and owned-temp sweep.

Never use the local filesystem's free bytes to certify a remote PostgreSQL
server; no market-data rows or backups are deleted by this module.
"""
from datetime import datetime, timezone
from pathlib import Path
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
    """Delete ONLY old staging .part files under a pre-marked owned directory.

    Does not follow symlinks, recurse, touch directories, or create its own
    marker. A missing/invalid marker raises; operator controls temp ownership.
    """
    if (type(older_than_seconds) is not int or older_than_seconds < 60 or
            type(max_files) is not int or not 1 <= max_files <= 1000):
        raise ValueError("Bounded positive temp cleanup limits required")
    path = Path(root)
    if path.is_symlink() or not path.is_dir():
        raise RuntimeError("Owned temp root must be a real directory")
    marker = path / _MARKER
    if (marker.is_symlink() or not marker.is_file() or
            marker.read_text(encoding="utf-8") != _MARKER_TEXT):
        raise RuntimeError("Exact ETH Tester temp ownership marker required")
    instant = datetime.now(timezone.utc).timestamp() if now is None else now
    if isinstance(instant, bool) or not isinstance(instant, (int, float)):
        raise ValueError("Invalid cleanup time")
    deleted = 0
    scanned = 0
    for child in sorted(path.iterdir(), key=lambda p: p.name):
        if deleted >= max_files:
            break
        if not _TEMP_PATTERN.fullmatch(child.name):
            continue
        scanned += 1
        try:
            info = child.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or
                    info.st_mtime > instant - older_than_seconds):
                continue
            child.unlink()
            deleted += 1
        except FileNotFoundError:
            continue
    return {"status": "ok", "deleted_owned_temp_files": deleted,
            "matched_candidates_scanned": scanned,
            "raw_market_data_deleted": 0, "backups_deleted": 0,
            "bounded": deleted == max_files}
