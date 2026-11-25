"""Single instance lock tied to the operating-system process lifetime.

This is deliberately *not* a PID file: stale text never blocks a restart, and
no second service may run until the previous process releases its OS lock.
"""

from contextlib import contextmanager
import os
from pathlib import Path


@contextmanager
def service_lease(path):
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    lockfile = directory / "supervisor.instance.lock"
    # Persistent dedicated lock file: never unlink it (inode replacement race).
    if lockfile.is_symlink():
        raise RuntimeError("Unsafe supervisor lock file symlink")
    lockfile.touch(exist_ok=True)
    with lockfile.open("r+b") as stream:
        if not stream.read(1):
            stream.seek(0)
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("ETH Tester supervisor already running") from exc
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError) as exc:
                raise RuntimeError("ETH Tester supervisor already running") from exc
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
