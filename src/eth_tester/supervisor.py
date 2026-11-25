"""Paper-only worker supervisor: graceful shutdown, status and bounded restarts.

No worker restart changes bot_control, so operator-stopped paper entries stay stopped.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .database import verify_existing_schema

LOGGER = logging.getLogger(__name__)
PERMANENT = ("live", "analyse", "engine")
PERIODIC = {"backfill": 86400, "archives": 86400, "aggregate": 21600,
            "label-horizons": 300, "train-causal-shadow": 3600}


def _is_windows():
    return os.name == "nt"


def runtime_path():
    custom = os.environ.get("ETH_TESTER_RUNTIME_DIR")
    if custom:
        return Path(custom).resolve()
    if os.name == "nt":
        return Path(os.environ.get("PROGRAMDATA", "C:/ProgramData")) / "ETHTester" / "runtime"
    return Path.cwd() / "runtime"


class Supervisor:
    def __init__(self, *, python=None, path=None, sleep=asyncio.sleep):
        self.python = python or sys.executable
        self.path = Path(path) if path is not None else runtime_path()
        self.sleep = sleep
        self.children = {}
        self.process_starts = {}
        self.status = {}

    def _argv(self, name):
        """Overridable argv for isolated subprocess regression tests."""
        return (self.python, "-m", "eth_tester.cli", name)

    def _record(self, name, state, **extra):
        self.status[name] = {
            "state": state,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **extra,
        }
        self.path.mkdir(parents=True, exist_ok=True)
        target = self.path / "supervisor.json"
        temp = self.path / f".supervisor.{os.getpid()}.tmp"
        temp.write_text(json.dumps(self.status, ensure_ascii=False), encoding="utf-8")
        os.replace(temp, target)

    async def _terminate(self, name, proc):
        """Gracefully stop only the child spawned by this supervisor.

        Windows subprocess.terminate() uses TerminateProcess and cannot run finally
        blocks. CTRL_BREAK_EVENT is attempted first for the dedicated process group.
        """
        if proc.returncode is not None:
            return
        LOGGER.info("Stopping owned worker %s pid=%s", name, proc.pid)
        graceful = False
        if _is_windows():
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                graceful = True
            except (OSError, ValueError, AttributeError, ProcessLookupError):
                LOGGER.warning("CTRL_BREAK unavailable for %s; using terminate fallback", name)
        if not graceful:
            try:
                proc.terminate()
            except ProcessLookupError:
                return
        try:
            await asyncio.wait_for(proc.wait(), timeout=8 if graceful else 12)
            return
        except asyncio.TimeoutError:
            pass
        if graceful and proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=4)
                return
            except asyncio.TimeoutError:
                pass
        if proc.returncode is None:
            LOGGER.error("Worker %s ignored shutdown; killing owned pid=%s", name, proc.pid)
            try:
                proc.kill()
            except ProcessLookupError:
                return
            await proc.wait()

    async def _wait_shutdown(self):
        """Local owned-task stop request, used before Windows task unregistration."""
        marker = self.path / "shutdown.request"
        while True:
            if marker.exists():
                marker.unlink()
                LOGGER.info("Owned supervisor shutdown requested")
                return
            await self.sleep(1)

    async def child(self, name, recurring_seconds=None):
        delay = 3.0
        while True:
            started = time.monotonic()
            proc = None
            try:
                kwargs = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if _is_windows() else {}
                proc = await asyncio.create_subprocess_exec(*self._argv(name), **kwargs)
                self.children[name] = proc
                self.process_starts[name] = time.monotonic()
                self._record(name, "running", pid=proc.pid)
                while True:
                    try:
                        code = await asyncio.wait_for(proc.wait(), timeout=30)
                        break
                    except asyncio.TimeoutError:
                        self._record(name, "running", pid=proc.pid)
                self._record(name, "exited", exit_code=code)
                if recurring_seconds is not None and code == 0:
                    delay = 3.0
                    # An intentional periodic sleep must not appear as a crash.
                    self._record(
                        name, "waiting_for_next_run", exit_code=0,
                        interval_seconds=recurring_seconds,
                        next_run_at=(datetime.now(timezone.utc) +
                                     timedelta(seconds=recurring_seconds)).isoformat(),
                    )
                    await self.sleep(recurring_seconds)
                    continue
                if time.monotonic() - started >= 60:
                    delay = 3.0
                LOGGER.warning("Worker %s exited with %s; restarting in %.1fs", name, code, delay)
                self._record(name, "restarting", exit_code=code, retry_seconds=delay)
                await self.sleep(delay)
                delay = min(delay * 2, 300)
            except asyncio.CancelledError:
                if proc is not None:
                    await self._terminate(name, proc)
                self._record(name, "stopped")
                raise
            except Exception as exc:
                # A status-write failure may happen after process creation.
                # Stop that owned process before discarding the handle.
                if proc is not None and proc.returncode is None:
                    try:
                        await self._terminate(name, proc)
                    except Exception:
                        LOGGER.exception("Unable to stop owned worker %s", name)
                        raise
                LOGGER.exception("Worker %s launch/monitor failure", name)
                self._record(name, "error", error=type(exc).__name__)
                await self.sleep(delay)
                delay = min(delay * 2, 300)
            finally:
                self.children.pop(name, None)
                self.process_starts.pop(name, None)

    async def run(self, settings):
        # Storage B verifies ownership and migration hashes without DDL.
        # The explicit CLI `init` is reserved for the administrator.
        verification = verify_existing_schema(settings)
        LOGGER.info("Verified existing ETH Tester schema: %s", verification)
        specs = [(cmd, None) for cmd in PERMANENT]
        specs.extend(PERIODIC.items())
        # Existing B retention still purges all bot_events, including E's
        # durable paper_trade_review_v1 markers. NEVER enable it by default.
        # Only an explicit owner opt-in after B protection is accepted.
        if os.environ.get("ETH_TESTER_SAFE_CLEANUP_APPROVED", "") == "1":
            specs.append(("cleanup", 86400))
        if settings.tg_bot_token and settings.tg_chat_id:
            if os.environ.get("ETH_TESTER_TG_USER_ID", "").strip().isdecimal():
                specs.append(("telegram", None))
            else:
                LOGGER.error("Telegram disabled: ETH_TESTER_TG_USER_ID not configured")
                self._record("telegram", "disabled_missing_owner_user_id")
        tasks = [
            asyncio.create_task(self.child(name, interval), name=name)
            for name, interval in specs
        ]
        watcher = asyncio.create_task(self._wait_shutdown(), name="supervisor_shutdown")
        from .service_health import HealthWatchdog
        watchdog = asyncio.create_task(HealthWatchdog().run(self, settings),
                                       name="supervisor_health_watchdog")
        loop = asyncio.get_running_loop()
        registered = []
        stop = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                registered.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        signal_watcher = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait(
                [*tasks, watcher, watchdog, signal_watcher], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            for task in (*tasks, watcher, watchdog, signal_watcher):
                task.cancel()
            await asyncio.gather(*tasks, watcher, watchdog, signal_watcher, return_exceptions=True)
            for sig in registered:
                loop.remove_signal_handler(sig)


async def supervise(settings):
    from .service_lease import service_lease
    supervisor = Supervisor()
    with service_lease(supervisor.path):
        await supervisor.run(settings)
