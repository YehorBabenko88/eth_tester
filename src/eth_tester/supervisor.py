"""Paper-only worker supervisor with bounded restart and graceful child shutdown.

The supervisor has no authority to change bot_control: an operator stop stays stopped.
"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import time

from .database import init_database

LOGGER = logging.getLogger(__name__)
PERMANENT = ("live", "analyse", "engine")
PERIODIC = {"backfill": 86400, "archives": 86400, "aggregate": 21600}


def runtime_path():
    return Path(os.environ.get("ETH_TESTER_RUNTIME_DIR", "runtime")).resolve()


class Supervisor:
    def __init__(self, *, python=None, path=None, sleep=asyncio.sleep):
        self.python = python or sys.executable
        self.path = Path(path) if path is not None else runtime_path()
        self.sleep = sleep
        self.children = {}
        self.status = {}

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
        if proc.returncode is not None:
            return
        LOGGER.info("Stopping owned worker %s pid=%s", name, proc.pid)
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=12)
        except asyncio.TimeoutError:
            LOGGER.error("Worker %s ignored shutdown; killing owned pid=%s", name, proc.pid)
            proc.kill()
            await proc.wait()

    async def child(self, name, recurring_seconds=None):
        delay = 3.0
        while True:
            started = time.monotonic()
            proc = None
            try:
                kwargs = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {}
                proc = await asyncio.create_subprocess_exec(
                    self.python, "-m", "eth_tester.cli", name, **kwargs
                )
                self.children[name] = proc
                self._record(name, "running", pid=proc.pid)
                code = await proc.wait()
                self._record(name, "exited", exit_code=code)
                if recurring_seconds is not None and code == 0:
                    delay = 3.0
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
                LOGGER.exception("Worker %s launch/monitor failure", name)
                self._record(name, "error", error=type(exc).__name__)
                await self.sleep(delay)
                delay = min(delay * 2, 300)
            finally:
                self.children.pop(name, None)

    async def run(self, settings):
        init_database(settings)
        specs = [(cmd, None) for cmd in PERMANENT]
        specs.extend(PERIODIC.items())
        if settings.tg_bot_token and settings.tg_chat_id:
            specs.append(("telegram", None))
        tasks = [asyncio.create_task(self.child(name, interval), name=name)
                 for name, interval in specs]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def supervise(settings):
    await Supervisor().run(settings)
