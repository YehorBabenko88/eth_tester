"""Fail-closed 24/7 process health checks, never market-quality certification.

Only PostgreSQL application heartbeats that are emitted continuously by the
corresponding component can justify a hung-worker restart. REST/WS market
activity is NOT a worker liveness clock. A DB outage is indeterminate, not a
reason to kill consumers and multiply reconnect load.
"""

import asyncio
import logging
import time

from .database import connection

LOGGER = logging.getLogger(__name__)
HEARTBEATS = {"engine": ("paper_engine", 120),
              "analyse": ("live_paper_window", 90)}
CHECK_SECONDS = 30
STARTUP_GRACE_SECONDS = 180
RESTART_COOLDOWN_SECONDS = 600
CONSECUTIVE_MISSES = 2


def db_health_snapshot(settings):
    """One read-only DB clock check. No other worker or market data is changed."""
    with connection(settings) as conn:
        rows = conn.execute("""SELECT component,
               EXTRACT(EPOCH FROM (clock_timestamp()-updated_at)) AS age_seconds
            FROM bot_heartbeat WHERE component=ANY(%s)""",
            ([item[0] for item in HEARTBEATS.values()],)).fetchall()
    return {row["component"]: float(row["age_seconds"]) for row in rows}


def classify_heartbeat(age, threshold):
    """Negative/future or missing DB time cannot be treated as healthy."""
    if age is None or not isinstance(age, (float, int)) or isinstance(age, bool):
        return "missing"
    if not (-2 <= age <= threshold):
        return "stale"
    return "healthy"


class HealthWatchdog:
    def __init__(self, *, clock=time.monotonic, checker=db_health_snapshot):
        self.clock = clock
        self.checker = checker
        self.misses = {}
        self.last_restart = {}

    async def check_once(self, supervisor, settings):
        try:
            ages = await asyncio.to_thread(self.checker, settings)
        except Exception as exc:
            self.misses.clear()
            LOGGER.warning("Health DB unavailable: %s", type(exc).__name__)
            supervisor._record("watchdog", "database_unavailable",
                               error_type=type(exc).__name__,
                               restarted_workers=0)
            return "database_unavailable"
        now = self.clock()
        for name, (component, maximum) in HEARTBEATS.items():
            proc = supervisor.children.get(name)
            born = supervisor.process_starts.get(name)
            if (proc is None or proc.returncode is not None or born is None
                    or now - born < STARTUP_GRACE_SECONDS):
                self.misses.pop(name, None)
                continue
            condition = classify_heartbeat(ages.get(component), maximum)
            if condition == "healthy":
                self.misses.pop(name, None)
                continue
            self.misses[name] = self.misses.get(name, 0) + 1
            if self.misses[name] < CONSECUTIVE_MISSES:
                continue
            if now - self.last_restart.get(name, float("-inf")) < RESTART_COOLDOWN_SECONDS:
                continue
            # Restart only our own tracked PID. A live/market outage alone
            # never triggers a process kill; restart does not change bot_control.
            self.last_restart[name] = now
            self.misses[name] = 0
            supervisor._record("watchdog", "recovering_worker", worker=name,
                               heartbeat=component, reason=condition)
            LOGGER.warning("Restarting owned hung worker %s (%s heartbeat)", name, condition)
            await supervisor._terminate(name, proc)
        return "checked"

    async def run(self, supervisor, settings):
        while True:
            await asyncio.sleep(CHECK_SECONDS)
            await self.check_once(supervisor, settings)
