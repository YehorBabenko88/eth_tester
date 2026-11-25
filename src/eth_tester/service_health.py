"""Fail-closed 24/7 process health checks, never market-quality certification.

Only PostgreSQL application heartbeats that are emitted continuously by the
corresponding component can justify a hung-worker restart. REST/WS market
activity is NOT a worker liveness clock. A DB outage is indeterminate, not a
reason to kill consumers and multiply reconnect load.
"""

import asyncio
import logging
import time


LOGGER = logging.getLogger(__name__)
# Analytics has TWO independently scheduled loops. A fast publisher can remain
# live while the slow aggregator is blocked; require both heartbeats.
HEARTBEATS = {"engine": (("paper_engine", 120),),
              "analyse": (("live_paper_window", 90), ("analytics", 420))}
CHECK_SECONDS = 30
STARTUP_GRACE_SECONDS = 180
RESTART_COOLDOWN_SECONDS = 600
CONSECUTIVE_MISSES = 2


def db_health_snapshot(settings):
    """One read-only DB clock check. No other worker or market data is changed."""
    # Database read-only and server-side bounded even when another writer is
    # blocked: a stuck health query must not suspend the only watchdog.
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(
            settings.database_url, row_factory=dict_row, connect_timeout=5,
            options="-c default_transaction_read_only=on "
                    "-c statement_timeout=4000 -c lock_timeout=2000"
    ) as conn:
        rows = conn.execute("""SELECT component,
               EXTRACT(EPOCH FROM (clock_timestamp()-updated_at)) AS age_seconds
            FROM public.bot_heartbeat WHERE component=ANY(%s)""",
            ([component for checks in HEARTBEATS.values()
              for component, _ in checks],)).fetchall()
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

    @staticmethod
    def _safe_record(supervisor, *args, **kwargs):
        # Observability must never become a single point of service shutdown.
        try:
            supervisor._record(*args, **kwargs)
        except Exception as exc:
            LOGGER.error("Watchdog status persistence failed (%s)", type(exc).__name__)

    async def check_once(self, supervisor, settings):
        try:
            ages = await asyncio.to_thread(self.checker, settings)
        except Exception as exc:
            self.misses.clear()
            LOGGER.warning("Health DB unavailable: %s", type(exc).__name__)
            self._safe_record(supervisor, "watchdog", "database_unavailable",
                              error_type=type(exc).__name__, restarted_workers=0)
            return "database_unavailable"
        now = self.clock()
        for name, components in HEARTBEATS.items():
            proc = supervisor.children.get(name)
            born = supervisor.process_starts.get(name)
            if (proc is None or proc.returncode is not None or born is None
                    or now - born < STARTUP_GRACE_SECONDS):
                self.misses.pop(name, None)
                continue
            failed = [(component, classify_heartbeat(ages.get(component), maximum))
                      for component, maximum in components]
            failed = [(component, condition) for component, condition in failed
                      if condition != "healthy"]
            if not failed:
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
            component, condition = failed[0]
            self._safe_record(supervisor, "watchdog", "recovering_worker",
                              worker=name, heartbeat=component, reason=condition)
            LOGGER.warning("Restarting owned hung worker %s (%s heartbeat)", name, condition)
            try:
                await supervisor._terminate(name, proc)
            except Exception as exc:
                LOGGER.error("Owned child recovery failed (%s)", type(exc).__name__)
                self._safe_record(supervisor, "watchdog", "recovery_failed",
                                  worker=name, error_type=type(exc).__name__)
        return "checked"

    async def run(self, supervisor, settings):
        while True:
            await asyncio.sleep(CHECK_SECONDS)
            try:
                await self.check_once(supervisor, settings)
            except Exception as exc:
                # An internal diagnostic exception must not finish the watched
                # task and trigger Supervisor.run FIRST_COMPLETED shutdown.
                self.misses.clear()
                LOGGER.error("Watchdog cycle isolated (%s)", type(exc).__name__)
