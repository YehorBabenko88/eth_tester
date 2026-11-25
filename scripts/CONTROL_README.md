# G Control — paper-only operations

## Telegram
Store TG_BOT_TOKEN and TG_CHAT_ID in a protected .env, never in Git. Set
ETH_TESTER_TG_USER_ID in the process environment to the numeric Telegram user ID.
Matching private chat ID, user ID and non-bot sender are all mandatory.
Run python -m eth_tester.cli telegram. /start displays Russian keyboard without starting trades.
Commands: /startsim /pause /resume /stop, /position /history /stats, /chart 123,
/progress /disk /system /processes /ws /errors /gaps /levels.
Control changes paper entries only. An open paper position remains monitored; no live orders.
Command audit writes metadata to bot_events and never records message bodies or tokens.
CPU/RAM uses optional psutil when installed; no new shared dependency is imposed.
Progress shows recorded source and cursor coverage, NOT a fabricated universal percentage.
Remote /delete is disabled until Storage #2 publishes an audited ownership-scoped contract.

## Supervisor
python -m eth_tester.cli run starts live, analyse, engine, periodic ingestion/aggregation,
and optionally telegram. Unexpected exits (including zero for permanent processes) restart
with bounded 3–300 second backoff. Ctrl+C cancels workers and terminates owned PIDs,
waiting 12 seconds before killing an unresponsive child. Existing bot_control state is never
automatically changed. Status: ETH_TESTER_RUNTIME_DIR/supervisor.json (default ./runtime on Linux;
C:\ProgramData\ETHTester\runtime\supervisor.json on Windows). The verified Windows
uninstaller requests graceful worker shutdown with runtime/shutdown.request.

## Windows
Install dependencies in .venv and configure a protected .env plus required
ETH_TESTER_TG_USER_ID environment variable for the LocalService account.
Grant NT AUTHORITY\LOCAL SERVICE only the read/execute ACLs needed for the installation,
and explicit read on .env. Never grant Everyone access.
Run elevated:
powershell -NoProfile -ExecutionPolicy Bypass -File install\install.ps1
This creates an AtStartup scheduled task as restricted LocalService. No user password stored.
Verify PostgreSQL localhost permissions, Telegram network and task history under actual account.
Unregister elevated:
powershell -NoProfile -ExecutionPolicy Bypass -File install\uninstall.ps1
Removal validates manifest and task action before stopping/removing it. It intentionally
preserves DB, downloaded ticks, .env, other files and PostgreSQL server. Full destructive
uninstall awaits Storage Guardian's ownership-scoped deletion and local confirmation API.

## Acceptance still pending
Real PostgreSQL and Windows Task Scheduler/ACL tests; authorized and unauthorized Telegram
integration tests; Ctrl+C and forced worker crash; reboot and power-loss recovery; disk full.


## G round 2 Windows acceptance (disposable machine only)
Installer now refuses broad-write ACLs on its code root/Python executable and broad-read
ACLs on .env. If a Telegram token is configured in .env, the LocalService account must
inherit a numeric machine-level ETH_TESTER_TG_USER_ID. Installation executes only a
read-only \`python -m eth_tester.cli --help\` import preflight before registering the
AtStartup task. It does not bootstrap, migrate, drop or wipe PostgreSQL.

Before native approval on a throwaway Windows VM:
1. Install project and Python under an admin-controlled root; configure LocalService
   read/execute and restrict .env to admins and LocalService. Put secrets outside Git.
2. Validate the read-only CLI preflight and Task Scheduler action, principal, trigger,
   ownership manifest and non-adoption of a pre-existing foreign task.
3. Start the task with a disposable dedicated PostgreSQL database only; validate
   Telegram owner rejection, paper-only start/pause/stop, worker exit/restart,
   WS freshness and progress using independent observations.
4. Stop via owned uninstall marker and verify no child process or registered task
   survives; verify unrelated task, directory, database, backups and archives are
   unchanged. Repeat after clean reboot and forced worker crash.
5. Save task XML/ACLs, child PID evidence, DB OID, process logs and exact commit SHA.
   Until that evidence exists, Windows native acceptance remains RUNTIME NO_GO.

On Windows the Supervisor first attempts CTRL_BREAK_EVENT for its dedicated child
process group; if console signals are unavailable, it falls back to terminate/kill.
This is not a guarantee of graceful shutdown when a service has no console.
Database migration/bootstrap remains B-owned; Supervisor currently calls init_database
on startup and therefore requires a separately approved least-privilege DB contract.
Remote Telegram /delete remains disabled despite Storage's manifest APIs.


## Round 4: read-only startup and guarded Windows ownership

Normal python -m eth_tester.cli run calls Storage B's
verify_existing_schema(settings) *before* launching any workers. It checks
dedicated database ownership, migration checksums, and mandatory tables
without DDL. Only the administrator explicitly runs python -m eth_tester.cli init.
Missing/stale schema blocks startup. The service role needs CONNECT, USAGE
on public schema, and SELECT on eth_storage_identity/eth_schema_migrations;
never give the worker CREATE DATABASE/DDL/migration rights.

The Windows installer is NOT approved for working computers until native
disposable-VM acceptance. It now rejects foreign scheduled tasks, unknown
pre-existing ProgramData/ETHTester runtime directories, broadly readable .env,
broadly writable install components, or missing machine-level numeric
ETH_TESTER_TG_USER_ID with Telegram configured. Read-only CLI and DB
preflights precede registering the one LocalService startup task.

The uninstaller checks the task's LocalService principal as well as manifest
and command-line. DB, downloads, backups, source, secrets and runtime are
preserved. Reinstallation after uninstall requires explicit operator inspection
of the preserved runtime root; do not recursively delete it automatically.
The installer can leave a partial runtime root after failure; inspect manually.

Acceptance requires a disposable Windows VM: PowerShell AST parser,
Task Scheduler XML, LocalService ACL/token evidence, reboot, worker crash,
owned-child shutdown, graceful uninstall, and non-owned resource preservation.
Tests/integration/test_control_pg.py exercises a disposable local PostgreSQL
database and read-only role with migration-checksum corruption; no real trades
or exchange connections occur. Telegram /delete remains disabled.
