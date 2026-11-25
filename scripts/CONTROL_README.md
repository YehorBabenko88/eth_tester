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
