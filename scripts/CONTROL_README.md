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


## Round 6: bounded paper-only research scheduling (Control G)

`python -m eth_tester.cli learn` is now **an alias for causal shadow-only
research**, never the disabled legacy PnL trainer. The explicit commands are
`python -m eth_tester.cli train-causal-shadow` and
`python -m eth_tester.cli label-horizons`.

The supervisor schedules an independent **F-owned, post-horizon label**
attempt every **300 seconds** and an F-owned **causal shadow training**
attempt every **3600 seconds**. These are bounded *one-shot subprocesses*,
not loops that relabel entry records. The G dispatcher delegates only to
`eth_tester.ml.label_worker.run_label_horizons(settings, limit=100)`
and `eth_tester.learning.run_causal_shadow_cycle(settings,
min_new=100, min_rows=180, embargo_ms=60000)`.

If the F source-proof worker has not yet been published, the job records
`blocked_missing_verified_label_worker` or
`blocked_missing_causal_shadow_cycle` in PostgreSQL `bot_heartbeat`
and exits normally, so the supervisor does not rapidly restart a known
missing dependency. Unexpected worker failures are recorded with an
exception **type only**, then propagated to the bounded supervisor retry.
Research reports are reduced to status and integer counts, never credentials
or untrusted source text. F owns persistent monotone trained-label cursors
and the real, independently checked label ledger; G does not create
certificates, labels or model approvals.

Use `python scripts/control_readiness_probe.py` to collect read-only
observations from the configured dedicated PostgreSQL database. The tool
uses read-only transactions and *always* returns
`production_go=false`. Healthy feed or process heartbeats are not
cryptographic market-quality proof, and the newly approved
`bounded_live_observed` paper-only tier is **not** historical
`eth_feature_snapshot_v2` certification and must never make samples
ML-eligible.

Acceptance still needs real bounded public-market observations, source
receipt/candle/WS-gap/L2/OI lineage, separate historical and paper
quality, reproducible PostgreSQL integration on PG17/18, native disposable
Windows lifecycle, plus F/B's independently checked post-horizon worker.
No live trading, no retrospective paper opens, no production GO.

## Round 9: 24/7 owner-scoped paper laboratory — STATUS: NOT ACCEPTED

Supervisor acquires a **nonblocking OS-held singleton lock** in the configured
protected runtime path (`supervisor.instance.lock`). A dead PID file does
not block recovery and a second supervisor cannot create duplicate workers.
Only `engine` and `analyse` currently expose trustworthy continuous
PostgreSQL heartbeats. Every **30 seconds** the watchdog checks those with the
**PostgreSQL clock**. After **180 seconds** of child startup grace, **two**
consecutive stale/missing heartbeats are needed before terminating **only**
the owned child. Subsequent forced recovery is limited to **once per worker
per 600 seconds**. A PostgreSQL outage is **indeterminate**: never kill
workers, infer market completeness, or unpause paper trading. The existing
per-child exponential restart/backoff handles actual exits. The public
`live` source currently has no independent continuous process heartbeat;
low trade volume is **not** proof of process hang. A must supply that contract
before using source inactivity as an auto-restart trigger.

The optional, uninstalled `install/eth-tester.service` is a non-root,
systemd-sandboxed **example** for disposable Linux acceptance; it does not
register a service. Windows remains a single owner-scoped LocalService
startup task. Require native restart/reboot, ACL, network-drop, schema,
least-privilege DB and owned-child shutdown acceptance before install.

Telegram adds owner-only, private-chat `/trade ID` with detailed paper costs,
`/signals`, `/models`, `/research`, `/risk`, `/health`,
`/coverage`, `/maintenance`. Responses are bounded into pages instead
of silently slicing at 3900 characters. The per-process burst guard is
**not** durable or distributed. **Remote /delete remains disabled**.
No automatic "daily report delivered" or "durable alert deduplicated" claim:
B has NOT yet published a persistent authorized outbox/attempt-ack ledger.
Until then these views are pull-only and Telegram outage does not
mutate bot_control or alter paper decisions.

**Important retention P0:** Existing B `cleanup_diagnostics` deletes
`bot_events` older than 180 days, which may include E's
`paper_trade_review_v1` replay/idempotency evidence. Therefore periodic
`cleanup` is **OFF by default**. Even the opt-in flag
`ETH_TESTER_SAFE_CLEANUP_APPROVED=1` MUST NOT be set until B independently
preserves all research/provenance records while allowing bounded diagnostic
cleanup; operator review is mandatory. B owns the purge SQL, schema and
ownership ledger. G neither deletes raw ETHUSDT prints/candles/trades,
nor truncates archived series or ML labels.

Executed locally on a stub-only **isolated G copy**:
`PYTHONPATH=src pytest -q tests/test_control_round9.py
tests/test_control_round8.py tests/test_control_readiness_probe.py`
→ **37 PASS** (round9 15; earlier G fixtures 22).
`python -m compileall -q src tests` passed. NOT full repository,
PG17/18, native Windows, real Telegram API, or real Bybit acceptance.
No live orders were placed. **CODE / RUNTIME / ML NO_GO**.
