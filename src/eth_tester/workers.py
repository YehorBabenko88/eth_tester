"""Independent bounded analytics cycles with per-subsystem failure isolation."""
import asyncio
import json

from .analytics import build_footprint, build_levels, enrich_features
from .analytics_core import FRAMES
from .book_analysis import replay_orderbook
from .cvd_incremental import update_cvd
from .indicator_incremental import indicator_worker_step
from .snapshot_producer import certify_snapshot
from .live_paper_window import inspect_latest_live_bar
from .database import connection
from .rollups import aggregate_pending

LIVE_FRAMES = tuple(tf for tf in FRAMES if tf not in ("W", "M"))


async def analytics_worker(settings):
    cycle = 0
    async def run_fast_window():
        # Independent 5s status sampler; no B witness, no admission, no ML.
        while True:
            try:
                sample = await asyncio.to_thread(inspect_latest_live_bar, settings)
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                        VALUES('live_paper_window','partial',%s::jsonb)
                        ON CONFLICT(component) DO UPDATE SET
                        status=EXCLUDED.status,detail=EXCLUDED.detail,
                        updated_at=clock_timestamp()""",
                        (json.dumps(sample),))
            except Exception as exc:
                try:
                    with connection(settings) as conn:
                        conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                            VALUES('live_paper_window','degraded',%s::jsonb)
                            ON CONFLICT(component) DO UPDATE SET
                            status=EXCLUDED.status,detail=EXCLUDED.detail,
                            updated_at=clock_timestamp()""",
                            (json.dumps({"reason": str(exc)[:200]}),))
                except Exception:
                    pass  # Next independent sample retries DB availability.
            await asyncio.sleep(5)

    fast_task = asyncio.create_task(run_fast_window())
    try:
        await _analytics_cycles(settings)
    finally:
        fast_task.cancel()
        await asyncio.gather(fast_task, return_exceptions=True)


async def _analytics_cycles(settings):
    cycle = 0
    while True:
        failures = []
        certification_pending = []
        live_paper = {"status": "pending", "reason": "not_checked"}

        async def run(name, function, *args):
            try:
                return await asyncio.to_thread(function, *args)
            except Exception as exc:
                failures.append({"component": name, "error": str(exc)[:400]})
                return None

        try:
            # The fast 1m availability gate runs ahead of expensive 13-TF work.
            # Until B publishes independently verified source witnesses this
            # remains an explicit pending state, NEVER certified/ML eligible.
            checked = await run("live_paper_window", inspect_latest_live_bar,
                                settings)
            if checked is not None:
                live_paper = checked
            await run("orderbook", replay_orderbook, settings)
            # Archive chunks are checksum/idempotency gated. One day per cycle
            # bounds DB load and makes backfill resume after interruption.
            await run("archive_rollup", aggregate_pending, settings, 1)
            for tf in FRAMES:
                if cycle % 3 == 0:
                    await run(f"levels:{tf}", build_levels, settings, tf)
                if tf in LIVE_FRAMES:
                    await run(f"live_footprint:{tf}", build_footprint, settings, tf, 2)
                    if tf in ("1", "5", "15", "60"):
                        await run(f"feature_enrichment:{tf}",
                                  enrich_features, settings, tf, 2)
                if cycle % 3 == 0:
                    await run(f"cvd:{tf}", update_cvd, settings, tf)
                    await run(f"indicators:{tf}", indicator_worker_step, settings, tf)
                    proof = await run(f"snapshot:{tf}", certify_snapshot, settings, tf)
                    if proof and proof.get("status") == "rejected":
                        certification_pending.append({"tf": tf, "reason": proof["reason"]})
            status = ("degraded" if failures else
                      "partial" if certification_pending or
                      live_paper.get("status") != "ready" else "ok")
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                    VALUES('analytics',%s,%s::jsonb)
                    ON CONFLICT(component) DO UPDATE SET status=EXCLUDED.status,
                      detail=EXCLUDED.detail,updated_at=now()""",
                    (status, json.dumps({"cycle": cycle,
                                         "errors": len(failures),
                                         "completed": not failures,
                                         "live_paper": live_paper,
                                         "ml_certified": False,
                                         "certification_pending": certification_pending})))
                if failures:
                    conn.execute("""INSERT INTO bot_events(severity,event,data)
                        VALUES('error','analytics_failure',%s::jsonb)""",
                        (json.dumps({"cycle": cycle, "failed": failures}),))
        except Exception as exc:
            # A broken heartbeat or DB connection is not a successful cycle.
            print(f"analytics heartbeat failure: {exc}", flush=True)
        cycle += 1
        await asyncio.sleep(180)
