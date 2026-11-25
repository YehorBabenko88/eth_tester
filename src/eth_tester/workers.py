"""Independent bounded analytics cycles with per-subsystem failure isolation."""
import asyncio
import json
from collections import Counter, deque
from time import monotonic

from .analytics import build_footprint, build_levels, enrich_features
from .analytics_core import FRAMES
from .cvd_incremental import update_cvd
from .indicator_incremental import indicator_worker_step
from .snapshot_producer import certify_snapshot
from .live_paper_window import (publish_latest_bounded_live,
                                read_live_paper_status)
from .database import connection
from .rollups import aggregate_pending

LIVE_FRAMES = tuple(tf for tf in FRAMES if tf not in ("W", "M"))


_FAST_SOURCE_REASONS = frozenset({
    "b_immutable_paper_only", "b_witness_already_issued",
    "l2_replay_backlog_or_invalid", "official_closed_1m_missing",
    "independent_closed_1m_rest_receipt_missing",
    "missing_or_mixed_ws_trade_receipts",
    "fresh_replayed_l2_receipt_missing",
    "independent_recent_oi_receipt_missing",
    "b_source_validation_rejected", "decision_window_elapsed",
})


def summarize_fast_lane(samples):
    """Finite rolling diagnostics, never a certification or trading signal.

    Near-real-time statistics are from *observed processing*, not backfilled
    exchange events. B remains the only authority for paper eligibility.
    """
    rows = list(samples)
    reasons = Counter()
    for item in rows:
        reason = item.get("reason")
        reasons[reason if reason in _FAST_SOURCE_REASONS else "other"] += 1
    latencies = sorted(
        x for row in rows
        if row.get("status") == "published"
        for x in (row.get("publication_latency_ms"),)
        if type(x) is int and 0 <= x <= 90000
    )
    def rank(pct):
        if not latencies:
            return None
        return latencies[max(0, (len(latencies) * pct + 99) // 100 - 1)]
    return {
        "sample_count": len(rows),
        "published_samples": sum(r.get("status") == "published" for r in rows),
        "latency_p50_ms": rank(50), "latency_p95_ms": rank(95),
        "latency_max_ms": latencies[-1] if latencies else None,
        "reason_counts": dict(sorted(reasons.items())),
        "quality": "observed_paper_only_not_ml_certified",
    }



async def analytics_worker(settings):
    recent_fast_samples = deque(maxlen=256)

    async def run_fast_window():
        # Independent 5s replay + B-only paper witness; never ML eligible.
        while True:
            try:
                started = monotonic()
                sample = await asyncio.to_thread(
                    publish_latest_bounded_live, settings)
                sample = dict(sample)
                sample["processing_elapsed_ms"] = int(
                    (monotonic() - started) * 1000)
                recent_fast_samples.append(sample)
                sample["rolling_diagnostics"] = summarize_fast_lane(
                    recent_fast_samples)
                condition = ("ok" if sample.get("status") == "published" else
                             "partial" if sample.get("status") in
                             ("pending", "expired") else "degraded")
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                        VALUES('live_paper_window',%s,%s::jsonb)
                        ON CONFLICT(component) DO UPDATE SET
                        status=EXCLUDED.status,detail=EXCLUDED.detail,
                        updated_at=clock_timestamp()""",
                        (condition, json.dumps(sample),))
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
            # Separate from the fast publisher: report the last actually
            # DB-persisted paper-only state, never a guessed positive flag.
            checked = await run("live_paper_status", read_live_paper_status,
                                settings)
            if checked is not None:
                live_paper = checked
            # The independent 5s lane owns ALL L2 replay; avoid a second
            # unbounded 180s replay competing with fresh source admission.
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
            # Operational cycle health is not a market-data quality or ML
            # certification verdict. G exposes those as independent axes.
            status = ("degraded" if failures else
                      "partial" if certification_pending else "ok")
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                    VALUES('analytics',%s,%s::jsonb)
                    ON CONFLICT(component) DO UPDATE SET status=EXCLUDED.status,
                      detail=EXCLUDED.detail,updated_at=now()""",
                    (status, json.dumps({"cycle": cycle,
                                         "errors": len(failures),
                                         "completed": not failures,
                                         "live_paper": live_paper,
                                         "paper_live_published":
                                             live_paper.get("status") == "published",
                                         "historical_ml_certified": False,
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
