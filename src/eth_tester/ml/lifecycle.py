"""Shadow model registry and objective paper-only promotion/rollback.

This module never sends exchange orders, never changes bot_control, and never
automatically opens a paper position; approval changes model_versions metadata.
"""
import json

from ..database import connection
from . import evaluate, fit, rollback_required, should_retrain, validate


def store_shadow(settings, model, train, validation, shadow, *,
                 eligible_count=None, research_report=None,
                 research_required=False, research_embargo_ms=60_000):
    """Persist an append-only candidate; no approval without independent evidence."""
    if not (train and validation and shadow):
        raise ValueError("three disjoint samples required")
    if eligible_count is not None and (
            type(eligible_count) is not int or eligible_count < (
                len(train) + len(validation) + len(shadow))):
        raise ValueError("invalid immutable eligible-count watermark")
    for partition in (train, validation, shadow):
        if len(validate(partition, max(r.exit_ms for r in partition))) != len(partition):
            raise ValueError("unresolved training evidence")
    identifiers = [r.event_id for group in (train, validation, shadow) for r in group]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("duplicate event across shadow partitions")
    if max(r.exit_ms for r in train) >= min(r.decision_ms for r in validation):
        raise ValueError("training label overlap")
    if model != fit(train):
        raise ValueError("shadow model is not canonical")
    if model.train_to_ms >= min(r.decision_ms for r in validation):
        raise ValueError("model has seen validation period")
    if max(r.exit_ms for r in validation) >= min(r.decision_ms for r in shadow):
        raise ValueError("shadow period overlaps validation")
    report = {"selection_bias": "observed_trades_only",
              "validation": evaluate(model, validation),
              "shadow": evaluate(model, shadow),
              "train_count": len(train),
              "train_to_ms": model.train_to_ms,
              "paper_only": True, "state": "shadow", "model_version": model.version}
    if eligible_count is not None:
        report["eligible_count"] = eligible_count
        report["source_contract"] = "eth_feature_snapshot_v2"
    if research_required or research_report is not None:
        # Validate values against the **supplied immutable cohorts**, not
        # merely an arbitrary 64-hex caller string. This remains a research
        # consistency check, not cryptographic proof of market authenticity.
        from .research import historical_shadow_report
        if research_report is None:
            report_embargo = research_embargo_ms
        else:
            report_embargo = research_report.get("embargo_ms") if isinstance(research_report, dict) else None
        if research_report is not None and (research_report.get("go") is not False or
                                           research_report.get("promotion_eligible") is not False):
            raise ValueError("invalid or promotable historical research report")
        if type(report_embargo) is not int or report_embargo < 60_000 or report_embargo > 86_400_000:
            raise ValueError("invalid historical research embargo")
        expected = historical_shadow_report(
            train, validation, shadow, model, embargo_ms=report_embargo)
        if research_report is not None and research_report != expected:
            raise ValueError("research metrics or fingerprint differ from immutable cohort")
        research_report = expected
    if research_report is not None:
        # The final research verdict is deliberately NOT a promotion gate.
        # Match the exact training model and immutable historical-v2 cohort.
        from .research import CONTRACT, RESEARCH_VERSION
        from re import fullmatch
        if (not isinstance(research_report, dict)
                or research_report.get("version") != RESEARCH_VERSION
                or research_report.get("cohort") != CONTRACT
                or research_report.get("model_version") != model.version
                or research_report.get("promotion_eligible") is not False
                or research_report.get("go") is not False
                or not isinstance(research_report.get("fingerprint_sha256"), str)
                or fullmatch(r"[0-9a-f]{64}", research_report["fingerprint_sha256"]) is None
                or eligible_count is None):
            raise ValueError("invalid or promotable historical research report")
        report["research"] = research_report
        report["research_go"] = False
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991992)")
        if research_report is not None:
            # Same train weights may be re-evaluated when a NEW forward OOS
            # cohort arrives. Preserve each immutable SHA-keyed experiment;
            # otherwise the old version-only dedup would discard new evidence
            # and prevent the eligible_count watermark from advancing.
            existing = conn.execute(
                """SELECT id FROM model_versions WHERE model_name='causal_softmax3'
                   AND params->>'version'=%s AND approved=false
                   AND validation->'research'->>'fingerprint_sha256'=%s
                   ORDER BY id DESC LIMIT 1""",
                (model.version, research_report["fingerprint_sha256"]),
            ).fetchone()
        else:
            existing = conn.execute(
                """SELECT id FROM model_versions WHERE model_name='causal_softmax3'
                   AND params->>'version'=%s ORDER BY id DESC LIMIT 1""",
                (model.version,),
            ).fetchone()
        if existing:
            return existing["id"], report
        row = conn.execute(
            """INSERT INTO model_versions(model_name,params,validation,approved)
               VALUES('causal_softmax3',%s::jsonb,%s::jsonb,false) RETURNING id""",
            (model.as_json(), json.dumps(report))).fetchone()
        return row["id"], report


def run_shadow_cycle(settings, rows, boundaries, previous_trained_count=0,
                     min_new=100, steps=240):
    """Explicit scheduling hook; caller invokes after fresh complete labels arrive.

    Three chronological periods with purging at each boundary. Never uses a
    holdout for optimization or enables paper trading.
    """
    if len(boundaries) != 2:
        raise ValueError("expected validation_start and shadow_start")
    if steps != 240:
        raise ValueError("noncanonical training optimizer prohibited")
    if not should_retrain(previous_trained_count, len(rows), min_new):
        return None
    from . import chronological
    train, validation, shadow = chronological(rows, boundaries[0], boundaries[1])
    model = fit(train, steps=steps)
    return store_shadow(settings, model, train, validation, shadow)


def admit_paper_candidate(settings, candidate_id, model, train, validation,
                          shadow, *, champion=None, gates=None):
    """Fail closed until MASTER defines verifiable external OOS attestation.

    The available Example type does not cryptographically attest the origin of
    independent labels or executed-trade selection. Never update approved=true.
    """
    return False, "independent certified OOS promotion contract not approved"


def rollback_paper_model(settings, active_id, previous_id, reference_metrics,
                         recent_metrics):
    """Paper metadata rollback on verified deterioration, fail-closed otherwise."""
    if active_id == previous_id or not rollback_required(reference_metrics, recent_metrics):
        return False
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991992)")
        rows = conn.execute(
            """SELECT id,model_name,approved,validation FROM model_versions
               WHERE id IN (%s,%s) FOR UPDATE""", (active_id, previous_id)
        ).fetchall()
        models = {r["id"]: r for r in rows}
        if len(models) != 2 or not models[active_id]["approved"]:
            return False
        if models[previous_id]["model_name"] != "causal_softmax3":
            return False
        historical = models[previous_id]["validation"] or {}
        if isinstance(historical, str):
            historical = json.loads(historical)
        if historical.get("state") not in ("paper_approved", "rollback_restored"):
            return False
        conn.execute("UPDATE model_versions SET approved=false WHERE id=%s", (active_id,))
        conn.execute(
            """UPDATE model_versions SET approved=true,
               validation=coalesce(validation,'{}'::jsonb) ||
               '{"state":"rollback_restored","paper_only":true}'::jsonb
               WHERE id=%s""", (previous_id,))
    return True
