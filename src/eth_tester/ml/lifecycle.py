"""Shadow model registry and objective paper-only promotion/rollback.

This module never sends exchange orders, never changes bot_control, and never
automatically opens a paper position; approval changes model_versions metadata.
"""
import json

from ..database import connection
from . import Gates, approve_candidate, evaluate, fit, rollback_required, should_retrain


def store_shadow(settings, model, train, validation, shadow):
    """Persist an append-only candidate; no approval without independent evidence."""
    if not (train and validation and shadow):
        raise ValueError("three disjoint samples required")
    if model.train_to_ms >= min(r.decision_ms for r in validation):
        raise ValueError("model has seen validation period")
    if max(r.exit_ms for r in validation) >= min(r.decision_ms for r in shadow):
        raise ValueError("shadow period overlaps validation")
    report = {"validation": evaluate(model, validation),
              "shadow": evaluate(model, shadow),
              "train_count": len(train),
              "train_to_ms": model.train_to_ms,
              "paper_only": True, "state": "shadow", "model_version": model.version}
    with connection(settings) as conn:
        existing = conn.execute(
            """SELECT id FROM model_versions WHERE model_name='causal_softmax3'
               AND params->>'version'=%s ORDER BY id DESC LIMIT 1""",
            (model.version,)).fetchone()
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
    if not should_retrain(previous_trained_count, len(rows), min_new):
        return None
    from . import chronological
    train, validation, shadow = chronological(rows, boundaries[0], boundaries[1])
    model = fit(train, steps=steps)
    return store_shadow(settings, model, train, validation, shadow)


def admit_paper_candidate(settings, candidate_id, model, train, validation,
                          shadow, *, champion=None, gates=Gates()):
    """Atomic metadata promotion if independent gates pass; not a trading command."""
    approved, reason = approve_candidate(model, champion, train, validation, shadow, gates)
    if not approved:
        return False, reason
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991992)")
        candidate = conn.execute(
            "SELECT id,model_name,params,approved FROM model_versions WHERE id=%s FOR UPDATE",
            (candidate_id,)).fetchone()
        if not candidate or candidate["model_name"] != "causal_softmax3":
            return False, "unknown candidate"
        serialized = json.loads(model.as_json())
        params = candidate["params"]
        if isinstance(params, str):
            params = json.loads(params)
        if params != serialized:
            return False, "candidate artifact mismatch"
        incumbent = conn.execute(
            "SELECT id,params FROM model_versions WHERE approved=true FOR UPDATE"
        ).fetchall()
        if len(incumbent) > 1:
            return False, "ambiguous champion"
        if incumbent and incumbent[0]["id"] != candidate_id:
            previous = incumbent[0]["params"]
            if isinstance(previous, str):
                previous = json.loads(previous)
            if champion is None or previous != json.loads(champion.as_json()):
                return False, "champion changed since evaluation"
        if incumbent and incumbent[0]["id"] == candidate_id:
            return True, "already approved paper metadata"
        conn.execute("UPDATE model_versions SET approved=false WHERE approved=true")
        conn.execute(
            """UPDATE model_versions SET approved=true,
               validation=coalesce(validation,'{}'::jsonb) ||
               %s::jsonb WHERE id=%s""",
            (json.dumps({"state": "paper_approved",
                         "paper_only": True, "gate_reason": reason}), candidate_id))
    return True, reason


def rollback_paper_model(settings, active_id, previous_id, reference_metrics,
                         recent_metrics):
    """Paper metadata rollback on verified deterioration, fail-closed otherwise."""
    if active_id == previous_id or not rollback_required(reference_metrics, recent_metrics):
        return False
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991992)")
        rows = conn.execute(
            """SELECT id,model_name,approved FROM model_versions
               WHERE id IN (%s,%s) FOR UPDATE""", (active_id, previous_id)
        ).fetchall()
        models = {r["id"]: r for r in rows}
        if len(models) != 2 or not models[active_id]["approved"]:
            return False
        if models[previous_id]["model_name"] != "causal_softmax3":
            return False
        conn.execute("UPDATE model_versions SET approved=false WHERE id=%s", (active_id,))
        conn.execute(
            """UPDATE model_versions SET approved=true,
               validation=coalesce(validation,'{}'::jsonb) ||
               '{"state":"rollback_restored","paper_only":true}'::jsonb
               WHERE id=%s""", (previous_id,))
    return True
