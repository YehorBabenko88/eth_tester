"""Shadow model registry and objective paper-only promotion/rollback.

This module never sends exchange orders, never changes bot_control, and never
automatically opens a paper position; approval changes model_versions metadata.
"""
import json

from ..database import connection
from . import evaluate, fit, rollback_required, should_retrain, validate


def store_shadow(settings, model, train, validation, shadow):
    """Persist an append-only candidate; no approval without independent evidence."""
    if not (train and validation and shadow):
        raise ValueError("three disjoint samples required")
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
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991992)")
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
