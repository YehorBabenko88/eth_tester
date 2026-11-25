"""Chronological shadow-only learning; never auto-promotes a model."""
import json
import math
from .database import connection

FEATURES=("risk_pct","reward_pct","atr_pct","direction_long")

def vector(row):
    try:
        entry=float(row["entry"])
        reason=row["reason"]
        if isinstance(reason,str):
            reason=json.loads(reason)
        if entry<=0:
            return None
        return (abs(float(row["entry"]-row["stop"]))/entry,
                abs(float(row["take"]-row["entry"]))/entry,
                float(reason.get("atr",0))/entry,
                1.0 if row["direction"]=="long" else 0.0)
    except (TypeError,ValueError,KeyError,ZeroDivisionError):
        return None

def sigmoid(value):
    return 1/(1+math.exp(-max(-25,min(25,value))))

def brier(weights,rows):
    return sum((sigmoid(weights[0]+sum(w*x for w,x in zip(weights[1:],v)))-label)**2
               for v,label in rows)/len(rows) if rows else None

def train_shadow(settings, minimum=100, epochs=20):
    """Legacy PnL-based training is disabled: no immutable causal evidence."""
    raise RuntimeError(
        "Legacy paper-trade labels are not causally certified; "
        "use train_causal_shadow with immutable snapshots"
    )


def train_causal_shadow(settings, as_of_ms, *, source="snapshots", tf="1",
                        min_rows=180, min_new=100, previous_count=0,
                        embargo_ms=0):
    """Retrain only when enough independently labeled, point-in-time data exist.

    Does not approve a model, enable the bot, or place trades. The two holdout
    periods are reserved for validation and independent shadow evaluation.
    """
    from .ml import chronological, fit, should_retrain
    from .ml.extract import load_causal_feature_examples
    from .ml.lifecycle import store_shadow

    if min_rows < 30 or embargo_ms < 0:
        raise ValueError("invalid training policy")
    if source != "snapshots":
        return {"status": "uncertified_source_disabled", "usable": 0,
                "required": min_rows, "rejections": {"source_not_immutable": 1},
                "approved": False}
    rows, rejected = load_causal_feature_examples(settings, as_of_ms, tf=tf)
    # The extractor intentionally bounds pagination at 100k raw paper rows.
    # An incomplete rolling slice is NOT a monotonic eligible population:
    # training from it could both drop old observations and silently stop
    # retraining forever when new rows replace old rows at the bound.
    if rejected.get("scan_limit_reached"):
        return {"status": "incomplete_verified_population_scan",
                "usable": len(rows),
                "required": max(min_rows, previous_count + min_new),
                "rejections": rejected, "approved": False}
    if len(rows) < min_rows or not should_retrain(previous_count, len(rows), min_new):
        return {"status": "insufficient_new_verified_data", "usable": len(rows),
                "required": max(min_rows, previous_count + min_new),
                "rejections": rejected, "approved": False}
    start = rows[int(len(rows) * .6)].decision_ms
    end = rows[int(len(rows) * .8)].decision_ms
    try:
        train, validation, shadow = chronological(rows, start, end, embargo_ms)
    except ValueError:
        return {"status": "insufficient_purged_splits", "usable": len(rows),
                "required": min_rows, "rejections": rejected, "approved": False}
    if len(train) < 100 or len(validation) < 30 or len(shadow) < 30:
        return {"status": "insufficient_purged_splits", "usable": len(rows),
                "required": min_rows, "rejections": rejected, "approved": False}
    model = fit(train)
    candidate_id, metrics = store_shadow(settings, model, train, validation, shadow,
                                         eligible_count=len(rows))
    return {"status": "shadow_registered", "candidate_id": candidate_id,
            "version": model.version, "metrics": metrics, "usable": len(rows),
            "rejections": rejected, "approved": False}


def run_causal_shadow_cycle(settings, *, min_new=100, min_rows=180,
                            embargo_ms=60000):
    """Single bounded, restart-idempotent shadow cycle; no orders or promotions.

    The monotonic eligible-count watermark lives in our model_versions
    validation payload; no new shared migration or elevated DDL is needed.
    A distinct advisory lock serializes scheduler invocations while B's
    canonical registry lock continues protecting model rows.
    """
    from .ml.contract import milliseconds
    from .ml.training_cursor import prior_eligible_count

    if (type(min_new) is not int or min_new < 1 or
            type(min_rows) is not int or min_rows < 180 or
            type(embargo_ms) is not int or embargo_ms < 0):
        raise ValueError("invalid shadow retrain policy")
    with connection(settings) as conn:
        locked = conn.execute(
            "SELECT pg_try_advisory_xact_lock(20991993) AS acquired"
        ).fetchone()["acquired"]
        if not locked:
            return {"status": "already_running", "approved": False}
        stamp = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        as_of = milliseconds(stamp)
        row = conn.execute(
            """SELECT validation FROM model_versions
               WHERE model_name='causal_softmax3' AND approved=false
                 AND validation->>'source_contract'='eth_feature_snapshot_v2'
                 AND validation ? 'eligible_count'
               ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        previous_count = prior_eligible_count(row)
        # The advisory transaction lock spans training and registration.
        # store_shadow uses a *different* B registry lock, no self-deadlock.
        try:
            result = train_causal_shadow(
                settings, as_of, tf="1", source="snapshots",
                min_rows=min_rows, min_new=min_new,
                previous_count=previous_count, embargo_ms=embargo_ms)
        except (ValueError, TypeError, OverflowError) as exc:
            return {"status": "invalid_verified_training_evidence",
                    "reason": str(exc)[:160], "approved": False,
                    "previous_eligible_count": previous_count,
                    "as_of_ms": as_of}
        return {**result, "previous_eligible_count": previous_count,
                "as_of_ms": as_of, "approved": False}
