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
    candidate_id, metrics = store_shadow(settings, model, train, validation, shadow)
    return {"status": "shadow_registered", "candidate_id": candidate_id,
            "version": model.version, "metrics": metrics, "usable": len(rows),
            "rejections": rejected, "approved": False}
