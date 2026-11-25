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

def train_shadow(settings,minimum=100,epochs=20):
    with connection(settings) as conn:
        deals=conn.execute("""SELECT direction,entry,stop,take,reason,net_pnl
        FROM paper_trades WHERE state='closed' AND entry>0 ORDER BY opened_at,id""").fetchall()
    data=[]
    for row in deals:
        item=vector(row)
        if item is not None:
            scaled=(min(5,item[0]*100),min(5,item[1]*100),
                    min(5,item[2]*100),item[3])
            data.append((scaled,1 if row["net_pnl"]>0 else 0))
    if len(data)<minimum:
        raise RuntimeError(f"Not enough closed trades: {len(data)}/{minimum}")
    a,b=int(len(data)*.6),int(len(data)*.8)
    train,validation,holdout=data[:a],data[a:b],data[b:]
    weights=[0.0]*5
    for epoch in range(epochs):
        rate=.05/(1+epoch/10)
        for features,y in train:
            prediction=sigmoid(weights[0]+sum(w*x for w,x in zip(weights[1:],features)))
            error=prediction-y
            weights[0]-=rate*error
            for k in range(4):
                weights[k+1]-=rate*(error*features[k]+.01*weights[k+1])
    base=sum(y for _,y in train)/len(train)
    base_brier=sum((base-y)**2 for _,y in holdout)/len(holdout)
    stats={"train":len(train),"validation":len(validation),
      "holdout":len(holdout),"validation_brier":brier(weights,validation),
      "holdout_brier":brier(weights,holdout),"baseline_brier":base_brier,
      "selection_bias":"only realized paper entries","approved":False}
    with connection(settings) as conn:
        result=conn.execute("""INSERT INTO model_versions(model_name,params,validation,approved)
          VALUES('shadow_logistic_v1',%s::jsonb,%s::jsonb,false) RETURNING id""",
          (json.dumps({"features":FEATURES,"weights":weights}),
           json.dumps(stats))).fetchone()
    return result["id"],stats


def train_causal_shadow(settings, as_of_ms, *, source="snapshots",
                        min_rows=180, min_new=100, previous_count=0,
                        embargo_ms=0):
    """Retrain only when enough independently labeled, point-in-time data exist.

    Does not approve a model, enable the bot, or place trades. The two holdout
    periods are reserved for validation and independent shadow evaluation.
    """
    from .ml import chronological, fit, should_retrain
    from .ml.extract import (load_backtest_examples, load_paper_examples,
                             load_causal_feature_examples)
    from .ml.lifecycle import store_shadow

    if min_rows < 30 or embargo_ms < 0:
        raise ValueError("invalid training policy")
    if source == "snapshots":
        rows, rejected = load_causal_feature_examples(settings, as_of_ms)
    elif source == "backtest":
        rows, rejected = load_backtest_examples(settings, as_of_ms)
    elif source == "paper":
        rows, rejected = load_paper_examples(settings, as_of_ms)
    else:
        raise ValueError("source must be snapshots, backtest or paper")
    if len(rows) < min_rows or not should_retrain(previous_count, len(rows), min_new):
        return {"status": "insufficient_new_verified_data", "usable": len(rows),
                "rejections": rejected, "approved": False}
    start = rows[int(len(rows) * .6)].decision_ms
    end = rows[int(len(rows) * .8)].decision_ms
    train, validation, shadow = chronological(rows, start, end, embargo_ms)
    if len(train) < 100 or len(validation) < 30 or len(shadow) < 30:
        return {"status": "insufficient_purged_splits", "usable": len(rows),
                "rejections": rejected, "approved": False}
    model = fit(train)
    candidate_id, metrics = store_shadow(settings, model, train, validation, shadow)
    return {"status": "shadow_registered", "candidate_id": candidate_id,
            "version": model.version, "metrics": metrics, "usable": len(rows),
            "rejections": rejected, "approved": False}
