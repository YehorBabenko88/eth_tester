"""Read-only ML extraction from completed paper/backtest trades.

No historical FEATURES row is silently treated as an immutable point-in-time
snapshot: recorded proof must be persisted with the original trade reason.
"""
from datetime import datetime, timezone
import json
import math
import re

from . import CLASSES, Example, FEATURE_NAMES, validate
from ..database import connection


def epoch_ms(timestamp):
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
        raise ValueError("timezone-aware timestamp required")
    return int(timestamp.astimezone(timezone.utc).timestamp() * 1000)


def from_trade_records(records, as_of_ms):
    """Require a separately captured, complete event-time feature snapshot.

    Expect reason.ml_snapshot = {values:{feature:value}, available_ms:{feature:ms},
    recorded_ms:int, quality:'verified', version:string}. The ML label must be
    ml_outcome, *not* whether the trade happened to make money.
    """
    examples = []
    rejected = {}
    for record in records:
        try:
            reason = record["reason"]
            if isinstance(reason, str):
                reason = json.loads(reason)
            snapshot = reason["ml_snapshot"]
            decision = int(snapshot["recorded_ms"])
            opened = epoch_ms(record["opened_at"])
            closed = epoch_ms(record["closed_at"])
            values = tuple(float(snapshot["values"][name]) for name in FEATURE_NAMES)
            available = tuple(int(snapshot["available_ms"][name]) for name in FEATURE_NAMES)
            if snapshot.get("quality") != "verified" or not snapshot.get("version"):
                raise ValueError("unverified snapshot")
            if decision > opened or any(t > decision for t in available):
                raise ValueError("lookahead snapshot")
            if not 0 < float(record["notional"]) <= 1000:
                raise ValueError("notional exceeds paper contract")
            gross = float(record["gross_pnl"]) / float(record["notional"])
            costs = (float(record["fees"]) + float(record["funding"])) / float(record["notional"])
            observed_net = float(record["net_pnl"]) / float(record["notional"])
            if abs((gross - costs) - observed_net) > 1e-7:
                raise ValueError("net pnl inconsistency")
            outcome = reason["ml_outcome"]
            if outcome not in CLASSES or closed <= opened or decision >= closed:
                raise ValueError("invalid label or horizon")
            event_id = str(record["event_id"])
            if not event_id:
                raise ValueError("missing event id")
            examples.append(Example(event_id, decision, closed, values, available,
                                    outcome, gross, costs))
        except (KeyError, ValueError, TypeError, OverflowError, ZeroDivisionError) as exc:
            key = str(exc)
            rejected[key] = rejected.get(key, 0) + 1
    return validate(examples, as_of_ms), rejected


def load_backtest_examples(settings, as_of_ms, *, limit=100000):
    """Read-only extraction; no DDL, no changes to Storage-owned schema."""
    if not 1 <= limit <= 1000000:
        raise ValueError("invalid extraction limit")
    with connection(settings) as conn:
        records = conn.execute(
            """SELECT ('backtest:'||t.run_id||':'||t.seq) AS event_id,
               t.opened_at,t.closed_at,t.notional_placeholder,
               t.gross_pnl,t.fees,t.funding,t.net_pnl,t.reason
               FROM (
                 SELECT bt.*, bt.qty*bt.entry AS notional_placeholder
                 FROM backtest_trades bt
               ) t
               JOIN backtest_runs b ON b.id=t.run_id
               WHERE b.symbol=%s AND t.closed_at<=to_timestamp(%s / 1000.0)
               ORDER BY t.opened_at,t.run_id,t.seq LIMIT %s""",
            (settings.symbol, as_of_ms, limit)).fetchall()
    return from_trade_records(
        [{**r, "notional": r["notional_placeholder"]} for r in records], as_of_ms)


def load_paper_examples(settings, as_of_ms, *, limit=100000):
    if not 1 <= limit <= 1000000:
        raise ValueError("invalid extraction limit")
    with connection(settings) as conn:
        rows = conn.execute(
            """SELECT ('paper:'||id) AS event_id,opened_at,closed_at,notional,
               gross_pnl,fee_paid AS fees,funding_paid AS funding,net_pnl,reason
               FROM paper_trades WHERE symbol=%s AND state='closed'
               AND closed_at<=to_timestamp(%s / 1000.0)
               ORDER BY opened_at,id LIMIT %s""",
            (settings.symbol, as_of_ms, limit)).fetchall()
    return from_trade_records(rows, as_of_ms)


def from_snapshot_records(records, as_of_ms):
    """Accept only the exact immutable snapshot captured with the original signal.

    Feature observation, computation and source receipt must predate the decision.
    An independently complete horizon label must be available by as_of_ms.
    A row with missing evidence is rejected, never reconstructed from hindsight.
    """
    if not isinstance(as_of_ms, int) or as_of_ms <= 0:
        raise ValueError("invalid evaluation cutoff")
    result, rejected = [], {}
    for record in records:
        try:
            reason = record["reason"]
            if isinstance(reason, str):
                reason = json.loads(reason)
            if not isinstance(reason, dict):
                raise ValueError("missing signal provenance")
            snapshot_key = record["snapshot_key"]
            source = record["source_checksum"]
            if not isinstance(snapshot_key, str) or not snapshot_key:
                raise ValueError("invalid snapshot key")
            if reason.get("ml_snapshot_key") != snapshot_key:
                raise ValueError("snapshot not bound to original signal")
            if (not isinstance(source, str) or not re.fullmatch(r"[0-9a-f]{64}", source)
                    or reason.get("ml_source_checksum") != source):
                raise ValueError("mismatched or invalid source checksum")
            if record["snapshot_quality"] != "complete" or record["is_causal"] is not True:
                raise ValueError("uncertified source")
            decision = epoch_ms(record["decision_at"])
            opening = epoch_ms(record["opened_at"])
            closing = epoch_ms(record["closed_at"])
            available = epoch_ms(record["available_at"])
            observed = epoch_ms(record["observed_at"])
            computed = epoch_ms(record["computed_at"])
            bar = epoch_ms(record["bar_ts"])
            if not (0 < bar <= available <= observed <= computed <= decision
                    <= opening < closing):
                raise ValueError("hindsight or backdated snapshot")
            provenance = record["provenance"]
            if isinstance(provenance, str):
                provenance = json.loads(provenance)
            if not isinstance(provenance, dict):
                raise ValueError("missing producer provenance")
            if provenance.get("source_coverage_verified") is not True:
                raise ValueError("unverified source coverage")
            receipt = provenance.get("source_receipt_ms")
            if not isinstance(receipt, int) or not available <= receipt <= observed:
                raise ValueError("invalid source receipt")
            available_by_feature = provenance["feature_available_ms"]
            if not isinstance(available_by_feature, dict):
                raise ValueError("missing per-feature times")
            feature_times = tuple(available_by_feature[name] for name in FEATURE_NAMES)
            if any(not isinstance(t, int) or t < bar or t > available
                   for t in feature_times):
                raise ValueError("future or invalid feature availability")
            values_json = record["values_json"]
            if isinstance(values_json, str):
                values_json = json.loads(values_json)
            if not isinstance(values_json, dict):
                raise ValueError("missing feature values")
            values = tuple(float(values_json[name]) for name in FEATURE_NAMES)
            if any(not math.isfinite(v) for v in values):
                raise ValueError("nonfinite features")
            label = reason["ml_label"]
            if not isinstance(label, dict):
                raise ValueError("missing independent structural label")
            outcome = label.get("outcome")
            horizon = label.get("horizon_end_ms")
            issued = label.get("issued_at_ms")
            if (label.get("method") != "fixed_horizon_barrier_v1"
                    or label.get("coverage_verified") is not True
                    or not isinstance(label.get("source_checksum"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", label["source_checksum"])
                    or outcome not in CLASSES
                    or not isinstance(horizon, int) or not isinstance(issued, int)
                    or not closing <= horizon <= issued <= as_of_ms):
                raise ValueError("unverified or unfinished structural label")
            if reason.get("ml_outcome", outcome) != outcome:
                raise ValueError("conflicting structural label")
            notional = float(record["notional"])
            gross_total = float(record["gross_pnl"])
            fees = float(record["fees"])
            funding = float(record["funding"])
            net_total = float(record["net_pnl"])
            if (not all(map(math.isfinite, (notional, gross_total, fees,
                                           funding, net_total)))
                    or not 0 < notional <= 1000):
                raise ValueError("invalid paper notional or PnL")
            gross = gross_total / notional
            costs = (fees + funding) / notional
            if abs(gross - costs - net_total / notional) > 1e-7:
                raise ValueError("pnl mismatch")
            event_id = record["event_id"]
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("invalid event id")
            result.append(Example(event_id, decision, horizon, values,
                                  feature_times, outcome, gross, costs))
        except (KeyError, ValueError, TypeError, OverflowError,
                ZeroDivisionError) as exc:
            error = str(exc)
            rejected[error] = rejected.get(error, 0) + 1
    return validate(result, as_of_ms), rejected


def load_causal_feature_examples(settings, as_of_ms, *, tf="5", limit=100000):
    """Use only the exact captured snapshot; no nearest historical fallback.

    Source receipts must be genuine. The B-owned schema does not independently
    prevent a privileged SQL writer from backdating computed_at, so an audited
    producer contract is mandatory before this data can be called certified.
    """
    if not isinstance(as_of_ms, int) or as_of_ms <= 0 or not 1 <= limit <= 1000000:
        raise ValueError("invalid extraction bound")
    with connection(settings) as conn:
        rows = conn.execute(
            """SELECT ('paper:' || p.id) AS event_id, p.reason,
               p.signal_ts AS decision_at, p.opened_at, p.closed_at,
               p.notional, p.gross_pnl, p.fee_paid AS fees,
               p.funding_paid AS funding, p.net_pnl,
               f.snapshot_key,f.bar_ts,f.available_at,f.observed_at,f.computed_at,
               f.source_checksum,f.quality AS snapshot_quality,f.is_causal,
               f.values_json,f.provenance
               FROM paper_trades p
               JOIN feature_snapshots f ON
                 f.snapshot_key=(p.reason->>'ml_snapshot_key')
                 AND f.source_checksum=(p.reason->>'ml_source_checksum')
                 AND f.symbol=p.symbol AND f.tf=%s
                 AND f.is_causal AND f.quality='complete'
                 AND f.available_at<=p.signal_ts
                 AND f.observed_at<=p.signal_ts
                 AND f.computed_at<=p.signal_ts
               WHERE p.symbol=%s AND p.state='closed' AND p.opened_at IS NOT NULL
                 AND p.closed_at<=to_timestamp(%s/1000.0)
               ORDER BY p.signal_ts,p.id LIMIT %s""",
            (tf, settings.symbol, as_of_ms, limit)).fetchall()
    return from_snapshot_records(rows, as_of_ms)
