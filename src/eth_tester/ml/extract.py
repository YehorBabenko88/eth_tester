"""Read-only ML extraction from completed paper/backtest trades.

No historical FEATURES row is silently treated as an immutable point-in-time
snapshot: recorded proof must be persisted with the original trade reason.
"""
from datetime import datetime, timezone
import json

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
    """Decode immutable B-owned feature_snapshots joined as-of decision time."""
    result = []
    rejected = {}
    for record in records:
        try:
            reason = record["reason"]
            if isinstance(reason, str):
                reason = json.loads(reason)
            source = record["source_checksum"]
            if not isinstance(source, str) or not source.strip():
                raise ValueError("missing source checksum")
            if record["snapshot_quality"] != "complete" or not record["is_causal"]:
                raise ValueError("uncertified source")
            decision = epoch_ms(record["decision_at"])
            opening = epoch_ms(record["opened_at"])
            closing = epoch_ms(record["closed_at"])
            available = epoch_ms(record["available_at"])
            observed = epoch_ms(record["observed_at"])
            computed = epoch_ms(record["computed_at"])
            bar = epoch_ms(record["bar_ts"])
            if not (bar <= available <= decision <= opening < closing
                    and observed <= decision and computed <= decision):
                raise ValueError("hindsight snapshot")
            provenance = record["provenance"]
            if isinstance(provenance, str):
                provenance = json.loads(provenance)
            available_by_feature = provenance["feature_available_ms"]
            feature_times = tuple(int(available_by_feature[name]) for name in FEATURE_NAMES)
            if any(t > available or t > decision for t in feature_times):
                raise ValueError("feature availability violates snapshot")
            values_json = record["values_json"]
            if isinstance(values_json, str):
                values_json = json.loads(values_json)
            values = tuple(float(values_json[name]) for name in FEATURE_NAMES)
            notional = float(record["notional"])
            if not 0 < notional <= 1000:
                raise ValueError("invalid paper notional")
            gross = float(record["gross_pnl"])/notional
            costs = (float(record["fees"])+float(record["funding"]))/notional
            if abs(gross-costs-float(record["net_pnl"])/notional) > 1e-7:
                raise ValueError("pnl mismatch")
            label = reason["ml_outcome"]
            if label not in CLASSES:
                raise ValueError("unverified structural label")
            result.append(Example(str(record["event_id"]), decision, closing, values,
                                  feature_times, label, gross, costs))
        except (KeyError, ValueError, TypeError, OverflowError, ZeroDivisionError) as exc:
            error = str(exc)
            rejected[error] = rejected.get(error, 0)+1
    return validate(result, as_of_ms), rejected


def load_causal_feature_examples(settings, as_of_ms, *, tf="5", limit=100000):
    """Read B's append-only snapshots and closed paper trades, no mutable features.

    The AS-OF predicate requires each of observed/computed/available <= decision.
    This intentionally excludes post-hoc historical backfills from paper inference.
    """
    if not 1 <= limit <= 1000000:
        raise ValueError("invalid extraction limit")
    with connection(settings) as conn:
        rows = conn.execute(
            """SELECT ('paper:' || p.id) AS event_id, p.reason,
               p.signal_ts AS decision_at, p.opened_at, p.closed_at,
               p.notional, p.gross_pnl, p.fee_paid AS fees,
               p.funding_paid AS funding, p.net_pnl,
               f.bar_ts,f.available_at,f.observed_at,f.computed_at,
               f.source_checksum,f.quality AS snapshot_quality,f.is_causal,
               f.values_json,f.provenance
               FROM paper_trades p
               JOIN LATERAL (
                 SELECT * FROM feature_snapshots s
                 WHERE s.symbol=p.symbol AND s.tf=%s
                   AND s.is_causal AND s.quality='complete'
                   AND s.source_checksum IS NOT NULL
                   AND s.available_at<=p.signal_ts
                   AND s.observed_at<=p.signal_ts
                   AND s.computed_at<=p.signal_ts
                 ORDER BY s.available_at DESC,s.computed_at DESC,s.snapshot_key DESC
                 LIMIT 1
               ) f ON true
               WHERE p.symbol=%s AND p.state='closed' AND p.opened_at IS NOT NULL
                 AND p.closed_at<=to_timestamp(%s/1000.0)
               ORDER BY p.signal_ts,p.id LIMIT %s""",
            (tf, settings.symbol, as_of_ms, limit)).fetchall()
    return from_snapshot_records(rows, as_of_ms)
