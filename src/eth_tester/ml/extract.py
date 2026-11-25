"""Read-only ML extraction from completed paper/backtest trades.

No historical FEATURES row is silently treated as an immutable point-in-time
snapshot: recorded proof must be persisted with the original trade reason.
"""
from datetime import datetime, timezone
import json
import math

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
    """Bounded diagnostic decoder; missing LEFT JOIN records are counted, not hidden.

    A label must be independently appended after the full bar horizon.
    A source row never becomes point-in-time valid merely because it existed
    somewhere in history before this *query*.
    """
    from .contract import milliseconds, validate_label, validate_snapshot

    if type(as_of_ms) is not int or as_of_ms <= 0:
        raise ValueError("invalid as-of cutoff")
    accepted, rejected = [], {}
    for record in records:
        try:
            reason = record["reason"]
            if isinstance(reason, str):
                reason = json.loads(reason)
            values, times, decision = validate_snapshot(record, reason)
            closed = milliseconds(record["closed_at"])
            trade_id = record["paper_trade_id"]
            outcome, horizon = validate_label(
                record, reason, as_of_ms, closed, trade_id)
            nominal = float(record["notional"])
            gross = float(record["gross_pnl"])
            fee = float(record["fees"])
            funding = float(record["funding"])
            net = float(record["net_pnl"])
            if (not all(math.isfinite(x) for x in (nominal, gross, fee, funding, net))
                    or not 0 < nominal <= 1000
                    or abs(gross - fee - funding - net) > max(1e-7, nominal*1e-9)):
                raise ValueError("inconsistent paper cost/notional basis")
            if decision >= closed:
                raise ValueError("position closed before ML decision")
            accepted.append(Example(
                f"paper:{trade_id}", decision, horizon, values, times, outcome,
                gross / nominal, (fee + funding) / nominal))
        except (KeyError, TypeError, ValueError, OverflowError,
                ZeroDivisionError, AttributeError) as exc:
            name = f"{type(exc).__name__}: {str(exc)[:100]}"
            rejected[name] = rejected.get(name, 0) + 1
    return validate(accepted, as_of_ms), rejected


def load_causal_feature_examples(settings, as_of_ms, *, tf="1", limit=100000):
    """Read closed paper, exact immutable snapshot and post-horizon label.

    LEFT JOIN is intentional: missing/mismatched/late evidence still counts
    as rejected data instead of disappearing from an inner-join training set.
    """
    if (type(as_of_ms) is not int or as_of_ms <= 0
            or not isinstance(tf, str) or not tf
            or not isinstance(limit, int) or not 1 <= limit <= 1000000):
        raise ValueError("invalid causal extraction bound")
    with connection(settings) as conn:
        present = conn.execute(
            "SELECT to_regclass('public.paper_ml_labels') IS NOT NULL AS installed"
        ).fetchone()
        if not present["installed"]:
            return [], {"missing_post_horizon_label_ledger": 1}
        rows = conn.execute(
            """SELECT p.id AS paper_trade_id, p.reason,
                 p.signal_ts AS decision_at,p.opened_at,p.closed_at,
                 p.notional,p.gross_pnl,p.fee_paid AS fees,
                 p.funding_paid AS funding,p.net_pnl,
                 f.snapshot_key,f.bar_ts,f.available_at,f.observed_at,f.computed_at,
                 f.source_checksum,f.quality AS snapshot_quality,f.is_causal,
                 f.values_json,f.provenance,
                 to_jsonb(l) AS label_ledger
               FROM paper_trades p
               LEFT JOIN feature_snapshots f
                 ON f.snapshot_key=(p.reason->>'ml_snapshot_key')
                 AND f.symbol=p.symbol AND f.tf=%s
               LEFT JOIN paper_ml_labels l ON l.paper_trade_id=p.id
               WHERE p.symbol=%s AND p.state='closed'
                 AND p.opened_at IS NOT NULL
                 AND p.closed_at<=to_timestamp(%s / 1000.0)
               ORDER BY p.signal_ts,p.id LIMIT %s""",
            (tf, settings.symbol, as_of_ms, limit)).fetchall()
    return from_snapshot_records(rows, as_of_ms)
