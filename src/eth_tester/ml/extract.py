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
