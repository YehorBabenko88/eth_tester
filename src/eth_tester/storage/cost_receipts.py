"""Append-only *provisional* paper-cost observation receipts.

No original official funding schedule/receipt clock exists in the older funding
schema. In particular no matching funding row never means verified zero. This
module NEVER settles paper capital, verifies funding or grants ML eligibility.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json

_MAX_EVENTS = 1000


def _amount(value):
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError('Invalid cost value') from exc
    if not result.is_finite():
        raise ValueError('Nonfinite cost value')
    return result


def _instant(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('Cost provenance requires timezone-aware DB clock')
    return value.astimezone(timezone.utc).isoformat()


def record_provisional_cost_observation(conn, paper_trade_id):
    """Insert immutable one-trade *observation*, never an economic certificate.

    Never takes E's global position lock (read-only post-close transaction).
    The actual source has no official funding schedule or f.received_at,
    therefore even a fully marked interval is ALWAYS an unbounded unknown.
    """
    if type(paper_trade_id) is not int or paper_trade_id < 1:
        raise ValueError('Positive paper trade ID required')
    conn.execute("SELECT set_config('lock_timeout',%s,true)", ('2000ms',))
    conn.execute("SELECT set_config('statement_timeout',%s,true)", ('5000ms',))
    trade = conn.execute('''SELECT id,symbol,state,direction,opened_at,closed_at,
        qty,notional,funding_paid,fee_paid,net_pnl,funding_verified
        FROM paper_trades WHERE id=%s FOR SHARE''', (paper_trade_id,)).fetchone()
    if not trade or trade['state'] != 'closed' or trade['closed_at'] is None:
        raise ValueError('Only a persisted closed paper trade may be observed')
    opened = trade['opened_at']
    closed = trade['closed_at']
    if not opened or not opened.tzinfo or not closed.tzinfo or closed < opened:
        raise ValueError('Invalid persisted paper cost window')
    if trade['direction'] not in ('long', 'short'):
        raise ValueError('Invalid persisted direction')
    qty = _amount(trade['qty'])
    notional = _amount(trade['notional'])
    if qty <= 0 or notional <= 0:
        raise ValueError('Invalid persisted paper quantity')
    if trade['funding_verified'] is True:
        raise RuntimeError('Verified funding requires separate authoritative audit')
    rows = conn.execute('''SELECT f.ts, f.rate,
        m.id AS mark_event_id, m.mark_price, m.received_ts AS mark_received_at
        FROM funding AS f LEFT JOIN LATERAL (
            SELECT id,mark_price,received_ts FROM market_ticker_events
            WHERE symbol=%s AND exchange_ts<=f.ts
              AND exchange_ts>=f.ts-interval '1 second'
              AND received_ts<=%s AND mark_price>0
            ORDER BY exchange_ts DESC,received_ts DESC,id DESC LIMIT 1
        ) AS m ON true
        WHERE f.symbol=%s AND f.ts>%s AND f.ts<=%s
        ORDER BY f.ts LIMIT 1001''',
        (trade['symbol'], closed, trade['symbol'], opened, closed)).fetchall()
    if len(rows) > _MAX_EVENTS:
        raise RuntimeError('Funding event observation is unbounded')
    samples = []
    missing = 0
    modeled = Decimal(0)
    sign = Decimal(1) if trade['direction'] == 'long' else Decimal(-1)
    for row in rows:
        at, rate = _instant(row['ts']), _amount(row['rate'])
        mark = row['mark_price']
        if mark is None or row['mark_event_id'] is None:
            missing += 1
            samples.append([at, str(rate), None, None, None])
            continue
        mark = _amount(mark)
        if mark <= 0 or row['mark_received_at'] is None or row['mark_received_at'] > closed:
            raise ValueError('Invalid or future-observed mark')
        modeled += sign * qty * mark * rate
        samples.append([at, str(rate), str(mark), int(row['mark_event_id']),
                        _instant(row['mark_received_at'])])
    recorded = _amount(trade['funding_paid'])
    fee = _amount(trade['fee_paid'])
    net = _amount(trade['net_pnl'])
    candidate = modeled if rows and missing == 0 else None
    cost_consistent = candidate is not None and candidate == recorded
    statement = {
        'contract': 'paper_cost_observation_v1',
        'paper_trade_id': paper_trade_id, 'symbol': trade['symbol'],
        'opened_at': _instant(opened), 'closed_at': _instant(closed),
        'funding_rows': samples, 'recorded_funding': str(recorded),
        'recorded_fee': str(fee), 'recorded_net': str(net),
        'candidate_funding': None if candidate is None else str(candidate),
        'funding_matches_observed_rows': cost_consistent,
        'official_schedule_verified': False, 'missing_event_periods_verified_zero': False,
        'funding_verified': False, 'economics_verified': False,
        'source_identity_authoritative': False,
        'ml_eligible': False, 'paper_only': True,
        'uncertainty_reserve_upper_bound_known': False,
        'reserve_usd': None, 'reserve_status': 'unbounded_no_certified_capital_release',
        'source_provenance': 'persisted_funding_rate_without_independent_received_at',
    }
    serialized = json.dumps(statement, sort_keys=True, separators=(',', ':'))
    digest = sha256(serialized.encode('utf-8')).hexdigest()
    conn.execute('''INSERT INTO paper_cost_observation_receipts(
        receipt_key,paper_trade_id,symbol,opened_at,closed_at,
        funding_events_observed,marks_missing,recorded_funding,
        candidate_funding,recorded_fee,recorded_net,observation_sha256,evidence)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT(receipt_key) DO NOTHING''',
        (digest, paper_trade_id, trade['symbol'], opened, closed,
         len(rows), missing, recorded, candidate, fee, net, digest, serialized))
    saved = conn.execute('''SELECT receipt_key,evidence FROM
        paper_cost_observation_receipts WHERE receipt_key=%s''', (digest,)).fetchone()
    if saved is None or saved['evidence'] != statement:
        raise RuntimeError('Conflicting immutable provisional cost receipt')
    return {'receipt_key': digest, 'funding_events_observed': len(rows),
            'marks_missing': missing, 'funding_verified': False,
            'costs_certified': False, 'ml_eligible': False,
            'reserve_status': 'unbounded_no_certified_capital_release',
            'zero_event_claim_certified': False}
