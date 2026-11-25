"""Deterministic, fail-closed ETHUSDT paper execution; no exchange connectivity.

Inputs are *verified executable quotes* with explicit liquidity. Historical public
trades do not prove quote/depth liquidity and must never be passed off as fills.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import json
import os


D = Decimal


def number(value):
    try:
        result = D(str(value))
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise ValueError('invalid numeric market input') from exc
    if not result.is_finite():
        raise ValueError('non-finite market input')
    return result


def utc(ts):
    if not isinstance(ts, datetime) or ts.tzinfo is None:
        raise ValueError('UTC-aware timestamp required')
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class Quote:
    ts: datetime
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    last: Decimal
    event_id: str

    def __post_init__(self):
        utc(self.ts)
        for key in ('bid', 'ask', 'bid_qty', 'ask_qty', 'last'):
            object.__setattr__(self, key, number(getattr(self, key)))
        if self.bid <= 0 or self.ask < self.bid or self.last <= 0:
            raise ValueError('invalid quote')
        if min(self.bid_qty, self.ask_qty) < 0 or not self.event_id:
            raise ValueError('invalid quote liquidity or event identity')


@dataclass(frozen=True)
class Costs:
    maker: Decimal = D('0.0002')
    taker: Decimal = D('0.00055')
    slippage_bps: Decimal = D('2')

    def __post_init__(self):
        for key in ('maker', 'taker', 'slippage_bps'):
            value = number(getattr(self, key))
            if value < 0:
                raise ValueError('negative costs')
            object.__setattr__(self, key, value)


@dataclass
class Fill:
    event_id: str
    ts: datetime
    kind: str
    action: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    slippage: Decimal
    reason: str


@dataclass
class Position:
    side: str
    stop: Decimal
    take: Decimal
    reason: dict
    opened_at: datetime | None = None
    qty: Decimal = D(0)
    entry_qty: Decimal = D(0)
    cost_basis: Decimal = D(0)
    entry_fees: Decimal = D(0)
    realized: Decimal = D(0)
    fees: Decimal = D(0)
    funding: Decimal = D(0)
    mae: Decimal = D(0)
    mfe: Decimal = D(0)
    close_reason: str | None = None
    fills: list[Fill] = field(default_factory=list)

    @property
    def entry(self):
        return self.cost_basis / self.entry_qty if self.entry_qty else None


class PaperEngine:
    """Single-position local state machine with deterministic event idempotency."""

    def __init__(self, *, costs=None, notional=D('1000'), equity=D('10000'),
                 max_risk_fraction=D('0.01'), daily_loss_fraction=D('0.03'),
                 max_quote_gap_seconds=15):
        self.costs = costs or Costs()
        self.notional = number(notional)
        self.equity = number(equity)
        self.max_risk_fraction = number(max_risk_fraction)
        self.daily_loss_fraction = number(daily_loss_fraction)
        self.max_quote_gap_seconds = max_quote_gap_seconds
        if min(self.notional, self.equity) <= 0 or not 0 < self.max_risk_fraction <= 1:
            raise ValueError('invalid risk configuration')
        if not 0 < self.daily_loss_fraction <= 1 or max_quote_gap_seconds <= 0:
            raise ValueError('invalid loss/gap configuration')
        self.position = None
        self.order = None
        self.pending_exit = None
        self.history = []
        self.last_ts = None
        self.seen = set()
        self.blocked = False
        self.day = None
        self.day_start_equity = self.equity
        self.daily_pnl = D(0)
        self.rejections = []

    def submit(self, side, stop, take, *, order_type='market', limit=None,
               reason=None):
        if self.position is not None or self.order is not None:
            raise RuntimeError('one position/order only')
        if self.blocked or self.daily_pnl <= -self.day_start_equity * self.daily_loss_fraction:
            raise RuntimeError('risk or data gap lockout')
        stop, take = number(stop), number(take)
        if side not in ('long', 'short') or stop <= 0 or take <= 0:
            raise ValueError('invalid side or protection')
        if order_type not in ('market', 'limit') or (order_type == 'limit') != (limit is not None):
            raise ValueError('invalid order type')
        if not isinstance(reason or {}, dict):
            raise ValueError('signal reason must be mapping')
        if not (stop < take if side == 'long' else take < stop):
            raise ValueError('stop must be adverse and take favorable')
        if limit is not None:
            limit = number(limit)
            if limit <= 0:
                raise ValueError('invalid limit')
        self.order = dict(side=side, stop=stop, take=take, type=order_type,
                          limit=limit, reason=reason or {}, remaining_notional=self.notional)

    def cancel_entry(self):
        self.order = None

    def reset_gap(self):
        """Caller must explicitly verify uninterrupted source before re-enabling fills."""
        self.blocked = False
        self.last_ts = None

    def quote(self, quote: Quote):
        if quote.event_id in self.seen:
            return []
        ts = utc(quote.ts)
        if self.last_ts is not None:
            if ts < self.last_ts:
                raise ValueError('out-of-order event')
            if (ts - self.last_ts).total_seconds() > self.max_quote_gap_seconds:
                self.blocked = True
                self.rejections.append(('gap', quote.event_id))
        self.last_ts = ts
        self.seen.add(quote.event_id)
        if self.day != ts.date():
            self.day = ts.date()
            self.day_start_equity = self.equity
            self.daily_pnl = D(0)
        if self.blocked:
            return []
        fills = []
        if self.position and self.position.qty:
            self._excursion(quote.last)
            side = self.position.side
            if self.pending_exit is None:
                if (quote.last <= self.position.stop if side == 'long'
                        else quote.last >= self.position.stop):
                    self.pending_exit = 'stop'
                elif (quote.last >= self.position.take if side == 'long'
                      else quote.last <= self.position.take):
                    self.pending_exit = 'take'
            if self.pending_exit:
                fill = self._execute(quote, 'exit', self.position.qty,
                                     'market', None, self.pending_exit)
                if fill:
                    fills.append(fill)
                    # Never reopen with leftover entry order on stop/take.
                    self.order = None
                    if self.position is None:
                        self.pending_exit = None
                return fills
        if self.order and self.pending_exit is None:
            order = self.order
            price = quote.ask if order['side'] == 'long' else quote.bid
            if not (order['stop'] < price < order['take'] if order['side'] == 'long'
                    else order['take'] < price < order['stop']):
                self.rejections.append(('invalid_entry_price', quote.event_id))
                return fills
            max_qty = order['remaining_notional'] / price
            worst_entry = price * (D(1) + self.costs.slippage_bps / D(10000)) if order['side'] == 'long' else price * (D(1) - self.costs.slippage_bps / D(10000))
            if worst_entry <= 0:
                raise ValueError('invalid slippage execution')
            # Existing partial fills keep their own cost basis; reserve fee
            # exposure on both existing and as-yet-unfilled entry units.
            extra_risk = max_qty * abs(worst_entry - order['stop'])
            extra_fee = max_qty * (worst_entry + order['stop']) * self.costs.taker
            existing_risk = (self.position.qty * abs(self.position.entry - order['stop'])
                             if self.position is not None else D(0))
            if existing_risk + extra_risk + extra_fee > self.equity * self.max_risk_fraction:
                self.rejections.append(('risk_cap', quote.event_id))
                self.order = None
                return fills
            fill = self._execute(quote, 'entry', max_qty, order['type'],
                                 order['limit'], 'signal')
            if fill:
                fills.append(fill)
                order['remaining_notional'] -= fill.quantity * fill.price
                if order['remaining_notional'] <= D('0.00000001'):
                    self.order = None
        return fills

    def _execute(self, quote, action, requested, order_type, limit, reason):
        side = self.order['side'] if action == 'entry' else self.position.side
        buy = (side == 'long') == (action == 'entry')
        best = quote.ask if buy else quote.bid
        liquidity = quote.ask_qty if buy else quote.bid_qty
        if order_type == 'limit':
            if not ((best < limit) if buy else (best > limit)):
                return None  # touch is not proof of queue priority
            execution = min(best, limit) if buy else max(best, limit)
            fee_rate = self.costs.maker
            slippage = D(0)
        else:
            execution = best * (1 + (D(1) if buy else D(-1))
                                * self.costs.slippage_bps / D(10000))
            fee_rate = self.costs.taker
            slippage = abs(execution - best)
        if execution <= 0:
            raise ValueError('invalid execution price')
        qty = min(requested, liquidity)
        if action == 'entry':
            qty = min(qty, self.order['remaining_notional'] / execution)
        if qty <= 0:
            self.rejections.append(('no_liquidity', quote.event_id))
            return None
        fee = qty * execution * fee_rate
        fill = Fill(quote.event_id, utc(quote.ts), order_type, action, qty,
                    execution, fee, qty * slippage, reason)
        if action == 'entry':
            if self.position is None:
                self.position = Position(side, self.order['stop'], self.order['take'],
                                         dict(self.order['reason']), opened_at=utc(quote.ts))
            pos = self.position
            pos.cost_basis += execution * qty
            pos.qty += qty
            pos.entry_qty += qty
            pos.entry_fees += fee
            pos.fees += fee
            self.equity -= fee
            self.daily_pnl -= fee
        else:
            pos = self.position
            sign = D(1) if pos.side == 'long' else D(-1)
            pnl = sign * qty * (execution - pos.entry)
            pos.realized += pnl
            pos.qty -= qty
            pos.fees += fee
            change = pnl - fee
            self.equity += change
            self.daily_pnl += change
            if pos.qty == 0:
                pos.close_reason = reason
                self.history.append(pos)
                self.position = None
        pos.fills.append(fill)
        return fill

    def _excursion(self, last):
        pos = self.position
        sign = D(1) if pos.side == 'long' else D(-1)
        excursion = sign * (last - pos.entry) * pos.qty
        pos.mae = min(pos.mae, excursion)
        pos.mfe = max(pos.mfe, excursion)

    def funding(self, *, event_id, ts, rate, mark_price):
        """Apply actual historical funding event, never infer missing funding."""
        ts = utc(ts)
        if event_id in self.seen:
            return D(0)
        if self.blocked or self.last_ts is None or ts < self.last_ts:
            raise RuntimeError('unverified funding sequence')
        price, rate = number(mark_price), number(rate)
        if price <= 0:
            raise ValueError('invalid mark')
        self.seen.add(event_id)
        if not self.position or not self.position.qty:
            return D(0)
        payment = (D(1) if self.position.side == 'long' else D(-1)) * self.position.qty * price * rate
        self.position.funding += payment
        self.equity -= payment
        self.daily_pnl -= payment
        return payment

    def snapshot(self):
        """JSON-safe snapshot; event IDs and pending exit survive process restart."""
        def encode(obj):
            if isinstance(obj, Decimal):
                return str(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, set):
                return sorted(obj)
            if hasattr(obj, '__dataclass_fields__'):
                return asdict(obj)
            raise TypeError(type(obj).__name__)
        state = {k: getattr(self, k) for k in ('notional','equity','max_risk_fraction',
                 'daily_loss_fraction','max_quote_gap_seconds','position','order','pending_exit',
                 'history','last_ts','seen','blocked','day','day_start_equity','daily_pnl','rejections')}
        state['costs'] = self.costs
        state['version'] = 1
        state['day'] = self.day.isoformat() if self.day else None
        return json.loads(json.dumps(state, default=encode))

    def save(self, path):
        """Atomically replace checkpoint; single-writer lock required by hosting service."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + '.tmp')
        with temp.open('w', encoding='utf-8') as handle:
            json.dump(self.snapshot(), handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)

    @classmethod
    def restore(cls, path):
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
        if raw.get('version') != 1:
            raise ValueError('unknown snapshot version')
        engine = cls(costs=Costs(**raw['costs']), notional=raw['notional'],
                     equity=raw['equity'], max_risk_fraction=raw['max_risk_fraction'],
                     daily_loss_fraction=raw['daily_loss_fraction'],
                     max_quote_gap_seconds=raw['max_quote_gap_seconds'])
        engine.order = raw['order']
        if engine.order:
            for key in ('stop','take','limit','remaining_notional'):
                if engine.order[key] is not None:
                    engine.order[key] = number(engine.order[key])
        def position(data):
            if not data:
                return None
            data = dict(data)
            data['opened_at'] = datetime.fromisoformat(data['opened_at']) if data['opened_at'] else None
            for key in ('stop','take','qty','cost_basis','entry_fees','realized','fees',
                        'funding','mae','mfe','entry_qty'):
                data[key] = number(data[key])
            data['fills'] = [Fill(event_id=item['event_id'],
                                   ts=datetime.fromisoformat(item['ts']), kind=item['kind'],
                                   action=item['action'], quantity=number(item['quantity']),
                                   price=number(item['price']), fee=number(item['fee']),
                                   slippage=number(item['slippage']), reason=item['reason'])
                             for item in data['fills']]
            return Position(**data)
        engine.position = position(raw['position'])
        engine.history = [position(p) for p in raw['history']]
        engine.pending_exit = raw['pending_exit']
        engine.last_ts = datetime.fromisoformat(raw['last_ts']) if raw['last_ts'] else None
        engine.seen = set(raw['seen'])
        engine.blocked = raw['blocked']
        engine.day = datetime.fromisoformat(raw['day']).date() if raw['day'] else None
        engine.day_start_equity = number(raw['day_start_equity'])
        engine.daily_pnl = number(raw['daily_pnl'])
        engine.rejections = [tuple(item) for item in raw['rejections']]
        return engine
