from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from eth_tester.simulation import Costs, PaperEngine, Quote


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def q(seconds, bid='99.9', ask='100.1', bid_qty='100', ask_qty='100', last='100', uid=None):
    return Quote(BASE + timedelta(seconds=seconds), D(bid), D(ask), D(bid_qty),
                 D(ask_qty), D(last), uid or str(seconds))


def test_single_position_partial_liquidity_and_replay():
    e = PaperEngine(costs=Costs(slippage_bps=D('0')))
    e.submit('long', D(98), D(104))
    first = e.quote(q(0, ask_qty='2'))
    assert len(first) == 1 and first[0].quantity == 2
    assert e.position.qty == 2
    assert e.quote(q(0, ask_qty='2')) == []
    with pytest.raises(RuntimeError):
        e.submit('short', D(102), D(96))
    e.quote(q(1, ask_qty='50'))
    assert e.position.qty > 2
    e.quote(q(2, bid='104.8', ask='105', last='105', bid_qty='1'))
    assert e.position is not None and e.pending_exit == 'take'
    e.quote(q(3, bid='104.7', ask='105', last='105', bid_qty='100'))
    assert e.position is None and len(e.history) == 1
    assert e.history[0].close_reason == 'take'


def test_stop_persists_even_after_price_recovers():
    e = PaperEngine()
    e.submit('short', 102, 96)
    e.quote(q(0))
    e.quote(q(1, bid='103', ask='103.2', last='103', ask_qty='0'))
    assert e.pending_exit == 'stop'
    e.quote(q(2, bid='99', ask='99.2', last='99'))
    assert e.position is None and e.history[0].close_reason == 'stop'


def test_gap_freezes_fills_until_explicit_reset():
    e = PaperEngine(max_quote_gap_seconds=3)
    e.submit('long', 98, 104)
    e.quote(q(0))
    assert e.quote(q(5, bid='110', ask='110.1', last='110')) == []
    assert e.blocked and e.position
    with pytest.raises(RuntimeError):
        e.submit('short', 110, 90)
    e.reset_gap()
    e.quote(q(6, bid='110', ask='110.1', last='110'))
    assert not e.position


def test_limit_requires_trade_through_and_uses_maker():
    e = PaperEngine(costs=Costs(maker=D('0.001')))
    e.submit('long', 95, 110, order_type='limit', limit=100)
    assert e.quote(q(0, ask='100')) == []
    fills = e.quote(q(1, ask='99.8', bid='99.7'))
    assert len(fills) == 1 and fills[0].kind == 'limit'
    assert fills[0].fee == fills[0].price * fills[0].quantity * D('0.001')


def test_funding_sign_and_idempotency():
    e = PaperEngine()
    e.submit('short', 105, 95)
    e.quote(q(0))
    amount = e.funding(event_id='f1', ts=BASE + timedelta(seconds=1),
                       rate=D('0.001'), mark_price=100)
    assert amount < 0
    assert e.funding(event_id='f1', ts=BASE + timedelta(seconds=1),
                     rate=D('0.001'), mark_price=100) == 0


def test_atomic_snapshot_restores_pending_exit(tmp_path):
    e = PaperEngine()
    e.submit('long', 98, 104)
    e.quote(q(0))
    e.quote(q(1, bid='104.9', ask='105', last='105', bid_qty='0'))
    path = tmp_path / 'state.json'
    e.save(path)
    restored = PaperEngine.restore(path)
    assert restored.position.qty == e.position.qty
    assert restored.pending_exit == 'take'
    assert restored.quote(q(1, bid='104.9', ask='105', last='105', bid_qty='0')) == []
    restored.quote(q(2, bid='104.5', ask='104.6', last='104'))
    assert restored.position is None


def test_invalid_quotes_and_no_depth_fail_closed():
    with pytest.raises(ValueError):
        q(0, bid='101', ask='100')
    with pytest.raises(ValueError):
        q(0, bid='NaN')
    e = PaperEngine()
    e.submit('long', 98, 104)
    assert e.quote(q(0, ask_qty='0')) == []
    assert e.position is None

def test_reject_inverted_stop_take_before_order_creation():
    e = PaperEngine()
    with pytest.raises(ValueError):
        e.submit('long', 110, 95)
    assert e.order is None
    with pytest.raises(ValueError):
        e.submit('short', 90, 110)
    assert e.order is None


def test_partial_entry_reserves_existing_and_remaining_risk():
    e = PaperEngine(equity=D(10000), max_risk_fraction=D('.01'))
    e.submit('long', 94, 120)
    e.quote(q(0, ask_qty='2'))
    assert e.position is not None and e.position.qty == 2
    # A price jump makes remaining-entry risk exceed the original 1% budget.
    e.quote(q(1, bid='106.9', ask='107', last='106', ask_qty='50'))
    assert e.position.qty == 2 and e.order is None
