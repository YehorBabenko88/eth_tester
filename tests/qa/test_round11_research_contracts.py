"""Cross-owner QA H contract for paper research and provisional funding."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace


class _Rows:
    def __init__(self, one=None, many=None):
        self.one, self.many = one, many or []

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


def test_review_drawdown_excludes_later_trade_and_ignores_forged_ml(monkeypatch):
    from eth_tester.simulation import review
    closing = datetime(2026, 11, 25, 12, tzinfo=timezone.utc)
    trade = {
        "id": 7, "symbol": "ETHUSDT", "strategy_version": "qa_shadow",
        "direction": "long", "opened_at": closing-timedelta(minutes=5),
        "closed_at": closing, "exit_reason": "qa_only",
        "entry": Decimal("3000"), "net_pnl": Decimal("1"),
        "gross_pnl": Decimal("2"), "fee_paid": Decimal("0.5"),
        "funding_paid": Decimal("0.5"), "entry_chart": None, "exit_chart": None,
        "reason": {"ml_eligible": True, "exchange_fill_proven": True},
    }
    stored = []

    class FakeConn:
        def execute(self, statement, args=None):
            q = str(statement)
            if q.startswith("SET LOCAL"):
                return _Rows()
            if "pg_try_advisory_xact_lock" in q:
                return _Rows(one={"acquired": True})
            if "SELECT p.* FROM paper_trades" in q:
                return _Rows(one=trade)
            if "SELECT 1 FROM data_gaps" in q:
                return _Rows()
            if "SELECT count(*) AS n,min(price)" in q:
                return _Rows(one={"n": 2, "low": Decimal("2990"),
                                  "high": Decimal("3010")})
            if "FROM paper_order_events" in q:
                return _Rows(many=[])
            if "FROM paper_account" in q:
                return _Rows(one={"initial_equity": Decimal("10000"),
                                  "equity": Decimal("10010")})
            if "SELECT net_pnl FROM paper_trades" in q:
                assert "closed_at" in q and "id" in q, q
                assert args is not None and closing in args and 7 in args, args
                return _Rows(many=[{"net_pnl": Decimal("1")}])
            if "INSERT INTO bot_events" in q:
                stored.append(json.loads(args[0]))
                return _Rows()
            raise AssertionError("Unexpected review SQL " + q[:150])

    @contextmanager
    def fake_db(_settings):
        yield FakeConn()

    monkeypatch.setattr(review, "connection", fake_db)
    assert review.review_one_closed_trade(
        SimpleNamespace(symbol="ETHUSDT")) == "review_recorded"
    assert len(stored) == 1
    assert stored[0]["trade_id"] == 7
    assert stored[0]["paper_only"] is True
    assert stored[0]["ml_eligible"] is False
    assert stored[0]["exchange_fill_proven"] is False
    assert stored[0]["trade_prints_exchange_complete"] is False


def test_missing_mark_funding_never_becomes_verified():
    from eth_tester.runner import estimated_funding

    class FakeConn:
        def execute(self, query, args):
            assert "market_ticker_events" in query
            return _Rows(many=[{
                "ts": datetime(2026, 11, 25, tzinfo=timezone.utc),
                "rate": Decimal("0.0001"), "mark_price": None,
            }])

    start = datetime(2026, 11, 24, tzinfo=timezone.utc)
    amount, quality, receipts = estimated_funding(
        FakeConn(), SimpleNamespace(symbol="ETHUSDT"), start,
        start + timedelta(days=2), Decimal("0.01"), Decimal("3000"),
        "long", with_events=True,
    )
    assert amount == Decimal("0")
    assert quality == "unverified"
    assert receipts == []
