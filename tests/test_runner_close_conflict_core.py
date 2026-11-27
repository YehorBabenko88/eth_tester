"""ETH TESTER D R42: native-repo pytest for the runner close return contract.

Original parent HEAD: 613bcdcf81c7c2f60454e6722496c4891d50e43c.
Actual module import: eth_tester.runner. PostgreSQL and the close_paper
return boundary are mocked; this test does not certify a settlement.
The None-return assertion is expected RED on the unpatched parent and GREEN
only after an explicitly authorized runner change.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import ModuleType, SimpleNamespace
import sys

import pytest

from eth_tester import runner


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _ReadOnlyDb:
    """Only the SELECTs needed to reach the original closing return seam."""

    def __init__(self):
        now = datetime.now(timezone.utc)
        self.trade_id = 41
        self.opened_at = now - timedelta(minutes=1)
        self.crossing_at = now - timedelta(seconds=2)
        self.recent_at = now - timedelta(seconds=1)
        self.log = []

    def execute(self, sql, params=()):
        self.log.append(sql)
        if "FROM feed_health" in sql:
            return _Result({"status": "connected"})
        if "FROM trades" in sql and "ORDER BY ts DESC,trade_id DESC LIMIT 1" in sql:
            return _Result({"ts": self.recent_at, "price": Decimal("95")})
        if "FROM paper_trades WHERE state='open'" in sql:
            return _Result({"id": self.trade_id, "opened_at": self.opened_at,
                            "stop": Decimal("95"), "take": Decimal("110"),
                            "direction": "long", "qty": Decimal("1"),
                            "entry": Decimal("100")})
        if "FROM data_gaps" in sql:
            return _Result(None)
        if "min(price) FILTER" in sql and "FROM trades" in sql:
            return _Result({"ts": self.crossing_at,
                            "low_cross": Decimal("95"), "high_cross": None})
        raise AssertionError("Unexpected SQL in isolated runner seam: " + sql[:120])


def _setup_runner_seam(monkeypatch, close_result=None, close_error=None):
    db = _ReadOnlyDb()

    @contextmanager
    def connection(_settings):
        yield db

    monkeypatch.setattr(runner, "connection", connection)
    monkeypatch.setattr(
        runner, "resolve_first_crossing",
        lambda hit, direction, stop, take: {
            "ts": db.crossing_at, "price": Decimal("95"),
            "reason": "stop", "ambiguous_same_timestamp": False,
        },
    )
    monkeypatch.setattr(
        runner, "estimated_funding",
        lambda *args, **kwargs: (Decimal("0"), "unverified", []),
    )

    seen = []

    def close(settings, price, reason, funding, event_at, expected_id,
              *, funding_evidence, require_live_crossing):
        seen.append({"expected_id": expected_id,
                     "require_live_crossing": require_live_crossing,
                     "funding_evidence": funding_evidence})
        if close_error is not None:
            raise close_error
        return close_result

    monkeypatch.setattr(runner, "close_paper", close)
    # Ensure success-path render_trade is inert, not evidence of a real trade.
    charts = ModuleType("eth_tester.charts")
    charts.render_trade = lambda *_args: None
    monkeypatch.setitem(sys.modules, "eth_tester.charts", charts)
    return SimpleNamespace(symbol="ETHUSDT"), db, seen


def test_round37_closed_none_must_not_be_reported_as_success(monkeypatch):
    """EXPECTED RED on pinned HEAD: close_paper(None) is an allowed race result.

    Future safe outcome could be an explicit non-success status or an exception;
    never forge a closed position and never report closed_None.
    """
    settings, db, seen = _setup_runner_seam(monkeypatch, close_result=None)
    result = runner.engine_tick(settings)
    assert len(seen) == 1
    assert seen[0]["expected_id"] == db.trade_id
    assert seen[0]["require_live_crossing"] is True
    assert result == "paper_close_conflict", "missing position must be reported as non-success conflict"
    assert not any("INSERT INTO" in sql or "UPDATE " in sql for sql in db.log), "fake boundary must not mutate storage"
    assert not result.startswith("closed_"), "no real id was closed"


def test_round37_real_close_id_is_preserved(monkeypatch):
    """Positive control: return exact durable-looking identifier unchanged."""
    settings, db, seen = _setup_runner_seam(monkeypatch, close_result=41)
    assert runner.engine_tick(settings) == "closed_41"
    assert len(seen) == 1 and seen[0]["expected_id"] == db.trade_id
    assert seen[0]["require_live_crossing"] is True


def test_round37_close_error_cannot_be_relabelled_success(monkeypatch):
    """Negative control: source validation failure must propagate here."""
    settings, _, seen = _setup_runner_seam(
        monkeypatch, close_error=RuntimeError("paper_exit_source_gap")
    )
    with pytest.raises(RuntimeError, match="paper_exit_source_gap"):
        runner.engine_tick(settings)
    assert len(seen) == 1 and seen[0]["require_live_crossing"] is True
