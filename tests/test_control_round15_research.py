"""G Round15 read-only evidence telemetry. These are isolated fake-DB tests.

No simulated result here certifies source completeness, finance, ML or PostgreSQL.
"""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from eth_tester import telegram_views as view


class FakeConnection:
    def __init__(self, result):
        self.result = result
        self.queries = []

    def execute(self, query, params=()):
        self.queries.append((str(query), params))
        return SimpleNamespace(fetchone=lambda: self.result)


def setup(monkeypatch, row):
    conn = FakeConnection(row)

    @contextmanager
    def fake_connection(_):
        yield conn

    monkeypatch.setattr(view, 'connection', fake_connection)
    return conn


def row(observed=0, no_trade=0, candidates=0, unavailable=0):
    return {'observed': observed, 'no_trade': no_trade,
            'candidates': candidates, 'unavailable': unavailable,
            'first_minute': None, 'last_minute': None}


def test_empty_day_is_not_no_signal(monkeypatch):
    db = setup(monkeypatch, row())
    output = view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    assert '1440' in output and 'пропущено: 1440' in output
    assert 'No-trade (только модель): 0' in output
    assert 'НЕ являются no_signal' in output
    assert 'НЕ OOS' in output and 'НЕ сертификат ML' in output
    assert len(db.queries) == 1
    assert db.queries[0][1] == ('ETHUSDT',)
    assert db.queries[0][0].strip().lower().startswith('with boundary as')


def test_all_candidate_minutes_still_not_source_certified(monkeypatch):
    setup(monkeypatch, row(1440, 0, 1440, 0))
    output = view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    assert 'пропущено: 0' in output
    assert 'кандидатов (НЕ ордера): 1440' in output
    assert 'нет допуска к обучению' in output


def test_no_trade_and_missing_are_disjoint(monkeypatch):
    setup(monkeypatch, row(100, 80, 10, 5))
    output = view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    assert 'пропущено: 1340' in output
    assert 'No-trade (только модель): 80' in output
    assert 'источник недоступен: 5' in output
    assert 'неполная/некорректная запись: 5' in output


def test_unavailable_cannot_be_relabelled_as_no_signal(monkeypatch):
    setup(monkeypatch, row(1440, 0, 0, 1440))
    output = view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    assert 'No-trade (только модель): 0' in output
    assert 'источник недоступен: 1440' in output


def test_invalid_envelopes_are_explicitly_uncertified(monkeypatch):
    setup(monkeypatch, row(3, 1, 0, 0))
    text = view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    assert 'неполная/некорректная запись: 2' in text
    assert 'Пропуски НЕ являются no_signal' in text


@pytest.mark.parametrize('bad', [
    row(1441,0,0,0), row(-1,0,0,0), row(2,3,0,0),
    row(1,0,1,1), row(0,0,0,1),
])
def test_impossible_counts_fail_closed(monkeypatch, bad):
    setup(monkeypatch,bad)
    with pytest.raises(RuntimeError, match='impossible counts'):
        view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')


def test_query_scopes_only_shadow_symbol_and_closed_past_minute(monkeypatch):
    db = setup(monkeypatch, row(2, 1, 1, 0))
    view.read_view(SimpleNamespace(symbol='ETHUSDT'), 'research')
    sql = db.queries[0][0]
    assert "version='eth_observed_minute_shadow_v1'" in sql
    assert "s.symbol=%s AND s.tf='1'" in sql
    assert "s.bar_ts<b.current_minute" in sql
    assert "interval '24 hours'" in sql
    assert "reason->>'ml_eligible' = 'false'" in sql
    assert "reason->>'research_cohort_complete' = 'false'" in sql
    assert "reason->>'prospective_source_certified' = 'false'" in sql
    assert "reason->>'source_identity_authoritative' = 'false'" in sql
    assert "reason#>>'{shadow,status}'='no_trade'" in sql
    assert "reason#>>'{shadow,reason}' IN" in sql
    assert 'SELECT count(*)' in sql
    assert not any(word in sql.lower() for word in ('insert into', 'delete from', 'update paper', 'drop table'))


def test_prospective_research_does_not_relabel_as_certified(monkeypatch):
    setup(monkeypatch,row(10,6,2,1))
    text=view.read_view(SimpleNamespace(symbol='ETHUSDT'),'research').lower()
    assert 'не сертификат ml' in text
    assert 'без независимых' in text
    assert 'нет допуска к обучению' in text
    assert 'исполнены' not in text


def test_preserves_empty_legacy_model_training_visibility(monkeypatch):
    setup(monkeypatch,row(0,0,0,0))
    text=view.read_view(SimpleNamespace(symbol='ETHUSDT'),'research')
    assert 'Нет исследований' not in text
    assert '1440' in text
