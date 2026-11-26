"""C Round 22: real, disposable PostgreSQL 17/18 enrichment bounds.

Target repository path: tests/integration/test_analytics_c_round22_pg.py.
Tests never modify user databases or claim exchange completeness.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
import os

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester import analytics
from eth_tester.config import Settings
from eth_tester.database import connection, init_database

pytestmark = pytest.mark.postgres


@pytest.fixture
def c22_pg():
    admin = os.environ.get("ETH_TEST_PG_DSN")
    if not admin:
        pytest.skip("Disposable PostgreSQL 17/18 needs ETH_TEST_PG_DSN")
    name = "eth_tester_c22_" + uuid4().hex[:12]
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin, dbname=name))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def _seed_features(settings, count):
    start = (datetime.now(timezone.utc).replace(second=0, microsecond=0)
             - timedelta(minutes=count + 1))
    with connection(settings) as conn:
        for n in range(count):
            conn.execute("""INSERT INTO features(symbol,tf,ts,delta,volume,
                         quality,metadata) VALUES('ETHUSDT','1',%s,%s,1,
                         'partial_unverified','{"preexisting":true}'::jsonb)""",
                         (start + timedelta(minutes=n), Decimal(n + 1)))
    return [start + timedelta(minutes=n) for n in range(count)]


@pytest.mark.parametrize("bad", [0, -1, 25, True, False, 1.5, "2", None])
def test_enrichment_rejects_invalid_window_before_db_access(bad, monkeypatch):
    def no_connection(_settings):
        raise AssertionError("Invalid window attempted to open database")

    monkeypatch.setattr(analytics, "connection", no_connection)
    with pytest.raises(ValueError, match="integer in 1..24"):
        analytics.enrich_features(object(), tf="1", hours=bad)


def test_enrichment_overflow_is_atomic_and_retriable(c22_pg, monkeypatch):
    times = _seed_features(c22_pg, 3)
    monkeypatch.setattr(analytics, "MAX_ENRICH_FEATURE_ROWS", 2)
    with pytest.raises(RuntimeError, match="live_enrichment_row_budget_exceeded"):
        analytics.enrich_features(c22_pg, tf="1", hours=2)
    with connection(c22_pg) as conn:
        rows = conn.execute("""SELECT ts,cvd,quality,metadata FROM features
                              WHERE symbol='ETHUSDT' AND tf='1'
                              ORDER BY ts""").fetchall()
        assert [r["ts"] for r in rows] == times
        assert all(r["cvd"] is None for r in rows)
        assert all(r["metadata"] == {"preexisting": True} for r in rows)
        assert all(r["quality"] == "partial_unverified" for r in rows)
    monkeypatch.setattr(analytics, "MAX_ENRICH_FEATURE_ROWS", 4)
    assert analytics.enrich_features(c22_pg, tf="1", hours=2) == 3
    with connection(c22_pg) as conn:
        rows = conn.execute("""SELECT cvd,quality,metadata FROM features
                              WHERE symbol='ETHUSDT' AND tf='1'
                              ORDER BY ts""").fetchall()
        assert [r["cvd"] for r in rows] == [Decimal(1), Decimal(3), Decimal(6)]
        assert all(r["metadata"]["volume_exchange_complete"] is False for r in rows)
        assert all(r["quality"] == "partial_unverified" for r in rows)


def test_enrichment_lock_timeout_rolls_back_then_recovers(c22_pg):
    (stamp,) = _seed_features(c22_pg, 1)
    # Keep a real row lock open on a DIFFERENT transaction. The enrichment's
    # bounded UPDATE must time out, preserving the old metadata and CVD.
    with connection(c22_pg) as holder:
        holder.execute("""SELECT ts FROM features
                        WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s
                        FOR UPDATE""", (stamp,)).fetchone()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            analytics.enrich_features(c22_pg, tf="1", hours=2)
    with connection(c22_pg) as conn:
        row = conn.execute("""SELECT cvd,metadata FROM features
                             WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
                           (stamp,)).fetchone()
        assert row["cvd"] is None
        assert row["metadata"] == {"preexisting": True}
    assert analytics.enrich_features(c22_pg, tf="1", hours=2) == 1
    with connection(c22_pg) as conn:
        row = conn.execute("""SELECT cvd,quality,metadata FROM features
                             WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
                           (stamp,)).fetchone()
        assert row["cvd"] == Decimal(1)
        assert row["quality"] == "partial_unverified"
        assert row["metadata"]["volume_exchange_complete"] is False

def test_enrichment_total_transaction_timeout_is_active(c22_pg, monkeypatch):
    """Inspect the real PG17/18 transaction GUC while C still owns the txn."""
    original_connection = analytics.connection
    seen = []

    @contextmanager
    def observed_connection(settings):
        with original_connection(settings) as conn:
            yield conn
            row = conn.execute("""SELECT
                EXTRACT(EPOCH FROM current_setting('transaction_timeout')::interval) AS total_s,
                EXTRACT(EPOCH FROM current_setting('statement_timeout')::interval) AS stmt_s,
                EXTRACT(EPOCH FROM current_setting('lock_timeout')::interval) AS lock_s""").fetchone()
            seen.append((row["total_s"], row["stmt_s"], row["lock_s"]))

    monkeypatch.setattr(analytics, "connection", observed_connection)
    assert analytics.enrich_features(c22_pg, tf="1", hours=2) == 0
    assert seen == [(Decimal(30), Decimal(10), Decimal(2))]


def test_enrichment_never_consumes_future_feature_timestamps(c22_pg):
    (past,) = _seed_features(c22_pg, 1)
    with connection(c22_pg) as conn:
        future = conn.execute("SELECT clock_timestamp() + interval '1 hour' AS future").fetchone()["future"]
        conn.execute("""INSERT INTO features(symbol,tf,ts,delta,volume,quality,metadata)
                        VALUES('ETHUSDT','1',%s,9,1,'partial_unverified',
                               '{"future_sentinel":true}'::jsonb)""", (future,))
    assert analytics.enrich_features(c22_pg, tf="1", hours=2) == 1
    with connection(c22_pg) as conn:
        older = conn.execute("""SELECT cvd,metadata FROM features
                                WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
                             (past,)).fetchone()
        later = conn.execute("""SELECT cvd,metadata FROM features
                                WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
                             (future,)).fetchone()
    assert older["cvd"] == Decimal(1)
    assert later["cvd"] is None
    assert later["metadata"] == {"future_sentinel": True}
