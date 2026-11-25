"""D-owned real PostgreSQL negative-chain tests, isolated disposable database.

These prove C/B producer output never bypasses D on missing independent
coverage. A synthetic positive snapshot is NOT certified exchange evidence.
"""
from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.ml import FEATURE_NAMES
from eth_tester.orderflow import _certified_snapshot
from eth_tester.snapshot_producer import certify_snapshot
from eth_tester.storage.snapshots import write_feature_snapshot


pytestmark = pytest.mark.postgres


@pytest.fixture
def strategy_pg():
    dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Isolated PostgreSQL 17/18 requires ETH_TEST_PG_DSN")
    name = "eth_tester_d_" + uuid4().hex[:12]
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(dsn, dbname=name))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                sql.Identifier(name)))


def test_real_b_writer_unverified_snapshot_is_not_admissible(strategy_pg):
    now = datetime.now(timezone.utc)
    start = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    available = int((now - timedelta(seconds=1)).timestamp() * 1000)
    data = {feature: "1" for feature in FEATURE_NAMES}
    times = {feature: available for feature in FEATURE_NAMES}
    with connection(strategy_pg) as conn:
        key = write_feature_snapshot(
            conn, symbol="ETHUSDT", tf="1", bar_ts=start, observed_at=now,
            values=data, feature_available_ms=times, source="qa_unverified",
            source_checksum="a" * 64, certified=False)
        row = conn.execute("""SELECT quality,is_causal,computed_at,provenance
             FROM feature_snapshots WHERE snapshot_key=%s""", (key,)).fetchone()
        assert row["quality"] == "partial_unverified"
        assert row["is_causal"] is False
        assert row["computed_at"] >= now
        assert row["provenance"]["certified_by"] == "none"
    with connection(strategy_pg) as conn:
        assert _certified_snapshot(conn, "ETHUSDT", start,
                                   start + timedelta(minutes=1),
                                   datetime.now(timezone.utc)) is None


def test_real_c_producer_cannot_publish_without_independent_evidence(strategy_pg):
    now = datetime.now(timezone.utc)
    start = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    with connection(strategy_pg) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,3,'qa')""",
            (start,))
        conn.execute("""INSERT INTO features(symbol,tf,ts,poc,delta,cvd,
             volume,tape_tps) VALUES('ETHUSDT','1',%s,100,1,1,3,0.05)""",
            (start,))
    result = certify_snapshot(strategy_pg, "1")
    assert result["status"] == "rejected", result
    with connection(strategy_pg) as conn:
        count = conn.execute("""SELECT count(*) AS n FROM feature_snapshots
             WHERE quality='complete' AND is_causal""").fetchone()["n"]
        assert count == 0


def test_real_b_certified_writer_refuses_missing_source_proof(strategy_pg):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=2)
    values = {feature: "1" for feature in FEATURE_NAMES}
    times = {feature: int(now.timestamp() * 1000) - 1000
             for feature in FEATURE_NAMES}
    with pytest.raises(ValueError, match="source|coverage|Certified"):
        with connection(strategy_pg) as conn:
            write_feature_snapshot(
                conn, symbol="ETHUSDT", tf="1", bar_ts=start,
                observed_at=now, values=values, feature_available_ms=times,
                source="qa_unverified", source_checksum="b" * 64,
                certified=True)
    with connection(strategy_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM feature_snapshots").fetchone()["n"] == 0
