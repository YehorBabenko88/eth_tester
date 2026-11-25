"""H R15: disposable-PG financial gate acceptance, never exchange source proof.

Requires ETH_TEST_PG_DSN with CREATE DATABASE privilege. Creates and drops
ONLY its UUID-named ETH Tester QA database. QA monkeypatch of source snapshot
is deliberately limited to financial gate testing; it CANNOT certify B data,
actual paper fill feasibility, funding, ML performance or production readiness.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql
from psycopg.conninfo import make_conninfo

from eth_tester import paper
from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.strategies import Parameters, strategy_identity

pytestmark = pytest.mark.postgres


@pytest.fixture
def h15_pg():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Explicit disposable PostgreSQL ETH_TEST_PG_DSN required")
    name = "eth_tester_qa_h15_" + uuid4().hex[:12]
    target = make_conninfo(admin_dsn, dbname=name)
    created = False
    try:
        with psycopg.connect(
            make_conninfo(admin_dsn, dbname="postgres"), autocommit=True
        ) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(name)))
            created = True
        settings = Settings(_env_file=None, database_url=target)
        init_database(settings)
        with connection(settings) as conn:
            conn.execute("UPDATE bot_control SET enabled=true,paused=false "
                         "WHERE singleton=true")
        yield settings
    finally:
        if created:
            with psycopg.connect(
                make_conninfo(admin_dsn, dbname="postgres"), autocommit=True
            ) as admin:
                admin.execute(sql.SQL(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)))


def _reason():
    return {"strategy_version": strategy_identity(),
            "rules_version": Parameters().version, **Parameters().identity(),
            "qa_only": True}


def _first_provisional_close(settings):
    opened = datetime.now(timezone.utc) - timedelta(minutes=4)
    first = paper.open_paper(
        settings, "long", Decimal("100"), Decimal("98"),
        Decimal("105"), _reason(), observed_at=opened,
    )
    assert paper.close_paper(
        settings, Decimal("105"), "qa_take", observed_at=opened +
        timedelta(seconds=20), expected_id=first,
    ) == first
    with connection(settings) as conn:
        closed = conn.execute(
            "SELECT state,notional,funding_verified FROM paper_trades "
            "WHERE id=%s", (first,)
        ).fetchone()
        assert closed["state"] == "closed"
        assert closed["notional"] == Decimal("1000")
        assert closed["funding_verified"] is False
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
        ).fetchone()["n"] == 0
    return first


def _synthetic_live_reason():
    return {**_reason(), "source": "closed_candles",
            "signal_bar_ts": (
                datetime.now(timezone.utc).replace(second=0,microsecond=0)
                - timedelta(minutes=1)
            ).isoformat()}


def _mock_snapshot_only(monkeypatch):
    # B original-source admission is NOT under test here. Never infer that
    # a successful financial test proves source quality or ML eligibility.
    monkeypatch.setattr(paper, "bind_snapshot_reference",
                        lambda context, **kwargs: context)
    monkeypatch.setattr(paper, "verify_persisted_snapshot",
                        lambda *args, **kwargs: None)


def test_no_unconditional_first_close_block_and_one_active(h15_pg, monkeypatch):
    """Old blanket first-close rejection is forbidden; finite-risk refusal is not.

    Positive financial continuation needs a separately verified bounded-cost
    fixture; unknown/unbounded funding MUST be rejected, never waived.
    """
    first = _first_provisional_close(h15_pg)
    _mock_snapshot_only(monkeypatch)
    decision = datetime.now(timezone.utc)
    try:
        second = paper.open_paper(
            h15_pg, "long", Decimal("100"), Decimal("98"),
            Decimal("105"), _synthetic_live_reason(), observed_at=decision,
        )
    except RuntimeError as exc:
        # An actual finite risk/reserve limit may refuse without inventing
        # a funding proof. The old unconditional historical-close veto must
        # not silently survive as the entire financial risk policy.
        assert "Unreconciled provisional paper equity; admission blocked" not in str(exc)
        with connection(h15_pg) as conn:
            assert conn.execute(
                "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
            ).fetchone()["n"] == 0
        return
    assert second != first
    with connection(h15_pg) as conn:
        rows = conn.execute(
            "SELECT id,state,notional,funding_verified FROM paper_trades "
            "ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert sum(row["state"] == "open" for row in rows) == 1
        assert all(row["notional"] == Decimal("1000") for row in rows)
        assert rows[0]["funding_verified"] is False
    with pytest.raises(RuntimeError):
        paper.open_paper(h15_pg, "long", Decimal("100"), Decimal("98"),
                         Decimal("105"), _synthetic_live_reason(),
                         observed_at=datetime.now(timezone.utc))
    with connection(h15_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
        ).fetchone()["n"] == 1


def test_exhausted_paper_equity_never_bypasses_cap(h15_pg, monkeypatch):
    _first_provisional_close(h15_pg)
    _mock_snapshot_only(monkeypatch)
    with connection(h15_pg) as conn:
        conn.execute("UPDATE paper_account SET equity=1 WHERE id=1")
    with pytest.raises((RuntimeError, ValueError)):
        paper.open_paper(h15_pg, "long", Decimal("100"), Decimal("98"),
                         Decimal("105"), _synthetic_live_reason(),
                         observed_at=datetime.now(timezone.utc))
    with connection(h15_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 1
