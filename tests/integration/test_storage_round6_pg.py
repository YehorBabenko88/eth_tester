"""Round-6 Storage B real PostgreSQL 17/18 contract tests.

Uses ONLY an explicitly supplied administrative DSN for a disposable,
uniquely named eth_tester_... database. No orders, no external API requests,
no shared user database and no synthetic exchange-completeness assertion.
"""
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.database import connection, init_database
from eth_tester.storage.bulk import merge_staged_trades
from eth_tester.storage.archive_receipts import (
    persist_archive_stage_bins, revalidate_archive_bins,
)
from eth_tester.storage.live_evidence import (
    record_trade_receipts, record_book_lineage,
    record_oi_observation, publish_bounded_paper_evidence,
    require_bounded_paper_evidence,
)


pytestmark = pytest.mark.postgres
UTC = timezone.utc


def _owned_pg_lab(prefix):
    """Create one test-owned DB; never adopt or drop a replaced database."""
    assert prefix in ("eth_tester_storage_r6_", "eth_tester_storage_r8_")
    administrative = os.getenv("ETH_TEST_PG_DSN")
    if not administrative:
        pytest.skip("ETH_TEST_PG_DSN required for disposable real PostgreSQL")
    db_name = prefix + uuid4().hex[:12]
    settings = SimpleNamespace(
        database_url=make_conninfo(administrative, dbname=db_name),
        initial_equity_usd=Decimal("10000"), symbol="ETHUSDT",
        category="linear",
    )
    admin_dsn = make_conninfo(administrative, dbname="postgres")
    created = False
    owned_oid = None
    owned_role = None
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        # Cooperative lifecycle lock; distinct from production init lock.
        lock_name = "eth_tester:fixture_owner:" + db_name
        admin.execute("SELECT pg_advisory_lock(hashtext(%s))", (lock_name,))
        try:
            # One atomic PostgreSQL CREATE: collision fails, never adopt.
            admin.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(db_name)
            ))
            created = True
            identity = admin.execute(
                "SELECT oid,datdba FROM pg_database WHERE datname=%s",
                (db_name,),
            ).fetchone()
            if identity is None:
                raise RuntimeError("Created test database absent from catalog")
            owned_oid, owned_role = identity
            init_database(settings)
            initialized = admin.execute(
                "SELECT oid,datdba FROM pg_database WHERE datname=%s",
                (db_name,),
            ).fetchone()
            if (initialized is None or initialized[0] != owned_oid
                    or initialized[1] != owned_role):
                raise RuntimeError("Test database identity changed on bootstrap")
            with connection(settings) as conn:
                marker = conn.execute(
                    "SELECT owner_tag FROM eth_storage_identity "
                    "WHERE singleton=true"
                ).fetchone()
                if not marker or marker["owner_tag"] != "eth_tester":
                    raise RuntimeError("Test database schema owner marker mismatch")
            yield settings
        finally:
            try:
                if created:
                    present = admin.execute(
                        "SELECT oid,datdba FROM pg_database WHERE datname=%s",
                        (db_name,),
                    ).fetchone()
                    if (present is None or present[0] != owned_oid
                            or present[1] != owned_role):
                        raise RuntimeError(
                            "Refusing to DROP replaced or unowned QA database"
                        )
                    admin.execute(sql.SQL("DROP DATABASE {}").format(
                        sql.Identifier(db_name)
                    ))
            finally:
                admin.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s))", (lock_name,)
                )


@pytest.fixture(scope="module")
def source_lab():
    yield from _owned_pg_lab("eth_tester_storage_r6_")


@pytest.fixture
def r8_source_lab():
    yield from _owned_pg_lab("eth_tester_storage_r8_")


def test_round34_owned_pg_fixture_collision_and_cleanup(monkeypatch):
    """Mock catalog: collision never drops; failed init drops only owned OID."""
    class Result:
        def __init__(self, value=None):
            self.value = value

        def fetchone(self):
            return self.value

    class Admin:
        def __init__(self, mode):
            self.mode = mode
            self.created = 0
            self.dropped = 0
            self.reads = 0
            self.unlocked = 0

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def execute(self, query, params=None):
            command = str(query)
            if "pg_advisory_unlock" in command:
                self.unlocked += 1
                return Result()
            if "pg_advisory_lock" in command:
                return Result()
            if "CREATE DATABASE" in command:
                self.created += 1
                if self.mode == "collision":
                    raise psycopg.errors.DuplicateDatabase("existing name")
                return Result()
            if "SELECT oid,datdba" in command:
                self.reads += 1
                replacement = self.mode == "replaced" and self.reads == 3
                return Result((43 if replacement else 42, 10))
            if "DROP DATABASE" in command:
                self.dropped += 1
                return Result()
            raise AssertionError("Unexpected SQL in ownership mock")

    class MarkerConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def execute(self, statement):
            assert "eth_storage_identity" in statement
            return Result({"owner_tag": "eth_tester"})

    monkeypatch.setenv("ETH_TEST_PG_DSN", "dbname=postgres")
    for mode in ("collision", "bootstrap_failure", "replaced"):
        admin = Admin(mode)
        monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: admin)
        calls = []
        def bootstrap(_settings):
            calls.append("init")
            if mode == "bootstrap_failure":
                raise RuntimeError("injected bootstrap error")

        monkeypatch.setitem(globals(), "init_database", bootstrap)
        monkeypatch.setitem(
            globals(), "connection", lambda _settings: MarkerConnection()
        )
        owned = _owned_pg_lab("eth_tester_storage_r8_")
        if mode == "collision":
            with pytest.raises(psycopg.errors.DuplicateDatabase):
                next(owned)
            assert calls == []
            assert admin.dropped == 0
        elif mode == "bootstrap_failure":
            with pytest.raises(RuntimeError, match="injected bootstrap"):
                next(owned)
            assert calls == ["init"]
            assert admin.dropped == 1
        else:
            next(owned)
            with pytest.raises(RuntimeError, match="replaced or unowned"):
                next(owned)
            assert calls == ["init"]
            assert admin.dropped == 0
        assert admin.created == 1
        assert admin.unlocked == 1


def _r8_previous_closed_bar(anchor):
    """Use one DB clock sample; [start, end) excludes the current minute."""
    end = anchor.replace(second=0, microsecond=0)
    return end - timedelta(minutes=1), end


def test_round8_natural_minute_boundary_is_disjoint():
    """Regression at both edges: no freshly witnessed trade enters old bar."""
    for second, microsecond in ((0, 1), (0, 999999),
                                (59, 1), (59, 999999)):
        anchor = datetime(2026, 11, 26, 19, 47, second, microsecond,
                          tzinfo=UTC)
        start, end = _r8_previous_closed_bar(anchor)
        in_bar = end - timedelta(milliseconds=100)
        assert start <= in_bar < end <= anchor
        assert anchor < end + timedelta(minutes=1)


def _trade(ident, ts, *, source="archive", price="3000", qty="0.1"):
    p, q = Decimal(price), Decimal(qty)
    return ("ETHUSDT", ts, ident, p, q, "Buy", p*q, source,
            json.dumps({"i": ident, "event_ts": ts.isoformat()}))


def test_round6_migration_owner_marker_and_quality_separation(source_lab):
    with connection(source_lab) as conn:
        versions = conn.execute(
            "SELECT version FROM eth_schema_migrations ORDER BY version"
        ).fetchall()
        assert versions[-1]["version"] == "0009_disambiguate_paper_label_candle.sql"
        tables = (
            "archive_stage_receipts", "archive_source_price_bins",
            "archive_validation_receipts", "live_trade_receipts",
            "live_book_receipts", "live_oi_receipts", "paper_live_evidence",
        )
        for name in tables:
            assert conn.execute(
                "SELECT to_regclass(%s) AS reg", ("public." + name,)
            ).fetchone()["reg"] is not None
        cols = {
            row["column_name"] for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='public'
                     AND table_name='paper_live_evidence'"""
            ).fetchall()
        }
        assert {"snapshot_key", "source_checksum", "observed_at",
                "computed_at", "expires_at", "quality", "paper_only",
                "ml_eligible", "exchange_complete", "evidence"} <= cols
        assert conn.execute(
            "SELECT owner_tag FROM eth_storage_identity WHERE singleton=true"
        ).fetchone()["owner_tag"] == "eth_tester"


def test_round6_sha_stage_bins_exclude_ws_only_and_replay_fail_closed(source_lab):
    archive_day = date(2026, 11, 20)
    midnight = datetime.combine(archive_day, datetime.min.time(), UTC)
    a = _trade("R6-A", midnight+timedelta(minutes=1), source="websocket")
    b = _trade("R6-B", midnight+timedelta(minutes=2))
    outside = _trade("R6-WS-ONLY", midnight+timedelta(minutes=3),
                     source="websocket", qty="2")
    digest = "a"*64
    with connection(source_lab) as conn:
        for item in (a, outside):
            conn.execute(
                """INSERT INTO trades(
                    symbol,ts,trade_id,price,qty,side,notional,source,raw)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                item,
            )
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        with conn.cursor() as cursor:
            with cursor.copy(
                "COPY eth_stage (symbol,ts,trade_id,price,qty,side,"
                "notional,source,raw) FROM STDIN"
            ) as copier:
                for item in (a, b):
                    current = list(item)
                    current[7] = "archive"
                    copier.write_row(current)
        merged = merge_staged_trades(conn, "eth_stage")
        assert merged["input_rows"] == 2
        assert merged["inserted_rows"] == 1
        capture = persist_archive_stage_bins(
            conn, "ETHUSDT", archive_day, digest, 2,
        )
        assert capture["original_rows"] == 2
        assert capture["distinct_trade_ids"] == 2
        assert capture["exchange_complete"] is False
        rows = conn.execute(
            """SELECT minute_ts,price,side,trade_count,base_qty
               FROM archive_source_price_bins
               WHERE symbol='ETHUSDT' AND archive_sha256=%s
               ORDER BY minute_ts""", (digest,),
        ).fetchall()
        assert len(rows) == 2
        assert [r["minute_ts"].minute for r in rows] == [1, 2]
        assert conn.execute(
            """SELECT source FROM trades
               WHERE symbol='ETHUSDT' AND trade_id='R6-A'"""
        ).fetchone()["source"] == "websocket"
        assert persist_archive_stage_bins(
            conn, "ETHUSDT", archive_day, digest, 2,
        )["price_bins"] == 2
        with pytest.raises(ValueError, match="Conflicting immutable archive stage receipt"):
            with conn.transaction():
                conn.execute(
                    "UPDATE eth_stage SET qty=qty+0.1 WHERE trade_id='R6-B'"
                )
                persist_archive_stage_bins(
                    conn, "ETHUSDT", archive_day, digest, 2,
                )
        assert conn.execute(
            """SELECT count(*) AS n FROM archive_source_price_bins
               WHERE symbol='ETHUSDT' AND archive_sha256=%s""",
            (digest,),
        ).fetchone()["n"] == 2


def test_round6_versioned_archive_volume_revalidation_is_never_full_identity(source_lab):
    archive_day = date(2026, 11, 21)
    midnight = datetime.combine(archive_day, datetime.min.time(), UTC)
    digest = "c"*64
    with connection(source_lab) as conn:
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        with conn.cursor() as cursor:
            with cursor.copy(
                "COPY eth_stage (symbol,ts,trade_id,price,qty,side,"
                "notional,source,raw) FROM STDIN"
            ) as copier:
                copier.write_row(_trade("R6-REVALIDATE-A",
                                        midnight+timedelta(minutes=1)))
                copier.write_row(_trade("R6-REVALIDATE-B",
                                        midnight+timedelta(minutes=2)))
        merge_staged_trades(conn, "eth_stage")
        persist_archive_stage_bins(conn, "ETHUSDT", archive_day, digest, 2)
        first = revalidate_archive_bins(conn, "ETHUSDT", archive_day, digest)
        assert first["status"] == "incomplete"
        assert first["exchange_complete"] is False
        with conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO candles(
                   symbol,kind,tf,ts,open,high,low,close,volume,turnover,
                   closed,source)
                   VALUES('ETHUSDT','trade','1',%s,3000,3000,3000,
                          3000,%s,%s,true,'bybit_v5')""",
                [(midnight+timedelta(minutes=i),
                  Decimal("0.1") if i in (1, 2) else Decimal("0"),
                  Decimal("300") if i in (1, 2) else Decimal("0"))
                 for i in range(1440)],
            )
        second = revalidate_archive_bins(conn, "ETHUSDT", archive_day, digest)
        assert second["status"] == "volume_only_reconciled"
        assert second["minutes_checked"] == 1440
        assert second["minutes_matched"] == 1440
        assert second["exchange_complete"] is False
        assert first["validation_key"] != second["validation_key"]
        assert revalidate_archive_bins(
            conn, "ETHUSDT", archive_day, digest
        )["validation_key"] == second["validation_key"]
        assert conn.execute(
            """SELECT count(*) AS n FROM archive_validation_receipts
               WHERE symbol='ETHUSDT' AND archive_sha256=%s""",
            (digest,),
        ).fetchone()["n"] == 2
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    """UPDATE archive_stage_receipts SET original_rows=999
                       WHERE symbol='ETHUSDT' AND archive_sha256=%s""",
                    (digest,),
                )


def test_round6_actual_db_live_receipts_and_no_retrospective_paper(source_lab):
    now = datetime.now(UTC)
    stamp = now-timedelta(seconds=1)
    ident = "R6-LIVE-" + uuid4().hex[:12]
    source_sha = "b"*64
    with connection(source_lab) as conn:
        conn.execute(
            """INSERT INTO trades(
                 symbol,ts,trade_id,price,qty,side,notional,source,raw)
               VALUES('ETHUSDT',%s,%s,3000,0.01,'Buy',30,
                      'websocket',%s::jsonb)""",
            (stamp, ident, json.dumps({"i": ident})),
        )
        witnessed = record_trade_receipts(
            conn, "ETHUSDT", "session-round6", [ident]
        )
        assert witnessed["recorded_ids"] == 1
        receipt = conn.execute(
            """SELECT received_at,source_event_at,exchange_complete
               FROM live_trade_receipts
               WHERE symbol='ETHUSDT' AND trade_id=%s""",
            (ident,),
        ).fetchone()
        assert receipt["source_event_at"] == stamp
        assert receipt["received_at"] >= stamp
        assert receipt["exchange_complete"] is False

        snapshot = conn.execute(
            """INSERT INTO book_events(
                 symbol,exchange_ts,recv_ts,update_id,sequence_no,
                 event_type,bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),
                 100,100,'snapshot','[["2999","1"]]'::jsonb,
                 '[["3001","1"]]'::jsonb,true) RETURNING id""",
        ).fetchone()["id"]
        delta = conn.execute(
            """INSERT INTO book_events(
                 symbol,exchange_ts,recv_ts,update_id,sequence_no,
                 event_type,bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),
                 101,101,'delta','[]'::jsonb,'[]'::jsonb,true)
               RETURNING id,recv_ts""",
        ).fetchone()
        conn.execute(
            """INSERT INTO market_book_state(
                 symbol,last_event_id,bids,asks,last_update_id,valid,updated_at)
               VALUES('ETHUSDT',%s,'{"2999":"1"}'::jsonb,
                      '{"3001":"1"}'::jsonb,101,true,%s)
               ON CONFLICT(symbol) DO UPDATE SET
                 last_event_id=EXCLUDED.last_event_id,
                 bids=EXCLUDED.bids,asks=EXCLUDED.asks,
                 last_update_id=EXCLUDED.last_update_id,
                 valid=EXCLUDED.valid,updated_at=EXCLUDED.updated_at""",
            (delta["id"], delta["recv_ts"]),
        )
        book_id = record_book_lineage(
            conn, "ETHUSDT", "session-round6", snapshot, delta["id"]
        )
        assert book_id > 0
        oi_ts = datetime.now(UTC)-timedelta(minutes=1)
        conn.execute(
            """INSERT INTO open_interest(symbol,interval,ts,value)
               VALUES('ETHUSDT','5min',%s,1000)""", (oi_ts,),
        )
        oi_id = record_oi_observation(
            conn, "ETHUSDT", "5min", oi_ts, source_sha
        )
        assert oi_id > 0
        conn.execute(
            """INSERT INTO feed_health(symbol,topic,status)
               VALUES('ETHUSDT','connection','connected')""",
        )
        old_bar = now.replace(second=0, microsecond=0) - timedelta(minutes=10)
        with pytest.raises(ValueError, match="prospective"):
            publish_bounded_paper_evidence(
                conn, "ETHUSDT", old_bar, "session-round6", book_id, oi_id,
            )
        with pytest.raises(ValueError, match="prospective"):
            publish_bounded_paper_evidence(
                conn, "ETHUSDT",
                datetime.now(UTC).replace(second=0, microsecond=0)
                    + timedelta(minutes=1),
                "session-round6", book_id, oi_id,
            )
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_live_evidence"
        ).fetchone()["n"] == 0
        with pytest.raises(ValueError, match="current prospective"):
            require_bounded_paper_evidence(
                conn, symbol="ETHUSDT", bar_ts=old_bar,
                evidence_key="a"*64, decision_at=datetime.now(UTC),
            )
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    """DELETE FROM live_trade_receipts
                       WHERE symbol='ETHUSDT' AND trade_id=%s""",
                    (ident,),
                )


def test_round6_direct_caller_flags_cannot_forge_paper_live_evidence(source_lab):
    """Even direct SQL cannot force an old bar into a new paper decision."""
    old = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    with connection(source_lab) as conn:
        oi_id = conn.execute(
            "SELECT id FROM live_oi_receipts ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        book_id = conn.execute(
            "SELECT id FROM live_book_receipts ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        proof = {
            "contract": "eth_paper_live_evidence_v1",
            "quality": "live_observed_bounded",
            "paper_only": True, "ml_eligible": False,
            "exchange_complete": False, "trade_session": "session-round6",
            "book_receipt_id": book_id, "oi_receipt_id": oi_id,
        }
        with pytest.raises(psycopg.Error, match="prospective|closed|source"):
            with conn.transaction():
                conn.execute(
                    """INSERT INTO paper_live_evidence(
                       snapshot_key,source_checksum,symbol,bar_ts,bar_close_at,
                       trade_session,trade_count,trade_first_receipt_at,
                       trade_last_receipt_at,book_receipt_id,oi_receipt_id,
                       base_volume,quote_turnover,evidence,expires_at,
                       observed_at,computed_at)
                       VALUES(%s,%s,'ETHUSDT',%s,%s,'session-round6',
                              1,%s,%s,%s,%s,0.01,30,%s::jsonb,%s,%s,%s)""",
                    ("a"*64, "b"*64, old, old+timedelta(minutes=1),
                     old, old, book_id, oi_id, json.dumps(proof),
                     old+timedelta(seconds=150),
                     old+timedelta(minutes=1), old+timedelta(minutes=1)),
                )
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_live_evidence"
        ).fetchone()["n"] == 0


def test_round8_issued_live_receipt_rejected_after_disconnect_or_l2_gap(r8_source_lab):
    """Real disposable PG; synthetic receipts prove transaction contract ONLY.

    Start from B's actual publish API, not a directly hand-crafted evidence
    row. The original row stays immutable; later gaps and invalid replay
    deny admission. This test never opens a real market order.
    """
    import eth_tester.storage.live_evidence as evidence

    session = "r8-" + uuid4().hex[:14]
    original_id = "R8-ORIGINAL-" + uuid4().hex[:12]
    fresh_id = "R8-LATEST-" + uuid4().hex[:12]
    with connection(r8_source_lab) as conn:
        # One PostgreSQL clock anchor is used for both trade event times.
        anchor = conn.execute(
            "SELECT clock_timestamp() AS ts"
        ).fetchone()["ts"]
        start, end = _r8_previous_closed_bar(anchor)
        original_at = end - timedelta(milliseconds=100)
        fresh_at = anchor  # Within the current minute, OUTSIDE [start, end).
        assert start <= original_at < end <= fresh_at
        for trade_id, event_at in (
            (original_id, original_at),
            (fresh_id, fresh_at),
        ):
            conn.execute(
                """INSERT INTO trades(symbol,ts,trade_id,price,qty,side,
                    notional,source,raw)
                   VALUES('ETHUSDT',%s,%s,3000,0.01,'Buy',30,'websocket',%s::jsonb)""",
                (event_at, trade_id, json.dumps({"r8": trade_id})),
            )
            evidence.record_trade_receipts(
                conn, "ETHUSDT", session, [trade_id],
            )
        conn.execute(
            """INSERT INTO candles(symbol,kind,tf,ts,open,high,low,close,
                volume,turnover,closed,source)
               VALUES('ETHUSDT','trade','1',%s,3000,3000,3000,3000,
                      0.01,30,true,'bybit_v5')""",
            (start,),
        )
        snapshot = conn.execute(
            """INSERT INTO book_events(symbol,recv_ts,update_id,sequence_no,
                event_type,bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),1000,1000,'snapshot',
                      '[["2999","1"]]'::jsonb,'[["3001","1"]]'::jsonb,true)
               RETURNING id""",
        ).fetchone()["id"]
        delta = conn.execute(
            """INSERT INTO book_events(symbol,recv_ts,update_id,sequence_no,
                event_type,bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),1001,1001,'delta',
                      '[]'::jsonb,'[]'::jsonb,true)
               RETURNING id,recv_ts""",
        ).fetchone()
        conn.execute(
            """INSERT INTO market_book_state(
                 symbol,last_event_id,bids,asks,last_update_id,valid,updated_at)
               VALUES('ETHUSDT',%s,'{"2999":"1"}'::jsonb,
                      '{"3001":"1"}'::jsonb,1001,true,%s)
               ON CONFLICT(symbol) DO UPDATE SET
                 last_event_id=EXCLUDED.last_event_id,bids=EXCLUDED.bids,
                 asks=EXCLUDED.asks,last_update_id=EXCLUDED.last_update_id,
                 valid=EXCLUDED.valid,updated_at=EXCLUDED.updated_at""",
            (delta["id"], delta["recv_ts"]),
        )
        book_id = evidence.record_book_lineage(
            conn, "ETHUSDT", session, snapshot, delta["id"],
        )
        oi_at = anchor - timedelta(minutes=1)
        conn.execute(
            """INSERT INTO open_interest(symbol,interval,ts,value)
               VALUES('ETHUSDT','5min',%s,1000)""", (oi_at,),
        )
        oi_id = evidence.record_oi_observation(
            conn, "ETHUSDT", "5min", oi_at, "a"*64,
        )
        conn.execute(
            """INSERT INTO feed_health(symbol,topic,ts,status,detail)
               VALUES('ETHUSDT','connection',clock_timestamp(),
                      'connected',%s::jsonb)""",
            (json.dumps({"session_id": session}),),
        )
        # Inspect the exact B publication window before calling the REAL issuer.
        observed = conn.execute(
            """SELECT count(*) AS n, count(DISTINCT session_id) AS sessions,
                      min(session_id) AS only_session,
                      coalesce(sum(qty),0) AS base,
                      coalesce(sum(notional),0) AS quote,
                      min(source_event_at) AS first_event,
                      max(source_event_at) AS last_event,
                      min(received_at) AS first_receipt,
                      max(received_at) AS last_receipt
               FROM live_trade_receipts
               WHERE symbol='ETHUSDT'
                 AND source_event_at >= %s AND source_event_at < %s""",
            (start, end),
        ).fetchone()
        in_bar_ids = conn.execute(
            """SELECT trade_id FROM live_trade_receipts
               WHERE symbol='ETHUSDT'
                 AND source_event_at >= %s AND source_event_at < %s
               ORDER BY trade_id""",
            (start, end),
        ).fetchall()
        extra = conn.execute(
            """SELECT source_event_at, received_at
               FROM live_trade_receipts
               WHERE symbol='ETHUSDT' AND trade_id=%s""",
            (fresh_id,),
        ).fetchone()
        db_now = conn.execute(
            "SELECT clock_timestamp() AS ts"
        ).fetchone()["ts"]
        assert observed["n"] == 1, observed
        assert observed["sessions"] == 1, observed
        assert observed["only_session"] == session, observed
        assert observed["base"] == Decimal("0.01"), observed
        assert observed["quote"] == Decimal("30"), observed
        assert [row["trade_id"] for row in in_bar_ids] == [original_id]
        assert observed["first_event"] == original_at, observed
        assert observed["last_event"] == original_at, observed
        assert anchor <= observed["first_receipt"] <= db_now, observed
        assert anchor <= observed["last_receipt"] <= db_now, observed
        assert observed["last_receipt"] >= db_now - timedelta(seconds=20)
        assert extra is not None
        assert extra["source_event_at"] == fresh_at
        assert end <= extra["source_event_at"] <= db_now
        assert anchor <= extra["received_at"] <= db_now
        key = evidence.publish_bounded_paper_evidence(
            conn, "ETHUSDT", start, session, book_id, oi_id,
        )
        decision = conn.execute(
            "SELECT clock_timestamp() AS ts"
        ).fetchone()["ts"]
        valid = evidence.require_bounded_paper_evidence(
            conn, symbol="ETHUSDT", bar_ts=start,
            evidence_key=key, decision_at=decision, at_admission=True,
        )
        assert valid["snapshot_key"] == key
        assert valid["paper_only"] is True
        assert valid["ml_eligible"] is False

        # Changing actual L2 state after B's immutable publication must not
        # admit a paper trade merely because the old SHA is still present.
        with pytest.raises(ValueError, match="L2"):
            with conn.transaction():
                conn.execute(
                    """UPDATE market_book_state SET valid=false
                       WHERE symbol='ETHUSDT'""",
                )
                evidence.require_bounded_paper_evidence(
                    conn, symbol="ETHUSDT", bar_ts=start,
                    evidence_key=key, decision_at=decision,
                    at_admission=True,
                )
        # A real recorded book-only interruption is a decisive rejection.
        with pytest.raises(ValueError, match="gap|interruption"):
            with conn.transaction():
                conn.execute(
                    """INSERT INTO data_gaps(
                         source,symbol,start_ts,end_ts,explanation)
                       VALUES('orderbook','ETHUSDT',%s,clock_timestamp(),
                              'synthetic_disposable_pg_r8')""",
                    (end,),
                )
                evidence.require_bounded_paper_evidence(
                    conn, symbol="ETHUSDT", bar_ts=start,
                    evidence_key=key, decision_at=decision,
                    at_admission=True,
                )
        # Even a still-valid original B-issued key cannot survive a fresh
        # disconnect after the proof; no mutating old evidence or backdating.
        with pytest.raises(ValueError, match="WebSocket"):
            with conn.transaction():
                conn.execute(
                    """INSERT INTO feed_health(symbol,topic,ts,status,detail)
                       VALUES('ETHUSDT','connection',clock_timestamp(),
                              'disconnected',%s::jsonb)""",
                    (json.dumps({"session_id": session}),),
                )
                evidence.require_bounded_paper_evidence(
                    conn, symbol="ETHUSDT", bar_ts=start,
                    evidence_key=key, decision_at=decision,
                    at_admission=True,
                )
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0
