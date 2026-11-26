"""H R19: real engine_tick -> real paper.open_paper -> disposable PostgreSQL.

No exchange/Telegram connections. Market inputs are synthetic QA receipts, NOT
proof of exchange-wide completeness, executable fills, verified funding or ML.
Never mock the signal engine, D confirmation, B source verifier or E paper DB.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.paper import close_paper, open_paper
from eth_tester.runner import engine_tick
from eth_tester.simulation.live_evidence import bind_live_paper_reference
from eth_tester.storage.live_evidence import (
    publish_bounded_paper_evidence,
    record_book_lineage,
    record_oi_observation,
    record_trade_receipts,
)
from eth_tester.strategies import Parameters
from eth_tester.strategy import price_signal
from eth_tester.orderflow import confirm_rejection


pytestmark = pytest.mark.postgres
UTC = timezone.utc
D = Decimal


@pytest.fixture
def h19_pg():
    admin_url = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_url:
        pytest.skip("Explicit disposable ETH_TEST_PG_DSN required")
    name = "eth_tester_h19_" + uuid4().hex[:12]
    target = make_conninfo(admin_url, dbname=name)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=target)
        init_database(settings)
        with connection(settings) as conn:
            conn.execute(
                "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
            )
        yield settings
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def _seed_genuine_db_witnesses(settings):
    """Populate B's actual storage/recheck path with synthetic QA observations.

    Receipt timestamps, immutable keys and SHA are computed by the DB/B code;
    none is fabricated or supplied by the test as a verified admission flag.
    """
    with connection(settings) as conn:
        clock = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        last = clock.replace(second=0, microsecond=0) - timedelta(minutes=1)
        end = last + timedelta(minutes=1)
        session = "qa-h19-" + uuid4().hex[:12]

        for i in range(35):
            at = last - timedelta(minutes=34 - i)
            high = D("100.7") if i == 25 else D("100.25")
            if i == 34:
                high = D("101.5")
            close = D("101") if i == 34 else D("100")
            qty = D("50") if i == 34 else D("2")
            turnover = D("0")
            if i == 34:
                turnover = D("100.7") * 48 + D("101") * 2
            conn.execute(
                """INSERT INTO candles(
                   symbol,kind,tf,ts,open,high,low,close,volume,turnover,
                   closed,source,quality)
                   VALUES('ETHUSDT','trade','1',%s,100,%s,99.8,%s,%s,%s,
                   true,'bybit_v5','test_synthetic')""",
                (at, high, close, qty, turnover),
            )

        for tf in (5, 15, 60):
            width = tf * 60
            recent = datetime.fromtimestamp(
                ((int(end.timestamp()) - width) // width) * width, UTC
            )
            for index, value in enumerate((D("102"), D("101"), D("100"))):
                conn.execute(
                    """INSERT INTO candles(
                       symbol,kind,tf,ts,open,high,low,close,volume,turnover,
                       closed,source,quality)
                       VALUES('ETHUSDT','trade',%s,%s,%s,%s,%s,%s,5,500,
                       true,'bybit_v5','test_synthetic')""",
                    (str(tf), recent - timedelta(minutes=tf * index),
                     value, value + 1, value - 1, value),
                )

        # Five closed baseline minutes with two real stored trade prints each.
        for m in range(5):
            stamp = last - timedelta(minutes=5 - m)
            for j, side in enumerate(("Buy", "Sell")):
                conn.execute(
                    """INSERT INTO trades(
                       symbol,ts,trade_id,price,qty,side,notional,source,raw)
                       VALUES('ETHUSDT',%s,%s,100,1,%s,100,
                       'websocket','{"qa_only":true}'::jsonb)""",
                    (stamp + timedelta(seconds=20 + j), f"h19-base-{m}-{j}", side),
                )

        ids = []
        for j in range(25):
            side = "Buy" if j < 21 else "Sell"
            price = D("101") if j == 24 else D("100.7")
            ident = f"h19-live-{j}"
            ids.append(ident)
            conn.execute(
                """INSERT INTO trades(
                   symbol,ts,trade_id,price,qty,side,notional,source,raw)
                   VALUES('ETHUSDT',%s,%s,%s,2,%s,%s,'websocket',
                   '{"qa_only":true}'::jsonb)""",
                (last + timedelta(seconds=30 + j), ident,
                 price, side, price * 2),
            )
        assert record_trade_receipts(
            conn, settings.symbol, session, ids
        )["recorded_ids"] == 25

        conn.execute(
            """INSERT INTO feed_health(symbol,topic,ts,status,detail)
               VALUES('ETHUSDT','connection',clock_timestamp(),'connected',
               %s::jsonb)""",
            (json.dumps({"session_id": session, "qa_only": True}),),
        )

        book = conn.execute(
            """INSERT INTO book_events(
               symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
               bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),1,1,
               'snapshot','{"100":"80"}'::jsonb,'{"101":"20"}'::jsonb,true)
               RETURNING id"""
        ).fetchone()["id"]
        conn.execute(
            """INSERT INTO market_book_state(
               symbol,last_event_id,bids,asks,last_update_id,valid,updated_at)
               VALUES('ETHUSDT',%s,'{"100":"80"}'::jsonb,
               '{"101":"20"}'::jsonb,1,true,clock_timestamp())""",
            (book,),
        )
        book_receipt = record_book_lineage(
            conn, settings.symbol, session, book, book
        )

        oi_latest = datetime.fromtimestamp(
            (int(end.timestamp()) // 300) * 300, UTC
        )
        for at, value in (
            (oi_latest - timedelta(minutes=5), D("1000")),
            (oi_latest, D("1050")),
        ):
            conn.execute(
                """INSERT INTO open_interest(symbol,interval,ts,value)
                   VALUES('ETHUSDT','5min',%s,%s)""",
                (at, value),
            )
        oi_receipt = record_oi_observation(
            conn, settings.symbol, "5min", oi_latest, "b" * 64
        )
        key = publish_bounded_paper_evidence(
            conn, settings.symbol, last, session, book_receipt, oi_receipt
        )
        row = conn.execute(
            """SELECT snapshot_key,paper_only,ml_eligible,exchange_complete
               FROM paper_live_evidence WHERE snapshot_key=%s""",
            (key,),
        ).fetchone()
        assert row["snapshot_key"] == key
        assert row["paper_only"] is True
        assert row["ml_eligible"] is False
        assert row["exchange_complete"] is False

        # Fresh print in the *new* minute for runner/admission feed freshness.
        fresh = conn.execute(
            "SELECT clock_timestamp() AS ts"
        ).fetchone()["ts"]
        conn.execute(
            """INSERT INTO trades(
               symbol,ts,trade_id,price,qty,side,notional,source,raw)
               VALUES('ETHUSDT',%s,'h19-after-close',101,1,'Buy',101,
               'websocket','{"qa_only":true}'::jsonb)""",
            (fresh,),
        )
        record_trade_receipts(
            conn, settings.symbol, session, ["h19-after-close"]
        )

    with connection(settings) as conn:
        bars = conn.execute(
            """SELECT ts,open,high,low,close,volume,closed
               FROM candles WHERE symbol='ETHUSDT' AND kind='trade'
               AND tf='1' ORDER BY ts DESC LIMIT 35"""
        ).fetchall()
        bars.reverse()
        actual = price_signal(
            bars, settings.taker_fee_rate, settings.slippage_bps
        )
        assert actual is not None, "Canonical price_signal must issue a real candidate"
        assert actual.direction == "long"
        assert actual.reason["source"] == "closed_candles"
        admitted, flow = confirm_rejection(
            conn, settings, actual, bars[-1],
            observed_at=conn.execute(
                "SELECT clock_timestamp() AS ts"
            ).fetchone()["ts"],
        )
        assert admitted, flow
        assert flow["immutable_live_evidence"]["snapshot_key"] == key
        assert flow["ml_eligible"] is False
    return last, key, actual, flow


def test_real_runner_to_open_paper_creates_one_provisional_pg_position(h19_pg):
    last, key, _signal, _flow = _seed_genuine_db_witnesses(h19_pg)
    result = engine_tick(h19_pg)  # REAL runner + REAL E paper + REAL B/D + PG.
    assert result.startswith("opened_"), result
    trade_id = int(result.removeprefix("opened_"))
    with connection(h19_pg) as conn:
        trades = conn.execute(
            """SELECT id,notional,state,reason,funding_verified,
                      execution_quality FROM paper_trades"""
        ).fetchall()
        assert len(trades) == 1
        trade = trades[0]
        assert (trade["id"], trade["notional"], trade["state"]) == (
            trade_id, D("1000"), "open"
        )
        assert trade["funding_verified"] is False
        assert trade["execution_quality"] == "trade_print_proxy_no_liquidity"
        reason = trade["reason"]
        assert reason["paper_live_snapshot_key"] == key
        assert reason["paper_only"] is True
        assert reason["ml_eligible"] is False
        assert reason["orderflow"]["immutable_live_evidence"]["snapshot_key"] == key
        assert "ml_snapshot_key" not in reason
        assert "ml_label" not in reason
        events = conn.execute(
            "SELECT action FROM paper_order_events WHERE paper_trade_id=%s",
            (trade_id,),
        ).fetchall()
        assert [e["action"] for e in events] == ["entry"]
        stored = conn.execute(
            """SELECT decision,reason FROM strategy_signals
               WHERE symbol='ETHUSDT' AND tf='1' AND bar_ts=%s""",
            (last,),
        ).fetchone()
        assert stored["decision"] == "opened"
        assert stored["reason"]["paper_trade_id"] == trade_id
        assert conn.execute(
            "SELECT count(*) AS n FROM model_versions"
        ).fetchone()["n"] == 0

    assert engine_tick(h19_pg) == "monitoring_position"
    with pytest.raises(RuntimeError, match="existing durable paper position"):
        open_paper(
            h19_pg, "short", D("100"), D("102"), D("95"),
            {"strategy_version": Parameters().engine_identity(),
             "rules_version": Parameters().version,
             **Parameters().identity(), "qa_only": True},
            observed_at=datetime.now(UTC),
        )
    with connection(h19_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 1


def test_tampered_proof_and_ml_binding_cannot_open_pg_position(h19_pg):
    last, _key, signal, flow = _seed_genuine_db_witnesses(h19_pg)
    reason = {**signal.reason, "orderflow": flow}
    now = datetime.now(UTC)

    # A partial forged binding is rejected rather than completed.
    with pytest.raises(ValueError, match="Incomplete original live paper binding"):
        bind_live_paper_reference(
            {**reason, "ml_eligible": True},
            bar_ts=last, decision_at=now,
        )

    # Rebinding an already valid original reason is strictly idempotent.
    # Bool identity matters: integer 0 must NOT be admitted as False.
    bound = bind_live_paper_reference(reason, bar_ts=last, decision_at=now)
    assert bind_live_paper_reference(
        bound, bar_ts=last, decision_at=now
    )["ml_eligible"] is False
    for invalid in (True, 0, "false"):
        with pytest.raises(
            ValueError, match="Conflicting paper-only immutable admission reason"
        ):
            bind_live_paper_reference(
                {**bound, "ml_eligible": invalid},
                bar_ts=last, decision_at=now,
            )
    with pytest.raises(
        ValueError, match="Bounded-live paper evidence cannot carry ML"
    ):
        bind_live_paper_reference(
            {**bound, "ml_label": "forged"},
            bar_ts=last, decision_at=now,
        )

    damaged = {**flow, "immutable_live_evidence": {
        **flow["immutable_live_evidence"],
        "source_checksum": "0" * 64,
    }}
    tampered = {
        **signal.reason, "orderflow": damaged,
        "market_trade_ts": now.isoformat(),
    }
    # Bad but well-formed SHA passes E binding and MUST fail B's real
    # PostgreSQL FOR SHARE immutable-row recheck, not an earlier fake gate.
    with pytest.raises(ValueError, match="Bounded live provenance/quality mismatch"):
        open_paper(
            h19_pg, signal.direction, D("101"), signal.stop, signal.take,
            tampered, observed_at=now,
        )
    with connection(h19_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0


def test_pg_close_and_new_manual_qa_trade_never_replays_original_signal(h19_pg):
    """Re-arm real PG ledger; second manual QA entry is NOT D/B-certified.

    A subsequent autonomous D/B entry needs the next independently observed
    closed minute. Never replay the first terminal runner decision.
    """
    last, original_key, _signal, _flow = _seed_genuine_db_witnesses(h19_pg)
    first = engine_tick(h19_pg)
    assert first.startswith("opened_"), first
    first_id = int(first.removeprefix("opened_"))
    assert close_paper(
        h19_pg, D("101"), "manual_qa_unverified",
        observed_at=datetime.now(UTC), expected_id=first_id,
    ) == first_id

    with connection(h19_pg) as conn:
        closed = conn.execute(
            "SELECT state,funding_verified,reason FROM paper_trades WHERE id=%s",
            (first_id,),
        ).fetchone()
        assert closed["state"] == "closed"
        assert closed["funding_verified"] is False
        assert closed["reason"]["paper_live_snapshot_key"] == original_key
        state = conn.execute(
            "SELECT paper_trade_id,version FROM paper_position_state"
        ).fetchone()
        assert state["paper_trade_id"] is None and state["version"] == 2
        events = conn.execute(
            """SELECT action FROM paper_order_events
               WHERE paper_trade_id=%s ORDER BY event_ts""", (first_id,)
        ).fetchall()
        assert [row["action"] for row in events] == ["entry", "exit"]

    replay = engine_tick(h19_pg)
    assert not replay.startswith("opened_"), replay
    with connection(h19_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
        ).fetchone()["n"] == 0
        assert conn.execute(
            """SELECT count(*) AS n FROM strategy_signals
               WHERE symbol='ETHUSDT' AND tf='1' AND bar_ts=%s
                 AND decision='opened'""", (last,)
        ).fetchone()["n"] == 1

    # Only QA/manual ledger re-arming is tested for the second entry.
    # It cannot certify market provenance, funding, executable PnL or ML.
    next_id = open_paper(
        h19_pg, "short", D("101"), D("105"), D("95"),
        {"strategy_version": Parameters().engine_identity(),
         "rules_version": Parameters().version,
         **Parameters().identity(), "qa_only": True},
        observed_at=datetime.now(UTC),
    )
    assert next_id != first_id
    with connection(h19_pg) as conn:
        trades = conn.execute(
            "SELECT id,state,notional,reason FROM paper_trades ORDER BY id"
        ).fetchall()
        assert [(row["id"], row["state"], row["notional"]) for row in trades] == [
            (first_id, "closed", D("1000")),
            (next_id, "open", D("1000")),
        ]
        assert trades[1]["reason"].get("ml_eligible") is not True
        assert trades[1]["reason"].get("paper_live_snapshot_key") is None
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events WHERE action='entry'"
        ).fetchone()["n"] == 2
        state = conn.execute(
            "SELECT paper_trade_id,version FROM paper_position_state"
        ).fetchone()
        assert state["paper_trade_id"] == next_id
        assert state["version"] == 3
