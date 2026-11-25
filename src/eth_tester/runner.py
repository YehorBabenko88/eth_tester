"""Paper-only signal engine, never exchange order routing."""
import asyncio
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json
import logging
from .database import connection
from .paper import open_paper,close_paper,_canonical_funding_rows
from .strategy import price_signal
from .orderflow import confirm_rejection
from .simulation.crossing import resolve_first_crossing
from .simulation.review import review_one_closed_trade

LOGGER=logging.getLogger(__name__)
from .simulation.identity import (bind_snapshot_reference, require_new_strategy_identity,
                                  rule_identity)

def estimated_funding(conn,settings,opened,observed,qty,entry,direction,*,with_events=False):
    """Never synthesize funding from entry price: require event-time mark evidence.

    Historical funding chronology may still be incomplete: a returned 'marked'
    result is not proof all funding events were supplied.
    """
    rows=_canonical_funding_rows(conn,settings.symbol,opened,observed)
    if not rows or any(row["mark_price"] is None for row in rows):
        return (Decimal(0),"unverified",[]) if with_events else (Decimal(0),"unverified")
    sign=Decimal(1) if direction=="long" else Decimal(-1)
    amount=sign*sum((Decimal(qty)*Decimal(row["mark_price"])*Decimal(row["rate"])
                     for row in rows),Decimal(0))
    return (amount,"marked_not_completeness_proven",rows) if with_events else (
        amount,"marked_not_completeness_proven")

_RETRYABLE_PROOF_DECISION = "pending_missing_proof"


def _paper_window(bar_ts, at):
    """No late evidence can revive a signal after its original 90s deadline."""
    end = bar_ts + timedelta(minutes=1)
    return end <= at <= end + timedelta(seconds=90)


def _decision_payload(signal, identity, metadata):
    payload = dict(signal.reason) if signal else {}
    if signal is None:
        payload.update({
            "strategy_version": identity,
            "rules_version": metadata["parameters"]["version"],
            "parameters": metadata["parameters"],
            "parameter_sha256": metadata["parameter_sha256"],
        })
    payload.update({
        "engine_identity": identity,
        "engine_parameters": metadata["parameters"],
        "engine_parameter_sha256": metadata["parameter_sha256"],
    })
    return payload


def _persist_paper_decision(conn, settings, bar_ts, version, decision, payload):
    # Retry ONLY the unresolved original proof request, never a closed decision.
    saved=conn.execute("""INSERT INTO strategy_signals(symbol,tf,bar_ts,version,decision,reason)
       VALUES(%s,'1',%s,%s,%s,%s::jsonb)
       ON CONFLICT(symbol,tf,bar_ts,version) DO UPDATE SET
         decision=EXCLUDED.decision,reason=EXCLUDED.reason
       WHERE strategy_signals.decision='pending_missing_proof'
       RETURNING decision,reason""",
       (settings.symbol,bar_ts,version,decision,json.dumps(payload,default=str))
       ).fetchone()
    # Concurrent terminal writer must NOT leave an orphan paper entry.
    if saved is None or saved["decision"]!=decision:
        raise RuntimeError("atomic_signal_decision_conflict")
    if decision=="opened" and (
            not isinstance(saved["reason"],dict) or
            saved["reason"].get("paper_trade_id")!=payload.get("paper_trade_id")):
        raise RuntimeError("opened_paper_trade_identity_conflict")
    return saved


def _try_paper_admission_mutex(conn):
    """Acquire E's global position mutex BEFORE D/B prospective source checks.

    close_paper uses global 20991991 -> source hashtext(...) order. Taking
    source in confirm_rejection BEFORE global causes the inverse lock cycle.
    Never acquire source, mutate ledgers or terminalize a signal on contention.
    The lease is transaction-scoped, including later open_paper re-entry.
    """
    result=conn.execute(
        "SELECT pg_try_advisory_xact_lock(20991991) AS acquired"
    ).fetchone()
    return result is not None and result["acquired"] is True


def engine_tick(settings):
    """Single-shot paper admission: evidence and entry commit atomically.

    Missing proof can be reconsidered only for the ORIGINAL still-open bar
    window; expired windows never replay after archive/WS recovery.
    """
    now=datetime.now(timezone.utc)
    opened_id=None
    with connection(settings) as conn:
        status=conn.execute("""SELECT status FROM feed_health WHERE symbol=%s
          AND topic='connection' ORDER BY ts DESC LIMIT 1""",(settings.symbol,)).fetchone()
        recent=conn.execute("""SELECT ts,price FROM trades WHERE symbol=%s
          AND ts>=%s AND ts<=%s AND source='websocket'
          ORDER BY ts DESC,trade_id DESC LIMIT 1""",
          (settings.symbol,now-timedelta(seconds=15),
           now+timedelta(seconds=2))).fetchone()
        trade=conn.execute("SELECT * FROM paper_trades WHERE state='open' LIMIT 1").fetchone()
        if not recent or not status or status["status"]!="connected":
            # Do not hide a durable open position after an internet outage.
            # Never fabricate a stop/take fill across missing trade prints.
            return ("waiting_live_feed_open_position" if trade
                    else "waiting_live_feed")
        if trade:
            stop,take=Decimal(trade["stop"]),Decimal(trade["take"])
            gap=conn.execute("""SELECT 1 FROM data_gaps WHERE symbol=%s
              AND source='publicTrade' AND resolved_at IS NULL
              AND start_ts<=%s AND (end_ts IS NULL OR end_ts>=%s) LIMIT 1""",
              (settings.symbol,recent["ts"],trade["opened_at"])).fetchone()
            if gap:
                return "blocked_unresolved_tick_gap"
            lower,upper=min(stop,take),max(stop,take)
            hit=conn.execute("""SELECT ts,
              min(price) FILTER (WHERE price<=%s) AS low_cross,
              max(price) FILTER (WHERE price>=%s) AS high_cross
              FROM trades WHERE symbol=%s AND source='websocket'
              AND ts>%s AND ts<=%s AND (price<=%s OR price>=%s)
              GROUP BY ts ORDER BY ts LIMIT 1""",
              (lower,upper,settings.symbol,trade["opened_at"],recent["ts"],
               lower,upper)).fetchone()
            crossing=resolve_first_crossing(hit,trade["direction"],stop,take)
            if crossing is None:
                return "monitoring_position"
            recent={"ts":crossing["ts"],"price":crossing["price"]}
            reason=crossing["reason"]
            if crossing["ambiguous_same_timestamp"]:
                reason+="_same_timestamp_ambiguous_stop_first"
            # close_paper independently reads the already-persisted as-of BBO
            # inside its own settlement transaction, never from caller flags.
            funding,funding_quality,funding_evidence=estimated_funding(
                conn,settings,trade["opened_at"],recent["ts"],trade["qty"],
                trade["entry"],trade["direction"],with_events=True)
            if funding_quality!="marked_not_completeness_proven":
                reason+="_funding_unverified"
        else:
            enabled=conn.execute(
                "SELECT enabled,paused FROM bot_control WHERE singleton=true").fetchone()
            if not enabled or not enabled["enabled"] or enabled["paused"]:
                return "paused"
            # Preserve historical outage evidence without permanently
            # blocking a future, independently observed clean bar.
            bars=conn.execute("""SELECT ts,open,high,low,close,volume FROM candles
              WHERE symbol=%s AND kind='trade' AND tf='1' AND closed
              AND ts<=%s ORDER BY ts DESC LIMIT 180""",
              (settings.symbol,now-timedelta(minutes=1))).fetchall()
            bars.reverse()
            if len(bars)<30 or now-bars[-1]["ts"]>timedelta(minutes=5):
                return "stale_or_insufficient_candles"
            if any(b["ts"]-a["ts"]!=timedelta(minutes=1)
                   for a,b in zip(bars,bars[1:])):
                return "blocked_candle_gap"
            last=bars[-1]["ts"]
            missing=conn.execute("""SELECT 1 FROM data_gaps
              WHERE symbol=%s AND source IN ('publicTrade','orderbook')
                AND start_ts < %s
                AND coalesce(end_ts,resolved_at,'infinity'::timestamptz) >= %s
              LIMIT 1""",(settings.symbol,now,last)).fetchone()
            if missing:
                return "blocked_unresolved_tick_gap"
            # Canonical R12 lock hierarchy for every prospective 1m decision:
            # GLOBAL (20991991) -> per-bar paper_signal -> B/D SOURCE.
            # Exit always takes GLOBAL -> SOURCE, avoiding both lock inversions.
            # Never publish a terminal decision if another process owns GLOBAL.
            conn.execute("SET LOCAL lock_timeout = '2000ms'")
            conn.execute("SET LOCAL statement_timeout = '5000ms'")
            if not _try_paper_admission_mutex(conn):
                return "paper_admission_busy"
            # Preliminary reads above were not protected by GLOBAL; a
            # concurrent worker might have opened a position or changed pause.
            if conn.execute(
                "SELECT 1 FROM paper_trades WHERE state='open' LIMIT 1"
            ).fetchone():
                return "paper_position_already_open"
            control=conn.execute(
                "SELECT enabled,paused FROM bot_control WHERE singleton=true"
            ).fetchone()
            if not control or not control["enabled"] or control["paused"]:
                return "paused"
            version,metadata=rule_identity()
            # Serialize same symbol/tf/bar/version AFTER global mutex.
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                         (f"paper_signal:{settings.symbol}:1:{last.isoformat()}:{version}",))
            exists=conn.execute("""SELECT decision FROM strategy_signals WHERE
               symbol=%s AND tf='1' AND bar_ts=%s AND version=%s""",
               (settings.symbol,last,version)).fetchone()
            if exists and exists["decision"]!=_RETRYABLE_PROOF_DECISION:
                return "already_evaluated"
            # A harmless no-signal diagnostic may be recorded for an older
            # closed bar. A POSITIVE signal must NEVER be admitted or confirmed
            # after its original 90s window, even if later proof arrives.
            signal=price_signal(bars,settings.taker_fee_rate,settings.slippage_bps)
            payload=_decision_payload(signal,version,metadata)
            if signal is None:
                decision="no_signal"
            elif not _paper_window(last,now):
                decision="expired_missing_proof"
                payload["admission_error"]="original_90s_window_expired"
            else:
                try:
                    require_new_strategy_identity(signal.reason)
                except ValueError:
                    decision="rejected_strategy_identity"
                else:
                    proof_ok,flow=confirm_rejection(
                        conn,settings,signal,bars[-1],observed_at=now)
                    signal.reason["orderflow"]=flow
                    if not proof_ok:
                        # Only D's explicit missing B publication is retryable.
                        if (isinstance(flow,dict)
                                and flow.get("reason")=="pending_bounded_live_evidence"
                                and flow.get("retryable") is True):
                            decision=_RETRYABLE_PROOF_DECISION
                            payload["proof_pending_reason"]=flow["reason"]
                        else:
                            decision="rejected_orderflow"
                            payload["proof_rejected_reason"]=(
                                str(flow.get("reason","invalid_orderflow"))[:150]
                                if isinstance(flow,dict) else "invalid_orderflow")
                    else:
                        # Use a server decision clock, never the trade print or
                        # retrospectively computed snapshot publication time.
                        decision_at=conn.execute(
                            "SELECT clock_timestamp() AS ts").fetchone()["ts"]
                        if not _paper_window(last,decision_at):
                            decision="expired_missing_proof"
                        elif recent["ts"]>decision_at or (
                            decision_at-recent["ts"]>timedelta(seconds=15)):
                            decision="rejected_stale_trade_print"
                        elif abs(Decimal(recent["price"])/Decimal(bars[-1]["close"])-1)>Decimal("0.005"):
                            decision="rejected_signal_expired_price_gap"
                        else:
                            try:
                                orderflow=signal.reason["orderflow"]
                                if "immutable_live_evidence" in orderflow:
                                    from .simulation.live_evidence import bind_live_paper_reference
                                    bound=bind_live_paper_reference(
                                        signal.reason,decision_at=decision_at,bar_ts=last)
                                else:
                                    bound=bind_snapshot_reference(
                                        signal.reason,decision_at=decision_at,bar_ts=last)
                                bound["market_trade_ts"]=recent["ts"].isoformat()
                                # A failed paper insert/event must roll back ALL
                                # admission writes before terminal diagnostics commit.
                                with conn.transaction():
                                    identifier=open_paper(
                                        settings,signal.direction,recent["price"],signal.stop,
                                        signal.take,bound,observed_at=decision_at,conn=conn)
                                decision="opened"
                                opened_id=identifier
                                payload.update(bound)
                                payload["paper_trade_id"]=identifier
                            except (ValueError,RuntimeError) as exc:
                                # Admission rejects atomically: no orphan candidate.
                                decision="rejected_paper_admission"
                                payload["admission_error"]=str(exc)[:250]
            if signal:
                payload.update(signal.reason)
            _persist_paper_decision(conn,settings,last,version,decision,payload)
            if opened_id is None:
                return decision
    if trade:
        identifier=close_paper(settings,recent["price"],reason,funding,recent["ts"],
                               trade["id"],funding_evidence=funding_evidence,
                               require_live_crossing=True)
        if identifier:
            try:
                from .charts import render_trade
                render_trade(settings,identifier)
            except Exception as exc:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO bot_events(severity,event,data)
                      VALUES('warning','exit_chart_failed',%s::jsonb)""",
                      (json.dumps({"trade":identifier,"error":str(exc)[:400]}),))
        return "closed_"+str(identifier)
    try:
        from .charts import render_trade
        render_trade(settings,opened_id)
    except Exception as exc:
        with connection(settings) as conn:
            conn.execute("""INSERT INTO bot_events(severity,event,data)
              VALUES('warning','entry_chart_failed',%s::jsonb)""",
              (json.dumps({"trade":opened_id,"error":str(exc)[:400]}),))
    return "opened_"+str(opened_id)


async def run_engine(settings):
    """24/7 paper loop: intermittent DB failures cannot terminate monitoring.

    Research reviews are restart-safe and cannot change simulated settlements.
    """
    while True:
        try:
            state=await asyncio.to_thread(engine_tick,settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state="engine_error"
            LOGGER.error("Paper simulation tick failed (%s)",type(exc).__name__)
            try:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO bot_events(severity,event,data)
                      VALUES('error','engine_failure',%s::jsonb)""",
                      (json.dumps({"error_type":type(exc).__name__,
                                   "paper_only":True}),))
            except Exception:
                LOGGER.warning("Engine event DB unavailable; next cycle retries")
        try:
            with connection(settings) as conn:
                # Persisted exposure, NOT a status-name heuristic. A position
                # blocked on a data gap is still open and genuinely at risk.
                exposed=conn.execute("""SELECT 1 FROM paper_trades
                    WHERE state='open' AND symbol=%s LIMIT 1""",
                    (settings.symbol,)).fetchone() is not None
                old=conn.execute("""SELECT status FROM bot_heartbeat
                    WHERE component='paper_engine'""").fetchone()
                previous=old["status"] if old else None
                conn.execute("""INSERT INTO bot_heartbeat(component,status,detail)
                    VALUES('paper_engine',%s,%s::jsonb)
                    ON CONFLICT(component) DO UPDATE SET
                    status=EXCLUDED.status,detail=EXCLUDED.detail,
                    updated_at=clock_timestamp()""",
                    (state[:250],json.dumps({"paper_only":True,
                     "open_position_risk":exposed,
                     "automatic_fake_exit":False})))
                if (state!=previous and state in (
                        "waiting_live_feed_open_position",
                        "blocked_unresolved_tick_gap",
                        "engine_error")):
                    conn.execute("""INSERT INTO bot_events(severity,event,data)
                      VALUES('warning','paper_source_risk',%s::jsonb)""",
                      (json.dumps({"status":state,"symbol":settings.symbol,
                                   "paper_only":True,
                                   "open_position_risk":exposed,
                                   "source_completeness_claimed":False}),))
        except Exception:
            LOGGER.warning("Paper heartbeat DB unavailable; next cycle retries")
        try:
            await asyncio.to_thread(review_one_closed_trade,settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.warning("Paper trade review failed; next cycle retries")
        await asyncio.sleep(15)
