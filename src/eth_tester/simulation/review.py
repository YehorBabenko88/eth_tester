"""Restart-idempotent, bounded, OBSERVED-ONLY paper trade research reviews.

This is diagnostics for an artificial paper fill, never ML ground truth,
exchange-complete data or a promise of profitability. No new tables needed.
"""
from datetime import timedelta
import json

from .operations import observed_excursions, closed_equity_drawdown
from ..database import connection


def review_one_closed_trade(settings):
    """Review at most one closed paper trade in a short independent transaction.

    The lock + existence check + INSERT commit is atomic across engine workers,
    and a crash before commit permits deterministic replay after restart.
    Failures cannot roll back an already-committed position or paper settlement.
    """
    with connection(settings) as conn:
        # The offline research lane must never starve the live-paper engine.
        # On lock/statement timeout the whole review transaction rolls back
        # and the next supervised cycle retries exactly once.
        conn.execute("SET LOCAL lock_timeout = '500ms'")
        conn.execute("SET LOCAL statement_timeout = '5000ms'")
        locked = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
            (f"eth_paper_trade_review:{settings.symbol}",)
        ).fetchone()
        if not locked or locked["acquired"] is not True:
            return "review_busy"
        trade = conn.execute("""
            SELECT p.* FROM paper_trades p
            WHERE p.symbol=%s AND p.state='closed'
              AND p.opened_at IS NOT NULL AND p.closed_at IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM bot_events e
                WHERE e.event='paper_trade_review_v1'
                  AND e.data->>'trade_id'=p.id::text)
            ORDER BY p.closed_at,p.id LIMIT 1 FOR UPDATE OF p SKIP LOCKED
        """, (settings.symbol,)).fetchone()
        if trade is None:
            return "review_up_to_date"
        duration = trade["closed_at"] - trade["opened_at"]
        covered = (timedelta(0) <= duration <= timedelta(days=1))
        gap = conn.execute("""
            SELECT 1 FROM data_gaps
            WHERE symbol=%s AND source='publicTrade'
              AND start_ts<=%s AND
                  coalesce(end_ts,resolved_at,'infinity'::timestamptz)>=%s
            LIMIT 1
        """, (settings.symbol, trade["closed_at"], trade["opened_at"])).fetchone()
        if not covered or gap:
            excursion = {"quality": "unavailable_gap_or_over_24h_window",
                         "mae_fraction": None, "mfe_fraction": None,
                         "observed_prints": 0}
        else:
            prints = conn.execute("""
                SELECT count(*) AS n,min(price) AS low,max(price) AS high
                FROM trades WHERE symbol=%s AND source='websocket'
                  AND ts >= %s AND ts <= %s
            """, (settings.symbol,trade["opened_at"],trade["closed_at"])).fetchone()
            excursion = observed_excursions(
                trade["direction"],trade["entry"],
                prints["low"],prints["high"],prints["n"])
        fills = conn.execute("""
            SELECT action,quality,fee,spread_cost,slippage_cost
            FROM paper_order_events WHERE paper_trade_id=%s ORDER BY event_ts
        """,(trade["id"],)).fetchall()
        account = conn.execute("""
            SELECT initial_equity,equity FROM paper_account WHERE id=1
        """).fetchone()
        if account is None:
            raise RuntimeError("Missing durable paper account")
        # Bounded oldest-first scan permits a precise closed-trade drawdown.
        # >10k rows => explicit unavailable, never a conveniently truncated peak.
        settled = conn.execute("""
            SELECT net_pnl FROM paper_trades
            WHERE state='closed' AND symbol=%s
            ORDER BY closed_at,id LIMIT 10001
        """,(settings.symbol,)).fetchall()
        drawdown = closed_equity_drawdown(
            account["initial_equity"],[p["net_pnl"] for p in settled])
        report = {
            "trade_id": trade["id"], "symbol": trade["symbol"],
            "version": "observed_paper_review_v1",
            "strategy_version": trade["strategy_version"],
            "direction": trade["direction"],
            "opened_at": trade["opened_at"].isoformat(),
            "closed_at": trade["closed_at"].isoformat(),
            "exit_reason": trade["exit_reason"],
            "paper_only": True, "ml_eligible": False,
            "exchange_fill_proven": False,
            "trade_prints_exchange_complete": False,
            "net_pnl": str(trade["net_pnl"]),
            "gross_pnl": str(trade["gross_pnl"]),
            "fees": str(trade["fee_paid"]),
            "funding": str(trade["funding_paid"]),
            "mae_fraction": (str(excursion["mae_fraction"])
                             if excursion["mae_fraction"] is not None else None),
            "mfe_fraction": (str(excursion["mfe_fraction"])
                             if excursion["mfe_fraction"] is not None else None),
            "observed_prints": excursion["observed_prints"],
            "observed_quality": excursion["quality"],
            "max_realized_drawdown_fraction": (
                str(drawdown["max_drawdown_fraction"])
                if drawdown["max_drawdown_fraction"] is not None else None),
            "drawdown_quality": drawdown["quality"],
            "account_equity_at_review": str(account["equity"]),
            "execution_events": [
                {"action": event["action"], "quality": event["quality"],
                 "fee": str(event["fee"]),
                 "spread_cost": str(event["spread_cost"]),
                 "slippage_cost": str(event["slippage_cost"])}
                for event in fills],
            "entry_chart": trade["entry_chart"],
            "exit_chart": trade["exit_chart"],
            "charts_are_local_paths_not_immutable_file_evidence": True,
        }
        conn.execute("""
            INSERT INTO bot_events(severity,event,data)
            VALUES('info','paper_trade_review_v1',%s::jsonb)
        """,(json.dumps(report,sort_keys=True),))
        return "review_recorded"
