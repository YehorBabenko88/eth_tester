"""Incremental observed-trade CVD, with corrections rewinding only the suffix.

The analytics_cursor record is protected by a transaction advisory lock.
Never label CVD as exchange-complete without independent trade coverage.
"""
import json
from decimal import Decimal

from .database import connection
from .rollups import TIMEFRAMES


def update_cvd(settings, tf="1", batch=5000):
    if tf not in TIMEFRAMES or batch < 1:
        raise ValueError("Unsupported timeframe or batch")
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"cvd:{settings.symbol}:{tf}",))
        row = conn.execute("""SELECT cursor_ts FROM analytics_cursor
            WHERE processor='cvd' AND symbol=%s AND tf=%s FOR UPDATE""",
            (settings.symbol, tf)).fetchone()
        since = row["cursor_ts"] if row else None
        if since is None:
            cvd = Decimal(0)
        else:
            seed = conn.execute("""SELECT cvd FROM features
                WHERE symbol=%s AND tf=%s AND ts=%s""",
                (settings.symbol, tf, since)).fetchone()
            # Any missing or null seed is a corrupted checkpoint. Roll back.
            if seed is None or seed["cvd"] is None:
                raise RuntimeError(f"Missing CVD checkpoint for {settings.symbol}/{tf}")
            cvd = Decimal(seed["cvd"])
        rows = conn.execute("""SELECT ts,delta FROM features
            WHERE symbol=%s AND tf=%s AND delta IS NOT NULL
              AND (%s::timestamptz IS NULL OR ts>%s)
            ORDER BY ts LIMIT %s""",
            (settings.symbol, tf, since, since, batch)).fetchall()
        for item in rows:
            cvd += Decimal(item["delta"])
            meta = json.dumps({"cvd_baseline": "first_observed_bar",
                               "cvd_scope": "observed_trades_only",
                               "cvd_completeness": "not_asserted"})
            conn.execute("""UPDATE features SET cvd=%s,
                metadata=features.metadata || %s::jsonb
                WHERE symbol=%s AND tf=%s AND ts=%s""",
                (cvd, meta, settings.symbol, tf, item["ts"]))
        if rows:
            conn.execute("""INSERT INTO analytics_cursor(
                processor,symbol,tf,cursor_ts,quality,state,details)
                VALUES('cvd',%s,%s,%s,'observed_trades_only','complete',
                       %s::jsonb)
                ON CONFLICT(processor,symbol,tf)
                DO UPDATE SET cursor_ts=EXCLUDED.cursor_ts,quality=EXCLUDED.quality,
                    state=EXCLUDED.state,details=EXCLUDED.details,updated_at=now()""",
                (settings.symbol, tf, rows[-1]["ts"],
                 json.dumps({"baseline": "first_observed_bar",
                             "completeness": "not_asserted"})))
        return len(rows)
