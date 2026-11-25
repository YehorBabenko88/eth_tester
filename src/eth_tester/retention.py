"""Safe pruning: no unique market data is ever silently erased."""
from .database import connection

def storage_report(settings):
    with connection(settings) as conn:
        size=conn.execute("SELECT pg_database_size(current_database()) AS bytes").fetchone()
        rows=conn.execute("""SELECT (SELECT COUNT(*) FROM trades) trades,
          (SELECT COUNT(*) FROM candles) candles,
          (SELECT COUNT(*) FROM book_events) book_events""").fetchone()
        return {"bytes":size["bytes"],**rows}

def cleanup_diagnostics(settings):
    with connection(settings) as conn:
        health=conn.execute("DELETE FROM feed_health WHERE ts<now()-interval '90 days'").rowcount
        logs=conn.execute("DELETE FROM bot_events WHERE ts<now()-interval '180 days'").rowcount
    return {"feed_health_deleted":health,"bot_events_deleted":logs,
            "raw_market_data_deleted":0}
