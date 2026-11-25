from contextlib import contextmanager
from importlib.resources import files
import psycopg
from psycopg.rows import dict_row
from .config import Settings

@contextmanager
def connection(settings: Settings):
    with psycopg.connect(settings.database_url, row_factory=dict_row, connect_timeout=10) as conn:
        yield conn

def init_database(settings: Settings):
    with connection(settings) as conn:
        conn.execute(files("eth_tester").joinpath("schema.sql").read_text())
        conn.execute("INSERT INTO paper_account(id,equity,initial_equity) VALUES(1,%s,%s) ON CONFLICT DO NOTHING",
                     (settings.initial_equity_usd, settings.initial_equity_usd))

def cursor_ts(conn, source, symbol, interval):
    row = conn.execute("SELECT cursor_ts FROM ingestion_cursor WHERE source=%s AND symbol=%s AND interval=%s",
                       (source,symbol,interval)).fetchone()
    return row["cursor_ts"] if row else None

def save_cursor(conn, source, symbol, interval, stamp, status="complete"):
    conn.execute("""INSERT INTO ingestion_cursor(source,symbol,interval,cursor_ts,last_success,status)
      VALUES(%s,%s,%s,%s,now(),%s) ON CONFLICT(source,symbol,instrument,interval)
      DO UPDATE SET cursor_ts=EXCLUDED.cursor_ts,last_success=EXCLUDED.last_success,status=EXCLUDED.status""",
      (source,symbol,interval,stamp,status))
