from contextlib import contextmanager
from importlib.resources import files
import psycopg
from psycopg.rows import dict_row
from psycopg.conninfo import conninfo_to_dict,make_conninfo
from psycopg import sql
from .config import Settings

@contextmanager
def connection(settings: Settings):
    with psycopg.connect(settings.database_url, row_factory=dict_row, connect_timeout=10) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        yield conn

def init_database(settings: Settings):
    params=conninfo_to_dict(settings.database_url)
    name=params.get('dbname','')
    if not (name=='eth_tester' or name.startswith('eth_tester_')):
        raise ValueError('Dedicated eth_tester database required')
    admin_url=make_conninfo(settings.database_url,dbname='postgres')
    with psycopg.connect(admin_url,autocommit=True,connect_timeout=10) as admin:
        if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(name,)).fetchone():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
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
