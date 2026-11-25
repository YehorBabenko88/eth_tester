"""Transactional PostgreSQL COPY and global trade identity validation.

Compatible with the archive collector's existing temporary eth_stage table.
Never silently overwrite or skip a contradictory (symbol,trade_id) payload.
"""
import json

from psycopg import sql

COLUMNS = ("symbol", "ts", "trade_id", "price", "qty", "side",
           "notional", "source", "raw")
_ALLOWED_STAGING_TABLES = frozenset(("eth_stage", "eth_trade_stage"))


def merge_staged_trades(conn, stage_name="eth_stage"):
    """Validate COPY staging, merge only consistent identities into trades.

    The staging relation must be a session-local PostgreSQL temporary table.
    Returns inserted and deduplicated counts. On exception, caller must
    roll back its enclosing transaction (which also owns the ingestion cursor).
    """
    if stage_name not in _ALLOWED_STAGING_TABLES:
        raise ValueError("Unapproved trade staging table")
    source = sql.SQL("pg_temp.{}").format(sql.Identifier(stage_name))
    with conn.cursor() as cur:
        # pg_temp qualifier prevents accidentally selecting unrelated tables.
        input_rows = cur.execute(
            sql.SQL("SELECT count(*) FROM {}").format(source)
        ).fetchone()[0]
        if input_rows == 0:
            return {"input_rows": 0, "inserted_rows": 0, "deduplicated_rows": 0}
        duplicate = cur.execute(
            sql.SQL(
                "SELECT 1 FROM {} GROUP BY symbol,trade_id "
                "HAVING count(DISTINCT ROW(ts,price,qty,side,notional)) > 1 "
                "LIMIT 1"
            ).format(source)
        ).fetchone()
        if duplicate:
            raise ValueError("Conflicting trade-id payloads within COPY batch")
        conflict = cur.execute(
            sql.SQL(
                "SELECT 1 FROM {} AS s JOIN trades AS t USING(symbol,trade_id) "
                "WHERE ROW(s.ts,s.price,s.qty,s.side,s.notional) "
                "IS DISTINCT FROM ROW(t.ts,t.price,t.qty,t.side,t.notional) "
                "LIMIT 1"
            ).format(source)
        ).fetchone()
        if conflict:
            raise ValueError("Conflicting trade-id payload already exists")
        inserted = cur.execute(
            sql.SQL(
                "INSERT INTO trades "
                "(symbol,ts,trade_id,price,qty,side,notional,source,raw) "
                "SELECT symbol,ts,trade_id,price,qty,side,notional,source,raw "
                "FROM (SELECT DISTINCT ON (symbol,trade_id) * FROM {} "
                "ORDER BY symbol,trade_id,ts) AS unique_trades "
                "ON CONFLICT(symbol,trade_id) DO NOTHING"
            ).format(source)
        ).rowcount
    return {"input_rows": input_rows, "inserted_rows": inserted,
            "deduplicated_rows": input_rows - inserted}


def copy_trades(conn, rows):
    """Stream one bounded batch via COPY; caller controls the transaction."""
    count = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE eth_trade_stage (LIKE trades) ON COMMIT DROP")
        with cur.copy(
            "COPY eth_trade_stage "
            "(symbol,ts,trade_id,price,qty,side,notional,source,raw) FROM STDIN"
        ) as copy:
            for row in rows:
                if len(row) != len(COLUMNS):
                    raise ValueError("Each trade must contain exactly 9 columns")
                values = list(row)
                if isinstance(values[8], (dict, list)):
                    values[8] = json.dumps(values[8])
                copy.write_row(tuple(values))
                count += 1
        result = merge_staged_trades(conn, "eth_trade_stage")
        cur.execute("DROP TABLE eth_trade_stage")
    if result["input_rows"] != count:
        raise RuntimeError("COPY source and staging row counts differ")
    return result
