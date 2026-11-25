"""Atomic PostgreSQL COPY ingestion with global trade-id conflict detection.

This preserves the existing trades(symbol, trade_id) global unique key, which
must not be weakened by time partitioning. Feed each call one bounded batch.
"""
import json
from psycopg import sql

COLUMNS = ("symbol", "ts", "trade_id", "price", "qty", "side",
           "notional", "source", "raw")


def copy_trades(conn, rows):
    """COPY a batch, reject conflicting identities, then atomically merge.

    Caller owns the transaction: ingestion_cursor / ingestion_job completion must
    be written in *the same transaction*, after this function succeeds.
    Exactly identical replays are harmless. Differently-valued identical
    (symbol,trade_id) records raise instead of being silently overwritten.
    Raw provenance/source differences are allowed when canonical fields match.
    """
    count = 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE eth_trade_stage (LIKE trades) ON COMMIT DROP")
        with cur.copy(
            "COPY eth_trade_stage (symbol,ts,trade_id,price,qty,side,notional,source,raw) FROM STDIN"
        ) as copy:
            for row in rows:
                if len(row) != len(COLUMNS):
                    raise ValueError("Each trade must contain exactly 9 columns")
                values = list(row)
                if isinstance(values[8], (dict, list)):
                    values[8] = json.dumps(values[8])
                copy.write_row(tuple(values))
                count += 1
        if not count:
            return {"input_rows": 0, "inserted_rows": 0, "deduplicated_rows": 0}

        # Detect contradictory duplicates even *within* an archive page.
        inconsistent = cur.execute(
            """SELECT 1 FROM eth_trade_stage
               GROUP BY symbol,trade_id
               HAVING count(DISTINCT ROW(ts,price,qty,side,notional)) > 1
               LIMIT 1"""
        ).fetchone()
        if inconsistent:
            raise ValueError("Conflicting trade-id payloads within COPY batch")
        inconsistent = cur.execute(
            """SELECT 1
               FROM eth_trade_stage s
               JOIN trades t USING(symbol,trade_id)
               WHERE ROW(s.ts,s.price,s.qty,s.side,s.notional)
                     IS DISTINCT FROM ROW(t.ts,t.price,t.qty,t.side,t.notional)
               LIMIT 1"""
        ).fetchone()
        if inconsistent:
            raise ValueError("Conflicting trade-id payload already exists")
        inserted = cur.execute(
            """INSERT INTO trades
               (symbol,ts,trade_id,price,qty,side,notional,source,raw)
               SELECT symbol,ts,trade_id,price,qty,side,notional,source,raw
               FROM (
                 SELECT DISTINCT ON (symbol,trade_id) *
                 FROM eth_trade_stage ORDER BY symbol,trade_id,ts
               ) deduplicated
               ON CONFLICT(symbol,trade_id) DO NOTHING"""
        ).rowcount
        # Named staging table is explicitly dropped so a second batch may run
        # inside the same outer transaction without a conflicting temp name.
        cur.execute("DROP TABLE eth_trade_stage")
    return {"input_rows": count, "inserted_rows": inserted,
            "deduplicated_rows": count - inserted}
