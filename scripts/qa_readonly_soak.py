"""Read-only, bounded ETH Tester QA soak sampler for DISPOSABLE PostgreSQL.

Example:
  python scripts/qa_readonly_soak.py --dsn "$ETH_TEST_PG_DSN" --seconds 3600
  python scripts/qa_readonly_soak.py --dsn "$ETH_TEST_PG_DSN" --seconds 86400

Every stdout line is JSON. No Telegram credentials, private Bybit endpoints,
DDL, mutations, installer actions, orders or user-database operations.
A successful sample NEVER constitutes full-service production GO.
"""
import argparse
from datetime import datetime, timezone
import json
import time

import psycopg
from psycopg.conninfo import conninfo_to_dict


def assert_disposable_target(dsn):
    if not isinstance(dsn, str) or not dsn:
        raise ValueError("Missing disposable database DSN")
    info = conninfo_to_dict(dsn)
    name = info.get("dbname", "")
    if not (name.startswith("eth_tester_qa_") or name.startswith("eth_tester_h8_")):
        raise ValueError("Refusing non-disposable database for soak sampling")
    return name


def sample(conn):
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        now = conn.execute("SELECT clock_timestamp() AS now").fetchone()[0]
        health = conn.execute(
            """SELECT component,status,updated_at FROM bot_heartbeat
               ORDER BY component LIMIT 30"""
        ).fetchall()
        limits = conn.execute(
            """SELECT
                 (SELECT max(ts) FROM trades WHERE symbol='ETHUSDT'
                  AND source='websocket') AS latest_ws_event,
                 (SELECT max(computed_at) FROM paper_live_evidence
                  WHERE symbol='ETHUSDT') AS latest_paper_witness,
                 (SELECT count(*) FROM paper_trades WHERE state='open') AS open_paper,
                 (SELECT count(*) FROM data_gaps WHERE symbol='ETHUSDT'
                   AND source IN ('publicTrade','orderbook')
                   AND resolved_at IS NULL) AS unresolved_stream_gaps"""
        ).fetchone()
    def iso(x):
        return x.isoformat() if isinstance(x, datetime) else x
    return {
        "observed_at": iso(now),
        "quality": "diagnostic_only_not_source_certification",
        "production_go": False, "real_orders_allowed": False,
        "workers": [
            {"component": name, "status": status, "updated_at": iso(updated)}
            for name, status, updated in health
        ],
        "latest_ws_event": iso(limits[0]),
        "latest_paper_witness": iso(limits[1]),
        "open_paper_positions": limits[2],
        "unresolved_stream_gaps": limits[3],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args(argv)
    assert_disposable_target(args.dsn)
    if not 1 <= args.seconds <= 7 * 86400:
        parser.error("seconds must be between 1 and 604800")
    if not 1 <= args.interval <= 3600:
        parser.error("interval must be between 1 and 3600")
    deadline = time.monotonic() + args.seconds
    with psycopg.connect(args.dsn, autocommit=True, connect_timeout=10) as conn:
        while True:
            try:
                result = sample(conn)
            except psycopg.Error as exc:
                result = {
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "quality": "sampling_degraded", "production_go": False,
                    "error_type": type(exc).__name__,
                }
                try:
                    conn.close()
                    conn = psycopg.connect(
                        args.dsn, autocommit=True, connect_timeout=10
                    )
                except psycopg.Error:
                    # The next bounded probe will retry without pretending
                    # that market/DB processing remained healthy.
                    pass
            print(json.dumps(result, sort_keys=True, default=str), flush=True)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(args.interval, remaining))


if __name__ == "__main__":
    main()
