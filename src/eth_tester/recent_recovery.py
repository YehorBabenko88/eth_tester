"""Bounded Bybit REST recent-public-trade recovery during websocket interruptions.

Recent REST is a rolling, capped snapshot: matching prices/volumes or inserting
1000 rows is NEVER proof that an outage was fully recovered. Leave publicTrade
gaps unresolved until archive/day-volume verification succeeds.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

from .data_coverage import digest_rows, write_coverage
from .data_quality import DataIntegrityError
from .database import connection
from .storage.bulk import copy_trades

UTC = timezone.utc


def normalize_recent_trade(row, symbol):
    if row.get("symbol") != symbol:
        raise DataIntegrityError("Recent public trade symbol mismatch")
    identifier = str(row["execId"])
    if not identifier:
        raise DataIntegrityError("Missing REST public trade ID")
    millis = int(row["time"])
    stamp = datetime.fromtimestamp(millis / 1000, UTC)
    price = Decimal(str(row["price"]))
    quantity = Decimal(str(row["size"]))
    side = row["side"]
    if side not in ("Buy", "Sell") or not price.is_finite() or not quantity.is_finite():
        raise DataIntegrityError("Malformed recent public trade payload")
    if price <= 0 or quantity <= 0:
        raise DataIntegrityError("Nonpositive recent public trade price/size")
    if stamp > datetime.now(UTC) + timedelta(seconds=10):
        raise DataIntegrityError("Future public trade timestamp")
    return (symbol, stamp, identifier, price, quantity, side, price*quantity,
            "recent_rest", json.dumps(row, sort_keys=True))


def _recent_unresolved_gaps(conn, symbol, cutoff):
    return conn.execute(
        """SELECT id,start_ts,end_ts FROM data_gaps
           WHERE symbol=%s AND source='publicTrade' AND resolved_at IS NULL
             AND (start_ts >= %s OR end_ts >= %s)
           ORDER BY id DESC LIMIT 20""",
        (symbol, cutoff, cutoff)).fetchall()


async def recover_recent_public_trades(settings, client):
    """Persist overlapping 1000-print REST tail while retaining gap uncertainty."""
    cutoff = datetime.now(UTC) - timedelta(minutes=15)
    with connection(settings) as conn:
        gaps = _recent_unresolved_gaps(conn, settings.symbol, cutoff)
    if not gaps:
        return {"status": "not_needed", "rows": 0}
    data = await client.get(
        "recent-trade", category=settings.category, symbol=settings.symbol, limit=1000)
    rows = data.get("list", [])
    if not isinstance(rows, list):
        raise DataIntegrityError("Invalid Bybit recent public trades page")
    if not rows:
        return {"status": "unavailable", "rows": 0}
    normalized = [normalize_recent_trade(row, settings.symbol) for row in rows]
    earliest = min(row[1] for row in normalized)
    latest = max(row[1] for row in normalized)
    ids = {}
    for row in normalized:
        key = row[2]
        fingerprint = row[1:7]
        if key in ids and ids[key] != fingerprint:
            raise DataIntegrityError(f"Conflicting REST recent trade ID {key}")
        ids[key] = fingerprint
    with connection(settings) as conn:
        merged = copy_trades(conn, normalized)
        if merged["input_rows"] != len(normalized):
            raise DataIntegrityError("Recent REST trade stage row count mismatch")
        write_coverage(
            conn, source="recent_public_trade", symbol=settings.symbol, interval="",
            start_ms=int(earliest.timestamp() * 1000),
            end_exclusive_ms=int(latest.timestamp() * 1000) + 1,
            status="partial", observed_rows=len(ids),
            expected_rows=None, checksum=digest_rows([(key, ids[key]) for key in sorted(ids)]),
            quality="rolling_api_limit_not_full_tape",
            evidence={"method": "recent-trade", "limit": 1000,
                      "returned_rows": len(rows), "page_saturated": len(rows) == 1000,
                      "unresolved_gap_ids": [row["id"] for row in gaps],
                      "cannot_certify_full_coverage": True},
            instrument=settings.category)
    return {"status": "partial", "rows": len(ids),
            "min_utc": earliest.isoformat(), "max_utc": latest.isoformat()}


async def run_recent_recovery(settings, *, interval_seconds=20):
    """Independent small REST loop, never blocks WebSocket message processing."""
    import asyncio
    from .bybit import MarketClient

    async with MarketClient(settings.bybit_rest, settings.rest_rps) as client:
        while True:
            try:
                await recover_recent_public_trades(settings, client)
            except Exception as exc:
                with connection(settings) as conn:
                    conn.execute(
                        """INSERT INTO feed_health(symbol,topic,status,detail)
                           VALUES(%s,'recent_rest_recovery','error',%s::jsonb)""",
                        (settings.symbol, json.dumps({"error": str(exc)[:500]})))
            await asyncio.sleep(interval_seconds)
