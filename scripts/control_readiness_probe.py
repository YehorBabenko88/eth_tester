"""Read-only ETH Tester runtime evidence probe. NOT a trading acceptance gate.

The PostgreSQL session sets default_transaction_read_only=on. This does not
contact Bybit, issue exchange orders, create labels or certify source quality.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

UTC = timezone.utc


def utc_age_seconds(now, stamp):
    if not isinstance(stamp, datetime) or stamp.tzinfo is None:
        return None
    delta = (now - stamp.astimezone(UTC)).total_seconds()
    if delta < -2:
        return None
    return max(0.0, delta)


def assess(observations, *, now):
    """Report independent evidence shortages, never return production GO."""
    observations = dict(observations)
    reasons = []
    age = {}
    limits = {"last_trade": 15, "last_candle": 240,
              "analytics_heartbeat": 420, "engine_heartbeat": 60}
    for key, maximum in limits.items():
        age[key] = utc_age_seconds(now, observations.get(key))
        if age[key] is None or age[key] > maximum:
            reasons.append(f"{key}_missing_or_stale")
    if observations.get("feed_status") != "connected":
        reasons.append("feed_not_connected")
    if observations.get("unresolved_trade_gaps") is None or observations["unresolved_trade_gaps"] > 0:
        reasons.append("trade_gap_not_cleared")
    if (observations.get("certified_snapshot_count_24h") or 0) == 0:
        reasons.append("no_verified_immutable_snapshot_v2")
    if (observations.get("post_horizon_label_count") or 0) == 0:
        reasons.append("no_issued_post_horizon_labels")
    if (observations.get("shadow_model_count") or 0) == 0:
        reasons.append("no_registered_shadow_model")
    return {
        "paper_only": True,
        "production_go": False,
        "status": "insufficient_evidence" if reasons else "needs_independent_runtime_acceptance",
        "reasons": sorted(set(reasons)),
        "ages_s": age,
        "observed_at": now.astimezone(UTC).isoformat(),
        "metrics": {k: v for k, v in observations.items() if not isinstance(v, datetime)},
    }


QUERIES = {
    "last_trade": ("SELECT max(ts) AS value FROM trades "
                   "WHERE symbol=%s AND source='websocket'", True),
    "last_candle": ("SELECT max(ts) AS value FROM candles "
                    "WHERE symbol=%s AND kind='trade' AND tf='1' AND closed=true", True),
    "analytics_heartbeat": ("SELECT max(updated_at) AS value FROM bot_heartbeat "
                            "WHERE component='analytics'", False),
    "engine_heartbeat": ("SELECT max(updated_at) AS value FROM bot_heartbeat "
                         "WHERE component='paper_engine'", False),
    "feed_status": ("SELECT status AS value FROM feed_health WHERE symbol=%s "
                    "AND topic='connection' ORDER BY ts DESC LIMIT 1", True),
    "unresolved_trade_gaps": ("SELECT count(*) AS value FROM data_gaps WHERE symbol=%s "
                              "AND source='publicTrade' AND resolved_at IS NULL", True),
    "certified_snapshot_count_24h": (
        "SELECT count(*) AS value FROM feature_snapshots WHERE symbol=%s "
        "AND quality='complete' AND is_causal=true "
        "AND provenance->>'contract'='eth_feature_snapshot_v2' "
        "AND provenance->>'source_coverage_verified'='true' "
        "AND computed_at > clock_timestamp() - interval '24 hours'", True),
    "post_horizon_label_count": (
        "SELECT count(*) AS value FROM paper_ml_labels l "
        "JOIN paper_trades p ON p.id=l.paper_trade_id WHERE p.symbol=%s", True),
    "shadow_model_count": (
        "SELECT count(*) AS value FROM model_versions "
        "WHERE model_name='causal_softmax3' AND approved=false", False),
}


def collect(dsn, symbol):
    import psycopg
    from psycopg.rows import dict_row

    readings = {}
    with psycopg.connect(dsn, row_factory=dict_row,
                         options='-c default_transaction_read_only=on',
                         connect_timeout=10) as conn:
        for key, (query, needs_symbol) in QUERIES.items():
            row = conn.execute(query, (symbol,) if needs_symbol else ()).fetchone()
            readings[key] = row["value"] if row is not None else None
    return readings


def main():
    parser = argparse.ArgumentParser(description="Read-only ETH Tester runtime telemetry")
    parser.add_argument("--output", type=Path, default=None,
                        help="Write local JSON diagnostic only (not user DB)")
    args = parser.parse_args()
    from eth_tester.config import Settings
    settings = Settings()
    result = assess(collect(settings.database_url, settings.symbol),
                    now=datetime.now(UTC))
    value = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2)
    if args.output:
        args.output.write_text(value + "\n", encoding="utf-8")
    else:
        print(value)


if __name__ == "__main__":
    main()
