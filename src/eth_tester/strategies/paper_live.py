"""Immutable bounded-live proof reader for paper simulation only.

B Storage validates its independent WS/candle/L2/OI source receipts through
require_bounded_paper_evidence. D never trusts caller booleans, makes an
exchange-completeness assertion, or borrows this tier for historical ML.
"""
from datetime import datetime, timedelta
from re import fullmatch


CONTRACT = "eth_paper_live_evidence_v1"
QUALITY = "live_observed_bounded"
_HEX64 = r"[0-9a-f]{64}"


def _utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if (not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() != timedelta(0)):
        raise ValueError("non-UTC or missing source timestamp")
    return value


def _same(left, right):
    return _utc(left) == _utc(right)


def read_live_evidence(conn, symbol, start, end, decision_at):
    """Return only B-verified, paper-only evidence available by actual decision.

    A missing migration, row, missing independent source, gap, stale receipt,
    late publication or malformed helper result returns None. E can retry only
    within this same original UTC close+90s window, never after the deadline.
    """
    try:
        start, end, decision_at = map(_utc, (start, end, decision_at))
        if (symbol != "ETHUSDT" or end - start != timedelta(minutes=1)
                or not end <= decision_at <= end + timedelta(seconds=90)):
            return None
        try:
            from ..storage.live_evidence import require_bounded_paper_evidence
        except ImportError:
            return None
        present = conn.execute(
            "SELECT to_regclass('public.paper_live_evidence') AS present"
        ).fetchone()
        if not present or present.get("present") is None:
            return None
        row = conn.execute("""SELECT snapshot_key,source_checksum,symbol,tf,
             bar_ts,observed_at,computed_at,expires_at,quality,paper_only,
             ml_eligible,evidence FROM paper_live_evidence
             WHERE symbol=%s AND tf='1' AND bar_ts=%s AND computed_at<=%s
             ORDER BY computed_at DESC,snapshot_key DESC LIMIT 1""",
             (symbol, start, decision_at)).fetchone()
        if not isinstance(row, dict):
            return None
        key, digest = row.get("snapshot_key"), row.get("source_checksum")
        if (not isinstance(key, str) or fullmatch(_HEX64, key) is None
                or not isinstance(digest, str)
                or fullmatch(_HEX64, digest) is None
                or row.get("symbol") != symbol or row.get("tf") != "1"
                or not _same(row["bar_ts"], start)
                or row.get("quality") != QUALITY
                or row.get("paper_only") is not True
                or row.get("ml_eligible") is not False):
            return None
        observed = _utc(row["observed_at"])
        computed = _utc(row["computed_at"])
        expires = _utc(row["expires_at"])
        if (not end <= observed == computed <= decision_at <= expires
                or expires != end + timedelta(seconds=90)):
            return None
        evidence = row["evidence"]
        if (not isinstance(evidence, dict)
                or evidence.get("contract") != CONTRACT
                or evidence.get("exchange_complete") is True
                or evidence.get("is_ml_certified") is True):
            return None
        # This B-owned function re-reads immutable original-source witnesses.
        # Never replace it with a caller-supplied complete/verified flag.
        checked = require_bounded_paper_evidence(
            conn, symbol=symbol, bar_ts=start,
            evidence_key=key, decision_at=decision_at)
        if (not isinstance(checked, dict)
                or checked.get("snapshot_key") != key
                or checked.get("source_checksum") != digest
                or checked.get("contract") != CONTRACT
                or checked.get("quality") != QUALITY
                or checked.get("paper_only") is not True
                or checked.get("ml_eligible") is not False
                or not _same(checked.get("bar_ts"), start)
                or not _same(checked.get("observed_at"), observed)
                or not _same(checked.get("computed_at"), computed)
                or not _same(checked.get("expires_at"), expires)):
            return None
        return {
            "snapshot_key": key, "source_checksum": digest,
            "contract": CONTRACT, "quality": QUALITY,
            "bar_ts": start.isoformat(), "observed_at": observed.isoformat(),
            "computed_at": computed.isoformat(),
            "expires_at": expires.isoformat(),
            "paper_only": True, "ml_eligible": False,
        }
    except (AttributeError, KeyError, TypeError, ValueError,
            RuntimeError, OverflowError):
        return None
