"""Append-only, post-horizon paper ML structural labels.

Labels are never inferred from paper PnL and never inserted at entry time.
The PostgreSQL trigger independently checks bound snapshot identity, closed
trade, verified source coverage and real issue time before accepting a row.
"""
from datetime import datetime
import re

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_OUTCOMES = frozenset(("breakout", "bounce", "uncertain"))


def _aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware label evidence required")
    return value


def record_paper_ml_label(
    conn, *, paper_trade_id, snapshot_key, source_checksum, outcome,
    horizon_end_at, label_source, label_interval, covered_from_at,
    coverage_sha256, source_receipt_at, coverage_checked_at,
    source_evidence, label_instrument="linear",
):
    """Record exactly one independently evidenced label per closed paper trade.

    Call from a separate post-horizon job after independent source checks,
    never from a paper entry or a PnL-derived classifier.
    """
    if type(paper_trade_id) is not int or paper_trade_id <= 0:
        raise ValueError("Invalid paper trade identity")
    if not isinstance(snapshot_key, str) or not snapshot_key:
        raise ValueError("Immutable snapshot key required")
    if any(not isinstance(item, str) or not _HEX64.fullmatch(item)
           for item in (source_checksum, coverage_sha256)):
        raise ValueError("Pinned source SHA256 values required")
    if outcome not in _OUTCOMES:
        raise ValueError("Independent structural outcome required")
    if (not isinstance(label_source, str) or not label_source
            or not isinstance(label_interval, str)
            or not isinstance(label_instrument, str) or not label_instrument):
        raise ValueError("Label source and interval required")
    if not isinstance(source_evidence, dict) or not _HEX64.fullmatch(
        str(source_evidence.get("barrier_evidence_sha256", ""))
    ):
        raise ValueError("Independent barrier evidence digest required")
    for when in (horizon_end_at, covered_from_at, source_receipt_at,
                 coverage_checked_at):
        _aware(when)
    if (covered_from_at >= horizon_end_at or
            source_receipt_at > coverage_checked_at):
        raise ValueError("Invalid horizon or provenance order")
    from json import dumps

    fields = (
        paper_trade_id, snapshot_key, source_checksum,
        "fixed_horizon_barrier_v1", outcome, horizon_end_at,
        label_source, label_interval, label_instrument, covered_from_at,
        coverage_sha256, source_receipt_at, coverage_checked_at,
        dumps(source_evidence, sort_keys=True),
    )
    row = conn.execute(
        """INSERT INTO paper_ml_labels(
           paper_trade_id,snapshot_key,source_checksum,method,outcome,
           horizon_end_at,label_source,label_interval,label_instrument,
           covered_from_at,coverage_sha256,source_receipt_at,
           coverage_checked_at,source_evidence)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
           ON CONFLICT(paper_trade_id) DO NOTHING RETURNING paper_trade_id""",
        fields,
    ).fetchone()
    if row is None:
        old = conn.execute(
            """SELECT snapshot_key,source_checksum,method,outcome,
                      horizon_end_at,label_source,label_interval,label_instrument,
                      covered_from_at,coverage_sha256,source_receipt_at,
                      coverage_checked_at,source_evidence
               FROM paper_ml_labels WHERE paper_trade_id=%s""",
            (paper_trade_id,),
        ).fetchone()
        expected = dict(zip((
            "snapshot_key", "source_checksum", "method", "outcome",
            "horizon_end_at", "label_source", "label_interval", "label_instrument",
            "covered_from_at", "coverage_sha256", "source_receipt_at",
            "coverage_checked_at"), fields[1:-1]))
        if old is None or any(old[k] != v for k, v in expected.items()):
            raise ValueError("Conflicting immutable post-horizon label")
        if old["source_evidence"] != source_evidence:
            raise ValueError("Conflicting immutable label provenance")
    return paper_trade_id
