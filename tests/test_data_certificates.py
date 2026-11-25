from datetime import datetime, timedelta, timezone
from decimal import Decimal

from eth_tester.archive_quality import build_minute_evidence, summarize_minutes
from eth_tester.data_certificates import certified_candle_coverage

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class Cursor:
    def __init__(self, rows=None, single=None):
        self.rows, self.single = rows or [], single

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.single


class Database:
    def __init__(self, windows, gap=None):
        self.windows, self.gap = windows, gap

    def execute(self, sql, params):
        return Cursor(self.windows) if "FROM ingestion_coverage" in sql else Cursor(
            single=self.gap)


def row(begin, end, checked):
    return {
        "start_ts": T0+timedelta(minutes=begin),
        "end_ts": T0+timedelta(minutes=end),
        "status": "verified", "checksum": "a"*64,
        "quality": "exchange_closed_grid_checked",
        "checked_at": T0+timedelta(minutes=checked),
    }


def test_certificate_is_strictly_observed_not_backdated():
    db = Database([row(0, 1, 3), row(1, 2, 4)])
    early = certified_candle_coverage(
        db, "ETHUSDT", "1", T0, T0+timedelta(minutes=2),
        T0+timedelta(minutes=2))
    assert early is None
    later = certified_candle_coverage(
        db, "ETHUSDT", "1", T0, T0+timedelta(minutes=2),
        T0+timedelta(minutes=5))
    assert later["observed_at"] == (T0+timedelta(minutes=4)).isoformat()
    assert later["certifies_trade_prints"] is False


def test_certificate_rejects_gaps_and_incomplete_receipts():
    start,end,decision = T0, T0+timedelta(minutes=2), T0+timedelta(minutes=5)
    assert certified_candle_coverage(
        Database([row(0, 1, 3)]), "ETHUSDT", "1", start,end,decision) is None
    assert certified_candle_coverage(
        Database([row(0, 2, 3)], gap={"id":42}), "ETHUSDT", "1",
        start,end,decision) is None
    unverified = row(0,2,3)
    unverified["status"] = "partial"
    assert certified_candle_coverage(
        Database([unverified]), "ETHUSDT", "1", start,end,decision) is None


def test_certificate_ties_hash_to_immutable_window():
    one = certified_candle_coverage(
        Database([row(0,2,3)]), "ETHUSDT", "1", T0,
        T0+timedelta(minutes=2), T0+timedelta(minutes=5))
    next_row = row(0,2,3)
    next_row["checksum"] = "b"*64
    two = certified_candle_coverage(
        Database([next_row]), "ETHUSDT", "1", T0,
        T0+timedelta(minutes=2), T0+timedelta(minutes=5))
    assert one["source_sha256"] != two["source_sha256"]


def test_archive_only_tape_does_not_allow_quote_mismatch():
    day = T0.date()
    bars = [{"ts": T0+timedelta(minutes=i), "volume": Decimal(1),
             "turnover": Decimal(100)} for i in range(1440)]
    tape = [{"ts": T0+timedelta(minutes=i), "trade_count": 1,
             "base_qty": Decimal(1), "quote_qty": Decimal(100)}
            for i in range(1440)]
    assert summarize_minutes(build_minute_evidence(day,bars,tape))["status"] == "verified"
    tape[12]["quote_qty"] = Decimal(99)
    assert summarize_minutes(build_minute_evidence(day,bars,tape))["status"] == "volume_mismatch"

def test_candle_certificate_separates_exchange_event_and_observation():
    start = T0
    end = T0 + timedelta(minutes=2)
    receipt = T0 + timedelta(minutes=5)
    result = certified_candle_coverage(
        Database([row(0, 2, 3)]), "ETHUSDT", "1", start, end, receipt)
    assert result["source_event_start_ms"] == int(start.timestamp() * 1000)
    assert result["source_event_end_exclusive_ms"] == int(end.timestamp() * 1000)
    assert result["source_receipt_ms"] == int(
        (T0 + timedelta(minutes=3)).timestamp() * 1000)
    assert result["source_checked_at_ms"] == result["feature_available_ms"]
    assert result["source_receipt_ms"] > result["source_event_end_exclusive_ms"]
    assert result["coverage_segments"][0]["checksum"] == "a" * 64
    assert result["certifies_trade_prints"] is False


def test_candle_certificate_rejects_noncanonical_source_sha():
    bad = row(0, 2, 3)
    bad["checksum"] = "NOT_A_SHA"
    assert certified_candle_coverage(
        Database([bad]), "ETHUSDT", "1", T0,
        T0 + timedelta(minutes=2), T0 + timedelta(minutes=5)) is None


def test_coverage_receipt_uses_actual_postgres_clock_not_transaction_start():
    from eth_tester.data_coverage import write_coverage

    class Recorder:
        statement = None

        def execute(self, statement, params):
            self.statement = statement

    conn = Recorder()
    write_coverage(
        conn, source="kline:trade", symbol="ETHUSDT", interval="1",
        start_ms=int(T0.timestamp() * 1000),
        end_exclusive_ms=int((T0 + timedelta(minutes=1)).timestamp() * 1000),
        status="partial", observed_rows=1)
    assert conn.statement.count("clock_timestamp()") == 2
