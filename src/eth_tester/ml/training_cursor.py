"""Read-only watermark retrieval for idempotent, shadow-only retraining."""

def prior_eligible_count(row):
    """A missing/malformed persisted cursor must never authorize retraining."""
    if row is None:
        return 0
    report = row.get("validation")
    if not isinstance(report, dict) or report.get("paper_only") is not True:
        raise ValueError("invalid historical shadow cursor")
    count = report.get("eligible_count")
    if type(count) is not int or not 0 <= count <= 1_000_000:
        raise ValueError("invalid persisted monotonic eligible count")
    return count
