-- B/F: immutable post-horizon structural labels (paper research ONLY).
-- NEVER modify an entry-time paper_trades.reason to insert future knowledge.
CREATE TABLE IF NOT EXISTS paper_ml_labels (
  paper_trade_id bigint PRIMARY KEY REFERENCES paper_trades(id),
  snapshot_key text NOT NULL REFERENCES feature_snapshots(snapshot_key),
  source_checksum text NOT NULL CHECK(source_checksum ~ '^[0-9a-f]{64}$'),
  method text NOT NULL CHECK(method='fixed_horizon_barrier_v1'),
  outcome text NOT NULL CHECK(outcome IN ('breakout','bounce','uncertain')),
  horizon_end_at timestamptz NOT NULL,
  label_source text NOT NULL,
  label_interval text NOT NULL,
  label_instrument text NOT NULL DEFAULT 'linear',
  covered_from_at timestamptz NOT NULL,
  coverage_sha256 text NOT NULL CHECK(coverage_sha256 ~ '^[0-9a-f]{64}$'),
  source_receipt_at timestamptz NOT NULL,
  coverage_checked_at timestamptz NOT NULL,
  source_evidence jsonb NOT NULL,
  issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK(covered_from_at < horizon_end_at),
  CHECK(source_receipt_at <= coverage_checked_at)
);
CREATE INDEX IF NOT EXISTS paper_ml_labels_issued_idx
 ON paper_ml_labels(issued_at DESC);

CREATE OR REPLACE FUNCTION eth_guard_paper_ml_label()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  target record;
  witness record;
  snapshot record;
  issued timestamptz;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION 'paper_ml_labels is append-only';
  END IF;
  -- Lock the closed entry while binding its immutable captured snapshot.
  SELECT state,signal_ts,closed_at,reason INTO target
    FROM paper_trades WHERE id=NEW.paper_trade_id FOR SHARE;
  IF NOT FOUND OR target.state <> 'closed' OR target.closed_at IS NULL
     OR target.reason->>'ml_snapshot_key' IS DISTINCT FROM NEW.snapshot_key
     OR target.reason->>'ml_source_checksum' IS DISTINCT FROM NEW.source_checksum
     OR target.signal_ts < NEW.covered_from_at
     OR target.closed_at > NEW.horizon_end_at THEN
    RAISE EXCEPTION 'label must bind a closed paper entry and its original snapshot';
  END IF;
  SELECT source_checksum,quality,is_causal,available_at,observed_at,
         computed_at,provenance INTO snapshot
    FROM feature_snapshots WHERE snapshot_key=NEW.snapshot_key;
  IF NOT FOUND OR snapshot.source_checksum IS DISTINCT FROM NEW.source_checksum
     OR snapshot.quality <> 'complete' OR snapshot.is_causal IS NOT TRUE
     OR snapshot.provenance->>'contract' <> 'eth_feature_snapshot_v2'
     OR snapshot.provenance->>'source_coverage_verified' <> 'true'
     OR snapshot.available_at > target.signal_ts
     OR snapshot.observed_at > target.signal_ts
     OR snapshot.computed_at > target.signal_ts THEN
    RAISE EXCEPTION 'post-horizon label cannot bind an unverified or future snapshot';
  END IF;
  -- PostgreSQL clock is authoritative; client-provided issued_at is ignored.
  issued := clock_timestamp();
  NEW.issued_at := issued;
  IF NEW.horizon_end_at > issued OR NEW.coverage_checked_at > issued
     OR NEW.coverage_checked_at < NEW.horizon_end_at
     OR NEW.source_receipt_at > issued THEN
    RAISE EXCEPTION 'post-horizon label evidence is not yet available';
  END IF;
  SELECT start_ts,end_ts,status,checksum,checked_at,evidence INTO witness
    FROM ingestion_coverage
    WHERE source=NEW.label_source
      AND instrument=NEW.label_instrument
      AND symbol=(SELECT symbol FROM paper_trades WHERE id=NEW.paper_trade_id)
      AND interval=NEW.label_interval
      AND status='verified'
      AND checksum=NEW.coverage_sha256
      AND start_ts<=NEW.covered_from_at
      AND end_ts>=NEW.horizon_end_at
      AND checked_at=NEW.coverage_checked_at
    ORDER BY start_ts DESC LIMIT 1;
  IF NOT FOUND OR (witness.evidence->>'source_receipt_ms') IS DISTINCT FROM
                  ((extract(epoch FROM NEW.source_receipt_at)*1000)::bigint)::text THEN
    RAISE EXCEPTION 'label coverage has no pinned independent receipt';
  END IF;
  IF NOT (NEW.source_evidence ? 'barrier_evidence_sha256')
     OR NEW.source_evidence->>'barrier_evidence_sha256' !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'missing structural barrier evidence digest';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS eth_paper_ml_label_guard ON paper_ml_labels;
CREATE TRIGGER eth_paper_ml_label_guard
  BEFORE INSERT OR UPDATE OR DELETE ON paper_ml_labels
  FOR EACH ROW EXECUTE FUNCTION eth_guard_paper_ml_label();
