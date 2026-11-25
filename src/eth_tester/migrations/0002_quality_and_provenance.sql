-- Additive contracts requested by Data A, Analytics C and Learning F.
-- Never rewrite historical market-data rows during automatic migration.
CREATE TABLE IF NOT EXISTS ingestion_coverage (
  source text NOT NULL,
  symbol text NOT NULL,
  instrument text NOT NULL DEFAULT 'linear',
  interval text NOT NULL DEFAULT '',
  start_ts timestamptz NOT NULL,
  end_ts timestamptz NOT NULL,
  status text NOT NULL CHECK(status IN ('verified','partial','gap','unavailable','pending')),
  observed_rows bigint NOT NULL DEFAULT 0 CHECK(observed_rows >= 0),
  expected_rows bigint CHECK(expected_rows >= 0),
  checksum text,
  quality text NOT NULL DEFAULT 'unknown',
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(source,symbol,instrument,interval,start_ts,end_ts),
  CHECK(end_ts > start_ts)
);
CREATE INDEX IF NOT EXISTS ingestion_coverage_time_idx
  ON ingestion_coverage(symbol,source,instrument,interval,end_ts DESC);
CREATE INDEX IF NOT EXISTS ingestion_coverage_incomplete_idx
  ON ingestion_coverage(symbol,source,start_ts)
  WHERE status IN ('partial','gap','pending');

CREATE TABLE IF NOT EXISTS trade_candle_volume_checks (
  symbol text NOT NULL,
  instrument text NOT NULL DEFAULT 'linear',
  minute_ts timestamptz NOT NULL,
  archive_source text NOT NULL,
  archive_sha256 text NOT NULL,
  trade_count bigint NOT NULL DEFAULT 0 CHECK(trade_count >= 0),
  trade_base_qty numeric,
  trade_quote_notional numeric,
  candle_base_volume numeric,
  candle_quote_turnover numeric,
  base_difference numeric,
  quote_difference numeric,
  status text NOT NULL CHECK(status IN
    ('match','mismatch','incomplete','not_comparable','pending')),
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol,instrument,minute_ts,archive_source,archive_sha256)
);
CREATE INDEX IF NOT EXISTS trade_candle_volume_checks_status_idx
  ON trade_candle_volume_checks(symbol,instrument,status,minute_ts DESC);

ALTER TABLE levels ADD COLUMN IF NOT EXISTS confirmed_at timestamptz;
ALTER TABLE levels ADD COLUMN IF NOT EXISTS touched_at timestamptz;
ALTER TABLE levels ADD COLUMN IF NOT EXISTS broken_at timestamptz;
ALTER TABLE levels ADD COLUMN IF NOT EXISTS retested_at timestamptz;
ALTER TABLE levels ADD COLUMN IF NOT EXISTS invalidated_at timestamptz;
ALTER TABLE levels ADD COLUMN IF NOT EXISTS last_evidence jsonb
  NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS level_events (
  symbol text NOT NULL,
  tf text NOT NULL,
  level_id bigint NOT NULL REFERENCES levels(id),
  event_type text NOT NULL CHECK(event_type IN (
    'confirmed','touched','broken','retested','invalidated','reopened')),
  event_ts timestamptz NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now(),
  provenance text NOT NULL,
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(symbol,tf,level_id,event_type,event_ts)
);
CREATE INDEX IF NOT EXISTS level_events_time_idx
  ON level_events(symbol,tf,event_ts DESC);
CREATE INDEX IF NOT EXISTS level_events_level_idx
  ON level_events(level_id,event_ts DESC);

CREATE TABLE IF NOT EXISTS analytics_cursor (
  processor text NOT NULL,
  symbol text NOT NULL,
  tf text NOT NULL,
  cursor_ts timestamptz,
  source_checksum text,
  quality text NOT NULL DEFAULT 'pending',
  state text NOT NULL DEFAULT 'pending',
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(processor,symbol,tf)
);

-- Append-only materialized as-of snapshots: no update/delete via normal SQL.
CREATE TABLE IF NOT EXISTS feature_snapshots (
  snapshot_key text PRIMARY KEY,
  symbol text NOT NULL,
  tf text NOT NULL,
  bar_ts timestamptz NOT NULL,
  available_at timestamptz NOT NULL,
  observed_at timestamptz NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL,
  source_checksum text,
  quality text NOT NULL,
  is_causal boolean NOT NULL DEFAULT false,
  values_json jsonb NOT NULL,
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  CHECK(available_at >= bar_ts),
  CHECK(observed_at >= available_at)
);
CREATE INDEX IF NOT EXISTS feature_snapshots_asof_idx
  ON feature_snapshots(symbol,tf,bar_ts,available_at DESC);
CREATE INDEX IF NOT EXISTS feature_snapshots_causal_idx
  ON feature_snapshots(symbol,tf,available_at DESC)
  WHERE is_causal AND quality='complete';

CREATE OR REPLACE FUNCTION eth_reject_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'feature_snapshots is append-only';
END;
$$;
DROP TRIGGER IF EXISTS eth_snapshots_immutable ON feature_snapshots;
CREATE TRIGGER eth_snapshots_immutable
  BEFORE UPDATE OR DELETE ON feature_snapshots
  FOR EACH ROW EXECUTE FUNCTION eth_reject_snapshot_mutation();
