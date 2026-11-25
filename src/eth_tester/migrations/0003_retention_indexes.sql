-- Additive, bounded-retention and time-window access paths.
-- Do not drop or rewrite any existing data/indexes.
CREATE INDEX IF NOT EXISTS feed_health_retention_ts_idx ON feed_health(ts);
CREATE INDEX IF NOT EXISTS bot_events_retention_ts_idx ON bot_events(ts);
CREATE INDEX IF NOT EXISTS ingestion_job_state_updated_idx
  ON ingestion_job(state,updated_at);
CREATE INDEX IF NOT EXISTS ingestion_coverage_status_updated_idx
  ON ingestion_coverage(status,checked_at DESC);
CREATE INDEX IF NOT EXISTS feature_snapshots_observed_idx
  ON feature_snapshots(symbol,tf,observed_at DESC);
