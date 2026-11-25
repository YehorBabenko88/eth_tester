-- Additive storage facilities; existing tables and primary keys remain unchanged.
CREATE TABLE IF NOT EXISTS eth_storage_identity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  owner_tag text NOT NULL CHECK(owner_tag = 'eth_tester'),
  created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO eth_storage_identity(singleton, owner_tag)
VALUES (true, 'eth_tester') ON CONFLICT(singleton) DO NOTHING;

-- True global trade identity remains in unpartitioned trades(symbol,trade_id).
-- The partitioned auxiliary table is append-only archive provenance.
CREATE TABLE IF NOT EXISTS trade_archive_batches (
  symbol text NOT NULL,
  archive_day date NOT NULL,
  source text NOT NULL,
  sha256 text NOT NULL,
  imported_at timestamptz NOT NULL DEFAULT now(),
  row_count bigint NOT NULL CHECK(row_count >= 0),
  bytes_downloaded bigint NOT NULL CHECK(bytes_downloaded >= 0),
  PRIMARY KEY(symbol, archive_day, source)
) PARTITION BY RANGE (archive_day);
CREATE TABLE IF NOT EXISTS trade_archive_batches_default
  PARTITION OF trade_archive_batches DEFAULT;
CREATE INDEX IF NOT EXISTS trade_archive_batches_day_idx
  ON trade_archive_batches(archive_day DESC);

CREATE TABLE IF NOT EXISTS eth_storage_events (
  id bigint GENERATED ALWAYS AS IDENTITY,
  created_at timestamptz NOT NULL DEFAULT now(),
  component text NOT NULL,
  action text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(id,created_at)
) PARTITION BY RANGE(created_at);
CREATE TABLE IF NOT EXISTS eth_storage_events_default
  PARTITION OF eth_storage_events DEFAULT;
CREATE INDEX IF NOT EXISTS eth_storage_events_created_idx
  ON eth_storage_events(created_at DESC);
