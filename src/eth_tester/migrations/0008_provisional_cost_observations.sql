-- Storage B Round15: append-only *provisional* observed cost facts.
-- NEVER use these rows to certify missing funding schedule, zero-event periods,
-- realized paper cash, an economic training label or an exchange execution.
-- No update to historical paper trades/accounts and no real trading routes.
CREATE TABLE IF NOT EXISTS paper_cost_observation_receipts (
  receipt_key text PRIMARY KEY CHECK(receipt_key ~ '^[0-9a-f]{64}$'),
  paper_trade_id bigint NOT NULL REFERENCES paper_trades(id),
  symbol text NOT NULL,
  opened_at timestamptz NOT NULL,
  closed_at timestamptz NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  funding_events_observed integer NOT NULL CHECK(funding_events_observed BETWEEN 0 AND 1000),
  marks_missing integer NOT NULL CHECK(marks_missing BETWEEN 0 AND funding_events_observed),
  recorded_funding numeric NOT NULL,
  candidate_funding numeric,
  recorded_fee numeric NOT NULL,
  recorded_net numeric NOT NULL,
  observation_sha256 text NOT NULL CHECK(observation_sha256 = receipt_key),
  schedule_complete boolean NOT NULL DEFAULT false CHECK(NOT schedule_complete),
  funding_verified boolean NOT NULL DEFAULT false CHECK(NOT funding_verified),
  economics_verified boolean NOT NULL DEFAULT false CHECK(NOT economics_verified),
  ml_eligible boolean NOT NULL DEFAULT false CHECK(NOT ml_eligible),
  reserve_bounded boolean NOT NULL DEFAULT false CHECK(NOT reserve_bounded),
  reserve_usd numeric CHECK(reserve_usd IS NULL),
  paper_only boolean NOT NULL DEFAULT true CHECK(paper_only),
  evidence jsonb NOT NULL,
  CHECK(closed_at >= opened_at),
  CHECK(observed_at >= opened_at),
  CHECK(jsonb_typeof(evidence) = 'object'),
  CHECK(evidence->>'contract' = 'paper_cost_observation_v1'),
  CHECK(evidence->>'funding_verified' = 'false'),
  CHECK(evidence->>'economics_verified' = 'false'),
  CHECK(evidence->>'source_identity_authoritative' = 'false'),
  CHECK(evidence->>'ml_eligible' = 'false'),
  CHECK(evidence->>'official_schedule_verified' = 'false'),
  CHECK(evidence->>'missing_event_periods_verified_zero' = 'false'),
  CHECK(evidence->>'uncertainty_reserve_upper_bound_known' = 'false'),
  CHECK(evidence->>'reserve_status' = 'unbounded_no_certified_capital_release')
);
CREATE INDEX IF NOT EXISTS paper_cost_observations_trade_idx
  ON paper_cost_observation_receipts(paper_trade_id,observed_at DESC);
CREATE TRIGGER eth_round15_cost_observations_immutable
  BEFORE UPDATE OR DELETE ON paper_cost_observation_receipts
  FOR EACH ROW EXECUTE FUNCTION eth_reject_snapshot_mutation();
