-- Simulation E: paper-only settlement provenance and cross-process cursor.
-- Additive; no trading API credentials, no mutation of historical settlements.
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS funding_verified boolean
  NOT NULL DEFAULT false;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS execution_quality text
  NOT NULL DEFAULT 'unknown';

CREATE TABLE IF NOT EXISTS paper_order_events (
  event_key text PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  event_id text NOT NULL,
  paper_trade_id bigint NOT NULL REFERENCES paper_trades(id),
  order_id text NOT NULL,
  action text NOT NULL CHECK(action IN
    ('entry','exit','partial_entry','partial_exit','cancel','reject')),
  side text NOT NULL CHECK(side IN ('Buy','Sell')),
  qty numeric NOT NULL CHECK(qty > 0),
  price numeric NOT NULL CHECK(price > 0),
  maker boolean NOT NULL,
  fee numeric NOT NULL DEFAULT 0 CHECK(fee >= 0),
  spread_cost numeric NOT NULL DEFAULT 0 CHECK(spread_cost >= 0),
  slippage_cost numeric NOT NULL DEFAULT 0 CHECK(slippage_cost >= 0),
  event_ts timestamptz NOT NULL,
  observed_ts timestamptz NOT NULL,
  quality text NOT NULL DEFAULT 'unknown',
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE(source,symbol,event_id),
  CHECK(observed_ts >= event_ts)
);
CREATE INDEX IF NOT EXISTS paper_order_events_trade_time_idx
  ON paper_order_events(paper_trade_id,event_ts);
CREATE INDEX IF NOT EXISTS paper_order_events_symbol_time_idx
  ON paper_order_events(symbol,event_ts DESC);

CREATE TABLE IF NOT EXISTS paper_funding_events (
  paper_trade_id bigint NOT NULL REFERENCES paper_trades(id),
  funding_ts timestamptz NOT NULL,
  rate numeric NOT NULL,
  mark_price numeric NOT NULL CHECK(mark_price > 0),
  position_qty numeric NOT NULL CHECK(position_qty > 0),
  amount numeric NOT NULL,
  observed_ts timestamptz NOT NULL DEFAULT now(),
  quality text NOT NULL DEFAULT 'unknown',
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(paper_trade_id,funding_ts),
  CHECK(observed_ts >= funding_ts)
);
CREATE INDEX IF NOT EXISTS paper_funding_events_time_idx
  ON paper_funding_events(funding_ts DESC);

CREATE TABLE IF NOT EXISTS paper_position_state (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  paper_trade_id bigint REFERENCES paper_trades(id),
  version bigint NOT NULL DEFAULT 0 CHECK(version >= 0),
  pending_exit boolean NOT NULL DEFAULT false,
  processed_cursor timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK(NOT pending_exit OR paper_trade_id IS NOT NULL)
);
INSERT INTO paper_position_state(singleton) VALUES(true) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION eth_reject_paper_cursor_regression()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.version <= OLD.version THEN
    RAISE EXCEPTION 'Paper position state version must increase';
  END IF;
  IF OLD.processed_cursor IS NOT NULL AND NEW.processed_cursor IS NOT NULL
     AND NEW.processed_cursor < OLD.processed_cursor THEN
    RAISE EXCEPTION 'Paper position cursor cannot move backwards';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS eth_paper_cursor_guard ON paper_position_state;
CREATE TRIGGER eth_paper_cursor_guard
  BEFORE UPDATE ON paper_position_state FOR EACH ROW
  EXECUTE FUNCTION eth_reject_paper_cursor_regression();
