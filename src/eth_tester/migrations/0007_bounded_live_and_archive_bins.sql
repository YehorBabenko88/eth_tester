-- Storage B round 6: independent archive-stage lineage vs bounded live PAPER evidence.
-- Additive only. Never upgrade archive partial quality or ML v2 from live observations.
-- No direct real trading paths or destructive changes to existing market tables.

CREATE TABLE IF NOT EXISTS archive_stage_receipts (
  symbol text NOT NULL,
  instrument text NOT NULL CHECK(instrument IN ('linear','inverse','spot')),
  archive_day date NOT NULL,
  archive_sha256 text NOT NULL CHECK(archive_sha256 ~ '^[0-9a-f]{64}$'),
  original_rows bigint NOT NULL CHECK(original_rows > 0),
  distinct_trade_ids bigint NOT NULL CHECK(distinct_trade_ids > 0),
  price_bins bigint NOT NULL CHECK(price_bins > 0),
  base_qty numeric NOT NULL CHECK(base_qty >= 0),
  quote_notional numeric NOT NULL CHECK(quote_notional >= 0),
  captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completeness_scope text NOT NULL DEFAULT 'downloaded_archive_membership_only'
    CHECK(completeness_scope='downloaded_archive_membership_only'),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  PRIMARY KEY(symbol,instrument,archive_day,archive_sha256)
);
CREATE INDEX IF NOT EXISTS archive_stage_receipts_day_idx
 ON archive_stage_receipts(symbol,archive_day DESC);

CREATE TABLE IF NOT EXISTS archive_source_price_bins (
  symbol text NOT NULL,
  instrument text NOT NULL,
  archive_day date NOT NULL,
  archive_sha256 text NOT NULL,
  minute_ts timestamptz NOT NULL,
  price numeric NOT NULL CHECK(price > 0),
  side text NOT NULL CHECK(side IN ('Buy','Sell')),
  trade_count bigint NOT NULL CHECK(trade_count > 0),
  base_qty numeric NOT NULL CHECK(base_qty > 0),
  quote_notional numeric NOT NULL CHECK(quote_notional > 0),
  PRIMARY KEY(symbol,instrument,archive_day,archive_sha256,minute_ts,price,side),
  FOREIGN KEY(symbol,instrument,archive_day,archive_sha256)
    REFERENCES archive_stage_receipts(symbol,instrument,archive_day,archive_sha256),
  CHECK(minute_ts >= (archive_day::timestamp AT TIME ZONE 'UTC')),
  CHECK(minute_ts < ((archive_day + 1)::timestamp AT TIME ZONE 'UTC'))
);
CREATE INDEX IF NOT EXISTS archive_source_price_bins_scope_idx
 ON archive_source_price_bins(symbol,instrument,archive_day,archive_sha256,minute_ts);

CREATE TABLE IF NOT EXISTS archive_validation_receipts (
  validation_key text PRIMARY KEY CHECK(validation_key ~ '^[0-9a-f]{64}$'),
  symbol text NOT NULL,
  instrument text NOT NULL,
  archive_day date NOT NULL,
  archive_sha256 text NOT NULL,
  candle_sha256 text NOT NULL CHECK(candle_sha256 ~ '^[0-9a-f]{64}$'),
  status text NOT NULL CHECK(status IN
    ('volume_only_reconciled','mismatch','incomplete')),
  minutes_checked integer NOT NULL CHECK(minutes_checked BETWEEN 0 AND 1440),
  minutes_matched integer NOT NULL CHECK(minutes_matched BETWEEN 0 AND minutes_checked),
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  FOREIGN KEY(symbol,instrument,archive_day,archive_sha256)
    REFERENCES archive_stage_receipts(symbol,instrument,archive_day,archive_sha256)
);
CREATE INDEX IF NOT EXISTS archive_validation_receipts_day_idx
 ON archive_validation_receipts(symbol,archive_day DESC,checked_at DESC);

-- These source receipts record the TRUE local first-observation clock.
-- They prove only what this process observed, not all exchange trade IDs.
CREATE TABLE IF NOT EXISTS live_trade_receipts (
  symbol text NOT NULL,
  trade_id text NOT NULL,
  session_id text NOT NULL CHECK(length(session_id) BETWEEN 1 AND 256),
  source_event_at timestamptz NOT NULL,
  price numeric NOT NULL CHECK(price > 0),
  qty numeric NOT NULL CHECK(qty > 0),
  notional numeric NOT NULL CHECK(notional > 0),
  side text NOT NULL CHECK(side IN ('Buy','Sell')),
  payload_sha256 text NOT NULL CHECK(payload_sha256 ~ '^[0-9a-f]{64}$'),
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  quality text NOT NULL DEFAULT 'observed_ws_trade'
    CHECK(quality='observed_ws_trade'),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  PRIMARY KEY(symbol,trade_id),
  FOREIGN KEY(symbol,trade_id) REFERENCES trades(symbol,trade_id),
  CHECK(received_at >= source_event_at)
);
CREATE INDEX IF NOT EXISTS live_trade_receipts_window_idx
 ON live_trade_receipts(symbol,source_event_at,session_id,received_at);

CREATE TABLE IF NOT EXISTS live_book_receipts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  symbol text NOT NULL,
  session_id text NOT NULL CHECK(length(session_id) BETWEEN 1 AND 256),
  snapshot_event_id bigint NOT NULL REFERENCES book_events(id),
  event_id bigint NOT NULL REFERENCES book_events(id),
  latest_update_id bigint,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  quality text NOT NULL DEFAULT 'observed_ws_book_lineage'
    CHECK(quality='observed_ws_book_lineage'),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  UNIQUE(symbol,session_id,snapshot_event_id,event_id),
  CHECK(event_id >= snapshot_event_id)
);
CREATE INDEX IF NOT EXISTS live_book_receipts_fresh_idx
 ON live_book_receipts(symbol,received_at DESC);

CREATE TABLE IF NOT EXISTS live_oi_receipts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  symbol text NOT NULL,
  instrument text NOT NULL CHECK(instrument IN ('linear','inverse','spot')),
  interval text NOT NULL,
  source_event_at timestamptz NOT NULL,
  oi_value numeric NOT NULL CHECK(oi_value >= 0),
  response_sha256 text NOT NULL CHECK(response_sha256 ~ '^[0-9a-f]{64}$'),
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  quality text NOT NULL DEFAULT 'observed_rest_oi'
    CHECK(quality='observed_rest_oi'),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  UNIQUE(symbol,interval,source_event_at,response_sha256),
  FOREIGN KEY(symbol,interval,source_event_at)
    REFERENCES open_interest(symbol,interval,ts),
  CHECK(received_at >= source_event_at)
);
CREATE INDEX IF NOT EXISTS live_oi_receipts_fresh_idx
 ON live_oi_receipts(symbol,interval,received_at DESC);

-- An explicitly different namespace from ML-v2 feature_snapshots.
-- Publication is post-receipt, within D/E's 90-second closed-bar window.
CREATE TABLE IF NOT EXISTS paper_live_evidence (
  snapshot_key text PRIMARY KEY CHECK(snapshot_key ~ '^[0-9a-f]{64}$'),
  source_checksum text NOT NULL CHECK(source_checksum ~ '^[0-9a-f]{64}$'),
  contract text NOT NULL DEFAULT 'eth_paper_live_evidence_v1'
    CHECK(contract='eth_paper_live_evidence_v1'),
  symbol text NOT NULL,
  instrument text NOT NULL DEFAULT 'linear' CHECK(instrument='linear'),
  tf text NOT NULL DEFAULT '1' CHECK(tf='1'),
  bar_ts timestamptz NOT NULL,
  bar_close_at timestamptz NOT NULL,
  trade_session text NOT NULL,
  trade_count bigint NOT NULL CHECK(trade_count > 0),
  trade_first_receipt_at timestamptz NOT NULL,
  trade_last_receipt_at timestamptz NOT NULL,
  book_receipt_id bigint NOT NULL REFERENCES live_book_receipts(id),
  oi_receipt_id bigint NOT NULL REFERENCES live_oi_receipts(id),
  base_volume numeric NOT NULL CHECK(base_volume > 0),
  quote_turnover numeric NOT NULL CHECK(quote_turnover > 0),
  evidence jsonb NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  computed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  expires_at timestamptz NOT NULL,
  quality text NOT NULL DEFAULT 'live_observed_bounded'
    CHECK(quality='live_observed_bounded'),
  paper_only boolean NOT NULL DEFAULT true CHECK(paper_only),
  ml_eligible boolean NOT NULL DEFAULT false CHECK(NOT ml_eligible),
  exchange_complete boolean NOT NULL DEFAULT false CHECK(NOT exchange_complete),
  is_ml_certified boolean NOT NULL DEFAULT false CHECK(NOT is_ml_certified),
  is_historical_certified boolean NOT NULL DEFAULT false
    CHECK(NOT is_historical_certified),
  CHECK(bar_close_at=bar_ts + interval '1 minute'),
  CHECK(expires_at=bar_close_at + interval '90 seconds'),
  CHECK(observed_at >= bar_close_at),
  CHECK(observed_at <= computed_at),
  CHECK(computed_at <= expires_at),
  CHECK(trade_first_receipt_at <= trade_last_receipt_at),
  CHECK(trade_last_receipt_at <= observed_at)
);
CREATE INDEX IF NOT EXISTS paper_live_evidence_asof_idx
 ON paper_live_evidence(symbol,tf,bar_ts,computed_at DESC);

CREATE OR REPLACE FUNCTION eth_round6_reject_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Round 6 source evidence is append-only';
END;
$$;
CREATE TRIGGER eth_round6_archive_receipt_immutable
 BEFORE UPDATE OR DELETE ON archive_stage_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_archive_bins_immutable
 BEFORE UPDATE OR DELETE ON archive_source_price_bins FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_archive_validation_immutable
 BEFORE UPDATE OR DELETE ON archive_validation_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_trade_receipts_immutable
 BEFORE UPDATE OR DELETE ON live_trade_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_book_receipts_immutable
 BEFORE UPDATE OR DELETE ON live_book_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_oi_receipts_immutable
 BEFORE UPDATE OR DELETE ON live_oi_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();
CREATE TRIGGER eth_round6_paper_live_immutable
 BEFORE UPDATE OR DELETE ON paper_live_evidence FOR EACH ROW
 EXECUTE FUNCTION eth_round6_reject_evidence_mutation();


-- Defense in depth: a caller may not INSERT a plausible quality flag or
-- backdated timestamp directly. DB triggers validate the underlying source
-- tables and use their OWN PostgreSQL observation clocks.
CREATE OR REPLACE FUNCTION eth_round6_guard_trade_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  raw_record record;
BEGIN
  SELECT ts,price,qty,notional,side,source INTO raw_record
    FROM trades WHERE symbol=NEW.symbol AND trade_id=NEW.trade_id;
  NEW.received_at := clock_timestamp();
  IF NOT FOUND OR raw_record.source <> 'websocket'
     OR raw_record.ts IS DISTINCT FROM NEW.source_event_at
     OR raw_record.price IS DISTINCT FROM NEW.price
     OR raw_record.qty IS DISTINCT FROM NEW.qty
     OR raw_record.notional IS DISTINCT FROM NEW.notional
     OR raw_record.side IS DISTINCT FROM NEW.side
     OR NEW.source_event_at > NEW.received_at THEN
    RAISE EXCEPTION 'Live trade has no actual fresh canonical WS origin';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER eth_round6_trade_receipt_origin
 BEFORE INSERT ON live_trade_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_guard_trade_receipt();

CREATE OR REPLACE FUNCTION eth_round6_guard_book_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  first_event record;
  final_event record;
BEGIN
  NEW.received_at := clock_timestamp();
  SELECT symbol,event_type,valid,recv_ts INTO first_event
    FROM book_events WHERE id=NEW.snapshot_event_id;
  SELECT symbol,event_type,valid,recv_ts,update_id INTO final_event
    FROM book_events WHERE id=NEW.event_id;
  IF NOT FOUND OR first_event.symbol IS DISTINCT FROM NEW.symbol
     OR first_event.event_type <> 'snapshot'
     OR first_event.valid IS NOT TRUE
     OR final_event.symbol IS DISTINCT FROM NEW.symbol
     OR final_event.valid IS NOT TRUE
     OR final_event.recv_ts > NEW.received_at
     OR final_event.recv_ts < first_event.recv_ts
     OR final_event.recv_ts < NEW.received_at - interval '10 seconds'
     OR NEW.latest_update_id IS DISTINCT FROM final_event.update_id THEN
    RAISE EXCEPTION 'Live L2 receipt has no current valid replayed snapshot';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER eth_round6_book_receipt_origin
 BEFORE INSERT ON live_book_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_guard_book_receipt();

CREATE OR REPLACE FUNCTION eth_round6_guard_oi_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  actual numeric;
BEGIN
  NEW.received_at := clock_timestamp();
  SELECT value INTO actual FROM open_interest
   WHERE symbol=NEW.symbol AND interval=NEW.interval
     AND ts=NEW.source_event_at;
  IF NOT FOUND OR actual IS DISTINCT FROM NEW.oi_value
     OR NEW.source_event_at > NEW.received_at
     OR NEW.received_at - NEW.source_event_at > interval '10 minutes' THEN
    RAISE EXCEPTION 'Live OI receipt has no actual fresh persisted REST origin';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER eth_round6_oi_receipt_origin
 BEFORE INSERT ON live_oi_receipts FOR EACH ROW
 EXECUTE FUNCTION eth_round6_guard_oi_receipt();

CREATE OR REPLACE FUNCTION eth_round6_guard_paper_live_publication()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  sampled timestamptz;
  official record;
  tape record;
  raw_tape record;
  book record;
  book_state record;
  oi record;
  active text;
BEGIN
  -- One clock for observed_at and computed_at, never caller-controlled.
  sampled := clock_timestamp();
  NEW.observed_at := sampled;
  NEW.computed_at := sampled;
  NEW.expires_at := NEW.bar_close_at + interval '90 seconds';
  IF NEW.bar_close_at <> NEW.bar_ts + interval '1 minute'
     OR NEW.bar_ts <> date_trunc('minute',NEW.bar_ts)
     OR sampled < NEW.bar_close_at OR sampled > NEW.expires_at THEN
    RAISE EXCEPTION 'Live paper evidence outside actual prospective decision window';
  END IF;
  IF EXISTS(
     SELECT 1 FROM data_gaps
      WHERE symbol=NEW.symbol AND source IN ('publicTrade','orderbook')
        AND start_ts < sampled
        AND coalesce(end_ts,resolved_at,'infinity'::timestamptz) >= NEW.bar_ts
   ) THEN
    RAISE EXCEPTION 'Unverified WS/L2 gap overlaps live paper evidence';
  END IF;
  SELECT status INTO active FROM feed_health
   WHERE symbol=NEW.symbol AND topic='connection'
   ORDER BY ts DESC LIMIT 1;
  IF NOT FOUND OR active <> 'connected' THEN
    RAISE EXCEPTION 'Live public WS session is not presently connected';
  END IF;
  SELECT volume,turnover,source INTO official
    FROM candles
    WHERE symbol=NEW.symbol AND kind='trade' AND tf='1'
      AND ts=NEW.bar_ts AND closed=true;
  IF NOT FOUND OR official.source <> 'bybit_v5'
     OR official.volume IS NULL OR official.turnover IS NULL
     OR official.volume <= 0 OR official.turnover <= 0 THEN
    RAISE EXCEPTION 'No independently closed original 1m exchange candle';
  END IF;
  SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
         coalesce(sum(notional),0) AS quote,
         count(DISTINCT session_id) AS sessions,
         min(received_at) AS first_at,max(received_at) AS last_at,
         min(session_id) AS observed_session
    INTO tape FROM live_trade_receipts
   WHERE symbol=NEW.symbol AND source_event_at>=NEW.bar_ts
     AND source_event_at<NEW.bar_close_at;
  SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
         coalesce(sum(notional),0) AS quote INTO raw_tape
    FROM trades WHERE symbol=NEW.symbol AND source='websocket'
      AND ts>=NEW.bar_ts AND ts<NEW.bar_close_at;
  IF tape.n=0 OR tape.sessions<>1 OR tape.observed_session<>NEW.trade_session
     OR tape.n<>raw_tape.n OR tape.base IS DISTINCT FROM raw_tape.base
     OR tape.quote IS DISTINCT FROM raw_tape.quote
     OR tape.n IS DISTINCT FROM NEW.trade_count
     OR tape.base IS DISTINCT FROM NEW.base_volume
     OR tape.quote IS DISTINCT FROM NEW.quote_turnover
     OR tape.first_at IS DISTINCT FROM NEW.trade_first_receipt_at
     OR tape.last_at IS DISTINCT FROM NEW.trade_last_receipt_at
     OR tape.last_at > sampled
     OR tape.last_at < sampled - interval '20 seconds'
     OR abs(tape.base-official.volume) >
          greatest(0.00000001,abs(official.volume)*0.0000000001)
     OR abs(tape.quote-official.turnover) >
          greatest(0.000001,abs(official.turnover)*0.0000000001) THEN
    RAISE EXCEPTION 'Live trade receipts do not reconcile with official 1m candle';
  END IF;
  SELECT symbol,session_id,event_id,received_at INTO book
    FROM live_book_receipts WHERE id=NEW.book_receipt_id;
  SELECT last_event_id,valid,updated_at INTO book_state
    FROM market_book_state WHERE symbol=NEW.symbol;
  IF NOT FOUND OR book.symbol IS DISTINCT FROM NEW.symbol
     OR book.session_id IS DISTINCT FROM NEW.trade_session
     OR book.received_at > sampled
     OR book.received_at < sampled - interval '10 seconds'
     OR book_state.valid IS NOT TRUE
     OR book_state.last_event_id < book.event_id
     OR book_state.updated_at < sampled - interval '10 seconds' THEN
    RAISE EXCEPTION 'Stale or missing replayed L2 source receipt';
  END IF;
  SELECT symbol,instrument,interval,source_event_at,received_at
    INTO oi FROM live_oi_receipts WHERE id=NEW.oi_receipt_id;
  IF NOT FOUND OR oi.symbol IS DISTINCT FROM NEW.symbol
     OR oi.instrument <> 'linear' OR oi.interval <> '5min'
     OR oi.received_at > sampled
     OR oi.received_at < sampled - interval '10 minutes'
     OR oi.source_event_at > sampled
     OR oi.source_event_at < sampled - interval '10 minutes' THEN
    RAISE EXCEPTION 'Stale or missing REST OI source receipt';
  END IF;
  IF NEW.evidence->>'contract' IS DISTINCT FROM 'eth_paper_live_evidence_v1'
     OR NEW.evidence->>'quality' IS DISTINCT FROM 'live_observed_bounded'
     OR NEW.evidence->>'paper_only' IS DISTINCT FROM 'true'
     OR NEW.evidence->>'ml_eligible' IS DISTINCT FROM 'false'
     OR NEW.evidence->>'exchange_complete' IS DISTINCT FROM 'false'
     OR NEW.evidence->>'trade_session' IS DISTINCT FROM NEW.trade_session
     OR NEW.evidence->>'book_receipt_id' IS DISTINCT FROM NEW.book_receipt_id::text
     OR NEW.evidence->>'oi_receipt_id' IS DISTINCT FROM NEW.oi_receipt_id::text THEN
    RAISE EXCEPTION 'Caller cannot self-certify live-paper provenance';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER eth_round6_paper_live_origin_gate
 BEFORE INSERT ON paper_live_evidence FOR EACH ROW
 EXECUTE FUNCTION eth_round6_guard_paper_live_publication();


-- The E entry binds the policy BEFORE future bars exist. F's post-horizon
-- label only uses fully post-decision official UTC bars. Reject caller-created
-- pseudo-horizons and recompute class outcome from stored immutable candles.
CREATE OR REPLACE FUNCTION eth_round6_guard_full_future_label_grid()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  original record;
  policy jsonb;
  first_start_ms bigint;
  decision_ms bigint;
  expected_start_ms bigint;
  expected_horizon_ms bigint;
  interval_ms bigint;
  horizon_bars integer;
  barrier_bps numeric;
  reference_level numeric;
  upper_bound numeric;
  lower_bound numeric;
  kind text;
  bars record;
  actual_count integer := 0;
  derived text := NULL;
  up_hit boolean;
  down_hit boolean;
BEGIN
  SELECT state,signal_ts,closed_at,reason,symbol INTO original
    FROM paper_trades WHERE id=NEW.paper_trade_id FOR SHARE;
  IF NOT FOUND OR original.state <> 'closed' OR original.closed_at IS NULL
     OR original.reason->>'ml_eligible' = 'false'
     OR original.reason->>'paper_only' = 'true'
     OR original.reason->>'paper_evidence_quality' = 'live_observed_bounded'
     OR original.reason->'orderflow'->>'evidence_quality' =
          'live_observed_bounded' THEN
    RAISE EXCEPTION 'Observed-only live paper entry cannot supply historical ML label';
  END IF;
  policy := original.reason->'ml_label_policy';
  IF policy IS NULL OR policy->>'version' <> 'utc_full_bars_v1'
     OR policy->>'method' <> 'fixed_horizon_barrier_v1'
     OR policy->>'anchor' <> 'first_full_bar_strictly_after_decision'
     OR policy->>'bar_interval_ms' <> '60000'
     OR policy->>'horizon_bars' <> '3'
     OR policy->>'barrier_bps' <> '20' THEN
    RAISE EXCEPTION 'Missing precommitted historical UTC future-bar label policy';
  END IF;
  IF NEW.source_evidence->>'complete_closed_bar_grid' <> 'true'
     OR NEW.source_evidence->>'receipt_is_independently_recorded' <> 'true'
     OR NEW.source_evidence->>'covered_from_decision' <> 'true' THEN
    RAISE EXCEPTION 'Incomplete independently checked closed future-bar grid';
  END IF;
  decision_ms := floor(extract(epoch FROM original.signal_ts)*1000)::bigint;
  expected_start_ms := (floor(decision_ms::numeric / 60000)::bigint + 1)*60000;
  first_start_ms := (NEW.source_evidence->>'first_full_bar_start_ms')::bigint;
  horizon_bars := (policy->>'horizon_bars')::integer;
  interval_ms := (policy->>'bar_interval_ms')::bigint;
  barrier_bps := (policy->>'barrier_bps')::numeric;
  expected_horizon_ms := first_start_ms + horizon_bars*interval_ms;
  IF first_start_ms IS NULL
     OR (NEW.source_evidence->>'decision_ms') IS NULL
     OR (NEW.source_evidence->>'expected_bars') IS NULL
     OR (NEW.source_evidence->>'verified_bars') IS NULL THEN
    RAISE EXCEPTION 'Missing complete first-future-bar evidence';
  END IF;
  IF first_start_ms IS NULL OR first_start_ms <> expected_start_ms
     OR (NEW.source_evidence->>'decision_ms')::bigint <> decision_ms
     OR (NEW.source_evidence->>'expected_bars')::integer <> horizon_bars
     OR (NEW.source_evidence->>'verified_bars')::integer <> horizon_bars
     OR NEW.covered_from_at IS DISTINCT FROM original.signal_ts
     OR floor(extract(epoch FROM NEW.horizon_end_at)*1000)::bigint
           <> expected_horizon_ms
     OR original.closed_at > NEW.horizon_end_at THEN
    RAISE EXCEPTION 'Label uses predecision, shifted, missing or late bar horizon';
  END IF;
  kind := original.reason->>'side';
  reference_level := (original.reason->>'level')::numeric;
  IF kind NOT IN ('support','resistance')
     OR reference_level IS NULL OR reference_level <= 0
     OR barrier_bps <= 0 OR barrier_bps > 1000 THEN
    RAISE EXCEPTION 'Label has no original structural level/side';
  END IF;
  upper_bound := reference_level*(1+barrier_bps/10000);
  lower_bound := reference_level*(1-barrier_bps/10000);
  FOR bars IN
    SELECT ts,high,low,closed,source FROM candles
     WHERE symbol=original.symbol AND kind='trade' AND tf='1'
       AND ts>=to_timestamp(first_start_ms/1000.0)
       AND ts<NEW.horizon_end_at
     ORDER BY ts
  LOOP
    IF bars.ts IS DISTINCT FROM
         to_timestamp((first_start_ms+actual_count*interval_ms)/1000.0)
       OR bars.closed IS NOT TRUE OR bars.source <> 'bybit_v5'
       OR bars.high IS NULL OR bars.low IS NULL
       OR bars.low<=0 OR bars.high<bars.low THEN
      RAISE EXCEPTION 'Unverified, shifted or corrupt full future OHLC bar';
    END IF;
    up_hit := bars.high >= upper_bound;
    down_hit := bars.low <= lower_bound;
    IF derived IS NULL THEN
      IF up_hit AND down_hit THEN
        derived := 'uncertain';
      ELSIF up_hit THEN
        derived := CASE WHEN kind='resistance' THEN 'breakout'
                        ELSE 'bounce' END;
      ELSIF down_hit THEN
        derived := CASE WHEN kind='resistance' THEN 'bounce'
                        ELSE 'breakout' END;
      END IF;
    END IF;
    actual_count := actual_count+1;
  END LOOP;
  IF actual_count <> horizon_bars THEN
    RAISE EXCEPTION 'Missing verified full future candle grid';
  END IF;
  IF COALESCE(derived,'uncertain') IS DISTINCT FROM NEW.outcome THEN
    RAISE EXCEPTION 'Structural label outcome disagrees with stored future OHLC';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER eth_round6_full_future_label_gate
 BEFORE INSERT ON paper_ml_labels FOR EACH ROW
 EXECUTE FUNCTION eth_round6_guard_full_future_label_grid();
