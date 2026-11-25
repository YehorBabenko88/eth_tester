CREATE TABLE IF NOT EXISTS ingestion_cursor (
  source text NOT NULL, symbol text NOT NULL, instrument text NOT NULL DEFAULT 'linear',
  interval text NOT NULL DEFAULT '', cursor_ts timestamptz,
  last_success timestamptz, status text NOT NULL DEFAULT 'pending',
  error text, meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (source,symbol,instrument,interval)
);
CREATE TABLE IF NOT EXISTS ingestion_job (
  source text NOT NULL, symbol text NOT NULL, day date NOT NULL,
  state text NOT NULL DEFAULT 'pending', attempts integer NOT NULL DEFAULT 0,
  bytes_downloaded bigint NOT NULL DEFAULT 0, row_count bigint NOT NULL DEFAULT 0,
  sha256 text, error text, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source,symbol,day)
);
CREATE TABLE IF NOT EXISTS instrument_metadata (
  symbol text PRIMARY KEY, category text NOT NULL, payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS candles (
  symbol text NOT NULL, kind text NOT NULL, tf text NOT NULL, ts timestamptz NOT NULL,
  open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL, close numeric NOT NULL,
  volume numeric, turnover numeric, closed boolean NOT NULL DEFAULT true,
  source text NOT NULL, quality text NOT NULL DEFAULT 'exchange',
  PRIMARY KEY (symbol,kind,tf,ts)
);
CREATE INDEX IF NOT EXISTS candles_tf_time ON candles(symbol,kind,tf,ts DESC);
CREATE TABLE IF NOT EXISTS trades (
  symbol text NOT NULL, ts timestamptz NOT NULL, trade_id text NOT NULL,
  price numeric NOT NULL, qty numeric NOT NULL, side text NOT NULL CHECK(side IN ('Buy','Sell')),
  notional numeric NOT NULL, source text NOT NULL, raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (symbol,trade_id)
);
CREATE INDEX IF NOT EXISTS trades_time ON trades(symbol,ts DESC);
CREATE TABLE IF NOT EXISTS funding (
  symbol text NOT NULL, ts timestamptz NOT NULL, rate numeric NOT NULL,
  PRIMARY KEY (symbol,ts)
);
CREATE TABLE IF NOT EXISTS open_interest (
  symbol text NOT NULL, interval text NOT NULL, ts timestamptz NOT NULL,
  value numeric NOT NULL, PRIMARY KEY (symbol,interval,ts)
);
CREATE TABLE IF NOT EXISTS ratios (
  symbol text NOT NULL, period text NOT NULL, ts timestamptz NOT NULL,
  buy_ratio numeric, sell_ratio numeric, raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (symbol,period,ts)
);
CREATE TABLE IF NOT EXISTS book_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
  exchange_ts timestamptz, recv_ts timestamptz NOT NULL DEFAULT now(),
  update_id bigint, sequence_no bigint, event_type text NOT NULL,
  bids jsonb NOT NULL, asks jsonb NOT NULL, valid boolean NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS book_events_time ON book_events(symbol,recv_ts DESC);
CREATE TABLE IF NOT EXISTS feed_health (
  symbol text NOT NULL, topic text NOT NULL, ts timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL, detail jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (symbol,topic,ts)
);
CREATE TABLE IF NOT EXISTS footprint (
  symbol text NOT NULL, tf text NOT NULL, bucket timestamptz NOT NULL,
  price numeric NOT NULL, buy_qty numeric NOT NULL DEFAULT 0,
  sell_qty numeric NOT NULL DEFAULT 0, buy_notional numeric NOT NULL DEFAULT 0,
  sell_notional numeric NOT NULL DEFAULT 0, trades bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (symbol,tf,bucket,price)
);
CREATE INDEX IF NOT EXISTS footprint_bucket ON footprint(symbol,tf,bucket);
CREATE TABLE IF NOT EXISTS features (
  symbol text NOT NULL, tf text NOT NULL, ts timestamptz NOT NULL,
  poc numeric, delta numeric, cvd numeric, volume numeric,
  tape_tps numeric, imbalance numeric, oi numeric, funding_rate numeric,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb, quality text NOT NULL DEFAULT 'complete',
  PRIMARY KEY (symbol,tf,ts)
);
CREATE TABLE IF NOT EXISTS levels (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
  tf text NOT NULL, side text NOT NULL, origin_ts timestamptz NOT NULL,
  level_price numeric NOT NULL, strength numeric NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'unbroken', broken_ts timestamptz,
  source text NOT NULL, UNIQUE(symbol,tf,side,origin_ts,level_price)
);
CREATE INDEX IF NOT EXISTS levels_active ON levels(symbol,status,level_price);
CREATE TABLE IF NOT EXISTS bot_control (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  enabled boolean NOT NULL DEFAULT false, paused boolean NOT NULL DEFAULT true,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO bot_control(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS paper_account (
  id integer PRIMARY KEY CHECK(id=1), equity numeric NOT NULL,
  initial_equity numeric NOT NULL, realized_pnl numeric NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS paper_trades (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
  state text NOT NULL, direction text NOT NULL, signal_ts timestamptz NOT NULL,
  opened_at timestamptz, closed_at timestamptz, entry numeric, exit numeric,
  qty numeric, notional numeric, stop numeric, take numeric, reason jsonb NOT NULL,
  fee_paid numeric NOT NULL DEFAULT 0, funding_paid numeric NOT NULL DEFAULT 0,
  slippage_paid numeric NOT NULL DEFAULT 0, gross_pnl numeric NOT NULL DEFAULT 0,
  net_pnl numeric NOT NULL DEFAULT 0, exit_reason text, strategy_version text NOT NULL,
  entry_chart text, exit_chart text
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_paper_trade
  ON paper_trades((true)) WHERE state='open';
CREATE TABLE IF NOT EXISTS bot_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(),
  severity text NOT NULL, event text NOT NULL, data jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS model_versions (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(), model_name text NOT NULL,
  params jsonb NOT NULL, train_from timestamptz, train_to timestamptz,
  validation jsonb, approved boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS backtest_runs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 created_at timestamptz NOT NULL DEFAULT now(), symbol text NOT NULL,
 strategy text NOT NULL, tf text NOT NULL, config jsonb NOT NULL,
 data_start timestamptz, data_end timestamptz, completeness text NOT NULL,
 trades bigint NOT NULL DEFAULT 0, pnl numeric NOT NULL DEFAULT 0,
 stats jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS backtest_trades (
 run_id bigint NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
 seq integer NOT NULL, opened_at timestamptz NOT NULL, closed_at timestamptz NOT NULL,
 direction text NOT NULL, entry numeric NOT NULL, exit numeric NOT NULL,
 stop numeric NOT NULL, take numeric NOT NULL, qty numeric NOT NULL,
 gross_pnl numeric NOT NULL, fees numeric NOT NULL, funding numeric NOT NULL,
 net_pnl numeric NOT NULL, outcome text NOT NULL, reason jsonb NOT NULL,
 PRIMARY KEY(run_id,seq)
);
CREATE TABLE IF NOT EXISTS data_gaps (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 source text NOT NULL, symbol text NOT NULL, start_ts timestamptz NOT NULL,
 end_ts timestamptz, detected_at timestamptz NOT NULL DEFAULT now(),
 resolved_at timestamptz, explanation text NOT NULL
);
CREATE INDEX IF NOT EXISTS gaps_unresolved ON data_gaps(symbol,source)
 WHERE resolved_at IS NULL;
CREATE TABLE IF NOT EXISTS bot_heartbeat (
 component text PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now(),
 status text NOT NULL, detail jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS strategy_signals (
 symbol text NOT NULL, tf text NOT NULL, bar_ts timestamptz NOT NULL,
 version text NOT NULL, decision text NOT NULL, reason jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(symbol,tf,bar_ts,version)
);

CREATE TABLE IF NOT EXISTS market_ticker_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
 exchange_ts timestamptz, received_ts timestamptz NOT NULL DEFAULT now(),
 mark_price numeric, index_price numeric, last_price numeric,
 open_interest numeric, funding_rate numeric,
 bid_price numeric, ask_price numeric,
 event_type text NOT NULL, raw jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS ticker_time ON market_ticker_events(symbol,received_ts DESC);
CREATE TABLE IF NOT EXISTS liquidation_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
 ts timestamptz NOT NULL, side text NOT NULL,
 price numeric NOT NULL, qty numeric NOT NULL,
 raw jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS liquidation_time ON liquidation_events(symbol,ts DESC);
CREATE TABLE IF NOT EXISTS orderbook_wall_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, symbol text NOT NULL,
 ts timestamptz NOT NULL, side text NOT NULL, price numeric NOT NULL,
 prior_qty numeric NOT NULL, new_qty numeric NOT NULL,
 action text NOT NULL, attribution text NOT NULL DEFAULT 'unknown',
 source_id bigint,
 UNIQUE(symbol,source_id,side,price)
);
CREATE INDEX IF NOT EXISTS wall_event_time ON orderbook_wall_events(symbol,ts DESC);

CREATE TABLE IF NOT EXISTS market_book_state (
 symbol text PRIMARY KEY,last_event_id bigint NOT NULL DEFAULT 0,
 bids jsonb NOT NULL DEFAULT '{}'::jsonb, asks jsonb NOT NULL DEFAULT '{}'::jsonb,
 last_update_id bigint,valid boolean NOT NULL DEFAULT false,
 updated_at timestamptz NOT NULL DEFAULT now()
);
