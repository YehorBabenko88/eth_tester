-- R16 B: forward-only PL/pgSQL repair; never rewrite checksum of existing 0007.
-- Preserve original paper/evidence/horizon constraints, triggered on existing label inserts.
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
  structural_side text;
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
  structural_side := original.reason->>'side';
  reference_level := (original.reason->>'level')::numeric;
  IF structural_side NOT IN ('support','resistance')
     OR reference_level IS NULL OR reference_level <= 0
     OR barrier_bps <= 0 OR barrier_bps > 1000 THEN
    RAISE EXCEPTION 'Label has no original structural level/side';
  END IF;
  upper_bound := reference_level*(1+barrier_bps/10000);
  lower_bound := reference_level*(1-barrier_bps/10000);
  FOR bars IN
    SELECT c.ts,c.high,c.low,c.closed,c.source FROM candles AS c
     WHERE c.symbol=original.symbol AND c.kind='trade' AND c.tf='1'
       AND c.ts>=to_timestamp(first_start_ms/1000.0)
       AND c.ts<NEW.horizon_end_at
     ORDER BY c.ts
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
        derived := CASE WHEN structural_side='resistance' THEN 'breakout'
                        ELSE 'bounce' END;
      ELSIF down_hit THEN
        derived := CASE WHEN structural_side='resistance' THEN 'bounce'
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
