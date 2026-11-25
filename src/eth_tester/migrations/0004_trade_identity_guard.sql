-- Concurrency-safe rejection of conflicting global trade identities.
-- Called from ON CONFLICT DO UPDATE ... WHERE canonical payload differs.
CREATE OR REPLACE FUNCTION eth_raise_trade_identity_conflict(
  p_symbol text, p_trade_id text
) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Conflicting ETH Tester trade identity: %/%',
    p_symbol, p_trade_id USING ERRCODE='23505';
END;
$$;
