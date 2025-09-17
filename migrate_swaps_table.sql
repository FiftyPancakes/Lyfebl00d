Run this SQL in your PostgreSQL database to add the missing columns:

-- Add Balancer specific fields
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS pool_id_balancer BYTEA;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS token_in BYTEA;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS token_out BYTEA;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS amount_in NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS amount_out NUMERIC;

-- Add Curve specific fields  
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS buyer BYTEA;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS sold_id INTEGER;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tokens_sold NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS bought_id INTEGER;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tokens_bought NUMERIC;

-- Add extended transaction metadata
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_from BYTEA;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_gas BIGINT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_gas_price NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_gas_used BIGINT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_block_number BIGINT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_max_fee_per_gas NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_max_priority_fee_per_gas NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_transaction_type INTEGER;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_chain_id BIGINT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_cumulative_gas_used NUMERIC;

-- Add USD pricing data
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS price_from_in_usd NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS price_to_in_usd NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS volume_in_usd NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS pool_liquidity_usd NUMERIC;

-- Add legacy string format fields
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS pool_id_str TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_hash TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS tx_from_address TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS from_token_amount NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS to_token_amount NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS price_from_in_currency_token NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS price_to_in_currency_token NUMERIC;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS block_timestamp BIGINT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS kind TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS from_token_address TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS to_token_address TEXT;
ALTER TABLE swaps ADD COLUMN IF NOT EXISTS first_seen BIGINT;

-- Add indexes for new columns
CREATE INDEX IF NOT EXISTS idx_swaps_tx_from ON swaps (tx_from);
CREATE INDEX IF NOT EXISTS idx_swaps_block_timestamp ON swaps (block_timestamp);
CREATE INDEX IF NOT EXISTS idx_swaps_first_seen ON swaps (first_seen);
CREATE INDEX IF NOT EXISTS idx_swaps_kind ON swaps (kind);
