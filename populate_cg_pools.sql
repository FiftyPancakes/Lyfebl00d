-- Populate cg_pools table from swaps data for testing
-- This creates pool metadata from the actual swap transactions

-- Insert pool data derived from swaps
INSERT INTO cg_pools (
    pool_id,
    pool_address,
    pool_name,
    chain_name,
    dex_id,
    dex_name,
    protocol_type,
    source,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    token0_name,
    token1_name,
    token0_decimals,
    token1_decimals,
    fee_tier,
    is_active,
    pool_created_at,
    first_seen,
    last_updated
)
SELECT DISTINCT
    encode(s.pool_address, 'hex') as pool_id,
    encode(s.pool_address, 'hex') as pool_address,
    'Pool_' || encode(s.pool_address, 'hex') as pool_name,
    s.network_id as chain_name,
    'unknown' as dex_id,
    COALESCE(s.dex_name, 'Unknown DEX') as dex_name,
    s.protocol_type,
    'swaps_table' as source,
    CASE
        WHEN s.token0_address < s.token1_address THEN encode(s.token0_address, 'hex')
        ELSE encode(s.token1_address, 'hex')
    END as token0_address,
    CASE
        WHEN s.token0_address < s.token1_address THEN encode(s.token1_address, 'hex')
        ELSE encode(s.token0_address, 'hex')
    END as token1_address,
    'T0_' || substr(encode(s.token0_address, 'hex'), 1, 6) as token0_symbol,
    'T1_' || substr(encode(s.token1_address, 'hex'), 1, 6) as token1_symbol,
    'Token0_' || substr(encode(s.token0_address, 'hex'), 1, 8) as token0_name,
    'Token1_' || substr(encode(s.token1_address, 'hex'), 1, 8) as token1_name,
    COALESCE(s.token0_decimals, 18) as token0_decimals,
    COALESCE(s.token1_decimals, 18) as token1_decimals,
    COALESCE(s.fee_tier, 3000) as fee_tier,
    true as is_active,
    MIN(s.block_number) as pool_created_at,
    MIN(s.unix_ts) as first_seen,
    MAX(s.unix_ts) as last_updated
FROM swaps s
WHERE s.pool_address IS NOT NULL
  AND s.token0_address IS NOT NULL
  AND s.token1_address IS NOT NULL
  AND s.network_id IS NOT NULL
GROUP BY
    s.pool_address,
    s.network_id,
    s.dex_name,
    s.protocol_type,
    s.token0_address,
    s.token1_address,
    s.token0_decimals,
    s.token1_decimals,
    s.fee_tier
ON CONFLICT (pool_id) DO NOTHING;

-- Set some basic price data for testing
UPDATE cg_pools
SET
    token0_price_usd = 1.0,
    token1_price_usd = 1.0,
    token0_price_native = 0.001,
    token1_price_native = 0.001,
    fdv_usd = 1000000,
    market_cap_usd = 1000000,
    reserve_in_usd = 100000,
    price_change_5m = 0.01,
    price_change_1h = 0.02,
    price_change_6h = -0.01,
    price_change_24h = 0.05,
    volume_usd_5m = 1000,
    volume_usd_1h = 5000,
    volume_usd_6h = 25000,
    volume_usd_24h = 100000,
    volume_usd_7d = 500000,
    tx_24h_buys = 100,
    tx_24h_sells = 80,
    tx_24h_buyers = 50,
    tx_24h_sellers = 40
WHERE pool_id IS NOT NULL;

COMMIT;
