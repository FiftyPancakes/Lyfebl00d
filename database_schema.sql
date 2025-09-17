-- Database Schema for Arbitrage Trading System
-- Generated from Rust source code

-- CG Pools Table (from cg_pools.rs)
CREATE TABLE IF NOT EXISTS cg_pools (
    pool_id TEXT PRIMARY KEY,
    pool_address TEXT NOT NULL,
    pool_name TEXT,
    chain_name TEXT NOT NULL,
    dex_id TEXT NOT NULL,
    dex_name TEXT NOT NULL,
    protocol_type SMALLINT NOT NULL,
    source TEXT NOT NULL,

    token0_address TEXT,
    token1_address TEXT,
    token0_symbol TEXT,
    token1_symbol TEXT,
    token0_name TEXT,
    token1_name TEXT,
    token0_decimals INTEGER,
    token1_decimals INTEGER,

    token0_price_usd NUMERIC,
    token1_price_usd NUMERIC,
    token0_price_native NUMERIC,
    token1_price_native NUMERIC,

    fdv_usd NUMERIC,
    market_cap_usd NUMERIC,
    reserve_in_usd NUMERIC,

    price_change_5m NUMERIC,
    price_change_1h NUMERIC,
    price_change_6h NUMERIC,
    price_change_24h NUMERIC,

    volume_usd_5m NUMERIC,
    volume_usd_1h NUMERIC,
    volume_usd_6h NUMERIC,
    volume_usd_24h NUMERIC,
    volume_usd_7d NUMERIC,

    tx_24h_buys BIGINT,
    tx_24h_sells BIGINT,
    tx_24h_buyers BIGINT,
    tx_24h_sellers BIGINT,

    fee_tier INTEGER,
    is_active BOOLEAN,

    pool_created_at BIGINT,
    first_seen BIGINT NOT NULL,
    last_updated BIGINT NOT NULL
);

-- Indexes for cg_pools
CREATE INDEX IF NOT EXISTS idx_cg_pools_chain ON cg_pools (chain_name);
CREATE INDEX IF NOT EXISTS idx_cg_pools_dex ON cg_pools (dex_id);
CREATE INDEX IF NOT EXISTS idx_cg_pools_protocol ON cg_pools (protocol_type);
CREATE INDEX IF NOT EXISTS idx_cg_pools_reserve ON cg_pools (reserve_in_usd);
CREATE INDEX IF NOT EXISTS idx_cg_pools_source ON cg_pools (source);

-- Swaps Table (from swap.rs)
CREATE TABLE IF NOT EXISTS swaps (
    -- Core identifiers
    network_id TEXT NOT NULL,
    chain_name TEXT NOT NULL,
    pool_address BYTEA NOT NULL,
    transaction_hash BYTEA NOT NULL,
    log_index BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    unix_ts BIGINT,

    -- Pool metadata
    pool_id BYTEA,
    dex_name TEXT,
    protocol_type SMALLINT NOT NULL,

    -- Token information
    token0_address BYTEA,
    token1_address BYTEA,
    token0_decimals INTEGER,
    token1_decimals INTEGER,
    fee_tier INTEGER,

    -- Swap amounts (V2 style)
    amount0_in NUMERIC,
    amount1_in NUMERIC,
    amount0_out NUMERIC,
    amount1_out NUMERIC,

    -- Price data
    price_t0_in_t1 NUMERIC,
    price_t1_in_t0 NUMERIC,

    -- V2 pool state
    token0_reserve NUMERIC,
    token1_reserve NUMERIC,

    -- V3 pool state
    sqrt_price_x96 NUMERIC,
    liquidity NUMERIC,
    tick INTEGER,

    -- V3 swap specifics
    v3_sender BYTEA,
    v3_recipient BYTEA,
    v3_amount0 NUMERIC,
    v3_amount1 NUMERIC,

    -- Transaction metadata
    gas_used BIGINT,
    gas_price NUMERIC,
    tx_origin BYTEA,
    tx_to BYTEA,
    tx_value NUMERIC,
    tx_data TEXT,

    -- Additional metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- V3 tick data (JSONB for flexible storage)
    v3_tick_data TEXT,

    -- Balancer specific fields
    pool_id_balancer BYTEA,
    token_in BYTEA,
    token_out BYTEA,
    amount_in NUMERIC,
    amount_out NUMERIC,

    -- Curve specific fields
    buyer BYTEA,
    sold_id INTEGER,
    tokens_sold NUMERIC,
    bought_id INTEGER,
    tokens_bought NUMERIC,

    -- Extended transaction metadata
    tx_from BYTEA,
    tx_gas BIGINT,
    tx_gas_price NUMERIC,
    tx_gas_used BIGINT,
    tx_block_number BIGINT,
    tx_max_fee_per_gas NUMERIC,
    tx_max_priority_fee_per_gas NUMERIC,
    tx_transaction_type INTEGER,
    tx_chain_id BIGINT,
    tx_cumulative_gas_used NUMERIC,

    -- USD pricing data
    price_from_in_usd NUMERIC,
    price_to_in_usd NUMERIC,
    volume_in_usd NUMERIC,
    pool_liquidity_usd NUMERIC,

    -- Legacy string format fields
    pool_id_str TEXT,
    tx_hash TEXT,
    tx_from_address TEXT,
    from_token_amount NUMERIC,
    to_token_amount NUMERIC,
    price_from_in_currency_token NUMERIC,
    price_to_in_currency_token NUMERIC,
    block_timestamp BIGINT,
    kind TEXT,
    from_token_address TEXT,
    to_token_address TEXT,
    first_seen BIGINT
);

-- Indexes for swaps table
CREATE INDEX IF NOT EXISTS idx_swaps_chain_block ON swaps (chain_name, block_number);
CREATE INDEX IF NOT EXISTS idx_swaps_pool ON swaps (pool_address);
CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps (unix_ts);
CREATE INDEX IF NOT EXISTS idx_swaps_tx ON swaps (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_swaps_tx_from ON swaps (tx_from);
CREATE INDEX IF NOT EXISTS idx_swaps_block_timestamp ON swaps (block_timestamp);
CREATE INDEX IF NOT EXISTS idx_swaps_first_seen ON swaps (first_seen);
CREATE INDEX IF NOT EXISTS idx_swaps_kind ON swaps (kind);

-- Trades Table (from state.rs)
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    pool_address TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,

    token_in TEXT NOT NULL,
    token_out TEXT NOT NULL,
    amount_in NUMERIC NOT NULL,
    amount_out NUMERIC NOT NULL,

    trader_address TEXT,
    profit_usd NUMERIC,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for trades
CREATE INDEX IF NOT EXISTS idx_trades_chain ON trades (chain_name);
CREATE INDEX IF NOT EXISTS idx_trades_pool ON trades (pool_address);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp);

-- Tokens Table (from coins.rs)
CREATE TABLE IF NOT EXISTS tokens (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    address TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    decimals INTEGER NOT NULL,
    price_usd NUMERIC,
    market_cap_usd NUMERIC,
    volume_24h_usd NUMERIC,
    price_change_24h NUMERIC,
    last_updated BIGINT NOT NULL,
    UNIQUE(chain_name, address)
);

-- Indexes for tokens
CREATE INDEX IF NOT EXISTS idx_tokens_chain ON tokens (chain_name);
CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens (symbol);

-- DEXes Table (from dexes.rs)
CREATE TABLE IF NOT EXISTS dexes (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    dex_name TEXT NOT NULL,
    factory_address TEXT NOT NULL,
    router_address TEXT,
    protocol_type INTEGER NOT NULL,
    version TEXT,
    is_active BOOLEAN DEFAULT true,
    last_updated BIGINT NOT NULL,
    UNIQUE(chain_name, factory_address)
);

-- Indexes for dexes
CREATE INDEX IF NOT EXISTS idx_dexes_chain ON dexes (chain_name);
CREATE INDEX IF NOT EXISTS idx_dexes_name ON dexes (dex_name);

-- Networks Table (from prices.rs)
CREATE TABLE IF NOT EXISTS networks (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    chain_id INTEGER NOT NULL UNIQUE,
    native_currency_symbol TEXT NOT NULL,
    native_currency_name TEXT NOT NULL,
    native_currency_decimals INTEGER NOT NULL,
    rpc_urls TEXT[],
    block_explorer_urls TEXT[],
    last_updated BIGINT NOT NULL
);

-- OHLC Data Table (from prices.rs)
CREATE TABLE IF NOT EXISTS ohlc_data (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    token_address TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    open_price NUMERIC NOT NULL,
    high_price NUMERIC NOT NULL,
    low_price NUMERIC NOT NULL,
    close_price NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    UNIQUE(chain_name, token_address, timestamp)
);

-- Indexes for ohlc_data
CREATE INDEX IF NOT EXISTS idx_ohlc_chain_token ON ohlc_data (chain_name, token_address);
CREATE INDEX IF NOT EXISTS idx_ohlc_timestamp ON ohlc_data (timestamp);

-- Market Trends Table (from prices.rs)
CREATE TABLE IF NOT EXISTS market_trends (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    trend_type TEXT NOT NULL,
    period_hours INTEGER NOT NULL,
    score NUMERIC NOT NULL,
    confidence NUMERIC NOT NULL,
    data_points INTEGER NOT NULL,
    calculated_at BIGINT NOT NULL,
    UNIQUE(chain_name, trend_type, period_hours)
);

-- Indexes for market_trends
CREATE INDEX IF NOT EXISTS idx_trends_chain ON market_trends (chain_name);
CREATE INDEX IF NOT EXISTS idx_trends_type ON market_trends (trend_type);
CREATE INDEX IF NOT EXISTS idx_trends_calculated ON market_trends (calculated_at);

-- Blocks Table (for robust timestamp lookups)
CREATE TABLE IF NOT EXISTS blocks (
    id SERIAL PRIMARY KEY,
    network_id TEXT NOT NULL,
    chain_name TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT,
    parent_hash TEXT,
    unix_ts BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(network_id, chain_name, block_number)
);

-- Indexes for blocks table
CREATE INDEX IF NOT EXISTS idx_blocks_chain_block ON blocks (chain_name, block_number);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (unix_ts);

-- V3 Pool Ticks Table (for Uniswap V3 tick data)
CREATE TABLE IF NOT EXISTS v3_ticks (
    id SERIAL PRIMARY KEY,
    chain_name TEXT NOT NULL,
    pool_address BYTEA NOT NULL,
    tick INTEGER NOT NULL,
    liquidity_net BIGINT NOT NULL,
    liquidity_gross BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash BYTEA,
    log_index BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chain_name, pool_address, tick, block_number)
);

-- Indexes for v3_ticks table
CREATE INDEX IF NOT EXISTS idx_v3_ticks_pool_block ON v3_ticks (pool_address, block_number);
CREATE INDEX IF NOT EXISTS idx_v3_ticks_pool_tick ON v3_ticks (pool_address, tick);
CREATE INDEX IF NOT EXISTS idx_v3_ticks_chain ON v3_ticks (chain_name);

-- Create a view that combines blocks and swaps for comprehensive timestamp data
CREATE OR REPLACE VIEW block_timestamps AS
SELECT
    b.chain_name,
    b.block_number,
    b.unix_ts,
    COUNT(s.transaction_hash) as swap_count,
    MAX(s.created_at) as last_swap_time
FROM blocks b
LEFT JOIN swaps s ON b.block_number = s.block_number AND b.chain_name = s.chain_name
GROUP BY b.chain_name, b.block_number, b.unix_ts;

-- Index for the view
CREATE INDEX IF NOT EXISTS idx_block_timestamps ON block_timestamps (chain_name, block_number);
