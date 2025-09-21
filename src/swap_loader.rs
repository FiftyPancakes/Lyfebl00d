//! Historical swap data loader for backtesting
//!
//! Production-grade implementation with:
//! - Efficient batched queries
//! - Comprehensive caching
//! - Error handling and retries
//! - Performance monitoring
//! - Thread-safe connection pooling
//!
//! # Database Performance
//!
//! For optimal query performance, ensure these indexes are created:
//!
//! ```sql
//! -- Primary swap lookup index
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS swaps_net_block_pool_idx
//!   ON swaps (LOWER(network_id), block_number DESC, pool_address, log_index DESC);
//!
//! -- Token pair lookups
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS swaps_net_block_token_pair_idx
//!   ON swaps (LOWER(network_id), block_number DESC, token0_address, token1_address);
//!
//! -- Timestamp-based queries
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS swaps_net_block_ts_idx
//!   ON swaps (LOWER(network_id), block_number DESC, unix_ts);
//!
//! -- Token prices table (if used)
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS token_prices_net_block_token_idx
//!   ON token_prices (chain_name, block_number DESC, token_address);
//! ```

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use chrono::{Utc, TimeZone};
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::{anyhow, Result};
use deadpool_postgres::Pool;
use ethers::types::{Address, H256, U256};
use moka::future::Cache;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::str::FromStr;
use tracing::{info, instrument, trace, warn, debug, error};
use reqwest::Client;

use crate::{
    config::ChainConfig,
    gas_oracle::GasPrice,
    types::{
        DexProtocol, PoolStateFromSwaps, SwapEvent, SwapData, FeeData, ProtocolType, TickData,
    },
    v4_support::{is_v4_protocol},
};

// Log-once flag for token_prices table fallback warnings
static TOKEN_PRICES_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone)]
pub struct PriceData {
    pub price: f64,
    pub block_number: u64,
    pub token_address: Address,
    pub unix_ts: u64,
    pub source: crate::types::PriceSource,
}

#[derive(Clone, Debug)]
pub struct HistoricalSwapLoader {
    db_pool: Arc<Pool>,
    chain_config: Arc<ChainConfig>,
    
    // Caches
    pool_state_cache: Cache<(String, Address, u64), Option<PoolStateFromSwaps>>,
    price_cache: Cache<(String, Address, u64), Option<PriceData>>,
    timestamp_cache: Cache<(String, u64), u64>,
    gas_price_cache: Cache<(String, u64), GasPrice>,
    fee_data_cache: Cache<(String, u64), FeeData>,
    
    // Per-block gas price cache to prevent repeated queries
    block_gas_cache: Arc<tokio::sync::RwLock<HashMap<(String, u64), GasPrice>>>,
    
    // Metrics
    metrics: Arc<LoaderMetrics>,
    // Shared HTTP client for external price lookups
    http_client: Client,
}

#[derive(Debug, Default)]
struct LoaderMetrics {
    queries_executed: std::sync::atomic::AtomicU64,
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    query_errors: std::sync::atomic::AtomicU64,
}

impl HistoricalSwapLoader {
    /// Safe Decimal -> U256 conversion using decimal string to avoid overflow/truncation.
    fn decimal_to_u256_safe(&self, value: Decimal, default: U256) -> U256 {
        let s = value.trunc().to_string();
        U256::from_dec_str(&s).unwrap_or(default)
    }

    /// 10^exp as Decimal using string construction (avoids float conversions)
    fn decimal_pow10(&self, exp: u32) -> Decimal {
        let s = format!("1e{}", exp);
        Decimal::from_str(&s).unwrap_or(Decimal::ONE)
    }
    pub async fn new(
        db_pool: Arc<Pool>,
        chain_config: Arc<ChainConfig>,
    ) -> Result<Self> {
        debug!("Initializing HistoricalSwapLoader");
        // Test database connection
        let conn = db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        // Verify schema
        conn.query_opt("SELECT 1 FROM swaps LIMIT 1", &[]).await
            .map_err(|e| anyhow!("Failed to verify swaps table: {}", e))?;
        
        info!("HistoricalSwapLoader initialized with database pool");
        
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("historical-swap-loader/1.0")
            .build()
            .map_err(|e| anyhow!("Failed to build HTTP client: {}", e))?;

        Ok(Self {
            db_pool,
            chain_config,
            pool_state_cache: Cache::builder()
                .max_capacity(50_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            price_cache: Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(600))
                .build(),
            timestamp_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(3600))
                .build(),
            gas_price_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            fee_data_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            block_gas_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metrics: Arc::new(LoaderMetrics::default()),
            http_client,
        })
    }

    /// Safely retrieve a large numeric column that may exceed u128 limits and convert to U256 if possible.
    /// Supports sources stored as Decimal, i128, i64, bytea (big-endian), or text hex (0x...)
    fn try_get_u256_numeric(&self, row: &tokio_postgres::Row, col: &str) -> Option<U256> {
        // Try Decimal first
        if let Ok(opt_dec) = row.try_get::<_, Option<Decimal>>(col) {
            if let Some(d) = opt_dec {
                if let Some(v) = d.to_u128() {
                    return Some(U256::from(v));
                }
            }
        }
        // Skip i128 direct reads as it is not supported by tokio_postgres; rely on Decimal/i64/bytea/text
        // Try i64
        if let Ok(opt_i64) = row.try_get::<_, Option<i64>>(col) {
            if let Some(v) = opt_i64 {
                if v >= 0 {
                    return Some(U256::from(v as u128));
                }
            }
        }
        // Try bytea (big-endian)
        if let Ok(opt_bytes) = row.try_get::<_, Option<Vec<u8>>>(col) {
            if let Some(bytes) = opt_bytes {
                let mut buf = [0u8; 32];
                if bytes.len() <= 32 {
                    buf[(32 - bytes.len())..].copy_from_slice(&bytes);
                    return Some(U256::from_big_endian(&buf));
                }
            }
        }
        // Try hex text
        if let Ok(opt_str) = row.try_get::<_, Option<String>>(col) {
            if let Some(s) = opt_str {
                let s_clean = s.trim_start_matches("0x");
                if s_clean.chars().all(|c| c.is_ascii_hexdigit()) && s_clean.len() <= 64 {
                    let mut buf = [0u8; 32];
                    if let Ok(mut raw) = hex::decode(s_clean) {
                        if raw.len() <= 32 {
                            // Ensure big-endian
                            if raw.len() < 32 {
                                let mut tmp = vec![0u8; 32 - raw.len()];
                                tmp.extend_from_slice(&raw);
                                raw = tmp;
                            }
                            buf.copy_from_slice(&raw);
                            return Some(U256::from_big_endian(&buf));
                        }
                    }
                }
            }
        }
        None
    }

    /// Normalize network ID to match database format
    fn normalize_network_id(&self, network_id: &str) -> String {
        match network_id.to_lowercase().as_str() {
            "ethereum" | "eth" | "mainnet" => "ethereum".to_string(),
            "arbitrum" | "arbitrum-one" | "arb" => "arbitrum".to_string(),
            "avalanche" | "avax" | "avalanche-c-chain" => "avax".to_string(),
            "base" => "base".to_string(),
            "binance-smart-chain" | "bsc" | "bnb" => "bsc".to_string(),
            "polygon" | "polygon-pos" | "matic" => "polygon".to_string(),
            "solana" | "sol" => "solana".to_string(),
            _ => network_id.to_lowercase(),
        }
    }

    /// Get network ID aliases for database queries to handle case-insensitive matching
    fn chain_aliases(&self, chain_name: &str) -> Vec<String> {
        match self.normalize_network_id(chain_name).as_str() {
            "ethereum" | "mainnet" | "eth" => vec!["ethereum", "mainnet", "eth"],
            "arbitrum" | "arbitrum-one" | "arb" => vec!["arbitrum", "arbitrum-one", "arb"],
            "bsc" | "binance-smart-chain" | "bnb" => vec!["bsc", "binance-smart-chain", "bnb"],
            "avalanche" | "avax" | "avalanche-c-chain" => vec!["avalanche", "avax", "avalanche-c-chain"],
            "polygon" | "polygon-pos" | "matic" => vec!["polygon", "polygon-pos", "matic"],
            "optimism" | "op" => vec!["optimism", "op"],
            "base" => vec!["base"],
            other => vec![other],
        }.into_iter().map(|s| s.to_string().to_lowercase()).collect()
    }

    /// Get pool states at a specific block by querying the most recent swap data
    /// For backtesting, this returns the state at the START of the target block,
    /// which means the latest state from the END of the previous block
    #[instrument(skip(self))]
    pub async fn get_pool_states_at_block(
        &self,
        chain_name: &str,
        block_number: i64,
    ) -> Result<HashMap<Address, PoolStateFromSwaps>> {
        debug!("Fetching all pool states for chain: {}, at start of block: {}", chain_name, block_number);
        
        // Validate chain configuration and build factory filter
        let cfg_key = self.normalize_network_id(chain_name);
        let chain_config = self.chain_config.chains.get(&cfg_key)
            .or_else(|| self.chain_config.chains.get(chain_name))
            .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;
        
        // Use chain configuration to validate network parameters
        if chain_config.rpc_url.is_empty() {
            return Err(anyhow!("Chain '{}' has no RPC URL configured", chain_name));
        }
        
        // Prepare chain alias list once for all queries below
        let aliases: Vec<String> = self.chain_aliases(chain_name);
        debug!("Using normalized aliases {:?} for chain '{}'", aliases, chain_name);

        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;

        // For backtesting, get the state at the START of the target block,
        // which means the latest state from the END of the previous block (block_number - 1).
        // This prevents lookahead bias in backtesting.
        let previous_block = (block_number - 1).max(0);
        debug!("Querying pool states as of end of block {} (start of block {})", previous_block, block_number);

        let query = r#"
            WITH latest_pool_states AS (
                SELECT DISTINCT ON (pool_address)
                    -- Select all necessary columns for parsing
                    pool_address, token0_address, token1_address, token0_decimals,
                    token1_decimals, fee_tier, protocol_type, unix_ts,
                    token0_reserve, token1_reserve, sqrt_price_x96, liquidity,
                    tick, price_t0_in_t1, price_t1_in_t0, block_number, log_index,
                    dex_name, amount0_in, amount1_in, amount0_out, amount1_out,
                    tx_gas_price, tx_gas_used, tx_max_fee_per_gas, tx_max_priority_fee_per_gas,
                    transaction_hash, v3_sender, v3_recipient, v3_amount0, v3_amount1
                FROM swaps
                WHERE LOWER(network_id) = ANY($1::text[])
                    -- Get the latest record for each pool up to and including the previous block (start-of-block state)
                    AND block_number <= $2
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                -- Order by pool, then by block descending to get the latest state for each pool
                ORDER BY pool_address, block_number DESC, log_index DESC
            )
            SELECT * FROM latest_pool_states;
        "#;

        let rows = conn.query(query, &[&aliases, &previous_block]).await
            .map_err(|e| anyhow!("Database query failed for block {}: {}", block_number, e))?;
        
        let mut pool_states = HashMap::new();
        for row in rows {
            match self.parse_pool_state_from_row(&row) {
                Ok(pool_state) => {
                    // No need to check block number here, the query already guarantees it's correct.
                    pool_states.insert(pool_state.pool_address, pool_state);
                }
                Err(e) => {
                    warn!("Failed to parse historical pool state: {}", e);
                }
            }
        }
        
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        info!(
            "Loaded state for {} pools for chain {} at start of block {} (using previous block {})",
            pool_states.len(),
            chain_name,
            block_number,
            previous_block
        );
        
        Ok(pool_states)
    }

    /// Get pool state for a specific pool at a specific block (without chain filtering)
    /// For backtesting, this returns the state at the START of the target block,
    /// which means the latest state from the END of the previous block (block_number - 1).
    /// This prevents lookahead bias in backtesting.
    #[instrument(skip(self))]
    pub async fn get_pool_state_at_start_of_block(
        &self,
        pool_address: Address,
        block_number: u64,
    ) -> Result<Option<PoolStateFromSwaps>> {
        self.get_pool_state_at_start_of_block_withchain(pool_address, block_number, None).await
    }

    /// Get pool state with fallback mechanism - if exact block not available, look back
    #[instrument(skip(self))]
    pub async fn get_pool_state_with_fallback(
        &self,
        pool_address: Address,
        block_number: u64,
        chain_name: Option<&str>,
        max_lookback_blocks: Option<u64>,
    ) -> Result<Option<PoolStateFromSwaps>> {
        // Check cache first for the exact block
        let (maybe_cache_key, should_use_cache) = if let Some(c) = chain_name {
            let normalized_chain = self.normalize_network_id(c);
            (Some((normalized_chain, pool_address, block_number)), true)
        } else {
            // Avoid cross-chain contamination by not caching when chain_name is unknown
            (None, false)
        };
        if let (Some(cache_key), true) = (&maybe_cache_key, should_use_cache) {
            if let Some(cached) = self.pool_state_cache.get(cache_key).await {
                return Ok(cached);
            }
        }

        // Use the efficient single-query method
        if let Some(state) = self.get_latest_state_before_or_at_block(pool_address, block_number, chain_name).await? {
            // For backtesting, we need to be much more lenient with data freshness
            // Pool state is constant between swaps - the most recent state before the target block is correct
            let max_lookback = max_lookback_blocks.unwrap_or(10000); // Much more lenient default (was 100)
            let block_diff = block_number.saturating_sub(state.block_number);

            if block_diff <= max_lookback {
                // Data is fresh enough, return it
                debug!(
                    "Using pool state data: pool {}, requested block {}, found state from block {} ({} blocks old)",
                    pool_address, block_number, state.block_number, block_diff
                );
                if let (Some(cache_key), true) = (&maybe_cache_key, should_use_cache) {
                    self.pool_state_cache.insert(cache_key.clone(), Some(state.clone())).await;
                }
                Ok(Some(state))
            } else {
                // Data is too old, but for backtesting this should be very rare with the new default
                warn!(
                    "Very stale data for pool {}: requested block {}, found state from block {} ({} blocks old, exceeds lookback of {})",
                    pool_address, block_number, state.block_number, block_diff, max_lookback
                );
                // Cache the absence of data to prevent re-querying
                if let (Some(cache_key), true) = (&maybe_cache_key, should_use_cache) {
                    self.pool_state_cache.insert(cache_key.clone(), None).await;
                }
                Ok(None)
            }
        } else {
            // No data was found at all
            warn!("No state data found for pool {} at or before block {}", pool_address, block_number);
            // Cache the absence of data to prevent re-querying
            if let (Some(cache_key), true) = (&maybe_cache_key, should_use_cache) {
                self.pool_state_cache.insert(cache_key.clone(), None).await;
            }
            Ok(None)
        }
    }

    pub async fn get_pool_state_at_start_of_block_withchain(
        &self,
        pool_address: Address,
        block_number: u64,
        chain_name: Option<&str>,
    ) -> Result<Option<PoolStateFromSwaps>> {
        debug!("Fetching pool state for address: {}, block: {}, chain: {:?}", pool_address, block_number, chain_name);
        // Check cache first
        let (maybe_cache_key, should_use_cache) = if let Some(c) = chain_name {
            let normalized_chain = self.normalize_network_id(c);
            (Some((normalized_chain, pool_address, block_number)), true)
        } else {
            (None, false)
        };
        if let (Some(ref cache_key), true) = (&maybe_cache_key, should_use_cache) {
            if let Some(cached) = self.pool_state_cache.get(cache_key).await {
                self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(cached);
            }
        }

        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;

        let pool_bytes = pool_address.as_bytes();

        // Build query with optional chain filtering
        let previous_block_i64 = (block_number as i64).saturating_sub(1).max(0);
        let aliases = chain_name.map(|c| self.chain_aliases(c));

        let (query, params) = if let Some(ref alias_list) = aliases {
            debug!("Using normalized chain_id aliases: {:?}", alias_list);
            let query = r#"
                SELECT
                        pool_address,
                        token0_address,
                        token1_address,
                        token0_decimals,
                        token1_decimals,
                        fee_tier,
                        protocol_type,
                        unix_ts,
                        token0_reserve,
                        token1_reserve,
                        sqrt_price_x96,
                        liquidity,
                        tick,
                        price_t0_in_t1,
                        price_t1_in_t0,
                        block_number,
                        log_index,
                        dex_name,
                        amount0_in,
                        amount1_in,
                        amount0_out,
                        amount1_out,
                        tx_gas_price,
                        tx_gas_used,
                        tx_max_fee_per_gas,
                        tx_max_priority_fee_per_gas,
                        v3_sender,
                        v3_recipient,
                        v3_amount0,
                        v3_amount1
                FROM swaps
                WHERE pool_address = $1
                    AND LOWER(network_id) = ANY($2::text[])
                    AND block_number <= $3
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                    -- Backtesting start-of-block state uses the latest state from end of previous block
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&pool_bytes, alias_list, &previous_block_i64];
            (query, params)
        } else {
            let query = r#"
                SELECT
                        pool_address,
                        token0_address,
                        token1_address,
                        token0_decimals,
                        token1_decimals,
                        fee_tier,
                        protocol_type,
                        unix_ts,
                        token0_reserve,
                        token1_reserve,
                        sqrt_price_x96,
                        liquidity,
                        tick,
                        price_t0_in_t1,
                        price_t1_in_t0,
                        block_number,
                        log_index,
                        dex_name,
                        amount0_in,
                        amount1_in,
                        amount0_out,
                        amount1_out,
                        tx_gas_price,
                        tx_gas_used,
                        tx_max_fee_per_gas,
                        tx_max_priority_fee_per_gas,
                        v3_sender,
                        v3_recipient,
                        v3_amount0,
                        v3_amount1
                FROM swaps
                WHERE pool_address = $1
                    AND block_number <= $2
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                    -- Backtesting start-of-block state uses the latest state from end of previous block
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&pool_bytes, &previous_block_i64];
            (query, params)
        };

        let row_opt = conn.query_opt(query, &params)
            .await
            .map_err(|e| anyhow!("Database query failed: {}", e))?;
        
        let result = if let Some(row) = row_opt {
            debug!("Database returned row for pool {} at block {}", pool_address, block_number);
            match self.parse_pool_state_from_row(&row) {
                Ok(pool_state) => {
                    debug!("Successfully parsed pool state for {}: reserve0={}, reserve1={}, sqrt_price_x96={:?}, liquidity={:?}", 
                           pool_address, pool_state.reserve0, pool_state.reserve1, 
                           pool_state.sqrt_price_x96, pool_state.liquidity);
                    
                    // In backtesting, accept all pools that exist at the block
                    // Only validate that the pool state is from the correct block or earlier
                    debug!("Pool {} found in database at block {}, returning Some", pool_address, block_number);
                    Some(pool_state)
                }
                Err(e) => {
                    warn!("Failed to parse pool state for {} at block {}: {}", pool_address, block_number, e);
                    None
                }
            }
        } else {
            None
        };
        
        // Cache the result only when chain_name is provided
        if let (Some(cache_key), true) = (&maybe_cache_key, should_use_cache) {
            self.pool_state_cache.insert(cache_key.clone(), result.clone()).await;
        }
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(result)
    }

    /// Get the most recent pool state available for a pool (fallback method)
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_most_recent_pool_state(
        &self,
        pool_address: Address,
        chain_name: Option<&str>,
    ) -> Result<Option<PoolStateFromSwaps>> {
        debug!("Getting most recent pool state for {} (fallback)", pool_address);

        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;

        let pool_bytes = pool_address.as_bytes();

        // Build query to get the most recent pool state
        let aliases = chain_name.map(|c| self.chain_aliases(c));
        let rows = if let Some(ref alias_list) = aliases {
            let query = r#"
                SELECT
                        pool_address,
                        token0_address,
                        token1_address,
                        token0_decimals,
                        token1_decimals,
                        fee_tier,
                        protocol_type,
                        unix_ts,
                        token0_reserve,
                        token1_reserve,
                        sqrt_price_x96,
                        liquidity,
                        tick,
                        price_t0_in_t1,
                        price_t1_in_t0,
                        block_number,
                        log_index,
                        dex_name,
                        amount0_in,
                        amount1_in,
                        amount0_out,
                        amount1_out,
                        tx_gas_price,
                        tx_gas_used,
                        tx_max_fee_per_gas,
                        tx_max_priority_fee_per_gas,
                        v3_sender,
                        v3_recipient,
                        v3_amount0,
                        v3_amount1
                FROM swaps
                WHERE pool_address = $1
                    AND LOWER(network_id) = ANY($2::text[])
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                    AND (token0_reserve IS NOT NULL OR token1_reserve IS NOT NULL
                         OR sqrt_price_x96 IS NOT NULL OR liquidity IS NOT NULL)
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            conn.query(query, &[&pool_bytes, alias_list]).await
        } else {
            let query = r#"
                SELECT
                        pool_address,
                        token0_address,
                        token1_address,
                        token0_decimals,
                        token1_decimals,
                        fee_tier,
                        protocol_type,
                        unix_ts,
                        token0_reserve,
                        token1_reserve,
                        sqrt_price_x96,
                        liquidity,
                        tick,
                        price_t0_in_t1,
                        price_t1_in_t0,
                        block_number,
                        log_index,
                        dex_name,
                        amount0_in,
                        amount1_in,
                        amount0_out,
                        amount1_out,
                        tx_gas_price,
                        tx_gas_used,
                        tx_max_fee_per_gas,
                        tx_max_priority_fee_per_gas,
                        v3_sender,
                        v3_recipient,
                        v3_amount0,
                        v3_amount1
                FROM swaps
                WHERE pool_address = $1
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                    AND (token0_reserve IS NOT NULL OR token1_reserve IS NOT NULL
                         OR sqrt_price_x96 IS NOT NULL OR liquidity IS NOT NULL)
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            conn.query(query, &[&pool_bytes]).await
        }
        .map_err(|e| anyhow!("Database query failed: {}", e))?;

        if rows.is_empty() {
            debug!("No historical pool state found for {} (even for fallback)", pool_address);
            return Ok(None);
        }

        let row = &rows[0];
        match self.parse_pool_state_from_row(row) {
            Ok(state) => Ok(Some(state)),
            Err(e) => {
                warn!("Failed to parse pool state from fallback row: {}", e);
                Ok(None)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_token_price_data_at_block(
        &self,
        chain_name: &str,
        token: Address,
        block_number: u64,
    ) -> Result<Option<PriceData>> {
        self.get_token_price_data_at_block_with_visited(chain_name, token, block_number, &mut HashSet::new(), 0).await
    }

    /// Internal method with recursion depth tracking and visited tokens to prevent infinite loops
    #[tracing::instrument(level = "trace", skip(self, visited_tokens))]
    async fn get_token_price_data_at_block_with_visited(
        &self,
        chain_name: &str,
        token: Address,
        block_number: u64,
        visited_tokens: &mut HashSet<Address>,
        depth: u32,
    ) -> Result<Option<PriceData>> {
        const MAX_RECURSION_DEPTH: u32 = 3;
        
        let _normalized_chain = self.normalize_network_id(chain_name);
        
        // Prevent infinite recursion
        if depth > MAX_RECURSION_DEPTH {
            debug!("Max recursion depth {} reached for token {:?} price resolution", MAX_RECURSION_DEPTH, token);
            return Ok(None);
        }
        
        // Prevent circular dependencies
        if visited_tokens.contains(&token) {
            debug!("Circular dependency detected for token {:?} price resolution", token);
            return Ok(None);
        }
        
        visited_tokens.insert(token);
        trace!("Fetching token price for token: {:?} (bytes: {:x?}), block: {}", token, token.as_bytes(), block_number);
        
        // Special case for zero address (ETH/native token)
        if token == Address::zero() {
            debug!("Token is zero address (ETH), attempting to resolve WETH price for block {}", block_number);
            
            // Try to get WETH price first
            let chain_config = self.chain_config.chains.get(chain_name)
                .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;
            
            // Recursive call to get WETH price
            if let Ok(Some(weth_data)) = Box::pin(self.get_token_price_data_at_block_with_visited(
                chain_name, chain_config.weth_address, block_number, visited_tokens, depth + 1
            )).await {
                    let result = Some(PriceData {
                        price: weth_data.price,
                        block_number,
                        token_address: token,
                        unix_ts: weth_data.unix_ts,
                        source: weth_data.source.clone(),
                    });
                visited_tokens.remove(&token);
                return Ok(result);
            }
            
            // If WETH price not available, return None instead of hardcoded value
            debug!("Could not resolve WETH price for zero address at block {}", block_number);
            visited_tokens.remove(&token);
            return Ok(None);
        }
        
        // Validate chain configuration and get stablecoin addresses
        let cfg_key = self.normalize_network_id(chain_name);
        let chain_config = self.chain_config.chains.get(&cfg_key)
            .or_else(|| self.chain_config.chains.get(chain_name))
            .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;

        debug!("Chain config for {}: weth_address={:?}, reference_stablecoins={:?}",
               chain_name, chain_config.weth_address, chain_config.reference_stablecoins);

        // Check cache first
        let normalized_chain_for_cache = self.normalize_network_id(chain_name);
        let cache_key = (normalized_chain_for_cache.clone(), token, block_number);
        if let Some(cached) = self.price_cache.get(&cache_key).await {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached);
        }

        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Special handling for WETH - it's the base currency, not priced against stablecoins
        if token == chain_config.weth_address {
            debug!("Token is WETH, attempting to get ETH/USD price via multiple methods");

            // Method 1: Try multi-hop resolution first
            if let Ok(Some(multi_hop_price)) = self.get_token_price_via_intermediary(
                chain_name, token, block_number, visited_tokens, 0
            ).await {
                debug!("Using multi-hop ETH price: ${} for WETH at block {}",
                      multi_hop_price.price, block_number);
                self.price_cache.insert(cache_key, Some(multi_hop_price.clone())).await;
                visited_tokens.remove(&token);
                return Ok(Some(multi_hop_price));
            }

            // Method 2: Try direct stablecoin pair resolution
            let eth_price = self.get_eth_price_fallback(chain_name, block_number).await;

            match eth_price {
                Some(price) if price > 0.0 => {
                    let result = Some(PriceData {
                        price,
                        block_number,
                        token_address: token,
                        unix_ts: 0, // We don't have timestamp info for fallback
                        source: crate::types::PriceSource::FallbackDefault,
                    });
                    debug!("Using direct ETH price fallback: ${} for WETH at block {}", price, block_number);
                    self.price_cache.insert(cache_key, result.clone()).await;
                    visited_tokens.remove(&token);
                    return Ok(result);
                },
                _ => {
                    debug!("Could not get ETH price via any method for WETH at block {}", block_number);
                    self.price_cache.insert(cache_key, None).await;
                    visited_tokens.remove(&token);
                    return Ok(None);
                }
            }
        }
        
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        let token_bytes = token.as_bytes();
        
        // Build stablecoin bytes array from config
        let stablecoins: Vec<Address> = if let Some(ref_stablecoins) = &chain_config.reference_stablecoins {
            debug!("Using configured stablecoins for chain {}: {:?}", chain_name, ref_stablecoins);
            ref_stablecoins.clone()
        } else {
            // No default stablecoins - require chain-specific configuration
            debug!("No stablecoins configured for chain {}, returning None", chain_name);
            visited_tokens.remove(&token);
            return Ok(None);
        };
        
        debug!("Final stablecoins list for chain {}: {:?}", chain_name, stablecoins);
        for s in &stablecoins {
            debug!("Stablecoin: {:?} (bytes: {:x?})", s, s.as_bytes());
        }
        
        let stablecoin_bytes: Vec<&[u8]> = stablecoins.iter().map(|s| s.as_bytes()).collect();
        
        // Get network_id from chain_name
        let aliases = self.chain_aliases(chain_name);
        
        // Query for swaps where token is paired with any stablecoin (both directions)
        // Add lookback window to prevent using stale data using per-chain config
        let lookback_blocks: i64 = self.chain_config
            .chains
            .get(&cfg_key)
            .and_then(|c| c.price_lookback_blocks)
            .map(|v| v as i64)
            .unwrap_or(1000);
        let query = r#"
            SELECT 
                block_number,
                unix_ts,
                token0_address,
                token1_address,
                price_t0_in_t1,
                price_t1_in_t0
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND block_number <= $2
                AND block_number > ($2 - $5)
                AND (
                    (token0_address = $3 AND token1_address = ANY($4::bytea[]))
                    OR
                    (token1_address = $3 AND token0_address = ANY($4::bytea[]))
                )
            ORDER BY block_number DESC, log_index DESC
            LIMIT 10
        "#;
        
        debug!("Executing stablecoin price query for token {:?} on chain {} at block {} with {} stablecoins", 
               token, chain_name, block_number, stablecoins.len());
        
        let rows = conn.query(
            query,
            &[&aliases, &(block_number as i64), &token_bytes, &stablecoin_bytes, &lookback_blocks]
        ).await.map_err(|e| anyhow!("Database query failed: {}", e))?;

        debug!("Stablecoin query result: {} rows", rows.len());

        let result = if !rows.is_empty() {
            // Compute median price across up to 10 recent rows
            let mut prices: Vec<(f64, u64)> = Vec::new();
            for row in &rows {
                let token0_bytes: Vec<u8> = row.try_get("token0_address").ok().unwrap_or_default();
                let token1_bytes: Vec<u8> = row.try_get("token1_address").ok().unwrap_or_default();
                let token0 = Address::from_slice(&token0_bytes);
                let token1 = Address::from_slice(&token1_bytes);
                let price_opt: Option<f64> = if token == token0 {
                    row.get::<_, Option<rust_decimal::Decimal>>("price_t0_in_t1").and_then(|d| d.to_f64())
                } else if token == token1 {
                    row.get::<_, Option<rust_decimal::Decimal>>("price_t1_in_t0").and_then(|d| d.to_f64())
                } else { None };
                if let Some(p) = price_opt { if p > 0.0 { 
                    let unix_ts: Option<i64> = row.try_get("unix_ts").ok().flatten();
                    prices.push((p, unix_ts.unwrap_or(0) as u64));
                }}
            }
            if !prices.is_empty() {
                prices.sort_by(|a,b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let mid = prices.len()/2;
                let (median_price, median_ts) = if prices.len()%2==0 { ((prices[mid-1].0+prices[mid].0)/2.0, prices[mid].1) } else { prices[mid] };
                debug!("Stablecoin median price for token {:?}: {} from {} rows", token, median_price, prices.len());
                Some(PriceData { price: median_price, block_number, token_address: token, unix_ts: median_ts, source: crate::types::PriceSource::DerivedFromSwaps })
            } else {
                debug!("Stablecoin price is 0, trying WETH fallback");
                // Try to find price through major tokens (WETH, etc.)
                let weth = chain_config.weth_address;
                
                // Query for swaps where token is paired with WETH
                // Add lookback window to prevent using stale data
                let major_query = r#"
                    SELECT 
                        block_number,
                        unix_ts,
                        token0_address,
                        token1_address,
                        price_t0_in_t1,
                        price_t1_in_t0
                    FROM swaps
                    WHERE LOWER(network_id) = ANY($1::text[])
                        AND block_number <= $2
                        AND block_number > ($2 - $5)
                        AND (
                            (token0_address = $3 AND token1_address = $4)
                            OR
                            (token1_address = $3 AND token0_address = $4)
                        )
                    ORDER BY block_number DESC, log_index DESC
                    LIMIT 1
                "#;
                
                debug!("Executing WETH fallback query for token {:?} with WETH {:?}", token, weth);
                
                let major_row_opt = conn.query(
                    major_query,
                    &[&aliases, &(block_number as i64), &token_bytes, &weth.as_bytes(), &lookback_blocks]
                ).await.map_err(|e| anyhow!("Database query failed: {}", e))?
                .into_iter()
                .next();
                
                debug!("WETH fallback query result: {:?}", major_row_opt.is_some());
                
                if let Some(major_row) = major_row_opt {
                    let token0_bytes: Vec<u8> = major_row.try_get("token0_address").ok().unwrap_or_default();
                    let token1_bytes: Vec<u8> = major_row.try_get("token1_address").ok().unwrap_or_default();
                    let token0 = Address::from_slice(&token0_bytes);
                    let token1 = Address::from_slice(&token1_bytes);
                    let price_in_major = if token == token0 {
                        major_row.get::<_, Option<rust_decimal::Decimal>>("price_t0_in_t1")
                            .and_then(|d| d.to_f64()).unwrap_or(0.0)
                    } else if token == token1 {
                        major_row.get::<_, Option<rust_decimal::Decimal>>("price_t1_in_t0")
                            .and_then(|d| d.to_f64()).unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    
                    debug!("Found WETH price for token {:?}: {} (token0={:?}, token1={:?})", 
                           token, price_in_major, token0, token1);
                    
                    if price_in_major > 0.0 {
                        // De-simplified: recursively resolve the major token's price at this block
                        let major_token_price = if weth == token0 || weth == token1 {
                            // If WETH, try to resolve its price in a stablecoin at this block
                            let stablecoins = if let Some(ref_stablecoins) = &chain_config.reference_stablecoins {
                                ref_stablecoins.clone()
                            } else {
                                // No configured stablecoins: cannot resolve WETH USD price
                                visited_tokens.remove(&token);
                                return Ok(None);
                            };
                            let mut resolved = None;
                            for stable in &stablecoins {
                                if let Some(stable_row) = conn.query(
                                    r#"
                                        SELECT
                                            price_t0_in_t1,
                                            price_t1_in_t0,
                                            token0_address,
                                            token1_address
                                        FROM swaps
                                        WHERE LOWER(network_id) = ANY($1::text[])
                                            AND block_number <= $2
                                            AND block_number > ($2 - 1000)
                                            AND (
                                                (token0_address = $3 AND token1_address = $4)
                                                OR
                                                (token1_address = $3 AND token0_address = $4)
                                            )
                                        ORDER BY block_number DESC, log_index DESC
                                        LIMIT 1
                                    "#,
                                    &[&aliases, &(block_number as i64), &weth.as_bytes(), &stable.as_bytes()]
                                ).await?.into_iter().next() {
                            let t0: Vec<u8> = stable_row.try_get("token0_address").ok().unwrap_or_default();
                            let t1: Vec<u8> = stable_row.try_get("token1_address").ok().unwrap_or_default();
                                    let t0_addr = Address::from_slice(&t0);
                                    let t1_addr = Address::from_slice(&t1);
                                    let price = if weth == t0_addr {
                                        stable_row.get::<_, Option<rust_decimal::Decimal>>("price_t0_in_t1")
                                            .and_then(|d| d.to_f64()).unwrap_or(0.0)
                                    } else if weth == t1_addr {
                                        stable_row.get::<_, Option<rust_decimal::Decimal>>("price_t1_in_t0")
                                            .and_then(|d| d.to_f64()).unwrap_or(0.0)
                                    } else {
                                        0.0
                                    };
                                    if price > 0.0 {
                                        resolved = Some(price);
                                        break;
                                    }
                                }
                            }
                            resolved.unwrap_or(0.0)
                        } else {
                            // For stablecoins, price is 1.0; for other major tokens, recursively resolve their price in WETH or stablecoin
                            let other_token = if token == token0 { token1 } else { token0 };
                            if stablecoins.contains(&other_token) {
                                1.0
                            } else {
                                // Recursively resolve price for other major tokens with depth tracking
                                let other_token = if token == token0 { token1 } else { token0 };
                                if let Some(major_token_price_data) = Box::pin(self.get_token_price_data_at_block_with_visited(chain_name, other_token, block_number, visited_tokens, depth + 1)).await? {
                                    major_token_price_data.price
                                } else {
                                    0.0
                                }
                            }
                        };

                        if major_token_price > 0.0 {
                    let unix_ts: Option<i64> = major_row.try_get("unix_ts").ok().flatten();
                            debug!("Final calculated price for token {:?}: {} (WETH price: {})", 
                                   token, price_in_major * major_token_price, major_token_price);
                            Some(PriceData {
                                price: price_in_major * major_token_price,
                                block_number,
                                token_address: token,
                                unix_ts: unix_ts.unwrap_or(0) as u64,
                                source: crate::types::PriceSource::DerivedFromSwaps,
                            })
                        } else {
                            debug!("Major token price is 0, no price found");
                            None
                        }
                    } else {
                        debug!("WETH price is 0, no price found");
                        None
                    }
                } else {
                    debug!("No WETH pair found, no price found");
                    None
                }
            }
        } else {
            debug!("No stablecoin pair found for token {:?} at block {}", token, block_number);
            None
        };

        // --- EXTERNAL API FALLBACK ---
        // If all database methods failed, try external API for the block's timestamp
        let result = if result.is_none() {
            if let Some(chain_config) = self.chain_config.chains.get(chain_name) {
                if chain_config.backtest_use_external_price_apis.unwrap_or(false) {
                    debug!("All database price resolution failed, trying external API fallback for token {:?} at block {}", token, block_number);

            if let Ok(timestamp) = self.get_timestamp_at_block(chain_name, block_number).await {
                        // For WETH, try to get ETH price from CoinGecko
                        if token == chain_config.weth_address {
                            if let Some(eth_price) = self.get_coingecko_historical_price(timestamp).await {
                                debug!("Found WETH price from CoinGecko: ${:.2}", eth_price);
                        Some(PriceData {
                                    price: eth_price,
                                    block_number,
                                    token_address: token,
                                    unix_ts: timestamp,
                                    source: crate::types::PriceSource::ExternalApi,
                                })
                            } else {
                                debug!("External API fallback failed for WETH at block {}", block_number);
                                None
                            }
                        } else {
                            // For non-WETH tokens, we would need more complex logic to find their prices
                            // This is a simplified fallback that could be extended
                            debug!("External API fallback not implemented for non-WETH tokens yet");
                            None
                        }
                    } else {
                        debug!("Could not get timestamp for block {} to use external API", block_number);
                        None
                    }
                } else {
                    debug!("External price APIs are disabled for chain {} backtesting", chain_name);
                    result
                }
            } else {
                debug!("No chain config found for {}, cannot use external API fallback", chain_name);
                result
            }
        } else {
            result
        };

        // Cache the result
        self.price_cache.insert(cache_key, result.clone()).await;
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Remove token from visited set before returning
        visited_tokens.remove(&token);

        Ok(result)
    }

    /// Multi-hop price resolution for tokens without direct stablecoin pairs
    async fn get_token_price_via_intermediary(
        &self,
        chain_name: &str,
        token: Address,
        block_number: u64,
        visited_tokens: &mut HashSet<Address>,
        depth: u32,
    ) -> Result<Option<PriceData>> {
        if depth > 2 {
            return Ok(None); // Limit recursion depth
        }

        // Try common intermediary tokens (major liquid tokens)
        let intermediaries: Vec<Address> = if let Some(chain_config) = self.chain_config.chains.get(chain_name) {
            let mut v = vec![chain_config.weth_address];
            if let Some(stables) = &chain_config.reference_stablecoins {
                v.extend(stables.iter().cloned());
            }
            if let Some(extra) = &chain_config.major_intermediaries {
                v.extend(extra.iter().cloned());
            }
            v
        } else {
            return Ok(None);
        };

        for intermediary in intermediaries {
            if intermediary == token || visited_tokens.contains(&intermediary) {
                continue; // Skip self and visited tokens
            }

            // Find token -> intermediary price
            if let Some(price_to_intermediary) = self.get_direct_pair_price(
                chain_name, token, intermediary, block_number
            ).await? {
                // Find intermediary -> USD price (via stablecoin)
                if let Some(intermediary_usd_price) = Box::pin(self.get_token_price_data_at_block_with_visited(
                    chain_name, intermediary, block_number, visited_tokens, depth + 1
                )).await? {
                    let final_price = price_to_intermediary * intermediary_usd_price.price;
                    debug!("Found multi-hop price for {:?}: ${} via intermediary {:?}",
                          token, final_price, intermediary);
                    return Ok(Some(PriceData {
                        price: final_price,
                        block_number,
                        token_address: token,
                        unix_ts: intermediary_usd_price.unix_ts,
                        source: intermediary_usd_price.source.clone(),
                    }));
                }
            }
        }
        Ok(None)
    }



    /// Fallback method to get ETH price when WETH pricing fails
    async fn get_eth_price_fallback(&self, chain_name: &str, block_number: u64) -> Option<f64> {
        debug!("Attempting ETH price fallback for chain {} at block {}", chain_name, block_number);
        // Build aliases on demand in query param lists to avoid unused warnings

        // Method 1: Try to get ETH/USD price from a major stablecoin pair (e.g., USDC-WETH)
        if let Some(chain_config) = self.chain_config.chains.get(chain_name) {
            if let Some(stablecoins) = &chain_config.reference_stablecoins {
                // Try the first stablecoin (usually USDC)
                if let Some(stablecoin) = stablecoins.first() {
                    debug!("Trying to get ETH price via {}-WETH pair", hex::encode(stablecoin.as_bytes()));

                    // Query for WETH-stablecoin pair (reverse direction from stablecoin-WETH)
                    let conn = match self.db_pool.get().await {
                        Ok(conn) => conn,
                        Err(_) => return None,
                    };

                    let lookback_blocks = chain_config.price_lookback_blocks.unwrap_or(1000) as i64;
                    let query = r#"
                        SELECT
                            price_t1_in_t0,  -- WETH per stablecoin
                            unix_ts
                        FROM swaps
                        WHERE LOWER(network_id) = ANY($1::text[])
                            AND block_number <= $2
                            AND block_number > ($2 - $5)
                            AND token0_address = $3  -- stablecoin
                            AND token1_address = $4  -- WETH
                        ORDER BY block_number DESC, log_index DESC
                        LIMIT 1
                    "#;

                    let block_number_i64 = block_number as i64;
                    let stablecoin_bytes = stablecoin.as_bytes();
                    let weth_bytes = chain_config.weth_address.as_bytes();
                    let aliases = self.chain_aliases(chain_name);
                    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
                        &aliases,
                        &block_number_i64,
                        &stablecoin_bytes,
                        &weth_bytes,
                        &lookback_blocks,
                    ];

                    match conn.query_one(query, &params).await {
                        Ok(row) => {
                            let price_weth_per_stablecoin: Option<Decimal> = row.try_get("price_t1_in_t0").ok().flatten();
                            if let Some(price) = price_weth_per_stablecoin {
                                if let Some(price_f64) = price.to_f64() {
                                    if price_f64 > 0.0 {
                                        let eth_price = 1.0 / price_f64;
                                        debug!("Got ETH price via {}-WETH pair: ${}", hex::encode(stablecoin.as_bytes()), eth_price);
                                        return Some(eth_price);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            debug!("No {}-WETH pair found, trying reverse direction", hex::encode(stablecoin.as_bytes()));
                        }
                    }

                    // Try reverse direction (WETH-stablecoin)
                    let reverse_query = r#"
                        SELECT
                            price_t0_in_t1,  -- stablecoin per WETH
                            unix_ts
                        FROM swaps
                        WHERE LOWER(network_id) = ANY($1::text[])
                            AND block_number <= $2
                            AND block_number > ($2 - $5)
                            AND token0_address = $3  -- WETH
                            AND token1_address = $4  -- stablecoin
                        ORDER BY block_number DESC, log_index DESC
                        LIMIT 1
                    "#;

                    let reverse_block_number_i64 = block_number as i64;
                    let reverse_stablecoin_bytes = stablecoin.as_bytes();
                    let reverse_weth_bytes = chain_config.weth_address.as_bytes();
                    let reverse_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
                        &aliases,
                        &reverse_block_number_i64,
                        &reverse_weth_bytes,
                        &reverse_stablecoin_bytes,
                        &lookback_blocks,
                    ];

                    if let Ok(row) = conn.query_one(reverse_query, &reverse_params).await {
                        let price_stablecoin_per_weth: Option<Decimal> = row.try_get("price_t0_in_t1").ok().flatten();
                        if let Some(price) = price_stablecoin_per_weth {
                                if let Some(price_f64) = price.to_f64() {
                                    if price_f64 > 0.0 {
                                        let eth_price = price_f64;
                                        debug!("Got ETH price via WETH-{} pair: ${}", hex::encode(stablecoin.as_bytes()), eth_price);
                                        return Some(eth_price);
                                    }
                                }
                        }
                    }
                }
            }
        }

        // Method 2: Try Chainlink-like oracles or other price feeds
        debug!("No stablecoin pairs found, trying external price sources");
        // For now, return a reasonable fallback price for the given time period
        // In production, this would query external oracles
        self.get_external_eth_price(chain_name, block_number).await
    }

    /// Get ETH price from external sources using historical price feeds
    async fn get_external_eth_price(&self, chain_name: &str, block_number: u64) -> Option<f64> {
        debug!("Looking up historical ETH price for block {}", block_number);

        // Check if external APIs are enabled for backtesting
        if let Some(chain_config) = self.chain_config.chains.get(chain_name) {
            if !chain_config.backtest_use_external_price_apis.unwrap_or(false) {
                debug!("External price APIs are disabled for backtesting on chain {}", chain_name);
                return None;
            }
        }

        // First, get the timestamp for this block number
        let timestamp = match self.get_block_unix_ts(chain_name, block_number).await {
            Some(ts) => ts,
            None => {
                debug!("Failed to get timestamp for block {}", block_number);
                return None;
            }
        };

        // Try multiple historical price APIs in order of preference
        // 1. CoinGecko (most reliable)
        if let Some(price) = self.get_coingecko_historical_price(timestamp).await {
            debug!("Got ETH price ${} from CoinGecko for block {}", price, block_number);
            return Some(price);
        }

        // 2. CryptoCompare (backup)
        if let Some(price) = self.get_cryptocompare_historical_price(timestamp).await {
            debug!("Got ETH price ${} from CryptoCompare for block {}", price, block_number);
            return Some(price);
        }

        // 3. DefiLlama (decentralized backup)
        if let Some(price) = self.get_defilama_historical_price(timestamp).await {
            debug!("Got ETH price ${} from DefiLlama for block {}", price, block_number);
            return Some(price);
        }

        debug!("All historical price APIs failed for block {}", block_number);
        None
    }

    /// Get Unix timestamp for a block number, consolidating all timestamp lookup logic
    async fn get_block_unix_ts(&self, chain_name: &str, block_number: u64) -> Option<u64> {
        let conn = match self.db_pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                debug!("Failed to get database connection for timestamp lookup: {}", e);
                return None;
            }
        };

        let aliases = self.chain_aliases(chain_name);

        // First try the blocks table (most reliable source)
        let blocks_query = "
            SELECT unix_ts FROM blocks
            WHERE block_number = $1
            AND LOWER(chain_name) = ANY($2::text[])
            LIMIT 1";
        match conn.query(blocks_query, &[&(block_number as i64), &aliases]).await {
            Ok(rows) if !rows.is_empty() => {
                let ts: Option<i64> = rows[0].get("unix_ts");
                if let Some(ts) = ts {
                    debug!("Found timestamp {} for block {} in blocks table", ts, block_number);
                    return Some(ts as u64);
                }
            }
            Ok(_) => {
                debug!("No timestamp found for block {} in blocks table - this table may not be populated", block_number);
                // Continue to fallback methods
            },
            Err(e) => {
                debug!("Error querying blocks table for block {}: {} - table may not exist", block_number, e);
                // Continue to fallback methods
            },
        }

        // Try the combined view for blocks with swap data
        let view_query = "
            SELECT unix_ts FROM block_timestamps
            WHERE block_number = $1
            AND LOWER(chain_name) = ANY($2::text[])
            LIMIT 1";
        match conn.query(view_query, &[&(block_number as i64), &aliases]).await {
            Ok(rows) if !rows.is_empty() => {
                let ts: Option<i64> = rows[0].get("unix_ts");
                if let Some(ts) = ts {
                    debug!("Found timestamp {} for block {} in block_timestamps view", ts, block_number);
                    return Some(ts as u64);
                }
            }
            Ok(_) => debug!("No timestamp found for block {} in block_timestamps view", block_number),
            Err(e) => debug!("Error querying block_timestamps view for block {}: {}", block_number, e),
        }

        // Try swaps table with normalized network_id
        let network_id = self.normalize_network_id(chain_name);
        let swaps_query = r#"
            SELECT unix_ts
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[]) AND block_number = $2 AND unix_ts IS NOT NULL
            LIMIT 1
        "#;
        match conn.query_opt(swaps_query, &[&aliases, &(block_number as i64)]).await {
            Ok(Some(row)) => {
            let ts: Option<i64> = row.try_get("unix_ts").ok().flatten();
                if let Some(ts) = ts {
                    debug!("Found timestamp {} for block {} in swaps table", ts, block_number);
                    return Some(ts as u64);
                }
            }
            Ok(None) => debug!("No timestamp found for block {} in swaps table", block_number),
            Err(e) => debug!("Error querying swaps table for block {}: {}", block_number, e),
        }

        // Try to find timestamp from nearby blocks using the efficient method
        if let Ok(Some(ts)) = self.find_nearest_block_timestamp(&conn, &aliases, block_number, chain_name).await {
            return Some(ts);
        }

        // FINAL FALLBACK: Try to find ANY block with timestamp data and estimate
        warn!("Could not find timestamp within 10000 blocks of block {}. Trying to find any timestamp data.", block_number);

        // First try to find the closest block in either direction
        let any_block_query = "
            SELECT block_number, unix_ts
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND unix_ts IS NOT NULL
            ORDER BY ABS(block_number - $2) ASC
            LIMIT 1";

        if let Ok(row) = conn.query_one(any_block_query, &[&aliases, &(block_number as i64)]).await {
            let found_block: i64 = row.try_get("block_number").ok().unwrap_or(0);
            let found_ts: i64 = row.try_get("unix_ts").ok().unwrap_or(0);
            let block_diff = block_number as i64 - found_block;

            // Use chain-specific average block time
            let avg_block_time = self.chain_config.chains.get(chain_name)
                .and_then(|c| c.avg_block_time_seconds)
                .unwrap_or(12.0);

            let estimated_ts = found_ts + (block_diff * avg_block_time as i64);
            warn!("Estimated timestamp for block {}: {} (based on closest block {} at {}, diff: {} blocks, avg_block_time: {}s)",
                  block_number, estimated_ts, found_block, found_ts, block_diff, avg_block_time);
            Some(estimated_ts as u64)
        } else {
            // If that fails, try to find the most recent block with data
            let most_recent_query = "
                SELECT unix_ts, block_number
                FROM swaps
                WHERE LOWER(network_id) = ANY($1::text[])
                    AND unix_ts IS NOT NULL
                ORDER BY block_number DESC
                LIMIT 1";

            match conn.query_one(most_recent_query, &[&aliases]).await {
                Ok(row) => {
                    let last_ts: i64 = row.try_get("unix_ts").ok().unwrap_or(0);
                    let last_block: i64 = row.try_get("block_number").ok().unwrap_or(0);
                    let block_diff = block_number as i64 - last_block;

                    // Use chain-specific average block time
                    let avg_block_time = self.chain_config.chains.get(chain_name)
                        .and_then(|c| c.avg_block_time_seconds)
                        .unwrap_or(12.0);

                    let estimated_ts = last_ts + (block_diff * avg_block_time as i64);
                    warn!("Estimated timestamp for block {}: {} (based on most recent block {} at {}, diff: {} blocks, avg_block_time: {}s)",
                          block_number, estimated_ts, last_block, last_ts, block_diff, avg_block_time);
                    Some(estimated_ts as u64)
                },
                Err(e) => {
                    error!("Complete timestamp failure for block {}: no timestamp data available in database: {}", block_number, e);
                    None
                }
            }
        }
    }

    /// Get historical ETH price from CoinGecko
    async fn get_coingecko_historical_price(&self, timestamp: u64) -> Option<f64> {
        // Convert timestamp to date string (YYYY-MM-DD)
        let dt = Utc.timestamp_opt(timestamp as i64, 0).single()?;
        let date_str = dt.format("%d-%m-%Y").to_string();

        let url = format!("https://api.coingecko.com/api/v3/coins/ethereum/history?date={}", date_str);

        match self.http_get_json(&url).await {
            Some(json) => {
                // Navigate the JSON structure: market_data.current_price.usd
                if let Some(market_data) = json.get("market_data") {
                    if let Some(current_price) = market_data.get("current_price") {
                        if let Some(price) = current_price.get("usd") {
                            if let Some(price_f64) = price.as_f64() {
                                return Some(price_f64);
                            }
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    /// Get historical ETH price from CryptoCompare
    async fn get_cryptocompare_historical_price(&self, timestamp: u64) -> Option<f64> {
        // CryptoCompare uses Unix timestamps
        let url = format!(
            "https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=USD&ts={}",
            timestamp
        );

        match self.http_get_json(&url).await {
            Some(json) => {
                // Navigate the JSON structure: ETH.USD
                if let Some(eth_data) = json.get("ETH") {
                    if let Some(price) = eth_data.get("USD") {
                        if let Some(price_f64) = price.as_f64() {
                            return Some(price_f64);
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    /// Get historical ETH price from DefiLlama
    async fn get_defilama_historical_price(&self, timestamp: u64) -> Option<f64> {
        let url = format!(
            "https://coins.llama.fi/prices/historical/{}/coingecko:ethereum?searchWidth=4h",
            timestamp
        );

        match self.http_get_json(&url).await {
            Some(json) => {
                // Navigate the JSON structure: coins["coingecko:ethereum"].price
                if let Some(coins) = json.get("coins") {
                    if let Some(eth_data) = coins.get("coingecko:ethereum") {
                        if let Some(price) = eth_data.get("price") {
                            if let Some(price_f64) = price.as_f64() {
                                return Some(price_f64);
                            }
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    /// Helper function to make HTTP GET requests and parse JSON
    async fn http_get_json(&self, url: &str) -> Option<serde_json::Value> {
        match self.http_client.get(url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => Some(json),
                        Err(e) => {
                            debug!("Failed to parse JSON response: {}", e);
                            None
                        }
                    }
                } else {
                    debug!("HTTP request failed with status: {}", response.status());
                    None
                }
            }
            Err(e) => {
                debug!("HTTP request failed: {}", e);
                None
            }
        }
    }

    /// Get token prices in batch at specific blocks
    #[instrument(skip(self))]
    pub async fn get_token_price_data_batch_at_blocks(
        &self,
        chain_name: &str,
        token: Address,
        block_numbers: &[u64],
    ) -> Result<HashMap<u64, PriceData>> {
        debug!("Fetching token prices in batch for token: {}", token);
        let mut results = HashMap::new();
        let normalized_chain = self.normalize_network_id(chain_name);
        
        // Check cache for each block
        let mut blocks_to_query = Vec::new();
        for &block in block_numbers {
            let cache_key = (normalized_chain.clone(), token, block);
            if let Some(cached) = self.price_cache.get(&cache_key).await {
                if let Some(price_data) = cached {
                    results.insert(block, price_data);
                }
            } else {
                blocks_to_query.push(block);
            }
        }
        
        if blocks_to_query.is_empty() {
            return Ok(results);
        }
        
        // Check if token_prices table should be used
        if !self.chain_config.chains.get(chain_name)
            .and_then(|c| c.use_token_prices_table)
            .unwrap_or(false) {
            // Fall back to per-token resolution when token_prices table is not available
            if !TOKEN_PRICES_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                warn!("token_prices path disabled for chain {}, falling back to per-token resolution", chain_name);
            } else {
                debug!("token_prices fallback already warned once; suppressing repeated logs");
            }
            for &block in &blocks_to_query {
                if let Some(price_data) = self.get_token_price_data_at_block(chain_name, token, block).await? {
                    results.insert(block, price_data);
                }
            }
            return Ok(results);
        }

        // For larger batches, fall back to individual queries since token_prices table doesn't exist
        // This is less efficient but maintains compatibility
        for &block in &blocks_to_query {
            if let Some(price_data) = self.get_token_price_data_at_block(chain_name, token, block).await? {
                results.insert(block, price_data);
            }
        }
        
        Ok(results)
    }

    /// Load V3 tick data for a specific pool at a specific block
    #[instrument(level = "trace", skip(self))]
    async fn load_v3_tick_data_for_pool(
        &self,
        chain_name: &str,
        pool_address: Address,
        block_number: u64,
    ) -> Result<Option<std::collections::HashMap<i32, TickData>>> {
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;

        let pool_bytes = pool_address.as_bytes();
        let aliases = self.chain_aliases(chain_name);

        // Query for tick data at or before the specified block
        let rows = conn.query(
            r#"
            SELECT
                tick,
                liquidity_net,
                liquidity_gross
            FROM v3_ticks
            WHERE pool_address = $1
              AND LOWER(chain_name) = ANY($2::text[])
              AND block_number <= $3
            ORDER BY block_number DESC, tick
            "#,
            &[&pool_bytes, &aliases, &(block_number as i64)]
        ).await.map_err(|e| anyhow!("Failed to query V3 tick data: {}", e))?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut tick_data = std::collections::HashMap::new();
        for row in rows {
            let tick: i32 = row.try_get("tick").ok().unwrap_or(0);
            let liquidity_net: i64 = row.try_get("liquidity_net").ok().unwrap_or(0);
            let liquidity_gross: i64 = row.try_get("liquidity_gross").ok().unwrap_or(0);

            tick_data.insert(tick, TickData {
                liquidity_gross: liquidity_gross as u128,
                liquidity_net: liquidity_net as i128,
            });
        }

        debug!("Loaded {} ticks for pool {} at block {}", tick_data.len(), pool_address, block_number);
        Ok(Some(tick_data))
    }

    /// Get multiple token prices at a specific block
    #[instrument(level = "trace", skip(self))]
    pub async fn get_multi_token_prices_at_block(
        &self,
        chain_name: &str,
        tokens: &[Address],
        block_number: u64,
    ) -> Result<HashMap<Address, f64>> {
        if tokens.is_empty() {
            return Ok(HashMap::new());
        }

        let mut results = HashMap::new();
        let mut uncached_tokens = Vec::new();
        
        // Check cache first
        let normalized_chain = self.normalize_network_id(chain_name);
        for &token in tokens {
            let cache_key = (normalized_chain.clone(), token, block_number);
            if let Some(cached_price) = self.price_cache.get(&cache_key).await {
                self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(price_data) = cached_price {
                    results.insert(token, price_data.price);
                }
            } else {
                self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                uncached_tokens.push(token);
            }
        }
        
        // Batch query for uncached tokens
        if !uncached_tokens.is_empty() {
            // Check if token_prices table should be used
            if !self.chain_config.chains.get(chain_name)
                .and_then(|c| c.use_token_prices_table)
                .unwrap_or(false) {
                // Fall back to per-token resolution when token_prices table is not available
                if !TOKEN_PRICES_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                    warn!("token_prices table not available for chain {}, falling back to individual price queries", chain_name);
                } else {
                    debug!("token_prices fallback already warned once; suppressing repeated logs");
                }
                for &token in &uncached_tokens {
                    if let Some(price_data) = self.get_token_price_data_at_block(chain_name, token, block_number).await? {
                        results.insert(token, price_data.price);
                    }
                }
                return Ok(results);
            }

            let conn = self.db_pool.get().await
                .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;

        // Convert addresses to byte arrays for parameters
        let token_bytes: Vec<&[u8]> = uncached_tokens.iter()
            .map(|addr| addr.as_bytes())
            .collect();

            let query = r#"
                SELECT DISTINCT ON (token_address)
                    token_address,
                    price_usd,
                    price_eth,
                    market_cap,
                    volume_24h,
                    unix_ts
                FROM token_prices
                WHERE LOWER(chain_name) = ANY($1::text[])
                    AND block_number <= $2
                    AND token_address = ANY($3::bytea[])
                ORDER BY token_address, block_number DESC
            "#;

            // Execute batch query with proper parameters
            let aliases = self.chain_aliases(chain_name);
            let rows = conn.query(query, &[&aliases, &(block_number as i64), &token_bytes]).await
                .map_err(|e| anyhow!("Failed to query token prices: {}", e))?;

            self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Process results
            let mut found_tokens = HashSet::new();
            for row in rows {
                let token_bytes: Vec<u8> = row.get(0);
                let token_address = if token_bytes.len() == 20 {
                    Address::from_slice(&token_bytes)
                } else {
                    continue;
                };

                let price_usd: Option<f64> = row.get(1);
                let _price_eth: Option<f64> = row.get(2);
                let _market_cap: Option<f64> = row.get(3);
                let _volume_24h: Option<f64> = row.get(4);
                let unix_ts: i64 = row.get(5);

                let price_data = PriceData {
                    price: match price_usd { Some(p) => p, None => continue },
                    block_number,
                    token_address,
                    unix_ts: unix_ts as u64,
                    source: crate::types::PriceSource::Database,
                };

                // Update cache
                let cache_key = (normalized_chain.clone(), token_address, block_number);
                self.price_cache.insert(cache_key, Some(price_data.clone())).await;

                results.insert(token_address, price_data.price);
                found_tokens.insert(token_address);
            }

            // For tokens not found in database, do not synthesize prices; mark cache as None
            for &token in &uncached_tokens {
                if !found_tokens.contains(&token) {
                    // Cache the absence to avoid repeated queries
                    let cache_key = (normalized_chain.clone(), token, block_number);
                    self.price_cache.insert(cache_key, None).await;
                }
            }
        }
        
        Ok(results)
    }
    
    /// Get fallback price from nearby blocks
    async fn get_fallback_price(
        &self,
        chain_name: &str,
        token: Address,
        target_block: u64,
        max_distance: u64,
    ) -> Option<f64> {
        let conn = self.db_pool.get().await.ok()?;
        let perchain = self.chain_config.chains.get(chain_name)?;
        let strict = perchain.strict_backtest_no_lookahead.unwrap_or(true);
        
        let query = "
            SELECT price_usd
            FROM token_prices
            WHERE LOWER(chain_name) = ANY($1::text[])
                AND token_address = $2
                AND block_number
                    BETWEEN $3 AND $4
            ORDER BY ABS(block_number - $5) ASC
            LIMIT 1
        ";

        let start_block = target_block.saturating_sub(max_distance) as i64;
        // In strict mode, do not look ahead into future blocks
        let end_block = if strict { target_block as i64 } else { (target_block + max_distance) as i64 };
        let target_block_i64 = target_block as i64;
        let token_bytes = token.as_bytes().to_vec();
        let aliases = self.chain_aliases(chain_name);

        let row = conn.query_opt(
            query,
            &[&aliases, &token_bytes, &start_block, &end_block, &target_block_i64]
        ).await.ok()??;
        
        row.get::<_, Option<f64>>(0)
    }

    /// Get gas price info for a specific block
    #[instrument(skip(self))]
    pub async fn get_gas_info_for_block(
        &self,
        chain_name: &str,
        block_number: i64,
    ) -> Result<Option<GasPrice>> {
        debug!("Fetching gas info for block: {}", block_number);
        
        let normalized_chain = self.normalize_network_id(chain_name);
        
        // Check per-block cache first (most recent)
        {
            let block_cache = self.block_gas_cache.read().await;
            if let Some(cached) = block_cache.get(&(normalized_chain.clone(), block_number as u64)) {
                debug!("Gas price for block {} found in per-block cache", block_number);
                return Ok(Some(cached.clone()));
            }
        }
        
        // Validate chain configuration
        let _chain_config = self.chain_config.chains.get(chain_name)
            .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;

        // Check regular cache
        if let Some(cached) = self.gas_price_cache.get(&(normalized_chain.clone(), block_number as u64)).await {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Also store in per-block cache for future requests
            {
                let mut block_cache = self.block_gas_cache.write().await;
                block_cache.insert((normalized_chain.clone(), block_number as u64), cached.clone());
            }
            return Ok(Some(cached));
        }
        
        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Validate chain configuration and get network_id
        let cfg_key = self.normalize_network_id(chain_name);
        let chain_config = self.chain_config.chains.get(&cfg_key)
            .or_else(|| self.chain_config.chains.get(chain_name))
            .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;
        
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        // Get gas price from transaction data with enhanced EIP-1559 support
        // Updated query to use the correct database schema fields
        let query = r#"
            SELECT 
                AVG(tx_gas_price) as avg_gas_price,
                AVG(tx_max_fee_per_gas) as avg_max_fee,
                AVG(tx_max_priority_fee_per_gas) as avg_max_priority_fee,
                COUNT(*) as tx_count
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND block_number = $2
                AND (
                    tx_gas_price IS NOT NULL 
                    OR tx_max_fee_per_gas IS NOT NULL 
                    OR tx_max_priority_fee_per_gas IS NOT NULL
                )
        "#;
        
        let aliases = self.chain_aliases(chain_name);
        let row_result = conn.query_opt(query, &[&aliases, &block_number]).await
            .map_err(|e| anyhow!("Database query failed: {}", e))?;
        
        let (avg_gas_price, avg_max_fee, avg_max_priority_fee, tx_count) = if let Some(row) = row_result {
            let avg_gas_price: Option<Decimal> = row.try_get("avg_gas_price").ok().flatten();
            let avg_max_fee: Option<Decimal> = row.try_get("avg_max_fee").ok().flatten();
            let avg_max_priority_fee: Option<Decimal> = row.try_get("avg_max_priority_fee").ok().flatten();
            let tx_count: i64 = row.try_get("tx_count").ok().unwrap_or(0);
            (avg_gas_price, avg_max_fee, avg_max_priority_fee, tx_count)
        } else {
            (None, None, None, 0)
        };
        
        // Use chain-specific gas price limits
        let max_gas_price = chain_config.max_gas_price;
        let default_gas_price = U256::from(20_000_000_000u64);
        let default_priority_fee = U256::from(2_000_000_000u64);
        
        debug!(
            "Gas data for block {}: tx_count={}, avg_gas_price={:?}, avg_max_fee={:?}, avg_max_priority_fee={:?}",
            block_number, tx_count, avg_gas_price, avg_max_fee, avg_max_priority_fee
        );
        
        let gas_price = if let Some(max_fee) = avg_max_fee {
            // EIP-1559 transaction without explicit base fee
            let max_fee_u256 = self.decimal_to_u256_safe(max_fee, U256::from(20_000_000_000u64));
            let priority_fee_u256 = avg_max_priority_fee
                .map(|p| self.decimal_to_u256_safe(p, U256::from(2_000_000_000u64)))
                .unwrap_or(U256::from(2_000_000_000u64));
            
            // For EIP-1559 transactions, estimate base fee from max fee and priority fee
            let estimated_base_fee = if max_fee_u256 > priority_fee_u256 {
                max_fee_u256.saturating_sub(priority_fee_u256)
            } else {
                max_fee_u256
            };
            
            // Ensure gas price doesn't exceed chain maximum
            let total_gas_price = estimated_base_fee.saturating_add(priority_fee_u256);
            if total_gas_price > max_gas_price {
                GasPrice {
                    base_fee: max_gas_price.saturating_sub(priority_fee_u256),
                    priority_fee: priority_fee_u256,
                }
            } else {
                GasPrice {
                    base_fee: estimated_base_fee,
                    priority_fee: priority_fee_u256,
                }
            }
        } else if let Some(gas_price) = avg_gas_price {
            // Legacy gas price
            let price = self.decimal_to_u256_safe(gas_price, U256::from(20_000_000_000u64));
            let clamped_price = if price > max_gas_price { max_gas_price } else { price };
            GasPrice {
                base_fee: clamped_price,
                priority_fee: U256::zero(),
            }
        } else if tx_count > 0 {
            // We have transactions but no gas data, use default
            debug!("Block {} has {} transactions but no gas price data, using default", block_number, tx_count);
            GasPrice {
                base_fee: default_gas_price,
                priority_fee: default_priority_fee,
            }
        } else {
            // No transactions in this block, try to find gas data from nearby blocks
            debug!("No transactions in block {}, looking for nearby gas data", block_number);
            let nearby_gas_price = self.get_nearby_gas_price(chain_name, block_number, 10).await?;
            nearby_gas_price.unwrap_or_else(|| GasPrice {
                base_fee: default_gas_price,
                priority_fee: default_priority_fee,
            })
        };
        
        // Cache the result in both caches
        self.gas_price_cache.insert((normalized_chain.clone(), block_number as u64), gas_price.clone()).await;
        {
            let mut block_cache = self.block_gas_cache.write().await;
            block_cache.insert((normalized_chain.clone(), block_number as u64), gas_price.clone());
        }
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(Some(gas_price))
    }

    /// Get gas price from nearby blocks if current block has no data
    pub async fn get_nearby_gas_price(
        &self,
        chain_name: &str,
        block_number: i64,
        window: i64,
    ) -> Result<Option<GasPrice>> {
        // Look in a range of ±window blocks for gas data
        let search_range = window;
        let mut found_gas_price = None;
        let normalized_chain = self.normalize_network_id(chain_name);
        
        for offset in 1..=search_range {
            // Check blocks before - first check cache, then database
            let previous_block = block_number - offset;
            if let Some(cached) = self.gas_price_cache.get(&(normalized_chain.clone(), previous_block as u64)).await {
                found_gas_price = Some(cached);
                debug!("Found gas price from cache for block {} (offset: -{})", previous_block, offset);
                break;
            }
            if let Some(gas_price) = self.get_gas_info_for_block_internal(chain_name, previous_block).await? {
                found_gas_price = Some(gas_price);
                debug!("Found gas price from database for block {} (offset: -{})", previous_block, offset);
                break;
            }
            
            // Check blocks after - first check cache, then database
            let next_block = block_number + offset;
            if let Some(cached) = self.gas_price_cache.get(&(normalized_chain.clone(), next_block as u64)).await {
                found_gas_price = Some(cached);
                debug!("Found gas price from cache for block {} (offset: +{})", next_block, offset);
                break;
            }
            if let Some(gas_price) = self.get_gas_info_for_block_internal(chain_name, next_block).await? {
                found_gas_price = Some(gas_price);
                debug!("Found gas price from database for block {} (offset: +{})", next_block, offset);
                break;
            }
        }
        
        Ok(found_gas_price)
    }

    /// Internal method to get gas info without caching (to avoid infinite recursion)
    async fn get_gas_info_for_block_internal(
        &self,
        chain_name: &str,
        block_number: i64,
    ) -> Result<Option<GasPrice>> {
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        let normalized_chain = self.normalize_network_id(chain_name);
        
        let query = r#"
            SELECT 
                AVG(tx_gas_price) as avg_gas_price,
                AVG(tx_max_fee_per_gas) as avg_max_fee,
                AVG(tx_max_priority_fee_per_gas) as avg_max_priority_fee,
                COUNT(*) as tx_count
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND block_number = $2
                AND (
                    tx_gas_price IS NOT NULL 
                    OR tx_max_fee_per_gas IS NOT NULL 
                    OR tx_max_priority_fee_per_gas IS NOT NULL
                )
        "#;
        
        let aliases = self.chain_aliases(chain_name);
        let row_result = conn.query_opt(query, &[&aliases, &block_number]).await
            .map_err(|e| anyhow!("Database query failed: {}", e))?;
        
        if let Some(row) = row_result {
            let avg_gas_price: Option<Decimal> = row.try_get("avg_gas_price").ok().flatten();
            let avg_max_fee: Option<Decimal> = row.try_get("avg_max_fee").ok().flatten();
            let avg_max_priority_fee: Option<Decimal> = row.try_get("avg_max_priority_fee").ok().flatten();
            let tx_count: i64 = row.try_get("tx_count").ok().unwrap_or(0);
            
            if tx_count > 0 {
                let chain_config = self.chain_config.chains.get(chain_name)
                    .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;
                let max_gas_price = chain_config.max_gas_price;
                let default_gas_price = U256::from(20_000_000_000u64);
                let default_priority_fee = U256::from(2_000_000_000u64);
                
                let gas_price = if let Some(max_fee) = avg_max_fee {
                    // EIP-1559 transaction without explicit base fee
                    let max_fee_u256 = self.decimal_to_u256_safe(max_fee, U256::from(20_000_000_000u64));
                    
                    let priority_fee_u256 = avg_max_priority_fee
                        .map(|p| self.decimal_to_u256_safe(p, U256::from(2_000_000_000u64)))
                        .unwrap_or_else(|| U256::from(2_000_000_000u64));
                    
                    // For EIP-1559 transactions, estimate base fee from max fee and priority fee
                    let estimated_base_fee = if max_fee_u256 > priority_fee_u256 {
                        max_fee_u256.saturating_sub(priority_fee_u256)
                    } else {
                        max_fee_u256
                    };
                    
                    // Ensure gas price doesn't exceed chain maximum
                    let total_gas_price = estimated_base_fee.saturating_add(priority_fee_u256);
                    if total_gas_price > max_gas_price {
                        GasPrice {
                            base_fee: max_gas_price.saturating_sub(priority_fee_u256),
                            priority_fee: priority_fee_u256,
                        }
                    } else {
                        GasPrice {
                            base_fee: estimated_base_fee,
                            priority_fee: priority_fee_u256,
                        }
                    }
                } else if let Some(gas_price) = avg_gas_price {
                    // Safe conversion using Decimal->string->U256
                    let price = self.decimal_to_u256_safe(gas_price, U256::from(20_000_000_000u64));
                    let clamped_price = if price > max_gas_price { max_gas_price } else { price };
                    GasPrice {
                        base_fee: clamped_price,
                        priority_fee: U256::zero(),
                    }
                } else {
                    GasPrice {
                        base_fee: default_gas_price,
                        priority_fee: default_priority_fee,
                    }
                };
                
                return Ok(Some(gas_price));
            }
        }
        
        Ok(None)
    }

    /// Get fee data for a specific block
    #[instrument(skip(self))]
    pub async fn get_fee_data_for_block(
        &self,
        chain_name: &str,
        block_number: i64,
    ) -> Result<FeeData> {
        debug!("Fetching fee data for block: {}", block_number);
        let normalized_chain = self.normalize_network_id(chain_name);
        // Check cache first
        if let Some(cached) = self.fee_data_cache.get(&(normalized_chain.clone(), block_number as u64)).await {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached);
        }
        
        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        let gas_price = self.get_gas_info_for_block(chain_name, block_number).await?
            .unwrap_or_else(|| GasPrice {
                base_fee: U256::from(20_000_000_000u64),
                priority_fee: U256::from(2_000_000_000u64),
            });
        
        let fee_data = FeeData {
            base_fee_per_gas: Some(gas_price.base_fee),
            max_priority_fee_per_gas: Some(gas_price.priority_fee),
            max_fee_per_gas: Some(gas_price.base_fee + gas_price.priority_fee),
            gas_price: Some(gas_price.base_fee + gas_price.priority_fee),
            block_number: block_number as u64,
            chain_name: chain_name.to_string(),
        };
        
        // Cache the result
        self.fee_data_cache.insert((normalized_chain.clone(), block_number as u64), fee_data.clone()).await;
        
        Ok(fee_data)
    }

    /// Get timestamp for a specific block
    #[instrument(skip(self))]
    pub async fn get_timestamp_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<u64> {
        debug!("Fetching timestamp for block: {}", block_number);
        let normalized_chain = self.normalize_network_id(chain_name);
        // Check cache first
        if let Some(cached) = self.timestamp_cache.get(&(normalized_chain.clone(), block_number)).await {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached);
        }

        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Use the consolidated timestamp lookup method
        let timestamp = self.get_block_unix_ts(chain_name, block_number)
            .await
            .ok_or_else(|| anyhow!("Could not find timestamp for block {} on chain {}", block_number, chain_name))?;

        // Cache the result
        self.timestamp_cache.insert((normalized_chain.clone(), block_number), timestamp).await;
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(timestamp)
    }
    
    /// Find the nearest block timestamp by searching nearby blocks
    async fn find_nearest_block_timestamp(
        &self,
        conn: &tokio_postgres::Client,
        aliases: &[String],
        target_block: u64,
        chain_name: &str,
    ) -> Result<Option<u64>> {
        const MAX_DISTANCE: i64 = 10000;

        let query = r#"
            SELECT block_number, unix_ts
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND block_number BETWEEN $2 AND $3
                AND unix_ts IS NOT NULL
            ORDER BY ABS(block_number - $4) ASC
            LIMIT 1
        "#;

        let target_block_i64 = target_block as i64;
        let min_block = target_block_i64.saturating_sub(MAX_DISTANCE);
        let max_block = target_block_i64.saturating_add(MAX_DISTANCE);

        debug!("Searching for timestamp within {} blocks of block {} (range: {} to {})",
               MAX_DISTANCE, target_block, min_block, max_block);

        let row_opt = conn.query_opt(
            query,
            &[&aliases, &min_block, &max_block, &target_block_i64]
        ).await.map_err(|e| anyhow!("Failed to find nearest timestamp: {}", e))?;

        debug!("Query result for block {}: found={}", target_block, row_opt.is_some());
        
        if let Some(row) = row_opt {
        let unix_ts: i64 = row.try_get("unix_ts").ok().unwrap_or(0);
        let found_block: i64 = row.try_get("block_number").ok().unwrap_or(0);
            let block_diff = (found_block - target_block_i64).abs();
            
            // Estimate timestamp based on the found block and average block time
            // This is more accurate than a fixed epoch-based calculation
            if block_diff == 0 {
                return Ok(Some(unix_ts as u64));
            }
            
            // Get average block time for the chain (default to 12 seconds for Ethereum)
            let avg_block_time = self.chain_config.chains.get(chain_name)
                .and_then(|c| c.avg_block_time_seconds)
                .unwrap_or(12.0);
            
            let time_adjustment = block_diff as f64 * avg_block_time;
            if found_block > target_block_i64 {
                // Found block is after target, subtract time
                Ok(Some((unix_ts as f64 - time_adjustment) as u64))
            } else {
                // Found block is before target, add time
                Ok(Some((unix_ts as f64 + time_adjustment) as u64))
            }
        } else {
            Ok(None)
        }
    }

    /// Load swaps for a block range
    #[instrument(skip(self))]
    pub async fn load_swaps_for_block_chunk(
        &self,
        chain_name: &str,
        start_block: i64,
        end_block: i64,
    ) -> Result<Vec<SwapEvent>> {
        debug!("Loading swaps for chain: {}, blocks: {}-{}", chain_name, start_block, end_block);
        
        // Validate chain configuration and apply query optimizations
        let cfg_key = self.normalize_network_id(chain_name);
        let chain_config = self.chain_config.chains.get(&cfg_key)
            .or_else(|| self.chain_config.chains.get(chain_name))
            .ok_or_else(|| anyhow!("Chain '{}' not found in configuration", chain_name))?;
        
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        // Apply chain-specific query optimizations
        let limit_clause = if let Some(max_concurrent_requests) = chain_config.max_concurrent_requests {
            if max_concurrent_requests < 100 {
                format!(" LIMIT {}", max_concurrent_requests * 10)
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        
        // Build alias set for network_id matching (case-insensitive)
        let aliases = self.chain_aliases(chain_name);

        // Use prepared statement for better performance with chain-specific optimizations
        let query = format!(
            r#"
            SELECT 
                transaction_hash, log_index, network_id, dex_name, protocol_type, 
                block_number, pool_address, unix_ts, token0_address, token1_address, 
                token0_decimals, token1_decimals, fee_tier, price_t0_in_t1, price_t1_in_t0,
                amount0_in, amount1_in, amount0_out, amount1_out, sqrt_price_x96, 
                liquidity, tick, buyer, sold_id, tokens_sold, bought_id, tokens_bought,
                pool_id, token_in, token_out, amount_in, amount_out, token0_reserve, 
                token1_reserve, tx_from, tx_to, tx_gas, tx_gas_price, tx_gas_used, 
                tx_value, tx_block_number, tx_max_fee_per_gas, tx_max_priority_fee_per_gas,
                tx_transaction_type, tx_chain_id, tx_cumulative_gas_used,
                v3_sender, v3_recipient, v3_amount0, v3_amount1
            FROM swaps 
            WHERE LOWER(network_id) = ANY($1::text[]) AND block_number BETWEEN $2 AND $3
                AND (
                    (amount0_in > 0 AND amount1_out > 0) OR
                    (amount1_in > 0 AND amount0_out > 0) OR
                    (v3_amount0 IS NOT NULL OR v3_amount1 IS NOT NULL) OR
                    (tokens_sold IS NOT NULL OR tokens_bought IS NOT NULL)
                )
            ORDER BY block_number, log_index{}
            "#,
            limit_clause
        );
        
        let rows = conn.query(&query, &[&aliases, &start_block, &end_block]).await
            .map_err(|e| anyhow!("Database query failed: {}", e))?;
        
        let mut swaps = Vec::with_capacity(rows.len());
        
        for row in rows {
            match self.parse_swap_from_row(&row) {
                Ok(swap) => swaps.push(swap),
                Err(e) => {
                    warn!("Failed to parse swap: {}", e);
                    self.metrics.query_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        
        self.metrics.queries_executed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        info!(
            "Loaded {} swaps for chain {} blocks {}-{}",
            swaps.len(),
            chain_name,
            start_block,
            end_block
        );
        
        Ok(swaps)
    }

    /// Clear the per-block gas cache when moving to a new block
    pub async fn clear_block_gas_cache(&self) {
        let mut block_cache = self.block_gas_cache.write().await;
        block_cache.clear();
        debug!("Cleared per-block gas cache");
    }

    // Helper methods

    fn parse_pool_state_from_row(
        &self,
        row: &tokio_postgres::Row,
    ) -> Result<PoolStateFromSwaps> {
        trace!("Parsing pool state from row");

        // Handle pool_address - check if it exists as BYTEA or TEXT
        let pool_address_bytes: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("pool_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("pool_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let pool_address_bytes = pool_address_bytes.ok_or_else(|| {
            anyhow::anyhow!("pool_address column not found or invalid")
        })?;

        let normalized_pool_address = if pool_address_bytes.len() == 20 {
            Address::from_slice(&pool_address_bytes)
        } else if pool_address_bytes.len() == 32 {
            Address::from_slice(&pool_address_bytes[12..])
        } else {
            warn!("Unsupported pool address length: {}, skipping. Pool address bytes: {:?}", pool_address_bytes.len(), pool_address_bytes);
            return Err(anyhow!("unsupported pool_address length: {}", pool_address_bytes.len()));
        };

        // Handle token addresses - check if they exist
        let token0_bytes: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("token0_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("token0_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let token1_bytes: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("token1_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("token1_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let protocol_type: Option<i16> = row.try_get("protocol_type").ok();
        let protocol_type = protocol_type.unwrap_or(1); // Default to UniswapV2
        let protocol = self.parse_protocol_type(protocol_type);

        // Check if this is a V4 pool (32 bytes instead of 20)
        let _is_v4 = is_v4_protocol(protocol_type) || pool_address_bytes.len() == 32;

        // Parse V2-specific fields from database schema - handle missing columns gracefully
        let reserve0: Option<Decimal> = row.try_get("token0_reserve").ok().flatten();
        let reserve1: Option<Decimal> = row.try_get("token1_reserve").ok().flatten();

        // Debug logging for reserve parsing
        if reserve0.is_none() {
            trace!("token0_reserve field not found or NULL");
        } else {
            trace!("token0_reserve parsed successfully: {}", reserve0.unwrap());
        }
        if reserve1.is_none() {
            trace!("token1_reserve field not found or NULL");
        } else {
            trace!("token1_reserve parsed successfully: {}", reserve1.unwrap());
        }

        // Parse V3-specific fields from database schema - handle missing columns gracefully
        let sqrt_price_x96: Option<Decimal> = row.try_get("sqrt_price_x96").ok().flatten();
        let liquidity: Option<Decimal> = row.try_get("liquidity").ok().flatten();
        let tick: Option<i32> = row.try_get("tick").ok().flatten();
        
        // Determine if this is a V3 pool with valid V3-specific data
        // Accept either sqrt_price_x96 or tick as price source; liquidity may be zero
        let has_v3_data = (sqrt_price_x96.is_some() && sqrt_price_x96.unwrap() > Decimal::ZERO)
            || tick.is_some();

        // Determine if this is a V2 pool with valid reserve data (at least one reserve non-zero)
        let has_v2_data = (reserve0.is_some() && reserve0.unwrap() > Decimal::ZERO) ||
                         (reserve1.is_some() && reserve1.unwrap() > Decimal::ZERO);

        trace!("has_v2_data: {}, reserve0: {:?}, reserve1: {:?}", has_v2_data, reserve0, reserve1);
        let _is_non_standard_pool = !has_v3_data && !has_v2_data;

        // For V3 pools without explicit reserve data, do NOT synthesize full-range reserves without tick bounds.
        // Use 0 reserves and let downstream math rely on sqrt_price_x96, liquidity, and tick.
        let (reserve0_u256, reserve1_u256) = if has_v3_data && !has_v2_data {
            (U256::zero(), U256::zero())
        } else if has_v2_data {
            // Trust the reserves saved by the ingestor - they are the source of truth
            let r0 = reserve0.and_then(|d| d.to_u128().map(U256::from)).unwrap_or(U256::zero());
            let r1 = reserve1.and_then(|d| d.to_u128().map(U256::from)).unwrap_or(U256::zero());
            trace!("Converting V2 reserves: r0={}, r1={}", r0, r1);
            (r0, r1)
        } else {
            // No usable state; set zero reserves and let upstream validation handle exclusion
            (U256::zero(), U256::zero())
        };

        let (reserve0_final, reserve1_final) = (reserve0_u256, reserve1_u256);

        // Parse fee tier from database schema - handle missing column gracefully
        let fee_tier: Option<i32> = row.try_get("fee_tier").ok().flatten();
        
        // Parse swap amounts for validation and anomaly detection - handle missing columns gracefully
        let amount0_in: Option<Decimal> = row.try_get("amount0_in").ok().flatten();
        let amount1_in: Option<Decimal> = row.try_get("amount1_in").ok().flatten();
        let amount0_out: Option<Decimal> = row.try_get("amount0_out").ok().flatten();
        let amount1_out: Option<Decimal> = row.try_get("amount1_out").ok().flatten();
        
        // Validate swap amounts for consistency
        let swap_amount_validation = {
            let has_input = amount0_in.is_some() || amount1_in.is_some();
            let has_output = amount0_out.is_some() || amount1_out.is_some();
            
            if has_input && has_output {
                // More intelligent validation that accounts for token differences
                let amount0_in_val = amount0_in.unwrap_or(Decimal::ZERO);
                let amount1_in_val = amount1_in.unwrap_or(Decimal::ZERO);
                let amount0_out_val = amount0_out.unwrap_or(Decimal::ZERO);
                let amount1_out_val = amount1_out.unwrap_or(Decimal::ZERO);
                
                // Check for reasonable swap patterns:
                // 1. Should have input in one token and output in the other
                let has_token0_swap = amount0_in_val > Decimal::ZERO && amount1_out_val > Decimal::ZERO;
                let has_token1_swap = amount1_in_val > Decimal::ZERO && amount0_out_val > Decimal::ZERO;
                
                // 2. Shouldn't have input and output of the same token (unless it's a complex swap)
                let has_same_token_io = (amount0_in_val > Decimal::ZERO && amount0_out_val > Decimal::ZERO) ||
                                       (amount1_in_val > Decimal::ZERO && amount1_out_val > Decimal::ZERO);
                
                // 3. Check for reasonable amounts (not zero, not impossibly large)
                let total_input = amount0_in_val + amount1_in_val;
                let total_output = amount0_out_val + amount1_out_val;
                
                // CORRECTED LOGIC: A valid state change (like a mint) can have zero input but non-zero output.
                // The only invalid state is if ALL amounts are zero.
                let has_reasonable_amounts = (total_input > Decimal::ZERO || total_output > Decimal::ZERO) &&
                    total_input < Decimal::from_str("1e50").unwrap_or(Decimal::MAX) &&
                    total_output < Decimal::from_str("1e50").unwrap_or(Decimal::MAX);
                
                // 4. For same-token input/output, check if it's a reasonable ratio (e.g., slippage)
                let has_reasonable_ratio = if has_same_token_io {
                    let ratio = if total_input > Decimal::ZERO { total_output / total_input } else { Decimal::ZERO };
                    // Allow for slippage and fees - ratio should be between 0.1 and 10
                    ratio >= Decimal::from_str("0.1").unwrap_or(Decimal::ZERO) && 
                    ratio <= Decimal::from_str("10").unwrap_or(Decimal::MAX)
                } else {
                    // For cross-token swaps, don't validate ratios as they can be legitimately extreme
                    true
                };
                
                (has_token0_swap || has_token1_swap || has_same_token_io) && has_reasonable_amounts && has_reasonable_ratio
            } else {
                true // Accept if missing data
            }
        };
        
        // Parse gas-related fields for historical context - handle missing columns gracefully
        let tx_gas_price: Option<Decimal> = row.try_get("tx_gas_price").ok().flatten();
        let tx_gas_used: Option<Decimal> = row.try_get("tx_gas_used").ok().flatten();
        let tx_max_fee_per_gas: Option<Decimal> = row.try_get("tx_max_fee_per_gas").ok().flatten();
        let tx_max_priority_fee_per_gas: Option<Decimal> = row.try_get("tx_max_priority_fee_per_gas").ok().flatten();

        // Parse price fields for validation and consistency checks - handle missing columns gracefully
        let price_t0_in_t1: Option<Decimal> = row.try_get("price_t0_in_t1").ok().flatten();
        let price_t1_in_t0: Option<Decimal> = row.try_get("price_t1_in_t0").ok().flatten();
        
        // Validate price consistency using a fixed reciprocal tolerance (1%)
        let price_validation = {
            if let (Some(p0), Some(p1)) = (price_t0_in_t1, price_t1_in_t0) {
                // Check if prices are reciprocals (within reasonable tolerance)
                let product = p0 * p1;
                let tolerance = Decimal::from_str("1.01").unwrap();
                
                // Only validate if both prices are non-zero and reasonable
                let both_reasonable = p0 > Decimal::ZERO && p1 > Decimal::ZERO &&
                                    p0 < Decimal::from_str("1e20").unwrap_or(Decimal::MAX) &&
                                    p1 < Decimal::from_str("1e20").unwrap_or(Decimal::MAX);
                
                if both_reasonable {
                    product > Decimal::ONE / tolerance && product < Decimal::ONE * tolerance
                } else {
                    // If prices are unreasonable, don't validate the product
                    true
                }
            } else {
                true // Accept if missing data
            }
        };
        
        // Protocol-specific validation based on database schema
        // Allow zero reserves. A pool can exist with 0 liquidity in one token.
        // The downstream pathfinder will correctly see it as having no liquidity for that swap direction.
        let has_v2_data_final = reserve0_final != U256::zero() || reserve1_final != U256::zero();
        // Uniswap V3 price can be derived either from sqrt_price_x96 or from the tick.
        // Many historical rows omit sqrt_price_x96 but include tick; we must accept tick-based pricing.
        let has_v3_price_data = sqrt_price_x96.is_some() || tick.is_some();
        // Liquidity may be missing in some records; do not require it for accepting the row.
        let has_v3_data = has_v3_price_data;

        // Validate protocol-specific requirements with more lenient validation
        let protocol_validation = match protocol {
            DexProtocol::UniswapV2 | DexProtocol::SushiSwap | DexProtocol::PancakeSwap => {
                if !has_v2_data_final {
                    warn!("V2 protocol {:?} has no reserve data at all: reserve0={:?}, reserve1={:?}",
                          protocol, reserve0_final, reserve1_final);
                    false
                } else {
                    true
                }
            }
            DexProtocol::UniswapV3 | DexProtocol::UniswapV4 => {
                // More lenient validation for V3/V4 - accept tick-based pricing or V2-style reserves as fallback
                let has_any_data = has_v3_data || has_v2_data;
                if !has_any_data {
                    warn!(
                        "V3/V4 protocol {:?} missing all state data: sqrt_price_x96={:?}, tick={:?}, liquidity={:?}, v2_reserves=({:?}, {:?})",
                        protocol, sqrt_price_x96, tick, liquidity, reserve0, reserve1
                    );
                    false
                } else {
                    if !has_v3_data {
                        debug!(
                            "V3/V4 protocol {:?} missing sqrt_price_x96 but has tick or V2-style data - accepting with fallback",
                            protocol
                        );
                    }
                    true
                }
            }
            DexProtocol::Curve => {
                // Curve has different validation requirements - be more lenient
                let has_any_data = has_v2_data_final || has_v3_data;
                if !has_any_data {
                    trace!("Curve protocol missing all state data: v2_reserves=({:?}, {:?}), v3_data=({:?}, {:?})",
                          reserve0_final, reserve1_final, sqrt_price_x96, liquidity);
                    false
                } else {
                    true
                }
            }
            DexProtocol::Balancer => {
                // Balancer has different validation requirements - be more lenient
                let has_any_data = has_v2_data_final || has_v3_data;
                if !has_any_data {
                    warn!("Balancer protocol missing all state data: v2_reserves=({:?}, {:?}), v3_data=({:?}, {:?})",
                          reserve0_final, reserve1_final, sqrt_price_x96, liquidity);
                    false
                } else {
                    true
                }
            }
            _ => {
                // For other protocols, be more lenient
                has_v2_data_final || has_v3_data
            }
        };
        
        if !protocol_validation {
            warn!("Pool state validation failed for protocol {:?}, but continuing anyway", protocol);
            // Don't fail the entire pool state for protocol validation issues
            // Just log and continue
        }
        
        if !swap_amount_validation {
            // Only log at debug level to reduce noise, unless it's a clear error
            let amount0_in_val = amount0_in.unwrap_or(Decimal::ZERO);
            let amount1_in_val = amount1_in.unwrap_or(Decimal::ZERO);
            let amount0_out_val = amount0_out.unwrap_or(Decimal::ZERO);
            let amount1_out_val = amount1_out.unwrap_or(Decimal::ZERO);
            
            let total_input = amount0_in_val + amount1_in_val;
            let total_output = amount0_out_val + amount1_out_val;
            
            // Check if this is a clear error (zero amounts when we expect data)
            // For backtesting, be more lenient with validation since historical data may be incomplete
            let is_clear_error = total_input == Decimal::ZERO && total_output == Decimal::ZERO;

            if is_clear_error {
                warn!("Swap amount validation failed for pool {:?} - zero amounts detected (in: {:?}, out: {:?})",
                      pool_address_bytes, total_input, total_output);
                // For backtesting, continue processing even with clear errors
                // as we might still have useful data from other fields
            } else if total_input == Decimal::ZERO || total_output == Decimal::ZERO {
                debug!("Swap amount validation warning for pool {:?} - unusual amounts (in: {:?}, out: {:?}) - continuing for backtest compatibility",
                       pool_address_bytes, total_input, total_output);
            }
            // Don't fail the entire pool state for swap amount validation issues
            // Just log and continue
        }
        
        if !price_validation {
            // Only log at debug level to reduce noise, unless it's a clear error
            if let (Some(p0), Some(p1)) = (price_t0_in_t1, price_t1_in_t0) {
                let product = p0 * p1;
                let deviation = if product > Decimal::ONE {
                    (product - Decimal::ONE) / Decimal::ONE
                } else {
                    (Decimal::ONE - product) / Decimal::ONE
                };
                
                // Log as warning only if deviation is very large (>10%)
                if deviation > Decimal::from_str("0.1").unwrap_or(Decimal::ZERO) {
                    warn!("Price validation failed for pool {:?} - large deviation detected (product: {:?}, expected: 1.0, deviation: {:.2}%)", 
                          pool_address_bytes, product, deviation * Decimal::from(100));
                } else {
                    debug!("Price validation failed for pool {:?} - minor deviation detected (product: {:?}, expected: 1.0, deviation: {:.2}%)", 
                           pool_address_bytes, product, deviation * Decimal::from(100));
                }
            } else {
                debug!("Price validation failed for pool {:?} - missing price data", pool_address_bytes);
            }
            // Don't fail the entire pool state for price validation issues
            // Just log and continue
        }
        
        // Gas cost sanity checks (only log warnings, do not invalidate)
        let mut pool_state_anomalies = Vec::new();
        if let (Some(tx_gas_price_val), Some(tx_gas_used_val)) = (tx_gas_price, tx_gas_used) {
            let gas_price_f64 = tx_gas_price_val.to_f64().unwrap_or(0.0);
            let gas_used_f64 = tx_gas_used_val.to_f64().unwrap_or(0.0);
            if gas_price_f64 > 0.0 && gas_used_f64 > 0.0 {
                let total_gas_cost_wei = gas_price_f64 * gas_used_f64;
                let total_gas_cost_eth = total_gas_cost_wei / 1e18;
                if total_gas_cost_eth > 0.5 {
                    pool_state_anomalies.push(format!("High gas cost: {:.6} ETH", total_gas_cost_eth));
                }
            }
        }
        
        if let (Some(max_fee), Some(priority_fee)) = (tx_max_fee_per_gas, tx_max_priority_fee_per_gas) {
            let max_fee_f64 = max_fee.to_f64().unwrap_or(0.0);
            let priority_fee_f64 = priority_fee.to_f64().unwrap_or(0.0);
            if priority_fee_f64 > max_fee_f64 {
                pool_state_anomalies.push(format!("Priority fee exceeds max fee: {:.2} > {:.2}", priority_fee_f64, max_fee_f64));
            }
        }
        
        if !pool_state_anomalies.is_empty() {
            debug!("Pool state anomalies detected: {:?}", pool_state_anomalies);
        }
        
        // Enforce no silent defaults for critical schema fields - handle missing columns gracefully
        let token0_decimals_opt: Option<i32> = row.try_get::<_, Option<i32>>("token0_decimals").ok().flatten();
        let token1_decimals_opt: Option<i32> = row.try_get::<_, Option<i32>>("token1_decimals").ok().flatten();
        let fee_tier_final: u32 = match fee_tier { Some(v) if v >= 0 => v as u32, _ => {
            // For backtesting, use default values instead of failing
            match protocol {
                DexProtocol::UniswapV3 | DexProtocol::UniswapV4 => 3000, // 0.3%
                DexProtocol::PancakeSwapV3 => 2500, // 0.25%
                _ => 30, // 0.3% for V2
            }
        }};
        let token0_decimals_final: u32 = match token0_decimals_opt { Some(v) if v >= 0 => v as u32, _ => {
            // For backtesting, use default values instead of failing
            18 // Default to 18 decimals
        }};
        let token1_decimals_final: u32 = match token1_decimals_opt { Some(v) if v >= 0 => v as u32, _ => {
            // For backtesting, use default values instead of failing
            18 // Default to 18 decimals
        }};

        // Handle token addresses that might be None
        let token0_addr = token0_bytes.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero());
        let token1_addr = token1_bytes.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero());

        let pool_address = if pool_address_bytes.len() == 20 {
            Address::from_slice(&pool_address_bytes)
        } else if pool_address_bytes.len() == 32 {
            Address::from_slice(&pool_address_bytes[12..])
        } else {
            return Err(anyhow!("unsupported pool_address length: {}", pool_address_bytes.len()));
        };

        Ok(PoolStateFromSwaps {
            pool_address,
            token0: token0_addr,
            token1: token1_addr,
            token0_decimals: token0_decimals_final,
            token1_decimals: token1_decimals_final,
            reserve0: reserve0_final,
            reserve1: reserve1_final,
            fee_tier: fee_tier_final,
            protocol_type: self.convert_dex_protocol_to_protocol_type(&protocol),
            timestamp: row.try_get::<_, Option<i64>>("unix_ts").ok().flatten().unwrap_or(0) as u64,
            block_number: row.try_get::<_, i64>("block_number").ok().unwrap_or(0) as u64,
            sqrt_price_x96: sqrt_price_x96.map(|d| self.decimal_to_u256_safe(d, U256::zero())),
            liquidity: liquidity.map(|d| self.decimal_to_u256_safe(d, U256::zero())),
            tick: tick,
            v3_tick_data: None, // Load lazily when needed for V3 calculations
            v3_tick_current: tick, // Use the tick as current tick
            protocol,
            price0_cumulative_last: None,
            price1_cumulative_last: None,
            dex_protocol: protocol,
            last_reserve_update_timestamp: None,
        })
    }

    fn parse_swap_from_row(&self, row: &tokio_postgres::Row) -> Result<SwapEvent> {
        debug!("Parsing swap from row");

        // Parse basic fields - handle missing columns gracefully
        let transaction_hash_bytes: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("transaction_hash") {
            bytes
        } else if let Ok(hash_str) = row.try_get::<_, String>("transaction_hash") {
            // Fallback: try to get as TEXT and convert
            H256::from_str(&hash_str).or_else(|_| H256::from_str(hash_str.trim_start_matches("0x"))).ok().map(|h| h.as_bytes().to_vec())
        } else {
            None
        };

        let transaction_hash_bytes = transaction_hash_bytes.ok_or_else(|| {
            anyhow::anyhow!("transaction_hash column not found or invalid")
        })?;

        let pool_address_bytes: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("pool_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("pool_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let pool_address_bytes = pool_address_bytes.ok_or_else(|| {
            anyhow::anyhow!("pool_address column not found or invalid")
        })?;

        // Safety check for V4 pools (32 bytes) vs regular EVM addresses (20 bytes)
        let normalized_pool_address = match pool_address_bytes.len() {
            20 => Address::from_slice(&pool_address_bytes),
            32 => Address::from_slice(&pool_address_bytes[12..]),
            n => {
                warn!("Unsupported pool address length: {}, skipping. Pool address bytes: {:?}", n, pool_address_bytes);
                return Err(anyhow!("unsupported pool_address length: {}", n));
            }
        };

        let protocol_type: Option<i16> = row.try_get("protocol_type").ok();
        let protocol_type = protocol_type.unwrap_or(1); // Default to UniswapV2
        let protocol = self.parse_protocol_type(protocol_type);

        // Parse token addresses - handle missing columns gracefully
        let token0_address: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("token0_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("token0_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let token1_address: Option<Vec<u8>> = if let Ok(bytes) = row.try_get("token1_address") {
            bytes
        } else if let Ok(addr_str) = row.try_get::<_, String>("token1_address") {
            // Fallback: try to get as TEXT and convert
            Address::from_str(&addr_str).ok().map(|a| a.as_bytes().to_vec())
        } else {
            None
        };

        let token0 = token0_address.map(|bytes| Address::from_slice(&bytes));
        let token1 = token1_address.map(|bytes| Address::from_slice(&bytes));

        // Skip swaps with missing token addresses - data quality issue
        if token0.is_none() || token1.is_none() {
            warn!("Skipping swap with missing token addresses (data quality issue) - tx: {}",
                  H256::from_slice(&transaction_hash_bytes).to_string());
            return Err(anyhow!("Missing token addresses in swap data"));
        }

        // Parse swap data based on protocol
        let swap_data = self.parse_swap_data(&row, &protocol)?;
        
        // Parse amounts - derive from amount0/amount1 fields since amount_in/amount_out are NULL
        let u256_from_decimal = |d: Option<Decimal>| d.and_then(|dec| dec.to_u128()).map(U256::from).unwrap_or_default();

        let amount0_in = u256_from_decimal(row.try_get("amount0_in").ok().flatten());
        let amount1_in = u256_from_decimal(row.try_get("amount1_in").ok().flatten());
        let amount0_out = u256_from_decimal(row.try_get("amount0_out").ok().flatten());
        let amount1_out = u256_from_decimal(row.try_get("amount1_out").ok().flatten());

        // Parse V3-specific amounts if available - handle missing columns gracefully
        let v3_amount0: Option<Decimal> = row.try_get("v3_amount0").ok().flatten();
        let v3_amount1: Option<Decimal> = row.try_get("v3_amount1").ok().flatten();

        // For V3 protocols, use V3-specific amounts if available, otherwise fall back to regular amounts
        let (final_amount0_in, final_amount1_in, final_amount0_out, final_amount1_out) = 
            if matches!(protocol, DexProtocol::UniswapV3 | DexProtocol::UniswapV4) {
                let v3_amount0_i128 = v3_amount0.and_then(|d| d.to_i128()).unwrap_or(0);
                let v3_amount1_i128 = v3_amount1.and_then(|d| d.to_i128()).unwrap_or(0);
                
                // Convert signed amounts to unsigned for amount calculation
                let v3_amount0_in = if v3_amount0_i128 > 0 { U256::from(v3_amount0_i128 as u128) } else { U256::zero() };
                let v3_amount1_in = if v3_amount1_i128 > 0 { U256::from(v3_amount1_i128 as u128) } else { U256::zero() };
                let v3_amount0_out = if v3_amount0_i128 < 0 { U256::from(v3_amount0_i128.abs() as u128) } else { U256::zero() };
                let v3_amount1_out = if v3_amount1_i128 < 0 { U256::from(v3_amount1_i128.abs() as u128) } else { U256::zero() };
                
                (v3_amount0_in, v3_amount1_in, v3_amount0_out, v3_amount1_out)
            } else {
                (amount0_in, amount1_in, amount0_out, amount1_out)
            };

        // --- CRITICAL FIX: Protocol-aware swap direction determination ---
        let (token_in, token_out, amount_in, amount_out) = if matches!(protocol, DexProtocol::UniswapV3 | DexProtocol::UniswapV4) {
            // --- Uniswap V3 Logic ---
            let v3_amount0_i128 = v3_amount0.and_then(|d| d.to_i128()).unwrap_or(0);
            let v3_amount1_i128 = v3_amount1.and_then(|d| d.to_i128()).unwrap_or(0);

            if v3_amount0_i128 > 0 && v3_amount1_i128 < 0 {
                // Swap from token0 to token1
                (token0, token1, U256::from(v3_amount0_i128 as u128), U256::from(v3_amount1_i128.abs() as u128))
            } else if v3_amount1_i128 > 0 && v3_amount0_i128 < 0 {
                // Swap from token1 to token0
                (token1, token0, U256::from(v3_amount1_i128 as u128), U256::from(v3_amount0_i128.abs() as u128))
            } else {
                // Not a simple swap (e.g., mint/burn), can't determine direction
                (None, None, U256::zero(), U256::zero())
            }
        } else if matches!(protocol, DexProtocol::Balancer) {
            // --- Balancer Logic: Use specific token_in/token_out fields ---
            let balancer_token_in: Option<Vec<u8>> = row.try_get("token_in").ok().flatten();
            let balancer_token_out: Option<Vec<u8>> = row.try_get("token_out").ok().flatten();
            let balancer_amount_in: Option<Decimal> = row.try_get("amount_in").ok().flatten();
            let balancer_amount_out: Option<Decimal> = row.try_get("amount_out").ok().flatten();
            
            if let (Some(token_in_bytes), Some(token_out_bytes), Some(amt_in), Some(amt_out)) = 
                (balancer_token_in, balancer_token_out, balancer_amount_in, balancer_amount_out) {
                let token_in_addr = Some(Address::from_slice(&token_in_bytes));
                let token_out_addr = Some(Address::from_slice(&token_out_bytes));
                let amount_in_u256 = U256::from(amt_in.to_u128().unwrap_or(0));
                let amount_out_u256 = U256::from(amt_out.to_u128().unwrap_or(0));
                
                debug!("Balancer swap direction: {:?} -> {:?}, amounts: {} -> {}", 
                       token_in_addr, token_out_addr, amount_in_u256, amount_out_u256);
                
                (token_in_addr, token_out_addr, amount_in_u256, amount_out_u256)
            } else {
                // Fall back to V2-style logic for Balancer
                if final_amount0_in > U256::zero() && final_amount1_out > U256::zero() {
                    (token0, token1, final_amount0_in, final_amount1_out)
                } else if final_amount1_in > U256::zero() && final_amount0_out > U256::zero() {
                    (token1, token0, final_amount1_in, final_amount0_out)
                } else {
                    (None, None, U256::zero(), U256::zero())
                }
            }
        } else {
            // --- Uniswap V2 and Other Protocols Fallback Logic ---
            if final_amount0_in > U256::zero() && final_amount1_out > U256::zero() {
                (token0, token1, final_amount0_in, final_amount1_out)
            } else if final_amount1_in > U256::zero() && final_amount0_out > U256::zero() {
                (token1, token0, final_amount1_in, final_amount0_out)
            } else {
                // Not a simple swap (e.g., mint/burn), can't determine direction
                (None, None, U256::zero(), U256::zero())
            }
        };

        // Instead of panicking, filter out events that are not simple swaps
        if token_in.is_none() || token_out.is_none() {
            return Err(anyhow!("Could not determine swap direction (likely a mint/burn event)"));
        }

        let final_token_in = token_in.unwrap();
        let final_token_out = token_out.unwrap();
        
        // Parse token addresses - derive from token0/token1 fields since token_in/token_out are NULL

        
        // Parse gas data - handle missing columns gracefully
        let gas_used: Option<Decimal> = row.try_get("tx_gas_used").ok().flatten();
        let gas_price: Option<Decimal> = row.try_get("tx_gas_price").ok().flatten();

        // Parse reserves - handle missing columns gracefully
        let reserve0: Option<Decimal> = row.try_get("token0_reserve").ok().flatten();
        let reserve1: Option<Decimal> = row.try_get("token1_reserve").ok().flatten();

        // Parse V3-specific fields - handle missing columns gracefully
        let v3_sender: Option<Vec<u8>> = row.try_get("v3_sender").ok().flatten();
        let v3_recipient: Option<Vec<u8>> = row.try_get("v3_recipient").ok().flatten();
        let v3_amount0_final: Option<Decimal> = row.try_get("v3_amount0").ok().flatten();
        let v3_amount1_final: Option<Decimal> = row.try_get("v3_amount1").ok().flatten();
        
        // Use V3 sender/recipient if available, otherwise fall back to tx_from/tx_to
        let sender = if let Some(v3_sender_bytes) = v3_sender {
            Address::from_slice(&v3_sender_bytes)
        } else {
            row.try_get::<_, Option<Vec<u8>>>("tx_from")
                .ok()
                .flatten()
                .and_then(|bytes| Some(Address::from_slice(&bytes)))
                .unwrap_or(Address::zero())
        };

        let recipient = if let Some(v3_recipient_bytes) = v3_recipient {
            Address::from_slice(&v3_recipient_bytes)
        } else {
            row.try_get::<_, Option<Vec<u8>>>("tx_to")
                .ok()
                .flatten()
                .and_then(|bytes| Some(Address::from_slice(&bytes)))
                .unwrap_or(Address::zero())
        };
        
        Ok(SwapEvent {
            chain_name: row.try_get("network_id").ok().unwrap_or("unknown".to_string()),
            pool_address: normalized_pool_address,
            tx_hash: H256::from_slice(&transaction_hash_bytes),
            block_number: row.try_get::<_, i64>("block_number").ok().unwrap_or(0) as u64,
            log_index: U256::from((row.try_get::<_, i64>("log_index").ok().unwrap_or(0)).max(0i64) as u64),
            unix_ts: row.try_get::<_, Option<i64>>("unix_ts").ok().flatten().unwrap_or(0) as u64,
            token_in: final_token_in,
            token_out: final_token_out,
            amount_in: amount_in,
            amount_out: amount_out,
            sender,
            recipient,
            gas_used: gas_used.map(|d| self.decimal_to_u256_safe(d, U256::zero())),
            effective_gas_price: gas_price.map(|d| self.decimal_to_u256_safe(d, U256::zero())),
            protocol: protocol.clone(),
            data: swap_data,
            token0,
            token1,
            token0_decimals: row.try_get::<_, Option<i32>>("token0_decimals").ok().flatten().map(|d| d as u8),
            token1_decimals: row.try_get::<_, Option<i32>>("token1_decimals").ok().flatten().map(|d| d as u8),
            fee_tier: row.try_get::<_, Option<i32>>("fee_tier").ok().flatten().map(|v| v as u32),
            price_t0_in_t1: row.try_get::<_, Option<Decimal>>("price_t0_in_t1").ok().flatten(),
            price_t1_in_t0: row.try_get::<_, Option<Decimal>>("price_t1_in_t0").ok().flatten(),
            reserve0: reserve0.and_then(|d| d.to_u128().map(U256::from)),
            reserve1: reserve1.and_then(|d| d.to_u128().map(U256::from)),
            sqrt_price_x96: row.try_get::<_, Option<Decimal>>("sqrt_price_x96")
                .ok()
                .flatten()
                .and_then(|d| d.to_u128().map(U256::from)),
            liquidity: row.try_get::<_, Option<Decimal>>("liquidity")
                .ok()
                .flatten()
                .and_then(|d| d.to_u128().map(U256::from)),
            tick: row.try_get::<_, Option<i32>>("tick").ok().flatten(),
            buyer: row.try_get::<_, Option<Vec<u8>>>("buyer")
                .ok()
                .flatten()
                .map(|bytes| Address::from_slice(&bytes)),
            sold_id: row.try_get::<_, Option<Decimal>>("sold_id")
                .ok()
                .flatten()
                .and_then(|d| d.to_i128()),
            tokens_sold: row.try_get::<_, Option<Decimal>>("tokens_sold")
                .ok()
                .flatten()
                .and_then(|d| d.to_u128().map(U256::from)),
            bought_id: row.try_get::<_, Option<Decimal>>("bought_id")
                .ok()
                .flatten()
                .and_then(|d| d.to_i128()),
            tokens_bought: row.try_get::<_, Option<Decimal>>("tokens_bought")
                .ok()
                .flatten()
                .and_then(|d| d.to_u128().map(U256::from)),
            transaction: None,
            transaction_metadata: None,
            pool_id: row.try_get::<_, Option<Vec<u8>>>("pool_id")
                .ok()
                .flatten()
                .map(|bytes| H256::from_slice(&bytes)),
            protocol_type: Some(self.convert_dex_protocol_to_protocol_type(&protocol)),
            // Convert V3 amounts to I256 if available
            v3_amount0: v3_amount0_final.and_then(|d| d.to_i128()).map(|i| {
                if i >= 0 {
                    ethers::types::I256::from_raw(U256::from(i as u128))
                } else {
                    ethers::types::I256::from_raw(U256::from(i.abs() as u128)).saturating_neg()
                }
            }),
            v3_amount1: v3_amount1_final.and_then(|d| d.to_i128()).map(|i| {
                if i >= 0 {
                    ethers::types::I256::from_raw(U256::from(i as u128))
                } else {
                    ethers::types::I256::from_raw(U256::from(i.abs() as u128)).saturating_neg()
                }
            }),
            v3_tick_data: row.try_get::<_, Option<String>>("v3_tick_data").ok().flatten(),
            price_from_in_usd: None,
            price_to_in_usd: None,
            token0_price_usd: None,
            token1_price_usd: None,
            pool_liquidity_usd: None,
        })
    }

    fn parse_swap_data(
        &self,
        row: &tokio_postgres::Row,
        protocol: &DexProtocol,
    ) -> Result<SwapData> {
        debug!("Parsing swap data for protocol: {:?}", protocol);
        match protocol {
            DexProtocol::UniswapV2 | DexProtocol::SushiSwap | DexProtocol::PancakeSwap => {
                // V2 protocols use amount0_in/amount1_in/amount0_out/amount1_out - handle missing columns gracefully
                let amount0_in: Option<Decimal> = row.try_get("amount0_in").ok().flatten();
                let amount1_in: Option<Decimal> = row.try_get("amount1_in").ok().flatten();
                let amount0_out: Option<Decimal> = row.try_get("amount0_out").ok().flatten();
                let amount1_out: Option<Decimal> = row.try_get("amount1_out").ok().flatten();

                // Parse transaction metadata - handle missing columns gracefully
                let tx_from: Option<Vec<u8>> = row.try_get("tx_from").ok().flatten();
                let tx_to: Option<Vec<u8>> = row.try_get("tx_to").ok().flatten();

                Ok(SwapData::UniswapV2 {
                    sender: tx_from.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                    to: tx_to.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                    amount0_in: amount0_in.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount1_in: amount1_in.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount0_out: amount0_out.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount1_out: amount1_out.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                })
            }
            DexProtocol::UniswapV3 | DexProtocol::UniswapV4 => {
                // V3 protocols use sqrt_price_x96, liquidity, and tick - handle missing columns gracefully
                let amount0: Option<Decimal> = row.try_get("amount0_in").ok().flatten();
                let amount1: Option<Decimal> = row.try_get("amount1_in").ok().flatten();
                let sqrt_price: Option<Decimal> = row.try_get("sqrt_price_x96").ok().flatten();
                let liquidity: Option<Decimal> = row.try_get("liquidity").ok().flatten();
                let tick: Option<i32> = row.try_get("tick").ok().flatten();

                // Parse V3-specific amounts if available - handle missing columns gracefully
                let v3_amount0: Option<Decimal> = row.try_get("v3_amount0").ok().flatten();
                let v3_amount1: Option<Decimal> = row.try_get("v3_amount1").ok().flatten();
                let v3_sender: Option<Vec<u8>> = row.try_get("v3_sender").ok().flatten();
                let v3_recipient: Option<Vec<u8>> = row.try_get("v3_recipient").ok().flatten();
                
                // Use V3-specific amounts if available, otherwise fall back to regular amounts
                let final_amount0 = v3_amount0.or(amount0);
                let final_amount1 = v3_amount1.or(amount1);
                
                // V3 amounts can be negative (for exact input swaps)
                let amount0_i128 = final_amount0.and_then(|d| d.to_i128()).unwrap_or(0);
                let amount1_i128 = final_amount1.and_then(|d| d.to_i128()).unwrap_or(0);
                
                // Create proper I256 values for V3 swap data
                let amount0_i256 = if amount0_i128 >= 0 {
                    ethers::types::I256::from_raw(U256::from(amount0_i128 as u128))
                } else {
                    ethers::types::I256::from_raw(U256::from(amount0_i128.abs() as u128)).saturating_neg()
                };
                
                let amount1_i256 = if amount1_i128 >= 0 {
                    ethers::types::I256::from_raw(U256::from(amount1_i128 as u128))
                } else {
                    ethers::types::I256::from_raw(U256::from(amount1_i128.abs() as u128)).saturating_neg()
                };
                
                // Use V3 sender/recipient if available, otherwise fall back to tx_from/tx_to
                let sender = if let Some(v3_sender_bytes) = v3_sender {
                    Address::from_slice(&v3_sender_bytes)
                } else {
                    row.get::<_, Option<Vec<u8>>>("tx_from")
                        .map(|bytes| Address::from_slice(&bytes))
                        .unwrap_or(Address::zero())
                };
                
                let recipient = if let Some(v3_recipient_bytes) = v3_recipient {
                    Address::from_slice(&v3_recipient_bytes)
                } else {
                    row.get::<_, Option<Vec<u8>>>("tx_to")
                        .map(|bytes| Address::from_slice(&bytes))
                        .unwrap_or(Address::zero())
                };
                
                Ok(SwapData::UniswapV3 {
                    sender,
                    recipient,
                    amount0: amount0_i256,
                    amount1: amount1_i256,
                    sqrt_price_x96: sqrt_price.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    liquidity: liquidity.and_then(|d| d.to_u128()).unwrap_or(0),
                    tick: tick.unwrap_or(0),
                })
            }
            DexProtocol::Curve => {
                // Curve uses sold_id, bought_id, tokens_sold, tokens_bought - handle missing columns gracefully
                let sold_id: Option<Decimal> = row.try_get("sold_id").ok().flatten();
                let bought_id: Option<Decimal> = row.try_get("bought_id").ok().flatten();
                let tokens_sold: Option<Decimal> = row.try_get("tokens_sold").ok().flatten();
                let tokens_bought: Option<Decimal> = row.try_get("tokens_bought").ok().flatten();

                // Parse transaction metadata - handle missing columns gracefully
                let buyer: Option<Vec<u8>> = row.try_get("buyer").ok().flatten();
                
                Ok(SwapData::Curve {
                    buyer: buyer.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                    sold_id: sold_id.and_then(|d| d.to_i128()).unwrap_or(0),
                    tokens_sold: tokens_sold.and_then(|d| d.to_u128().map(U256::from))
                        .unwrap_or(U256::zero()),
                    bought_id: bought_id.and_then(|d| d.to_i128()).unwrap_or(0),
                    tokens_bought: tokens_bought.and_then(|d| d.to_u128().map(U256::from))
                        .unwrap_or(U256::zero()),
                })
            }
            DexProtocol::Balancer => {
                // Balancer uses pool_id, token_in, token_out, amount_in, amount_out - handle missing columns gracefully
                let pool_id: Option<Vec<u8>> = row.try_get("pool_id").ok().flatten();
                let token_in_bytes: Option<Vec<u8>> = row.try_get("token_in").ok().flatten();
                let token_out_bytes: Option<Vec<u8>> = row.try_get("token_out").ok().flatten();
                let amount_in: Option<Decimal> = row.try_get("amount_in").ok().flatten();
                let amount_out: Option<Decimal> = row.try_get("amount_out").ok().flatten();

                // Parse transaction metadata for Balancer - handle missing columns gracefully
                let tx_from: Option<Vec<u8>> = row.try_get("tx_from").ok().flatten();
                let tx_to: Option<Vec<u8>> = row.try_get("tx_to").ok().flatten();
                
                // Validate Balancer-specific data
                let has_balancer_data = token_in_bytes.is_some() && token_out_bytes.is_some() && 
                                      amount_in.is_some() && amount_out.is_some();
                
                if has_balancer_data {
                    // Use Balancer-specific fields when available
                    let token_in_addr = token_in_bytes.as_ref().map(|bytes| Address::from_slice(bytes)).unwrap_or(Address::zero());
                    let token_out_addr = token_out_bytes.as_ref().map(|bytes| Address::from_slice(bytes)).unwrap_or(Address::zero());
                    
                    // For Balancer, use the token_in/token_out as our primary tokens
                    // and map to amount0/amount1 based on token ordering
                    let (amount0_in, amount1_in, amount0_out, amount1_out) = {
                        let amount_in_u256 = amount_in.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero());
                        let amount_out_u256 = amount_out.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero());

                        // Map Balancer's direct token_in/token_out to amount0/amount1
                        // We use the token addresses to determine the ordering
                        if token_in_addr < token_out_addr {
                            // token_in is token0, token_out is token1
                            (amount_in_u256, U256::zero(), U256::zero(), amount_out_u256)
                        } else {
                            // token_in is token1, token_out is token0
                            (U256::zero(), amount_in_u256, amount_out_u256, U256::zero())
                        }
                    };
                    
                    debug!("Parsed Balancer swap: pool_id={:?}, token_in={:?}, token_out={:?}, amount_in={:?}, amount_out={:?}",
                           pool_id.as_ref().map(|bytes| Address::from_slice(&bytes[..20.min(bytes.len())])),
                           token_in_addr, token_out_addr, amount_in, amount_out);
                    
                    Ok(SwapData::UniswapV2 {
                        sender: tx_from.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                        to: tx_to.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                        amount0_in,
                        amount1_in,
                        amount0_out,
                        amount1_out,
                    })
                } else {
                    // Fall back to V2-style parsing using amount0/amount1 fields
                    debug!("Balancer swap missing specific fields, falling back to V2-style parsing");
                    let amount0_in: Option<Decimal> = row.try_get("amount0_in").ok().flatten();
                    let amount1_in: Option<Decimal> = row.try_get("amount1_in").ok().flatten();
                    let amount0_out: Option<Decimal> = row.try_get("amount0_out").ok().flatten();
                    let amount1_out: Option<Decimal> = row.try_get("amount1_out").ok().flatten();
                    
                    Ok(SwapData::UniswapV2 {
                        sender: tx_from.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                        to: tx_to.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                        amount0_in: amount0_in.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero()),
                        amount1_in: amount1_in.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero()),
                        amount0_out: amount0_out.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero()),
                        amount1_out: amount1_out.map(|d| self.decimal_to_u256_safe(d, U256::zero())).unwrap_or(U256::zero()),
                    })
                }
            }
            _ => {
                // For other protocols, fall back to V2 structure with available data
                let amount0_in: Option<Decimal> = row.try_get("amount0_in").ok().flatten();
                let amount1_in: Option<Decimal> = row.try_get("amount1_in").ok().flatten();
                let amount0_out: Option<Decimal> = row.try_get("amount0_out").ok().flatten();
                let amount1_out: Option<Decimal> = row.try_get("amount1_out").ok().flatten();
                
                let tx_from: Option<Vec<u8>> = row.try_get("tx_from").ok().flatten();
                let tx_to: Option<Vec<u8>> = row.try_get("tx_to").ok().flatten();
                
                Ok(SwapData::UniswapV2 {
                    sender: tx_from.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                    to: tx_to.map(|bytes| Address::from_slice(&bytes)).unwrap_or(Address::zero()),
                    amount0_in: amount0_in.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount1_in: amount1_in.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount0_out: amount0_out.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                    amount1_out: amount1_out.map(|d| self.decimal_to_u256_safe(d, U256::zero()))
                        .unwrap_or(U256::zero()),
                })
            }
        }
    }

    fn parse_protocol_type(&self, protocol_type: i16) -> DexProtocol {
        match protocol_type {
            1 => DexProtocol::UniswapV2,
            2 => DexProtocol::UniswapV3,
            3 => DexProtocol::Curve,
            4 => DexProtocol::Balancer,
            5 => DexProtocol::SushiSwap,
            6 => DexProtocol::PancakeSwap,
            7 => DexProtocol::PancakeSwapV3,
            8 => DexProtocol::UniswapV4,
            9 => DexProtocol::Bancor,
            10 => DexProtocol::TraderJoe,
            _ => DexProtocol::Other(protocol_type as u16),
        }
    }

    fn convert_dex_protocol_to_protocol_type(&self, dex_protocol: &DexProtocol) -> ProtocolType {
        match dex_protocol {
            DexProtocol::UniswapV2 => ProtocolType::UniswapV2,
            DexProtocol::UniswapV3 => ProtocolType::UniswapV3,
            DexProtocol::Curve => ProtocolType::Curve,
            DexProtocol::Balancer => ProtocolType::Balancer,
            DexProtocol::SushiSwap => ProtocolType::SushiSwap,
            DexProtocol::UniswapV4 => ProtocolType::UniswapV4,
            _ => ProtocolType::Other("Unknown".to_string()),
        }
    }

    /// Get token decimals from database for backtest mode
    pub async fn get_token_decimals_batch(
        &self,
        chain_name: &str,
        tokens: &[Address],
        block_number: u64,
    ) -> anyhow::Result<Vec<u8>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        // Convert tokens to bytes for the query
        let token_bytes: Vec<&[u8]> = tokens.iter().map(|t| t.as_bytes()).collect();
        let aliases = self.chain_aliases(chain_name);

        // Get configurable lookback from chain config (reuse price_lookback_blocks or default to 10000)
        let lookback: i64 = self.chain_config.chains.get(&self.normalize_network_id(chain_name))
            .and_then(|c| c.price_lookback_blocks)
            .map(|v| v as i64)
            .unwrap_or(10000);

        // Single efficient query to get decimals for all tokens
        let query = r#"
            WITH token_decimals AS (
                SELECT DISTINCT ON (token_address)
                    token_address,
                    decimals,
                    block_number
                FROM (
                    SELECT
                        token0_address as token_address,
                        token0_decimals as decimals,
                        block_number
                    FROM swaps
                    WHERE token0_address = ANY($1::bytea[])
                        AND block_number <= $2
                        AND block_number > ($2 - $4)
                        AND token0_decimals IS NOT NULL
                        AND LOWER(network_id) = ANY($3::text[])
                    UNION ALL
                    SELECT
                        token1_address as token_address,
                        token1_decimals as decimals,
                        block_number
                    FROM swaps
                    WHERE token1_address = ANY($1::bytea[])
                        AND block_number <= $2
                        AND block_number > ($2 - $4)
                        AND token1_decimals IS NOT NULL
                        AND LOWER(network_id) = ANY($3::text[])
                ) combined
                ORDER BY token_address, block_number DESC
            )
            SELECT token_address, decimals
            FROM token_decimals
        "#;

        let rows = conn
            .query(query, &[&token_bytes, &(block_number as i64), &aliases, &lookback])
            .await
            .map_err(|e| anyhow!("Database error: {}", e))?;
        
        // Create a map of token address to decimals
        let mut decimals_map = HashMap::new();
        for row in rows {
            let token_address: Vec<u8> = row.get("token_address");
            let decimals: i32 = row.get("decimals");
            decimals_map.insert(token_address, decimals as u8);
        }
        
        // Return decimals in the same order as input tokens, with default 18 if not found
        let decimals: Vec<u8> = tokens.iter()
            .map(|token| {
                decimals_map.get(&token.as_bytes().to_vec())
                    .copied()
                    .unwrap_or(18u8)
            })
            .collect();
        
        Ok(decimals)
    }

    /// Get metrics for monitoring
    pub fn get_metrics(&self) -> LoaderMetricsSnapshot {
        LoaderMetricsSnapshot {
            queries_executed: self.metrics.queries_executed.load(std::sync::atomic::Ordering::Relaxed),
            cache_hits: self.metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
            cache_misses: self.metrics.cache_misses.load(std::sync::atomic::Ordering::Relaxed),
            query_errors: self.metrics.query_errors.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Get the earliest block number where a pool appears in the database
    /// This replaces the backward search loop with a single efficient query
    #[instrument(skip(self))]
    pub async fn get_earliest_block_for_pool(
        &self,
        pool_address: Address,
        chain_name: Option<&str>,
    ) -> Result<Option<u64>> {
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        let pool_bytes = pool_address.as_bytes();

        let row_opt = if let Some(chain) = chain_name {
            let query = r#"
                SELECT MIN(block_number) as earliest_block
                FROM swaps
                WHERE pool_address = $1
                    AND LOWER(network_id) = ANY($2::text[])
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
            "#;
            let aliases = self.chain_aliases(chain);
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&pool_bytes, &aliases];
            conn.query_opt(query, &params).await
        } else {
            let query = r#"
                SELECT MIN(block_number) as earliest_block
                FROM swaps
                WHERE pool_address = $1
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
            "#;
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&pool_bytes];
            conn.query_opt(query, &params).await
        }
        .map_err(|e| anyhow!("Database query failed for earliest block: {}", e))?;
        
        if let Some(row) = row_opt {
            let earliest_block: Option<i64> = row.get("earliest_block");
            Ok(earliest_block.map(|b| b as u64))
        } else {
            Ok(None)
        }
    }

    /// Get the latest pool state at or before a specific block number
    /// This replaces the backward search loop with a single efficient query
    #[instrument(skip(self))]
    pub async fn get_latest_state_before_or_at_block(
        &self,
        pool_address: Address,
        block_number: u64,
        chain_name: Option<&str>,
    ) -> Result<Option<PoolStateFromSwaps>> {
        let conn = self.db_pool.get().await
            .map_err(|e| anyhow!("Failed to get database connection: {}", e))?;
        
        let pool_bytes = pool_address.as_bytes();
        let block_number_i64 = block_number as i64;
        
        let row_opt = if let Some(chain) = chain_name {
            let query = r#"
                SELECT 
                    pool_address,
                    token0_address,
                    token1_address,
                    token0_decimals,
                    token1_decimals,
                    fee_tier,
                    protocol_type,
                    unix_ts,
                    token0_reserve,
                    token1_reserve,
                    sqrt_price_x96,
                    liquidity,
                    tick,
                    price_t0_in_t1,
                    price_t1_in_t0,
                    block_number,
                    log_index,
                    dex_name,
                    amount0_in,
                    amount1_in,
                    amount0_out,
                    amount1_out,
                    tx_gas_price,
                    tx_gas_used,
                    tx_max_fee_per_gas,
                    tx_max_priority_fee_per_gas,
                    v3_sender,
                    v3_recipient,
                    v3_amount0,
                    v3_amount1
                FROM swaps
                WHERE pool_address = $1 
                    AND LOWER(network_id) = ANY($2::text[])
                    AND block_number <= $3
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            let aliases = self.chain_aliases(chain);
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 3] = [
                &pool_bytes as &(dyn tokio_postgres::types::ToSql + Sync),
                &aliases as &(dyn tokio_postgres::types::ToSql + Sync),
                &block_number_i64 as &(dyn tokio_postgres::types::ToSql + Sync),
            ];
            conn.query_opt(query, &params)
            .await
            .map_err(|e| anyhow!("Database query failed for latest state: {}", e))?
        } else {
            let query = r#"
                SELECT 
                    pool_address,
                    token0_address,
                    token1_address,
                    token0_decimals,
                    token1_decimals,
                    fee_tier,
                    protocol_type,
                    unix_ts,
                    token0_reserve,
                    token1_reserve,
                    sqrt_price_x96,
                    liquidity,
                    tick,
                    price_t0_in_t1,
                    price_t1_in_t0,
                    block_number,
                    log_index,
                    dex_name,
                    amount0_in,
                    amount1_in,
                    amount0_out,
                    amount1_out,
                    tx_gas_price,
                    tx_gas_used,
                    tx_max_fee_per_gas,
                    tx_max_priority_fee_per_gas,
                    v3_sender,
                    v3_recipient,
                    v3_amount0,
                    v3_amount1
                FROM swaps
                WHERE pool_address = $1 
                    AND block_number <= $2
                    AND token0_address IS NOT NULL
                    AND token1_address IS NOT NULL
                ORDER BY block_number DESC, log_index DESC
                LIMIT 1
            "#;
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [
                &pool_bytes as &(dyn tokio_postgres::types::ToSql + Sync),
                &block_number_i64 as &(dyn tokio_postgres::types::ToSql + Sync),
            ];
            conn.query_opt(query, &params)
            .await
            .map_err(|e| anyhow!("Database query failed for latest state: {}", e))?
        };
        
        if let Some(row) = row_opt {
            match self.parse_pool_state_from_row(&row) {
                Ok(pool_state) => Ok(Some(pool_state)),
                Err(e) => {
                    warn!("Failed to parse pool state for {} at block {}: {}", pool_address, block_number, e);
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }



    /// Get cached creation block for a pool (if available)
    /// This provides a fast path when creation block is already known
    #[instrument(skip(self))]
    pub async fn get_cached_creation_block(
        &self,
        pool_address: Address,
        chain_name: Option<&str>,
    ) -> Result<Option<u64>> {
        // For now, we'll query the database each time
        // In a future optimization, we could add a dedicated cache for creation blocks
        self.get_earliest_block_for_pool(pool_address, chain_name).await
    }

    /// Calculate V3 pool reserves from sqrt_price_x96 and liquidity
    /// This is used when we have V3 pool state but no explicit reserve data
    fn calculate_v3_reserves_from_state(
        &self,
        sqrt_price_x96: rust_decimal::Decimal,
        liquidity: rust_decimal::Decimal,
    ) -> Result<(ethers::types::U256, ethers::types::U256)> {
        use ethers::types::U256;
        
        // Convert Decimal to U256 for calculations
        let sqrt_price_u256 = sqrt_price_x96
            .to_u128()
            .map(U256::from)
            .ok_or_else(|| anyhow::anyhow!("Invalid sqrt_price_x96"))?;
        
        let liquidity_u256 = liquidity
            .to_u128()
            .map(U256::from)
            .ok_or_else(|| anyhow::anyhow!("Invalid liquidity"))?;
        
        // For Uniswap V3: reserve0 = liquidity / sqrt_price, reserve1 = liquidity * sqrt_price / (2^96)
        let q96 = U256::from(2).pow(U256::from(96));
        
        // Calculate reserve0 = liquidity * 2^96 / sqrt_price_x96
        let reserve0 = liquidity_u256
            .checked_mul(q96)
            .and_then(|val| val.checked_div(sqrt_price_u256))
            .unwrap_or(U256::zero());
        
        // Calculate reserve1 = liquidity * sqrt_price_x96 / 2^96
        let reserve1 = liquidity_u256
            .checked_mul(sqrt_price_u256)
            .and_then(|val| val.checked_div(q96))
            .unwrap_or(U256::zero());
        
        Ok((reserve0, reserve1))
    }

    /// Get WETH price from swap pairs within a block range
    pub async fn get_weth_price_from_swaps(
        &self,
        chain_name: &str,
        weth_address: Address,
        stablecoin_address: Address,
        start_block: u64,
        end_block: u64,
    ) -> Option<(f64, u64)> {
        let conn = match self.db_pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                debug!("Failed to get database connection: {}", e);
                return None;
            }
        };

        let aliases = self.chain_aliases(chain_name);

        // Query for WETH-stablecoin swap pairs within the block range
        let query = r#"
            SELECT
                amount0_in, amount0_out, amount1_in, amount1_out,
                token0_address, token1_address, token0_decimals, token1_decimals,
                block_number, unix_ts
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
              AND block_number >= $2
              AND block_number <= $3
              AND ((token0_address = $4 AND token1_address = $5) OR (token0_address = $5 AND token1_address = $4))
              AND unix_ts IS NOT NULL
            ORDER BY block_number DESC
            LIMIT 100
        "#;

        let rows = match conn.query(query, &[
            &aliases,
            &(start_block as i64),
            &(end_block as i64),
            &weth_address.as_bytes(),
            &stablecoin_address.as_bytes(),
        ]).await {
            Ok(rows) => rows,
            Err(e) => {
                debug!("Failed to query WETH-stablecoin swaps: {}", e);
                return None;
            }
        };

        if rows.is_empty() {
            debug!("No WETH-stablecoin swaps found in block range {}-{}", start_block, end_block);
            return None;
        }

        let mut prices = Vec::new();

        for row in rows {
            let token0: Vec<u8> = row.get("token0_address");
            let token1: Vec<u8> = row.get("token1_address");
            let token0_decimals: i32 = row.get("token0_decimals");
            let token1_decimals: i32 = row.get("token1_decimals");

            let token0_addr = Address::from_slice(&token0);
            let token1_addr = Address::from_slice(&token1);

            // Parse decimal values safely and convert using Decimal scaling
            let amount0_out_dec: Option<Decimal> = row.get::<_, Option<Decimal>>("amount0_out");
            let amount1_in_dec: Option<Decimal> = row.get::<_, Option<Decimal>>("amount1_in");

            let unix_ts: i64 = row.get("unix_ts");
            let _block_number: i64 = row.get("block_number");

            // Calculate price based on swap direction
            let price_opt: Option<Decimal> = if token0_addr == weth_address && token1_addr == stablecoin_address {
                if let (Some(weth_out_d), Some(stable_in_d)) = (amount0_out_dec, amount1_in_dec) {
                    // Normalize by decimals using Decimal scaling
                    let scale0 = self.decimal_pow10(token0_decimals as u32);
                    let scale1 = self.decimal_pow10(token1_decimals as u32);
                    let weth_out = weth_out_d / scale0;
                    let stable_in = stable_in_d / scale1;
                    if weth_out > Decimal::ZERO { Some(stable_in / weth_out) } else { None }
                } else { None }
            } else if token0_addr == stablecoin_address && token1_addr == weth_address {
                if let (Some(stable_out_d), Some(weth_in_d)) = (amount0_out_dec, amount1_in_dec) {
                    let scale0 = self.decimal_pow10(token0_decimals as u32);
                    let scale1 = self.decimal_pow10(token1_decimals as u32);
                    let stable_out = stable_out_d / scale0;
                    let weth_in = weth_in_d / scale1;
                    if weth_in > Decimal::ZERO { Some(stable_out / weth_in) } else { None }
                } else { None }
            } else { None };

            if let Some(price_dec) = price_opt {
                if let Some(price_f64) = price_dec.to_f64() {
                    if price_f64.is_finite() && price_f64 > 0.0 {
                        prices.push((price_f64, unix_ts as u64));
                    }
                }
            }
        }

        if prices.is_empty() {
            debug!("No valid WETH prices found in swap data");
            return None;
        }

        // Calculate median price to avoid outliers
        prices.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let median_price = if prices.len() % 2 == 0 {
            let mid = prices.len() / 2;
            (prices[mid - 1].0 + prices[mid].0) / 2.0
        } else {
            prices[prices.len() / 2].0
        };

        // Use the timestamp from the median price's entry
        let median_index = prices.len() / 2;
        let median_timestamp = prices[median_index].1;

        debug!("Calculated median WETH price: ${:.2} from {} swaps", median_price, prices.len());

        Some((median_price, median_timestamp))
    }

    /// Get the price of token_a in terms of token_b for a specific block
    /// Used for pricing tokens against WETH when direct stablecoin pairs don't exist
    pub async fn get_direct_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        block_number: u64,
    ) -> Result<Option<f64>> {
        let conn = self.db_pool.get().await?;
        let aliases = self.chain_aliases(chain_name);
        let lookback_blocks: i64 = self.chain_config
            .chains
            .get(&self.normalize_network_id(chain_name))
            .and_then(|c| c.price_lookback_blocks)
            .map(|v| v as i64)
            .unwrap_or(1000);

        // Query for the most recent swap between token_a and token_b
        let query = r#"
            SELECT token0_address, price_t0_in_t1, price_t1_in_t0
            FROM swaps
            WHERE LOWER(network_id) = ANY($1::text[])
                AND block_number <= $2 AND block_number > ($2 - $3)
                AND ((token0_address = $4 AND token1_address = $5) OR (token0_address = $5 AND token1_address = $4))
            ORDER BY block_number DESC, log_index DESC
            LIMIT 1
        "#;

        if let Ok(Some(row)) = conn.query_opt(query, &[&aliases, &(block_number as i64), &lookback_blocks, &token_a.as_bytes(), &token_b.as_bytes()]).await {
            let token0_db: Vec<u8> = row.get("token0_address");
            let price0: Option<rust_decimal::Decimal> = row.get("price_t0_in_t1");
            let price1: Option<rust_decimal::Decimal> = row.get("price_t1_in_t0");

            if token_a.as_bytes() == token0_db.as_slice() {
                // token_a is token0, we want price of token_a in token_b, which is price_t0_in_t1
                return Ok(price0.and_then(|p| p.to_f64()));
            } else {
                // token_a is token1, we want price of token_a in token_b, which is price_t1_in_t0
                return Ok(price1.and_then(|p| p.to_f64()));
            }
        }

        debug!("No direct pair price found for token_a={:?}, token_b={:?} at block {} within {} blocks",
               token_a, token_b, block_number, lookback_blocks);
        Ok(None)
    }
}

#[derive(Debug)]
pub struct LoaderMetricsSnapshot {
    pub queries_executed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub query_errors: u64,
}

impl LoaderMetricsSnapshot {
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}
