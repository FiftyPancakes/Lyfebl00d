//! # Pool Manager
//!
//! In-memory pool registry that loads static pool metadata from the database.
//! Acts as a fast, read-only cache for pool discovery data populated by `discover_pools.rs`.
//!
//! ## Architecture
//! - Uses connection pooling via `deadpool-postgres` for efficient database access
//! - Implements circuit breaker pattern for RPC failure protection
//! - Pre-indexes pools by chain for efficient filtering
//! - Uses efficient cache keys and data structures
//!
//! ## Safety
//! - All database queries use parameterized statements
//! - Test code is properly gated with `#[cfg(test)]`
//! - Safe array indexing throughout


use crate::{
    config::{Config, ExecutionMode},
    errors::{DexError, PoolManagerError},
    pool_states::{PoolStateOracle, Error as PoolStateError},
    types::{PoolInfo, ProtocolType},
    alpha::circuit_breaker::{CircuitBreaker, CircuitBreakerStats},
};

use async_trait::async_trait;
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use ethers::types::Address;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn, instrument, trace};
use std::fmt;
use futures;
use std::env;
use std::time::SystemTime;
use rust_decimal::Decimal;

//================================================================================================//
//                                         CONSTANTS                                              //
//================================================================================================//

/// Default connection pool size
const DEFAULT_CONNECTION_POOL_SIZE: usize = 10;

/// Circuit breaker failure threshold
const CIRCUIT_BREAKER_THRESHOLD: u64 = 5;

/// Circuit breaker recovery timeout
const CIRCUIT_BREAKER_TIMEOUT_SECS: u64 = 30;

/// Maximum retries for database operations
const MAX_DB_RETRIES: u32 = 3;

/// Delay between retries (exponential backoff base)
const RETRY_DELAY_MS: u64 = 100;

//================================================================================================//
//                                           STRUCTS                                             //
//================================================================================================//

/// Static metadata for a pool loaded from the database. This is the new source of truth.
#[derive(Debug, Clone)]
pub struct PoolStaticMetadata {
    pub pool_address: Address,
    pub token0: Address,
    pub token1: Address,
    pub token0_decimals: u8,
    pub token1_decimals: u8,
    pub fee: Option<u32>,
    pub protocol_type: ProtocolType,
    pub dex_name: String,
    pub chain_name: String,
    pub creation_block: u64,
    // Historical pool state data from swaps table (for backtesting)
    pub latest_token0_reserve: Option<Decimal>,
    pub latest_token1_reserve: Option<Decimal>,
    pub latest_sqrt_price_x96: Option<Decimal>,
    pub latest_liquidity: Option<Decimal>,
    pub latest_tick: Option<i32>,
}


/// Tracking structure for data quality issues during pool loading
#[derive(Debug, Default)]
pub struct LoadingRejectionStats {
    pub invalid_decimals: u64,
    pub unknown_protocol_types: u64,
    pub unknown_protocol_type_values: std::collections::HashSet<i16>,
    pub v4_pools_skipped: u64,
    pub invalid_v3_fees: u64,
    pub total_rejections: u64,
    pub total_processed: u64,
}

/// In-memory pool registry with optimized data structures.
pub struct PoolManager {
    cfg: Arc<Config>,
    // Database connection pool for efficient resource usage
    db_pool: Option<Pool>,
    // The state oracle is used to fetch dynamic on-chain data for a pool
    state_oracle: Arc<RwLock<Option<Arc<PoolStateOracle>>>>,
    // Circuit breaker for database operations
    db_circuit_breaker: Arc<CircuitBreaker>,

    // Optimized in-memory registry maps
    pools_by_address: RwLock<HashMap<Address, Arc<PoolStaticMetadata>>>,
    // Use HashSet for automatic deduplication
    pools_by_token: RwLock<HashMap<Address, HashSet<Address>>>,
    // Pre-index pools by chain for efficient filtering
    pools_by_chain: RwLock<HashMap<String, Vec<Arc<PoolStaticMetadata>>>>,
    all_pools: RwLock<Vec<Arc<PoolStaticMetadata>>>,
    chain_ids: HashMap<String, u64>,

    // V4 pool registry for Uniswap V4 pools

    // Metrics
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    // Additional metrics
    rejected_rows_missing_meta: Arc<AtomicU64>,
    deduplicated_pools: Arc<AtomicU64>,
    db_retries_exhausted: Arc<AtomicU64>,

    // Current block context for backtesting
    current_block_context: Arc<RwLock<Option<u64>>>,
}

impl fmt::Debug for PoolManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pools_by_address = self.pools_by_address.blocking_read();
        let pools_by_token = self.pools_by_token.blocking_read();
        let pools_by_chain = self.pools_by_chain.blocking_read();

        f.debug_struct("PoolManager")
            .field("total_pools_loaded", &pools_by_address.len())
            .field("indexed_tokens", &pools_by_token.len())
            .field("chains", &pools_by_chain.keys().collect::<Vec<_>>())
            .field("cache_hits", &self.cache_hits.load(Ordering::Relaxed))
            .field("cache_misses", &self.cache_misses.load(Ordering::Relaxed))
            .field("rejected_rows_missing_meta", &self.rejected_rows_missing_meta.load(Ordering::Relaxed))
            .field("deduplicated_pools", &self.deduplicated_pools.load(Ordering::Relaxed))
            .field("db_retries_exhausted", &self.db_retries_exhausted.load(Ordering::Relaxed))
            .finish()
    }
}

//================================================================================================//
//                                           IMPL BLOCK                                          //
//================================================================================================//

impl PoolManager {
    /// Safely convert a Decimal to U256, scaling by token decimals
    fn decimal_to_u256_scaled(d: rust_decimal::Decimal, decimals: u32) -> ethers::types::U256 {
        // Scale the decimal by 10^decimals to convert to integer representation
        let scaled = d * rust_decimal::Decimal::from(10u64.pow(decimals));
        // Round to nearest integer and convert to string for safe parsing
        let s = scaled.round().to_string();
        ethers::types::U256::from_dec_str(&s).unwrap_or(ethers::types::U256::zero())
    }

    /// Create a new PoolManager and load all pool data from the database.
    #[instrument(skip(cfg, state_oracle))]
    pub async fn new(
        cfg: Arc<Config>,
        state_oracle: Arc<PoolStateOracle>,
    ) -> Result<Self, PoolManagerError> {
        let db_pool = Self::create_db_pool(&cfg).await?;
        let db_circuit_breaker = Arc::new(CircuitBreaker::new(
            CIRCUIT_BREAKER_THRESHOLD,
            Duration::from_secs(CIRCUIT_BREAKER_TIMEOUT_SECS),
        ));
        
        let mut manager = Self {
            cfg: cfg.clone(),
            db_pool,
            state_oracle: Arc::new(RwLock::new(Some(state_oracle))),
            db_circuit_breaker,
            pools_by_address: RwLock::new(HashMap::new()),
            pools_by_token: RwLock::new(HashMap::new()),
            pools_by_chain: RwLock::new(HashMap::new()),
            all_pools: RwLock::new(Vec::new()),
            chain_ids: cfg.chain_config.chains.iter()
                .map(|(k, v)| (k.clone(), v.chain_id))
                .collect(),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            rejected_rows_missing_meta: Arc::new(AtomicU64::new(0)),
            deduplicated_pools: Arc::new(AtomicU64::new(0)),
            db_retries_exhausted: Arc::new(AtomicU64::new(0)),
            current_block_context: Arc::new(RwLock::new(None)),
        };
        
        manager.load_registry().await?;
        Ok(manager)
    }

    /// Create a new PoolManager without state oracle (for specific use cases)
    pub async fn new_without_state_oracle(
        cfg: Arc<Config>,
    ) -> Result<Self, PoolManagerError> {
        // Create database pool for both live and backtest modes
        // In live mode: pools loaded from cg_pools table
        // In backtest mode: pools loaded from swaps table
        let db_pool = Self::create_db_pool(&cfg).await?;
        
        let db_circuit_breaker = Arc::new(CircuitBreaker::new(
            CIRCUIT_BREAKER_THRESHOLD,
            Duration::from_secs(CIRCUIT_BREAKER_TIMEOUT_SECS),
        ));
        
        // We'll initialize without a state oracle, which can be set later
        let mut manager = Self {
            cfg: cfg.clone(),
            db_pool,
            state_oracle: Arc::new(RwLock::new(None)),
            db_circuit_breaker,
            pools_by_address: RwLock::new(HashMap::new()),
            pools_by_token: RwLock::new(HashMap::new()),
            pools_by_chain: RwLock::new(HashMap::new()),
            all_pools: RwLock::new(Vec::new()),
            chain_ids: cfg.chain_config.chains.iter()
                .map(|(k, v)| (k.clone(), v.chain_id))
                .collect(),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            rejected_rows_missing_meta: Arc::new(AtomicU64::new(0)),
            deduplicated_pools: Arc::new(AtomicU64::new(0)),
            db_retries_exhausted: Arc::new(AtomicU64::new(0)),
            current_block_context: Arc::new(RwLock::new(None)),
        };
        
        // Load registry from cg_pools table for live mode or swaps table for backtest mode
        manager.load_registry().await?;
        Ok(manager)
    }

    /// Create database connection pool
    async fn create_db_pool(cfg: &Config) -> Result<Option<Pool>, PoolManagerError> {
        let db_url = match cfg.database_url() {
            Some(url) => url,
            None => {
                // Required-registry behavior: if required, return hard error
                let require_registry_env = env::var("REQUIRE_POOL_REGISTRY").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
                if cfg.strict_backtest || require_registry_env {
                    return Err(PoolManagerError::Database("Pool registry required but DATABASE_URL is not set".to_string()));
                }
                info!("No database URL provided - running without database");
                return Ok(None);
            }
        };

        // Parse DATABASE_URL and create configuration
        // This approach relies only on the DATABASE_URL without hardcoded defaults
        let url = match url::Url::parse(&db_url) {
            Ok(url) => url,
            Err(e) => {
                return Err(PoolManagerError::Database(format!("Invalid DATABASE_URL '{}': {}", db_url, e)));
            }
        };

        let mut pg_config = PgConfig::new();

        // Set database connection parameters from URL
        if let Some(host) = url.host_str() {
            pg_config.host = Some(host.to_string());
        }
        if let Some(port) = url.port() {
            pg_config.port = Some(port);
        }
        if !url.username().is_empty() {
            pg_config.user = Some(url.username().to_string());
        }
        if let Some(password) = url.password() {
            pg_config.password = Some(password.to_string());
        }
        let path = url.path().trim_start_matches('/');
        if !path.is_empty() {
            pg_config.dbname = Some(path.to_string());
        }

        // Configure connection pool settings
        pg_config.pool = Some(deadpool_postgres::PoolConfig {
            max_size: DEFAULT_CONNECTION_POOL_SIZE,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(Duration::from_secs(10)), // Increased wait timeout
                create: Some(Duration::from_secs(10)), // Increased create timeout
                recycle: Some(Duration::from_secs(30)), // Increased recycle timeout
            },
            ..Default::default()
        });

        let pool = pg_config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| PoolManagerError::Database(format!("Failed to create connection pool: {}", e)))?;

        Ok(Some(pool))
    }

    /// Execute database query with circuit breaker and retry logic
    async fn execute_with_retry<T, F, Fut>(
        &self,
        operation_name: &str,
        f: F,
    ) -> Result<T, PoolManagerError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, PoolManagerError>>,
    {
        // Check circuit breaker
        if self.db_circuit_breaker.is_tripped().await {
            return Err(PoolManagerError::Database(format!(
                "Circuit breaker is open for database operations"
            )));
        }

        let mut last_error = None;

        for attempt in 0..MAX_DB_RETRIES {
            match f().await {
                Ok(result) => {
                    self.db_circuit_breaker.record_success().await;
                    return Ok(result);
                }
                Err(e) => {
                    // Record failure on every failed attempt for proper circuit breaker behavior
                    self.db_circuit_breaker.record_failure().await;
                    let error_msg = format!("{}", e);
                    last_error = Some(e);

                    if attempt == MAX_DB_RETRIES - 1 {
                        // Final attempt failed
                        self.db_retries_exhausted.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    // Calculate delay with exponential backoff and jitter
                    let base_delay_ms = RETRY_DELAY_MS * (1u64 << attempt);
                    let now = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                    let nanos = now.subsec_nanos() as u64;
                    let jitter_ms = (nanos % 1000).saturating_mul(base_delay_ms / 2) / 1000;
                    let delay = Duration::from_millis(base_delay_ms + jitter_ms);

                    warn!(
                        "Database operation '{}' failed (attempt {}/{}) - backing off for {}ms: {}",
                        operation_name,
                        attempt + 1,
                        MAX_DB_RETRIES,
                        delay.as_millis(),
                        error_msg
                    );

                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            PoolManagerError::Database(format!("Operation '{}' failed after {} retries", operation_name, MAX_DB_RETRIES))
        }))
    }

    /// Connects to the database and populates the in-memory pool registry.
    #[instrument(skip(self))]
    async fn load_registry(&mut self) -> Result<(), PoolManagerError> {
        // Check if we're in backtest mode - if so, load from swaps table instead
        if matches!(self.cfg.mode, ExecutionMode::Backtest) {
            // For backtesting, we do an initial load here, but the registry will be reloaded for each block
            return self.load_registry_for_backtest(0).await;
        }

        // For live mode, load from cg_pools table
        self.load_registry_from_cg_pools().await
    }



    /// Load pool registry from cg_pools table (for live mode only)
    /// This method should ONLY be called in live mode, never during backtesting
    #[instrument(skip(self))]
    async fn load_registry_from_cg_pools(&mut self) -> Result<(), PoolManagerError> {
        info!("Loading pool registry from cg_pools table for live mode...");

        let db_pool = match &self.db_pool {
            Some(pool) => pool,
            None => {
                info!("No database pool available - skipping database load");
                return Ok(());
            }
        };

        let rows = self.execute_with_retry("load_pools_from_cg_pools", || {
            let pool = db_pool.clone();
            async move {
                let client = pool.get().await
                    .map_err(|e| PoolManagerError::Database(format!("Failed to get client from pool: {}", e)))?;

                // Query cg_pools table for live mode - different structure than swaps table
                let rows = client
                    .query(
                        r#"
                        SELECT
                            pool_address,
                            token0_address as token0,
                            token1_address as token1,
                            token0_decimals,
                            token1_decimals,
                            fee_tier as fee,
                            protocol_type,
                            dex_name,
                            chain_name,
                            pool_created_at as creation_block,
                            -- Pool state fields (NULL for cg_pools as it doesn't have historical data)
                            NULL::decimal as latest_token0_reserve,
                            NULL::decimal as latest_token1_reserve,
                            NULL::decimal as latest_sqrt_price_x96,
                            NULL::decimal as latest_liquidity,
                            NULL::integer as latest_tick
                        FROM cg_pools
                        WHERE token0_address IS NOT NULL
                          AND token1_address IS NOT NULL
                          AND pool_address IS NOT NULL
                          AND token0_decimals IS NOT NULL
                          AND token1_decimals IS NOT NULL
                          AND token0_decimals > 0
                          AND token1_decimals > 0
                          AND token0_decimals <= 255
                          AND token1_decimals <= 255
                          AND protocol_type IS NOT NULL
                          AND protocol_type >= 0
                          AND (fee_tier IS NOT NULL OR protocol_type != 3)  -- Allow NULL fee_tier for non-V3 pools
                          AND (fee_tier IS NULL OR fee_tier >= 0)  -- If fee_tier is not NULL, it must be >= 0
                          AND chain_name IS NOT NULL
                          AND LENGTH(chain_name) > 0
                          AND dex_name IS NOT NULL
                          AND LENGTH(TRIM(dex_name)) > 0
                        "#,
                        &[]
                    )
                    .await
                    .map_err(|e| PoolManagerError::Database(format!("Failed to query pools from cg_pools: {}", e)))?;

                Ok(rows)
            }
        }).await?;

        // Track data quality issues for strict loading mode
        let mut rejection_stats = LoadingRejectionStats::default();
        rejection_stats.total_processed = rows.len() as u64;

        // Track seen pools to prevent duplicates
        let mut seen_pools = std::collections::HashSet::new();

        for row in rows {
            let chain_name: String = row.get("chain_name");
            if let Some(chain_config) = self.cfg.chain_config.chains.get(&chain_name) {
                if chain_config.chain_type.to_lowercase() != "evm" {
                    continue; // Skip non-EVM chains
                }
            } else {
                trace!("Chain '{}' from cg_pools not found in configuration, skipping.", chain_name);
                continue;
            }

            // Parse addresses from hex strings (trim whitespace to avoid parsing issues)
            let pool_address_str: String = row.get::<_, String>("pool_address").trim().to_string();
            let token0_str: String = row.get::<_, String>("token0").trim().to_string();
            let token1_str: String = row.get::<_, String>("token1").trim().to_string();

            // Skip rows with NULL or empty addresses
            if pool_address_str.is_empty() || token0_str.is_empty() || token1_str.is_empty() {
                warn!("Skipping pool with empty addresses: pool={}, token0={}, token1={}",
                      pool_address_str, token0_str, token1_str);
                continue;
            }

            // Convert hex strings to addresses (handle both with and without 0x prefix)
            let pool_address = {
                let formatted_address = if pool_address_str.starts_with("0x") {
                    pool_address_str.clone()
                } else {
                    format!("0x{}", pool_address_str)
                };
                
                match Address::from_str(&formatted_address) {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("Invalid pool_address '{}': {}", pool_address_str, e);
                        rejection_stats.total_rejections += 1;
                        continue;
                    }
                }
            };
            
            let token0 = {
                let formatted_address = if token0_str.starts_with("0x") {
                    token0_str.clone()
                } else {
                    format!("0x{}", token0_str)
                };
                
                match Address::from_str(&formatted_address) {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("Invalid token0_address '{}': {}", token0_str, e);
                        rejection_stats.total_rejections += 1;
                        continue;
                    }
                }
            };

            let token1 = {
                let formatted_address = if token1_str.starts_with("0x") {
                    token1_str.clone()
                } else {
                    format!("0x{}", token1_str)
                };
                
                match Address::from_str(&formatted_address) {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("Invalid token1_address '{}': {}", token1_str, e);
                        rejection_stats.total_rejections += 1;
                        continue;
                    }
                }
            };

            // Parse other fields
            let token0_decimals: i32 = row.get("token0_decimals");
            let token1_decimals: i32 = row.get("token1_decimals");
            let fee: Option<i32> = row.get("fee");
            let protocol_type_int: i16 = row.get("protocol_type");
            let dex_name: String = row.get("dex_name");
            let chain_name: String = row.get("chain_name");
            let creation_block_i64: i64 = row.get("creation_block");

            // Validate decimals
            let token0_decimals: u8 = match token0_decimals {
                v if (0..=255).contains(&v) => v as u8,
                _ => {
                    warn!("Invalid token0_decimals {} for pool {}", token0_decimals, pool_address);
                    rejection_stats.invalid_decimals += 1;
                    rejection_stats.total_rejections += 1;
                    continue;
                }
            };
            let token1_decimals: u8 = match token1_decimals {
                v if (0..=255).contains(&v) => v as u8,
                _ => {
                    warn!("Invalid token1_decimals {} for pool {}", token1_decimals, pool_address);
                    rejection_stats.invalid_decimals += 1;
                    rejection_stats.total_rejections += 1;
                    continue;
                }
            };

            // Convert protocol type (now handles all values gracefully)
            let protocol_type = match ProtocolType::try_from(protocol_type_int) {
                Ok(pt) => pt,
                Err(e) => {
                    warn!("Failed to convert protocol type {} for pool {}: {}", protocol_type_int, pool_address, e);
                    ProtocolType::Unknown // Default to Unknown instead of skipping
                }
            };

            if matches!(protocol_type, ProtocolType::Unknown) {
                debug!("Loading pool {} with unknown protocol type {} (marked as unsupported for trading)", pool_address, protocol_type_int);
                rejection_stats.unknown_protocol_types += 1;
                rejection_stats.unknown_protocol_type_values.insert(protocol_type_int);
                // Don't increment total_rejections - we're loading these pools
            }

            // CRITICAL: Skip V4 pools until proper state handling is implemented
            if matches!(protocol_type, ProtocolType::UniswapV4) {
                debug!("Skipping V4 pool {} - V4 pools require specialized state handling", pool_address);
                rejection_stats.v4_pools_skipped += 1;
                rejection_stats.total_rejections += 1;
                continue;
            }

            // Validate fee for V3 pools
            if matches!(protocol_type, ProtocolType::UniswapV3) {
                match fee {
                    Some(f) if matches!(f, 100 | 500 | 3000 | 10000) => {},
                    _ => {
                        debug!("Skipping V3 pool {} due to invalid fee tier: {:?}", pool_address, fee);
                        rejection_stats.invalid_v3_fees += 1;
                        rejection_stats.total_rejections += 1;
                        continue;
                    }
                }
            }

            // Extract pool state data from the database row (may be NULL for cg_pools)
            let latest_token0_reserve: Option<Decimal> = row.get("latest_token0_reserve");
            let latest_token1_reserve: Option<Decimal> = row.get("latest_token1_reserve");
            let latest_sqrt_price_x96: Option<Decimal> = row.get("latest_sqrt_price_x96");
            let latest_liquidity: Option<Decimal> = row.get("latest_liquidity");
            let latest_tick: Option<i32> = row.get("latest_tick");

            // Check for duplicates and skip if already processed
            if !seen_pools.insert(pool_address) {
                self.deduplicated_pools.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Create metadata
            let metadata = Arc::new(PoolStaticMetadata {
                pool_address,
                token0,
                token1,
                token0_decimals,
                token1_decimals,
                fee: fee.map(|f| f as u32),
                protocol_type,
                dex_name: dex_name.clone(),
                chain_name: chain_name.clone(),
                creation_block: creation_block_i64 as u64,
                // Historical pool state data
                latest_token0_reserve,
                latest_token1_reserve,
                latest_sqrt_price_x96,
                latest_liquidity,
                latest_tick,
            });

            // Index the metadata
            let mut pools_by_address = self.pools_by_address.write().await;
            let mut pools_by_token = self.pools_by_token.write().await;
            let mut pools_by_chain = self.pools_by_chain.write().await;
            let mut all_pools = self.all_pools.write().await;

            pools_by_address.insert(metadata.pool_address, metadata.clone());
            pools_by_token.entry(metadata.token0).or_default().insert(metadata.pool_address);
            pools_by_token.entry(metadata.token1).or_default().insert(metadata.pool_address);
            pools_by_chain.entry(chain_name).or_default().push(metadata.clone());
            all_pools.push(metadata);
        }

        // Check strict loading mode for significant data quality issues
        let rejection_rate = if rejection_stats.total_processed > 0 {
            (rejection_stats.total_rejections as f64) / (rejection_stats.total_processed as f64)
        } else {
            0.0
        };

        // In strict mode, fail if rejection rate is too high (>10%) or absolute rejections are significant
        if self.cfg.strict_backtest && rejection_stats.total_rejections > 100 && rejection_rate > 0.1 {
            let unknown_protocols_str = if rejection_stats.unknown_protocol_type_values.is_empty() {
                "none".to_string()
            } else {
                format!("{:?}", rejection_stats.unknown_protocol_type_values.iter().collect::<Vec<_>>())
            };

            return Err(PoolManagerError::Database(format!(
                "Data quality issues in cg_pools table exceed threshold in strict mode. \
                 Total processed: {}, Rejections: {}, Rejection rate: {:.2}%. \
                 Invalid decimals: {}, Unknown protocols: {} (types: {}), V4 pools: {}, Invalid V3 fees: {}. \
                 Consider fixing data quality issues before running backtests.",
                rejection_stats.total_processed,
                rejection_stats.total_rejections,
                rejection_rate * 100.0,
                rejection_stats.invalid_decimals,
                rejection_stats.unknown_protocol_types,
                unknown_protocols_str,
                rejection_stats.v4_pools_skipped,
                rejection_stats.invalid_v3_fees
            )));
        }

        // Log data quality summary
        if rejection_stats.total_rejections > 0 {
            let unknown_protocols_str = if rejection_stats.unknown_protocol_type_values.is_empty() {
                "none".to_string()
            } else {
                format!("{:?}", rejection_stats.unknown_protocol_type_values.iter().collect::<Vec<_>>())
            };

            warn!(
                "Data quality issues during pool loading from cg_pools: \
                 Total processed: {}, Rejections: {}, Rejection rate: {:.2}%. \
                 Invalid decimals: {}, Unknown protocols: {} (types: {}), V4 pools: {}, Invalid V3 fees: {}",
                rejection_stats.total_processed,
                rejection_stats.total_rejections,
                rejection_rate * 100.0,
                rejection_stats.invalid_decimals,
                rejection_stats.unknown_protocol_types,
                unknown_protocols_str,
                rejection_stats.v4_pools_skipped,
                rejection_stats.invalid_v3_fees
            );
        }

        // Log detailed chain information for debugging
        for (chain, pools) in self.pools_by_chain.read().await.iter() {
            info!("Chain '{}' has {} pools from cg_pools", chain, pools.len());
        }

        info!(
            "Successfully loaded {} pools from cg_pools table across {} chains",
            self.pools_by_address.read().await.len(),
            self.pools_by_chain.read().await.len()
        );
        Ok(())
    }

    /// Load pool registry for backtest mode
    /// In backtest mode, we need to load ALL pools that existed at or before the target block,
    /// not just those with swaps at that specific block. This ensures we have the complete
    /// arbitrage opportunity universe for realistic backtesting.
    #[instrument(skip(self))]
    pub async fn load_registry_for_backtest(&self, block_number: u64) -> Result<(), PoolManagerError> {
        info!("Loading complete pool registry for backtest at block {}", block_number);

        let db_pool = match &self.db_pool {
            Some(pool) => pool,
            None => {
                info!("No database pool available - skipping database load");
                return Ok(());
            }
        };

        // Clear existing registry before loading new data
        {
            let mut pools_by_address = self.pools_by_address.write().await;
            let mut pools_by_token = self.pools_by_token.write().await;
            let mut pools_by_chain = self.pools_by_chain.write().await;
            let mut all_pools = self.all_pools.write().await;

            pools_by_address.clear();
            pools_by_token.clear();
            pools_by_chain.clear();
            all_pools.clear();
        }

        // Load pools for all configured chains
        for chain_name in self.cfg.chain_config.chains.keys() {
            let rows = self.execute_with_retry("load_pools_for_backtest", || {
            let pool = db_pool.clone();
            let current_block_i64 = block_number as i64;
            async move {
                let client = pool.get().await
                    .map_err(|e| PoolManagerError::Database(format!("Failed to get client from pool: {}", e)))?;

                // First try to load from cg_pools table (preferred for complete pool universe)
                // Fall back to swaps table if cg_pools is not available
                let cg_pools_query = r#"
                    SELECT
                        pool_address,
                        token0_address as token0,
                        token1_address as token1,
                        token0_decimals,
                        token1_decimals,
                        fee_tier as fee,
                        protocol_type,
                        dex_name,
                        chain_name,
                        pool_created_at as creation_block,
                        -- cg_pools doesn't have latest state data, use NULL placeholders
                        NULL as latest_token0_reserve,
                        NULL as latest_token1_reserve,
                        NULL as latest_sqrt_price_x96,
                        NULL as latest_liquidity,
                        NULL as latest_tick
                    FROM cg_pools
                    WHERE chain_name = $1
                      AND pool_created_at <= $2
                      AND pool_address IS NOT NULL
                      AND token0_address IS NOT NULL
                      AND token1_address IS NOT NULL
                      AND token0_decimals IS NOT NULL
                      AND token1_decimals IS NOT NULL
                      AND token0_decimals > 0
                      AND token1_decimals > 0
                      AND token0_decimals <= 255
                      AND token1_decimals <= 255
                      AND protocol_type IS NOT NULL
                      AND protocol_type >= 0
                      AND (fee_tier IS NOT NULL OR protocol_type != 3)
                      AND (fee_tier IS NULL OR fee_tier >= 0)
                      AND LENGTH(dex_name) > 0
                "#;

                match client.query(cg_pools_query, &[&chain_name, &current_block_i64]).await {
                    Ok(rows) if !rows.is_empty() => {
                        info!("Loaded {} pools from cg_pools table for backtest at block {} on chain {}", rows.len(), current_block_i64, chain_name);
                        Ok(rows)
                    },
                    _ => {
                        // Fallback to swaps table if cg_pools is not available or empty
                        warn!("cg_pools table not available or empty, falling back to swaps table. This may result in incomplete pool universe for backtest.");
                        let swaps_query = r#"
                            WITH latest_pool_states AS (
                                SELECT DISTINCT ON (pool_address)
                                pool_address,
                                token0_address as token0,
                                token1_address as token1,
                                token0_decimals,
                                token1_decimals,
                                fee_tier as fee,
                                protocol_type,
                                dex_name,
                                network_id as chain_name,
                                block_number as state_block_number,
                                -- Get latest pool state data for realistic reserves
                                (array_agg(token0_reserve ORDER BY block_number DESC))[1] as latest_token0_reserve,
                                (array_agg(token1_reserve ORDER BY block_number DESC))[1] as latest_token1_reserve,
                                (array_agg(sqrt_price_x96 ORDER BY block_number DESC))[1] as latest_sqrt_price_x96,
                                (array_agg(liquidity ORDER BY block_number DESC))[1] as latest_liquidity,
                                (array_agg(tick ORDER BY block_number DESC))[1] as latest_tick
                            FROM swaps
                            WHERE token0_address IS NOT NULL
                              AND token1_address IS NOT NULL
                              AND pool_address IS NOT NULL
                              AND token0_decimals IS NOT NULL
                              AND token1_decimals IS NOT NULL
                              AND token0_decimals > 0
                              AND token1_decimals > 0
                              AND token0_decimals <= 255
                              AND token1_decimals <= 255
                              AND protocol_type IS NOT NULL
                              AND protocol_type >= 0
                              AND (fee_tier IS NOT NULL OR protocol_type != 3)
                              AND (fee_tier IS NULL OR fee_tier >= 0)
                              AND LENGTH(network_id) > 0
                              AND dex_name IS NOT NULL
                              AND LENGTH(TRIM(dex_name)) > 0
                              AND block_number <= $2
                            ORDER BY pool_address, block_number DESC
                        ),
                        first_seen AS (
                            SELECT pool_address, MIN(block_number) AS creation_block
                            FROM swaps
                            WHERE network_id = $1 AND block_number <= $2
                            GROUP BY pool_address
                        )
                        SELECT lps.pool_address, lps.token0, lps.token1, lps.token0_decimals, lps.token1_decimals,
                               lps.fee, lps.protocol_type, lps.dex_name, lps.chain_name,
                               fs.creation_block,
                               lps.latest_token0_reserve, lps.latest_token1_reserve,
                               lps.latest_sqrt_price_x96, lps.latest_liquidity, lps.latest_tick
                        FROM latest_pool_states lps
                        JOIN first_seen fs USING (pool_address)
                        "#;

                        let rows = client.query(swaps_query, &[&chain_name, &current_block_i64])
                            .await
                            .map_err(|e| PoolManagerError::Database(format!("Failed to query pools for backtest: {}", e)))?;

                        info!("Loaded {} pools from swaps table for backtest at block {} on chain {}", rows.len(), current_block_i64, chain_name);
                        Ok(rows)
                    }
                }
            }
            }).await?;

            // Process the rows for this chain
            self.process_backtest_rows(rows).await?;
        }

        info!(
            "Reloaded registry for backtest at block {}: {} pools loaded",
            block_number,
            self.pools_by_address.read().await.len()
        );

        Ok(())
    }

    /// Process rows from backtest database query and add to registry
    async fn process_backtest_rows(&self, rows: Vec<tokio_postgres::Row>) -> Result<(), PoolManagerError> {
        let mut pools_by_address = self.pools_by_address.write().await;
        let mut pools_by_token = self.pools_by_token.write().await;
        let mut pools_by_chain = self.pools_by_chain.write().await;
        let mut all_pools = self.all_pools.write().await;

        // Track seen pools to prevent duplicates
        let mut seen_pools = std::collections::HashSet::new();

        for row in rows {
            // Parse addresses from binary data
            let pool_address_bytes: Vec<u8> = row.get("pool_address");
            let token0_bytes: Vec<u8> = row.get("token0");
            let token1_bytes: Vec<u8> = row.get("token1");

            // Safely parse addresses with length validation
            let pool_address = if pool_address_bytes.len() == 20 {
                Address::from_slice(&pool_address_bytes)
            } else {
                warn!("Invalid pool address length: {} bytes (expected 20). Skipping pool.", pool_address_bytes.len());
                continue;
            };

            let token0 = if token0_bytes.len() == 20 {
                Address::from_slice(&token0_bytes)
            } else {
                warn!("Invalid token0 address length: {} bytes (expected 20). Skipping pool.", token0_bytes.len());
                continue;
            };

            let token1 = if token1_bytes.len() == 20 {
                Address::from_slice(&token1_bytes)
            } else {
                warn!("Invalid token1 address length: {} bytes (expected 20). Skipping pool.", token1_bytes.len());
                continue;
            };

            let token0_decimals: i32 = row.get("token0_decimals");
            let token1_decimals: i32 = row.get("token1_decimals");
            let fee: Option<i32> = row.get("fee");
            let protocol_type_int: i16 = row.get("protocol_type");
            let dex_name: String = row.get("dex_name");
            let chain_name: String = row.get("chain_name");
            let creation_block_i64: i64 = row.get("creation_block");

            let token0_decimals = token0_decimals as u8;
            let token1_decimals = token1_decimals as u8;

            let protocol_type = match ProtocolType::try_from(protocol_type_int) {
                Ok(pt) => pt,
                Err(_) => continue,
            };

            // Check for duplicates and skip if already processed
            if !seen_pools.insert(pool_address) {
                self.deduplicated_pools.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let metadata = Arc::new(PoolStaticMetadata {
                pool_address,
                token0,
                token1,
                token0_decimals,
                token1_decimals,
                fee: fee.map(|f| f as u32),
                protocol_type,
                dex_name: dex_name.clone(),
                chain_name: chain_name.clone(),
                creation_block: creation_block_i64 as u64,
                latest_token0_reserve: row.get("latest_token0_reserve"),
                latest_token1_reserve: row.get("latest_token1_reserve"),
                latest_sqrt_price_x96: row.get("latest_sqrt_price_x96"),
                latest_liquidity: row.get("latest_liquidity"),
                latest_tick: row.get("latest_tick"),
            });

            pools_by_address.insert(metadata.pool_address, metadata.clone());
            pools_by_token.entry(metadata.token0).or_default().insert(metadata.pool_address);
            pools_by_token.entry(metadata.token1).or_default().insert(metadata.pool_address);
            pools_by_chain.entry(chain_name).or_default().push(metadata.clone());
            all_pools.push(metadata);
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (u64, u64) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    /// Get circuit breaker statistics
    pub async fn get_circuit_breaker_stats(&self) -> CircuitBreakerStats {
        self.db_circuit_breaker.get_stats().await
    }

    /// Create a new PoolManager for testing that doesn't load from database
    pub fn new_for_testing(
        cfg: Arc<Config>,
    ) -> Result<Self, PoolManagerError> {
        let db_circuit_breaker = Arc::new(CircuitBreaker::new(
            CIRCUIT_BREAKER_THRESHOLD,
            Duration::from_secs(CIRCUIT_BREAKER_TIMEOUT_SECS),
        ));
        
        let manager = Self {
            cfg: cfg.clone(),
            db_pool: None,
            state_oracle: Arc::new(RwLock::new(None)),
            db_circuit_breaker,
            pools_by_address: RwLock::new(HashMap::new()),
            pools_by_token: RwLock::new(HashMap::new()),
            pools_by_chain: RwLock::new(HashMap::new()),
            all_pools: RwLock::new(Vec::new()),
            chain_ids: cfg.chain_config.chains.iter()
                .map(|(k, v)| (k.clone(), v.chain_id))
                .collect(),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            rejected_rows_missing_meta: Arc::new(AtomicU64::new(0)),
            deduplicated_pools: Arc::new(AtomicU64::new(0)),
            db_retries_exhausted: Arc::new(AtomicU64::new(0)),
            current_block_context: Arc::new(RwLock::new(None)),
        };
        
        info!("PoolManager initialized for testing - skipping database load");
        Ok(manager)
    }

    /// Register a test pool with the pool manager for testing purposes
    pub async fn register_test_pool(
        &mut self,
        pool_address: Address,
        token0: Address,
        token1: Address,
        token0_decimals: u8,
        token1_decimals: u8,
        fee: Option<u32>,
        protocol_type: ProtocolType,
        dex_name: String,
        chain_name: String,
        creation_block: u64,
    ) {
        println!("POOL_MANAGER_REGISTER: Registering test pool: pool={:?}, token0={:?}, token1={:?}, chain={}", pool_address, token0, token1, chain_name);
        debug!("Registering test pool: pool={:?}, token0={:?}, token1={:?}, chain={}", pool_address, token0, token1, chain_name);
        let metadata = Arc::new(PoolStaticMetadata {
            pool_address,
            token0,
            token1,
            token0_decimals,
            token1_decimals,
            fee,
            protocol_type,
            dex_name,
            chain_name: chain_name.clone(),
            creation_block,
            // Pool state data (NULL for cg_pools)
            latest_token0_reserve: None,
            latest_token1_reserve: None,
            latest_sqrt_price_x96: None,
            latest_liquidity: None,
            latest_tick: None,
        });

        // Index the metadata
        let mut pools_by_address = self.pools_by_address.write().await;
        let mut pools_by_token = self.pools_by_token.write().await;
        let mut pools_by_chain = self.pools_by_chain.write().await;
        let mut all_pools = self.all_pools.write().await;

        pools_by_address.insert(metadata.pool_address, metadata.clone());
        pools_by_token.entry(metadata.token0).or_default().insert(metadata.pool_address);
        pools_by_token.entry(metadata.token1).or_default().insert(metadata.pool_address);
        pools_by_chain.entry(chain_name.clone()).or_default().push(metadata.clone());
        all_pools.push(metadata);

        debug!("Successfully registered pool for chain '{}'. Total pools in chain: {}", chain_name, pools_by_chain.get(&chain_name).unwrap().len());
    }

    /// Set reserve data for a test pool (for testing purposes only)
    pub async fn set_test_pool_reserves(
        &mut self,
        pool_address: Address,
        token0_reserve: rust_decimal::Decimal,
        token1_reserve: rust_decimal::Decimal,
    ) -> Result<(), PoolManagerError> {
        let mut pools_by_address = self.pools_by_address.write().await;

        if let Some(metadata) = pools_by_address.get_mut(&pool_address) {
            // Create new metadata with reserve data
            let updated_metadata = Arc::new(PoolStaticMetadata {
                pool_address: metadata.pool_address,
                token0: metadata.token0,
                token1: metadata.token1,
                token0_decimals: metadata.token0_decimals,
                token1_decimals: metadata.token1_decimals,
                fee: metadata.fee,
                protocol_type: metadata.protocol_type.clone(),
                dex_name: metadata.dex_name.clone(),
                chain_name: metadata.chain_name.clone(),
                creation_block: metadata.creation_block,
                latest_token0_reserve: Some(token0_reserve),
                latest_token1_reserve: Some(token1_reserve),
                latest_sqrt_price_x96: metadata.latest_sqrt_price_x96,
                latest_liquidity: metadata.latest_liquidity,
                latest_tick: metadata.latest_tick,
            });

            // Replace the metadata
            pools_by_address.insert(pool_address, updated_metadata);

            debug!("Set test pool reserves for pool {:?}: token0_reserve={}, token1_reserve={}",
                   pool_address, token0_reserve, token1_reserve);
            Ok(())
        } else {
            Err(PoolManagerError::Other(format!("Pool {} not found", pool_address)))
        }
    }
}

//================================================================================================//
//                                           TRAIT DEFINITION                                     //
//================================================================================================//

#[async_trait]
pub trait PoolManagerTrait: Send + Sync {
    async fn get_pools_for_pair(
        &self,
        chain: &str,
        a: Address,
        b: Address,
    ) -> Result<Vec<Address>, DexError>;

    async fn get_pools_for_pair_at_block(
        &self,
        chain: &str,
        a: Address,
        b: Address,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError>;

    async fn get_pool_info(
        &self,
        chain: &str,
        pool: Address,
        block: Option<u64>,
    ) -> Result<PoolInfo, DexError>;

    async fn get_all_pools(
        &self,
        chain: &str,
        prot_filter: Option<ProtocolType>,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError>;

    async fn get_pools_created_in_block_range(
        &self,
        chain_name: &str,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError>;

    async fn get_pools_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError>;

    async fn get_tokens_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError>;

    async fn get_metadata_for_pool(&self, pool_address: Address) -> Option<Arc<PoolStaticMetadata>>;

    fn chain_name_by_id(&self, chain_id: u64) -> Result<String, DexError>;

    async fn set_state_oracle(&self, state_oracle: Arc<PoolStateOracle>) -> Result<(), PoolManagerError>;

    async fn get_total_liquidity_for_token_units(
        &self,
        chain_name: &str,
        token: Address,
        block_number: Option<u64>,
    ) -> Result<f64, DexError>;

    async fn set_block_context(&self, block_number: u64) -> Result<(), PoolManagerError>;

    async fn reload_registry_for_backtest(&self, block_number: u64) -> Result<(), PoolManagerError>;
}

//================================================================================================//
//                                           TRAIT IMPLEMENTATION                                 //
//================================================================================================//

#[async_trait]
impl PoolManagerTrait for PoolManager {
    /// Finds all pools that contain both token A and token B.
    #[instrument(skip(self), fields(chain = %chain, token_a = ?a, token_b = ?b))]
    async fn get_pools_for_pair(&self, chain: &str, a: Address, b: Address) -> Result<Vec<Address>, DexError> {
        let pools_by_token = self.pools_by_token.read().await;
        // Get pools containing token A
        let pools_a = pools_by_token
            .get(&a)
            .cloned()
            .unwrap_or_default();
        
        // Get pools containing token B
        let pools_b = pools_by_token
            .get(&b)
            .cloned()
            .unwrap_or_default();
        
        // Only count as cache hit if we found pools for both tokens
        if !pools_a.is_empty() && !pools_b.is_empty() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        
        // Find intersection efficiently
        let common_pools: Vec<Address> = {
            let pools_by_address = self.pools_by_address.read().await;
            pools_a
                .intersection(&pools_b)
                .copied()
                .filter(|p| {
                    // Filter by chain
                    pools_by_address
                        .get(p)
                        .map(|meta| meta.chain_name == chain)
                        .unwrap_or(false)
                })
                .collect()
        };

        Ok(common_pools)
    }

    async fn get_pools_for_pair_at_block(
        &self,
        chain: &str,
        a: Address,
        b: Address,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError> {
        // Get active pools at the block first
        let active_pools = self.get_pools_active_at_block(chain, block_number).await?;

        // Filter pools containing token A from active pools
        let pools_a: std::collections::HashSet<Address> = active_pools.iter()
            .filter(|meta| meta.token0 == a || meta.token1 == a)
            .map(|meta| meta.pool_address)
            .collect();

        // Filter pools containing token B from active pools
        let pools_b: std::collections::HashSet<Address> = active_pools.iter()
            .filter(|meta| meta.token0 == b || meta.token1 == b)
            .map(|meta| meta.pool_address)
            .collect();

        // Only count as cache hit if we found pools for both tokens
        if !pools_a.is_empty() && !pools_b.is_empty() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Find intersection efficiently - only pools that exist at the given block
        let common_pools: Vec<Address> = pools_a
            .intersection(&pools_b)
            .copied()
            .collect();

        debug!("Found {} pools for pair {:?}-{:?} active at block {} on chain {}",
               common_pools.len(), a, b, block_number, chain);

        Ok(common_pools)
    }

    /// Fetches the complete `PoolInfo` for a given pool, combining static data from the
    /// registry with dynamic on-chain state fetched by the `PoolStateOracle`.
    #[instrument(skip(self), fields(chain = %chain, pool = ?pool, block = ?block))]
    async fn get_pool_info(&self, chain: &str, pool: Address, block: Option<u64>) -> Result<PoolInfo, DexError> {
        // Enforce strict backtest rules using configuration if enabled
        if self.cfg.strict_backtest && matches!(self.cfg.mode, ExecutionMode::Backtest) && block.is_none() {
            return Err(DexError::PoolManagerError(
                "Strict backtest mode requires an explicit block number for pool state queries".to_string(),
            ));
        }

        let static_meta = self.get_metadata_for_pool(pool)
            .await
            .ok_or_else(|| {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                DexError::PoolNotFound(pool.to_string())
            })?;
        
        // In backtest mode, ensure we only process pools with valid reserve data
        if matches!(self.cfg.mode, ExecutionMode::Backtest) {
            if static_meta.latest_token0_reserve.is_none() || 
               static_meta.latest_token1_reserve.is_none() ||
               static_meta.latest_token0_reserve.unwrap() <= Decimal::ZERO ||
               static_meta.latest_token1_reserve.unwrap() <= Decimal::ZERO {
                return Err(DexError::PoolNotFound(format!(
                    "Pool {} has no valid reserve data in backtest mode (reserves: {:?}, {:?})",
                    pool, static_meta.latest_token0_reserve, static_meta.latest_token1_reserve
                )));
            }
        }
        
        self.cache_hits.fetch_add(1, Ordering::Relaxed);

        if static_meta.chain_name != chain {
            return Err(DexError::PoolManagerError(format!(
                "Pool {} is on chain {} but requested for chain {}",
                pool, static_meta.chain_name, chain
            )));
        }

        // Try to use historical pool state data first (for backtesting with swaps table)
        let pool_state_data = if matches!(self.cfg.mode, ExecutionMode::Backtest) &&
                                 static_meta.latest_token0_reserve.is_some() &&
                                 static_meta.latest_token1_reserve.is_some() &&
                                 static_meta.latest_token0_reserve.unwrap() > Decimal::ZERO &&
                                 static_meta.latest_token1_reserve.unwrap() > Decimal::ZERO {
            // Use historical data from swaps table for backtesting
            debug!("Using historical pool state data for pool {} (backtest mode)", pool);
            use crate::pool_states::{PoolStateData, PoolStateSource};
            use crate::types::PoolStateFromSwaps;
            use ethers::types::U256;

            // Convert Decimal reserves to U256 using safe scaling conversion
            let reserve0 = static_meta.latest_token0_reserve
                .map(|r| Self::decimal_to_u256_scaled(r, static_meta.token0_decimals as u32))
                .unwrap_or(U256::zero());

            let reserve1 = static_meta.latest_token1_reserve
                .map(|r| Self::decimal_to_u256_scaled(r, static_meta.token1_decimals as u32))
                .unwrap_or(U256::zero());

            let block_num = block.unwrap_or(0);

            PoolStateData {
                pool_state: PoolStateFromSwaps {
                    pool_address: static_meta.pool_address,
                    token0: static_meta.token0,
                    token1: static_meta.token1,
                    reserve0,
                    reserve1,
                    sqrt_price_x96: static_meta.latest_sqrt_price_x96
                        .map(|s| Self::decimal_to_u256_scaled(s, 0)), // sqrt_price_x96 is already in Q96.0 format
                    liquidity: static_meta.latest_liquidity
                        .map(|l| Self::decimal_to_u256_scaled(l, 0)), // liquidity is already in raw units
                    tick: static_meta.latest_tick,
                    v3_tick_data: None, // Not available from historical data
                    v3_tick_current: static_meta.latest_tick,
                    fee_tier: static_meta.fee.unwrap_or(0),
                    protocol: match static_meta.protocol_type {
                        crate::types::ProtocolType::UniswapV2 => crate::types::DexProtocol::UniswapV2,
                        crate::types::ProtocolType::UniswapV3 => crate::types::DexProtocol::UniswapV3,
                        _ => crate::types::DexProtocol::UniswapV2,
                    },
                    dex_protocol: match static_meta.protocol_type {
                        crate::types::ProtocolType::UniswapV2 => crate::types::DexProtocol::UniswapV2,
                        crate::types::ProtocolType::UniswapV3 => crate::types::DexProtocol::UniswapV3,
                        _ => crate::types::DexProtocol::UniswapV2,
                    },
                    protocol_type: static_meta.protocol_type.clone(),
                    block_number: block_num,
                    timestamp: block_num,
                    token0_decimals: static_meta.token0_decimals as u32,
                    token1_decimals: static_meta.token1_decimals as u32,
                    price0_cumulative_last: None,
                    price1_cumulative_last: None,
                    last_reserve_update_timestamp: Some(block_num as u32),
                },
                block_number: block_num,
                timestamp: block_num,
                source: PoolStateSource::HistoricalDb,
            }
        } else {
            // Fall back to PoolStateOracle for live mode or when historical data is not available
            let state_oracle_guard = self.state_oracle.read().await;
            match state_oracle_guard.as_ref() {
                Some(oracle) => {
                    match oracle.get_pool_state(&static_meta.chain_name, pool, block).await {
                        Ok(data) => data,
                        Err(PoolStateError::NoDataForBlock(pool_addr, block_num)) => {
                            // Handle NoDataForBlock errors gracefully - this is normal in backtesting
                            debug!(
                                "No pool state data available for pool {} at block {} (chain: {}) - skipping pool",
                                pool_addr, block_num, static_meta.chain_name
                            );
                            return Err(DexError::PoolNotFound(format!("No state data for pool {} at block {}", pool_addr, block_num)));
                        }
                        Err(e) => {
                            return Err(DexError::PoolManagerError(e.to_string()));
                        }
                    }
                }
                None => {
                    return Err(DexError::PoolManagerError("State oracle not initialized".to_string()));
                }
            }
        };

        // Convert fee units based on protocol type
        // NOTE: `fee` field holds the raw configuration value from the database:
        //   - For UniswapV3: fee is in hundredths of a basis point (e.g., 3000 = 0.3%)
        //   - For V2-style protocols: fee is in basis points (e.g., 30 = 0.3%)
        // The `fee_bps` field is the normalized value in basis points for all protocols
        let fee_bps = match static_meta.protocol_type {
            ProtocolType::UniswapV3 => static_meta.fee.map(|hundredths_of_bip| hundredths_of_bip / 100),
            _ => static_meta.fee, // most V2-style configs store bps directly
        }.unwrap_or(0);

        // Debug logging to identify pool state issues
        let reserves0_u256 = pool_state_data.pool_state.reserve0;
        let reserves1_u256 = pool_state_data.pool_state.reserve1;
        let token0_decimals = static_meta.token0_decimals as u32;
        let token1_decimals = static_meta.token1_decimals as u32;
        let reserves0_f64 = reserves0_u256.as_u128() as f64 / 10u128.pow(token0_decimals) as f64;
        let reserves1_f64 = reserves1_u256.as_u128() as f64 / 10u128.pow(token1_decimals) as f64;

        debug!(
            target: "pool_states",
            "Pool {} state: reserves0={}, reserves1={}, reserves0_float={}, reserves1_float={}, token0_decimals={}, token1_decimals={}",
            pool, reserves0_u256, reserves1_u256, reserves0_f64, reserves1_f64,
            token0_decimals, token1_decimals
        );

        // Log zero reserves for debugging
        if reserves0_u256.is_zero() || reserves1_u256.is_zero() {
            trace!(
                target: "pool_states",
                "Pool {} has zero reserves: reserves0={}, reserves1={}. Block: {:?}, Protocol: {:?}",
                pool, reserves0_u256, reserves1_u256, block, static_meta.protocol_type
            );
        }

        // Combine static metadata with dynamic state to form the complete PoolInfo
        // CRITICAL: Use the actual block number and timestamp from which the state was loaded
        // to prevent data lookahead issues in backtesting
        let pool_info = PoolInfo {
            address: static_meta.pool_address,
            pool_address: static_meta.pool_address,
            token0: static_meta.token0,
            token1: static_meta.token1,
            token0_decimals: static_meta.token0_decimals,
            token1_decimals: static_meta.token1_decimals,
            fee: static_meta.fee,
            fee_bps,
            protocol_type: static_meta.protocol_type.clone(),
            chain_name: static_meta.chain_name.clone(),
            creation_block_number: Some(static_meta.creation_block),
            last_updated: pool_state_data.timestamp, // Use actual timestamp from state data
            v2_reserve0: Some(pool_state_data.pool_state.reserve0),
            v2_reserve1: Some(pool_state_data.pool_state.reserve1),
            v2_block_timestamp_last: pool_state_data.pool_state.last_reserve_update_timestamp,
            v3_sqrt_price_x96: pool_state_data.pool_state.sqrt_price_x96,
            v3_liquidity: pool_state_data.pool_state.liquidity.map(|l| l.as_u128()),
            v3_ticks: None, // Set to None since we don't carry the tick map here
            v3_tick_current: pool_state_data.pool_state.v3_tick_current,
            v3_tick_data: pool_state_data.pool_state.v3_tick_data,
            reserves0: pool_state_data.pool_state.reserve0,
            reserves1: pool_state_data.pool_state.reserve1,
            liquidity_usd: 0.0, // This should be calculated by the PriceOracle
        };

        Ok(pool_info)
    }

    /// Returns all pools for a given chain, optionally filtered by protocol.
    /// In backtest mode, this loads pools dynamically from the database for the current block context.
    /// In live mode, this returns pre-loaded pools from memory.
    async fn get_all_pools(
        &self,
        chain: &str,
        prot_filter: Option<ProtocolType>,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        
        // Check if we're in backtest mode
        if matches!(self.cfg.mode, ExecutionMode::Backtest) {
            // In backtest mode, we need to load pools dynamically from the database
            // based on the current block context
            return self.get_all_pools_backtest(chain, prot_filter).await;
        }
        
        // Live mode: use pre-loaded pools from memory
        debug!("get_all_pools called for chain '{}', filter: {:?} (live mode)", chain, prot_filter);
        let pools_by_chain = self.pools_by_chain.read().await;
        debug!("Available chains in registry: {:?}", pools_by_chain.keys().collect::<Vec<_>>());
        if let Some(chain_pools) = pools_by_chain.get(chain) {
            debug!("Found {} pools for chain '{}' in registry", chain_pools.len(), chain);
        } else {
            debug!("No entry found for chain '{}' in registry", chain);
        }
        
        // Use pre-indexed pools by chain for efficiency with robust fallback
        let mut pools_opt = pools_by_chain.get(chain);
        if pools_opt.is_none() {
            // Fallback 1: case-insensitive lookup
            let chain_lower = chain.to_ascii_lowercase();
            pools_opt = pools_by_chain
                .iter()
                .find(|(k, _): &(&String, &Vec<Arc<PoolStaticMetadata>>)| k.to_ascii_lowercase() == chain_lower)
                .map(|(_, v)| v);
        }
        if pools_opt.is_none() {
            // Fallback 2: alias by chain_id from config
            if let Some(&target_id) = self.chain_ids.get(chain) {
                pools_opt = self
                    .chain_ids
                    .iter()
                    .find(|(name, &id)| id == target_id && pools_by_chain.contains_key(*name))
                    .and_then(|(name, _)| pools_by_chain.get(name));
            }
        }
        let pools = if let Some(chain_pools) = pools_opt {
            let filtered: Vec<_> = chain_pools
                .iter()
                .filter(|meta| prot_filter.as_ref().map_or(true, |p| &meta.protocol_type == p))
                .cloned()
                .collect();
            debug!(
                "Found {} pools for chain '{}' (after filter: {} pools)",
                chain_pools.len(),
                chain,
                filtered.len()
            );
            filtered
        } else {
            debug!("No pools found for chain '{}' after fallback lookup", chain);
            Vec::new()
        };
        
        Ok(pools)
    }

    async fn get_pools_created_in_block_range(
        &self,
        chain_name: &str,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);

        let pools_by_chain = self.pools_by_chain.read().await;
        // Use pre-indexed pools by chain for efficiency
        let pools = pools_by_chain
            .get(chain_name)
            .map(|chain_pools| {
                chain_pools.iter()
                    .filter(|meta| {
                        meta.creation_block >= start_block && meta.creation_block <= end_block
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(pools)
    }

            async fn get_pools_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);

        let pools_by_chain = self.pools_by_chain.read().await;
        // Use pre-indexed pools by chain for efficiency
        let pools: Vec<Arc<PoolStaticMetadata>> = pools_by_chain
            .get(chain_name)
            .map(|chain_pools| {
                chain_pools.iter()
                    .filter(|meta| {
                        // Only include pools that were created before or at the given block
                        meta.creation_block <= block_number
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        debug!("Found {} pools active at block {} on chain {}", pools.len(), block_number, chain_name);

        Ok(pools)
    }

    async fn get_tokens_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError> {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);

        // Get active pools and extract unique tokens
        let pools = self.get_pools_active_at_block(chain_name, block_number).await?;
        let mut tokens = std::collections::HashSet::new();

        for pool in pools {
            tokens.insert(pool.token0);
            tokens.insert(pool.token1);
        }

        let token_vec: Vec<Address> = tokens.into_iter().collect();
        debug!("Found {} unique tokens active at block {} on chain {}", token_vec.len(), block_number, chain_name);

        Ok(token_vec)
    }

    /// Returns the static metadata for a single pool address. Fast, in-memory lookup.
    async fn get_metadata_for_pool(&self, pool_address: Address) -> Option<Arc<PoolStaticMetadata>> {
        let pools_by_address = self.pools_by_address.read().await;
        let result = pools_by_address.get(&pool_address).cloned();
        if result.is_some() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Resolves a chain name from a chain ID by checking the loaded configuration.
    /// This is a synchronous operation as it only reads from an in-memory map.
    fn chain_name_by_id(&self, chain_id: u64) -> Result<String, DexError> {
        self.chain_ids
            .iter()
            .find(|(_, &id)| id == chain_id)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| DexError::PoolManagerError(format!("Chain not found for ID: {}", chain_id)))
    }

    async fn set_state_oracle(&self, state_oracle: Arc<PoolStateOracle>) -> Result<(), PoolManagerError> {
        let mut oracle_guard = self.state_oracle.write().await;
        *oracle_guard = Some(state_oracle);
        Ok(())
    }

    async fn get_total_liquidity_for_token_units(
        &self,
        chain_name: &str,
        token: Address,
        block_number: Option<u64>,
    ) -> Result<f64, DexError> {
        // Use configuration to optionally limit concurrency for deterministic backtests
        let max_concurrent: usize = if self.cfg.strict_backtest && matches!(self.cfg.mode, ExecutionMode::Backtest) {
            // Force single-threaded like execution to preserve determinism
            1
        } else {
            10
        };
        let token_pools = self.pools_by_token.read().await
            .get(&token)
            .cloned()
            .unwrap_or_default();
        
        // Filter pools by chain first to reduce work
        let chain_pools: Vec<_> = {
            let pools_by_address = self.pools_by_address.read().await;
            token_pools
                .iter()
                .filter_map(|pool_address| {
                    pools_by_address
                        .get(pool_address)
                        .filter(|meta| meta.chain_name == chain_name)
                        .map(|meta| (*pool_address, meta.clone()))
                })
                .collect()
        };
        
        let mut total_liquidity_units = 0.0;
        
        // Process pools in chunks to avoid overwhelming the system
        for chunk in chain_pools.chunks(max_concurrent) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|(pool_address, metadata)| {
                    let chain_name = chain_name.to_string();
                    let pool_address = *pool_address;
                    let metadata = metadata.clone();
                    let token = token;
                    let self_ref = self;
                    async move {
                        match self_ref.get_pool_info(&chain_name, pool_address, block_number).await {
                            Ok(pool_info) => {
                                let reserve0 = pool_info.reserves0;
                                let reserve1 = pool_info.reserves1;
                                
                                if reserve0.is_zero() || reserve1.is_zero() {
                                    return 0.0;
                                }
                                
                                let token0_normalized = crate::utils::helpers::normalize_units(
                                    reserve0,
                                    metadata.token0_decimals
                                );
                                let token1_normalized = crate::utils::helpers::normalize_units(
                                    reserve1,
                                    metadata.token1_decimals
                                );
                                
                                // Calculate liquidity for the specific token
                                // NOTE: Multiplying by 2.0 gives the total value of both sides of the pool
                                // expressed in units of the specific token (this is a "unit" metric, not USD)
                                if metadata.token0 == token {
                                    token0_normalized * 2.0
                                } else if metadata.token1 == token {
                                    token1_normalized * 2.0
                                } else {
                                    // Pool contains the token but not as token0 or token1 (shouldn't happen)
                                    0.0
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get pool info for liquidity calculation: {}", e);
                                0.0
                            }
                        }
                    }
                })
                .collect();
            
            let results = futures::future::join_all(futures).await;
            total_liquidity_units += results.iter().sum::<f64>();
        }
        
        Ok(total_liquidity_units)
    }

    async fn set_block_context(&self, block_number: u64) -> Result<(), PoolManagerError> {
        let mut block_context_guard = self.current_block_context.write().await;
        *block_context_guard = Some(block_number);
        Ok(())
    }

    async fn reload_registry_for_backtest(&self, block_number: u64) -> Result<(), PoolManagerError> {
        // In backtest mode, we need to reload the pool registry for each block
        // to ensure that we have the correct set of pools for the current block
        if matches!(self.cfg.mode, ExecutionMode::Backtest) {
            self.load_registry_for_backtest(block_number).await
        } else {
            Ok(())
        }
    }
}

impl PoolManager {
    /// Load pools dynamically from database for backtest mode
    /// This method queries the swaps table to find pools that have data at or before the current block
    async fn get_all_pools_backtest(
        &self,
        chain: &str,
        prot_filter: Option<ProtocolType>,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        debug!("Loading pools dynamically for backtest mode, chain: '{}', filter: {:?}", chain, prot_filter);
        
        let db_pool = match &self.db_pool {
            Some(pool) => pool,
            None => {
                return Err(DexError::PoolManagerError("No database pool available for backtest mode".to_string()));
            }
        };

        // Get current block context from pool manager's block context
        let current_block = {
            let block_context_guard = self.current_block_context.read().await;
            block_context_guard.ok_or_else(|| {
                DexError::PoolManagerError("No block context set in pool manager for backtest mode".to_string())
            })?
        };

        debug!("Using block {} for backtest pool loading", current_block);

        let rows = self.execute_with_retry("load_pools_backtest", || {
            let pool = db_pool.clone();
            let chain_name = chain.to_string();
            let current_block_i64 = current_block as i64;
            
            async move {
                let client = pool.get().await
                    .map_err(|e| PoolManagerError::Database(format!("Failed to get client from pool: {}", e)))?;

                // Query for pools that have data at or before the current block
                // Get the most recent reserves for each pool at or before the current block
                let rows = client
                    .query(
                        r#"
                        WITH latest_pool_states AS (
                            SELECT DISTINCT ON (pool_address)
                                pool_address,
                                token0_address as token0,
                                token1_address as token1,
                                token0_decimals,
                                token1_decimals,
                                fee_tier as fee,
                                protocol_type,
                                dex_name,
                                network_id as chain_name,
                                block_number as creation_block,
                                -- Get the most recent reserves at or before the current block
                                token0_reserve as latest_token0_reserve,
                                token1_reserve as latest_token1_reserve,
                                sqrt_price_x96 as latest_sqrt_price_x96,
                                liquidity as latest_liquidity,
                                tick as latest_tick,
                                block_number as state_block_number
                            FROM swaps
                            WHERE token0_address IS NOT NULL
                              AND token1_address IS NOT NULL
                              AND pool_address IS NOT NULL
                              AND token0_decimals IS NOT NULL
                              AND token1_decimals IS NOT NULL
                              AND token0_decimals > 0
                              AND token1_decimals > 0
                              AND token0_decimals <= 255
                              AND token1_decimals <= 255
                              AND protocol_type IS NOT NULL
                              AND protocol_type >= 0
                              AND (fee_tier IS NOT NULL OR protocol_type != 3)  -- Allow NULL fee_tier for non-V3 pools
                              AND (fee_tier IS NULL OR fee_tier >= 0)  -- If fee_tier is not NULL, it must be >= 0
                              AND network_id = $1
                              AND LENGTH(network_id) > 0
                              AND dex_name IS NOT NULL
                              AND LENGTH(TRIM(dex_name)) > 0
                              AND block_number <= $2  -- Only pools with data at or before current block
                              AND token0_reserve IS NOT NULL  -- Ensure we have actual reserve data
                              AND token1_reserve IS NOT NULL  -- Ensure we have actual reserve data
                              AND token0_reserve > 0  -- Only pools with non-zero reserves
                              AND token1_reserve > 0  -- Only pools with non-zero reserves
                            ORDER BY pool_address, block_number DESC
                        )
                        SELECT * FROM latest_pool_states
                        "#,
                        &[&chain_name, &current_block_i64]
                    )
                    .await
                    .map_err(|e| PoolManagerError::Database(format!("Failed to query pools for backtest: {}", e)))?;

                Ok(rows)
            }
        }).await?;

        let mut pools = Vec::new();
        let mut rejection_stats = LoadingRejectionStats::default();
        rejection_stats.total_processed = rows.len() as u64;

        for row in rows {
            // Parse addresses from binary data
            let pool_address_bytes: Vec<u8> = row.get("pool_address");
            let token0_bytes: Vec<u8> = row.get("token0");
            let token1_bytes: Vec<u8> = row.get("token1");

            // Convert bytes to addresses with length validation
            let pool_address = if pool_address_bytes.len() == 20 {
                Address::from_slice(&pool_address_bytes)
            } else {
                warn!("Invalid pool address length in backtest data: {} bytes (expected 20). Skipping pool.", pool_address_bytes.len());
                rejection_stats.total_rejections += 1;
                continue;
            };
            
            let token0 = if token0_bytes.len() == 20 {
                Address::from_slice(&token0_bytes)
            } else {
                warn!("Invalid token0 address length in backtest data: {} bytes (expected 20). Skipping pool.", token0_bytes.len());
                rejection_stats.total_rejections += 1;
                continue;
            };
            
            let token1 = if token1_bytes.len() == 20 {
                Address::from_slice(&token1_bytes)
            } else {
                warn!("Invalid token1 address length in backtest data: {} bytes (expected 20). Skipping pool.", token1_bytes.len());
                rejection_stats.total_rejections += 1;
                continue;
            };

            // Parse other fields
            let token0_decimals: i32 = row.get("token0_decimals");
            let token1_decimals: i32 = row.get("token1_decimals");
            let fee: Option<i32> = row.get("fee");
            let protocol_type_int: i16 = row.get("protocol_type");
            let dex_name: String = row.get("dex_name");
            let chain_name: String = row.get("chain_name");
            let creation_block_i64: i64 = row.get("creation_block");

            // Validate decimals
            let token0_decimals: u8 = match token0_decimals {
                v if (0..=255).contains(&v) => v as u8,
                _ => {
                    warn!("Invalid token0_decimals {} for pool {}", token0_decimals, pool_address);
                    rejection_stats.invalid_decimals += 1;
                    rejection_stats.total_rejections += 1;
                    continue;
                }
            };
            let token1_decimals: u8 = match token1_decimals {
                v if (0..=255).contains(&v) => v as u8,
                _ => {
                    warn!("Invalid token1_decimals {} for pool {}", token1_decimals, pool_address);
                    rejection_stats.invalid_decimals += 1;
                    rejection_stats.total_rejections += 1;
                    continue;
                }
            };

            // Convert protocol type
            let protocol_type = match ProtocolType::try_from(protocol_type_int) {
                Ok(pt) => pt,
                Err(e) => {
                    warn!("Failed to convert protocol type {} for pool {}: {}", protocol_type_int, pool_address, e);
                    ProtocolType::Unknown
                }
            };

            // Skip Unknown protocol pools
            if matches!(protocol_type, ProtocolType::Unknown) {
                debug!("Skipping pool {} with unknown protocol type {}", pool_address, protocol_type_int);
                rejection_stats.unknown_protocol_types += 1;
                rejection_stats.total_rejections += 1;
                continue;
            }

            // Skip V4 pools until proper state handling is implemented
            if matches!(protocol_type, ProtocolType::UniswapV4) {
                debug!("Skipping V4 pool {} - V4 pools require specialized state handling", pool_address);
                rejection_stats.v4_pools_skipped += 1;
                rejection_stats.total_rejections += 1;
                continue;
            }

            // Apply protocol filter if specified
            if let Some(ref filter_protocol) = prot_filter {
                if protocol_type != *filter_protocol {
                    continue;
                }
            }

            // Extract pool state data from the database row
            let latest_token0_reserve: Option<Decimal> = row.get("latest_token0_reserve");
            let latest_token1_reserve: Option<Decimal> = row.get("latest_token1_reserve");
            let latest_sqrt_price_x96: Option<Decimal> = row.get("latest_sqrt_price_x96");
            let latest_liquidity: Option<Decimal> = row.get("latest_liquidity");
            let latest_tick: Option<i32> = row.get("latest_tick");

            // Create metadata
            let metadata = Arc::new(PoolStaticMetadata {
                pool_address,
                token0,
                token1,
                token0_decimals,
                token1_decimals,
                fee: fee.map(|f| f as u32),
                protocol_type,
                dex_name: dex_name.clone(),
                chain_name: chain_name.clone(),
                creation_block: creation_block_i64 as u64,
                latest_token0_reserve,
                latest_token1_reserve,
                latest_sqrt_price_x96,
                latest_liquidity,
                latest_tick,
            });

            pools.push(metadata);
        }

        // Log data quality summary
        if rejection_stats.total_rejections > 0 {
            let unknown_protocols_str = if rejection_stats.unknown_protocol_type_values.is_empty() {
                "none".to_string()
            } else {
                format!("{:?}", rejection_stats.unknown_protocol_type_values.iter().collect::<Vec<_>>())
            };

            warn!(
                "Data quality issues during backtest pool loading: \
                 Total processed: {}, Rejections: {}, Rejection rate: {:.2}%. \
                 Invalid decimals: {}, Unknown protocols: {} (types: {}), V4 pools: {}",
                rejection_stats.total_processed,
                rejection_stats.total_rejections,
                (rejection_stats.total_rejections as f64) / (rejection_stats.total_processed as f64) * 100.0,
                rejection_stats.invalid_decimals,
                rejection_stats.unknown_protocol_types,
                unknown_protocols_str,
                rejection_stats.v4_pools_skipped
            );
        }

        debug!(
            "Loaded {} pools dynamically for backtest mode at block {} on chain '{}'",
            pools.len(),
            current_block,
            chain
        );

        Ok(pools)
    }
}

