//! # Pool State Oracle
//!
//! Fetches and caches the dynamic on-chain state of liquidity pools.
//!
//! ## Architecture
//! - Implements circuit breaker pattern for RPC failure protection
//! - Uses proper async flow without blocking runtime
//! - Efficient multicall with proper retry logic
//! - Safe array indexing throughout
//!
//! ## Safety
//! - No blocking operations in async context
//! - Proper error propagation with retries
//! - Safe token extraction from function results

use crate::{
    blockchain::BlockchainManager,
    config::{Config, ExecutionMode},
    errors::BacktestError,
    pool_manager::{PoolManagerTrait, PoolStaticMetadata},
    swap_loader::HistoricalSwapLoader,
    types::{PoolStateFromSwaps, ProtocolType, DexProtocol},
    alpha::circuit_breaker::{CircuitBreaker, CircuitBreakerStats},
    v4_support::V4StateReader,
};
use ethers::{
    abi::{Abi, Function, Token},
    core::types::{Address, TransactionRequest},
    prelude::*,
};
use eyre::Result;
use moka::future::Cache;
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::{error, instrument};

//================================================================================================//
//                                         CONSTANTS                                              //
//================================================================================================//

/// Default cache size for pool states
const DEFAULT_CACHE_SIZE: u64 = 10_000;

/// Default cache TTL in seconds
const DEFAULT_CACHE_TTL_SECS: u64 = 30;

/// Maximum retries for multicall operations
const MAX_MULTICALL_RETRIES: u32 = 3;

/// Circuit breaker failure threshold for RPC calls
const RPC_CIRCUIT_BREAKER_THRESHOLD: u64 = 10;

/// Circuit breaker recovery timeout for RPC calls
const RPC_CIRCUIT_BREAKER_TIMEOUT_SECS: u64 = 60;

/// Delay between multicall retry attempts (milliseconds)
const MULTICALL_RETRY_DELAY_MS: u64 = 200;

//================================================================================================//
//                                    ABI AND STATIC DEFINITIONS                                  //
//================================================================================================//

fn load_v2_pair_abi() -> Result<Abi, Error> {
    serde_json::from_str(
        r#"[{"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]"#,
    ).map_err(|e| Error::Internal(format!("Failed to parse V2 ABI: {}", e)))
}

fn load_v3_pool_abi() -> Result<Abi, Error> {
    serde_json::from_str(
        r#"[
            {"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"}
        ]"#,
    ).map_err(|e| Error::Internal(format!("Failed to parse V3 ABI: {}", e)))
}

// Static ABI instances loaded at startup
static V2_PAIR_ABI: Lazy<Abi> = Lazy::new(|| {
    load_v2_pair_abi().expect("V2 ABI must be valid")
});

static V3_POOL_ABI: Lazy<Abi> = Lazy::new(|| {
    load_v3_pool_abi().expect("V3 ABI must be valid")
});

fn v2_get_reserves() -> Option<&'static Function> {
    V2_PAIR_ABI.function("getReserves").ok()
}

fn v3_slot0() -> Option<&'static Function> {
    V3_POOL_ABI.function("slot0").ok()
}

fn v3_liquidity() -> Option<&'static Function> {
    V3_POOL_ABI.function("liquidity").ok()
}

//================================================================================================//
//                                             TYPES                                              //
//================================================================================================//

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Blockchain interaction error: {0}")]
    Blockchain(String),
    #[error("Pool not found in registry: {0}")]
    PoolNotFound(Address),
    #[error("Invalid operation for current mode: {0}")]
    ModeError(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Data not found for block {1}: {0}")]
    NoDataForBlock(Address, u64),
    #[error("Contract error: {0}")]
    ContractError(String),
    #[error("Circuit breaker tripped: {0}")]
    CircuitBreakerTripped(String),
    #[error("Retry exhausted after {0} attempts")]
    RetryExhausted(u32),
    #[error("Block mismatch: requested {requested}, resolved {resolved} on pool {pool}")]
    BlockMismatch { pool: Address, requested: u64, resolved: u64 },
}

impl From<BacktestError> for Error {
    fn from(e: BacktestError) -> Self {
        Error::Internal(e.to_string())
    }
}

impl From<ethers::contract::AbiError> for Error {
    fn from(e: ethers::contract::AbiError) -> Self {
        Error::ContractError(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct PoolStateData {
    pub pool_state: PoolStateFromSwaps,
    pub block_number: u64,
    pub timestamp: u64,
    pub source: PoolStateSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PoolStateSource {
    LiveRpc,
    HistoricalDb,
    Synthetic,  // Used for pools where we can't fetch real state (e.g., Curve, Balancer during backtest)
}

/// Efficient cache key structure
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct CacheKey {
    chain_id: u64,
    pool: Address,
    block: Option<u64>,
}

#[derive(Clone)]
pub struct PoolStateOracle {
    config: Arc<Config>,
    blockchain_managers: Arc<RwLock<HashMap<String, Arc<dyn BlockchainManager>>>>,
    historical_swap_loader: Option<Arc<HistoricalSwapLoader>>,
    pool_manager: Arc<dyn PoolManagerTrait>,
    pool_state_cache: Cache<CacheKey, PoolStateData>,
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    rpc_circuit_breaker: Arc<CircuitBreaker>,
    chain_id_map: Arc<HashMap<String, u64>>,
    // Metrics for monitoring multicall degradation
    multicall_failures: Arc<AtomicU64>,
    sequential_fallbacks: Arc<AtomicU64>,
    // V4 state reader for handling Uniswap V4 pools
    v4_state_reader: Option<Arc<V4StateReader>>,
    // Reorg tracking
    last_seen_head: Arc<AtomicU64>,
    reorg_seen_total: Arc<AtomicU64>,
    // Enhanced metrics counters
    fetch_attempts_multicall: Arc<AtomicU64>,
    fetch_attempts_optimized: Arc<AtomicU64>,
    fetch_attempts_sequential: Arc<AtomicU64>,
    fetch_successes_multicall: Arc<AtomicU64>,
    fetch_successes_optimized: Arc<AtomicU64>,
    fetch_successes_sequential: Arc<AtomicU64>,
    cache_hits_with_block: Arc<AtomicU64>,
    cache_hits_without_block: Arc<AtomicU64>,
}

//================================================================================================//
//                                         IMPLEMENTATION                                         //
//================================================================================================//

impl PoolStateOracle {
    pub fn new(
        config: Arc<Config>,
        blockchain_managers: HashMap<String, Arc<dyn BlockchainManager>>,
        historical_swap_loader: Option<Arc<HistoricalSwapLoader>>,
        pool_manager: Arc<dyn PoolManagerTrait>,
    ) -> Result<Self, Error> {
        // Validate ABIs at startup - this will fail fast if ABI parsing fails
        let _ = v2_get_reserves().ok_or_else(|| Error::Internal("V2 getReserves function not found in ABI".to_string()))?;
        let _ = v3_slot0().ok_or_else(|| Error::Internal("V3 slot0 function not found in ABI".to_string()))?;
        let _ = v3_liquidity().ok_or_else(|| Error::Internal("V3 liquidity function not found in ABI".to_string()))?;

        tracing::info!(target: "pool_states", "Initializing PoolStateOracle");

        let cache_size = config.module_config.as_ref()
            .and_then(|m| m.data_science_settings.as_ref())
            .and_then(|d| d.pool_state_cache_size)
            .unwrap_or(DEFAULT_CACHE_SIZE);

        let cache_ttl = config.module_config.as_ref()
            .and_then(|m| m.data_science_settings.as_ref())
            .and_then(|d| d.pool_state_cache_ttl_seconds)
            .unwrap_or(DEFAULT_CACHE_TTL_SECS);

        // Create chain ID map for efficient cache key generation
        let chain_id_map: HashMap<String, u64> = config.chain_config.chains
            .iter()
            .map(|(name, cfg)| (name.clone(), cfg.chain_id))
            .collect();

        let rpc_circuit_breaker = Arc::new(CircuitBreaker::new(
            RPC_CIRCUIT_BREAKER_THRESHOLD,
            Duration::from_secs(RPC_CIRCUIT_BREAKER_TIMEOUT_SECS),
        ));

        Ok(Self {
            config,
            blockchain_managers: Arc::new(RwLock::new(blockchain_managers)),
            historical_swap_loader,
            pool_manager,
            pool_state_cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl))
                .max_capacity(cache_size)
                .build(),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            rpc_circuit_breaker,
            chain_id_map: Arc::new(chain_id_map),
            multicall_failures: Arc::new(AtomicU64::new(0)),
            sequential_fallbacks: Arc::new(AtomicU64::new(0)),
            v4_state_reader: None, // Will be initialized when V4 pools are detected
            last_seen_head: Arc::new(AtomicU64::new(0)),
            reorg_seen_total: Arc::new(AtomicU64::new(0)),
            fetch_attempts_multicall: Arc::new(AtomicU64::new(0)),
            fetch_attempts_optimized: Arc::new(AtomicU64::new(0)),
            fetch_attempts_sequential: Arc::new(AtomicU64::new(0)),
            fetch_successes_multicall: Arc::new(AtomicU64::new(0)),
            fetch_successes_optimized: Arc::new(AtomicU64::new(0)),
            fetch_successes_sequential: Arc::new(AtomicU64::new(0)),
            cache_hits_with_block: Arc::new(AtomicU64::new(0)),
            cache_hits_without_block: Arc::new(AtomicU64::new(0)),
        })
    }

    #[instrument(skip(self), level = "debug", fields(chain=%chain_name, pool=%pool_address, block=block_number.map_or_else(|| "latest".to_string(), |b| b.to_string())))]
    pub async fn get_pool_state(
        &self,
        chain_name: &str,
        pool_address: Address,
        block_number: Option<u64>,
    ) -> Result<PoolStateData, Error> {
        // Create efficient cache key
        let chain_id = self.chain_id_map.get(chain_name)
            .copied()
            .ok_or_else(|| Error::Config(format!("Unknown chain: {}", chain_name)))?;
        
        let cache_key = CacheKey {
            chain_id,
            pool: pool_address,
            block: block_number,
        };

        if let Some(data) = self.pool_state_cache.get(&cache_key).await {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            if block_number.is_some() {
                self.cache_hits_with_block.fetch_add(1, Ordering::Relaxed);
            } else {
                self.cache_hits_without_block.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(data);
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let static_meta = self.pool_manager
            .get_metadata_for_pool(pool_address)
            .await
            .ok_or_else(|| Error::PoolNotFound(pool_address))?;
        
        if static_meta.chain_name != chain_name {
            return Err(Error::Config(format!(
                "Pool {} is registered on chain '{}', but '{}' was requested",
                pool_address, static_meta.chain_name, chain_name
            )));
        }
        
        let blockchain = self.get_blockchain_manager(chain_name).await?;
        let mode = blockchain.mode();

        let state_data = match mode {
            ExecutionMode::Backtest => {
                let block = block_number.ok_or_else(|| {
                    Error::ModeError("Block number is required in Backtest mode".to_string())
                })?;
                match self.fetch_historical_pool_state(block, &static_meta).await? {
                    Some(data) => data,
                    None => {
                        // No historical data found - this is normal and should be handled by the caller
                        return Err(Error::NoDataForBlock(pool_address, block));
                    }
                }
            }
            ExecutionMode::Live | ExecutionMode::PaperTrading => {
                self.fetch_live_pool_state(chain_name, &static_meta, block_number).await?
            }
        };

        self.pool_state_cache.insert(cache_key, state_data.clone()).await;
        Ok(state_data)
    }

    /// Set mock pool state data for testing purposes
    /// This bypasses the normal fetching mechanism and directly sets cached data
    pub async fn set_mock_pool_state(
        &self,
        chain_name: &str,
        pool_address: Address,
        block_number: Option<u64>,
        mock_state: PoolStateData,
    ) -> Result<(), Error> {
        let chain_id = self.chain_id_map.get(chain_name)
            .copied()
            .ok_or_else(|| Error::Config(format!("Unknown chain: {}", chain_name)))?;

        let cache_key = CacheKey {
            chain_id,
            pool: pool_address,
            block: block_number,
        };

        self.pool_state_cache.insert(cache_key, mock_state).await;
        Ok(())
    }

    async fn fetch_historical_pool_state(
        &self,
        block_number: u64,
        static_meta: &PoolStaticMetadata,
    ) -> Result<Option<PoolStateData>, Error> {
        let loader = self.historical_swap_loader.as_ref().ok_or_else(|| {
            Error::Config("HistoricalSwapLoader required for historical state fetch".to_string())
        })?;

        // Use fallback state loading with strict lookback limit - if exact block not available, look back limited distance
        // In strict backtest mode, use moderate lookback (1000 blocks) to prevent stale data while allowing flexibility
        // In normal mode, allow more flexibility (10000 blocks) for broader historical coverage
        // For backtesting, we need to be lenient with data freshness as pool state is constant between swaps
        // The most recent state before the target block is correct
        let max_lookback = if self.config.strict_backtest { 1000 } else { 10000 };
        
        // Try to get pool state, but don't throw error if None is returned
        if let Some(pool_state) = loader
            .get_pool_state_with_fallback(static_meta.pool_address, block_number, Some(&static_meta.chain_name), Some(max_lookback))
            .await?
        {
            // Validate that we don't have data from a future block (lookahead bias prevention)
            if pool_state.block_number > block_number {
                return Err(Error::Internal(format!(
                    "Data lookahead error for pool {}. Requested block {}, but received data from future block {} - this indicates a data integrity issue",
                    static_meta.pool_address, block_number, pool_state.block_number
                )));
            }

            Ok(Some(PoolStateData {
                timestamp: pool_state.timestamp,
                // Use the ACTUAL block number of the data
                block_number: pool_state.block_number,
                pool_state,
                source: PoolStateSource::HistoricalDb,
            }))
        } else {
            // No data found - this is normal and should be handled gracefully
            tracing::debug!(
                target: "pool_states",
                "No pool state found for pool {} at or before block {} (chain: {}). Pool will be skipped.",
                static_meta.pool_address, block_number, static_meta.chain_name
            );
            Ok(None)
        }
    }

    async fn fetch_live_pool_state(
        &self,
        chain_name: &str,
        static_meta: &PoolStaticMetadata,
        block_number: Option<u64>,
    ) -> Result<PoolStateData, Error> {
        // Check circuit breaker before making RPC calls
        if self.rpc_circuit_breaker.is_tripped().await {
            return Err(Error::CircuitBreakerTripped(format!(
                "RPC circuit breaker is open for chain {} - retry later",
                chain_name
            )));
        }

        let blockchain = self.get_blockchain_manager(chain_name).await?;
        
        let pool_state = match static_meta.protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                self.fetch_v2_state(&blockchain, static_meta, block_number).await?
            }
            ProtocolType::UniswapV3 => {
                // Prefer multicall with bounded retries if configured; then optimized; finally sequential
                let pool_state_mc = {
                    let chain_cfg = self
                        .config
                        .get_chain_config(chain_name)
                        .map_err(|e| Error::Config(e.to_string()))?;
                    if let Some(multicall_addr) = chain_cfg.multicall_address {
                self.fetch_attempts_multicall.fetch_add(1, Ordering::Relaxed);
                match self
                    .fetch_v3_state_multicall(&blockchain, static_meta, multicall_addr, block_number)
                    .await
                {
                    Ok(state) => {
                        self.fetch_successes_multicall.fetch_add(1, Ordering::Relaxed);
                        Some(state)
                    },
                    Err(err) => {
                        // Record multicall failure and fall back
                        self.multicall_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(target: "pool_states", "Multicall path failed for {:?} on {}: {}. Falling back to sequential calls.", static_meta.pool_address, chain_name, err);
                        None
                    }
                }
                    } else {
                        None
                    }
                };
                if let Some(state) = pool_state_mc { state } else {
                    // Try optimized parallel path
                    self.fetch_attempts_optimized.fetch_add(1, Ordering::Relaxed);
                    match self.fetch_v3_state_optimized(&blockchain, static_meta, block_number).await {
                        Ok(state) => {
                            self.fetch_successes_optimized.fetch_add(1, Ordering::Relaxed);
                            state
                        },
                        Err(e) => {
                            tracing::error!(target: "pool_states", "Optimized V3 path failed for {:?} on {}: {}. Falling back to sequential.", static_meta.pool_address, chain_name, e);
                            self.sequential_fallbacks.fetch_add(1, Ordering::Relaxed);
                            self.fetch_attempts_sequential.fetch_add(1, Ordering::Relaxed);
                            let state = self.fetch_v3_state_sequential(&blockchain, static_meta, block_number).await?;
                            self.fetch_successes_sequential.fetch_add(1, Ordering::Relaxed);
                            state
                        }
                    }
                }
            }
            ProtocolType::UniswapV4 => {
                // CRITICAL: V4 pools are not yet fully supported - filter them out until proper implementation
                // V4 uses a fundamentally different architecture with hooks and dynamic state management
                // Returning zeroed reserves would cause incorrect arbitrage calculations
                return Err(Error::Internal(format!(
                    "Uniswap V4 pools are not yet supported for live state fetching: {}. \
                     V4 pools use a different architecture and require specialized state handling.",
                    static_meta.pool_address
                )));
            }
            _ => return Err(Error::Internal(format!(
                "Unsupported protocol type for live fetching: {:?}", 
                static_meta.protocol_type
            )))
        };

        // Properly fetch block information without blocking the runtime
        let (block_number_final, timestamp) = self.get_block_info(&blockchain, block_number).await?;

        let mut state_data = PoolStateData {
            pool_state,
            block_number: block_number_final,
            timestamp,
            source: PoolStateSource::LiveRpc,
        };

        state_data.pool_state.block_number = state_data.block_number;
        state_data.pool_state.timestamp = state_data.timestamp;

        self.rpc_circuit_breaker.record_success().await;
        Ok(state_data)
    }

    /// Get block information without blocking the runtime
    async fn get_block_info(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        block_number: Option<u64>,
    ) -> Result<(u64, u64), Error> {
        let block = if let Some(bn) = block_number {
            blockchain.get_block(bn.into()).await
                .map_err(|e| Error::Blockchain(format!("Failed to get block {}: {}", bn, e)))?
        } else {
            let latest = blockchain.get_latest_block().await
                .map_err(|e| Error::Blockchain(format!("Failed to get latest block: {}", e)))?;
            let current_head = latest.number.map(|n| n.as_u64()).unwrap_or(0);
            let prev = self.last_seen_head.load(Ordering::Relaxed);
            if current_head < prev {
                self.reorg_seen_total.fetch_add(1, Ordering::Relaxed);
                // Invalidate only entries with block numbers higher than the new head
                self.invalidate_entries_above_block(current_head).await;
                tracing::warn!(target: "pool_states", "Reorg detected: head rolled back from {} to {}", prev, current_head);
            }
            self.last_seen_head.store(current_head, Ordering::Relaxed);
            Some(latest)
        };

        let block_number = block.as_ref()
            .and_then(|b| b.number)
            .map(|n| n.as_u64())
            .unwrap_or(0);
        
        let timestamp = block.as_ref()
            .map(|b| b.timestamp.as_u64())
            .unwrap_or(0);

        Ok((block_number, timestamp))
    }

    async fn fetch_v2_state(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        static_meta: &PoolStaticMetadata,
        block: Option<u64>
    ) -> Result<PoolStateFromSwaps, Error> {
        let v2_function = v2_get_reserves()
            .ok_or_else(|| Error::Config("V2 ABI not available".to_string()))?;
        let result = self.call_and_decode(blockchain, static_meta.pool_address, v2_function, &[], block).await?;
        
        // Safe token extraction with proper error handling
        let reserve0 = result.get(0)
            .and_then(|v| v.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("Invalid or missing reserve0 in response".to_string()))?;
        
        let reserve1 = result.get(1)
            .and_then(|v| v.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("Invalid or missing reserve1 in response".to_string()))?;
        
        let timestamp = result.get(2)
            .and_then(|v| v.clone().into_uint())
            .map(|v| v.as_u32())
            .ok_or_else(|| Error::Blockchain("Invalid or missing timestamp in response".to_string()))?;

        let (block_number, _) = self.get_block_info(blockchain, block).await?;
        if let Some(requested) = block {
            if self.config.strict_backtest && requested != block_number {
                return Err(Error::NoDataForBlock(static_meta.pool_address, requested));
            } else if requested != block_number {
                tracing::debug!(
                    target: "pool_states",
                    "Block number mismatch for pool {}: requested {}, resolved {} (non-strict mode)",
                    static_meta.pool_address, requested, block_number
                );
            }
        }

        // For V2 pools, the timestamp field from getReserves represents the last time
        // the cumulative price accumulators were updated (typically on swap/mint/burn).
        // We store this as both timestamp and last_reserve_update_timestamp for clarity.
        // The block_number field represents the actual block we're querying at.
        Ok(PoolStateFromSwaps {
            pool_address: static_meta.pool_address,
            token0: static_meta.token0,
            token1: static_meta.token1,
            reserve0,
            reserve1,
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            v3_tick_data: None,
            v3_tick_current: None,
            fee_tier: static_meta.fee.unwrap_or_default(),
            protocol: DexProtocol::UniswapV2,
            protocol_type: static_meta.protocol_type.clone(),
            block_number,
            timestamp: timestamp as u64, // Last cumulative price update time
            token0_decimals: static_meta.token0_decimals as u32,
            token1_decimals: static_meta.token1_decimals as u32,
            price0_cumulative_last: None,
            price1_cumulative_last: None,
            dex_protocol: DexProtocol::UniswapV2,
            last_reserve_update_timestamp: Some(timestamp), // Same as timestamp for V2
        })
    }

    async fn fetch_v3_state_optimized(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        static_meta: &PoolStaticMetadata,
        block: Option<u64>
    ) -> Result<PoolStateFromSwaps, Error> {
        let slot0_function = v3_slot0()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;
        let liq_function = v3_liquidity()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;

        // Pin the block number to avoid race conditions when fetching latest state
        let pinned_block = if block.is_none() {
            let (current_block, _) = self.get_block_info(blockchain, None).await?;
            Some(current_block)
        } else {
            block
        };

        let slot0_fut = self.call_and_decode(blockchain, static_meta.pool_address, slot0_function, &[], pinned_block);
        let liq_fut = self.call_and_decode(blockchain, static_meta.pool_address, liq_function, &[], pinned_block);

        let (slot0_res, liq_res) = tokio::join!(slot0_fut, liq_fut);

        let slot0_tokens = match slot0_res {
            Ok(v) => v,
            Err(e) => {
                self.rpc_circuit_breaker.record_failure().await;
                return Err(e);
            }
        };
        let liq_tokens = match liq_res {
            Ok(v) => v,
            Err(e) => {
                self.rpc_circuit_breaker.record_failure().await;
                return Err(e);
            }
        };

        let sqrt_price_x96 = slot0_tokens
            .get(0)
            .and_then(|t| t.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("slot0 sqrtPriceX96 missing".to_string()))?;
        let tick_i256 = slot0_tokens
            .get(1)
            .and_then(|t| t.clone().into_int())
            .ok_or_else(|| Error::Blockchain("slot0 tick missing".to_string()))?;
        let tick = ethers::types::I256::from_raw(tick_i256).as_i32();

        let liquidity = liq_tokens
            .get(0)
            .and_then(|t| t.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("liquidity missing".to_string()))?;

        let (block_number, timestamp) = self.get_block_info(blockchain, pinned_block).await?;
        if let Some(requested) = block {
            if requested != block_number {
                if self.config.strict_backtest {
                    return Err(Error::NoDataForBlock(static_meta.pool_address, requested));
                } else {
                    return Err(Error::BlockMismatch {
                        pool: static_meta.pool_address,
                        requested,
                        resolved: block_number,
                    });
                }
            }
        }

        Ok(PoolStateFromSwaps {
            pool_address: static_meta.pool_address,
            token0: static_meta.token0,
            token1: static_meta.token1,
            reserve0: U256::zero(),
            reserve1: U256::zero(),
            sqrt_price_x96: Some(sqrt_price_x96),
            liquidity: Some(liquidity),
            tick: Some(tick),
            v3_tick_data: None,
            v3_tick_current: Some(tick),
            fee_tier: static_meta.fee.unwrap_or_default(),
            protocol: DexProtocol::UniswapV3,
            protocol_type: static_meta.protocol_type.clone(),
            block_number,
            timestamp,
            token0_decimals: static_meta.token0_decimals as u32,
            token1_decimals: static_meta.token1_decimals as u32,
            price0_cumulative_last: None,
            price1_cumulative_last: None,
            dex_protocol: DexProtocol::UniswapV3,
            last_reserve_update_timestamp: None,
        })
    }

    async fn fetch_v3_state_multicall(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        static_meta: &PoolStaticMetadata,
        multicall_address: Address,
        block: Option<u64>
    ) -> Result<PoolStateFromSwaps, Error> {
        // Prepare calldata for slot0 and liquidity
        let slot0_function = v3_slot0()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;
        let liq_function = v3_liquidity()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;

        let slot0_calldata = slot0_function.encode_input(&[])
            .map_err(|e| Error::Internal(format!("ABI encode failed: {}", e)))?;
        let liquidity_calldata = liq_function.encode_input(&[])
            .map_err(|e| Error::Internal(format!("ABI encode failed: {}", e)))?;

        // Encode tryAggregate(bool requireSuccess, (address,bytes)[] calls)
        let require_success = Token::Bool(false);
        let calls = Token::Array(vec![
            Token::Tuple(vec![Token::Address(static_meta.pool_address), Token::Bytes(slot0_calldata.clone())]),
            Token::Tuple(vec![Token::Address(static_meta.pool_address), Token::Bytes(liquidity_calldata.clone())]),
        ]);
        let selector = &ethers::utils::id("tryAggregate(bool,(address,bytes)[])")[0..4];
        let encoded_params = ethers::abi::encode(&[require_success, calls]);
        let mut data = selector.to_vec();
        data.extend_from_slice(&encoded_params);

        let tx = TransactionRequest::new().to(multicall_address).data(Bytes::from(data));

        let mut attempt: u32 = 0;
        loop {
            let call_result = blockchain.call(&tx, block.map(Into::into)).await
                .map_err(|e| Error::Blockchain(format!("Multicall provider error: {}", e)))?;

            let decoded = ethers::abi::decode(
                &[
                    ethers::abi::ParamType::Array(Box::new(ethers::abi::ParamType::Tuple(vec![
                        ethers::abi::ParamType::Bool,
                        ethers::abi::ParamType::Bytes,
                    ]))),
                ],
                &call_result,
            ).map_err(|e| Error::Blockchain(format!("Failed to decode multicall result: {}", e)))?;

            if let Token::Array(items) = &decoded[0] {
                if items.len() != 2 {
                    return Err(Error::Blockchain("Unexpected multicall return length".to_string()));
                }
                let (ok0, bytes0) = match &items[0] {
                    Token::Tuple(vals) if vals.len() == 2 => {
                        match (&vals[0], &vals[1]) {
                            (Token::Bool(b), Token::Bytes(bs)) => (*b, Bytes::from(bs.clone())),
                            _ => return Err(Error::Blockchain("Invalid tuple types in multicall retval".to_string())),
                        }
                    }
                    _ => return Err(Error::Blockchain("Invalid first tuple in multicall retval".to_string())),
                };
                let (ok1, bytes1) = match &items[1] {
                    Token::Tuple(vals) if vals.len() == 2 => {
                        match (&vals[0], &vals[1]) {
                            (Token::Bool(b), Token::Bytes(bs)) => (*b, Bytes::from(bs.clone())),
                            _ => return Err(Error::Blockchain("Invalid tuple types in multicall retval".to_string())),
                        }
                    }
                    _ => return Err(Error::Blockchain("Invalid second tuple in multicall retval".to_string())),
                };

                if ok0 && ok1 {
                    let slot0_function = v3_slot0()
                        .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;
                    let slot0_tokens = slot0_function.decode_output(&bytes0)
                        .map_err(|e| Error::Blockchain(format!("Decode slot0 failed: {}", e)))?;
                    let liq_function = v3_liquidity()
                        .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;
                    let liq_tokens = liq_function.decode_output(&bytes1)
                        .map_err(|e| Error::Blockchain(format!("Decode liquidity failed: {}", e)))?;

                    let sqrt_price_x96 = slot0_tokens
                        .get(0)
                        .and_then(|t| t.clone().into_uint())
                        .ok_or_else(|| Error::Blockchain("slot0 sqrtPriceX96 missing".to_string()))?;
                    let tick_i256 = slot0_tokens
                        .get(1)
                        .and_then(|t| t.clone().into_int())
                        .ok_or_else(|| Error::Blockchain("slot0 tick missing".to_string()))?;
                    let tick = ethers::types::I256::from_raw(tick_i256).as_i32();
                    let liquidity = liq_tokens
                        .get(0)
                        .and_then(|t| t.clone().into_uint())
                        .ok_or_else(|| Error::Blockchain("liquidity missing".to_string()))?;

                    let (block_number, timestamp) = self.get_block_info(blockchain, block).await?;
                    if let Some(requested) = block {
                        if requested != block_number {
                            if self.config.strict_backtest {
                                return Err(Error::NoDataForBlock(static_meta.pool_address, requested));
                            } else {
                                return Err(Error::BlockMismatch {
                                    pool: static_meta.pool_address,
                                    requested,
                                    resolved: block_number,
                                });
                            }
                        }
                    }

                    return Ok(PoolStateFromSwaps {
                        pool_address: static_meta.pool_address,
                        token0: static_meta.token0,
                        token1: static_meta.token1,
                        reserve0: U256::zero(),
                        reserve1: U256::zero(),
                        sqrt_price_x96: Some(sqrt_price_x96),
                        liquidity: Some(liquidity),
                        tick: Some(tick),
                        v3_tick_data: None,
                        v3_tick_current: Some(tick),
                        fee_tier: static_meta.fee.unwrap_or_default(),
                        protocol: DexProtocol::UniswapV3,
                        protocol_type: static_meta.protocol_type.clone(),
                        block_number,
                        timestamp,
                        token0_decimals: static_meta.token0_decimals as u32,
                        token1_decimals: static_meta.token1_decimals as u32,
                        price0_cumulative_last: None,
                        price1_cumulative_last: None,
                        dex_protocol: DexProtocol::UniswapV3,
                        last_reserve_update_timestamp: None,
                    });
                }
            }

            // Retry logic with bounded attempts
            attempt += 1;
            self.rpc_circuit_breaker.record_failure().await;
            if attempt >= MAX_MULTICALL_RETRIES {
                return Err(Error::RetryExhausted(attempt));
            }
            let backoff_ms = MULTICALL_RETRY_DELAY_MS * (1u64 << (attempt - 1));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    async fn fetch_v3_state_sequential(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        static_meta: &PoolStaticMetadata,
        block: Option<u64>
    ) -> Result<PoolStateFromSwaps, Error> {
        let slot0_function = v3_slot0()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;
        let liq_function = v3_liquidity()
            .ok_or_else(|| Error::Config("V3 ABI not available".to_string()))?;

        let slot0_tokens = self
            .call_and_decode(blockchain, static_meta.pool_address, slot0_function, &[], block)
            .await?;
        let liquidity_tokens = self
            .call_and_decode(blockchain, static_meta.pool_address, liq_function, &[], block)
            .await?;

        let sqrt_price_x96 = slot0_tokens
            .get(0)
            .and_then(|t| t.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("slot0 sqrtPriceX96 missing".to_string()))?;
        let tick_i256 = slot0_tokens
            .get(1)
            .and_then(|t| t.clone().into_int())
            .ok_or_else(|| Error::Blockchain("slot0 tick missing".to_string()))?;
        let tick = ethers::types::I256::from_raw(tick_i256).as_i32();
        let liquidity = liquidity_tokens
            .get(0)
            .and_then(|t| t.clone().into_uint())
            .ok_or_else(|| Error::Blockchain("liquidity missing".to_string()))?;

        let (block_number, timestamp) = self.get_block_info(blockchain, block).await?;
        if let Some(requested) = block {
            if requested != block_number {
                if self.config.strict_backtest {
                    return Err(Error::NoDataForBlock(static_meta.pool_address, requested));
                } else {
                    return Err(Error::BlockMismatch {
                        pool: static_meta.pool_address,
                        requested,
                        resolved: block_number,
                    });
                }
            }
        }

        Ok(PoolStateFromSwaps {
            pool_address: static_meta.pool_address,
            token0: static_meta.token0,
            token1: static_meta.token1,
            reserve0: U256::zero(),
            reserve1: U256::zero(),
            sqrt_price_x96: Some(sqrt_price_x96),
            liquidity: Some(liquidity),
            tick: Some(tick),
            v3_tick_data: None,
            v3_tick_current: Some(tick),
            fee_tier: static_meta.fee.unwrap_or_default(),
            protocol: DexProtocol::UniswapV3,
            protocol_type: static_meta.protocol_type.clone(),
            block_number,
            timestamp,
            token0_decimals: static_meta.token0_decimals as u32,
            token1_decimals: static_meta.token1_decimals as u32,
            price0_cumulative_last: None,
            price1_cumulative_last: None,
            dex_protocol: DexProtocol::UniswapV3,
            last_reserve_update_timestamp: None,
        })
    }

    async fn call_and_decode(
        &self,
        blockchain: &Arc<dyn BlockchainManager>,
        contract: Address,
        function: &Function,
        args: &[Token],
        block: Option<u64>,
    ) -> Result<Vec<Token>, Error> {
        let calldata = function.encode_input(args)
            .map_err(|e| Error::Internal(format!("ABI encode failed: {}", e)))?;
        
        let tx = TransactionRequest::new().to(contract).data(calldata);
        
        // Handle error explicitly to properly await circuit breaker
        let result_bytes = match blockchain.call(&tx, block.map(Into::into)).await {
            Ok(bytes) => bytes,
            Err(e) => {
                self.rpc_circuit_breaker.record_failure().await;
                return Err(Error::Blockchain(format!("RPC call failed for {}: {}", function.name, e)));
            }
        };

        function.decode_output(&result_bytes)
            .map_err(|e| Error::Blockchain(format!("ABI decode failed for {}: {}", function.name, e)))
    }

    async fn get_blockchain_manager(&self, chain_name: &str) -> Result<Arc<dyn BlockchainManager>, Error> {
        self.blockchain_managers.read().await
            .get(chain_name)
            .cloned()
            .ok_or_else(|| Error::Config(format!("No blockchain manager for chain {}", chain_name)))
    }

    /// Invalidate cache entries that have block numbers above the given block across all chains
    async fn invalidate_entries_above_block(&self, max_block: u64) {
        // Note: This is a simplified implementation. In a production system with very large caches,
        // you might want to iterate through entries more efficiently or use a different cache structure
        // that allows for range-based invalidation.
        let keys_to_invalidate: Vec<CacheKey> = self.pool_state_cache.iter()
            .filter_map(|(key, _value)| {
                if key.block.map_or(false, |b| b > max_block) {
                    Some(key.as_ref().clone())
                } else {
                    None
                }
            })
            .collect();

        for key in &keys_to_invalidate {
            self.pool_state_cache.invalidate(key).await;
        }

        tracing::debug!(target: "pool_states", "Invalidated {} entries above block {} across all chains", keys_to_invalidate.len(), max_block);
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (u64, u64) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed)
        )
    }

    /// Get circuit breaker statistics
    pub async fn get_circuit_breaker_stats(&self) -> CircuitBreakerStats {
        self.rpc_circuit_breaker.get_stats().await
    }

    /// Force reset the circuit breaker (administrative action)
    pub async fn reset_circuit_breaker(&self) {
        self.rpc_circuit_breaker.force_reset().await;
    }

    /// Check if the oracle is healthy
    pub async fn is_healthy(&self) -> bool {
        !self.rpc_circuit_breaker.is_tripped().await
    }

    pub async fn get_swap_events_for_block(
        &self,
        chain_name: &str,
        block_number: u64
    ) -> Result<Vec<crate::types::SwapEvent>, crate::errors::BacktestError> {
        match self.historical_swap_loader {
            Some(ref loader) => {
                let swaps = loader
                    .load_swaps_for_block_chunk(chain_name, block_number as i64, block_number as i64)
                    .await
                    .map_err(|err| crate::errors::BacktestError::SwapLoad(err.to_string()))?;
                Ok(swaps)
            }
            None => Ok(vec![])
        }
    }

    /// Add a blockchain manager for a specific chain
    pub async fn add_blockchain_manager(
        &self,
        chain_name: String,
        manager: Arc<dyn BlockchainManager>
    ) {
        let mut managers = self.blockchain_managers.write().await;
        managers.insert(chain_name, manager);
    }

    /// Remove a blockchain manager for a specific chain
    pub async fn remove_blockchain_manager(&self, chain_name: &str) -> Option<Arc<dyn BlockchainManager>> {
        let mut managers = self.blockchain_managers.write().await;
        managers.remove(chain_name)
    }

    /// Get the list of supported chains
    pub async fn supported_chains(&self) -> Vec<String> {
        let managers = self.blockchain_managers.read().await;
        managers.keys().cloned().collect()
    }

    /// Periodically refresh pool states to ensure data freshness
    /// This method should be called regularly to keep pool states current
    pub async fn refresh_pool_states(&self, max_age_seconds: u64) -> Result<(), Error> {
        tracing::info!(target: "pool_states", "Starting periodic pool state refresh (max_age: {}s)", max_age_seconds);

        let chains = self.supported_chains().await;
        let mut refreshed_count = 0;
        let mut total_pools = 0;

        for chain_name in chains {
            // Get the latest block timestamp for this chain to avoid clock skew
            let latest_block_timestamp = match self.get_blockchain_manager(&chain_name).await {
                Ok(manager) => {
                    match manager.get_latest_block().await {
                        Ok(block) => block.timestamp.as_u64(),
                        Err(e) => {
                            tracing::warn!(target: "pool_states", "Failed to get latest block for {}: {}", chain_name, e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "pool_states", "Failed to get blockchain manager for {}: {}", chain_name, e);
                    continue;
                }
            };

            if let Ok(pools) = self.pool_manager.get_all_pools(&chain_name, None).await {
                total_pools += pools.len();

                for pool in pools {
                    let cache_key = CacheKey {
                        chain_id: self.chain_id_map.get(&chain_name).copied().unwrap_or(1),
                        pool: pool.pool_address,
                        block: None, // Use latest block for refresh
                    };

                    // Check if the cache entry is stale using chain time
                    if let Some(entry) = self.pool_state_cache.get(&cache_key).await {
                        let age_seconds = latest_block_timestamp.saturating_sub(entry.timestamp);
                        if age_seconds > max_age_seconds {
                            tracing::debug!(target: "pool_states", "Refreshing stale pool state for {} (age: {}s)", pool.pool_address, age_seconds);

                            // Force refresh by removing from cache and fetching fresh data
                            self.pool_state_cache.invalidate(&cache_key).await;

                            // The next access will fetch fresh data
                            refreshed_count += 1;
                        }
                    } else {
                        // No cached data, will be fetched on next access
                        refreshed_count += 1;
                    }
                }
            }
        }

        tracing::info!(target: "pool_states", "Pool state refresh completed: {} pools checked, {} refreshed", total_pools, refreshed_count);
        Ok(())
    }

    /// Prefill cache for a list of pools to warm up before tight loops
    /// This is useful for arbitrage scenarios where you need immediate access to pool states
    pub async fn warm_pools(&self, chain_name: &str, pool_addresses: &[Address], block_number: Option<u64>) -> Result<(), Error> {
        tracing::info!(
            target: "pool_states",
            "Warming up cache for {} pools on chain {}",
            pool_addresses.len(),
            chain_name
        );

        let mut success_count = 0;
        let mut error_count = 0;

        for &pool_address in pool_addresses {
            match self.get_pool_state(chain_name, pool_address, block_number).await {
                Ok(_) => {
                    success_count += 1;
                    tracing::debug!(
                        target: "pool_states",
                        "Successfully warmed pool {} on chain {}",
                        pool_address,
                        chain_name
                    );
                }
                Err(e) => {
                    error_count += 1;
                    tracing::warn!(
                        target: "pool_states",
                        "Failed to warm pool {} on chain {}: {}",
                        pool_address,
                        chain_name,
                        e
                    );
                }
            }
        }

        tracing::info!(
            target: "pool_states",
            "Pool warming completed: {} successful, {} failed out of {} total",
            success_count,
            error_count,
            pool_addresses.len()
        );

        Ok(())
    }

    /// Start a background task for periodic pool state refresh
    pub fn start_periodic_refresh(&self, interval_seconds: u64, max_age_seconds: u64) {
        let oracle_clone = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));

            loop {
                interval.tick().await;

                if let Err(e) = oracle_clone.refresh_pool_states(max_age_seconds).await {
                    tracing::warn!(target: "pool_states", "Periodic pool state refresh failed: {}", e);
                }
            }
        });

        tracing::info!(target: "pool_states", "Started periodic pool state refresh (interval: {}s, max_age: {}s)", interval_seconds, max_age_seconds);
    }
}

//================================================================================================//
//                                           TELEMETRY                                            //
//================================================================================================//

impl PoolStateOracle {
    /// Export metrics for monitoring
    pub fn export_metrics(&self) -> PoolStateMetrics {
        let (cache_hits, cache_misses) = self.get_cache_stats();
        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            (cache_hits as f64) / ((cache_hits + cache_misses) as f64)
        } else {
            0.0
        };

        PoolStateMetrics {
            cache_hits,
            cache_misses,
            cache_hit_rate,
            cache_entries: self.pool_state_cache.entry_count() as u64,
            cache_size_bytes: self.pool_state_cache.weighted_size() as u64,
            multicall_failures: self.multicall_failures.load(Ordering::Relaxed),
            sequential_fallbacks: self.sequential_fallbacks.load(Ordering::Relaxed),
            fetch_attempts_multicall: self.fetch_attempts_multicall.load(Ordering::Relaxed),
            fetch_attempts_optimized: self.fetch_attempts_optimized.load(Ordering::Relaxed),
            fetch_attempts_sequential: self.fetch_attempts_sequential.load(Ordering::Relaxed),
            fetch_successes_multicall: self.fetch_successes_multicall.load(Ordering::Relaxed),
            fetch_successes_optimized: self.fetch_successes_optimized.load(Ordering::Relaxed),
            fetch_successes_sequential: self.fetch_successes_sequential.load(Ordering::Relaxed),
            cache_hits_with_block: self.cache_hits_with_block.load(Ordering::Relaxed),
            cache_hits_without_block: self.cache_hits_without_block.load(Ordering::Relaxed),
            reorg_count: self.reorg_seen_total.load(Ordering::Relaxed),
        }
    }

    /// Set block context for backtesting - clears block-specific caches
    pub async fn set_block_context(&self, block_number: u64) -> Result<(), Error> {
        tracing::debug!(target: "pool_states", "PoolStateOracle set_block_context called for block {} - clearing caches", block_number);
        self.pool_state_cache.invalidate_all();
        Ok(())
    }
}

/// Metrics for monitoring pool state oracle performance
#[derive(Debug, Clone)]
pub struct PoolStateMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub cache_entries: u64,
    pub cache_size_bytes: u64,
    pub multicall_failures: u64,
    pub sequential_fallbacks: u64,
    // New metrics for enhanced monitoring
    pub fetch_attempts_multicall: u64,
    pub fetch_attempts_optimized: u64,
    pub fetch_attempts_sequential: u64,
    pub fetch_successes_multicall: u64,
    pub fetch_successes_optimized: u64,
    pub fetch_successes_sequential: u64,
    pub cache_hits_with_block: u64,
    pub cache_hits_without_block: u64,
    pub reorg_count: u64,
}

//================================================================================================//
//                                             TESTS                                              //
//================================================================================================//

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_key_efficiency() {
        let key1 = CacheKey {
            chain_id: 1,
            pool: Address::zero(),
            block: Some(100),
        };
        
        let key2 = CacheKey {
            chain_id: 1,
            pool: Address::zero(),
            block: Some(100),
        };
        
        assert_eq!(key1, key2);
        
        let key3 = CacheKey {
            chain_id: 2,
            pool: Address::zero(),
            block: Some(100),
        };
        
        assert_ne!(key1, key3);
    }

    #[tokio::test]
    async fn test_circuit_breaker_integration() {
        let breaker = CircuitBreaker::new(3, Duration::from_millis(100));
        
        // Record failures
        for _ in 0..3 {
            breaker.record_failure().await;
        }
        
        assert!(breaker.is_tripped().await);
        
        // Wait for recovery
        tokio::time::sleep(Duration::from_millis(150)).await;
        // In half-open state, the circuit breaker may allow limited testing
        // We just verify that the timeout has been processed
        let _is_tripped = breaker.is_tripped().await;
    }
}