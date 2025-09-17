// src/path/finder.rs

use super::{
    graph::TokenGraph,
    search::SearchParameters,
    types::{Path, PathFinderMetrics, ValidationResult, GraphSnapshot, PathSearchNode, SearchStrategy, PathLeg},
};
use crate::{
    blockchain::BlockchainManager,
    config::{Config, PathFinderSettings},
    errors::PathError,
    gas_oracle::GasOracle,
    price_oracle::PriceOracle,
    pool_manager::PoolManagerTrait,
    types::{normalize_units, PoolInfo, SwapEvent, ProtocolType, ExecutionPriority},
    dex_math,
};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use ethers::types::{Address, U256};
use lru::LruCache;
use moka::future::Cache;
use petgraph::graph::UnGraph;
use smallvec::smallvec;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, Semaphore, Notify};
use tracing::{instrument, debug, info};
use std::collections::HashSet as AHashSet;
use std::time::Instant;

/// Maximum cache entries to prevent unbounded growth
const MAX_CACHE_ENTRIES: u64 = 10_000;
/// Cache time-to-live in seconds
const CACHE_TTL_SECONDS: u64 = 300;
// Legacy constants retained only for backward compatibility in defaults; limits are now config-driven.

/// Enhanced path finder trait with additional capabilities
#[async_trait]
pub trait PathFinder: Send + Sync + std::fmt::Debug {
    /// Preload required data for path finding
    async fn preload_data(&self) -> Result<(), PathError>;
    
    /// Validate a path for execution readiness
    async fn validate_path(&self, path: &Path, block_number: Option<u64>) -> Result<ValidationResult, PathError>;
    
    /// Calculate the output amount for a given path and input
    async fn calculate_path_output_amount(&self, path: &Path, amount_in: U256, block_number: Option<u64>) -> Result<U256, PathError>;
    
    /// Calculate price impact in basis points
    async fn calculate_path_price_impact(&self, path: &Path, amount_in: U256, block_number: Option<u64>) -> Result<u32, PathError>;
    
    /// Update the token graph with latest pool data
    async fn update_graph(&self) -> Result<(), PathError>;

    /// Update the token graph for a specific block (backtest-safe)
    async fn update_graph_for_block(&self, block_number: Option<u64>) -> Result<(), PathError>;
    
    /// Get current metrics
    async fn get_metrics(&self) -> Arc<PathFinderMetrics>;
    
    /// Find paths for a specific swap event
    async fn find_paths_for_swap_event(&self, event: &SwapEvent, block_number: Option<u64>) -> Result<Vec<Path>, PathError>;
    
    /// Find the optimal path between two tokens
    async fn find_optimal_path(&self, token_in: Address, token_out: Address, amount_in: U256, block_number: Option<u64>) -> Result<Option<Path>, PathError>;
    
    /// Build graph for a specific block
    async fn build_graph_for_block(&self, block_number: Option<u64>) -> Result<(), PathError>;
    async fn build_graph_with_pools(&self, pools: &[PoolInfo], block_number: u64) -> Result<(), PathError>;
    async fn build_graph_with_pools_and_tokens(&self, pools: &[PoolInfo], tokens: Vec<Address>, block_number: u64) -> Result<(), PathError>;
    async fn get_pool_states_for_block(&self, block_number: u64) -> Result<Vec<PoolInfo>, PathError>;
    async fn get_graph_snapshot(&self) -> GraphSnapshot;
    fn as_any(&self) -> &dyn std::any::Any;
    
    /// Find circular arbitrage paths
    async fn find_circular_arbitrage_paths(&self, token: Address, amount_in: U256, block_number: Option<u64>) -> Result<Vec<Path>, PathError>;
    
    /// Set the block context for historical queries
    async fn set_block_context(&self, block_number: u64) -> Result<(), PathError>;
    
    /// Add a pool to the graph
    async fn add_pool_to_graph(&self, pool_info: &PoolInfo) -> Result<(), PathError>;
}

/// High-performance A* path finder with advanced features
pub struct AStarPathFinder {
    pub(super) config: Arc<PathFinderSettings>,
    pub(super) full_config: Arc<Config>,
    pub(super) pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    pub(super) blockchain: Arc<dyn BlockchainManager>,
    pub(super) path_cache: Arc<Cache<String, Vec<Path>>>,
    pub(super) path_score_cache: Arc<Mutex<LruCache<String, f64>>>,
    pub(super) semaphore: Arc<Semaphore>,
    pub(super) weth_address: Address,
    pub(super) chain_id: u64,
    pub(super) gas_oracle: Arc<GasOracle>,
    pub(super) price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub(super) graph: Arc<Mutex<TokenGraph>>,
    pub(super) metrics: Arc<PathFinderMetrics>,
    pub(super) router_tokens: Arc<RwLock<Vec<Address>>>,
    pub(super) hot_paths: Arc<DashSet<(Address, Address)>>,
    pub(super) is_initialized: Arc<AtomicBool>,
    pub(super) strict_backtest: bool,
    pub(super) last_built_block: Arc<AtomicU64>,
    pub(super) token_decimals_cache: Arc<DashMap<Address, u8>>,
    pub(super) is_building_graph: Arc<AtomicBool>,
    pub(super) graph_build_notify: Arc<Notify>, // Add this
}

impl AStarPathFinder {
    /// Create a new A* path finder instance
    /// 
    /// # Arguments
    /// * `config` - Path finder specific configuration
    /// * `pool_manager` - Pool information manager
    /// * `blockchain` - Blockchain interaction manager
    /// * `full_config` - Full system configuration
    /// * `gas_oracle` - Gas price oracle
    /// * `price_oracle` - Token price oracle
    /// * `chain_id` - Blockchain chain ID
    /// * `strict_backtest` - Whether to enforce strict backtesting mode
    pub fn new(
        config: PathFinderSettings,
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        blockchain: Arc<dyn BlockchainManager>,
        full_config: Arc<Config>,
        gas_oracle: Arc<GasOracle>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        chain_id: u64,
        strict_backtest: bool,
        router_tokens: Vec<Address>,
    ) -> Result<Self, PathError> {
        let weth_address = blockchain.get_weth_address();
        
        // Create cache with TTL and size limits
        let path_cache = Arc::new(
            Cache::builder()
                .max_capacity(MAX_CACHE_ENTRIES)
                .time_to_live(Duration::from_secs(CACHE_TTL_SECONDS))
                .time_to_idle(Duration::from_secs(CACHE_TTL_SECONDS / 2))
                .build()
        );
        
        Ok(Self {
            config: Arc::new(config),
            full_config,
            pool_manager,
            blockchain,
            path_cache,
            path_score_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            semaphore: Arc::new(Semaphore::new(10)),
            weth_address,
            chain_id,
            gas_oracle,
            price_oracle,
            graph: Arc::new(Mutex::new(TokenGraph {
                graph: Arc::new(RwLock::new(UnGraph::new_undirected())),
                token_to_node: Arc::new(DashMap::new()),
                pool_to_edge: Arc::new(DashMap::new()),
            })),
            metrics: Arc::new(PathFinderMetrics::default()),
            router_tokens: Arc::new(RwLock::new(router_tokens)),
            hot_paths: Arc::new(DashSet::new()),
            is_initialized: Arc::new(AtomicBool::new(false)),
            strict_backtest,
            last_built_block: Arc::new(AtomicU64::new(0)),
            token_decimals_cache: Arc::new(DashMap::new()),
            is_building_graph: Arc::new(AtomicBool::new(false)),
            graph_build_notify: Arc::new(Notify::new()), // And initialize it here
        })
    }

    /// Set the block context for this path finder
    /// 
    /// This updates the blockchain manager's context and invalidates
    /// all cached paths to ensure consistency
    pub(super) async fn set_block_context_internal(&self, block_number: u64) -> Result<(), PathError> {
        if block_number == 0 {
            return Err(PathError::InvalidInput("Block number must be greater than 0".to_string()));
        }
        
        self.blockchain.set_block_context(block_number).await
            .map_err(|e| PathError::Unexpected(format!("Failed to set block context on blockchain manager: {}", e)))?;
        
        // Invalidate caches when context changes
        self.path_cache.invalidate_all();
        self.path_score_cache.lock().await.clear();
        
        Ok(())
    }

        /// Get token decimals with caching
    ///
    /// Retrieves and caches token decimal places for amount normalization
    pub(super) async fn get_token_decimals(&self, token: Address, block_number: Option<u64>) -> Result<u8, PathError> {
        if let Some(decimals) = self.token_decimals_cache.get(&token) {
            return Ok(*decimals);
        }

        let chain = self.blockchain.get_chain_name();

        // First, try to get decimals from pool metadata
        let pools = if block_number.is_some() {
            self.pool_manager.get_pools_active_at_block(&chain, block_number.unwrap()).await
        } else {
            self.pool_manager.get_all_pools(&chain, None).await
        };

        let decimals = match pools {
            Ok(pools_meta) => {
                let mut inferred: Option<u8> = None;
                for meta in pools_meta {
                    if meta.token0 == token {
                        inferred = Some(meta.token0_decimals);
                        break;
                    }
                    if meta.token1 == token {
                        inferred = Some(meta.token1_decimals);
                        break;
                    }
                }
                if let Some(decimals) = inferred {
                    decimals
                } else {
                    // Fallback to blockchain call if not found in pools
                    let block_id = block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into())));
                    self.blockchain.get_token_decimals(token, block_id).await
                        .map_err(|e| PathError::Unexpected(format!("Failed to fetch token decimals: {}", e)))?
                }
            }
            Err(_) => {
                // Fallback to blockchain call if pool loading fails
                let block_id = block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into())));
                self.blockchain.get_token_decimals(token, block_id).await
                    .map_err(|e| PathError::Unexpected(format!("Failed to fetch token decimals: {}", e)))?
            }
        };

        self.token_decimals_cache.insert(token, decimals);
        Ok(decimals)
    }

    /// Convert token amount to USD value using price oracle
    /// 
    /// # Arguments
    /// * `token` - Token address
    /// * `amount` - Amount in token's smallest units
    /// * `block_number` - Optional block number for historical prices
    pub(super) async fn amount_usd(
        &self,
        token: Address,
        amount: U256,
        block_number: Option<u64>,
    ) -> Result<f64, PathError> {
        let chain = self.blockchain.get_chain_name();
        if self.strict_backtest && block_number.is_none() {
            return Err(PathError::MarketData("Historical price required in strict backtest".to_string()));
        }
        let px = match block_number {
            Some(b) => self.price_oracle
                .get_token_price_usd_at(&chain, token, Some(b), None).await,
            None => self.price_oracle.get_token_price_usd(&chain, token).await,
        };

        // Handle missing price data gracefully by returning 0.0 USD value
        let px = match px {
            Ok(price) => price,
            Err(_) => {
                tracing::debug!("No price data available for token {:?} at block {:?}, using 0.0 USD value", token, block_number);
                0.0
            }
        };
        
        // Validate price
        if !px.is_finite() || px < 0.0 {
            return Err(PathError::MarketData(format!("Invalid price {} for token {:?}", px, token)));
        }
        
        let decimals = self.get_token_decimals(token, block_number).await?;
        let amount_f = normalize_units(amount, decimals);
        
        let usd_value = amount_f * px;
        if !usd_value.is_finite() {
            return Err(PathError::Calculation(format!("USD value calculation resulted in non-finite value")));
        }
        
        Ok(usd_value)
    }

    /// Calculate hop output using only on-chain pool state for consistency
    /// 
    /// # Arguments
    /// * `pool_address` - Address of the pool
    /// * `token_in` - Input token address
    /// * `amount_in` - Input amount
    /// * `block_number` - Optional block number for historical state
    #[instrument(skip(self), fields(pool = %pool_address, token_in = %token_in, amount_in = %amount_in), level = "debug")]
    pub(super) async fn calculate_hop_output(
        &self,
        pool_address: Address,
        token_in: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<(U256, Address, u32, ProtocolType), PathError> {
        // Input validation
        if amount_in.is_zero() {
            return Err(PathError::InvalidInput("Zero input amount".to_string()));
        }
        
        let chain_name = self.blockchain.get_chain_name();
        if self.strict_backtest && block_number.is_none() {
            return Err(PathError::GraphUpdateFailed("Block number required in strict backtest for pool state".to_string()));
        }
        
        let pool_info = self.pool_manager.get_pool_info(&chain_name, pool_address, block_number).await?;
        
        // Skip pools with zero reserves - return a specific error that can be handled upstream
        if pool_info.reserves0.is_zero() || pool_info.reserves1.is_zero() {
            log::debug!("Skipping pool {} with zero reserves (reserves0={}, reserves1={})",
                       pool_address, pool_info.reserves0, pool_info.reserves1);
            return Err(PathError::InvalidInput(format!("Pool {} has zero reserves", pool_address)));
        }
        
        let token_out = if pool_info.token0 == token_in {
            pool_info.token1
        } else if pool_info.token1 == token_in {
            pool_info.token0
        } else {
            return Err(PathError::InvalidInput(format!("Token {:?} not in pool {:?}", token_in, pool_address)));
        };

        let amount_out = dex_math::get_amount_out(&pool_info, token_in, amount_in)?;

        if amount_out.is_zero() {
            return Err(PathError::Calculation(format!("Zero output calculated for pool {}", pool_address)));
        }
        
        Ok((amount_out, token_out, pool_info.fee_bps, pool_info.protocol_type))
    }
    
    /// Calculate the cost of a path hop for A* search
    /// 
    /// Cost calculation incorporates gas costs, liquidity depth, fees, and strategy preferences
    pub(super) async fn calculate_cost(
        &self,
        current: &PathSearchNode,
        output_token: Address,
        output_amount: U256,
        protocol: &ProtocolType,
        gas_price: U256,
        eth_usd_price: f64,
        block_number: Option<u64>,
        strategy: &SearchStrategy,
        liquidity_usd: f64,
        fee_bps: u32,
    ) -> Result<f64, PathError> {
        let in_usd = self.amount_usd(current.token, current.amount, block_number).await?;
        let out_usd = self.amount_usd(output_token, output_amount, block_number).await?;
        
        // Use gas oracle provider's protocol-specific estimate
        let hop_gas_estimate = self.gas_oracle.inner().estimate_protocol_gas(protocol);
        let gas_cost_wei = gas_price.saturating_mul(U256::from(hop_gas_estimate));
        let gas_cost_eth = normalize_units(gas_cost_wei, 18);
        let gas_cost_usd = gas_cost_eth * eth_usd_price;
        
        // Safe arithmetic with validation
        let net_profit_usd = out_usd - in_usd - gas_cost_usd;
        if !net_profit_usd.is_finite() {
            return Err(PathError::Calculation("Profit calculation resulted in non-finite value".to_string()));
        }
        
        // The cost should be inverse to profit (lower cost is better).
        // A positive cost indicates a loss, while a negative cost indicates a profit.
        let base_cost = -net_profit_usd;

        let cost = match strategy {
            SearchStrategy::Balanced => base_cost,
            SearchStrategy::HighLiquidity => {
                const MAX_LIQUIDITY_PENALTY: f64 = 100.0;
                let liquidity_penalty = (1_000_000.0 / (liquidity_usd + 1.0)).min(MAX_LIQUIDITY_PENALTY);
                base_cost + liquidity_penalty
            }
            SearchStrategy::LowFee => {
                const FEE_SCALING_FACTOR: f64 = 100.0;
                let fee_penalty = (fee_bps as f64 / 10000.0) * FEE_SCALING_FACTOR;
                base_cost + fee_penalty
            }
            SearchStrategy::ShortPath => 1.0 + gas_cost_usd,
        };
        
        // Validate result
        if !cost.is_finite() {
            return Err(PathError::Calculation(format!("Cost calculation resulted in non-finite value: in_usd={}, out_usd={}, gas_usd={}", in_usd, out_usd, gas_cost_usd)));
        }
        
        Ok(cost)
    }
    
    /// Construct a Path from a search node
    pub(super) async fn construct_path_from_node_with_block(
        &self,
        node: &PathSearchNode,
        block_number: Option<u64>,
    ) -> Result<Path, PathError> {
        let tokens = node.tokens.clone();
        let pools = node.pools.clone();
        let fees = node.fees.clone();
        let protocols = node.protocols.clone();
        
        if tokens.is_empty() || pools.len() != tokens.len() - 1 {
            return Err(PathError::InvalidInput("Invalid path structure".to_string()));
        }
        
        let gas_estimate = self.estimate_path_gas(&protocols);
        let gas_cost_native = self.estimate_gas_cost_native(U256::from(gas_estimate), block_number).await?;
        
        let in_usd = self.amount_usd(tokens.first().copied().unwrap_or_default(), node.initial_amount_in_wei, block_number).await?;
        let out_usd = self.amount_usd(tokens.last().copied().unwrap_or_default(), node.amount, block_number).await?;
        
        let eth_usd = self.price_oracle.get_token_price_usd_at(&self.blockchain.get_chain_name(), self.weth_address, block_number, None).await
            .map_err(|e| PathError::MarketData(format!("Failed to get eth price: {e}")))?;

        let gas_cost_usd = gas_cost_native * eth_usd;
        
        let profit_estimate_usd = out_usd - in_usd - gas_cost_usd;
        if !profit_estimate_usd.is_finite() {
            return Err(PathError::Calculation("Profit calculation resulted in non-finite value".to_string()));
        }
            
        let profit_estimate_native = if eth_usd > 0.0 { profit_estimate_usd / eth_usd } else { 0.0 };
        
        // Try to get gas price with block number. In strict backtest, never fall back.
        let gas_price = match self.gas_oracle.get_gas_price(&self.blockchain.get_chain_name(), block_number).await {
            Ok(price) => price,
            Err(e) => {
                if self.strict_backtest {
                    return Err(PathError::GasEstimation(format!("Historical gas price required in strict backtest: {}", e)));
                }
                self.gas_oracle
                    .get_gas_price(&self.blockchain.get_chain_name(), None)
                    .await
                    .map_err(|e2| PathError::GasEstimation(e2.to_string()))?
            }
        };
        
        let estimated_gas_cost_wei = U256::from(gas_estimate).saturating_mul(gas_price.effective_price());

        let mut legs_vec = Vec::with_capacity(pools.len());
        for i in 0..pools.len() {
            if i + 1 < tokens.len() {
                legs_vec.push(PathLeg {
                    from_token: tokens[i],
                    to_token: tokens[i + 1],
                    pool_address: pools[i],
                    protocol_type: protocols.get(i).cloned().unwrap_or(ProtocolType::UniswapV2),
                    fee: fees.get(i).copied().unwrap_or(3000),
                    amount_in: if i == 0 { node.initial_amount_in_wei } else { U256::zero() },
                    amount_out: if i == pools.len() - 1 { node.amount } else { U256::zero() },
                    requires_approval: false,
                    wrap_native: false,
                    unwrap_native: false,
                });
            }
        }

        let mut path = Path {
            token_path: tokens.clone(),
            pool_path: pools,
            fee_path: fees,
            protocol_path: protocols,
            initial_amount_in_wei: node.initial_amount_in_wei,
            expected_output: node.amount,
            min_output: U256::zero(),
            price_impact_bps: 0,
            gas_estimate,
            gas_cost_native,
            profit_estimate_native,
            profit_estimate_usd,
            execution_priority: ExecutionPriority::Default,
            tags: smallvec![],
            source: Some("a_star".to_string()),
            discovery_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            confidence_score: 0.0,
            mev_resistance_score: 0.0,
            token_in: tokens.first().copied().unwrap_or_default(),
            token_out: tokens.last().copied().unwrap_or_default(),
            legs: legs_vec,
            estimated_gas_cost_wei,
        };
        
        path.calculate_min_output(self.config.max_slippage_bps);
        // Reject if price impact cannot be computed
        match self.calculate_path_price_impact(&path, path.initial_amount_in_wei, block_number).await {
            Ok(impact) => { path.price_impact_bps = impact; }
            Err(e) => {
                return Err(PathError::Calculation(format!("Failed to compute price impact: {}", e)));
            }
        }
        let chain_name = self.blockchain.get_chain_name();
        path.calculate_confidence_score(&self.pool_manager, &chain_name, block_number).await;
        path.calculate_mev_resistance();

        Ok(path)
    }

    /// Estimate gas cost in native currency using gas oracle
    pub(super) async fn estimate_gas_cost_native(&self, gas_units: U256, block_number: Option<u64>) -> Result<f64, PathError> {
        if gas_units > U256::from(10_000_000u64) {
            return Err(PathError::InvalidInput(format!("Gas units {} exceeds reasonable limit", gas_units)));
        }

        let chain_name = self.blockchain.get_chain_name();

        // Try to get gas price with block number. In strict backtest, never fall back.
        if self.strict_backtest && block_number.is_none() {
            return Err(PathError::GasEstimation("Block number required in strict backtest".to_string()));
        }
        let gas_price = match self.gas_oracle.get_gas_price(&chain_name, block_number).await {
            Ok(price) => price,
            Err(e) => {
                if self.strict_backtest {
                    return Err(PathError::GasEstimation(format!("Historical gas price required in strict backtest: {}", e)));
                }
                self.gas_oracle
                    .get_gas_price(&chain_name, None)
                    .await
                    .map_err(|e2| PathError::GasEstimation(e2.to_string()))?
            }
        };

        let gas_cost_wei = gas_units.saturating_mul(gas_price.effective_price());
        let gas_cost_eth = normalize_units(gas_cost_wei, 18);
        if !gas_cost_eth.is_finite() || gas_cost_eth < 0.0 {
            return Err(PathError::Calculation(format!("Invalid gas cost calculation: {}", gas_cost_eth)));
        }
        Ok(gas_cost_eth)
    }
    
    /// Get search parameters based on current configuration
    pub(super) async fn get_search_parameters(&self) -> SearchParameters {
        SearchParameters {
            max_routes: self.config.max_routes_to_consider,
            is_backtesting: self.strict_backtest,
        }
    }

    /// Get current gas and ETH prices with validation
    pub(super) async fn get_gas_and_eth_prices(&self, is_backtesting: bool, block_number: Option<u64>, unix_ts: Option<u64>) -> Result<(U256, f64), PathError> {
        let chain_name = self.blockchain.get_chain_name();
        
        if is_backtesting {
            let block = block_number.ok_or_else(|| PathError::InvalidInput("Block number required for backtesting".to_string()))?;
            let gas_price = self.gas_oracle.get_gas_price_with_block(&chain_name, Some(block)).await
                .map_err(|e| PathError::GasEstimation(e.to_string()))?;
            let eth_price = self.price_oracle.get_token_price_usd_at(&chain_name, self.weth_address, Some(block), unix_ts).await
                .map_err(|e| PathError::MarketData(e.to_string()))?;
            
            if !eth_price.is_finite() || eth_price <= 0.0 {
                return Err(PathError::MarketData(format!("Invalid ETH price: {}", eth_price)));
            }
            
            return Ok((gas_price.effective_price(), eth_price));
        }

        let gas_price = self.gas_oracle.get_gas_price(&chain_name, None).await
            .map_err(|e| PathError::GasEstimation(e.to_string()))?;
        let eth_price = self.blockchain.get_eth_price_usd().await
            .map_err(|e| PathError::MarketData(e.to_string()))?;
        
        if !eth_price.is_finite() || eth_price <= 0.0 {
            return Err(PathError::MarketData(format!("Invalid ETH price: {}", eth_price)));
        }
        
        Ok((gas_price.effective_price(), eth_price))
    }
    
    /// Estimate path gas with detailed debugging
    pub(super) fn estimate_path_gas(&self, protocols: &[ProtocolType]) -> u64 {
        const BASE_TX_COST: u64 = 21_000;
        const EIP1559_OVERHEAD: u64 = 5_000;
        const MAX_PATH_GAS: u64 = 2_000_000;
        
        let mut total_gas = BASE_TX_COST.saturating_add(EIP1559_OVERHEAD);

        for p in protocols {
            total_gas = total_gas.saturating_add(self.estimate_hop_gas(p));
            if total_gas > MAX_PATH_GAS {
                return MAX_PATH_GAS;
            }
        }

        total_gas
    }

    /// Estimate hop gas based on protocol type
    pub(super) fn estimate_hop_gas(&self, protocol: &ProtocolType) -> u64 {
        let estimates = &self.full_config.gas_estimates;
        match protocol {
            ProtocolType::UniswapV2 => estimates.uniswap_v2,
            ProtocolType::UniswapV3 => estimates.uniswap_v3,
            ProtocolType::UniswapV4 => estimates.uniswap_v4,
            ProtocolType::SushiSwap => estimates.sushiswap,
            ProtocolType::Curve => estimates.curve,
            ProtocolType::Balancer => estimates.balancer,
            _ => estimates.other,
        }
    }
    
    /// Get cached price ratio with fallback calculation
    pub(super) async fn get_price_ratio_cached(&self, token_a: Address, token_b: Address, block_number: Option<u64>) -> Result<f64, PathError> {
        let cache_key = format!("{:?}-{:?}-{:?}", token_a, token_b, block_number);
        
        {
            let mut cache = self.path_score_cache.lock().await;
            if let Some(price) = cache.get(&cache_key) {
                self.metrics.cache_hits.fetch_add(1, AtomicOrdering::Relaxed);
                return Ok(*price);
            }
        }
        
        self.metrics.cache_misses.fetch_add(1, AtomicOrdering::Relaxed);
        
        let chain_name = self.blockchain.get_chain_name();
        
        let price_a = self.price_oracle.get_token_price_usd_at(&chain_name, token_a, block_number, None).await;
        let price_b = self.price_oracle.get_token_price_usd_at(&chain_name, token_b, block_number, None).await;

        match (price_a, price_b) {
            (Ok(pa), Ok(pb)) if pa > 0.0 && pb > 0.0 && pa.is_finite() && pb.is_finite() => {
                let ratio = pa / pb;
                if ratio.is_finite() && ratio > 0.0 {
                    let mut cache = self.path_score_cache.lock().await;
                    cache.put(cache_key, ratio);
                    Ok(ratio)
                } else {
                    Err(PathError::MarketData(format!("Invalid price ratio: {}", ratio)))
                }
            },
            _ => Err(PathError::MarketData("Could not fetch valid prices for ratio".to_string()))
        }
    }

    #[instrument(skip_all, fields(block_number = ?block_number), level = "debug")]
    pub async fn build_graph_internal(&self, block_number: Option<u64>) -> Result<(), PathError> {
        debug!(target: "path_finder", "Starting full graph rebuild for block {:?}", block_number);
        let start_time = Instant::now();
        let chain_name = self.blockchain.get_chain_name();
        info!(target: "path_finder", "Building graph for chain: '{}'", chain_name);

        let block_num = match block_number {
            Some(bn) => bn,
            None => self.blockchain.get_block_number().await.unwrap_or(0),
        };

        let pools = {
            debug!(target: "path_finder", "Using time-aware pool retrieval for block {:?}", block_num);
            self.get_pool_states_for_block(block_num).await?
        };
        info!(target: "path_finder", "Retrieved {} pools for chain '{}' for graph rebuild", pools.len(), chain_name);
        
        let swap_tokens: AHashSet<Address> = pools.iter().flat_map(|p| vec![p.token0, p.token1]).collect();
        let router_tokens_vec = self.router_tokens.read().await.clone();
        let router_set: AHashSet<Address> = router_tokens_vec.into_iter().collect();
        let all_tokens: Vec<Address> = swap_tokens.union(&router_set).cloned().collect();

        self.build_graph_with_pools_and_tokens(&pools, all_tokens, block_num).await?;

        self.last_built_block.store(block_num, AtomicOrdering::Relaxed);
        let elapsed = start_time.elapsed();
        self.metrics.graph_build_time_ms.fetch_add(elapsed.as_millis() as u64, AtomicOrdering::Relaxed);
        debug!(target: "path_finder", "Graph rebuild completed in {:?}", elapsed);

        Ok(())
    }
}