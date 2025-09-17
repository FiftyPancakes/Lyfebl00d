//! # Cross-Chain Pathfinding Engine
//!
//! This module is responsible for discovering profitable cross-chain arbitrage opportunities.
//! It uses `BridgeAdapter` implementations to get quotes from various bridge protocols and
//! combines this with on-chain swap data to construct potential arbitrage routes.
//! The `CrossChainPathfinder` does not execute trades; it only identifies and evaluates
//! potential `CrossChainExecutionPlan`s, which are then passed to the `ArbitrageEngine`
//! for execution.

use crate::{
    blockchain::BlockchainManager,
    bridges::adapter::BridgeAdapter,
    config::{ChainConfig, CrossChainSettings},
    errors::CrossChainError,
    gas_oracle::GasOracleProvider,
    price_oracle::PriceOracle,
    synergy_scorer::{SynergyScorer},
    types::{
        CrossChainExecutionPlan, SwapRoute, SwapEvent, normalize_units, PoolInfo,
    },
    path::{PathFinder, Path},
};
use std::collections::HashSet;
use ethers::types::{Address, U256};
use eyre::Result;
use futures::{future::join_all, stream::{FuturesUnordered, StreamExt}};
use moka::future::Cache;
use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing::{debug, info, instrument, warn};

const ROUTE_CACHE_TTL_SECS: u64 = 60;
const ROUTE_CACHE_CAPACITY: u64 = 1000;
const MAX_CONCURRENT_ROUTE_QUERIES: usize = 20;
const MAX_CONCURRENT_PATHFINDING_TASKS: usize = 8;

/// Manages the discovery and evaluation of cross-chain arbitrage routes.
///
/// It queries various registered `BridgeAdapter`s to find the best quotes for
/// transferring assets between chains and combines them with potential swaps on
/// the source and destination chains to create a complete execution plan.
pub struct CrossChainPathfinder {
    /// Configuration for all supported blockchain networks.
    pub chain_configs: Arc<HashMap<u64, ChainConfig>>,
    /// Specific settings for cross-chain arbitrage.
    pub config: Arc<CrossChainSettings>,
    /// Managers for interacting with each blockchain.
    pub blockchain_managers: Arc<HashMap<u64, Arc<dyn BlockchainManager>>>,
    /// A collection of adapters for different bridge protocols.
    pub bridge_adapters: Arc<HashMap<String, Arc<dyn BridgeAdapter>>>,
    /// Provides token price information.
    pub price_oracle: Arc<dyn PriceOracle>,
    /// Provides gas cost estimates.
    pub gas_oracle: Arc<dyn GasOracleProvider>,
    /// Scores the synergy between two chains for arbitrage.
    pub synergy_scorer: Arc<dyn SynergyScorer>,
    /// Caches profitable routes to avoid redundant computation.
    pub route_cache: Cache<String, CrossChainExecutionPlan>,
    /// Limits concurrent queries to external bridge APIs.
    pub query_semaphore: Arc<Semaphore>,
    /// Limits concurrent pathfinding tasks.
    pub pathfinding_semaphore: Arc<Semaphore>,
    /// Per-chain pathfinders for swap discovery.
    pub path_finders: Arc<HashMap<u64, Arc<dyn PathFinder>>>,
}

impl CrossChainPathfinder {
    /// Creates a new `CrossChainPathfinder`.
    pub fn new(
        chain_configs: Arc<HashMap<u64, ChainConfig>>,
        config: Arc<CrossChainSettings>,
        blockchain_managers: Arc<HashMap<u64, Arc<dyn BlockchainManager>>>,
        bridge_adapters: Arc<HashMap<String, Arc<dyn BridgeAdapter>>>,
        price_oracle: Arc<dyn PriceOracle>,
        gas_oracle: Arc<dyn GasOracleProvider>,
        synergy_scorer: Arc<dyn SynergyScorer>,
        path_finders: Arc<HashMap<u64, Arc<dyn PathFinder>>>,
    ) -> Self {
        Self {
            chain_configs,
            config,
            blockchain_managers,
            bridge_adapters,
            price_oracle,
            gas_oracle,
            synergy_scorer,
            path_finders,
            route_cache: Cache::builder()
                .time_to_live(Duration::from_secs(ROUTE_CACHE_TTL_SECS))
                .max_capacity(ROUTE_CACHE_CAPACITY)
                .build(),
            query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_ROUTE_QUERIES)),
            pathfinding_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_PATHFINDING_TASKS)),
        }
    }

    /// The main entry point for finding cross-chain arbitrage opportunities.
    ///
    /// This function orchestrates the process of generating potential swaps,
    /// querying bridges for quotes, and evaluating the profitability of each
    /// potential cross-chain route.
    ///
    /// # Arguments
    /// * `token_in` - The starting token address on the source chain.
    /// * `amount_in` - The amount of the starting token.
    /// * `block_number` - Optional block number for historical context (required for backtesting).
    ///
    /// # Returns
    /// A `Result` containing a vector of profitable `CrossChainExecutionPlan`s.
    #[instrument(skip(self), fields(token_in = %token_in, amount_in = %amount_in))]
    pub async fn find_routes(
        &self,
        token_in: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<Vec<CrossChainExecutionPlan>, CrossChainError> {
        // Check if cross-chain is enabled early
        if !self.config.cross_chain_enabled {
            debug!("Cross-chain arbitrage is disabled in configuration");
            return Ok(vec![]);
        }

        info!("Starting cross-chain route discovery.");
        let mut profitable_plans = Vec::new();
        let mut seen_fingerprints: HashSet<String> = HashSet::new();

        // 1. Iterate over all configured source chains.
        for (source_chain_id, source_chain_config) in self.chain_configs.iter() {

            // Validate chain configuration before proceeding
            if !self.validate_chain_config(source_chain_id, source_chain_config).await? {
                warn!("Skipping chain {} due to configuration validation failure", source_chain_id);
                continue;
            }

            // 2. Generate potential source chain swaps.
            let source_swaps = self.generate_source_swaps(
                *source_chain_id,
                token_in,
                amount_in,
                block_number,
            ).await?;

            // 3. For each potential source swap, find bridgeable routes.
            for source_swap in source_swaps {
                let bridge_token = *source_swap.path.last().unwrap_or(&token_in);
                let amount_to_bridge = *source_swap.amounts.last().unwrap_or(&amount_in);

                let bridge_tasks = self.bridge_adapters.values().map(|adapter| {
                    self.find_bridge_routes_for_token(
                        *source_chain_id,
                        bridge_token,
                        amount_to_bridge,
                        adapter.clone(),
                        source_swap.clone(),
                        block_number,
                    )
                });

                let results = join_all(bridge_tasks).await;
                for result in results {
                    match result {
                        Ok(mut plans) => profitable_plans.append(&mut plans),
                        Err(e) => warn!("Error finding bridge routes: {}", e),
                    }
                }
            }
        }

        // Deduplicate plans based on fingerprint
        let mut unique_plans = Vec::new();
        for plan in profitable_plans {
            if seen_fingerprints.insert(plan.fingerprint.clone()) {
                unique_plans.push(plan);
            } else {
                debug!("Skipping duplicate plan with fingerprint: {}", plan.fingerprint);
            }
        }

        // Sort plans by net profit (desc), then synergy score (desc) as tiebreaker
        unique_plans.sort_by(|a, b| {
            b.net_profit_usd
                .partial_cmp(&a.net_profit_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.synergy_score.partial_cmp(&a.synergy_score).unwrap_or(std::cmp::Ordering::Equal))
        });

        info!("Found {} unique profitable cross-chain plans.", unique_plans.len());
        Ok(unique_plans)
    }

    /// Validates chain configuration for cross-chain operations
    async fn validate_chain_config(
        &self,
        chain_id: &u64,
        chain_config: &ChainConfig,
    ) -> Result<bool, CrossChainError> {
        // Check if blockchain manager exists for this chain
        if !self.blockchain_managers.contains_key(chain_id) {
            warn!("No blockchain manager found for chain {}", chain_id);
            return Ok(false);
        }

        // Check if pathfinder exists for this chain
        if !self.path_finders.contains_key(chain_id) {
            warn!("No pathfinder found for chain {}", chain_id);
            return Ok(false);
        }

        // Validate chain configuration has required fields
        for (_chain_name, per_chain_config) in &chain_config.chains {
            if per_chain_config.chain_id != *chain_id {
                continue;
            }

            // Check if chain has bridgeable tokens configured
            let has_stablecoins = per_chain_config.reference_stablecoins
                .as_ref()
                .map_or(false, |v| !v.is_empty());
            if !has_stablecoins && per_chain_config.weth_address == Address::zero() {
                warn!("Chain {} has no bridgeable tokens configured", chain_id);
                return Ok(false);
            }

            // Check if chain is enabled for live operations
            if !per_chain_config.live_enabled {
                warn!("Chain {} is not enabled for live operations", chain_id);
                return Ok(false);
            }

            debug!("Chain {} configuration validated successfully", chain_id);
            return Ok(true);
        }

        warn!("No matching chain configuration found for chain {}", chain_id);
        Ok(false)
    }

    /// Finds and evaluates routes across a specific bridge protocol for a given token.
    async fn find_bridge_routes_for_token(
        &self,
        source_chain_id: u64,
        bridge_token: Address,
        amount_to_bridge: U256,
        adapter: Arc<dyn BridgeAdapter>,
        source_swap: SwapRoute,
        block_number: Option<u64>,
    ) -> Result<Vec<CrossChainExecutionPlan>, CrossChainError> {
        let mut plans = Vec::new();
        // Iterate over all potential destination chains.
        for (dest_chain_id, _) in self.chain_configs.iter().filter(|(id, _)| **id != source_chain_id) {
            // Include time bucket and block number for cache key to avoid stale quotes
            let time_bucket = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() / 300; // 5-minute buckets
            let blk = block_number.unwrap_or(0);
            let cache_key = format!(
                "{}-{}-{}-{}-{}-{}-{}",
                source_chain_id, dest_chain_id, bridge_token, amount_to_bridge, adapter.name(), time_bucket, blk
            );

            // Check cache first without acquiring semaphore for fast hits
            if let Some(plan) = self.route_cache.get(&cache_key).await {
                plans.push(plan);
                continue;
            }

            // Acquire semaphore only for expensive operations
            let _permit = self.query_semaphore.acquire().await.unwrap();

            // Try different destination tokens supported by the bridge
            let dest_tokens = self.get_possible_dest_tokens(*dest_chain_id, bridge_token);
            let mut quote_opt = None;

            for dest_token in dest_tokens {
                match adapter.fetch_quote(
                    source_chain_id,
                    *dest_chain_id,
                    bridge_token,
                    dest_token,
                    amount_to_bridge,
                ).await {
                    Ok(quote) => {
                        quote_opt = Some(quote);
                        break; // Use first successful quote
                    }
                    Err(e) => {
                        debug!("Bridge {} failed for token {} → {}: {}", adapter.name(), bridge_token, dest_token, e);
                        continue;
                    }
                }
            }

            if let Some(quote) = quote_opt {
                // Generate potential destination swaps.
                let dest_swaps = self.generate_dest_swaps(
                    *dest_chain_id,
                    bridge_token,
                    quote.amount_out,
                    block_number,
                ).await?;

                // Score the synergy before evaluating profitability
                // Get chain names from gas oracle for the synergy scorer
                let source_chain_name = self.gas_oracle.get_chain_name_from_id(source_chain_id)
                    .ok_or_else(|| CrossChainError::ChainNotFound(source_chain_id))?;
                let dest_chain_name = self.gas_oracle.get_chain_name_from_id(*dest_chain_id)
                    .ok_or_else(|| CrossChainError::ChainNotFound(*dest_chain_id))?;
                
                let synergy_score = self
                    .synergy_scorer
                    .score(
                        &source_chain_name,
                        &dest_chain_name,
                        bridge_token,
                    )
                    .await?;

                // Only proceed if synergy score is above the configured threshold
                if synergy_score < self.config.min_synergy_score {
                    debug!("Skipping route due to low synergy score: {:.4} < {:.4}", synergy_score, self.config.min_synergy_score);
                    continue;
                }

                // Evaluate profitability for each complete route.
                for dest_swap in dest_swaps {
                    let mut plan = CrossChainExecutionPlan {
                        source_swap: Some(source_swap.clone()),
                        bridge_quote: quote.clone(),
                        dest_swap: Some(dest_swap),
                        synergy_score: synergy_score, // <-- Store the score
                        net_profit_usd: 0.0,
                        fingerprint: String::new(),
                    };

                    if self.is_plan_profitable(&mut plan, block_number).await? {
                        self.route_cache.insert(cache_key.clone(), plan.clone()).await;
                        plans.push(plan);
                    } else {
                        debug!("Plan rejected due to insufficient profit: net=${:.4}, threshold=${:.4}, fingerprint={}",
                               plan.net_profit_usd, self.config.min_profit_usd, plan.fingerprint);
                    }
                }
            }
            // Permit is automatically dropped here when it goes out of scope
        }
        Ok(plans)
    }

    /// Checks if a complete execution plan is likely to be profitable after all costs.
    async fn is_plan_profitable(
        &self,
        plan: &mut CrossChainExecutionPlan,
        block_number: Option<u64>,
    ) -> Result<bool, CrossChainError> {
        let src_id = plan.bridge_quote.source_chain_id;
        let dst_id = plan.bridge_quote.dest_chain_id;

        // ---- determine initial USD value (source chain) ----
        let initial_amount_in = plan.source_swap
            .as_ref()
            .and_then(|s| s.amounts.first().copied())
            .ok_or_else(|| CrossChainError::Calculation("Missing initial amount".to_string()))?;

        // initial token on source chain
        let initial_token = plan.source_swap
            .as_ref()
            .and_then(|s| s.path.first().copied())
            .unwrap_or(plan.bridge_quote.from_token);

        let src_name = self.gas_oracle.get_chain_name_from_id(src_id)
            .ok_or(CrossChainError::ChainNotFound(src_id))?;
        let dst_name = self.gas_oracle.get_chain_name_from_id(dst_id)
            .ok_or(CrossChainError::ChainNotFound(dst_id))?;

        let init_dec = self.get_token_decimals(src_id, initial_token, block_number).await?;
        let init_px  = self.price_oracle
            .get_token_price_usd_at(&src_name, initial_token, block_number, None)
            .await?;
        let initial_usd = normalize_units(initial_amount_in, init_dec) * init_px;

        // ---- determine final USD value (dest chain) ----
        // If we did a dest swap, use its last token/amount; else the bridged token/amount.
        let (final_token, final_amount_out) = if let Some(dest) = &plan.dest_swap {
            (
                *dest.path.last().ok_or_else(|| CrossChainError::Calculation("Empty dest path".to_string()))?,
                *dest.amounts.last().ok_or_else(|| CrossChainError::Calculation("Missing dest amount".to_string()))?,
            )
        } else {
            (plan.bridge_quote.to_token, plan.bridge_quote.amount_out)
        };

        let final_dec = self.get_token_decimals(dst_id, final_token, block_number).await?;
        let final_px  = self.price_oracle
            .get_token_price_usd_at(&dst_name, final_token, block_number, None)
            .await?;
        let final_usd = normalize_units(final_amount_out, final_dec) * final_px;

        // ---- gas costs (both chains) in USD ----
        let src_gas_units    = plan.source_swap.as_ref().map_or(0, |s| s.gas_used.as_u64())
                               .saturating_add(plan.bridge_quote.estimated_gas.as_u64()); // bridge on source
        let dst_gas_units    = plan.dest_swap.as_ref().map_or(0, |s| s.gas_used.as_u64());
        let src_fee          = self.gas_oracle.get_gas_price(&src_name, block_number).await
                                  .map_err(|e| CrossChainError::DependencyError(format!("src gas: {e}")))?;
        let dst_fee          = self.gas_oracle.get_gas_price(&dst_name, block_number).await
                                  .map_err(|e| CrossChainError::DependencyError(format!("dst gas: {e}")))?;

        let src_gas_cost_wei = U256::from(src_gas_units).saturating_mul(src_fee.effective_price());
        let dst_gas_cost_wei = U256::from(dst_gas_units).saturating_mul(dst_fee.effective_price());

        // native (assume 18) → USD
        let src_native = self.get_native_token_address(src_id)?;
        let dst_native = self.get_native_token_address(dst_id)?;
        let src_native_px = self.price_oracle.get_token_price_usd_at(&src_name, src_native, block_number, None).await?;
        let dst_native_px = self.price_oracle.get_token_price_usd_at(&dst_name, dst_native, block_number, None).await?;
        let src_gas_usd = normalize_units(src_gas_cost_wei, 18) * src_native_px;
        let dst_gas_usd = normalize_units(dst_gas_cost_wei, 18) * dst_native_px;

        // ---- bridge fees / relayer fees ----
        // If the adapter exposes them separately, subtract here; otherwise assume included in amount_out.
        let bridge_fee_usd = plan.bridge_quote.fee_usd.unwrap_or(0.0);

        // ---- net ----
        let net_usd = final_usd - initial_usd - src_gas_usd - dst_gas_usd - bridge_fee_usd;

        // Store the calculated profit and fingerprint
        plan.net_profit_usd = net_usd;
        plan.fingerprint = format!(
            "{}-{}-{}-{}-{}-{}-{}",
            src_id,
            dst_id,
            plan.bridge_quote.bridge_name,
            initial_token,
            final_token,
            initial_amount_in,
            block_number.unwrap_or(0)
        );

        debug!("Cross-chain plan: initial ${:.4} → final ${:.4}; gas src ${:.4}, gas dst ${:.4}, bridge ${:.4}; net ${:.4}",
               initial_usd, final_usd, src_gas_usd, dst_gas_usd, bridge_fee_usd, net_usd);

        Ok(net_usd > self.config.min_profit_usd)
    }

    /// Helper function to get native token address for a chain
    fn get_native_token_address(&self, chain_id: u64) -> Result<Address, CrossChainError> {
        let chain_config = self.chain_configs.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("Chain config not found for chain_id {}", chain_id)))?;
        
        // Find the chain configuration and get WETH address as native token
        for (_chain_name, per_chain_config) in &chain_config.chains {
            if per_chain_config.chain_id == chain_id {
                return Ok(per_chain_config.weth_address);
            }
        }
        
        Err(CrossChainError::Configuration(format!("Native token not found for chain_id {}", chain_id)))
    }

    /// Helper function to get token decimals
    async fn get_token_decimals(
        &self,
        chain_id: u64,
        token: Address,
        block_number: Option<u64>,
    ) -> Result<u8, CrossChainError> {
        let blockchain_manager = self.blockchain_managers.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("Blockchain manager not found for chain_id {}", chain_id)))?;

        blockchain_manager.get_token_decimals(token, block_number.map(Into::into)).await
            .map_err(|e| CrossChainError::DependencyError(format!("Failed to get token decimals: {}", e)))
    }

    /// Get possible destination tokens for a bridge operation, prioritizing the same token
    fn get_possible_dest_tokens(&self, dest_chain_id: u64, source_token: Address) -> Vec<Address> {
        let mut tokens = vec![source_token]; // Try same token first

        // Get destination chain config to add common tokens
        if let Some(chain_config) = self.chain_configs.get(&dest_chain_id) {
            for (_chain_name, per_chain_config) in &chain_config.chains {
                if per_chain_config.chain_id == dest_chain_id {
                    // Add WETH if different from source token
                    if per_chain_config.weth_address != Address::zero() && per_chain_config.weth_address != source_token {
                        tokens.push(per_chain_config.weth_address);
                    }

                    // Add reference stablecoins
                    if let Some(stablecoins) = &per_chain_config.reference_stablecoins {
                        for &stable in stablecoins {
                            if stable != source_token && !tokens.contains(&stable) {
                                tokens.push(stable);
                            }
                        }
                    }
                    break;
                }
            }
        }

        tokens
    }

    /// Generate all profitable source-side swaps that end in a bridgeable token.
    async fn generate_source_swaps(
        &self,
        chain_id: u64,
        token_in: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<Vec<SwapRoute>, CrossChainError> {
        // Find the chain name for this chain_id
        let chain_config = self.chain_configs.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("Chain config not found for chain_id {}", chain_id)))?;
        let chain_name = chain_config.chains.values().find(|c| c.chain_id == chain_id)
            .map(|c| c.chain_name.as_str())
            .ok_or_else(|| CrossChainError::Configuration(format!("Chain name not found for chain_id {}", chain_id)))?;
        let per_chain_config = chain_config.chains.get(chain_name)
            .ok_or_else(|| CrossChainError::Configuration(format!("PerChainConfig not found for chain {}", chain_name)))?;
        let path_finder = self.path_finders.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("PathFinder not found for chain_id {}", chain_id)))?;
        
        let mut bridgeable_tokens = Vec::new();
        if let Some(stablecoins) = &per_chain_config.reference_stablecoins {
            bridgeable_tokens.extend(stablecoins.iter().copied());
        }
        if per_chain_config.weth_address != Address::zero() {
            bridgeable_tokens.push(per_chain_config.weth_address);
        }
        bridgeable_tokens.sort();
        bridgeable_tokens.dedup();
        
        // Use bounded concurrency with FuturesUnordered
        let mut futures = FuturesUnordered::new();
        
        for &bridge_token in &bridgeable_tokens {
            let pf = path_finder.clone();
            let sem = self.pathfinding_semaphore.clone();
            
            futures.push(async move {
                let _permit = sem.acquire().await.unwrap();
                match pf.find_optimal_path(token_in, bridge_token, amount_in, block_number).await {
                    Ok(Some(path)) => {
                        let swap_route = SwapRoute {
                            path: path.token_path.to_vec(),
                            pools: path.pool_path.to_vec(),
                            amounts: vec![path.initial_amount_in_wei, path.expected_output],
                            gas_used: U256::from(path.gas_estimate),
                            expected_output: Some(path.expected_output),
                        };
                        Some(swap_route)
                    },
                    Ok(None) => None,
                    Err(e) => {
                        warn!("PathFinder error for chain {}: {}", chain_id, e);
                        None
                    }
                }
            });
        }
        
        let mut results = Vec::new();
        while let Some(route_opt) = futures.next().await {
            if let Some(route) = route_opt {
                results.push(route);
            }
        }
        
        if results.is_empty() {
            warn!("No bridgeable source swaps found for chain {} token {:?}", chain_id, token_in);
        }
        Ok(results)
    }

    /// Generate all profitable destination-side swaps from the bridge token to major tokens.
    async fn generate_dest_swaps(
        &self,
        chain_id: u64,
        token_in: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<Vec<SwapRoute>, CrossChainError> {
        // Find the chain name for this chain_id
        let chain_config = self.chain_configs.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("Chain config not found for chain_id {}", chain_id)))?;
        let chain_name = chain_config.chains.values().find(|c| c.chain_id == chain_id)
            .map(|c| c.chain_name.as_str())
            .ok_or_else(|| CrossChainError::Configuration(format!("Chain name not found for chain_id {}", chain_id)))?;
        let per_chain_config = chain_config.chains.get(chain_name)
            .ok_or_else(|| CrossChainError::Configuration(format!("PerChainConfig not found for chain {}", chain_name)))?;
        let path_finder = self.path_finders.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("PathFinder not found for chain_id {}", chain_id)))?;
        
        let mut major_tokens = Vec::new();
        if let Some(stablecoins) = &per_chain_config.reference_stablecoins {
            major_tokens.extend(stablecoins.iter().copied());
        }
        if per_chain_config.weth_address != Address::zero() {
            major_tokens.push(per_chain_config.weth_address);
        }
        major_tokens.sort();
        major_tokens.dedup();
        
        // Use bounded concurrency with FuturesUnordered
        let mut futures = FuturesUnordered::new();
        
        for &dest_token in &major_tokens {
            let pf = path_finder.clone();
            let sem = self.pathfinding_semaphore.clone();
            
            futures.push(async move {
                let _permit = sem.acquire().await.unwrap();
                match pf.find_optimal_path(token_in, dest_token, amount_in, block_number).await {
                    Ok(Some(path)) => {
                        let swap_route = SwapRoute {
                            path: path.token_path.to_vec(),
                            pools: path.pool_path.to_vec(),
                            amounts: vec![path.initial_amount_in_wei, path.expected_output],
                            gas_used: U256::from(path.gas_estimate),
                            expected_output: Some(path.expected_output),
                        };
                        Some(swap_route)
                    },
                    Ok(None) => None,
                    Err(e) => {
                        warn!("PathFinder error for chain {}: {}", chain_id, e);
                        None
                    }
                }
            });
        }
        
        let mut results = Vec::new();
        while let Some(route_opt) = futures.next().await {
            if let Some(route) = route_opt {
                results.push(route);
            }
        }
        
        if results.is_empty() {
            warn!("No destination swaps found for chain {} token {:?}", chain_id, token_in);
        }
        Ok(results)
    }

    /// Analyzes swap events to find profitable paths using them.
    /// This method helps identify arbitrage opportunities based on recent swap activity.
    pub async fn analyze_swap_events_for_paths(
        &self,
        chain_id: u64,
        swap_events: &[SwapEvent],
    ) -> Result<Vec<Path>, CrossChainError> {
        let path_finder = self.path_finders.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("PathFinder not found for chain_id {}", chain_id)))?;
        
        let mut all_paths = Vec::new();
        
        // Find paths for each swap event
        for event in swap_events {
            match path_finder.find_paths_for_swap_event(event, Some(event.block_number)).await {
                Ok(paths) => all_paths.extend(paths),
                Err(e) => warn!("Failed to find paths from swap event: {}", e),
            }
        }
        
        Ok(all_paths)
    }
    
    /// Discovers and adds new pools to the pathfinding graph based on pool information.
    /// This method actually updates the graph with new pools, unlike analyze_swap_events_for_paths
    /// which only finds paths.
    pub async fn add_pools_to_graph(
        &self,
        chain_id: u64,
        pool_infos: &[PoolInfo],
    ) -> Result<(), CrossChainError> {
        let path_finder = self.path_finders.get(&chain_id)
            .ok_or_else(|| CrossChainError::Configuration(format!("PathFinder not found for chain_id {}", chain_id)))?;
        
        for pool_info in pool_infos {
            if let Err(e) = path_finder.add_pool_to_graph(pool_info).await {
                warn!("Failed to add pool to graph: {}", e);
            }
        }
        
        Ok(())
    }
}

impl fmt::Debug for CrossChainPathfinder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrossChainPathfinder")
            .field("chain_configs", &self.chain_configs.keys())
            .field("config", &self.config)
            .field("bridge_adapters", &self.bridge_adapters.keys())
            .finish_non_exhaustive()
    }
}