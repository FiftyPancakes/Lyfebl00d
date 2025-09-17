use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn, error};
use ethers::types::{Address, Log, H256, U256};

use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    types::{ArbitrageRoute, ProtocolType, ChainEvent, TokenPairStats},
    strategies::discovery::{DiscoveryConfig, ScannerContext},
};

/// Strategy types for different arbitrage approaches
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ArbitrageStrategy {
    Cyclic,
    Statistical,
    MarketMaking,
    CrossChain,
}

/// Represents a discovered arbitrage path with metadata
#[derive(Clone, Debug)]
pub struct DiscoveredPath {
    pub path: crate::path::Path,
    pub profit_usd: f64,
    pub strategy: ArbitrageStrategy,
    pub confidence_score: f64,
    pub risk_score: f64,
    pub timestamp: std::time::Instant,
}

/// Unified arbitrage discovery service that centralizes pathfinding
/// and eliminates redundant searches across different strategies
#[derive(Clone)]
pub struct UnifiedArbitrageDiscovery {
    context: ScannerContext,
    config: Arc<RwLock<DiscoveryConfig>>,
    running: Arc<RwLock<bool>>,
    event_sender: broadcast::Sender<ChainEvent>,
    affected_tokens: Arc<RwLock<HashSet<Address>>>,
    discovered_paths: Arc<RwLock<HashMap<String, DiscoveredPath>>>,
    statistical_pair_stats: Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
    active_strategies: Arc<RwLock<HashSet<ArbitrageStrategy>>>,
    path_cache: Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
}

impl UnifiedArbitrageDiscovery {
    pub fn new(
        context: ScannerContext,
        event_broadcaster: broadcast::Sender<ChainEvent>,
        statistical_pair_stats: Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
    ) -> Self {
        Self {
            context,
            config: Arc::new(RwLock::new(DiscoveryConfig::default())),
            running: Arc::new(RwLock::new(false)),
            event_sender: event_broadcaster,
            affected_tokens: Arc::new(RwLock::new(HashSet::new())),
            discovered_paths: Arc::new(RwLock::new(HashMap::new())),
            statistical_pair_stats,
            active_strategies: Arc::new(RwLock::new(HashSet::new())),
            path_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the unified discovery service
    pub async fn start(&self) -> Result<(), ArbitrageError> {
        info!("Starting UnifiedArbitrageDiscovery");
        *self.running.write().await = true;

        // Initialize with all strategies active by default
        {
            let mut strategies = self.active_strategies.write().await;
            strategies.insert(ArbitrageStrategy::Cyclic);
            strategies.insert(ArbitrageStrategy::Statistical);
            strategies.insert(ArbitrageStrategy::MarketMaking);
            strategies.insert(ArbitrageStrategy::CrossChain);
        }

        // Start the main discovery loop
        let context = self.context.clone();
        let config = self.config.clone();
        let running = self.running.clone();
        let mut event_receiver = self.event_sender.subscribe();
        let affected_tokens = self.affected_tokens.clone();
        let discovered_paths = self.discovered_paths.clone();
        let statistical_pair_stats = self.statistical_pair_stats.clone();
        let active_strategies = self.active_strategies.clone();
        let path_cache = self.path_cache.clone();

        tokio::spawn(async move {
            Self::discovery_loop(
                context,
                config,
                running,
                &mut event_receiver,
                affected_tokens,
                discovered_paths,
                statistical_pair_stats,
                active_strategies,
                path_cache,
            ).await;
        });

        Ok(())
    }

    /// Stop the unified discovery service
    pub async fn stop(&self) -> Result<(), ArbitrageError> {
        info!("Stopping UnifiedArbitrageDiscovery");
        *self.running.write().await = false;
        Ok(())
    }

    /// Main discovery event processing loop
    async fn discovery_loop(
        context: ScannerContext,
        config: Arc<RwLock<DiscoveryConfig>>,
        running: Arc<RwLock<bool>>,
        event_receiver: &mut broadcast::Receiver<ChainEvent>,
        affected_tokens: Arc<RwLock<HashSet<Address>>>,
        discovered_paths: Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        statistical_pair_stats: Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
        active_strategies: Arc<RwLock<HashSet<ArbitrageStrategy>>>,
        path_cache: Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) {
        info!("Unified discovery loop started");

        while *running.read().await {
            match event_receiver.recv().await {
                Ok(event) => {
                    match event {
                        ChainEvent::EvmLog { chain_name: _, log } => {
                            if let Some(tokens) = Self::extract_tokens_from_log(&log).await {
                                debug!("Pool event detected, affected tokens: {:?}", tokens);

                                // Add affected tokens to the set
                                {
                                    let mut affected = affected_tokens.write().await;
                                    for token in tokens {
                                        affected.insert(token);
                                    }
                                }

                                // Process affected tokens for all active strategies
                                if let Err(e) = Self::process_affected_tokens_for_all_strategies(
                                    &context,
                                    &config,
                                    &affected_tokens,
                                    &discovered_paths,
                                    &statistical_pair_stats,
                                    &active_strategies,
                                    &path_cache,
                                ).await {
                                    error!("Failed to process affected tokens: {}", e);
                                }
                            }
                        }
                        ChainEvent::EvmNewHead { .. } => {
                            // Periodic cleanup
                            Self::cleanup_expired_paths(&discovered_paths, &path_cache).await;
                        }
                        _ => {} // Ignore other event types
                    }
                }
                Err(e) => {
                    warn!("Error receiving event in unified discovery: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }

        info!("Unified discovery loop stopped");
    }

    /// Process affected tokens for all active strategies
    async fn process_affected_tokens_for_all_strategies(
        context: &ScannerContext,
        config: &Arc<RwLock<DiscoveryConfig>>,
        affected_tokens: &Arc<RwLock<HashSet<Address>>>,
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        statistical_pair_stats: &Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
        active_strategies: &Arc<RwLock<HashSet<ArbitrageStrategy>>>,
        path_cache: &Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) -> Result<(), ArbitrageError> {
        let config = config.read().await;
        let active_strategies = active_strategies.read().await;

        // Get affected tokens
        let tokens_to_check: Vec<Address> = {
            let affected = affected_tokens.read().await;
            affected.iter().cloned().collect()
        };

        if tokens_to_check.is_empty() {
            return Ok(());
        }

        // Clear affected tokens after processing
        {
            let mut affected = affected_tokens.write().await;
            affected.clear();
        }

        debug!("Processing {} affected tokens for {} active strategies",
               tokens_to_check.len(), active_strategies.len());

        // Process each active strategy
        for strategy in active_strategies.iter() {
            match strategy {
                ArbitrageStrategy::Cyclic => {
                    Self::discover_cyclic_opportunities(
                        context, &config, &tokens_to_check, discovered_paths, path_cache,
                    ).await?;
                }
                ArbitrageStrategy::Statistical => {
                    Self::discover_statistical_opportunities(
                        context, &config, &tokens_to_check, discovered_paths,
                        statistical_pair_stats, path_cache,
                    ).await?;
                }
                ArbitrageStrategy::MarketMaking => {
                    Self::discover_market_making_opportunities(
                        context, &config, &tokens_to_check, discovered_paths, path_cache,
                    ).await?;
                }
                ArbitrageStrategy::CrossChain => {
                    // Cross-chain discovery would be implemented here
                    // For now, we'll skip it as it requires additional infrastructure
                }
            }
        }

        Ok(())
    }

    /// Discover cyclic arbitrage opportunities
    async fn discover_cyclic_opportunities(
        context: &ScannerContext,
        config: &DiscoveryConfig,
        tokens: &[Address],
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        path_cache: &Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) -> Result<(), ArbitrageError> {
        for &start_token in tokens {
            let chain_id = context.blockchain_manager.get_chain_id();
            let cache_key = format!("cyclic_{}_{}", chain_id, start_token);

            // Check cache first
            {
                let cache = path_cache.read().await;
                if let Some((cached_path, timestamp)) = cache.get(&cache_key) {
                    if timestamp.elapsed() < std::time::Duration::from_secs(30) {
                        // Use cached path
                        if let Ok(profit_usd) = Self::calculate_profit_usd(context, start_token, cached_path.expected_output, None).await {
                            if profit_usd >= config.min_profit_usd {
                                Self::store_discovered_path(
                                    discovered_paths,
                                    cache_key,
                                    cached_path.clone(),
                                    profit_usd,
                                    ArbitrageStrategy::Cyclic,
                                    0.8, // High confidence for cached paths
                                    0.2, // Low risk for cyclic
                                ).await;
                            }
                        }
                        continue;
                    }
                }
            }

            // Find new cyclic paths
            if let Ok(paths) = context.path_finder.find_circular_arbitrage_paths(
                start_token,
                U256::from(config.min_profit_usd as u64) * U256::from(1_000_000_000_000_000_000u64),
                None,
            ).await {
                for path in paths {
                    let profit_wei = path.expected_output.saturating_sub(path.initial_amount_in_wei);
                    if profit_wei.is_zero() {
                        continue;
                    }

                    if let Ok(profit_usd) = Self::calculate_profit_usd(context, start_token, profit_wei, None).await {
                        if profit_usd >= config.min_profit_usd && path.price_impact_bps <= config.max_price_impact_bps {
                            // Cache the path
                            {
                                let mut cache = path_cache.write().await;
                                cache.insert(cache_key.clone(), (path.clone(), std::time::Instant::now()));
                            }

                            Self::store_discovered_path(
                                discovered_paths,
                                cache_key.clone(),
                                path,
                                profit_usd,
                                ArbitrageStrategy::Cyclic,
                                0.9, // High confidence for fresh paths
                                0.2, // Low risk for cyclic
                            ).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Discover statistical arbitrage opportunities
    async fn discover_statistical_opportunities(
        context: &ScannerContext,
        config: &DiscoveryConfig,
        tokens: &[Address],
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        statistical_pair_stats: &Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
        path_cache: &Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) -> Result<(), ArbitrageError> {
        // Get current block number for proper caching
        let current_block = match context.get_current_block_number().await {
            Ok(block) => block,
            Err(e) => {
                debug!("Failed to get current block number: {}", e);
                0
            }
        };
        let stats = statistical_pair_stats.read().await;

        for &token_a in tokens {
            for &token_b in tokens {
                if token_a == token_b {
                    continue;
                }

                let pair_key = (token_a, token_b);
                if let Some(pair_stats) = stats.get(&pair_key) {
                    // Check for statistical signal
                    if Self::has_statistical_signal(pair_stats, config) {
                        let chain_id = context.blockchain_manager.get_chain_id();
                        let cache_key = format!("statistical_{}_{}_{}", chain_id, token_a, token_b);

                        // Check cache first
                        {
                            let cache = path_cache.read().await;
                            if let Some((cached_path, timestamp)) = cache.get(&cache_key) {
                                if timestamp.elapsed() < std::time::Duration::from_secs(60) {
                                    // Use cached path
                                    if let Ok(profit_usd) = Self::calculate_profit_usd(context, token_a, cached_path.expected_output, None).await {
                                        if profit_usd >= config.min_profit_usd {
                                            Self::store_discovered_path(
                                                discovered_paths,
                                                cache_key,
                                                cached_path.clone(),
                                                profit_usd,
                                                ArbitrageStrategy::Statistical,
                                                pair_stats.cointegration_score,
                                                0.3, // Medium risk for statistical
                                            ).await;
                                        }
                                    }
                                    continue;
                                }
                            }
                        }

                        // Find new statistical paths
                        if let Ok(Some(path)) = context.path_finder.find_optimal_path(
                            token_a,
                            token_b,
                            U256::from(config.min_profit_usd as u64) * U256::from(1_000_000_000_000_000_000u64),
                            Some(current_block),
                        ).await {
                            let profit_wei = path.expected_output.saturating_sub(path.initial_amount_in_wei);
                            if profit_wei.is_zero() {
                                continue;
                            }

                            if let Ok(profit_usd) = Self::calculate_profit_usd(context, token_a, profit_wei, None).await {
                                if profit_usd >= config.min_profit_usd && path.price_impact_bps <= config.max_price_impact_bps {
                                    // Cache the path
                                    {
                                        let mut cache = path_cache.write().await;
                                        cache.insert(cache_key.clone(), (path.clone(), std::time::Instant::now()));
                                    }

                                    Self::store_discovered_path(
                                        discovered_paths,
                                        cache_key,
                                        path,
                                        profit_usd,
                                        ArbitrageStrategy::Statistical,
                                        pair_stats.cointegration_score,
                                        0.3, // Medium risk for statistical
                                    ).await;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Discover market making opportunities
    async fn discover_market_making_opportunities(
        context: &ScannerContext,
        config: &DiscoveryConfig,
        tokens: &[Address],
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        path_cache: &Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) -> Result<(), ArbitrageError> {
        // Get current block number for proper caching
        let current_block = match context.get_current_block_number().await {
            Ok(block) => block,
            Err(e) => {
                debug!("Failed to get current block number: {}", e);
                0
            }
        };
        for &token in tokens {
            let chain_id = context.blockchain_manager.get_chain_id();
            let cache_key = format!("market_making_{}_{}", chain_id, token);

            // Check cache first
            {
                let cache = path_cache.read().await;
                if let Some((cached_path, timestamp)) = cache.get(&cache_key) {
                    if timestamp.elapsed() < std::time::Duration::from_secs(10) {
                        // Use cached path
                        if let Ok(profit_usd) = Self::calculate_profit_usd(context, token, cached_path.expected_output, None).await {
                            if profit_usd >= config.min_profit_usd {
                                Self::store_discovered_path(
                                    discovered_paths,
                                    cache_key,
                                    cached_path.clone(),
                                    profit_usd,
                                    ArbitrageStrategy::MarketMaking,
                                    0.7, // Medium confidence for market making
                                    0.1, // Very low risk for market making
                                ).await;
                            }
                        }
                        continue;
                    }
                }
            }

            // Find new market making paths
            if let Ok(Some(path)) = context.path_finder.find_optimal_path(
                token,
                token, // Same token out for market making
                U256::from(config.min_profit_usd as u64) * U256::from(1_000_000_000_000_000_000u64),
                Some(current_block),
            ).await {
                let profit_wei = path.expected_output.saturating_sub(path.initial_amount_in_wei);
                if profit_wei.is_zero() {
                    continue;
                }

                if let Ok(profit_usd) = Self::calculate_profit_usd(context, token, profit_wei, None).await {
                    if profit_usd >= config.min_profit_usd && path.price_impact_bps <= config.max_price_impact_bps {
                        // Cache the path
                        {
                            let mut cache = path_cache.write().await;
                            cache.insert(cache_key.clone(), (path.clone(), std::time::Instant::now()));
                        }

                        Self::store_discovered_path(
                            discovered_paths,
                            cache_key,
                            path,
                            profit_usd,
                            ArbitrageStrategy::MarketMaking,
                            0.8, // High confidence for fresh paths
                            0.1, // Very low risk for market making
                        ).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Store a discovered path
    async fn store_discovered_path(
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        key: String,
        path: crate::path::Path,
        profit_usd: f64,
        strategy: ArbitrageStrategy,
        confidence_score: f64,
        risk_score: f64,
    ) {
        let discovered_path = DiscoveredPath {
            path,
            profit_usd,
            strategy,
            confidence_score,
            risk_score,
            timestamp: std::time::Instant::now(),
        };

        let mut paths = discovered_paths.write().await;
        paths.insert(key, discovered_path);
    }

    /// Check if there's a statistical signal for arbitrage
    fn has_statistical_signal(pair_stats: &TokenPairStats, config: &DiscoveryConfig) -> bool {
        // Check volatility threshold
        let volatility_threshold = pair_stats.volatility * 2.0;
        if pair_stats.volatility < volatility_threshold {
            return false;
        }

        // Check cointegration
        let cointegration_threshold = config.min_confidence_score.max(0.7);
        if pair_stats.cointegration_score < cointegration_threshold {
            return false;
        }

        // Check mean absolute difference
        if pair_stats.mean_abs_diff <= 0.0 {
            return false;
        }

        // Check mean squared difference
        if pair_stats.mean_sq_diff < pair_stats.mean_abs_diff.powi(2) {
            return false;
        }

        true
    }

    /// Extract affected tokens from a pool event log using ABI config
    async fn extract_tokens_from_log(log: &Log) -> Option<Vec<Address>> {
        if log.topics.is_empty() {
            return None;
        }

        // Import the ABI config from event_driven_discovery
        use crate::strategies::discovery::event_driven_discovery::ABI_CONFIG;

        let event_topic = log.topics[0];

        // Check if this topic matches any known DEX event
        let is_known_event = ABI_CONFIG.dexes.values()
            .flat_map(|dex| dex.topic0.values())
            .any(|topic_hex| {
                if let Ok(decoded) = hex::decode(topic_hex.trim_start_matches("0x")) {
                    if decoded.len() == 32 {
                        let topic = H256::from_slice(&decoded);
                        topic == event_topic
                    } else {
                        false
                    }
                } else {
                    false
                }
            });

        if is_known_event {
            debug!("Detected pool event for address: {:?}", log.address);
            // Return None to indicate this pool needs processing but without specific tokens yet
            // The processing logic will handle pool resolution
            return None;
        }

        None
    }

    /// Calculate profit in USD for a given token and amount
    async fn calculate_profit_usd(
        context: &ScannerContext,
        profit_token: Address,
        profit_amount: U256,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let chain_name = context.get_chain_name();

        let price = if let Some(block) = block_number {
            context.price_oracle.get_token_price_usd_at(&chain_name, profit_token, Some(block), None).await
        } else {
            context.price_oracle.get_token_price_usd(&chain_name, profit_token).await
        }.map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get token price: {}", e)))?;

        let token_info = context.chain_infra.token_registry.get_token(profit_token).await
            .map_err(|e| ArbitrageError::TokenRegistry(format!("Failed to get token info: {}", e)))?
            .ok_or_else(|| ArbitrageError::TokenRegistry(format!("Token not found: {:?}", profit_token)))?;

        // Safe unit scaling using format_units to avoid truncation
        let profit_tokens = ethers::utils::format_units(profit_amount, token_info.decimals as i32)
            .unwrap_or_else(|_| "0".into())
            .parse::<f64>()
            .unwrap_or(0.0);
        let profit_usd = price * profit_tokens;

        Ok(profit_usd)
    }

    /// Clean up expired paths and cache entries
    async fn cleanup_expired_paths(
        discovered_paths: &Arc<RwLock<HashMap<String, DiscoveredPath>>>,
        path_cache: &Arc<RwLock<HashMap<String, (crate::path::Path, std::time::Instant)>>>,
    ) {
        let expiry_duration = std::time::Duration::from_secs(300); // 5 minutes

        // Clean discovered paths
        {
            let mut paths = discovered_paths.write().await;
            paths.retain(|_, path| path.timestamp.elapsed() < expiry_duration);
        }

        // Clean path cache
        {
            let mut cache = path_cache.write().await;
            cache.retain(|_, (_, timestamp)| timestamp.elapsed() < expiry_duration);
        }

        debug!("Cleaned up expired paths and cache entries");
    }

    /// Get all currently discovered paths
    pub async fn get_discovered_paths(&self) -> HashMap<String, DiscoveredPath> {
        self.discovered_paths.read().await.clone()
    }

    /// Get paths for a specific strategy
    pub async fn get_paths_for_strategy(&self, strategy: ArbitrageStrategy) -> Vec<DiscoveredPath> {
        let paths = self.discovered_paths.read().await;
        paths.values()
            .filter(|path| path.strategy == strategy)
            .cloned()
            .collect()
    }

    /// Convert a discovered path to MEV opportunity
    pub async fn path_to_mev_opportunity(&self, discovered_path: &DiscoveredPath) -> Result<MEVOpportunity, ArbitrageError> {
        let legs = discovered_path.path.legs.iter().map(|leg| {
            let dex_name = match leg.protocol_type.clone() {
                ProtocolType::UniswapV2 => "Uniswap V2".to_string(),
                ProtocolType::UniswapV3 => "Uniswap V3".to_string(),
                _ => "DEX".to_string(),
            };

            crate::types::ArbitrageRouteLeg {
                dex_name,
                protocol_type: leg.protocol_type.clone(),
                pool_address: leg.pool_address,
                token_in: leg.from_token,
                token_out: leg.to_token,
                amount_in: leg.amount_in,
                min_amount_out: leg.amount_out.saturating_mul(U256::from(95u64)).checked_div(U256::from(100u64)).unwrap_or(leg.amount_out),
                fee_bps: leg.fee,
                expected_out: Some(leg.amount_out),
            }
        }).collect();

        let route = ArbitrageRoute {
            legs,
            initial_amount_in_wei: discovered_path.path.initial_amount_in_wei,
            amount_out_wei: discovered_path.path.expected_output,
            profit_usd: discovered_path.profit_usd,
            estimated_gas_cost_wei: discovered_path.path.estimated_gas_cost_wei,
            price_impact_bps: discovered_path.path.price_impact_bps,
            route_description: format!("{:?} arbitrage: {} -> ...",
                discovered_path.strategy, discovered_path.path.token_in),
            risk_score: discovered_path.risk_score,
            chain_id: self.context.blockchain_manager.get_chain_id(),
            created_at_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            mev_risk_score: discovered_path.risk_score * 1.2,
            flashloan_amount_wei: Some(discovered_path.path.initial_amount_in_wei),
            flashloan_fee_wei: Some(discovered_path.path.initial_amount_in_wei.saturating_mul(U256::from(5u64)).checked_div(U256::from(1000u64)).unwrap_or_default()),
            token_in: discovered_path.path.token_in,
            token_out: discovered_path.path.token_out,
            amount_in: discovered_path.path.initial_amount_in_wei,
            amount_out: discovered_path.path.expected_output,
        };

        Ok(MEVOpportunity::Arbitrage(route))
    }

    /// Update configuration
    pub async fn update_config(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        *self.config.write().await = config;
        Ok(())
    }

    /// Get current configuration
    pub async fn get_config(&self) -> DiscoveryConfig {
        self.config.read().await.clone()
    }

    /// Check if service is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Enable or disable specific strategies
    pub async fn set_strategy_enabled(&self, strategy: ArbitrageStrategy, enabled: bool) -> Result<(), ArbitrageError> {
        let mut strategies = self.active_strategies.write().await;
        if enabled {
            strategies.insert(strategy.clone());
        } else {
            strategies.remove(&strategy);
        }
        info!("Strategy {:?} {}", strategy, if enabled { "enabled" } else { "disabled" });
        Ok(())
    }

    /// Get active strategies
    pub async fn get_active_strategies(&self) -> HashSet<ArbitrageStrategy> {
        self.active_strategies.read().await.clone()
    }
}
