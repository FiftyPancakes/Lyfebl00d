use async_trait::async_trait;
use ethers::types::{Address, U256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, OwnedSemaphorePermit};
use tracing::{debug, info, warn, error, instrument};
use std::collections::HashMap;

use crate::{
    strategies::{MEVEngineError, MEVStrategy, MEVStrategyConfig, StrategyContext as ExecutionStrategyContext},
    mev_engine::{SafeActiveMEVPlay, MEVPlayStatus},
    mempool::MEVOpportunity,
    path::{Path, PathFinder},
    pool_manager::PoolManagerTrait,
    price_oracle::PriceOracle,
    types::{ArbitrageRoute, StrategyContext as TypesStrategyContext, ExecutionPriority},
    utils::normalize_units,
};

// Configuration constants - no magic numbers
const MAX_SLIPPAGE_BPS: u32 = 500; // 5% maximum slippage
const DEFAULT_SLIPPAGE_BPS: u32 = 50; // 0.5% default slippage
const MIN_LIQUIDITY_USD: f64 = 1.0; // Minimum pool liquidity - lowered for debugging
const MAX_PRICE_IMPACT_BPS: u32 = 300; // 3% max price impact
const SIMULATION_TIMEOUT_MS: u64 = 5_000; // 5 second timeout
const MAX_PATH_HOPS: usize = 4; // Maximum hops in arbitrage path
const GAS_PRICE_BUFFER_MULTIPLIER: f64 = 1.2; // 20% gas price buffer
const MIN_PROFIT_AFTER_GAS_USD: f64 = 1.0; // Minimum profit after gas
const BALANCE_CHECK_BUFFER_WEI: u128 = 100_000_000_000_000_000; // 0.1 ETH buffer

#[derive(Debug, Clone)]
pub struct ArbitrageStrategyConfig {
    pub min_profit_usd: f64,
    pub max_profit_usd: f64,
    pub slippage_tolerance_bps: u32,
    pub max_gas_price_gwei: u64,
    pub enable_flashloans: bool,
    pub max_position_size_usd: f64,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout_seconds: u64,
    pub volatility_discount: f64,
    pub gas_escalation_factor: f64,
}

impl Default for ArbitrageStrategyConfig {
    fn default() -> Self {
        Self {
            min_profit_usd: 10.0,
            max_profit_usd: 10_000.0,
            slippage_tolerance_bps: DEFAULT_SLIPPAGE_BPS,
            max_gas_price_gwei: 500,
            enable_flashloans: true,
            max_position_size_usd: 100_000.0,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_seconds: 300,
            volatility_discount: 0.95,  // 5% discount for high volatility
            gas_escalation_factor: 1.5,  // Escalate gas by 50% for competitive scenarios
        }
    }
}

#[derive(Debug)]
struct CircuitBreaker {
    consecutive_failures: RwLock<u32>,
    last_failure_time: RwLock<Option<Instant>>,
    is_open: RwLock<bool>,
    threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, timeout_seconds: u64) -> Self {
        Self {
            consecutive_failures: RwLock::new(0),
            last_failure_time: RwLock::new(None),
            is_open: RwLock::new(false),
            threshold,
            timeout: Duration::from_secs(timeout_seconds),
        }
    }

    async fn record_success(&self) {
        let mut failures = self.consecutive_failures.write().await;
        *failures = 0;
        let mut is_open = self.is_open.write().await;
        *is_open = false;
    }

    async fn record_failure(&self) -> Result<(), MEVEngineError> {
        let mut failures = self.consecutive_failures.write().await;
        *failures += 1;
        
        if *failures >= self.threshold {
            let mut is_open = self.is_open.write().await;
            *is_open = true;
            let mut last_failure = self.last_failure_time.write().await;
            *last_failure = Some(Instant::now());
            
            return Err(MEVEngineError::RiskError(
                format!("Circuit breaker triggered after {} consecutive failures", *failures)
            ));
        }
        
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        let is_open = self.is_open.read().await;
        if !*is_open {
            return true;
        }

        let last_failure = self.last_failure_time.read().await;
        if let Some(failure_time) = *last_failure {
            if failure_time.elapsed() > self.timeout {
                drop(last_failure);
                let mut is_open_mut = self.is_open.write().await;
                *is_open_mut = false;
                let mut failures = self.consecutive_failures.write().await;
                *failures = 0;
                return true;
            }
        }
        
        false
    }
}

pub struct ArbitrageStrategy {
    config: ArbitrageStrategyConfig,
    base_config: MEVStrategyConfig,
    path_finder: Arc<dyn PathFinder + Send + Sync>,
    pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    circuit_breaker: Arc<CircuitBreaker>,
    execution_semaphore: Arc<Semaphore>,
    path_cache: Arc<RwLock<HashMap<(Address, U256, u64), CachedPaths>>>,
    cache_ttl: Duration,
}

#[derive(Clone, Debug)]
struct CachedPaths {
    paths: Vec<Path>,
    inserted_at: Instant,
}

impl ArbitrageStrategy {
    pub fn new(
        config: ArbitrageStrategyConfig,
        base_config: MEVStrategyConfig,
        path_finder: Arc<dyn PathFinder + Send + Sync>,
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    ) -> Self {
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_threshold,
            config.circuit_breaker_timeout_seconds,
        ));
        
        Self {
            config,
            base_config,
            path_finder,
            pool_manager,
            price_oracle,
            circuit_breaker,
            execution_semaphore: Arc::new(Semaphore::new(10)), // Max 10 concurrent executions
            path_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(30),
        }
    }

    #[instrument(skip(self))]
    async fn find_cyclic_arbitrage_paths(
        &self,
        token_in: Address,
        amount_in: U256,
        block_number: u64,
    ) -> Result<Vec<Path>, MEVEngineError> {
        // Check cache first with TTL
        let cache_key = (token_in, amount_in, block_number);
        let now = Instant::now();
        {
            let cache = self.path_cache.read().await;
            if let Some(cached) = cache.get(&cache_key) {
                if now.duration_since(cached.inserted_at) <= self.cache_ttl && !cached.paths.is_empty() {
                    debug!("Using cached paths for token={:?}", token_in);
                    return Ok(cached.paths.clone());
                }
            }
        }

        // Find new paths with timeout
        let paths_future = self.path_finder.find_circular_arbitrage_paths(
            token_in,
            amount_in,
            Some(block_number)
        );
        
        let paths = tokio::time::timeout(
            Duration::from_millis(SIMULATION_TIMEOUT_MS),
            paths_future
        )
        .await
        .map_err(|_| MEVEngineError::Timeout("Path finding timeout".into()))?
        .map_err(|e| MEVEngineError::StrategyError(format!("Path finding failed: {}", e)))?;

        // Validate paths
        let valid_paths: Vec<Path> = paths
            .into_iter()
            .filter(|p| p.pool_path.len() <= MAX_PATH_HOPS)
            .collect();

        if valid_paths.is_empty() {
            debug!("No valid cyclic arbitrage paths found for token={:?}", token_in);
        } else {
            info!("Found {} valid cyclic arbitrage paths for token={:?}", valid_paths.len(), token_in);

            // Update cache with timestamp
            let mut cache = self.path_cache.write().await;
            cache.insert(cache_key, CachedPaths { paths: valid_paths.clone(), inserted_at: now });

            // Clean old entries with proper TTL
            cache.retain(|_, cached_paths| now.duration_since(cached_paths.inserted_at) <= self.cache_ttl);
        }
        
        Ok(valid_paths)
    }

    /// Convert an ArbitrageRoute from an opportunity to a Path for execution
    /// This trusts the opportunity's path instead of re-finding paths, eliminating lookahead bias
    async fn convert_arbitrage_route_to_path(
        &self,
        route: &ArbitrageRoute,
    ) -> Result<Path, MEVEngineError> {
        use smallvec::SmallVec;
        use crate::types::ProtocolType;

        // Build token path from route legs
        let mut token_path: SmallVec<[Address; 8]> = SmallVec::new();
        let mut pool_path: SmallVec<[Address; 8]> = SmallVec::new();
        let mut fee_path: SmallVec<[u32; 8]> = SmallVec::new();
        let mut protocol_path: SmallVec<[ProtocolType; 8]> = SmallVec::new();

        // Add initial token
        token_path.push(route.token_in);

        // Process each leg
        for leg in &route.legs {
            token_path.push(leg.token_out);
            pool_path.push(leg.pool_address);
            fee_path.push(leg.fee_bps);
            protocol_path.push(leg.protocol_type.clone());
        }

        // Simplified gas estimation
        let _gas_estimate = 21_000 + 5_000 + (route.legs.len() as u64 * 150_000);
        let gas_cost_usd = 0.01; // Simplified gas cost
        let profit_estimate_native = route.profit_usd;

        // Build path legs
        let mut legs = Vec::with_capacity(route.legs.len());
        for (i, leg) in route.legs.iter().enumerate() {
            legs.push(crate::path::types::PathLeg {
                from_token: leg.token_in,
                to_token: leg.token_out,
                pool_address: leg.pool_address,
                protocol_type: leg.protocol_type.clone(),
                fee: leg.fee_bps,
                amount_in: if i == 0 { route.initial_amount_in_wei } else { U256::zero() },
                amount_out: if i == route.legs.len() - 1 { route.amount_out_wei } else { U256::zero() },
                requires_approval: false,
                wrap_native: false,
                unwrap_native: false,
            });
        }

        Ok(Path {
            token_path,
            pool_path,
            fee_path,
            protocol_path,
            initial_amount_in_wei: route.initial_amount_in_wei,
            expected_output: route.amount_out_wei,
            min_output: U256::zero(), // Will be calculated by the path
            price_impact_bps: route.price_impact_bps,
            gas_estimate: route.legs.len() as u64 * 150_000,
            gas_cost_native: gas_cost_usd,
            profit_estimate_native,
            profit_estimate_usd: route.profit_usd,
            execution_priority: ExecutionPriority::Default,
            tags: SmallVec::new(),
            source: Some("opportunity".to_string()),
            discovery_timestamp: route.created_at_ns,
            confidence_score: 1.0 - route.risk_score, // Convert risk to confidence
            mev_resistance_score: 1.0 - route.mev_risk_score, // Convert risk to resistance
            token_in: route.token_in,
            token_out: route.token_out,
            legs,
            estimated_gas_cost_wei: route.estimated_gas_cost_wei,
        })
    }

    // Helper retained for readability. Not part of trait.
    #[instrument(skip(self, active_play, opportunity, context))]
    async fn execute_with_permit(
        &self,
        _permit: OwnedSemaphorePermit,
        active_play: &SafeActiveMEVPlay,
        opportunity: MEVOpportunity,
        context: &ExecutionStrategyContext,
    ) -> Result<(), MEVEngineError> {
        let (route, block_number) = match &opportunity {
            MEVOpportunity::Arbitrage(arb_route) => {
                let block_num = active_play
                    .read(|play| play.original_signal.block_number())
                    .await;
                (arb_route.clone(), block_num)
            }
            _ => {
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError(
                    "Incorrect opportunity type".into(),
                ));
            }
        };

        // Validate gas price
        if let Err(e) = self.validate_gas_price(context, block_number).await {
            if let Err(_) = active_play.record_simulation_failure(format!(
                "Gas price validation failed: {}",
                e
            )).await {
                warn!("Failed to record simulation failure: {}", e);
            }
            self.circuit_breaker.record_failure().await?;
            return Err(e);
        }

        // Convert ArbitrageRoute to Path - trust the opportunity's path instead of re-finding
        let path = match self.convert_arbitrage_route_to_path(&route).await {
            Ok(p) => p,
            Err(e) => {
                if let Err(_) = active_play.record_simulation_failure(format!(
                    "Path conversion failed: {}",
                    e
                )).await {
                    warn!("Failed to record simulation failure: {}", e);
                }
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError(format!("Path conversion failed: {}", e)));
            }
        };

        let paths = vec![path]; // Use the single path from the opportunity

        let chain = context.blockchain_manager.get_chain_name();

        // Try each path until one succeeds
        let mut last_error = None;
        for path in paths {
            // Validate pools in path
            let mut pools_valid = true;
            for pool in &path.pool_path {
                match context
                    .pool_manager
                    .get_pool_info(&chain, *pool, Some(block_number))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Pool {} validation failed: {}", pool, e);
                        pools_valid = false;
                        break;
                    }
                }
            }

            if !pools_valid {
                continue;
            }

            // Simulate and validate path
            let (_simulation_result, gas_used, net_profit_usd) = match self
                .simulate_and_validate_path(&path, route.token_in, block_number, context)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            // Update play with simulation results
            active_play
                .update_atomic(|play| {
                    play.gas_used = Some(gas_used);
                    play.estimated_profit_usd = net_profit_usd;
                })
                .await;

            // Calculate gas parameters with proper arithmetic
            let gas_price = context
                .gas_oracle.read().await
                .get_gas_price(&context.blockchain_manager.get_chain_name(), Some(block_number))
                .await
                .map_err(|e| MEVEngineError::StrategyError(format!(
                    "Failed to get gas price: {}",
                    e
                )))?;

            let priority_multiplier = U256::from(2);
            let max_fee_per_gas = gas_price
                .base_fee
                .checked_mul(priority_multiplier)
                .and_then(|v| v.checked_add(gas_price.priority_fee))
                .ok_or_else(|| {
                    MEVEngineError::GasEstimation("Gas calculation overflow".into())
                })?;

            active_play
                .update_atomic(|play| {
                    play.max_fee_per_gas = Some(max_fee_per_gas);
                    play.max_priority_fee_per_gas = Some(gas_price.priority_fee);
                })
                .await;

            if net_profit_usd < self.min_profit_threshold_usd() {
                active_play
                    .update_atomic(|play| {
                        if let Err(e) = play.record_simulation_failure("Profit below threshold".into()) {
                            warn!("Failed to record simulation failure: {}", e);
                        }
                    })
                    .await;
                continue;
            }

            // Risk Assessment
            if !self.base_config.ignore_risk {
                let opportunity_for_risk = MEVOpportunity::Arbitrage(route.clone());
                let risk_assessment = context
                    .risk_manager
                    .assess_arbitrage_risk(&opportunity_for_risk, Some(block_number))
                    .await
                    .map_err(|e| {
                        MEVEngineError::StrategyError(format!(
                            "Risk assessment failed: {}",
                            e
                        ))
                    })?;

                if risk_assessment.total_risk_score > 0.8 {
                    let mitigation_msg =
                        risk_assessment.mitigation.unwrap_or_else(|| "Risk score too high".to_string());
                    active_play
                        .update_atomic(|play| {
                            play.status = MEVPlayStatus::RiskAssessmentFailed(
                                mitigation_msg.clone(),
                            );
                        })
                        .await;
                    warn!("Risk assessment failed: {}", mitigation_msg);
                    continue;
                }
            }

            if let Err(_) = active_play.mark_ready_for_submission().await {
                warn!("Failed to mark play as ready for submission");
            }

            // Execute the trade
            let trade_id = active_play.read(|play| play.play_id.to_string()).await;

            // Calculate gas cost for paper trade
            let gas_cost_usd = if let Some(gas_used) = active_play.read(|play| play.gas_used).await
            {
                let gas_price = context
                    .gas_oracle.read().await
                    .get_gas_price(&context.blockchain_manager.get_chain_name(), Some(block_number))
                    .await
                    .map_err(|e| {
                        MEVEngineError::StrategyError(format!(
                            "Failed to get gas price: {}",
                            e
                        ))
                    })?;

                let gas_cost_wei = gas_used
                    .checked_mul(gas_price.base_fee)
                    .ok_or_else(|| {
                        MEVEngineError::ArithmeticError(
                            "Gas cost calculation overflow".into(),
                        )
                    })?;

                let gas_cost_eth = normalize_units(gas_cost_wei, 18);
                let eth_price = self
                    .price_oracle
                    .get_token_price_usd_at(
                        &context.blockchain_manager.get_chain_name(),
                        context.blockchain_manager.get_native_token_address(),
                        Some(block_number),
                        None,
                    )
                    .await
                    .map_err(|e| {
                        MEVEngineError::PricingError(format!(
                            "Failed to get ETH price: {}",
                            e
                        ))
                    })?;

                gas_cost_eth * eth_price
            } else {
                0.0
            };

            let tx_hash = match context
                .transaction_optimizer
                .paper_trade(&trade_id, &route, block_number, net_profit_usd, gas_cost_usd)
                .await
            {
                Ok(hash) => hash,
                Err(e) => {
                    error!("Trade execution failed: {}", e);
                    active_play
                        .update_atomic(|play| {
                            if let Err(err) = play.record_simulation_failure(format!("Execution failed: {}", e)) {
                                warn!("Failed to record simulation failure: {}", err);
                            }
                        })
                        .await;
                    self.circuit_breaker.record_failure().await?;
                    return Err(MEVEngineError::StrategyError(format!(
                        "Execution failed: {}",
                        e
                    )));
                }
            };

            let play_id = active_play.read(|play| play.play_id).await;
            if let Err(_) = active_play.submit_to_mempool(vec![tx_hash]).await {
                warn!("Failed to submit play to mempool");
            }
            info!(
                play_id = %play_id,
                tx_hash = %tx_hash,
                profit_usd = net_profit_usd,
                "Cyclic arbitrage submitted successfully"
            );

            // Record success
            self.circuit_breaker.record_success().await;
            return Ok(());
        }

        // All paths failed
        let error_msg = last_error
            .map(|e| format!("All paths failed. Last error: {}", e))
            .unwrap_or_else(|| "No profitable arbitrage path found".to_string());

        active_play
            .update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure(error_msg.clone()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            })
            .await;
        self.circuit_breaker.record_failure().await?;
        Ok(())
    }

    #[instrument(skip(self, path, context))]
    async fn calculate_profit_usd(
        &self,
        path: &Path,
        token_in: Address,
        block_number: u64,
        context: &ExecutionStrategyContext,
    ) -> Result<f64, MEVEngineError> {
        // Safe arithmetic for profit calculation
        let profit_in_source_token = path.expected_output
            .checked_sub(path.initial_amount_in_wei)
            .ok_or_else(|| MEVEngineError::ArithmeticError(
                "Underflow calculating profit".into()
            ))?;
        
        if profit_in_source_token.is_zero() {
            return Ok(0.0);
        }

        let chain_name = context.blockchain_manager.get_chain_name();
        
        // Get price with proper error handling
        let price_in = self.price_oracle
            .get_token_price_usd_at(&chain_name, token_in, Some(block_number), None)
            .await
            .map_err(|e| {
                warn!("Failed to get historical price for token {:?} at block {}: {}", 
                      token_in, block_number, e);
                MEVEngineError::PricingError(format!(
                    "Price lookup failed for token {} at block {}: {}", 
                    token_in, block_number, e
                ))
            })?;

        // Validate price
        if !price_in.is_finite() || price_in <= 0.0 {
            return Err(MEVEngineError::PricingError(
                format!("Invalid price for token {}: {}", token_in, price_in)
            ));
        }

        // Get token decimals
        let decimals = context.blockchain_manager
            .get_token_decimals(token_in, None)
            .await
            .map_err(|e| MEVEngineError::StrategyError(
                format!("Failed to get token decimals: {}", e)
            ))?;
        
        // Calculate USD value with validation
        let profit_tokens = normalize_units(profit_in_source_token, decimals);
        let profit_usd = profit_tokens * price_in;
        
        // Validate result
        if !profit_usd.is_finite() || profit_usd < 0.0 {
            return Err(MEVEngineError::StrategyError(
                format!("Invalid profit calculation: {} tokens * {} price = {}", 
                        profit_tokens, price_in, profit_usd)
            ));
        }
        
        // Check against max profit (sanity check)
        if profit_usd > self.config.max_profit_usd {
            warn!("Profit {} USD exceeds maximum {}, possible calculation error", 
                  profit_usd, self.config.max_profit_usd);
            return Err(MEVEngineError::ValidationError(
                format!("Profit {} exceeds maximum allowed", profit_usd)
            ));
        }
        
        Ok(profit_usd)
    }

    #[instrument(skip(self, path, context))]
    async fn validate_liquidity(
        &self,
        path: &Path,
        block_number: u64,
        context: &ExecutionStrategyContext,
    ) -> Result<(), MEVEngineError> {
        let chain = context.blockchain_manager.get_chain_name();
        
        for pool in &path.pool_path {
            let pool_info = context.pool_manager
                .get_pool_info(&chain, *pool, Some(block_number))
                .await
                .map_err(|e| MEVEngineError::StrategyError(
                    format!("Failed to fetch pool data: {}", e)
                ))?;
            
            // Calculate liquidity in USD
            let liquidity_usd = self.price_oracle
                .calculate_liquidity_usd(&pool_info)
                .await
                .map_err(|e| MEVEngineError::PricingError(
                    format!("Failed to calculate liquidity: {}", e)
                ))?;
            
            if liquidity_usd < MIN_LIQUIDITY_USD {
                warn!(
                    "Pool {} excluded due to low liquidity: {} USD < {} USD minimum (reserves0: {}, reserves1: {})",
                    pool, liquidity_usd, MIN_LIQUIDITY_USD, pool_info.reserves0, pool_info.reserves1
                );
                return Err(MEVEngineError::ValidationError(
                    format!("Insufficient liquidity: {} USD < {} USD minimum",
                            liquidity_usd, MIN_LIQUIDITY_USD)
                ));
            }

            debug!(
                "Pool {} passed liquidity check: {} USD >= {} USD minimum",
                pool, liquidity_usd, MIN_LIQUIDITY_USD
            );
        }
        
        Ok(())
    }

    #[instrument(skip(self, path, context))]
    async fn calculate_price_impact(
        &self,
        path: &Path,
        block_number: u64,
        context: &ExecutionStrategyContext,
    ) -> Result<u32, MEVEngineError> {
        let chain = context.blockchain_manager.get_chain_name();
        let mut cumulative_impact_bps = 0u32;
        
        for (i, pool) in path.pool_path.iter().enumerate() {
            let pool_info = context.pool_manager
                .get_pool_info(&chain, *pool, Some(block_number))
                .await
                .map_err(|e| MEVEngineError::StrategyError(
                    format!("Failed to get pool info: {}", e)
                ))?;
            
            // Calculate impact for this hop
            let amount_in = if i == 0 {
                path.initial_amount_in_wei
            } else {
                // Use output from previous hop
                path.expected_output
            };
            
            // Simple price impact calculation
            let reserve_in = pool_info.reserves0;
            let impact_bps = if !reserve_in.is_zero() {
                let impact_ratio = amount_in
                    .checked_mul(U256::from(10000))
                    .and_then(|v| v.checked_div(reserve_in))
                    .unwrap_or(U256::from(10000));
                
                impact_ratio.as_u32().min(10000)
            } else {
                10000 // 100% impact if no reserves
            };
            
            cumulative_impact_bps = cumulative_impact_bps
                .checked_add(impact_bps)
                .ok_or_else(|| MEVEngineError::ArithmeticError("Cumulative impact calculation overflow".into()))?;
        }
        
        Ok(cumulative_impact_bps)
    }

    #[instrument(skip(self, path, context))]
    async fn simulate_and_validate_path(
        &self,
        path: &Path,
        token_in: Address,
        block_number: u64,
        context: &ExecutionStrategyContext,
    ) -> Result<(ArbitrageRoute, U256, f64), MEVEngineError> {
        // Validate liquidity first
        self.validate_liquidity(path, block_number, context).await?;
        
        // Calculate and validate price impact
        let price_impact_bps = self.calculate_price_impact(path, block_number, context).await?;
        if price_impact_bps > MAX_PRICE_IMPACT_BPS {
            return Err(MEVEngineError::ValidationError(
                format!("Price impact {} bps exceeds maximum {} bps", 
                        price_impact_bps, MAX_PRICE_IMPACT_BPS)
            ));
        }
        
        // Calculate profit with context
        let profit_usd = self.calculate_profit_usd(path, token_in, block_number, context).await?;
        
        if profit_usd < self.config.min_profit_usd {
            return Err(MEVEngineError::StrategyError("Profit below threshold".into()));
        }

        // Convert path to route with proper slippage
        // Create a minimal types::StrategyContext with just the chain name
        let chain_name = context.blockchain_manager.get_chain_name();
        let types_context = TypesStrategyContext::new(block_number, chain_name.to_string());
        let mut route = ArbitrageRoute::from_path_with_context(path, &types_context);
        
        // Apply configurable slippage
        let slippage_factor = U256::from(10000)
            .checked_sub(U256::from(self.config.slippage_tolerance_bps.min(MAX_SLIPPAGE_BPS)))
            .ok_or_else(|| MEVEngineError::ArithmeticError("Slippage calculation error".into()))?;
        
        // Apply slippage to amount_out
        route.amount_out = route.amount_out
            .checked_mul(slippage_factor)
            .and_then(|v| v.checked_div(U256::from(10000)))
            .ok_or_else(|| MEVEngineError::ArithmeticError("Min output calculation error".into()))?;
        
        // Validate sender has sufficient balance with buffer
        let sender_address = context.blockchain_manager.get_uar_address()
            .ok_or_else(|| MEVEngineError::StrategyError(
                "Failed to get UAR address".into()
            ))?;
        let sender_balance = context.blockchain_manager
            .get_balance(sender_address, Some(block_number.into()))
            .await
            .map_err(|e| MEVEngineError::StrategyError(
                format!("Failed to get sender balance: {}", e)
            ))?;
        
        let required_balance = path.initial_amount_in_wei
            .checked_add(U256::from(BALANCE_CHECK_BUFFER_WEI))
            .ok_or_else(|| MEVEngineError::ArithmeticError("Balance check overflow".into()))?;
        
        if sender_balance < required_balance {
            return Err(MEVEngineError::InsufficientBalance(
                format!("Sender balance {} < required {}", sender_balance, required_balance)
            ));
        }
        
        // Simulate the transaction with timeout
        let simulation_future = context.transaction_optimizer
            .simulate_trade_submission(&route, block_number);
        
        let (gas_used, gas_cost_usd) = tokio::time::timeout(
            Duration::from_millis(SIMULATION_TIMEOUT_MS),
            simulation_future
        )
        .await
        .map_err(|_| MEVEngineError::Timeout("Simulation timeout".into()))?
        .map_err(|e| MEVEngineError::SimulationError(format!("Simulation failed: {}", e)))?;

        // Validate gas cost
        let gas_cost_with_buffer = gas_cost_usd * GAS_PRICE_BUFFER_MULTIPLIER;
        let net_profit_usd = profit_usd - gas_cost_with_buffer;
        
        if net_profit_usd < MIN_PROFIT_AFTER_GAS_USD {
            return Err(MEVEngineError::StrategyError(
                format!("Net profit {} USD below minimum {} USD after gas", 
                        net_profit_usd, MIN_PROFIT_AFTER_GAS_USD)
            ));
        }

        Ok((route, gas_used, net_profit_usd))
    }

    async fn validate_gas_price(
        &self,
        context: &ExecutionStrategyContext,
        block_number: u64,
    ) -> Result<(), MEVEngineError> {
        let chain_name = context.blockchain_manager.get_chain_name();
        let gas_price = context.gas_oracle.read().await
            .get_gas_price(&chain_name, Some(block_number))
            .await
            .map_err(|e| MEVEngineError::StrategyError(
                format!("Failed to get gas price: {}", e)
            ))?;
        
        let gas_price_gwei = gas_price.base_fee
            .checked_div(U256::exp10(9))
            .unwrap_or(U256::zero())
            .as_u64();
        
        if gas_price_gwei > self.config.max_gas_price_gwei {
            return Err(MEVEngineError::ValidationError(
                format!("Gas price {} gwei exceeds maximum {} gwei", 
                        gas_price_gwei, self.config.max_gas_price_gwei)
            ));
        }
        
        Ok(())
    }
}

#[async_trait]
impl MEVStrategy for ArbitrageStrategy {
    fn name(&self) -> &'static str {
        "CyclicArbitrage"
    }

    fn handles_opportunity_type(&self, opportunity: &MEVOpportunity) -> bool {
        matches!(opportunity, MEVOpportunity::Arbitrage(_))
    }

    fn min_profit_threshold_usd(&self) -> f64 {
        self.config.min_profit_usd
    }

    #[instrument(skip(self, active_play, opportunity, context))]
    async fn execute(
        &self,
        active_play: &SafeActiveMEVPlay,
        opportunity: MEVOpportunity,
        context: &ExecutionStrategyContext,
    ) -> Result<(), MEVEngineError> {
        // Check circuit breaker health
        if !self.circuit_breaker.is_healthy().await {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("Circuit breaker is open".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Err(MEVEngineError::RiskError(
                "Strategy circuit breaker is open".into()
            ));
        }

        // Acquire execution permit and delegate to inner executor to ensure the permit is held for the full execution
        let permit = self.execution_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MEVEngineError::OpportunityProcessing("Execution semaphore closed".into()))?;

        // Hold the permit in this scope while executing
        let _permit: OwnedSemaphorePermit = permit;

        let (_token_in, _amount_in, block_number) = match &opportunity {
            MEVOpportunity::Arbitrage(route) => {
                let block_num = active_play.read(|play| {
                    play.original_signal.block_number()
                }).await;
                (route.token_in, route.amount_in, block_num)
            },
            _ => {
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError("Incorrect opportunity type".into()));
            }
        };

        // Validate gas price
        if let Err(e) = self.validate_gas_price(context, block_number).await {
            active_play.update_atomic(|play| {
                if let Err(err) = play.record_simulation_failure(format!("Gas price validation failed: {}", e)) {
                    warn!("Failed to record simulation failure: {}", err);
                }
            }).await;
            self.circuit_breaker.record_failure().await?;
            return Err(e);
        }

        // Trust the pre-validated opportunity from AlphaEngine instead of redundant pathfinding
        // The AlphaEngine has already found and validated the arbitrage path
        let route = match &opportunity {
            MEVOpportunity::Arbitrage(arb_route) => arb_route.clone(),
            _ => {
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError("Incorrect opportunity type".into()));
            }
        };

        let path = match self.convert_arbitrage_route_to_path(&route).await {
            Ok(p) => p,
            Err(e) => {
                active_play.update_atomic(|play| {
                    if let Err(err) = play.record_simulation_failure(format!("Failed to convert arbitrage route to path: {}", e)) {
                        warn!("Failed to record simulation failure: {}", err);
                    }
                }).await;
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError(format!("Failed to convert arbitrage route to path: {}", e)));
            }
        };

        let chain = context.blockchain_manager.get_chain_name();

        // Execute the single pre-validated path from AlphaEngine

        // Validate pools in path
        let mut pools_valid = true;
        for pool in &path.pool_path {
            match context.pool_manager.get_pool_info(&chain, *pool, Some(block_number)).await {
                Ok(_) => {},
                Err(e) => {
                    warn!("Pool {} validation failed: {}", pool, e);
                    pools_valid = false;
                    break;
                }
            }
        }

        if !pools_valid {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("Pool validation failed".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        // Simulate and validate path
        let (_simulation_result, gas_used, net_profit_usd) = match self
            .simulate_and_validate_path(&path, route.token_in, block_number, context)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                active_play.update_atomic(|play| {
                    if let Err(err) = play.record_simulation_failure(format!("Path simulation failed: {}", e)) {
                        warn!("Failed to record simulation failure: {}", err);
                    }
                }).await;
                self.circuit_breaker.record_failure().await?;
                return Err(e);
            }
        };

        // Update play with simulation results
        active_play.update_atomic(|play| {
            play.gas_used = Some(gas_used);
            play.estimated_profit_usd = net_profit_usd;
        }).await;

        // Calculate gas parameters with proper arithmetic
        let gas_price = context.gas_oracle.read().await
            .get_gas_price(&context.blockchain_manager.get_chain_name(), Some(block_number))
            .await
            .map_err(|e| MEVEngineError::StrategyError(format!("Failed to get gas price: {}", e)))?;

        let priority_multiplier = U256::from(2);
        let max_fee_per_gas = gas_price.base_fee
            .checked_mul(priority_multiplier)
            .and_then(|v| v.checked_add(gas_price.priority_fee))
            .ok_or_else(|| MEVEngineError::GasEstimation("Gas calculation overflow".into()))?;

        active_play.update_atomic(|play| {
            play.max_fee_per_gas = Some(max_fee_per_gas);
            play.max_priority_fee_per_gas = Some(gas_price.priority_fee);
        }).await;

        if net_profit_usd < self.min_profit_threshold_usd() {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("Profit below threshold".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        // Risk Assessment
        if !self.base_config.ignore_risk {
            let opportunity_for_risk = MEVOpportunity::Arbitrage(route.clone());
            let risk_assessment = context.risk_manager
                .assess_arbitrage_risk(&opportunity_for_risk, Some(block_number))
                .await
                .map_err(|e| MEVEngineError::StrategyError(format!("Risk assessment failed: {}", e)))?;

            if risk_assessment.total_risk_score > 0.8 {
                let mitigation_msg = risk_assessment.mitigation
                    .unwrap_or_else(|| "Risk score too high".to_string());
                active_play.update_atomic(|play| {
                    play.status = MEVPlayStatus::RiskAssessmentFailed(mitigation_msg.clone());
                }).await;
                warn!("Risk assessment failed: {}", mitigation_msg);
                return Ok(());
            }
        }

        active_play.update_atomic(|play| {
            if let Err(e) = play.mark_ready_for_submission() {
                warn!("Failed to mark ready for submission: {}", e);
            }
        }).await;

        // Execute the trade
        let trade_id = active_play.read(|play| play.play_id.to_string()).await;
            
        // Calculate gas cost for paper trade
        let gas_cost_usd = if let Some(gas_used) = active_play.read(|play| play.gas_used).await {
            let gas_price = context.gas_oracle.read().await
                .get_gas_price(&context.blockchain_manager.get_chain_name(), Some(block_number))
                .await
                .map_err(|e| MEVEngineError::StrategyError(format!("Failed to get gas price: {}", e)))?;

            let gas_cost_wei = gas_used
                .checked_mul(gas_price.base_fee)
                .ok_or_else(|| MEVEngineError::ArithmeticError("Gas cost calculation overflow".into()))?;

            let gas_cost_eth = normalize_units(gas_cost_wei, 18);
            let eth_price = self.price_oracle
                .get_token_price_usd_at(&context.blockchain_manager.get_chain_name(),
                                      context.blockchain_manager.get_native_token_address(),
                                      Some(block_number), None)
                .await
                .map_err(|e| MEVEngineError::PricingError(format!("Failed to get ETH price: {}", e)))?;

            gas_cost_eth * eth_price
        } else {
            0.0 // Fallback if gas_used is not available
        };

        let tx_hash = match context.transaction_optimizer
            .paper_trade(&trade_id, &route, block_number, net_profit_usd, gas_cost_usd)
            .await
        {
            Ok(hash) => hash,
            Err(e) => {
                error!("Trade execution failed: {}", e);
                active_play.update_atomic(|play| {
                    if let Err(err) = play.record_simulation_failure(format!("Execution failed: {}", e)) {
                        warn!("Failed to record simulation failure: {}", err);
                    }
                }).await;
                self.circuit_breaker.record_failure().await?;
                return Err(MEVEngineError::StrategyError(format!("Execution failed: {}", e)));
            }
        };

        let play_id = active_play.read(|play| play.play_id).await;
        active_play.update_atomic(|play| {
            if let Err(e) = play.submit_to_mempool(vec![tx_hash]) {
                warn!("Failed to submit to mempool: {}", e);
            }
        }).await;
        info!(
            play_id = %play_id,
            tx_hash = %tx_hash,
            profit_usd = net_profit_usd,
            "Cyclic arbitrage submitted successfully"
        );

        // Record success
        self.circuit_breaker.record_success().await;
        Ok(())
    }

    // removed: duplicate helper not in trait

    #[instrument(skip(self, opportunity, context))]
    async fn estimate_profit(
        &self,
        opportunity: &MEVOpportunity,
        context: &ExecutionStrategyContext,
    ) -> Result<f64, MEVEngineError> {
        let (token_in, amount_in, block_number) = match opportunity {
            MEVOpportunity::Arbitrage(route) => {
                let block_number = context.blockchain_manager.get_block_number().await
                    .unwrap_or(0);
                (route.token_in, route.amount_in, block_number)
            },
            _ => return Ok(0.0),
        };

        // Check circuit breaker
        if !self.circuit_breaker.is_healthy().await {
            return Ok(0.0);
        }

        // Find arbitrage paths
        let paths = self
            .find_cyclic_arbitrage_paths(token_in, amount_in, block_number)
            .await?;

        if paths.is_empty() {
            return Ok(0.0);
        }

        let chain = context.blockchain_manager.get_chain_name();

        // Calculate maximum profit from all paths, tie-breaking by minimum gas used
        let mut max_profit = 0.0;
        let mut min_gas_for_max_profit = U256::max_value();
        for path in paths {
            // Quick validation of pools
            let mut pools_valid = true;
            for pool in &path.pool_path {
                if context.pool_manager
                    .get_pool_info(&chain, *pool, Some(block_number))
                    .await
                    .is_err()
                {
                    pools_valid = false;
                    break;
                }
            }

            if !pools_valid {
                continue;
            }

            // Try to calculate profit for this path
            match self.simulate_and_validate_path(&path, token_in, block_number, context).await {
                Ok((_route, gas_used, net_profit_usd)) => {
                    if net_profit_usd > max_profit {
                        max_profit = net_profit_usd;
                        min_gas_for_max_profit = gas_used;
                    } else if (net_profit_usd - max_profit).abs() <= f64::EPSILON && gas_used < min_gas_for_max_profit {
                        // Prefer lower gas usage when profits are effectively equal
                        min_gas_for_max_profit = gas_used;
                    }
                },
                Err(e) => {
                    debug!("Path simulation failed: {}", e);
                    continue;
                }
            }
        }

        Ok(max_profit)
    }


}