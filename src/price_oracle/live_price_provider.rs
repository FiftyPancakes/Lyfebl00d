use async_trait::async_trait;
use ethers::types::{Address, U256, I256, Bytes};
use ethers::prelude::Middleware;
use moka::future::Cache;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, warn, info};

// Chainlink Aggregator ABI for type-safe calls
ethers::contract::abigen!(
    ChainlinkAggregator,
    r#"[
        function latestRoundData() external view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
        function decimals() external view returns (uint8)
    ]"#,
);

use crate::{
    alpha::circuit_breaker::{CircuitBreaker, CircuitBreakerStats},
    blockchain::BlockchainManager,
    config::Config,
    errors::PriceError,
    price_oracle::{
        types::{
            ChainlinkPriceData, DecimalResolver, Environment, OracleMetrics,
            PriceCacheKey, RecursionGuard, RequestDeduplicator,
            safe_f64_to_u256, safe_u256_to_f64,
            MAX_USD_PRICE, MIN_USD_PRICE, CHAINLINK_MAX_STALENESS_SECONDS,
        },
        aggregator_quote_wrapper::AggregatorPriceAdapter,
    },
    quote_source::AggregatorQuoteSource,
    types::PoolInfo,
};

/// Enhanced Live Price Provider with all safety and performance improvements
pub struct LivePriceProvider {
    chain_cfg: Arc<Config>,
    chain: Arc<str>,
    chain_id: u64,
    environment: Environment,
    bm: Arc<dyn BlockchainManager>,
    qs: Arc<dyn AggregatorQuoteSource>,
    quote_adapter: Arc<AggregatorPriceAdapter>,

    // Caching with optimized key type
    price_cache: Cache<PriceCacheKey, f64>,
    feed_decimals_cache: Cache<Address, u8>,

    // Safety mechanisms
    recursion_guard: Arc<RecursionGuard>,
    circuit_breaker: Arc<CircuitBreaker>,
    deduplicator: Arc<RequestDeduplicator<PriceCacheKey>>,

    // Helpers
    decimal_resolver: Arc<DecimalResolver>,

    // Metrics
    metrics: Arc<OracleMetrics>,

    // Chainlink configuration
    chainlink_feeds: HashMap<Address, Address>,
    weth_address: Address,

    // Chain-specific configuration cached
    chain_config_cached: ChainConfigCache,
}

#[derive(Clone)]
struct ChainConfigCache {
    max_gas_price: U256,
    weth_address: Address,
    stablecoin_addresses: HashMap<String, Address>,
    avg_block_time_seconds: f64,
    is_test_environment: bool,
}

impl ChainConfigCache {
    fn from_config(config: &Config, chain_name: &str) -> Result<Self, PriceError> {
        let chain_config = config.chain_config.chains
            .get(chain_name)
            .ok_or_else(|| PriceError::NotAvailable(format!("Unknown chain: {}", chain_name)))?;
        
        let stablecoin_addresses = chain_config.stablecoin_symbols
            .clone()
            .unwrap_or_else(HashMap::new);
        
        Ok(Self {
            max_gas_price: chain_config.max_gas_price,
            weth_address: chain_config.weth_address,
            stablecoin_addresses,
            avg_block_time_seconds: chain_config.avg_block_time_seconds.unwrap_or(12.0),
            is_test_environment: chain_config.is_test_environment.unwrap_or(false),
        })
    }
}

impl std::fmt::Debug for LivePriceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LivePriceProvider")
            .field("chain", &self.chain)
            .field("chain_id", &self.chain_id)
            .field("environment", &self.environment)
            .field("cache_size", &self.price_cache.entry_count())
            .field("metrics", &self.metrics)
            .field("circuit_breaker_stats", &"<async>")
            .finish()
    }
}

impl LivePriceProvider {
    pub async fn new(
        chain_cfg: Arc<Config>,
        blockchain_manager: Arc<dyn BlockchainManager>,
        quote_source: Arc<dyn AggregatorQuoteSource>,
    ) -> Result<Self, PriceError> {
        let chain_name = blockchain_manager.get_chain_name();
        let chain: Arc<str> = Arc::from(chain_name.as_ref());

        // Validate chain configuration exists
        let chain_config_cached = ChainConfigCache::from_config(&chain_cfg, &chain_name)?;

        // Initialize caching and safety mechanisms
        let price_cache = Cache::builder()
            .time_to_live(Duration::from_secs(15))
            .max_capacity(1000)
            .build();

        let feed_decimals_cache = Cache::builder()
            .time_to_live(Duration::from_secs(3600)) // Cache feed decimals for 1 hour
            .max_capacity(100)
            .build();

        let recursion_guard = Arc::new(RecursionGuard::new());
        let deduplicator = Arc::new(RequestDeduplicator::<PriceCacheKey>::new());
        let decimal_resolver = Arc::new(DecimalResolver::new());
        let metrics = Arc::new(OracleMetrics::new());

        // Get chain_id from configuration - this is now the single source of truth
        let chain_id = chain_cfg.chain_config.chains
            .get(chain_name.as_ref() as &str)
            .map(|c| c.chain_id)
            .ok_or_else(|| PriceError::NotAvailable(format!("Chain ID not found for {}", chain_name)))?;
        
        let environment = if chain_config_cached.is_test_environment {
            Environment::Testing
        } else {
            Environment::from_chain_id(chain_id)
        };
        
        // Load Chainlink feeds from configuration using chain_id
        let chainlink_feeds = Self::load_chainlink_feeds(&chain_cfg, chain_id)?;
        
        // Initialize circuit breaker with appropriate thresholds
        let circuit_breaker = Arc::new(CircuitBreaker::with_advanced_config(
            5,                              // threshold: 5 consecutive failures
            Duration::from_secs(60),        // timeout: 60 seconds
            3,                              // half_open_test_count
            2,                              // half_open_success_threshold
            8,                              // max_backoff_multiplier
            0.2,                            // jitter_factor
        ));
        
        let quote_adapter = Arc::new(AggregatorPriceAdapter::new(quote_source.clone(), decimal_resolver.clone()));
        
        Ok(Self {
            chain_cfg,
            chain: chain.clone(),
            chain_id,
            environment,
            bm: blockchain_manager,
            qs: quote_source,
            quote_adapter,
            price_cache,
            feed_decimals_cache,
            recursion_guard,
            circuit_breaker,
            deduplicator,
            decimal_resolver,
            metrics,
            chainlink_feeds,
            weth_address: chain_config_cached.weth_address,
            chain_config_cached,
        })
    }
    
    fn load_chainlink_feeds(config: &Config, chain_id: u64) -> Result<HashMap<Address, Address>, PriceError> {
        let mut feeds = HashMap::new();

        // Load from configuration if available - use chain_id for lookup
        for (chain_name, chain_config) in &config.chain_config.chains {
            if chain_config.chain_id == chain_id {
                debug!("Loading Chainlink feeds for chain {} (ID: {})", chain_name, chain_id);
                if let Some(chainlink_feeds) = &chain_config.chainlink_token_feeds {
                    for (token, feed) in chainlink_feeds {
                        feeds.insert(*token, *feed);
                    }
                }
                break;
            }
        }

        // Add well-known feeds for major chains using chain_id
        if chain_id == 1 { // Ethereum Mainnet
            // ETH/USD feed
            let weth_addr: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
                .parse()
                .map_err(|e| PriceError::Validation(format!("Invalid WETH address: {}", e)))?;
            let eth_feed: Address = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
                .parse()
                .map_err(|e| PriceError::Validation(format!("Invalid ETH/USD feed address: {}", e)))?;
            feeds.insert(weth_addr, eth_feed);

            // USDC/USD feed
            let usdc_addr: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                .parse()
                .map_err(|e| PriceError::Validation(format!("Invalid USDC address: {}", e)))?;
            let usdc_feed: Address = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
                .parse()
                .map_err(|e| PriceError::Validation(format!("Invalid USDC/USD feed address: {}", e)))?;
            feeds.insert(usdc_addr, usdc_feed);
        }

        Ok(feeds)
    }
    
    fn validate_price(&self, price: f64, token: Address) -> Result<f64, PriceError> {
        if !price.is_finite() {
            return Err(PriceError::Validation(format!(
                "Non-finite price for token {:?}: {}",
                token, price
            )));
        }
        
        if price <= 0.0 {
            return Err(PriceError::Validation(format!(
                "Non-positive price for token {:?}: {}",
                token, price
            )));
        }
        
        // Environment-specific bounds
        let (min_price, max_price) = if self.environment.is_test() {
            (0.000000000000000001, 10_000_000.0) // More lenient for test environments
        } else {
            (MIN_USD_PRICE, MAX_USD_PRICE)
        };
        
        if price < min_price || price > max_price {
            return Err(PriceError::Validation(format!(
                "Price {} for token {:?} outside bounds [{}, {}]",
                price, token, min_price, max_price
            )));
        }
        
        Ok(price)
    }
    
    async fn fetch_chainlink_price_with_staleness_check(
        &self,
        feed: Address,
    ) -> Result<ChainlinkPriceData, PriceError> {
        let start = std::time::Instant::now();

        // Get feed decimals first (with caching)
        let feed_decimals = self.get_feed_decimals_cached(feed).await?;

        // Use typed ABI for safe calls
        let provider = self.bm.get_provider();
        let aggregator = ChainlinkAggregator::new(feed, provider);

        // Call latestRoundData with typed ABI
        let (
            round_id,
            price_raw,
            _started_at,
            updated_at,
            answered_in_round
        ) = aggregator.latest_round_data().call().await
            .map_err(|e| PriceError::DataSource(format!("Chainlink latestRoundData call failed: {}", e)))?;

        // Validate Chainlink response
        if answered_in_round < round_id {
            return Err(PriceError::Validation(format!(
                "Invalid Chainlink response: answeredInRound {} < roundId {}",
                answered_in_round, round_id
            )));
        }

        // Validate price is not zero
        if price_raw.is_zero() {
            return Err(PriceError::Validation("Chainlink price is zero".to_string()));
        }

        // Handle negative prices (shouldn't happen but be safe)
        if price_raw < I256::zero() {
            return Err(PriceError::Validation(format!(
                "Negative price from Chainlink: {}",
                price_raw
            )));
        }

        // Get current timestamp from blockchain for consistency
        let current_timestamp = self.get_blockchain_timestamp().await?;

        // Use dynamic staleness threshold based on block time
        let dynamic_staleness_threshold = (CHAINLINK_MAX_STALENESS_SECONDS as f64 / self.chain_config_cached.avg_block_time_seconds).ceil() as u64;

        // Convert updated_at to u64 for comparison
        let updated_at_u64 = updated_at.as_u64();

        // Check staleness using dynamic threshold (not hardcoded constant)
        if current_timestamp < updated_at_u64 {
            return Err(PriceError::Validation(format!(
                "Chainlink price is from future: updated_at {} > current_time {}",
                updated_at_u64, current_timestamp
            )));
        }

        let age_seconds = current_timestamp.saturating_sub(updated_at_u64);
        if age_seconds > dynamic_staleness_threshold {
            warn!(
                "Chainlink feed {:?} is stale: last updated {} seconds ago (chain avg block time: {}s, threshold: {}s)",
                feed,
                age_seconds,
                self.chain_config_cached.avg_block_time_seconds,
                dynamic_staleness_threshold
            );
            return Err(PriceError::Validation(format!(
                "Chainlink price is stale ({}s old, max: {}s for chain with {}s block time)",
                age_seconds,
                dynamic_staleness_threshold,
                self.chain_config_cached.avg_block_time_seconds
            )));
        }

        // Convert price using actual feed decimals (not hardcoded 8)
        let price_u256 = price_raw.into_raw();
        let price = safe_u256_to_f64(price_u256, feed_decimals)?;

        let chainlink_data = ChainlinkPriceData {
            price,
            timestamp: updated_at_u64,
            round_id: round_id as u64,
        };

        // Validate the data
        chainlink_data.validate()?;

        // Record metrics
        self.metrics.record_api_call();
        self.metrics.record_latency(start.elapsed().as_millis() as u64);

        Ok(chainlink_data)
    }
    

    
    async fn raw_call(&self, to: Address, data: Bytes) -> Result<Vec<u8>, PriceError> {
        let provider = self.bm.get_provider();
        let tx = ethers::types::TransactionRequest::new()
            .to(to)
            .data(data);
        
        provider.call(&tx.into(), None).await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| PriceError::DataSource(format!("RPC call failed: {}", e)))
    }
    
    async fn fetch_weth_price_usd(&self) -> Result<f64, PriceError> {
        let weth = self.weth_address;
        
        // Prevent recursion using guard - guard is automatically dropped at end of function
        let guard = self.recursion_guard.enter(weth)?;
        
        // Check cache first with optimized key
        let cache_key = PriceCacheKey::new(self.chain.clone(), weth);
        if let Some(price) = self.price_cache.get(&cache_key).await {
            self.metrics.record_cache_hit();
            return Ok(price);
        }
        self.metrics.record_cache_miss();
        
        // Use deduplicator to prevent concurrent identical requests
        let price = self.deduplicator.deduplicate(
            cache_key.clone(),
            || async {
                std::hint::black_box(&guard);
                // Try Chainlink first if available
                if let Some(feed) = self.chainlink_feeds.get(&weth) {
                    match self.fetch_chainlink_price_with_staleness_check(*feed).await {
                        Ok(data) => {
                            let validated_price = self.validate_price(data.price, weth)?;
                            return Ok(validated_price);
                        }
                        Err(e) => {
                            warn!("Chainlink feed failed for WETH: {}", e);
                        }
                    }
                }
                
                // Fallback to quote source via adapter
                let stablecoin_addresses: Vec<Address> = self.chain_config_cached.stablecoin_addresses
                    .values()
                    .cloned()
                    .collect();
                
                // First try adapter, then try direct quote source if needed
                match self.quote_adapter.estimate_token_price_usd(
                    self.chain_id,
                    weth,
                    &stablecoin_addresses,
                ).await {
                    Ok(price) => self.validate_price(price, weth),
                    Err(adapter_err) => {
                        // Try direct quote source as final fallback
                        debug!("Adapter failed, trying direct quote source: {}", adapter_err);
                        
                        // Use the direct quote source for additional price discovery if available
                        // Select best available stablecoin (prefer USDC, then USDT, then first available)
                        let stablecoin_address = self.select_best_stablecoin()?;
                        let stablecoin_decimals = self.get_token_decimals_cached(stablecoin_address).await;

                        match self.qs.get_rate_estimate(
                            self.chain_id,
                            weth,
                            stablecoin_address,
                            U256::from(10).pow(U256::from(18)), // 1 WETH
                        ).await {
                            Ok(rate_estimate) => {
                                let price = safe_u256_to_f64(rate_estimate.amount_out, stablecoin_decimals)?;
                                self.validate_price(price, weth)
                            }
                            Err(qs_err) => {
                                // Only use fallback in test environments
                                if self.environment.is_test() {
                                    warn!(
                                        "Using fallback WETH price for testing (adapter: {}, quote source: {})",
                                        adapter_err, qs_err
                                    );
                                    Ok(2000.0) // Test environment fallback
                                } else {
                                    // In production, fail if we can't get a real price
                                    Err(PriceError::NotAvailable(format!(
                                        "Failed to fetch WETH price: Chainlink unavailable, adapter error: {}, quote source error: {}",
                                        adapter_err, qs_err
                                    )))
                                }
                            }
                        }
                    }
                }
            }
        ).await?;
        
        // Cache the result
        self.price_cache.insert(cache_key, price).await;
        Ok(price)
    }
    
    async fn get_feed_decimals_cached(&self, feed: Address) -> Result<u8, PriceError> {
        // Check cache first
        if let Some(decimals) = self.feed_decimals_cache.get(&feed).await {
            return Ok(decimals);
        }

        // Fetch from blockchain using typed ABI
        let provider = self.bm.get_provider();
        let aggregator = ChainlinkAggregator::new(feed, provider);

        let decimals = aggregator.decimals().call().await
            .map_err(|e| PriceError::DataSource(format!("Failed to fetch feed decimals: {}", e)))?;

        // Cache the result
        self.feed_decimals_cache.insert(feed, decimals).await;

        Ok(decimals)
    }

    async fn get_blockchain_timestamp(&self) -> Result<u64, PriceError> {
        let provider = self.bm.get_provider();

        // Get latest block timestamp to avoid local clock skew
        let block = provider.get_block(ethers::types::BlockId::Number(
            ethers::types::BlockNumber::Latest
        )).await
            .map_err(|e| PriceError::DataSource(format!("Failed to get latest block: {}", e)))?
            .ok_or_else(|| PriceError::DataSource("No latest block returned".to_string()))?;

        Ok(block.timestamp.as_u64())
    }

    fn select_best_stablecoin(&self) -> Result<Address, PriceError> {
        // Priority order: USDC, USDT, DAI, then first available
        let priorities = ["USDC", "USDT", "DAI"];

        for symbol in &priorities {
            if let Some(addr) = self.chain_config_cached.stablecoin_addresses.get(*symbol) {
                return Ok(*addr);
            }
        }

        // If no priority stablecoin found, take the first available
        self.chain_config_cached.stablecoin_addresses
            .values()
            .next()
            .copied()
            .ok_or_else(|| PriceError::NotAvailable(
                "No stablecoin addresses configured for this chain".to_string()
            ))
    }

    async fn get_token_decimals_cached(&self, token: Address) -> u8 {
        self.decimal_resolver.get_decimals_with_fallback(
            token,
            || async {
                // Try to fetch from blockchain
                // Correct ABI call data: just the 4-byte function selector
                let selector = &ethers::utils::id("decimals()")[..4];
                let data = Bytes::from(selector.to_vec());

                match self.raw_call(token, data).await {
                    Ok(result) if !result.is_empty() => {
                        if let Ok(tokens) = ethers::abi::decode(&[ethers::abi::ParamType::Uint(8)], &result) {
                            if let Some(decimals) = tokens[0].clone().into_uint() {
                                return Ok(decimals.as_u32() as u8);
                            }
                        }
                    }
                    _ => {}
                }

                Err(PriceError::DataSource("Failed to fetch decimals".to_string()))
            }
        ).await
    }
}

#[async_trait]
impl crate::price_oracle::PriceOracle for LivePriceProvider {
    async fn get_token_price_usd(
        &self,
        chain_name: &str,
        token: Address,
    ) -> Result<f64, PriceError> {
        // Validate chain name
        if chain_name != self.chain.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "Provider configured for {} but requested {}",
                self.chain, chain_name
            )));
        }
        
        // Check circuit breaker
        if self.circuit_breaker.is_tripped().await {
            self.metrics.record_error();
            return Err(PriceError::ServiceUnavailable(
                "Price oracle circuit breaker is open".to_string()
            ));
        }
        
        let start = std::time::Instant::now();
        
        // Special handling for WETH
        if token == self.weth_address {
            let result = self.fetch_weth_price_usd().await;
            self.handle_circuit_breaker_result(&result).await;
            
            // Record metrics before returning
            match &result {
                Ok(_) => {
                    self.metrics.record_latency(start.elapsed().as_millis() as u64);
                }
                Err(_) => {
                    self.metrics.record_error();
                }
            }
            
            return result;
        }
        
        // Check cache with optimized key
        let cache_key = PriceCacheKey::new(self.chain.clone(), token);
        if let Some(price) = self.price_cache.get(&cache_key).await {
            self.metrics.record_cache_hit();
            self.metrics.record_latency(start.elapsed().as_millis() as u64);
            return Ok(price);
        }
        self.metrics.record_cache_miss();
        
        // Use deduplicator to prevent concurrent identical requests
        let result = self.deduplicator.deduplicate(
            cache_key.clone(),
            || async {
                // 1. Try Chainlink if available
                if let Some(feed) = self.chainlink_feeds.get(&token) {
                    match self.fetch_chainlink_price_with_staleness_check(*feed).await {
                        Ok(data) => {
                            return self.validate_price(data.price, token);
                        }
                        Err(e) => {
                            debug!("Chainlink failed for token {:?}: {}", token, e);
                        }
                    }
                }
                
                // 2. Try quote aggregator via adapter
                let stablecoin_addresses: Vec<Address> = self.chain_config_cached.stablecoin_addresses
                    .values()
                    .cloned()
                    .collect();
                
                if !stablecoin_addresses.is_empty() {
                    match self.quote_adapter.estimate_token_price_usd(
                        self.chain_id,
                        token,
                        &stablecoin_addresses,
                    ).await {
                        Ok(price) => {
                            return self.validate_price(price, token);
                        }
                        Err(e) => {
                            debug!("Quote source failed for token {:?}: {}", token, e);
                        }
                    }
                }
                
                // 3. Check if it's a known stablecoin
                for (symbol, addr) in &self.chain_config_cached.stablecoin_addresses {
                    if *addr == token {
                        // In production, require live quote to confirm peg within tight band
                        if !self.environment.is_test() {
                            // Try to get a live quote from Chainlink or adapter to verify peg
                            if let Some(feed) = self.chainlink_feeds.get(&token) {
                                match self.fetch_chainlink_price_with_staleness_check(*feed).await {
                                    Ok(data) => {
                                        let deviation = (data.price - 1.0).abs();
                                        if deviation <= 0.015 { // ±1.5% peg tolerance
                                            self.metrics.record_peg_assumption_used();
                                            info!("Using verified stablecoin peg for {}: ${:.6}", symbol, data.price);
                                            return Ok(data.price);
                                        } else {
                                            warn!("Stablecoin {} depegged: ${:.6} (deviation: {:.2}%)", symbol, data.price, deviation * 100.0);
                                            // Continue to try other methods instead of assuming 1.0
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Could not verify {} peg from Chainlink: {}", symbol, e);
                                        // Continue to try other methods
                                    }
                                }
                            }
                        } else {
                            // In test environments, allow the assumption
                            info!("Using stablecoin assumption for {} in test environment", symbol);
                            return Ok(1.0);
                        }
                    }
                }
                
                Err(PriceError::NotAvailable(format!(
                    "No price available for token {:?}",
                    token
                )))
            }
        ).await;
        
        // Handle circuit breaker
        self.handle_circuit_breaker_result(&result).await;
        
        // Cache successful results
        if let Ok(price) = result {
            self.price_cache.insert(cache_key, price).await;
            self.metrics.record_latency(start.elapsed().as_millis() as u64);
            Ok(price)
        } else {
            self.metrics.record_error();
            result
        }
    }
    

    
    async fn get_pair_price_ratio(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        let price_a = self.get_token_price_usd(chain_name, token_a).await?;
        let price_b = self.get_token_price_usd(chain_name, token_b).await?;
        
        if price_b == 0.0 {
            return Err(PriceError::Calculation(format!(
                "Token B ({:?}) has zero price",
                token_b
            )));
        }
        
        Ok(price_a / price_b)
    }
    

    

    
    async fn calculate_liquidity_usd(&self, pool: &PoolInfo) -> Result<f64, PriceError> {
        let p0 = self.get_token_price_usd(&self.chain, pool.token0).await?;
        let p1 = self.get_token_price_usd(&self.chain, pool.token1).await?;
        
        let l0 = crate::types::normalize_units(pool.reserves0, pool.token0_decimals) * p0;
        let l1 = crate::types::normalize_units(pool.reserves1, pool.token1_decimals) * p1;
        
        Ok(l0 + l1)
    }
    
    
    

    
    async fn get_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }
    
    async fn convert_token_to_usd(
        &self,
        chain_name: &str,
        amount: U256,
        token: Address,
    ) -> Result<f64, PriceError> {
        let price = self.get_token_price_usd(chain_name, token).await?;
        let decimals = self.get_token_decimals_cached(token).await;
        let normalized_amount = safe_u256_to_f64(amount, decimals)?;
        Ok(normalized_amount * price)
    }
    
    async fn convert_native_to_usd(
        &self,
        chain_name: &str,
        amount_native: U256,
    ) -> Result<f64, PriceError> {
        let weth_price = self.get_token_price_usd(chain_name, self.weth_address).await?;
        let normalized_amount = safe_u256_to_f64(amount_native, 18)?;
        Ok(normalized_amount * weth_price)
    }
    
    async fn estimate_token_conversion(
        &self,
        chain_name: &str,
        amount_in: U256,
        token_in: Address,
        token_out: Address,
    ) -> Result<U256, PriceError> {
        let price_in = self.get_token_price_usd(chain_name, token_in).await?;
        let price_out = self.get_token_price_usd(chain_name, token_out).await?;
        
        if price_out == 0.0 {
            return Err(PriceError::Calculation("Output token price is zero".to_string()));
        }
        
        let decimals_in = self.get_token_decimals_cached(token_in).await;
        let decimals_out = self.get_token_decimals_cached(token_out).await;
        
        let normalized_amount_in = safe_u256_to_f64(amount_in, decimals_in)?;
        let usd_value = normalized_amount_in * price_in;
        let normalized_amount_out = usd_value / price_out;
        
        safe_f64_to_u256(normalized_amount_out, decimals_out)
    }
    
    async fn estimate_swap_value_usd(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Result<f64, PriceError> {
        let amount_out = self.estimate_token_conversion(chain_name, amount_in, token_in, token_out).await?;
        self.convert_token_to_usd(chain_name, amount_out, token_out).await
    }

    async fn get_token_price_usd_at(
        &self,
        _chain_name: &str,
        _token: Address,
        _block_number: Option<u64>,
        _unix_ts: Option<u64>,
    ) -> Result<f64, PriceError> {
        Err(PriceError::NotAvailable("LivePriceProvider does not support historical price queries".to_string()))
    }

    async fn get_historical_prices_batch(
        &self,
        _chain_name: &str,
        _tokens: &[Address],
        _block_number: u64,
    ) -> Result<HashMap<Address, f64>, PriceError> {
        Err(PriceError::NotAvailable("LivePriceProvider does not support historical price queries".to_string()))
    }

    async fn get_token_volatility(
        &self,
        _chain_name: &str,
        _token: Address,
        _timeframe_seconds: u64,
    ) -> Result<f64, PriceError> {
        Err(PriceError::NotAvailable("LivePriceProvider does not support volatility calculations".to_string()))
    }

    async fn get_pair_price_at_block(
        &self,
        _chain_name: &str,
        _token_a: Address,
        _token_b: Address,
        _block_number: u64,
    ) -> Result<f64, PriceError> {
        Err(PriceError::NotAvailable("LivePriceProvider does not support historical price queries".to_string()))
    }
}

impl LivePriceProvider {
    async fn handle_circuit_breaker_result<T>(&self, result: &Result<T, PriceError>) {
        match result {
            Ok(_) => {
                self.circuit_breaker.record_success().await;
            }
            Err(_) => {
                self.circuit_breaker.record_failure().await;
            }
        }
    }
    
    /// Validate gas price against configured maximum
    pub async fn validate_gas_price(&self, gas_price: U256) -> Result<(), PriceError> {
        if gas_price > self.chain_config_cached.max_gas_price {
            return Err(PriceError::Validation(format!(
                "Gas price {} exceeds maximum configured limit of {}",
                gas_price, self.chain_config_cached.max_gas_price
            )));
        }
        Ok(())
    }
    
    /// Get estimated blocks until a target timestamp
    pub fn estimate_blocks_until(&self, target_timestamp: u64) -> u64 {
        let current_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        if target_timestamp <= current_timestamp {
            return 0;
        }
        
        let seconds_until = target_timestamp - current_timestamp;
        (seconds_until as f64 / self.chain_config_cached.avg_block_time_seconds).ceil() as u64
    }
    
    /// Get chain-specific configuration value
    pub fn get_chain_config_value<T>(&self, key: &str) -> Option<T> 
    where
        T: serde::de::DeserializeOwned,
    {
        // Access chain-specific configuration through chain_cfg
        self.chain_cfg.chain_config.chains
            .get(self.chain.as_ref())
            .and_then(|chain_config| {
                // Convert the chain config to a JSON value for dynamic field access
                serde_json::to_value(chain_config).ok()
                    .and_then(|value| {
                        // Access the field by key
                        value.get(key)
                            .and_then(|field_value| {
                                // Deserialize the field value to the requested type
                                serde_json::from_value::<T>(field_value.clone()).ok()
                            })
                    })
            })
    }
    
    pub async fn get_metrics(&self) -> OracleMetrics {
        (*self.metrics).clone()
    }
    
    pub async fn get_circuit_breaker_stats(&self) -> CircuitBreakerStats {
        self.circuit_breaker.get_stats().await
    }
    
    pub async fn force_reset_circuit_breaker(&self) {
        self.circuit_breaker.force_reset().await;
    }
    
    /// Check if a given block is too old based on chain's average block time
    pub fn is_block_stale(&self, block_number: u64, current_block: u64, max_age_seconds: u64) -> bool {
        if block_number >= current_block {
            return false;
        }
        
        let blocks_behind = current_block - block_number;
        let estimated_age_seconds = blocks_behind as f64 * self.chain_config_cached.avg_block_time_seconds;
        
        estimated_age_seconds > max_age_seconds as f64
    }
    
    /// Get configuration for a specific token if available
    pub fn get_token_config(&self, token: Address) -> Option<TokenConfig> {
        // Check if token has specific configuration in chain_cfg
        self.chain_cfg.chain_config.chains
            .get(self.chain.as_ref())
            .and_then(|chain_config| {
                // Look for token-specific configurations
                chain_config.chainlink_token_feeds
                    .as_ref()
                    .and_then(|feeds| feeds.get(&token))
                    .map(|feed_address| TokenConfig {
                        chainlink_feed: Some(*feed_address),
                        decimals: None, // Would need to be fetched
                        is_stablecoin: self.chain_config_cached.stablecoin_addresses
                            .values()
                            .any(|addr| *addr == token),
                    })
            })
    }
}

/// Token-specific configuration
#[derive(Debug, Clone)]
pub struct TokenConfig {
    pub chainlink_feed: Option<Address>,
    pub decimals: Option<u8>,
    pub is_stablecoin: bool,
}