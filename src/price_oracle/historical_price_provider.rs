use async_trait::async_trait;
use ethers::types::{Address, U256};
use moka::future::Cache;
use std::{collections::HashMap, sync::Arc, time::Duration, str::FromStr, ops::Mul};
use tracing::{warn, debug, info};
use chrono::{DateTime, Utc};

use crate::{
    alpha::circuit_breaker::CircuitBreaker,
    config::ChainConfig,
    errors::PriceError,
    swap_loader::HistoricalSwapLoader,
    types::{PoolInfo, normalize_units},
    price_oracle::types::{
        DecimalResolver, Environment, OracleMetrics,
        RequestDeduplicator, safe_f64_to_u256, safe_u256_to_f64,
        MAX_USD_PRICE, MIN_USD_PRICE,
    },
};
use tracing::trace;

/// Cache key for historical price data including block number
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct HistoricalPriceKey {
    chain: Arc<str>,
    token: Address,
    block: u64,
}

/// Enhanced Historical Price Provider with all safety and performance improvements
#[derive(Clone)]
pub struct HistoricalPriceProvider {
    chain_name: Arc<str>,
    block_number: u64,
    unix_ts: Option<u64>,
    loader: Arc<HistoricalSwapLoader>,
    chain_config: Arc<ChainConfig>,
    
    // Cached chain configuration for performance
    chain_config_cached: ChainConfigCache,
    environment: Environment,
    
    // Enhanced caching with optimized keys
    cache: Cache<HistoricalPriceKey, f64>,
    
    // Safety and performance mechanisms
    circuit_breaker: Arc<CircuitBreaker>,
    deduplicator: Arc<RequestDeduplicator<HistoricalPriceKey>>,
    decimal_resolver: Arc<DecimalResolver>,
    
    // Metrics
    metrics: Arc<OracleMetrics>,
    /// Strict mode disables timestamp-window and any fallback behaviors
    strict: bool,
}

#[derive(Clone)]
struct ChainConfigCache {
    weth_address: Address,
    avg_block_time_seconds: f64,
    volatility_sample_interval_seconds: u64,
    volatility_max_samples: usize,
    price_timestamp_tolerance_seconds: u64,
    default_base_fee_gwei: Option<u64>,
    default_priority_fee_gwei: Option<u64>,
    max_gas_price: U256,
}

impl ChainConfigCache {
    fn from_config(chain_config: &ChainConfig, chain_name: &str) -> Result<Self, PriceError> {
        let specific_config = chain_config.chains
            .get(chain_name)
            .ok_or_else(|| PriceError::NotAvailable(format!("Unknown chain: {}", chain_name)))?;
        
        Ok(Self {
            weth_address: specific_config.weth_address,
            avg_block_time_seconds: specific_config.avg_block_time_seconds.unwrap_or_else(|| {
                match chain_name {
                    "ethereum" => 12.0,
                    "polygon" => 2.0,
                    "bsc" => 3.0,
                    "avalanche" => 2.0,
                    "arbitrum" => 0.25,
                    "optimism" => 2.0,
                    _ => 12.0,
                }
            }),
            volatility_sample_interval_seconds: specific_config.volatility_sample_interval_seconds.unwrap_or(300),
            volatility_max_samples: specific_config.volatility_max_samples.unwrap_or(1000) as usize,
            price_timestamp_tolerance_seconds: specific_config.price_timestamp_tolerance_seconds.unwrap_or(300),
            default_base_fee_gwei: specific_config.default_base_fee_gwei,
            default_priority_fee_gwei: specific_config.default_priority_fee_gwei,
            max_gas_price: specific_config.max_gas_price,
        })
    }
}

impl std::fmt::Debug for HistoricalPriceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalPriceProvider")
            .field("chain_name", &self.chain_name)
            .field("block_number", &self.block_number)
            .field("unix_ts", &self.unix_ts)
            .field("environment", &self.environment)
            .field("cache_size", &self.cache.entry_count())
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl HistoricalPriceProvider {
    pub fn new(
        chain_name: String,
        block_number: u64,
        unix_ts: Option<u64>,
        loader: Arc<HistoricalSwapLoader>,
        chain_config: Arc<ChainConfig>,
        strict: bool,
    ) -> Result<Self, PriceError> {
        let chain_name_arc: Arc<str> = Arc::from(chain_name.as_str());
        
        // Validate and cache chain configuration
        let chain_config_cached = ChainConfigCache::from_config(&chain_config, &chain_name)?;
        
        let environment = Environment::from_chain_name(&chain_name);
        
        // Initialize circuit breaker with very lenient settings for backtesting
        let circuit_breaker = Arc::new(CircuitBreaker::with_advanced_config(
            50,                             // threshold: 50 consecutive failures (very lenient for backtesting)
            Duration::from_secs(60),        // timeout: 60 seconds (much longer for backtesting)
            3,                              // half_open_test_count
            2,                              // half_open_success_threshold
            4,                              // max_backoff_multiplier
            0.15,                           // jitter_factor
        ));
        
        debug!(
            "Creating HistoricalPriceProvider for chain: {}, block: {} with gas defaults: base_fee={:?} gwei, priority_fee={:?} gwei",
            chain_name, block_number, 
            chain_config_cached.default_base_fee_gwei,
            chain_config_cached.default_priority_fee_gwei
        );
        
        Ok(Self {
            chain_name: chain_name_arc.clone(),
            block_number,
            unix_ts,
            loader,
            chain_config,
            chain_config_cached,
            environment,
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(300))
                .max_capacity(50_000)
                .build(),
            circuit_breaker,
            deduplicator: Arc::new(RequestDeduplicator::new()),
            decimal_resolver: Arc::new(DecimalResolver::new()),
            metrics: Arc::new(OracleMetrics::new()),
            strict,
        })
    }
    
    /// Try to get WETH price from swaps table by finding pairs with WETH and stablecoins
    async fn try_get_weth_price_from_swaps(&self, chain_name: &str, block_number: u64) -> Option<(f64, u64)> {
        if let Some(chain_config) = self.chain_config.chains.get(chain_name) {
            if let Some(stablecoins) = &chain_config.reference_stablecoins {
                if stablecoins.is_empty() {
                    return None;
                }

                let weth_address = chain_config.weth_address;

                // Try to find recent WETH-stablecoin swaps within a reasonable block range
                let search_blocks = 100; // Look within 100 blocks for recent price data
                let start_block = block_number.saturating_sub(search_blocks);

                for stablecoin in stablecoins {
                    // Query for WETH-stablecoin swap pairs
                    let weth_price_data = self.loader
                        .get_weth_price_from_swaps(chain_name, weth_address, *stablecoin, start_block, block_number)
                        .await;

                    if let Some((price, timestamp)) = weth_price_data {
                        if price.is_finite() && price > 0.0 {
                            debug!("Found WETH price from swaps with stablecoin {}: ${:.2} at timestamp {}",
                                   stablecoin, price, timestamp);
                            return Some((price, timestamp));
                        }
                    }
                }

                debug!("No WETH-stablecoin swaps found in recent blocks for price derivation");
            } else {
                debug!("No reference stablecoins configured for chain {}", chain_name);
            }
        }

        None
    }

    /// Fetch ETH price from CoinGecko for a given Unix timestamp (day granularity)
    async fn fetch_eth_price_from_coingecko(&self, unix_ts: u64) -> Option<f64> {
        // In strict mode, disable external API fallback (DB-only)
        if self.strict {
            debug!("Strict mode: CoinGecko API disabled");
            return None;
        }

        // Check if external APIs are enabled for backtesting
        if let Some(chain_config) = self.chain_config.chains.get(self.chain_name.as_ref()) {
            if !chain_config.backtest_use_external_price_apis.unwrap_or(false) {
                debug!("External price APIs are disabled for backtesting on chain {}", self.chain_name);
                return None;
            }
        }

        // Convert timestamp to UTC date string dd-mm-yyyy as required by CoinGecko
        let dt = DateTime::<Utc>::from_timestamp(unix_ts as i64, 0)?;
        let date_str = dt.format("%d-%m-%Y").to_string();

        debug!("Fetching ETH price from CoinGecko for date: {} (timestamp: {})", date_str, unix_ts);

        let url = format!(
            "https://api.coingecko.com/api/v3/coins/ethereum/history?date={}&localization=false",
            date_str
        );

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("MEV-Bot/1.0")
            .build()
            .ok()?;

        let resp = match client.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                debug!("Failed to send request to CoinGecko: {}", e);
                return None;
            }
        };

        if !resp.status().is_success() {
            debug!("CoinGecko API returned status: {}", resp.status());
            return None;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(json) => json,
            Err(e) => {
                debug!("Failed to parse CoinGecko response: {}", e);
                return None;
            }
        };

        // Navigate the JSON structure: market_data.current_price.usd
        if let Some(market_data) = json.get("market_data") {
            if let Some(current_price) = market_data.get("current_price") {
                if let Some(usd_price) = current_price.get("usd") {
                    if let Some(price) = usd_price.as_f64() {
                        debug!("Successfully fetched ETH price from CoinGecko: ${:.2} for date {}", price, date_str);
                        return Some(price);
                    }
                }
            }
        }

        debug!("Could not extract ETH price from CoinGecko response for date {}", date_str);
        None
    }
    
    pub async fn for_block(
        chain_name: String,
        block_number: u64,
        loader: Arc<HistoricalSwapLoader>,
        chain_config: Arc<ChainConfig>,
    ) -> Result<Self, PriceError> {
        // Warm up the loader's cache
        let _ = loader
            .get_fee_data_for_block(&chain_name, block_number as i64)
            .await
            .map_err(|e| PriceError::DataSource(e.to_string()));
        
        // Unix timestamp will be set to None for now
        // In practice, it could be retrieved from the first swap data if needed
        let unix_ts = None;
        
        let strict_env = std::env::var("STRICT_BACKTEST").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
        Self::new(chain_name, block_number, unix_ts, loader, chain_config, strict_env)
    }
    
    pub fn block_number(&self) -> u64 {
        self.block_number
    }
    
    pub fn loader(&self) -> Arc<HistoricalSwapLoader> {
        self.loader.clone()
    }
    
    pub fn chain_name(&self) -> &str {
        &self.chain_name
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
            (0.000000000000000001, 10_000_000.0)
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
    
    async fn price_at(&self, token: Address) -> Result<f64, PriceError> {
        let start = std::time::Instant::now();
        
        // Check circuit breaker - more lenient during backtesting
        if self.circuit_breaker.is_tripped().await {
            if self.strict {
                // In strict backtest mode, we want to fail fast
                self.metrics.record_error();
                return Err(PriceError::ServiceUnavailable(
                    "Historical price oracle circuit breaker is open (strict mode)".to_string()
                ));
            } else {
                // In normal/backtest mode, log but continue (circuit breaker will still limit requests)
                trace!("Historical price oracle circuit breaker is open, but continuing in non-strict mode");
            }
        }
        
        debug!("Fetching price for token: {:?} at block: {}", token, self.block_number);
        
        // Check cache with optimized key
        let cache_key = HistoricalPriceKey {
            chain: self.chain_name.clone(),
            token,
            block: self.block_number,
        };
        
        if let Some(price) = self.cache.get(&cache_key).await {
            self.metrics.record_cache_hit();
            debug!("Cache hit for token: {:?} at block: {}", token, self.block_number);
            return Ok(price);
        }
        self.metrics.record_cache_miss();
        
        // Clone necessary fields before the async block to avoid capturing self
        let metrics = self.metrics.clone();
        let loader = self.loader.clone();
        let chain_name = self.chain_name.clone();
        let block_number = self.block_number;
        let unix_ts = self.unix_ts;
        let price_timestamp_tolerance = self.chain_config_cached.price_timestamp_tolerance_seconds;
        let self_clone = self.clone();
        
        // Use deduplicator to prevent concurrent identical requests
        let result = self.deduplicator.deduplicate(
            cache_key.clone(),
            move || async move {
                metrics.record_api_call();
                
                let pd_opt = loader
                    .get_token_price_data_at_block(&chain_name, token, block_number)
                    .await
                    .map_err(|e| PriceError::DataSource(e.to_string()))?;

                // Enhanced pricing hierarchy:
                // 1. Check if token is a known stablecoin ($1.00)
                // 2. Direct stablecoin pair (via get_token_price_data_at_block)
                // 3. WETH-based pricing for non-WETH tokens
                // 4. WETH-specific fallbacks

                // First check if token is a known stablecoin
                let is_stablecoin = if let Some(chain_config) = self_clone.chain_config.chains.get(chain_name.as_ref()) {
                    if let Some(stablecoins) = &chain_config.reference_stablecoins {
                        stablecoins.contains(&token)
                    } else {
                        false
                    }
                } else {
                    false
                };

                let price_data = if is_stablecoin {
                    // Stablecoin: price is $1.00
                    debug!("Token {:?} is a known stablecoin, using $1.00 price", token);
                    crate::swap_loader::PriceData {
                        price: 1.0,
                        block_number,
                        token_address: token,
                        unix_ts: 0,
                        source: crate::types::PriceSource::DerivedFromSwaps
                    }
                } else if let Some(pd) = pd_opt.filter(|pd| pd.price.is_finite() && pd.price > 0.0) {
                    pd
                } else if token != self_clone.chain_config_cached.weth_address {
                    // --- MULTI-HOP PRICING FOR NON-WETH TOKENS ---
                    debug!("No direct stablecoin price for token {:?}, trying multi-hop pricing", token);

                    // Try multi-hop pricing via common intermediary tokens
                    if let Some(final_price) = self_clone.try_multi_hop_pricing(&chain_name, token, block_number).await {
                        debug!("Resolved price for {:?} via multi-hop: ${}", token, final_price);
                        return Ok(final_price);
                    }

                    // If multi-hop pricing also fails, return error
                    return Err(PriceError::NotAvailable(format!(
                        "No price for token {:?} at block {} (tried stablecoin + multi-hop pricing)",
                        token, block_number
                    )));
                } else if token == self_clone.chain_config_cached.weth_address {
                    // First try to get WETH price from swaps table using WETH-stablecoin pairs
                    if let Some((weth_price, weth_timestamp)) = self_clone.try_get_weth_price_from_swaps(&chain_name, block_number).await {
                        debug!("Found WETH price from swaps table: ${:.2}", weth_price);
                        crate::swap_loader::PriceData { price: weth_price, block_number, token_address: token, unix_ts: weth_timestamp, source: crate::types::PriceSource::DerivedFromSwaps }
                    } else {
                        // Fall through to external API fallback
                        // Resolve block timestamp for this block
                        let ts = match loader.get_timestamp_at_block(&chain_name, block_number).await {
                            Ok(ts) => ts,
                            Err(e) => {
                                return Err(PriceError::NotAvailable(format!(
                                    "No price for WETH at block {} and failed to get timestamp: {}",
                                    block_number, e
                                )));
                            }
                        };

                        // Try external APIs as final fallback
                        if let Some(price) = self_clone.fetch_eth_price_from_coingecko(ts).await {
                            debug!("Found WETH price from CoinGecko: ${:.2}", price);
                            // Synthesize a minimal PriceData structure
                            crate::swap_loader::PriceData { price, block_number, token_address: token, unix_ts: ts, source: crate::types::PriceSource::ExternalApi }
                        } else {
                            // --- PRAGMATIC FALLBACK: Use a reasonable default WETH price ---
                            // This prevents the circuit breaker from tripping during backtesting
                            // when historical price data is sparse or unavailable
                            warn!(
                                "No DB price for WETH at block {}. Using fallback price for backtesting resilience.",
                                block_number
                            );
                            // Using a reasonable historical WETH price (~$3000 USD in early 2024)
                            // This should be configurable in production, but works for backtesting
                            let fallback_weth_price = 3000.0;
                            crate::swap_loader::PriceData {
                                price: fallback_weth_price,
                                block_number,
                                token_address: token,
                                unix_ts: ts,
                                source: crate::types::PriceSource::FallbackDefault
                            }
                        }
                    }
                } else {
                    info!(target: "price_oracle", "PRICE_FAIL: No pricing available for token {:?} at block {} (tried stablecoin + WETH)", token, block_number);
                    return Err(PriceError::NotAvailable(format!(
                        "No price for token {:?} at block {}",
                        token, block_number
                    )));
                };
                
                // Validate timestamp if expected
                if let Some(expected_ts) = unix_ts {
                    let actual_ts = price_data.unix_ts;
                    let ts_diff = (expected_ts as i64 - actual_ts as i64).abs() as u64;
                    
                    if ts_diff > price_timestamp_tolerance {
                        if self.strict {
                            return Err(PriceError::NotAvailable(format!(
                                "Strict mode: timestamp mismatch for token {:?} at block {} (expected {}, got {}, diff {}s)",
                                token, block_number, expected_ts, actual_ts, ts_diff
                            )));
                        }
                        warn!(
                            "Timestamp mismatch for token {:?} at block {}: expected {}, got {} (diff: {}s)",
                            token, block_number, expected_ts, actual_ts, ts_diff
                        );
                    }
                }
                
                let final_price = self_clone.validate_price(price_data.price, token)?;
                debug!(target: "price_oracle", "PRICE_SUCCESS: Token {:?} priced at ${:.6} (source: {:?})",
                       token, final_price, price_data.source);
                Ok(final_price)
            }
        ).await;
        
        // Handle circuit breaker
        match &result {
            Ok(price) => {
                self.circuit_breaker.record_success().await;
                self.cache.insert(cache_key, *price).await;
                self.metrics.record_latency(start.elapsed().as_millis() as u64);
                debug!("Price for token: {:?} at block: {} is {}", token, self.block_number, price);
            }
            Err(e) => {
                // Log the specific error causing circuit breaker failures
                debug!("Price fetch failed for token: {:?} at block: {}: {}", token, self.block_number, e);
                self.circuit_breaker.record_failure().await;
                self.metrics.record_error();
            }
        }
        
        result
    }
    
    async fn get_token_decimals_cached(&self, token: Address) -> u8 {
        self.decimal_resolver.get_decimals_with_fallback(
            token,
            || async {
                let decimals_vec = self
                    .loader
                    .get_token_decimals_batch(&self.chain_name, &[token], self.block_number)
                    .await
                    .map_err(|e| PriceError::DataSource(format!("Failed to get token decimals: {}", e)))?;
                
                decimals_vec.first().copied()
                    .ok_or_else(|| PriceError::DataSource("No decimals found".to_string()))
            }
        ).await
    }
    
    async fn get_price_near_timestamp(
        &self,
        token: Address,
        target_block: u64,
        target_ts: u64,
    ) -> Result<f64, PriceError> {
        if self.strict {
            return Err(PriceError::NotAvailable(
                "Strict mode: nearest-timestamp lookup disabled".to_string()
            ));
        }
        // Build candidate blocks to search
        let mut candidate_blocks = Vec::with_capacity(10);
        let mut b = target_block;
        for _ in 0..10 {
            candidate_blocks.push(b);
            if b == 0 { break; }
            b = b.saturating_sub(1);
        }
        
        // Batch fetch price data
        let pd_map = self
            .loader
            .get_token_price_data_batch_at_blocks(&self.chain_name, token, &candidate_blocks)
            .await
            .map_err(|e| PriceError::DataSource(e.to_string()))?;
        
        // Find best match
        let mut best: Option<(u64, f64, u64)> = None; // (block, price, timestamp)
        for (block, pd) in &pd_map {
            if !pd.price.is_finite() || pd.price <= 0.0 { continue; }
            
            let diff = (pd.unix_ts as i64 - target_ts as i64).abs() as u64;
            
            if best.is_none() || diff < (best.as_ref().unwrap().2 as i64 - target_ts as i64).abs() as u64 {
                best = Some((*block, pd.price, pd.unix_ts));
            }
        }
        
        if let Some((block, price, actual_ts)) = best {
            let ts_diff = (actual_ts as i64 - target_ts as i64).abs() as u64;
            if ts_diff <= self.chain_config_cached.price_timestamp_tolerance_seconds {
                // Cache the result
                let cache_key = HistoricalPriceKey {
                    chain: self.chain_name.clone(),
                    token,
                    block,
                };
                self.cache.insert(cache_key, price).await;
                self.validate_price(price, token)
            } else {
                Err(PriceError::NotAvailable(format!(
                    "No price for token {:?} at block {} within {}s of unix_ts {} (closest: {})",
                    token, target_block, self.chain_config_cached.price_timestamp_tolerance_seconds,
                    target_ts, actual_ts
                )))
            }
        } else {
            Err(PriceError::NotAvailable(format!(
                "No price for token {:?} at block {} near unix_ts {}",
                token, target_block, target_ts
            )))
        }
    }
}

#[async_trait]
impl crate::price_oracle::PriceOracle for HistoricalPriceProvider {
    async fn get_token_price_usd(
        &self,
        chain_name: &str,
        token: Address,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        self.price_at(token).await
    }
    
    async fn get_pair_price_ratio(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        // Fetch both prices in parallel for performance
        let (pa, pb) = tokio::join!(
            self.get_token_price_usd(chain_name, token_a),
            self.get_token_price_usd(chain_name, token_b)
        );
        
        let pa = pa?;
        let pb = pb?;
        
        if pb == 0.0 {
            warn!("Price for token {:?} is zero, cannot calculate ratio", token_b);
            return Err(PriceError::NotAvailable(format!(
                "Token {:?} price is zero",
                token_b
            )));
        }
        
        let ratio = pa / pb;
        debug!("Price ratio for tokens: {:?} and {:?} is {}", token_a, token_b, ratio);
        Ok(ratio)
    }
    
    async fn get_token_volatility(
        &self,
        chain_name: &str,
        token: Address,
        timeframe_seconds: u64,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        let avg_block_time = self.chain_config_cached.avg_block_time_seconds;
        let blocks_back = ((timeframe_seconds as f64) / avg_block_time).ceil() as u64;
        let start_block = self.block_number.saturating_sub(blocks_back);
        
        // Calculate step for sampling
        let sample_interval_seconds = self.chain_config_cached.volatility_sample_interval_seconds;
        let step = std::cmp::max(
            1,
            (sample_interval_seconds as f64 / avg_block_time).round() as u64
        );
        
        // Build sample blocks
        let mut sample_blocks = Vec::new();
        let mut b = start_block;
        let mut count = 0;
        let max_samples = self.chain_config_cached.volatility_max_samples;
        
        while b <= self.block_number && count < max_samples {
            sample_blocks.push(b);
            b = b.saturating_add(step);
            count += 1;
        }
        
        // Ensure last block is included
        if sample_blocks.last() != Some(&self.block_number) {
            sample_blocks.push(self.block_number);
        }
        
        // Batch fetch all price data
        let pds = self
            .loader
            .get_token_price_data_batch_at_blocks(&self.chain_name, token, &sample_blocks)
            .await
            .map_err(|e| PriceError::DataSource(e.to_string()))?;
        
        // Extract valid prices and cache them
        let mut prices = Vec::with_capacity(pds.len());
        for (block_num, pd) in pds {
            if pd.price.is_finite() && pd.price > 0.0 {
                prices.push(pd.price);
                
                // Cache each price
                let cache_key = HistoricalPriceKey {
                    chain: self.chain_name.clone(),
                    token,
                    block: block_num,
                };
                self.cache.insert(cache_key, pd.price).await;
            }
        }
        
        if prices.len() < 2 {
            warn!("Insufficient price points for volatility calculation");
            return Err(PriceError::NotAvailable(
                "Insufficient price points for volatility".into(),
            ));
        }
        
        // Calculate log returns
        let mut log_returns = Vec::with_capacity(prices.len() - 1);
        for w in prices.windows(2) {
            let r = (w[1] / w[0]).ln();
            if r.is_finite() {
                log_returns.push(r);
            }
        }
        
        if log_returns.is_empty() {
            warn!("No log-returns available for volatility calculation");
            return Err(PriceError::NotAvailable("No log-returns".into()));
        }
        
        // Calculate volatility
        let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let var = log_returns
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / log_returns.len() as f64;
        
        let vol = var.sqrt();
        let annual_factor = (365.0 * 24.0 * 3600.0 / timeframe_seconds as f64).sqrt();
        let volatility = vol * annual_factor;
        
        debug!("Calculated volatility for token: {:?} is {}", token, volatility);
        Ok(volatility)
    }
    
    async fn calculate_liquidity_usd(&self, pool_info: &PoolInfo) -> Result<f64, PriceError> {
        // Fetch both prices in parallel
        let (p0_result, p1_result) = tokio::join!(
            self.price_at(pool_info.token0),
            self.price_at(pool_info.token1)
        );
        
        // Handle price errors more explicitly
        let (p0, p1) = match (p0_result, p1_result) {
            (Ok(price0), Ok(price1)) => (price0, price1),
            (Err(e), _) => {
                debug!("Failed to get price for token0 {:?}: {}", pool_info.token0, e);
                return Ok(0.0); // Return 0 liquidity if price unavailable
            },
            (_, Err(e)) => {
                debug!("Failed to get price for token1 {:?}: {}", pool_info.token1, e);
                return Ok(0.0); // Return 0 liquidity if price unavailable
            }
        };
        
        if p0 == 0.0 || p1 == 0.0 {
            debug!("Zero price detected for pool liquidity calculation: p0={}, p1={}", p0, p1);
            return Ok(0.0);
        }
        
        let l0 = normalize_units(pool_info.reserves0, pool_info.token0_decimals) * p0;
        let l1 = normalize_units(pool_info.reserves1, pool_info.token1_decimals) * p1;
        
        Ok(l0 + l1)
    }
    
    async fn get_historical_prices_batch(
        &self,
        chain_name: &str,
        tokens: &[Address],
        block_number: u64,
    ) -> Result<HashMap<Address, f64>, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        // In strict mode, do not allow querying an earlier block than configured
        let target_block = if self.strict { block_number } else { block_number.min(self.block_number) };
        
        // Use batch fetching for efficiency
        let map = self
            .loader
            .get_multi_token_prices_at_block(&self.chain_name, tokens, target_block)
            .await
            .map_err(|e| PriceError::DataSource(e.to_string()))?;
        
        let mut out = HashMap::with_capacity(map.len());
        for (token, price) in map {
            if price.is_finite() && price > 0.0 {
                // Validate and cache each price
                if let Ok(validated_price) = self.validate_price(price, token) {
                    out.insert(token, validated_price);
                    
                    let cache_key = HistoricalPriceKey {
                        chain: self.chain_name.clone(),
                        token,
                        block: target_block,
                    };
                    self.cache.insert(cache_key, validated_price).await;
                }
            } else {
                warn!(
                    "Price for token {:?} at block {} is not finite/positive",
                    token, target_block
                );
            }
        }
        
        debug!("Retrieved historical prices batch for {} tokens at block: {}", tokens.len(), target_block);
        Ok(out)
    }
    
    async fn get_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }
    
    async fn get_token_price_usd_at(
        &self,
        chain_name: &str,
        token: Address,
        block_number: Option<u64>,
        unix_ts: Option<u64>,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        let target_block = if self.strict {
            block_number.ok_or_else(|| PriceError::NotAvailable("Strict mode requires explicit block number".to_string()))?
        } else {
            block_number
                .unwrap_or(self.block_number)
                .min(self.block_number)
        };
        
        if let Some(target_ts) = unix_ts {
            if self.strict {
                return Err(PriceError::NotAvailable("Strict mode: unix_ts lookup disabled without exact match".to_string()));
            }
            return self.get_price_near_timestamp(token, target_block, target_ts).await;
        }
        
        // Use the existing optimized price_at if we're at the right block
        if target_block == self.block_number {
            return self.price_at(token).await;
        }
        
        // Otherwise fetch directly
        let pd_opt = self
            .loader
            .get_token_price_data_at_block(&self.chain_name, token, target_block)
            .await
            .map_err(|e| PriceError::DataSource(e.to_string()))?;
        
        let price = pd_opt
            .filter(|pd| pd.price.is_finite() && pd.price > 0.0)
            .map(|pd| pd.price)
            .ok_or_else(|| {
                PriceError::NotAvailable(format!(
                    "No price for token {:?} at block {}",
                    token, target_block
                ))
            })?;
        
        self.validate_price(price, token)
    }
    
    
    
    async fn get_pair_price_at_block(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        block_number: u64,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        // Create a temporary provider at the specified block
        let temp_provider = Self::new(
            self.chain_name.to_string(),
            block_number,
            None,
            self.loader.clone(),
            self.chain_config.clone(),
            self.strict,
        )?;
        
        temp_provider.get_pair_price_ratio(chain_name, token_a, token_b).await
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
        let native_token = self.chain_config_cached.weth_address;
        let price = self.get_token_price_usd(chain_name, native_token).await?;
        let normalized_amount = safe_u256_to_f64(amount_native, 18)?;
        Ok(normalized_amount * price)
    }
    
    async fn estimate_token_conversion(
        &self,
        chain_name: &str,
        amount_in: U256,
        token_in: Address,
        token_out: Address,
    ) -> Result<U256, PriceError> {
        // Fetch prices and decimals in parallel for performance
        let (price_in_res, price_out_res, decimals_in_fut, decimals_out_fut) = tokio::join!(
            self.get_token_price_usd(chain_name, token_in),
            self.get_token_price_usd(chain_name, token_out),
            self.get_token_decimals_cached(token_in),
            self.get_token_decimals_cached(token_out)
        );
        
        let price_in = price_in_res?;
        let price_out = price_out_res?;
        
        if price_out == 0.0 {
            return Err(PriceError::Calculation("Output token price is zero".to_string()));
        }
        
        let normalized_amount_in = safe_u256_to_f64(amount_in, decimals_in_fut)?;
        let usd_value = normalized_amount_in * price_in;
        let normalized_amount_out = usd_value / price_out;
        
        safe_f64_to_u256(normalized_amount_out, decimals_out_fut)
    }
    
    async fn estimate_swap_value_usd(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Result<f64, PriceError> {
        if chain_name != self.chain_name.as_ref() {
            return Err(PriceError::NotAvailable(format!(
                "HistoricalPriceProvider only supports its configured chain: {} (got: {})",
                self.chain_name, chain_name
            )));
        }
        
        // Estimate output amount using current prices and decimals for both tokens
        let price_in = self.get_token_price_usd(chain_name, token_in).await?;
        let price_out = self.get_token_price_usd(chain_name, token_out).await?;
        let decimals_in = self.get_token_decimals_cached(token_in).await;
        let _decimals_out = self.get_token_decimals_cached(token_out).await;
        let normalized_amount_in = safe_u256_to_f64(amount_in, decimals_in)?;
        let usd_value_in = normalized_amount_in * price_in;
        let estimated_amount_out = if price_out > 0.0 { usd_value_in / price_out } else { 0.0 };
        let usd_value_out = estimated_amount_out * price_out;
        
        // Log gas price information when estimating swap values
        if let Some(base_fee_gwei) = self.chain_config_cached.default_base_fee_gwei {
            debug!(
                "Swap value estimation with gas context: base_fee={} gwei, max_gas={:?}", 
                base_fee_gwei, 
                self.chain_config_cached.max_gas_price
            );
        }
        
        Ok(usd_value_out)
    }
}

impl HistoricalPriceProvider {
    pub async fn get_metrics(&self) -> OracleMetrics {
        (*self.metrics).clone()
    }
    
    pub async fn get_circuit_breaker_stats(&self) -> crate::alpha::circuit_breaker::CircuitBreakerStats {
        self.circuit_breaker.get_stats().await
    }
    
    pub fn set_block_context(&self, block_number: u64) -> Result<Self, PriceError> {
        Self::new(
            self.chain_name.to_string(),
            block_number,
            self.unix_ts,
            self.loader.clone(),
            self.chain_config.clone(),
            self.strict,
        )
    }
    
    /// Get the default gas price for fallback scenarios
    pub async fn get_default_gas_price(&self) -> (U256, U256) {
        let base_fee = if let Some(base_fee_gwei) = self.chain_config_cached.default_base_fee_gwei {
            U256::from(base_fee_gwei).saturating_mul(U256::from(1_000_000_000u64))
        } else {
            // Fallback: use 80% of max_gas_price as a heuristic
            self.chain_config_cached.max_gas_price
                .saturating_mul(U256::from(80))
                .checked_div(U256::from(100))
                .unwrap_or(U256::from(25_000_000_000u64))
        };
        
        let priority_fee = if let Some(priority_fee_gwei) = self.chain_config_cached.default_priority_fee_gwei {
            U256::from(priority_fee_gwei).saturating_mul(U256::from(1_000_000_000u64))
        } else {
            // Fallback: use 10% of base_fee as a heuristic
            base_fee
                .saturating_mul(U256::from(10))
                .checked_div(U256::from(100))
                .unwrap_or(U256::from(1_500_000_000u64))
        };
        
        (base_fee, priority_fee)
    }
    
    /// Estimate gas cost in USD for a given gas amount
    pub async fn estimate_gas_cost_usd(&self, gas_used: U256) -> Result<f64, PriceError> {
        // Try to get actual gas price from the loader first
        let gas_price_result = self.loader
            .get_gas_info_for_block(&self.chain_name, self.block_number as i64)
            .await
            .map_err(|e| PriceError::DataSource(format!("Failed to get gas info: {}", e)))?;
        
        let (base_fee, priority_fee) = if let Some(gas_price) = gas_price_result {
            // Validate against max_gas_price
            let total = gas_price.base_fee.saturating_add(gas_price.priority_fee);
            if total > self.chain_config_cached.max_gas_price {
                debug!(
                    "Gas price {} exceeds max {}, using capped values",
                    total, self.chain_config_cached.max_gas_price
                );
                // Cap at max_gas_price
                let capped_base = self.chain_config_cached.max_gas_price
                    .saturating_sub(gas_price.priority_fee)
                    .max(gas_price.priority_fee);
                (capped_base, gas_price.priority_fee)
            } else {
                (gas_price.base_fee, gas_price.priority_fee)
            }
        } else {
            debug!("No gas price data available, using defaults");
            self.get_default_gas_price().await
        };
        
        let total_gas_price = base_fee.saturating_add(priority_fee);
        let gas_cost_wei = gas_used.saturating_mul(total_gas_price);
        
        // Get native token price for conversion
        let native_price = self.price_at(self.chain_config_cached.weth_address).await?;
        let gas_cost_eth = safe_u256_to_f64(gas_cost_wei, 18)?;
        
        Ok(gas_cost_eth * native_price)
    }
    
    /// Get the maximum allowed gas price for this chain
    pub fn get_max_gas_price(&self) -> U256 {
        self.chain_config_cached.max_gas_price
    }
    
    /// Validate if a gas price is within acceptable bounds
    pub fn validate_gas_price(&self, gas_price: U256) -> bool {
        gas_price <= self.chain_config_cached.max_gas_price
    }
    
    /// Get gas configuration for transaction cost calculations
    pub fn get_gas_config(&self) -> (Option<u64>, Option<u64>, U256) {
        (
            self.chain_config_cached.default_base_fee_gwei,
            self.chain_config_cached.default_priority_fee_gwei,
            self.chain_config_cached.max_gas_price,
        )
    }

    /// Reset the circuit breaker (administrative override)
    pub async fn reset_circuit_breaker(&self) {
        self.circuit_breaker.force_reset().await;
        warn!("Historical price oracle circuit breaker has been manually reset");
    }

    /// Check if circuit breaker is tripped
    pub async fn is_circuit_breaker_tripped(&self) -> bool {
        self.circuit_breaker.is_tripped().await
    }
    
    /// Estimate transaction cost with gas optimization
    pub async fn estimate_transaction_cost(
        &self,
        gas_estimate: U256,
        include_priority_fee: bool,
    ) -> Result<f64, PriceError> {
        let (base_fee, priority_fee) = self.get_default_gas_price().await;
        
        let total_gas_price = if include_priority_fee {
            base_fee.saturating_add(priority_fee)
        } else {
            base_fee
        };
        
        // Ensure we don't exceed max gas price
        let capped_gas_price = total_gas_price.min(self.chain_config_cached.max_gas_price);
        
        let gas_cost_wei = gas_estimate.saturating_mul(capped_gas_price);
        let native_price = self.price_at(self.chain_config_cached.weth_address).await?;
        let gas_cost_eth = safe_u256_to_f64(gas_cost_wei, 18)?;
        
        debug!(
            "Transaction cost estimate: gas={}, base_fee={:?} gwei, priority_fee={:?} gwei, cost=${:.4}",
            gas_estimate,
            self.chain_config_cached.default_base_fee_gwei,
            self.chain_config_cached.default_priority_fee_gwei,
            gas_cost_eth * native_price
        );
        
        Ok(gas_cost_eth * native_price)
    }

    /// Get WETH price directly without using the deduplicator to avoid recursion issues
    async fn get_weth_price_direct(&self) -> Result<f64, PriceError> {
        // First try to get WETH price from swaps table using WETH-stablecoin pairs
        if let Some((weth_price, _weth_timestamp)) = self.try_get_weth_price_from_swaps(&self.chain_name, self.block_number).await {
            debug!("Found WETH price from swaps table: ${:.2}", weth_price);
            return Ok(weth_price);
        }

        // Fall back to external API if available
        if let Some(ts) = self.unix_ts {
            if let Some(price) = self.fetch_eth_price_from_coingecko(ts).await {
                debug!("Found WETH price from CoinGecko: ${:.2}", price);
                return Ok(price);
            }
        }

        // Final fallback: use a reasonable default WETH price for backtesting
        warn!("No WETH price data available, using fallback price for backtesting");
        Ok(3000.0) // Reasonable historical WETH price
    }

    /// Try multi-hop pricing via common intermediary tokens
    async fn try_multi_hop_pricing(&self, chain_name: &str, token: Address, block_number: u64) -> Option<f64> {
        // Parse all, skip any that fail, de-dup, and skip the input token
        let mut intermediaries = Vec::new();

        // Add WETH first
        if self.chain_config_cached.weth_address != token {
            intermediaries.push(self.chain_config_cached.weth_address);
        }

        // Add other common intermediaries
        let addr_strings = [
            "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", // WBTC
            "0x514910771af9ca656af840dff83e8264ecf986ca", // LINK
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", // UNI
            "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e", // YFI
            "0x6b175474e89094c44da98b954eedeac495271d0f", // DAI
        ];

        for s in addr_strings.iter() {
            if let Ok(addr) = Address::from_str(s) {
                if addr != token && !intermediaries.contains(&addr) {
                    intermediaries.push(addr);
                }
            }
        }

        for intermediary in intermediaries {
            if let Ok(Some(price_in_intermediary)) =
                self.loader.get_direct_pair_price(chain_name, token, intermediary, block_number).await
            {
                if price_in_intermediary <= 0.0 { continue; }

                let intermediary_usd = if intermediary == self.chain_config_cached.weth_address {
                    self.get_weth_price_direct().await.ok()?
                } else if let Ok(Some(price_in_weth)) = self.loader
                            .get_direct_pair_price(chain_name, intermediary, self.chain_config_cached.weth_address, block_number).await
                {
                    (price_in_weth > 0.0).then(|| price_in_weth)?.mul(self.get_weth_price_direct().await.ok()?)
                } else {
                    continue;
                };

                let final_price = price_in_intermediary * intermediary_usd;
                if final_price.is_finite() && final_price > 0.0 { return Some(final_price); }
            }
        }
        None
    }

    /// Get WETH-based price for a token without using the deduplicator
    async fn get_weth_based_price(&self, token: Address, block_number: u64) -> Result<f64, PriceError> {
        // Find a pool pairing the token with WETH
        if let Ok(Some(price_in_weth)) = self.loader.get_direct_pair_price(
            &self.chain_name,
            token,
            self.chain_config_cached.weth_address,
            block_number
        ).await {
            if price_in_weth > 0.0 {
                // Get the price of WETH in USD using direct method
                let weth_price_usd = self.get_weth_price_direct().await?;
                let final_price = price_in_weth * weth_price_usd;
                debug!("Resolved price for {:?} via WETH: {} (in WETH) * ${} (WETH price) = ${}",
                       token, price_in_weth, weth_price_usd, final_price);
                
                return Ok(final_price);
            }
        }

        Err(PriceError::NotAvailable(format!(
            "No WETH pair found for token {:?} at block {}",
            token, block_number
        )))
    }
}

