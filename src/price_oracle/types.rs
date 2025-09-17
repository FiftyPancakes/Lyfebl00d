use ethers::types::Address;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use parking_lot::Mutex as ParkingLotMutex;
use tokio::sync::Mutex;

/// Maximum reasonable USD price for sanity checking (prevents integer overflow attacks)
/// This prevents malicious or erroneous price feeds from causing arithmetic overflows
pub const MAX_USD_PRICE: f64 = 1_000_000.0; // $1M per token

/// Minimum reasonable USD price (prevents division by zero and underflow)
/// This ensures we don't process dust values that could cause precision issues
pub const MIN_USD_PRICE: f64 = 0.000_001; // $0.000001 per token

/// Maximum age for Chainlink price feeds before considering them stale
/// Stale prices can lead to incorrect arbitrage calculations
pub const CHAINLINK_MAX_STALENESS_SECONDS: u64 = 3600; // 1 hour

/// Default number of decimals for ERC20 tokens when not retrievable
pub const DEFAULT_TOKEN_DECIMALS: u8 = 18;

/// Default number of decimals for stablecoins (USDC, USDT, DAI, etc.)
pub const STABLECOIN_DECIMALS: u8 = 6;

/// Environment configuration for proper test detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Production,
    Testing,
    Development,
}

impl Environment {
    pub fn from_chain_name(chain_name: &str) -> Self {
        if chain_name.contains("anvil") ||
           chain_name.contains("test") ||
           chain_name.contains("localhost") ||
           chain_name.contains("127.0.0.1") ||
           chain_name.contains("hardhat") ||
           chain_name.contains("ganache") {
            Environment::Testing
        } else if chain_name.contains("dev") || chain_name.contains("staging") {
            Environment::Development
        } else {
            Environment::Production
        }
    }

    /// Create Environment from chain_id instead of brittle string matching
    pub fn from_chain_id(chain_id: u64) -> Self {
        // Known test chain IDs
        if matches!(chain_id, 31337 | 1337 | 43113 | 5 | 11155111 | 80001 | 421611) {
            Environment::Testing
        } else if matches!(chain_id, 3 | 4 | 42) { // Ropsten, Rinkeby, Kovan (deprecated testnets)
            Environment::Development
        } else {
            Environment::Production
        }
    }

    pub fn is_test(&self) -> bool {
        matches!(self, Environment::Testing)
    }

    pub fn is_production(&self) -> bool {
        matches!(self, Environment::Production)
    }
}

/// Optimized price cache key using Arc for string to avoid cloning
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PriceCacheKey {
    pub chain: Arc<str>,
    pub token: Address,
}

impl PriceCacheKey {
    pub fn new(chain: Arc<str>, token: Address) -> Self {
        Self { chain, token }
    }

    pub fn from_str(chain: &str, token: Address) -> Self {
        Self {
            chain: Arc::from(chain),
            token,
        }
    }
}

/// Recursion guard to prevent infinite loops in price resolution
pub struct RecursionGuard {
    active_tokens: ParkingLotMutex<HashSet<Address>>,
}

impl RecursionGuard {
    pub fn new() -> Self {
        Self {
            active_tokens: ParkingLotMutex::new(HashSet::new()),
        }
    }

    pub fn enter(&self, token: Address) -> Result<RecursionGuardToken, crate::errors::PriceError> {
        let mut guard = self.active_tokens.lock();
        if !guard.insert(token) {
            return Err(crate::errors::PriceError::Calculation(
                format!("Recursion detected for token {:?}", token)
            ));
        }
        Ok(RecursionGuardToken {
            token,
            guard: self,
        })
    }
}

pub struct RecursionGuardToken<'a> {
    token: Address,
    guard: &'a RecursionGuard,
}

impl<'a> Drop for RecursionGuardToken<'a> {
    fn drop(&mut self) {
        self.guard.active_tokens.lock().remove(&self.token);
    }
}

/// Oracle metrics for monitoring and observability
#[derive(Debug, Default)]
pub struct OracleMetrics {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub api_calls: AtomicU64,
    pub errors: AtomicU64,
    pub total_latency_ms: AtomicU64,
    pub request_count: AtomicU64,
    pub peg_assumption_used: AtomicU64,
}

impl Clone for OracleMetrics {
    fn clone(&self) -> Self {
        Self {
            cache_hits: AtomicU64::new(self.cache_hits.load(Ordering::Relaxed)),
            cache_misses: AtomicU64::new(self.cache_misses.load(Ordering::Relaxed)),
            api_calls: AtomicU64::new(self.api_calls.load(Ordering::Relaxed)),
            errors: AtomicU64::new(self.errors.load(Ordering::Relaxed)),
            total_latency_ms: AtomicU64::new(self.total_latency_ms.load(Ordering::Relaxed)),
            request_count: AtomicU64::new(self.request_count.load(Ordering::Relaxed)),
            peg_assumption_used: AtomicU64::new(self.peg_assumption_used.load(Ordering::Relaxed)),
        }
    }
}

impl OracleMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_api_call(&self) {
        self.api_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency_ms: u64) {
        self.total_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_peg_assumption_used(&self) {
        self.peg_assumption_used.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_average_latency_ms(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            self.total_latency_ms.load(Ordering::Relaxed) as f64 / count as f64
        }
    }

    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn get_error_rate(&self) -> f64 {
        let errors = self.errors.load(Ordering::Relaxed);
        let total = self.request_count.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }
}

/// Request deduplicator to prevent duplicate concurrent requests
pub struct RequestDeduplicator<K: Clone + Send + Sync + 'static> {
    pending: Arc<Mutex<std::collections::HashMap<K, Arc<tokio::sync::broadcast::Sender<Result<f64, crate::errors::PriceError>>>>>>,
}

impl<K: Clone + Send + Sync + std::hash::Hash + Eq + 'static> RequestDeduplicator<K> {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub async fn deduplicate<F, Fut>(
        &self,
        key: K,
        fetch_fn: F,
    ) -> Result<f64, crate::errors::PriceError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<f64, crate::errors::PriceError>> + Send,
    {
        // Check if there's already a pending request
        let existing_receiver = {
            let pending = self.pending.lock().await;
            pending.get(&key).map(|sender| sender.subscribe())
        };
        
        if let Some(mut receiver) = existing_receiver {
            return match receiver.recv().await {
                Ok(result) => result,
                Err(_) => Err(crate::errors::PriceError::Other("Request broadcast failed".to_string())),
            };
        }

        // No pending request, create a new one
        let (sender, _) = tokio::sync::broadcast::channel(16);
        let sender = Arc::new(sender);
        
        {
            let mut pending = self.pending.lock().await;
            pending.insert(key.clone(), sender.clone());
        }

        // Execute the fetch
        let result = fetch_fn().await;

        // Remove from pending and broadcast result
        {
            let mut pending = self.pending.lock().await;
            pending.remove(&key);
        }

        let _ = sender.send(result.clone());
        result
    }
}

/// Chainlink price feed data with staleness checking
#[derive(Debug, Clone)]
pub struct ChainlinkPriceData {
    pub price: f64,
    pub timestamp: u64,
    pub round_id: u64,
}

impl ChainlinkPriceData {
    pub fn is_stale(&self, current_timestamp: u64) -> bool {
        if current_timestamp < self.timestamp {
            return true; // Price is from the future, definitely wrong
        }
        (current_timestamp - self.timestamp) > CHAINLINK_MAX_STALENESS_SECONDS
    }

    pub fn validate(&self) -> Result<(), crate::errors::PriceError> {
        if !self.price.is_finite() || self.price <= 0.0 {
            return Err(crate::errors::PriceError::Validation(
                format!("Invalid Chainlink price: {}", self.price)
            ));
        }
        if self.price < MIN_USD_PRICE || self.price > MAX_USD_PRICE {
            return Err(crate::errors::PriceError::Validation(
                format!("Chainlink price {} outside reasonable bounds [{}, {}]", 
                    self.price, MIN_USD_PRICE, MAX_USD_PRICE)
            ));
        }
        Ok(())
    }
}

/// Centralized decimal resolution with intelligent fallbacks
pub struct DecimalResolver {
    cache: moka::future::Cache<Address, u8>,
    known_stablecoins: HashSet<Address>,
}

impl DecimalResolver {
    pub fn new() -> Self {
        let mut known_stablecoins = HashSet::new();
        // Add known stablecoin addresses here
        // USDC on Ethereum
        known_stablecoins.insert("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".parse().unwrap());
        // USDT on Ethereum  
        known_stablecoins.insert("0xdac17f958d2ee523a2206206994597c13d831ec7".parse().unwrap());
        // DAI on Ethereum
        known_stablecoins.insert("0x6b175474e89094c44da98b954eedeac495271d0f".parse().unwrap());

        Self {
            cache: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(3600))
                .max_capacity(1000)
                .build(),
            known_stablecoins,
        }
    }

    pub async fn get_decimals_with_fallback<F, Fut>(
        &self,
        token: Address,
        fetch_fn: F,
    ) -> u8
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<u8, crate::errors::PriceError>>,
    {
        // Check cache first
        if let Some(decimals) = self.cache.get(&token).await {
            return decimals;
        }

        // Try to fetch from chain
        let decimals = match fetch_fn().await {
            Ok(d) if d <= 77 => d, // Sanity check - no token should have more than 77 decimals
            _ => {
                // Use intelligent fallback
                if self.known_stablecoins.contains(&token) {
                    STABLECOIN_DECIMALS
                } else {
                    DEFAULT_TOKEN_DECIMALS
                }
            }
        };

        self.cache.insert(token, decimals).await;
        decimals
    }

    pub fn is_stablecoin(&self, token: &Address) -> bool {
        self.known_stablecoins.contains(token)
    }
}

/// Safe arithmetic operations for U256 to f64 conversions
pub fn safe_u256_to_f64(
    amount: ethers::types::U256,
    decimals: u8,
) -> Result<f64, crate::errors::PriceError> {
    use ethers::types::U256;
    
    // Protect against excessive decimals that could cause overflow
    let safe_decimals = decimals.min(76);
    
    if amount.is_zero() {
        return Ok(0.0);
    }

    // Check if the amount is too large for direct conversion
    if amount > U256::from(u128::MAX) {
        // Use format_units for precise conversion of large numbers
        let amount_str = ethers::utils::format_units(amount, safe_decimals as u32)
            .map_err(|e| crate::errors::PriceError::Calculation(
                format!("Failed to format amount: {}", e)
            ))?;
        
        amount_str.parse::<f64>()
            .map_err(|e| crate::errors::PriceError::Calculation(
                format!("Failed to parse amount: {}", e)
            ))
    } else {
        // Safe direct conversion for smaller numbers
        let amount_u128 = amount.as_u128();
        let divisor = 10_f64.powi(safe_decimals as i32);
        
        // Check for potential infinity
        let result = amount_u128 as f64 / divisor;
        if !result.is_finite() {
            return Err(crate::errors::PriceError::Calculation(
                "Arithmetic resulted in non-finite value".to_string()
            ));
        }
        
        Ok(result)
    }
}

/// Safe f64 to U256 conversion with bounds checking
pub fn safe_f64_to_u256(
    amount: f64,
    decimals: u8,
) -> Result<ethers::types::U256, crate::errors::PriceError> {
    use ethers::types::U256;
    
    // Validate input
    if !amount.is_finite() || amount < 0.0 {
        return Err(crate::errors::PriceError::Calculation(
            format!("Invalid amount for conversion: {}", amount)
        ));
    }
    
    // Protect against excessive decimals
    let safe_decimals = decimals.min(76);
    let multiplier = 10_f64.powi(safe_decimals as i32);
    let scaled = amount * multiplier;
    
    // Check for overflow
    if scaled > u128::MAX as f64 {
        return Err(crate::errors::PriceError::Calculation(
            "Amount too large for U256 conversion".to_string()
        ));
    }
    
    // Safe conversion
    let amount_u128 = scaled.min(u128::MAX as f64).max(0.0) as u128;
    Ok(U256::from(amount_u128))
}