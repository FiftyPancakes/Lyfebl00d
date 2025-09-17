//! # Transaction Optimizer V2
//!
//! Production-grade transaction optimization and submission for DeFi arbitrage operations
//! with enhanced security, performance optimizations, and robust error handling.

use crate::{
    alpha::circuit_breaker::CircuitBreaker,
    blockchain::BlockchainManager,
    config::TransactionOptimizerSettings,
    errors::OptimizerError,
    gas_oracle::{GasOracleProvider, GasPrice as OracleGasPrice},
    mempool_stats::MempoolStatsProvider,
    types::{TxGasPrice, ArbitrageRoute},
};
use async_trait::async_trait;
use ethers::{
    prelude::*,
    signers::{LocalWallet, Signer},
    types::{
        transaction::eip2718::TypedTransaction,
        Address, Bytes, H256, TransactionRequest, U256, U64,
    },
};
use ethers_flashbots::FlashbotsMiddleware;
use ethers::providers::Middleware;
use ethers::utils::keccak256;
use lru::LruCache;
use prometheus::{
    Gauge, Histogram, HistogramOpts, IntCounter, IntGauge,
    register_gauge, register_histogram, register_int_counter, register_int_gauge,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{self, Debug},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::{RwLock, Semaphore, Mutex, Notify},
    task::JoinHandle,
    time::{sleep},
};
use tracing::{debug, info, warn};

// ======================== Configuration ========================

/// Monitoring configuration with all magic numbers properly documented
#[derive(Debug, Clone, Deserialize)]
pub struct MonitoringConfig {
    /// How often to check pending transactions
    pub monitor_interval: Duration,
    /// How long before considering a transaction stuck
    pub stuck_threshold: Duration,
    /// Gas price increase percentage for bumping (in basis points)
    pub gas_bump_bps: u32,
    /// Maximum pending transactions to track
    pub max_pending_transactions: usize,
    /// TTL for pending transaction entries
    pub pending_tx_ttl: Duration,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            monitor_interval: Duration::from_secs(10),
            stuck_threshold: Duration::from_secs(60),
            gas_bump_bps: 1100, // 11% increase
            max_pending_transactions: 1000,
            pending_tx_ttl: Duration::from_secs(600), // 10 minutes
        }
    }
}

/// Enhanced configuration with security and performance settings
#[derive(Debug, Clone)]
pub struct EnhancedOptimizerConfig {
    pub base_config: Arc<TransactionOptimizerSettings>,
    pub monitoring: MonitoringConfig,
    pub security: SecurityConfig,
    pub performance: PerformanceConfig,
    pub circuit_breaker_threshold: u64,
    pub circuit_breaker_timeout: Duration,
}

impl Default for EnhancedOptimizerConfig {
    fn default() -> Self {
        Self {
            base_config: Arc::new(TransactionOptimizerSettings::default()),
            monitoring: MonitoringConfig::default(),
            security: SecurityConfig::default(),
            performance: PerformanceConfig::default(),
            circuit_breaker_threshold: 1000,
            circuit_breaker_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    pub max_transaction_value: U256,
    pub max_gas_limit: U256,
    pub blacklisted_addresses: HashSet<Address>,
    pub whitelisted_addresses: HashSet<Address>,
    pub require_whitelist: bool,
    pub max_daily_transactions: u64,
    pub max_daily_value: U256,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_transaction_value: U256::from(1000u64).saturating_mul(U256::exp10(18)), // 1000 ETH
            max_gas_limit: U256::from(500000u64),
            blacklisted_addresses: HashSet::new(),
            whitelisted_addresses: HashSet::new(),
            require_whitelist: false,
            max_daily_transactions: 1000,
            max_daily_value: U256::from(10000u64).saturating_mul(U256::exp10(18)), // 10000 ETH
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerformanceConfig {
    pub simulation_cache_size: usize,
    pub simulation_cache_ttl: Duration,
    pub max_concurrent_retries: usize,
    pub retry_jitter_factor: f64,
    pub bundle_status_check_method: BundleCheckMethod,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            simulation_cache_size: 1000,
            simulation_cache_ttl: Duration::from_secs(300), // 5 minutes
            max_concurrent_retries: 3,
            retry_jitter_factor: 0.1,
            bundle_status_check_method: BundleCheckMethod::Hybrid,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum BundleCheckMethod {
    BlockScan,
    EventLogs,
    Hybrid,
}

// ======================== Metrics ========================

lazy_static::lazy_static! {
    static ref TX_SUBMITTED: IntCounter = register_int_counter!(
        "transaction_optimizer_submitted_total",
        "Total number of transactions submitted"
    ).unwrap();
    
    static ref TX_CONFIRMED: IntCounter = register_int_counter!(
        "transaction_optimizer_confirmed_total",
        "Total number of transactions confirmed"
    ).unwrap();
    
    static ref TX_FAILED: IntCounter = register_int_counter!(
        "transaction_optimizer_failed_total",
        "Total number of transaction failures"
    ).unwrap();
    
    static ref GAS_PRICE_GAUGE: Gauge = register_gauge!(
        "transaction_optimizer_gas_price_gwei",
        "Current gas price in gwei"
    ).unwrap();
    
    static ref SUBMISSION_LATENCY: Histogram = register_histogram!(
        HistogramOpts::new(
            "transaction_optimizer_submission_latency_seconds",
            "Transaction submission latency"
        )
        .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0])
    ).unwrap();
    
    static ref PENDING_TX_COUNT: IntGauge = register_int_gauge!(
        "transaction_optimizer_pending_transactions",
        "Number of pending transactions"
    ).unwrap();
}

// ======================== Core Types ========================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BundleStatus {
    Pending,
    Submitted(H256),
    Included(u64, Vec<TransactionResult>),
    NotIncluded,
    Failed(String),
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    pub tx_hash: H256,
    pub reverted: bool,
    pub error_message: Option<String>,
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
}

#[derive(Debug, Clone)]
pub struct SwapParams {
    pub deadline_seconds: u64,
    pub recipient: Option<Address>,
    pub min_amount_out: Option<U256>,
    pub exact_input: bool,
    pub amount_in: U256,
}

impl Default for SwapParams {
    fn default() -> Self {
        Self {
            deadline_seconds: 300,
            recipient: None,
            min_amount_out: None,
            exact_input: true,
            amount_in: U256::zero(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SubmissionStrategy {
    PublicMempool,
    Flashbots,
    PrivateRelay { name: String, endpoint: String },
}

#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    pub tx: TransactionRequest,
    pub submission_time: Instant,
    pub gas_price: TxGasPrice,
    pub submission_method: SubmissionStrategy,
    pub retry_count: u32,
    pub nonce: U256,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
struct SimulationResult {
    pub success: bool,
    pub output: Option<Bytes>,
    pub gas_used: U256,
    pub cached_at: Instant,
}

// ======================== Bounded Collections ========================

/// Thread-safe bounded collection for pending transactions with TTL
struct BoundedPendingTransactions {
    map: Arc<RwLock<HashMap<H256, TransactionMetadata>>>,
    max_size: usize,
    ttl: Duration,
}

impl BoundedPendingTransactions {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            ttl,
        }
    }

    async fn insert(&self, hash: H256, metadata: TransactionMetadata) {
        let mut map = self.map.write().await;
        
        // Evict expired entries first
        let now = SystemTime::now();
        map.retain(|_, v| {
            now.duration_since(v.created_at)
                .map(|d| d < self.ttl)
                .unwrap_or(false)
        });
        
        // Evict oldest if at capacity
        if map.len() >= self.max_size {
            if let Some(oldest_key) = map
                .iter()
                .min_by_key(|(_, v)| v.created_at)
                .map(|(k, _)| *k)
            {
                map.remove(&oldest_key);
            }
        }
        
        map.insert(hash, metadata);
        PENDING_TX_COUNT.set(map.len() as i64);
    }

    async fn remove(&self, hash: &H256) -> Option<TransactionMetadata> {
        let mut map = self.map.write().await;
        let result = map.remove(hash);
        PENDING_TX_COUNT.set(map.len() as i64);
        result
    }

    async fn get_all(&self) -> HashMap<H256, TransactionMetadata> {
        self.map.read().await.clone()
    }
}

// ======================== Submission Strategies ========================

#[async_trait]
trait SubmissionStrategyTrait: Send + Sync + Debug {
    async fn submit(&self, tx: TransactionRequest) -> Result<H256, OptimizerError>;
    fn supports_mev_protection(&self) -> bool;
    async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError>;
}

struct PublicMempoolStrategy {
    provider: Arc<dyn BlockchainManager>,
    gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    nonce_manager: Arc<NonceManager>,
}

impl fmt::Debug for PublicMempoolStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublicMempoolStrategy").finish()
    }
}

#[async_trait]
impl SubmissionStrategyTrait for PublicMempoolStrategy {
    async fn submit(&self, tx: TransactionRequest) -> Result<H256, OptimizerError> {
        // Note: Nonce assignment is handled atomically by BlockchainManager::submit_raw_transaction
        // to avoid race conditions between nonce fetching and transaction submission

        let gas_price = self.gas_oracle
            .get_gas_price(&self.provider.get_chain_name(), None)
            .await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;

        self.provider.submit_raw_transaction(tx, gas_price).await
            .map_err(|e| OptimizerError::SubmissionFinal(e.to_string()))
    }
    
    fn supports_mev_protection(&self) -> bool {
        false
    }
    
    async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError> {
        let gas = self.provider.estimate_gas(tx, None).await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        let gas_price = self.gas_oracle
            .get_gas_price(&self.provider.get_chain_name(), None)
            .await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        
        Ok(gas.saturating_mul(gas_price.base_fee.saturating_add(gas_price.priority_fee)))
    }
}

// ======================== Main Optimizer ========================

/// Production-grade transaction optimizer with enhanced features
/// Transaction optimizer trait for abstracting transaction submission
#[async_trait]
pub trait TransactionOptimizerTrait: Send + Sync {
    async fn submit_transaction(
        &self,
        tx: ethers::types::transaction::eip2718::TypedTransaction,
        submission_strategy: SubmissionStrategy,
    ) -> Result<H256, OptimizerError>;
    
    async fn submit_bundle(
        &self,
        txs: Vec<ethers::types::transaction::eip2718::TypedTransaction>,
        target_block: Option<u64>,
    ) -> Result<BundleStatus, OptimizerError>;
    
    async fn simulate_trade_submission(
        &self,
        route: &crate::types::ArbitrageRoute,
        block_number: u64,
    ) -> Result<(U256, f64), OptimizerError>;
    
    fn get_wallet_address(&self) -> Address;
    
    async fn auto_optimize_and_submit(
        &self,
        tx: TransactionRequest,
        use_flashbots_hint: Option<bool>,
    ) -> Result<H256, OptimizerError>;
    
    async fn paper_trade(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
        block_number: u64,
        profit_usd: f64,
        gas_cost_usd: f64,
    ) -> Result<H256, OptimizerError>;
}

/// Type alias for backward compatibility
pub type TransactionOptimizer = TransactionOptimizerV2;

pub struct TransactionOptimizerV2 {
    config: Arc<EnhancedOptimizerConfig>,
    provider: Arc<dyn BlockchainManager>,
    gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    price_oracle: Arc<dyn crate::price_oracle::PriceOracle + Send + Sync>,
    mempool_monitor: Option<Arc<dyn MempoolStatsProvider>>,
    flashbots_middleware: Option<Arc<FlashbotsMiddleware<Arc<ethers::providers::Provider<ethers::providers::Http>>, LocalWallet>>>,
    nonce_manager: Arc<NonceManager>,
    execution_semaphore: Arc<Semaphore>,
    pending_transactions: Arc<BoundedPendingTransactions>,
    shutdown_signal: Arc<Notify>,
    monitor_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    arbitrage_executor_address: Address,
    circuit_breaker: Arc<CircuitBreaker>,
    simulation_cache: Arc<Mutex<LruCache<(H256, u64, U256), SimulationResult>>>,
    daily_stats: Arc<DailyTransactionStats>,
    submission_strategies: Arc<RwLock<HashMap<String, Arc<dyn SubmissionStrategyTrait>>>>,
}

impl Debug for TransactionOptimizerV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionOptimizerV2")
            .field("arbitrage_executor_address", &self.arbitrage_executor_address)
            .field("circuit_breaker_active", &"<CircuitBreaker>")
            .finish()
    }
}

/// Daily transaction statistics for rate limiting
struct DailyTransactionStats {
    transaction_count: Arc<AtomicU64>,
    total_value: Arc<RwLock<U256>>,
    last_reset: Arc<RwLock<SystemTime>>,
}

impl DailyTransactionStats {
    fn new() -> Self {
        Self {
            transaction_count: Arc::new(AtomicU64::new(0)),
            total_value: Arc::new(RwLock::new(U256::zero())),
            last_reset: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    async fn check_and_reset(&self) {
        let mut last_reset = self.last_reset.write().await;
        let now = SystemTime::now();
        
        if let Ok(duration) = now.duration_since(*last_reset) {
            if duration > Duration::from_secs(86400) {
                self.transaction_count.store(0, Ordering::Relaxed);
                *self.total_value.write().await = U256::zero();
                *last_reset = now;
            }
        }
    }

    async fn record_transaction(&self, value: U256) -> Result<(), OptimizerError> {
        self.check_and_reset().await;
        
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        let mut total = self.total_value.write().await;
        *total = total.saturating_add(value);
        
        Ok(())
    }

    async fn can_submit(&self, config: &SecurityConfig, value: U256) -> bool {
        self.check_and_reset().await;
        
        let count = self.transaction_count.load(Ordering::Relaxed);
        let total = *self.total_value.read().await;
        
        count < config.max_daily_transactions &&
        total.saturating_add(value) <= config.max_daily_value
    }
}

#[async_trait]
impl TransactionOptimizerTrait for TransactionOptimizerV2 {
    async fn submit_transaction(
        &self,
        tx: ethers::types::transaction::eip2718::TypedTransaction,
        submission_strategy: SubmissionStrategy,
    ) -> Result<H256, OptimizerError> {
        let tx_request = match tx {
            TypedTransaction::Legacy(legacy) => legacy,
            TypedTransaction::Eip2930(eip2930) => eip2930.tx,
            TypedTransaction::Eip1559(eip1559) => eip1559.into(),
        };

        let strategies = self.submission_strategies.read().await;
        let strategy = match submission_strategy {
            SubmissionStrategy::PublicMempool => strategies.get("public_mempool"),
            SubmissionStrategy::Flashbots => strategies.get("flashbots"),
            SubmissionStrategy::PrivateRelay { name, .. } => strategies.get(&name),
        };
        
        if let Some(strategy) = strategy {
            let result = strategy.submit(tx_request).await;
            if result.is_ok() { TX_SUBMITTED.inc(); }
            result
        } else {
            Err(OptimizerError::SubmissionFinal("No submission strategy found".to_string()))
        }
    }

    async fn submit_bundle(
        &self,
        txs: Vec<ethers::types::transaction::eip2718::TypedTransaction>,
        target_block: Option<u64>,
    ) -> Result<BundleStatus, OptimizerError> {
        if txs.is_empty() {
            return Err(OptimizerError::SubmissionFinal("Empty bundle".to_string()));
        }
        
        if let Some(flashbots_middleware) = &self.flashbots_middleware {
            let mut signed_transactions = Vec::new();
            let wallet = self.provider.get_wallet();
            
            for tx in txs {
                // Sign the transaction
                let signature = wallet.sign_transaction(&tx).await
                    .map_err(|e| OptimizerError::Signing(e.to_string()))?;
                let raw_tx = tx.rlp_signed(&signature);
                signed_transactions.push(raw_tx);
            }
            
            let block_number = self.provider.get_block_number().await
                .map_err(|e| OptimizerError::Provider(e.to_string()))?;
            
            let target_block = target_block.unwrap_or_else(|| block_number + 1);

            let mut bundle_request = ethers_flashbots::BundleRequest::new()
                .set_block(target_block.into());
            for raw in signed_transactions.into_iter() {
                bundle_request = bundle_request.push_transaction(raw);
            }

            let pending_bundle = flashbots_middleware.send_bundle(&bundle_request).await
                .map_err(|e| OptimizerError::SubmissionFinal(format!("Flashbots bundle submission failed: {}", e)))?;

            // Extract bundle hash from pending bundle
            let bundle_hash = match pending_bundle.bundle_hash {
                Some(hash) => hash,
                None => {
                    return Err(OptimizerError::SubmissionFinal(
                        "Bundle submission succeeded but no bundle hash was returned".to_string()
                    ));
                }
            };

            self.check_bundle_status_efficient(bundle_hash, target_block).await
        } else {
            Err(OptimizerError::SubmissionFinal("Flashbots middleware not available".to_string()))
        }
    }

    async fn simulate_trade_submission(
        &self,
        route: &crate::types::ArbitrageRoute,
        block_number: u64,
    ) -> Result<(U256, f64), OptimizerError> {
        // Get gas price first for accurate cache key and cost calculation
        let gas_price = self.gas_oracle
            .get_gas_price(&self.provider.get_chain_name(), Some(block_number))
            .await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        let effective_gas_price = gas_price.base_fee.saturating_add(gas_price.priority_fee);

        // Check simulation cache with gas price included for accuracy
        let route_hash = self.compute_route_hash(route)?;
        let cache_key = (route_hash, block_number, effective_gas_price);
        if let Some(cached) = {
            let mut cache = self.simulation_cache.lock().await;
            cache.get(&cache_key).cloned()
        } {
            if cached.cached_at.elapsed() < self.config.performance.simulation_cache_ttl {
                // Validate cached simulation result before using
                if !cached.success {
                    // Fallback to fresh simulation on unsuccessful cache entries
                } else if let Some(ref out) = cached.output {
                    // Basic sanity check on output payload to ensure it looks like ABI-encoded data
                    if out.len() < 4 {
                        // Treat as insufficient; recompute
                    } else {
                        // Use cached result with pre-fetched gas price
                        let gas_cost_wei = cached
                            .gas_used
                            .saturating_mul(effective_gas_price);
                        let gas_cost_usd = self.price_oracle
                            .get_token_price_usd(&self.provider.get_chain_name(), self.provider.get_native_token_address())
                            .await
                            .map(|price| price * crate::types::u256_to_f64(gas_cost_wei))
                            .unwrap_or(0.0);
                        return Ok((cached.gas_used, gas_cost_usd));
                    }
                } else {
                    // No output available; still acceptable for gas-only consumers
                    let gas_cost_wei = cached
                        .gas_used
                        .saturating_mul(effective_gas_price);
                    let gas_cost_usd = self.price_oracle
                        .get_token_price_usd(&self.provider.get_chain_name(), self.provider.get_native_token_address())
                        .await
                        .map(|price| price * crate::types::u256_to_f64(gas_cost_wei))
                        .unwrap_or(0.0);
                    return Ok((cached.gas_used, gas_cost_usd));
                }
                // Use cached gas_used to compute costs at this block
                // Else fall through to recompute fresh
            }
        }

        let tx_request = route.to_tx(self.get_wallet_address());
        
        let gas_estimate = self.provider.estimate_gas(&tx_request, Some(block_number.into())).await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;

        // Use the pre-fetched effective gas price for cost calculation
        let gas_cost_wei = gas_estimate.saturating_mul(effective_gas_price);
        
        let gas_cost_usd = self.price_oracle
            .get_token_price_usd(&self.provider.get_chain_name(), self.provider.get_native_token_address())
            .await
            .map(|price| price * crate::types::u256_to_f64(gas_cost_wei))
            .unwrap_or(0.0);

        // Cache the simulation result
        {
            let mut cache = self.simulation_cache.lock().await;
            cache.put(
                cache_key,
                SimulationResult {
                    success: true,
                    output: None,
                    gas_used: gas_estimate,
                    cached_at: Instant::now(),
                },
            );
        }

        Ok((gas_estimate, gas_cost_usd))
    }

    fn get_wallet_address(&self) -> Address {
        self.provider.get_wallet().address()
    }

    async fn auto_optimize_and_submit(
        &self,
        tx: TransactionRequest,
        use_flashbots_hint: Option<bool>,
    ) -> Result<H256, OptimizerError> {
        // Circuit breaker gate
        if self.circuit_breaker.is_tripped().await {
            return Err(OptimizerError::CircuitBreakerOpen("Circuit breaker is open due to excessive failures".to_string()));
        }

        self.validate_transaction(&tx)?;

        let daily_stats = self.daily_stats.clone();
        let security_config = &self.config.security;
        let tx_value = tx.value.unwrap_or(U256::zero());
        
        if !daily_stats.can_submit(security_config, tx_value).await {
            return Err(OptimizerError::CircuitBreakerOpen("Daily transaction limits exceeded".to_string()));
        }

        // Determine best submission strategy using mempool stats and strategy trait metadata
        let submission_strategy = {
            let strategies = self.submission_strategies.read().await;

            // Gather mempool stats if available
            let (congestion_score, base_fee_trend) = if let Some(m) = &self.mempool_monitor {
                if let Ok(stats) = m.get_stats().await {
                    (stats.congestion_score, stats.base_fee_per_gas_trend)
                } else {
                    (0.0, 1.0)
                }
            } else {
                (0.0, 1.0)
            };

            let mut candidates: Vec<(&str, Arc<dyn SubmissionStrategyTrait>, Option<String>)> = Vec::new();
            if let Some(s) = strategies.get("public_mempool") { candidates.push(("public_mempool", s.clone(), None)); }
            if let Some(s) = strategies.get("flashbots") { candidates.push(("flashbots", s.clone(), None)); }
            for relay in &self.config.base_config.private_relays {
                if relay.enabled {
                    if let Some(s) = strategies.get(&relay.name) {
                        candidates.push((relay.name.as_str(), s.clone(), Some(relay.endpoint.clone())));
                    }
                }
            }

            // Filter by MEV protection if hint requested or network is congested and tx looks MEV-sensitive
            let mev_sensitive = tx.data.as_ref().map_or(false, |d| !d.0.is_empty());
            let require_mev_protection = use_flashbots_hint.unwrap_or(false) || (mev_sensitive && (congestion_score > 0.6 || base_fee_trend > 1.05));

            let filtered: Vec<(&str, Arc<dyn SubmissionStrategyTrait>, Option<String>)> = if require_mev_protection {
                candidates.into_iter().filter(|(_, s, _)| s.supports_mev_protection()).collect()
            } else {
                candidates
            };

            // Rank by estimated cost
            let mut best: Option<(&str, U256, Option<String>)> = None;
            for (name, strat, endpoint) in &filtered {
                if let Ok(cost) = strat.estimate_cost(&tx).await {
                    match best {
                        Some((_, c, _)) if cost < c => best = Some((name, cost, endpoint.clone())),
                        None => best = Some((name, cost, endpoint.clone())),
                        _ => {}
                    }
                }
            }

            match best {
                Some(("flashbots", _, _)) if self.flashbots_middleware.is_some() => SubmissionStrategy::Flashbots,
                Some(("public_mempool", _, _)) => SubmissionStrategy::PublicMempool,
                Some((other, _, Some(endpoint))) => SubmissionStrategy::PrivateRelay {
                    name: other.to_string(),
                    endpoint,
                },
                _ => {
                    if use_flashbots_hint.unwrap_or(false) && self.flashbots_middleware.is_some() {
                        SubmissionStrategy::Flashbots
                    } else {
                        SubmissionStrategy::PublicMempool
                    }
                }
            }
        };

        let typed_tx = TypedTransaction::Legacy(tx.clone());
        // Record current gas price for metadata and metrics
        if let Ok(current_gas) = self.gas_oracle
            .get_gas_price(&self.provider.get_chain_name(), None)
            .await
        {
            let effective = current_gas.base_fee.saturating_add(current_gas.priority_fee);
            GAS_PRICE_GAUGE.set(effective.as_u128() as f64 / 1e9);
        }

        // Acquire concurrency permit and submit with retry + circuit breaker bookkeeping
        let _permit = self.execution_semaphore.acquire().await
            .map_err(|_| OptimizerError::Concurrency("Failed to acquire submission permit".to_string()))?;

        let submit_fn = || async {
            self.submit_transaction(typed_tx.clone(), submission_strategy.clone()).await
        };

        let tx_hash = match self.retry_with_backoff(submit_fn, self.config.base_config.max_retry_attempts).await {
            Ok(h) => {
                self.circuit_breaker.record_success().await;
                h
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                return Err(e);
            }
        };

        daily_stats.record_transaction(tx_value).await?;

        let metadata = TransactionMetadata {
            tx: tx.clone(),
            submission_time: Instant::now(),
            gas_price: self.gas_oracle
                .get_gas_price(&self.provider.get_chain_name(), None)
                .await
                .map(|gp| crate::types::TxGasPrice::from_oracle_price(&gp))
                .unwrap_or_else(|_| TxGasPrice::legacy(U256::zero())),
            submission_method: submission_strategy,
            retry_count: 0,
            nonce: tx.nonce.unwrap_or(U256::zero()),
            created_at: SystemTime::now(),
        };

        self.pending_transactions.insert(tx_hash, metadata).await;

        Ok(tx_hash)
    }

    async fn paper_trade(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
        block_number: u64,
        profit_usd: f64,
        gas_cost_usd: f64,
    ) -> Result<H256, OptimizerError> {
        let net_profit_usd = profit_usd - gas_cost_usd;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_else(|_| {
                warn!("System time appears to be before UNIX epoch, using 0 for timestamp");
                0u64
            });

        self.store_paper_trade_data(
            trade_id,
            route,
            block_number,
            profit_usd,
            gas_cost_usd,
            net_profit_usd,
            timestamp,
        ).await?;

        // Generate a unique pseudo-hash for paper trades based on trade parameters
        let route_hash = self.compute_route_hash(route)?;
        let mut hash_data = Vec::new();
        hash_data.extend_from_slice(trade_id.as_bytes());
        hash_data.extend_from_slice(route_hash.as_bytes());
        hash_data.extend_from_slice(&block_number.to_le_bytes());
        hash_data.extend_from_slice(&timestamp.to_le_bytes());
        // Include profit and gas cost for additional uniqueness (converted to integer representation)
        let profit_bits = (profit_usd as f64).to_bits();
        let gas_bits = (gas_cost_usd as f64).to_bits();
        hash_data.extend_from_slice(&profit_bits.to_le_bytes());
        hash_data.extend_from_slice(&gas_bits.to_le_bytes());

        Ok(H256::from_slice(&keccak256(&hash_data)))
    }
}

impl TransactionOptimizerV2 {
    pub async fn start_monitoring_task(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.monitoring.monitor_interval);
        
        loop {
            tokio::select! {
                _ = self.shutdown_signal.notified() => {
                    break;
                }
                _ = interval.tick() => {
                    self.monitor_pending_transactions().await;
                }
            }
        }
    }
}

impl TransactionOptimizerV2 {
    /// Creates a new transaction optimizer with enhanced configuration
    pub async fn new(
        base_config: Arc<TransactionOptimizerSettings>,
        enhanced_config: EnhancedOptimizerConfig,
        provider: Arc<dyn BlockchainManager>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
        price_oracle: Arc<dyn crate::price_oracle::PriceOracle + Send + Sync>,
        mempool_monitor: Option<Arc<dyn MempoolStatsProvider>>,
        arbitrage_executor_address: Address,
    ) -> Result<Arc<Self>, OptimizerError> {
        let execution_semaphore = Arc::new(Semaphore::new(base_config.max_concurrent_submissions));
        let nonce_manager = Arc::new(NonceManager::new(provider.clone()));
        
        let flashbots_middleware = if base_config.use_flashbots {
            Some(Arc::new(Self::create_flashbots_middleware(&base_config, provider.clone()).await?))
        } else {
            None
        };

        let pending_transactions = Arc::new(BoundedPendingTransactions::new(
            enhanced_config.monitoring.max_pending_transactions,
            enhanced_config.monitoring.pending_tx_ttl,
        ));

        let circuit_breaker = Arc::new(CircuitBreaker::new(
            enhanced_config.circuit_breaker_threshold,
            enhanced_config.circuit_breaker_timeout,
        ));

        let cache_size = NonZeroUsize::new(enhanced_config.performance.simulation_cache_size)
            .unwrap_or_else(|| {
                NonZeroUsize::new(100).expect("100 is a valid NonZeroUsize - this should never fail")
            });
        let simulation_cache = Arc::new(Mutex::new(LruCache::<(H256, u64, U256), SimulationResult>::new(cache_size)));

        let daily_stats = Arc::new(DailyTransactionStats::new());

        let optimizer = Self {
            config: Arc::new(enhanced_config),
            provider: provider.clone(),
            gas_oracle,
            price_oracle,
            mempool_monitor,
            flashbots_middleware,
            nonce_manager,
            execution_semaphore,
            pending_transactions,
            shutdown_signal: Arc::new(Notify::new()),
            monitor_handle: Arc::new(RwLock::new(None)),
            arbitrage_executor_address,
            circuit_breaker,
            simulation_cache,
            daily_stats,
            submission_strategies: Arc::new(RwLock::new(HashMap::new())),
        };

        let optimizer = Arc::new(optimizer);
        optimizer.initialize_submission_strategies().await?;

        // Start monitoring task in background and store handle
        let optimizer_clone = optimizer.clone();
        let handle = tokio::spawn(async move {
            optimizer_clone.start_monitoring_task().await;
        });
        *optimizer.monitor_handle.write().await = Some(handle);

        Ok(optimizer)
    }

    /// Initialize submission strategies
    async fn initialize_submission_strategies(&self) -> Result<(), OptimizerError> {
        let mut strategies = self.submission_strategies.write().await;
        
        strategies.insert(
            "public_mempool".to_string(),
            Arc::new(PublicMempoolStrategy {
                provider: self.provider.clone(),
                gas_oracle: self.gas_oracle.clone(),
                nonce_manager: self.nonce_manager.clone(),
            }),
        );

        if let Some(mw) = &self.flashbots_middleware {
            // Register a simple Flashbots single-tx strategy
            #[derive(Clone)]
            struct FlashbotsSingleTxStrategy {
                middleware: Arc<FlashbotsMiddleware<Arc<ethers::providers::Provider<ethers::providers::Http>>, LocalWallet>>,
                provider: Arc<dyn BlockchainManager>,
                nonce_manager: Arc<NonceManager>,
            }
            impl fmt::Debug for FlashbotsSingleTxStrategy {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct("FlashbotsSingleTxStrategy").finish()
                }
            }
            #[async_trait]
            impl SubmissionStrategyTrait for FlashbotsSingleTxStrategy {
                async fn submit(&self, mut tx: TransactionRequest) -> Result<H256, OptimizerError> {
                    let sender = tx.from.unwrap_or_else(|| self.provider.get_wallet().address());
                    let nonce = self.nonce_manager.get_next_nonce(sender).await?;
                    tx.nonce = Some(nonce);
                    tx.from = Some(sender);

                    let typed: TypedTransaction = tx.into();
                    let wallet = self.provider.get_wallet();
                    let sig = wallet.sign_transaction(&typed).await
                        .map_err(|e| OptimizerError::Signing(e.to_string()))?;
                    let raw = typed.rlp_signed(&sig);

        let current_block = self.provider.get_block_number().await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;
                    let target_block = current_block.saturating_add(1);

                    let bundle = ethers_flashbots::BundleRequest::new()
                        .set_block(target_block.into())
                        .push_transaction(raw);

                    let pending = self.middleware.send_bundle(&bundle).await
                        .map_err(|e| OptimizerError::SubmissionFinal(format!("Flashbots single-tx submission failed: {}", e)))?;
                    // Return bundle hash as identifier
                    pending.bundle_hash.ok_or_else(|| OptimizerError::SubmissionFinal("Bundle hash not available".to_string()))
                }
                fn supports_mev_protection(&self) -> bool { true }
                async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError> {
                    self.provider.estimate_gas(tx, None).await
                        .map_err(|e| OptimizerError::GasEstimation(e.to_string()))
                }
            }

            strategies.insert(
                "flashbots".to_string(),
                Arc::new(FlashbotsSingleTxStrategy {
                    middleware: mw.clone(),
                    provider: self.provider.clone(),
                    nonce_manager: self.nonce_manager.clone(),
                }),
            );
        }

        // Register private relays from configuration
        #[derive(Clone)]
        struct PrivateRelayStrategy {
            name: String,
            endpoint: String,
            provider: Arc<dyn BlockchainManager>,
            nonce_manager: Arc<NonceManager>,
        }
        impl fmt::Debug for PrivateRelayStrategy {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("PrivateRelayStrategy").field("name", &self.name).finish()
            }
        }
        #[async_trait]
        impl SubmissionStrategyTrait for PrivateRelayStrategy {
            async fn submit(&self, mut tx: TransactionRequest) -> Result<H256, OptimizerError> {
                let relay_provider = Arc::new(
                    ethers::providers::Provider::<ethers::providers::Http>::try_from(&self.endpoint)
                        .map_err(|e| OptimizerError::Provider(e.to_string()))?
                );

                let sender = tx.from.unwrap_or_else(|| self.provider.get_wallet().address());
                let nonce = self.nonce_manager.get_next_nonce(sender).await?;
                tx.nonce = Some(nonce);
                tx.from = Some(sender);

                let wallet = self.provider.get_wallet();
                let typed_tx: TypedTransaction = tx.into();
                let signature = wallet.sign_transaction(&typed_tx).await
                    .map_err(|e| OptimizerError::Signing(e.to_string()))?;
                let raw_tx = typed_tx.rlp_signed(&signature);

                let pending = relay_provider.send_raw_transaction(raw_tx).await
                    .map_err(|e| OptimizerError::SubmissionFinal(e.to_string()))?;
                Ok(pending.tx_hash())
            }

            fn supports_mev_protection(&self) -> bool { true }

            async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError> {
                self.provider.estimate_gas(tx, None).await
                    .map_err(|e| OptimizerError::GasEstimation(e.to_string()))
            }
        }

        for relay in &self.config.base_config.private_relays {
            if !relay.enabled { continue; }
            strategies.insert(
                relay.name.clone(),
                Arc::new(PrivateRelayStrategy {
                    name: relay.name.clone(),
                    endpoint: relay.endpoint.clone(),
                    provider: self.provider.clone(),
                    nonce_manager: self.nonce_manager.clone(),
                })
            );
        }

        Ok(())
    }

    /// Validates a transaction before submission
    fn validate_transaction(&self, tx: &TransactionRequest) -> Result<(), OptimizerError> {
        // Validate recipient address
        if let Some(to) = &tx.to {
            if let NameOrAddress::Address(addr) = to {
                if self.config.security.blacklisted_addresses.contains(addr) {
                    return Err(OptimizerError::Config("Blacklisted recipient address".to_string()));
                }
                
                if self.config.security.require_whitelist &&
                   !self.config.security.whitelisted_addresses.contains(addr) {
                    return Err(OptimizerError::Config("Recipient not in whitelist".to_string()));
                }
            }
        }
        
        // Validate transaction value
        if let Some(value) = tx.value {
            if value > self.config.security.max_transaction_value {
                return Err(OptimizerError::Config("Transaction value exceeds maximum".to_string()));
            }
        }
        
        // Validate gas limit
        if let Some(gas) = tx.gas {
            if gas > self.config.security.max_gas_limit {
                return Err(OptimizerError::Config("Gas limit exceeds maximum".to_string()));
            }
        }
        
        Ok(())
    }

    /// Retry logic with exponential backoff and jitter
    async fn retry_with_backoff<F, Fut, T>(
        &self,
        f: F,
        max_attempts: u32,
    ) -> Result<T, OptimizerError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, OptimizerError>>,
    {
        let mut delay = Duration::from_millis(100);
        
        for attempt in 0..max_attempts {
            match f().await {
                Ok(result) => {
                    self.circuit_breaker.record_success().await;
                    return Ok(result);
                }
                Err(e) if is_retriable_error_v2(&e) && attempt < max_attempts - 1 => {
                    self.circuit_breaker.record_failure().await;
                    
                    if self.circuit_breaker.is_tripped().await {
                        return Err(OptimizerError::CircuitBreakerOpen("Circuit breaker is open due to excessive failures".to_string()));
                    }
                    
                    // Derive a pseudo-random jitter deterministically from time and attempt
                    let now_nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or_else(|_| {
                            warn!("System time appears to be before UNIX epoch, using 0 for jitter seed");
                            0u64
                        });
                    let mut seed_bytes = Vec::with_capacity(8 + 8 + 8);
                    seed_bytes.extend_from_slice(&now_nanos.to_le_bytes());
                    let attempt_u64 = attempt as u64;
                    seed_bytes.extend_from_slice(&attempt_u64.to_le_bytes());
                    seed_bytes.extend_from_slice(&self.config.performance.retry_jitter_factor.to_bits().to_le_bytes());
                    let h = keccak256(seed_bytes);
                    let r = u64::from_le_bytes([h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]]);
                    let unit = (r as f64) / (u64::MAX as f64);
                    let jitter = unit * self.config.performance.retry_jitter_factor;
                    let jittered_delay = delay.mul_f64(1.0 + jitter);
                    sleep(jittered_delay).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(30));
                }
                Err(e) => {
                    self.circuit_breaker.record_failure().await;
                    return Err(e);
                }
            }
        }
        
        Err(OptimizerError::MaxRetriesExceeded("Maximum retry attempts exceeded".to_string()))
    }

    /// Efficiently check bundle status using event logs
    async fn check_bundle_status_efficient(
        &self,
        _bundle_hash: H256,
        target_block: u64,
    ) -> Result<BundleStatus, OptimizerError> {
        let current_block = self.provider.get_block_number().await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;

        if current_block < target_block {
            return Ok(BundleStatus::Pending);
        }

        match self.config.performance.bundle_status_check_method {
            BundleCheckMethod::EventLogs => {
                self.check_bundle_via_logs(target_block).await
            }
            BundleCheckMethod::BlockScan => {
                self.check_bundle_via_blocks(_bundle_hash, target_block).await
            }
            BundleCheckMethod::Hybrid => {
                // Try logs first (more efficient), fall back to block scan if needed
                match self.check_bundle_via_logs(target_block).await {
                    Ok(status) => Ok(status),
                    Err(_) => self.check_bundle_via_blocks(_bundle_hash, target_block).await,
                }
            }
        }
    }

    /// Check bundle status via event logs (efficient)
    async fn check_bundle_via_logs(&self, target_block: u64) -> Result<BundleStatus, OptimizerError> {
        let filter = Filter::new()
            .from_block(target_block)
            .to_block(target_block + 5)
            .address(self.arbitrage_executor_address);
        
        let logs = self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;
        
        if !logs.is_empty() {
            let tx_results: Vec<TransactionResult> = logs.iter()
                .filter_map(|log| {
                    match log.transaction_hash {
                        Some(tx_hash) => Some(TransactionResult {
                            tx_hash,
                            reverted: false,
                            error_message: None,
                            gas_used: None,
                            effective_gas_price: None,
                        }),
                        None => {
                            warn!("Log entry missing transaction hash, skipping");
                            None
                        }
                    }
                })
                .collect();
            
            Ok(BundleStatus::Included(target_block, tx_results))
        } else {
            Ok(BundleStatus::NotIncluded)
        }
    }

    /// Check bundle status via block scanning (fallback)
    async fn check_bundle_via_blocks(
        &self,
        _bundle_hash: H256,
        target_block: u64,
    ) -> Result<BundleStatus, OptimizerError> {
        let current_block = self.provider.get_block_number().await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;
        
        let check_range = 5u64;
        let end_block = std::cmp::min(current_block, target_block + check_range);
        
        for block_num in target_block..=end_block {
            let http_provider = self.provider
                .get_provider_http()
                .ok_or_else(|| OptimizerError::Provider("HTTP provider not available for block-with-txs query".to_string()))?;
            let block = http_provider
                .get_block_with_txs(block_num)
                .await
                .map_err(|e| OptimizerError::Provider(e.to_string()))?;
            
            if let Some(block) = block {
                let mut found_transactions = Vec::new();
                
                for tx in &block.transactions {
                    if tx.from == self.provider.get_wallet().address() {
                        match http_provider
                            .get_transaction_receipt(tx.hash)
                            .await
                        {
                            Ok(Some(receipt)) => {
                                let reverted = match receipt.status {
                                    Some(status) => status == U64::zero(),
                                    None => true,
                                };

                                let gas_used = match receipt.gas_used {
                                    Some(gas) => gas,
                                    None => {
                                        warn!("Transaction receipt missing gas_used for tx: {:?}", tx.hash);
                                        continue;
                                    }
                                };

                                let tx_result = TransactionResult {
                                    tx_hash: tx.hash,
                                    reverted,
                                    error_message: None,
                                    gas_used: Some(gas_used),
                                    effective_gas_price: receipt.effective_gas_price,
                                };
                                found_transactions.push(tx_result);
                            }
                            Ok(None) => {
                                warn!("No receipt found for transaction: {:?}", tx.hash);
                                continue;
                            }
                            Err(e) => {
                                warn!("Failed to get receipt for transaction {:?}: {}", tx.hash, e);
                                continue;
                            }
                        }
                    }
                }
                
                if !found_transactions.is_empty() {
                    return Ok(BundleStatus::Included(block_num, found_transactions));
                }
            }
        }
        
        if current_block > target_block + check_range {
            Ok(BundleStatus::NotIncluded)
        } else {
            Ok(BundleStatus::Pending)
        }
    }

    /// Store paper trade data using async I/O
    async fn store_paper_trade_data(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
        block_number: u64,
        profit_usd: f64,
        gas_cost_usd: f64,
        net_profit_usd: f64,
        timestamp: u64,
    ) -> Result<(), OptimizerError> {
        let log_file = "paper_trading_log.csv";
        let file_exists = tokio::fs::metadata(log_file).await.is_ok();
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)
            .await
            .map_err(|e| OptimizerError::FileIO(e.to_string()))?;
        
        let mut writer = BufWriter::new(file);
        
        if !file_exists {
            writer.write_all(b"timestamp,trade_id,block_number,route_legs,initial_amount_wei,profit_usd,gas_cost_usd,net_profit_usd,roi_percentage\n")
                .await
                .map_err(|e| OptimizerError::FileIO(e.to_string()))?;
        }
        
        let roi_percentage = if gas_cost_usd > 0.0 {
            (net_profit_usd / gas_cost_usd) * 100.0
        } else {
            0.0
        };
        
        let line = format!(
            "{},{},{},{},{},{:.6},{:.6},{:.6},{:.2}\n",
            timestamp,
            trade_id,
            block_number,
            route.legs.len(),
            route.initial_amount_in_wei,
            profit_usd,
            gas_cost_usd,
            net_profit_usd,
            roi_percentage
        );
        
        writer.write_all(line.as_bytes())
            .await
            .map_err(|e| OptimizerError::FileIO(e.to_string()))?;
        
        writer.flush()
            .await
            .map_err(|e| OptimizerError::FileIO(e.to_string()))?;
        
        debug!(
            target: "paper_trading_storage",
            trade_id,
            block_number,
            profit_usd,
            gas_cost_usd,
            net_profit_usd,
            "Paper trade data stored"
        );
        
        Ok(())
    }

    /// Creates Flashbots middleware with sanitized error handling
    async fn create_flashbots_middleware(
        config: &TransactionOptimizerSettings,
        provider: Arc<dyn BlockchainManager>,
    ) -> Result<FlashbotsMiddleware<Arc<ethers::providers::Provider<ethers::providers::Http>>, LocalWallet>, OptimizerError> {
        let identity = config.flashbots_signing_key.parse::<LocalWallet>()
            .map_err(|_| OptimizerError::Config("Invalid Flashbots signing key format".to_string()))?;

        let inner: Arc<ethers::providers::Provider<ethers::providers::Http>> =
            provider.get_provider_http().ok_or_else(|| 
                OptimizerError::Provider("HTTP provider required for Flashbots inner client".into())
            )?;

        let relay_url: url::Url = config.flashbots_endpoint.parse()
            .map_err(|_| OptimizerError::Config("Invalid Flashbots URL format".to_string()))?;

        Ok(FlashbotsMiddleware::new(inner, relay_url, identity))
    }

    /// Monitor pending transactions with improved efficiency
    async fn monitor_pending_transactions(&self) {
        let pending = self.pending_transactions.get_all().await;
        let mut to_remove = Vec::new();
        let mut to_update = Vec::new();
        
        for (tx_hash, metadata) in pending.iter() {
            // Check if transaction is confirmed
            if let Ok(Some(_receipt)) = self.provider.get_transaction_receipt(*tx_hash).await {
                to_remove.push(*tx_hash);
                TX_CONFIRMED.inc();
                continue;
            }
            
            // Check if transaction is stuck
            if metadata.submission_time.elapsed() > self.config.monitoring.stuck_threshold {
                if metadata.retry_count < self.config.base_config.max_retry_attempts {
                    let new_gas_price = self.bump_gas_price(&metadata.gas_price);
                    
                    let mut bumped_tx = metadata.tx.clone();
                    bumped_tx.nonce = Some(metadata.nonce);
                    
                    match &new_gas_price {
                        TxGasPrice::Legacy { gas_price } => {
                            bumped_tx.gas_price = Some(*gas_price);
                        }
                        TxGasPrice::Eip1559 { max_fee_per_gas, .. } => {
                            bumped_tx.gas_price = Some(*max_fee_per_gas);
                        }
                    }
                    
                    let oracle_gas_price = self.tx_gas_to_oracle_gas(&new_gas_price);
                    
                    if let Ok(new_tx_hash) = self.provider.submit_raw_transaction(bumped_tx.clone(), oracle_gas_price).await {
                        let updated_metadata = TransactionMetadata {
                            tx: bumped_tx,
                            submission_time: Instant::now(),
                            gas_price: new_gas_price,
                            submission_method: metadata.submission_method.clone(),
                            retry_count: metadata.retry_count + 1,
                            nonce: metadata.nonce,
                            created_at: SystemTime::now(),
                        };
                        
                        to_update.push((new_tx_hash, updated_metadata));
                        to_remove.push(*tx_hash);
                        
                        info!("Transaction bumped: old={:?}, new={:?}", tx_hash, new_tx_hash);
                    }
                } else {
                    warn!("Transaction {:?} exceeded max retry attempts", tx_hash);
                    to_remove.push(*tx_hash);
                    TX_FAILED.inc();
                }
            }
        }
        
        // Apply updates
        for tx_hash in to_remove {
            self.pending_transactions.remove(&tx_hash).await;
        }
        
        for (tx_hash, metadata) in to_update {
            self.pending_transactions.insert(tx_hash, metadata).await;
        }
    }

    /// Public shutdown API to gracefully stop monitoring
    pub async fn shutdown(&self) {
        self.shutdown_signal.notify_waiters();
        if let Some(handle) = self.monitor_handle.write().await.take() {
            let _ = handle.await;
        }
    }

    /// Bump gas price for stuck transactions
    fn bump_gas_price(&self, current: &TxGasPrice) -> TxGasPrice {
        let multiplier = U256::from(self.config.monitoring.gas_bump_bps);
        let divisor = U256::from(10000);
        
        match current {
            TxGasPrice::Legacy { gas_price } => TxGasPrice::Legacy {
                gas_price: gas_price.saturating_mul(multiplier) / divisor,
            },
            TxGasPrice::Eip1559 { max_fee_per_gas, max_priority_fee_per_gas } => TxGasPrice::Eip1559 {
                max_fee_per_gas: max_fee_per_gas.saturating_mul(multiplier) / divisor,
                max_priority_fee_per_gas: max_priority_fee_per_gas.saturating_mul(multiplier) / divisor,
            },
        }
    }

    /// Convert TxGasPrice to OracleGasPrice
    fn tx_gas_to_oracle_gas(&self, tx_gas: &TxGasPrice) -> OracleGasPrice {
        match tx_gas {
            TxGasPrice::Legacy { gas_price } => OracleGasPrice {
                base_fee: *gas_price,
                priority_fee: U256::zero(),
            },
            TxGasPrice::Eip1559 { max_fee_per_gas, max_priority_fee_per_gas } => OracleGasPrice {
                base_fee: max_fee_per_gas.saturating_sub(*max_priority_fee_per_gas),
                priority_fee: *max_priority_fee_per_gas,
            },
        }
    }

    fn compute_route_hash(&self, route: &ArbitrageRoute) -> Result<H256, OptimizerError> {
        let serialized = serde_json::to_vec(route)
            .map_err(|e| OptimizerError::Config(format!("Failed to serialize route for hash computation: {}", e)))?;
        Ok(H256::from_slice(&keccak256(&serialized)))
    }
}

// ======================== Nonce Manager ========================

/// Robust, actor-style nonce management system for EVM-compatible blockchains
#[derive(Clone, Debug)]
pub struct NonceManager {
    nonces: Arc<Mutex<BTreeMap<Address, U256>>>,
    provider: Arc<dyn BlockchainManager + Send + Sync>,
    notify: Arc<Notify>,
}

impl NonceManager {
    pub fn new(provider: Arc<dyn BlockchainManager + Send + Sync>) -> Self {
        Self {
            nonces: Arc::new(Mutex::new(BTreeMap::new())),
            provider,
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn get_next_nonce(&self, address: Address) -> Result<U256, OptimizerError> {
        let mut nonces_guard = self.nonces.lock().await;

        let on_chain_nonce = self.provider
            .get_transaction_count(address, None)
            .await
            .map_err(|e| OptimizerError::Provider(format!("Failed to fetch on-chain nonce: {}", e)))?;

        let local_nonce = nonces_guard.get(&address).cloned().unwrap_or(on_chain_nonce);
        let next_nonce = if local_nonce > on_chain_nonce {
            local_nonce
        } else {
            on_chain_nonce
        };

        let incremented = next_nonce.checked_add(U256::one()).ok_or_else(|| {
            OptimizerError::Provider(format!(
                "Nonce overflow for address {:?}: {} + 1",
                address, next_nonce
            ))
        })?;
        nonces_guard.insert(address, incremented);

        Ok(next_nonce)
    }

    pub async fn mark_nonce_used(&self, address: Address, nonce: U256) {
        let mut nonces_guard = self.nonces.lock().await;
        let current = nonces_guard.get(&address).cloned().unwrap_or(U256::zero());
        let next = nonce.checked_add(U256::one()).unwrap_or(U256::MAX);
        if next > current {
            nonces_guard.insert(address, next);
        }
        self.notify.notify_waiters();
    }

    pub async fn reset_nonce(&self, address: Address) {
        let mut nonces_guard = self.nonces.lock().await;
        nonces_guard.remove(&address);
        self.notify.notify_waiters();
    }

    pub async fn get_local_nonce(&self, address: Address) -> Option<U256> {
        let nonces_guard = self.nonces.lock().await;
        nonces_guard.get(&address).cloned()
    }

    pub async fn wait_for_update(&self) {
        self.notify.notified().await;
    }
}

// ======================== Helper Functions ========================

fn is_retriable_error_v2(e: &OptimizerError) -> bool {
    match e {
        OptimizerError::Provider(msg) => {
            let msg_lower = msg.to_lowercase();
            msg_lower.contains("nonce") ||
            msg_lower.contains("replacement") ||
            msg_lower.contains("underpriced") ||
            msg_lower.contains("connection") ||
            msg_lower.contains("timeout") ||
            msg_lower.contains("temporary")
        }
        OptimizerError::Timeout(_) => true,
        OptimizerError::Config(msg) => {
            msg == "Circuit breaker is open" || msg == "Maximum retries exceeded"
        }
        _ => false,
    }
}

// ======================== Error Types Extension ========================

impl OptimizerError {
    pub fn circuit_breaker_open() -> Self {
        OptimizerError::Config("Circuit breaker is open".to_string())
    }
    
    pub fn max_retries_exceeded() -> Self {
        OptimizerError::Config("Maximum retries exceeded".to_string())
    }
}
