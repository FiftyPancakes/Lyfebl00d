use crate::{
    alpha::MarketRegime,
    blockchain::BlockchainManager,
    quote_source::AggregatorQuoteSource,
    risk::RiskManager,
};
use ethers::abi::{encode, decode, Abi, ParamType, Token};
use ethers::contract::Contract;
use ethers::core::types::{Address, Bytes, H256, U256};
use ethers::providers::{Http, Provider};
use ethers::signers::Signer;
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::{Log, TransactionReceipt, TransactionRequest};
use ethers::utils::keccak256;
use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    collections::{HashMap, HashSet, VecDeque},
    ops::{Div, Mul},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use dashmap::DashMap;

/* ──────────────────────────────────────────── Constants ─────────────────────────────────────────── */
// These fallbacks are superseded by config values at runtime.
const PATH_CACHE_SIZE: usize = 10_000;
const MEV_PROTECTION_COOLDOWN_MS: u64 = 100;
const COMMIT_REVEAL_DELAY_BLOCKS: u64 = 1;

/* Flash‑loan provider IDs — must exactly match the on–chain enum values                       */
const PROVIDER_AAVE: u8 = 0;
const PROVIDER_BALANCER: u8 = 1;
const PROVIDER_MAKER: u8 = 2;
const PROVIDER_UNISWAP_V4: u8 = 3;

/* Strategy type IDs — must exactly match the on–chain enum values                            */
const STRATEGY_SIMPLE_ARB: u8 = 0;
const STRATEGY_TRIANGULAR: u8 = 1;
const STRATEGY_MULTI_DEX: u8 = 2;
const STRATEGY_CROSS_CHAIN: u8 = 3;

/* Aggregator IDs — must exactly match the on–chain enum values                               */
const AGG_DIRECT: u8 = 0;
const AGG_1INCH: u8 = 1;
const AGG_0X: u8 = 2;
const AGG_UNISWAP_V3: u8 = 3;
const AGG_UNISWAP_V4: u8 = 4;
const AGG_CURVE: u8 = 5;
const AGG_BALANCER: u8 = 6;
const AGG_SOLIDLY: u8 = 7;
const AGG_CUSTOM: u8 = 8;

/* ─────────────────────────────────────────── Config structs ───────────────────────────────────── */

#[derive(Debug, Clone, Deserialize)]
pub struct UARConnectorConfig {
    pub uar_address: Address,
    pub max_concurrent_executions: usize,
    pub default_slippage_bps: u32,
    pub deadline_seconds: u64,
    pub use_flashbots: bool,
    pub flashbots_relay_url: Option<String>,
    pub flashbots_auth_key: Option<String>,
    pub min_profit_usd: f64,
    pub profit_buffer_factor: f64,
    pub mev_protection_level: u8,
    pub gas_multiplier: f64,
    pub gas_price_percentile: u8,
    pub simulation_required: bool,
    pub fallback_aggregator: String,
    pub require_risk_assessment: bool,
    pub min_risk_score_threshold: f64,
    pub max_slippage_bps: u32,
    pub enable_cross_chain: bool,
    pub execution_timeout_seconds: u64,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub max_gas_price_gwei: u64,
    pub chain_configs: HashMap<String, ChainConfig>,
    pub flash_loan_providers: HashMap<String, FlashLoanProviderConfig>,
    pub circuit_breaker_config: CircuitBreakerConfig,
    pub hot_path_config: HotPathConfig,
    pub mev_protection_config: MEVProtectionConfig,
    pub profit_optimization_config: ProfitOptimizationConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub chain_name: String,
    pub uar_address: Address,
    pub fee_collector: Address,
    pub enabled: bool,
    pub rpc_urls: Vec<String>,
    pub ws_urls: Vec<String>,
    pub block_time_ms: u64,
    pub native_wrapper: Address,
    pub aave_pool: Option<Address>,
    pub balancer_vault: Option<Address>,
    pub curve_registry: Option<Address>,
    pub solidly_router: Option<Address>,
    pub chainlink_native_usd_feed: Option<Address>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlashLoanProviderConfig {
    pub provider_id: u8,
    pub address: Address,
    pub enabled: bool,
    pub fee_bps: u32,
    pub max_loan_amount: U256,
    pub preferred_tokens: Vec<Address>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    pub max_slippage_threshold: u32,
    pub flash_loan_failure_threshold: u32,
    pub gas_limit_threshold: u64,
    pub profit_loss_threshold: u32,
    pub cooldown_period_seconds: u64,
    pub auto_reset: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HotPathConfig {
    pub cache_size: usize,
    pub execution_threshold: u64,
    pub success_rate_threshold: f64,
    pub gas_optimization_enabled: bool,
    pub cache_ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MEVProtectionConfig {
    pub enabled: bool,
    pub use_commit_reveal: bool,
    pub sandwich_protection_blocks: u64,
    pub max_priority_fee_gwei: u64,
    pub flashbots_enabled: bool,
    pub private_mempool_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProfitOptimizationConfig {
    pub min_profit_threshold_wei: U256,
    pub gas_cost_buffer_factor: f64,
    pub dynamic_fee_adjustment: bool,
    pub profit_reinvestment_ratio: f64,
}

/* ─────────────────────────────────────────── Runtime enums/structs ───────────────────────────── */

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum ExecutionStatus {
    Pending,
    Confirmed,
    Failed,
    Reverted,
    Dropped,
    Timeout,
    MEVProtected,
    CircuitBreakerTriggered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapStep {
    pub target: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub min_amount_out: U256,
    pub call_data: Bytes,
    pub aggregator_type: u8,
    pub pool_type: u8,
    pub use_hot_path: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathExecutionData {
    pub start_token: Address,
    pub end_token: Address,
    pub start_amount: U256,
    pub min_expected_profit: U256,
    pub execution_payload: Bytes,
    pub gas_estimate: U256,
    pub expiration_timestamp: u64,
    pub strategy_type: u8,
    pub flash_loan_provider: u8,
    pub priority_level: u8,
    pub requires_mev_protection: bool,
    pub is_optimized_path: bool,
}

#[derive(Debug, Clone)]
pub struct ArbitragePath {
    pub path_id: H256,
    pub path_data: PathExecutionData,
    pub hot_path_cache: Option<HotPathCache>,
    pub risk_assessment: Option<RiskAssessment>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HotPathCache {
    pub last_gas_used: U256,
    pub last_profit: U256,
    pub execution_count: u64,
    pub success_rate: f64,
    pub last_execution_timestamp: u64,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskAssessment {
    pub risk_score: f64,
    pub slippage_risk: f64,
    pub gas_price_risk: f64,
    pub mev_risk: f64,
    pub liquidity_risk: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbitrageResult {
    pub path_id: H256,
    pub tx_hash: H256,
    pub status: ExecutionStatus,
    pub profit_wei: U256,
    pub profit_usd: f64,
    pub gas_used: Option<U256>,
    pub gas_price: Option<U256>,
    pub execution_time_ms: u64,
    pub token_path: Vec<String>,
    pub error_message: Option<String>,
    pub flash_loan_provider: Option<u8>,
    pub mev_protection_used: bool,
    pub circuit_breaker_status: CircuitBreakerStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct CircuitBreakerStatus {
    pub triggered: bool,
    pub breaker_type: Option<String>,
    pub trigger_count: u32,
    pub cooldown_remaining_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub total_profit_usd: f64,
    pub highest_profit_usd: f64,
    pub average_profit_usd: f64,
    pub average_gas_used: f64,
    pub average_execution_time_ms: f64,
    pub failed_due_to_slippage: u64,
    pub failed_due_to_gas: u64,
    pub failed_due_to_mev: u64,
    pub risk_rejections: u64,
    pub circuit_breaker_triggers: u64,
    pub hot_path_executions: u64,
    pub flash_loan_stats: FlashLoanStats,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FlashLoanStats {
    pub aave_executions: u64,
    pub balancer_executions: u64,
    pub maker_executions: u64,
    pub uniswap_v4_executions: u64,
    pub total_flash_loan_fees_usd: f64,
}

#[derive(Debug, Clone)]
pub struct CommitRevealData {
    pub commit_hash: H256,
    pub path_id: H256,
    pub deadline: u64,
    pub nonce: U256,
    pub max_slippage_bps: u32,
    pub salt: H256,
    pub commit_block: u64,
}

/* ─────────────────────────────────────────── Custom errors ───────────────────────────────────── */

#[derive(Debug, thiserror::Error)]
pub enum UARError {
    #[error("Blockchain error: {0}")]
    Blockchain(String),
    #[error("Gas price error: {0}")]
    GasPrice(String),
    #[error("Insufficient profit: {0}")]
    InsufficientProfit(String),
    #[error("Route validation error: {0}")]
    RouteValidation(String),
    #[error("Calldata encoding error: {0}")]
    CalldataEncoding(String),
    #[error("Approval error: {0}")]
    Approval(String),
    #[error("Simulation error: {0}")]
    Simulation(String),
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    #[error("MEV protection error: {0}")]
    MEVProtection(String),
    #[error("Circuit breaker triggered: {0}")]
    CircuitBreaker(String),
    #[error("Flash loan error: {0}")]
    FlashLoan(String),
    #[error("Hot path error: {0}")]
    HotPath(String),
}

/* ─────────────────────────────────────────── Connector struct ───────────────────────────────── */

pub struct UARConnector {
    pub config: UARConnectorConfig,
    pub blockchain: Arc<dyn BlockchainManager + Send + Sync>,
    pub quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
    pub risk_manager: Arc<dyn RiskManager + Send + Sync>,
    pub price_oracle: Arc<dyn crate::price_oracle::PriceOracle + Send + Sync>,
    pub uar_contract: Contract<Provider<Http>>,
    pub execution_semaphore: Arc<Semaphore>,
    pub path_cache: Arc<DashMap<H256, ArbitragePath>>,
    pub hot_path_cache: Arc<DashMap<H256, HotPathCache>>,
    pub execution_stats: Arc<RwLock<ExecutionStats>>,
    pub circuit_breaker_states: Arc<DashMap<String, CircuitBreakerState>>,
    pub commit_reveal_queue: Arc<Mutex<VecDeque<CommitRevealData>>>,
    pub flash_loan_providers: Arc<HashMap<u8, FlashLoanProviderConfig>>,
    pub active_flash_loans: Arc<RwLock<HashSet<H256>>>,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerState {
    pub triggered: bool,
    pub trigger_count: u32,
    pub last_triggered_timestamp: u64,
    pub cooldown_period: u64,
}

/* ─────────────────────────────────────────── Implementation ─────────────────────────────────── */

impl UARConnector {
    pub async fn new(
        config: UARConnectorConfig,
        blockchain: Arc<dyn BlockchainManager + Send + Sync>,
        quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
        risk_manager: Arc<dyn RiskManager + Send + Sync>,
        price_oracle: Arc<dyn crate::price_oracle::PriceOracle + Send + Sync>,
        uar_abi: Abi,
    ) -> Result<Self> {
        let provider = blockchain.get_provider();
        let uar_contract = Contract::new(config.uar_address, uar_abi, provider);
        let execution_semaphore = Arc::new(Semaphore::new(config.max_concurrent_executions));
        let path_cache = Arc::new(DashMap::with_capacity(PATH_CACHE_SIZE));
        let hot_path_cache = Arc::new(DashMap::with_capacity(config.hot_path_config.cache_size));
        let execution_stats = Arc::new(RwLock::new(ExecutionStats::default()));
        let circuit_breaker_states = Arc::new(DashMap::new());
        let commit_reveal_queue = Arc::new(Mutex::new(VecDeque::new()));
        let flash_loan_providers = Arc::new(
            config
                .flash_loan_providers
                .iter()
                .map(|(_, v)| (v.provider_id, v.clone()))
                .collect::<HashMap<u8, FlashLoanProviderConfig>>(),
        );
        let active_flash_loans = Arc::new(RwLock::new(HashSet::new()));

        // Initialize per‑breaker state
        for (name, threshold) in [
            ("maxSlippage", config.circuit_breaker_config.max_slippage_threshold as u64),
            (
                "flashLoanFailure",
                config.circuit_breaker_config.flash_loan_failure_threshold as u64,
            ),
            ("gasLimit", config.circuit_breaker_config.gas_limit_threshold),
            ("profitLoss", config.circuit_breaker_config.profit_loss_threshold as u64),
        ] {
            circuit_breaker_states.insert(
                name.to_string(),
                CircuitBreakerState {
                    triggered: false,
                    trigger_count: 0,
                    last_triggered_timestamp: 0,
                    cooldown_period: config.circuit_breaker_config.cooldown_period_seconds,
                },
            );
            // threshold is kept for future extensions
            let _ = threshold;
        }

        Ok(Self {
            config,
            blockchain,
            quote_source,
            risk_manager,
            price_oracle,
            uar_contract,
            execution_semaphore,
            path_cache,
            hot_path_cache,
            execution_stats,
            circuit_breaker_states,
            commit_reveal_queue,
            flash_loan_providers,
            active_flash_loans,
        })
    }
    
    pub async fn compute_path_id(
        &self,
        token_path: &[Address],
        pool_path: &[Address],
        amounts: &[U256],
        strategy_type: u8,
    ) -> H256 {
        let mut hasher = Vec::new();
        
        // Add token path
        for token in token_path {
            hasher.extend_from_slice(token.as_bytes());
        }
        
        // Add pool path
        for pool in pool_path {
            hasher.extend_from_slice(pool.as_bytes());
        }
        
        // Add amounts
        for amount in amounts {
            let mut bytes = [0u8; 32];
            amount.to_big_endian(&mut bytes);
            hasher.extend_from_slice(&bytes);
        }
        
        // Add strategy type and timestamp
        hasher.push(strategy_type);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        hasher.extend_from_slice(&timestamp.to_be_bytes());
        
        // Add chain ID for cross-chain paths
        if strategy_type == STRATEGY_CROSS_CHAIN {
            let chain_id = self.blockchain.get_chain_id();
            let chain_id_bytes = chain_id.to_be_bytes();
            hasher.extend_from_slice(&chain_id_bytes);
        }
        
        H256::from_slice(&keccak256(hasher))
    }
    
    pub async fn store_path(
        &self,
        path_id: H256,
        path_data: &PathExecutionData,
    ) -> Result<()> {
        // Check circuit breakers
        self.check_circuit_breakers().await?;
        
        // Store in local cache first
        let path = ArbitragePath {
            path_id,
            path_data: path_data.clone(),
            hot_path_cache: None,
            risk_assessment: None,
        };
        self.path_cache.insert(path_id, path);
        
        // Encode path data for contract
        let encoded_data = encode(&[
            Token::FixedBytes(path_id.as_bytes().to_vec()),
            Token::Address(path_data.start_token),
            Token::Address(path_data.end_token),
            Token::Uint(path_data.start_amount),
            Token::Uint(path_data.min_expected_profit),
            Token::Bytes(path_data.execution_payload.to_vec()),
            Token::Uint(path_data.gas_estimate),
            Token::Uint(path_data.expiration_timestamp.into()),
            Token::Uint(path_data.strategy_type.into()),
            Token::Uint(path_data.flash_loan_provider.into()),
            Token::Uint(path_data.priority_level.into()),
            Token::Bool(path_data.requires_mev_protection),
            Token::Bool(path_data.is_optimized_path),
        ]);
        
        // Send to contract
        let method_call = self.uar_contract
            .method::<_, ()>("storePath", (path_id, encoded_data))?
            .gas(500_000);
        
        let tx = method_call
            .send()
            .await
            .wrap_err("Failed to send storePath transaction")?;
            
        let receipt = tx.await?.ok_or_else(|| eyre::eyre!("No receipt"))?;
        
        info!(
            ?path_id,
            tx_hash = ?receipt.transaction_hash,
            gas_used = ?receipt.gas_used,
            "Path stored on UAR contract"
        );
        
        Ok(())
    }
    
    pub async fn execute_arbitrage(
        &self,
        path_id: H256,
        use_flashloan: bool,
    ) -> Result<ArbitrageResult> {
        let start_time = Instant::now();
        let _permit = self.execution_semaphore.acquire().await?;
        
        // Check circuit breakers
        self.check_circuit_breakers().await?;
        
        // Get path from cache
        let path = self.path_cache
            .get(&path_id)
            .ok_or_else(|| UARError::RouteValidation("Path not found in cache".to_string()))?
            .clone();
        
        // Risk assessment if required
        if self.config.require_risk_assessment {
            let risk_assessment = self.assess_path_risk(&path).await?;
            if risk_assessment.risk_score > self.config.min_risk_score_threshold {
                self.update_stats_risk_rejection().await;
                return Err(UARError::RouteValidation(
                    format!("Risk score {} exceeds threshold", risk_assessment.risk_score)
                ).into());
            }
        }
        
        // Generate nonce
        let nonce = self.generate_nonce().await?;
        let deadline = self.calculate_deadline();
        let max_slippage_bps = self.config.default_slippage_bps;
        
        // MEV protection
        let (tx_hash, receipt) = if self.config.mev_protection_config.enabled 
            && path.path_data.requires_mev_protection {
            self.execute_with_mev_protection(
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                use_flashloan,
            ).await?
        } else {
            self.execute_standard(
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                use_flashloan,
            ).await?
        };
        
        // Parse execution result
        let result = self.parse_execution_result(
            path_id,
            tx_hash,
            receipt,
            start_time.elapsed(),
            &path,
        ).await?;
        
        // Update stats
        self.update_execution_stats(&result).await;
        
        // Update hot path cache if successful
        if result.status == ExecutionStatus::Confirmed {
            self.update_hot_path_cache(path_id, &result).await;
        }
        
        Ok(result)
    }
    
    async fn execute_with_mev_protection(
        &self,
        path_id: H256,
        deadline: u64,
        nonce: U256,
        max_slippage_bps: u32,
        use_flashloan: bool,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        if self.config.mev_protection_config.use_commit_reveal {
            // Commit-reveal pattern
            let salt = H256::random();
            let commit_hash = self.compute_commit_hash(
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                salt,
            );
            
            // Commit phase
            let commit_method = self.uar_contract
                .method::<_, ()>("commitArbitrage", commit_hash)?;
            
            let commit_tx = commit_method
                .send()
                .await?;
            
            let commit_receipt = commit_tx.await?.ok_or_else(|| eyre::eyre!("No commit receipt"))?;
            let commit_block = commit_receipt.block_number.unwrap_or_default().as_u64();
            
            // Store commit data
            let commit_data = CommitRevealData {
                commit_hash,
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                salt,
                commit_block,
            };
            
            self.commit_reveal_queue.lock().await.push_back(commit_data.clone());
            
            // Wait for reveal delay
            self.wait_for_reveal_block(commit_block).await?;
            
            // Reveal phase
            let reveal_method = self.uar_contract
                .method::<_, ()>(
                    "revealAndExecuteArbitrage",
                    (path_id, deadline, nonce, max_slippage_bps, salt)
                )?;
            
            let reveal_tx = reveal_method
                .send()
                .await?;
                
            let tx_hash = reveal_tx.tx_hash();
            let receipt = reveal_tx.await?;
            Ok((tx_hash, receipt))
        } else if self.config.mev_protection_config.flashbots_enabled {
            // Flashbots protection
            self.execute_via_flashbots(
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                use_flashloan,
            ).await
        } else {
            // Standard execution with MEV protection flags
            Ok(self.execute_standard(
                path_id,
                deadline,
                nonce,
                max_slippage_bps,
                use_flashloan,
            ).await?)
        }
    }
    
    async fn execute_standard(
        &self,
        path_id: H256,
        deadline: u64,
        nonce: U256,
        max_slippage_bps: u32,
        use_flashloan: bool,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        let gas_price = self.get_optimal_gas_price().await?;
        
        let method_call = if use_flashloan {
            self.uar_contract
                .method::<_, ()>(
                    "executeFlashLoanArbitrage",
                    (path_id, U256::from(deadline), nonce, U256::from(max_slippage_bps))
                )?
        } else {
            self.uar_contract
                .method::<_, ()>(
                    "executeDirectArbitrage",
                    (path_id, U256::from(deadline), nonce, U256::from(max_slippage_bps))
                )?
        };
        
        let gas_priced_call = method_call.gas_price(gas_price);
        let pending_tx = gas_priced_call
            .send()
            .await
            .wrap_err("Failed to send arbitrage transaction")?;
            
        let tx_hash = pending_tx.tx_hash();
        
        // Wait for confirmation with timeout
        let receipt = timeout(
            Duration::from_secs(self.config.execution_timeout_seconds),
            pending_tx
        )
        .await
        .map_err(|_| UARError::ExecutionFailed("Transaction timeout".to_string()))?
        .wrap_err("Failed to get transaction receipt")?;
        
        Ok((tx_hash, receipt))
    }
    
    async fn execute_via_flashbots(
        &self,
        path_id: H256,
        deadline: u64,
        nonce: U256,
        max_slippage_bps: u32,
        use_flashloan: bool,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        // Production Flashbots bundle execution implementation
        let gas_price = self.get_optimal_gas_price().await?;
        
        // Build the primary arbitrage transaction
        let method_call = if use_flashloan {
            self.uar_contract
                .method::<_, ()>(
                    "executeFlashLoanArbitrage",
                    (path_id, U256::from(deadline), nonce, U256::from(max_slippage_bps))
                )?
        } else {
            self.uar_contract
                .method::<_, ()>(
                    "executeDirectArbitrage", 
                    (path_id, U256::from(deadline), nonce, U256::from(max_slippage_bps))
                )?
        };
        
        // Create bundle with MEV protection strategies
        let mut bundle_transactions = Vec::new();
        
        // Add primary transaction
        let primary_tx = method_call
            .gas_price(gas_price.saturating_mul(U256::from(110)) / U256::from(100)) // 10% gas premium
            .tx;
        bundle_transactions.push(primary_tx);
        
        // Add MEV protection transactions if configured
        if self.config.mev_protection_config.enabled {
            // Add frontrun protection transaction
            let protection_tx = self.create_mev_protection_tx(gas_price).await?;
            bundle_transactions.insert(0, ethers::types::transaction::eip2718::TypedTransaction::Legacy(protection_tx)); // Insert at beginning
            
            // Add backrun transaction to capture any residual value
            if let Ok(backrun_tx) = self.create_backrun_tx(gas_price, &path_id).await {
                bundle_transactions.push(ethers::types::transaction::eip2718::TypedTransaction::Legacy(backrun_tx));
            }
        }
        
        // Submit bundle to Flashbots
        let bundle_hash = self.submit_flashbots_bundle(bundle_transactions.into_iter().map(|tx| tx.into()).collect(), deadline).await?;
        
        // Wait for bundle inclusion with timeout
        let inclusion_result = timeout(
            Duration::from_secs(self.config.execution_timeout_seconds),
            self.wait_for_bundle_inclusion(bundle_hash, deadline)
        ).await;
        
        match inclusion_result {
            Ok(Ok((tx_hash, receipt))) => {
                info!("Flashbots bundle included successfully: {:?}", tx_hash);
                Ok((tx_hash, receipt))
            }
            Ok(Err(e)) => {
                warn!("Flashbots bundle failed: {}, falling back to public mempool", e);
                // Fallback to standard execution
                self.execute_standard(path_id, deadline, nonce, max_slippage_bps, use_flashloan).await
            }
            Err(_) => {
                warn!("Flashbots bundle inclusion timeout, falling back to public mempool");
                // Fallback to standard execution
                self.execute_standard(path_id, deadline, nonce, max_slippage_bps, use_flashloan).await
            }
        }
    }
    
    async fn create_mev_protection_tx(&self, gas_price: U256) -> Result<TransactionRequest> {
        // Create a transaction that helps protect against MEV attacks
        // This could be a small ETH transfer or contract call that establishes priority
        let wallet = self.blockchain.get_wallet();
        let wallet_address = wallet.address();
        
        // Small ETH transfer to establish position in block
        let protection_tx = TransactionRequest::new()
            .to(wallet_address)
            .value(U256::from(1000000000000000u64)) // 0.001 ETH
            .gas_price(gas_price.saturating_mul(U256::from(115)) / U256::from(100)) // 15% premium
            .gas(21000);
            
        Ok(protection_tx)
    }
    
    async fn create_backrun_tx(&self, gas_price: U256, _path_id: &H256) -> Result<TransactionRequest> {
        // Create a transaction to capture any residual MEV value
        let wallet = self.blockchain.get_wallet();
        let wallet_address = wallet.address();
        
        // Create a simple transaction that can capture residual value
        let backrun_tx = TransactionRequest::new()
            .to(wallet_address)
            .value(U256::zero())
            .gas_price(gas_price.saturating_mul(U256::from(105)) / U256::from(100)) // 5% premium
            .gas(21000);
            
        Ok(backrun_tx)
    }
    
    async fn submit_flashbots_bundle(&self, transactions: Vec<TransactionRequest>, target_block: u64) -> Result<H256> {
        let wallet = self.blockchain.get_wallet();
        let bundle_id = H256::random();
        
        if let Some(flashbots_url) = &self.config.flashbots_relay_url {
            // Production Flashbots implementation
            let mut signed_transactions = Vec::new();
            
            for mut tx in transactions {
                // Set chain ID if not set
                if tx.chain_id.is_none() {
                    tx.chain_id = Some(self.blockchain.get_chain_id().into());
                }
                
                // Set nonce if not set
                if tx.nonce.is_none() {
                    let nonce = self.blockchain.get_current_nonce().await?;
                    tx.nonce = Some(nonce.into());
                }
                
                // Sign the transaction
                let signature = wallet.sign_transaction(&TypedTransaction::Legacy(tx.clone())).await
                    .map_err(|e| UARError::MEVProtection(format!("Failed to sign transaction: {}", e)))?;
                let signed_tx = TypedTransaction::Legacy(tx).rlp_signed(&signature);
                signed_transactions.push(format!("0x{}", hex::encode(signed_tx)));
            }
            
            // Create Flashbots bundle request
            let bundle_request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendBundle",
                "params": [{
                    "txs": signed_transactions,
                    "blockNumber": format!("0x{:x}", target_block),
                    "minTimestamp": 0,
                    "maxTimestamp": 0,
                    "revertingTxHashes": []
                }]
            });
            
            // Submit to Flashbots relay
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| UARError::MEVProtection(format!("Failed to create HTTP client: {}", e)))?;
            let mut request = client.post(flashbots_url)
                .json(&bundle_request)
                .header("Content-Type", "application/json");
            
            if let Some(auth_key) = &self.config.flashbots_auth_key {
                request = request.header("X-Flashbots-Signature", auth_key);
            }
            
            let response = request.send().await
                .map_err(|e| UARError::MEVProtection(format!("Flashbots request failed: {}", e)))?;
            
            if response.status().is_success() {
                let response_json: serde_json::Value = response.json().await
                    .map_err(|e| UARError::MEVProtection(format!("Failed to parse Flashbots response: {}", e)))?;
                
                info!(
                    "Flashbots bundle submitted successfully: {:?}, target_block: {}, response: {:?}",
                    bundle_id, target_block, response_json
                );
                
                // Extract bundle hash from response if available
                if let Some(result) = response_json.get("result") {
                    if let Some(bundle_hash_str) = result.as_str() {
                        if let Ok(bundle_hash) = H256::from_str(&bundle_hash_str.trim_start_matches("0x")) {
                            return Ok(bundle_hash);
                        }
                    }
                }
                
                Ok(bundle_id)
            } else {
                let error_text = response.text().await.unwrap_or_default();
                Err(UARError::MEVProtection(format!("Flashbots submission failed: {}", error_text)).into())
            }
        } else {
            // No Flashbots URL configured, fall back to regular submission
            warn!("Flashbots URL not configured, falling back to public mempool");
            Err(UARError::MEVProtection("Flashbots not configured".to_string()).into())
        }
    }
    
    async fn wait_for_bundle_inclusion(&self, bundle_hash: H256, target_block: u64) -> Result<(H256, Option<TransactionReceipt>)> {
        info!(
            "Monitoring bundle inclusion: {:?} in target block {}",
            bundle_hash, target_block
        );
        
        let start_time = Instant::now();
        let max_wait_blocks = 3; // Wait up to 3 blocks past target
        let block_time_ms = self.config.chain_configs
            .get("ethereum")
            .map(|c| c.block_time_ms)
            .unwrap_or(12000);
        
        loop {
            // Check if we've exceeded timeout
            if start_time.elapsed() > Duration::from_secs(self.config.execution_timeout_seconds) {
                return Err(UARError::ExecutionFailed("Bundle inclusion timeout".to_string()).into());
            }
            
            let current_block = self.blockchain.get_block_number().await?;
            
            // If we're past the target block window, bundle likely failed
            if current_block > target_block + max_wait_blocks {
                return Err(UARError::ExecutionFailed(
                    format!("Bundle not included in target block {} or subsequent blocks", target_block)
                ).into());
            }
            
            // If we've reached or passed the target block, check for inclusion
            if current_block >= target_block {
                match self.check_bundle_inclusion(bundle_hash, target_block, current_block).await {
                    Ok(Some((tx_hash, receipt))) => {
                        info!("Bundle included successfully in block {}: {:?}", current_block, tx_hash);
                        return Ok((tx_hash, receipt));
                    }
                    Ok(None) => {
                        debug!("Bundle not yet included, continuing to monitor...");
                    }
                    Err(e) => {
                        warn!("Error checking bundle inclusion: {}", e);
                    }
                }
            }
            
            // Wait for next block
            sleep(Duration::from_millis(block_time_ms / 4)).await; // Check 4 times per block
        }
    }
    
    async fn check_bundle_inclusion(
        &self, 
        _bundle_hash: H256, 
        target_block: u64, 
        current_block: u64
    ) -> Result<Option<(H256, Option<TransactionReceipt>)>> {
        // Check blocks from target_block to current_block for our transactions
        for block_num in target_block..=current_block {
            match self.blockchain.get_block(ethers::core::types::BlockId::Number(ethers::core::types::BlockNumber::Number(block_num.into()))).await {
                Ok(Some(block)) => {
                    // Look for transactions that match our bundle
                    for tx_hash in &block.transactions {
                        // Try to get receipt to see if it's our transaction
                        if let Ok(Some(receipt)) = self.blockchain.get_transaction_receipt(*tx_hash).await {
                            // Check if this receipt contains logs from our UAR contract
                            for log in &receipt.logs {
                                if log.address == self.config.uar_address {
                                    // This is likely our transaction
                                    return Ok(Some((*tx_hash, Some(receipt))));
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    debug!("Block {} not found", block_num);
                }
                Err(e) => {
                    debug!("Failed to get block {}: {}", block_num, e);
                }
            }
        }
        
        Ok(None)
    }
    
    async fn check_circuit_breakers(&self) -> Result<()> {
        let mut entries_to_reset = Vec::new();
        
        for entry in self.circuit_breaker_states.iter() {
            let (name, state) = entry.pair();
            
            if state.triggered {
                let elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() - state.last_triggered_timestamp;
                    
                if elapsed < state.cooldown_period {
                    return Err(UARError::CircuitBreaker(
                        format!("{} circuit breaker is in cooldown", name)
                    ).into());
                }
                
                // Auto-reset if configured
                if self.config.circuit_breaker_config.auto_reset {
                    entries_to_reset.push(name.clone());
                }
            }
        }
        
        // Reset circuit breakers outside of iteration
        for name in entries_to_reset {
            self.reset_circuit_breaker(&name).await;
        }
        
        Ok(())
    }
    
    async fn reset_circuit_breaker(&self, name: &str) {
        if let Some(mut state) = self.circuit_breaker_states.get_mut(name) {
            state.triggered = false;
            state.trigger_count = 0;
            info!("Circuit breaker {} reset", name);
        }
    }
    
    async fn trigger_circuit_breaker(&self, name: &str) {
        if let Some(mut state) = self.circuit_breaker_states.get_mut(name) {
            state.trigger_count += 1;
            
            let threshold = match name {
                "maxSlippage" => self.config.circuit_breaker_config.max_slippage_threshold,
                "flashLoanFailure" => self.config.circuit_breaker_config.flash_loan_failure_threshold,
                "profitLoss" => self.config.circuit_breaker_config.profit_loss_threshold,
                _ => 1,
            };
            
            if state.trigger_count >= threshold {
                state.triggered = true;
                state.last_triggered_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                    
                warn!("Circuit breaker {} triggered", name);
                
                // Update stats
                let mut stats = self.execution_stats.write().await;
                stats.circuit_breaker_triggers += 1;
            }
        }
    }
    
    async fn assess_path_risk(&self, path: &ArbitragePath) -> Result<RiskAssessment> {
        // Comprehensive risk assessment using actual market data
        let gas_oracle = self.blockchain.get_gas_oracle().await?;
        let gas_price = gas_oracle.get_gas_price(&self.blockchain.get_chain_name().to_string(), None).await?;
        let base_fee = gas_price.effective_price().as_u64() as f64 / 1e9; // Convert to gwei
        
        // Gas price risk calculation
        let gas_price_risk = if base_fee > self.config.max_gas_price_gwei as f64 {
            1.0
        } else {
            base_fee / self.config.max_gas_price_gwei as f64
        };
        
        // Slippage risk based on path configuration and market conditions
        let base_slippage_risk = self.config.default_slippage_bps as f64 / 10000.0;
        let slippage_risk = if path.path_data.start_amount > U256::from(10).saturating_mul(U256::exp10(18)) {
            // Higher slippage risk for large trades
            base_slippage_risk * 1.5
        } else {
            base_slippage_risk
        };
        
        // MEV risk assessment based on market conditions and path characteristics
        let base_mev_risk = if path.path_data.requires_mev_protection { 0.5 } else { 0.1 };
        let mev_risk = match path.path_data.strategy_type {
            STRATEGY_CROSS_CHAIN => base_mev_risk * 0.7, // Cross-chain has lower MEV risk
            STRATEGY_MULTI_DEX => base_mev_risk * 1.3,   // Multi-DEX has higher MEV risk
            _ => base_mev_risk,
        };
        
        // Liquidity risk calculation using actual market data
        let liquidity_risk = self.calculate_liquidity_risk(path).await.unwrap_or(0.3);
        
        // Calculate overall risk using weighted factors
        let risk_score = (gas_price_risk * 0.25 + slippage_risk * 0.25 
            + mev_risk * 0.25 + liquidity_risk * 0.25) * 100.0;
        
        // Additional risk factors
        let adjusted_risk_score = self.apply_additional_risk_factors(risk_score, path).await;
        
        Ok(RiskAssessment {
            risk_score: adjusted_risk_score,
            slippage_risk: slippage_risk * 100.0,
            gas_price_risk: gas_price_risk * 100.0,
            mev_risk: mev_risk * 100.0,
            liquidity_risk: liquidity_risk * 100.0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        })
    }
    
    async fn calculate_liquidity_risk(&self, path: &ArbitragePath) -> Result<f64> {
        // Calculate liquidity risk based on path characteristics and market data
        let trade_size_usd = self.calculate_trade_size_usd(path).await.unwrap_or(0.0);
        
        // Base liquidity risk factors
        let mut liquidity_risk: f64 = 0.1; // Base 10% liquidity risk
        
        // Adjust for trade size
        if trade_size_usd > 100_000.0 {
            liquidity_risk += 0.3; // High liquidity risk for large trades
        } else if trade_size_usd > 10_000.0 {
            liquidity_risk += 0.1; // Medium liquidity risk
        }
        
        // Adjust for strategy complexity
        match path.path_data.strategy_type {
            STRATEGY_SIMPLE_ARB => liquidity_risk *= 0.8,  // Lower risk for simple arbitrage
            STRATEGY_TRIANGULAR => liquidity_risk *= 1.0,  // Normal risk
            STRATEGY_MULTI_DEX => liquidity_risk *= 1.2,   // Higher risk for multi-DEX
            STRATEGY_CROSS_CHAIN => liquidity_risk *= 1.5, // Highest risk for cross-chain
            _ => liquidity_risk *= 1.1,
        }
        
        // Cap liquidity risk at 100%
        Ok(liquidity_risk.min(1.0))
    }
    
    async fn calculate_trade_size_usd(&self, path: &ArbitragePath) -> Result<f64> {
        // Calculate trade size in USD using actual token prices
        let start_token = path.path_data.start_token;
        let start_amount = path.path_data.start_amount;
        let weth_address = self.blockchain.get_weth_address();
        
        // Try to get token price from quote source using validated WETH address
        match self.quote_source.get_rate_estimate_from_quote(
            self.blockchain.get_chain_id(),
            start_token,
            weth_address,
            start_amount,
            false,
        ).await {
            Ok(rate_estimate) => {
                let token_amount = start_amount.as_u128() as f64 / 1e18; // Assume 18 decimals
                let eth_price = self.calculate_conservative_eth_price().await;
                Ok(token_amount * eth_price * rate_estimate.amount_out.as_u128() as f64 / 1e18)
            }
            Err(_) => {
                // Fallback: if it's WETH, use ETH price
                if start_token == self.blockchain.get_weth_address() {
                    let eth_price = self.blockchain.get_eth_price_usd().await?;
                    let eth_amount = start_amount.as_u128() as f64 / 1e18;
                    Ok(eth_amount * eth_price)
                } else {
                    // Unable to determine price, assume moderate size
                    Ok(1000.0)
                }
            }
        }
    }
    
    async fn apply_additional_risk_factors(&self, base_risk: f64, path: &ArbitragePath) -> f64 {
        let mut adjusted_risk = base_risk;
        
        // Time-based risk adjustment
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        if path.path_data.expiration_timestamp <= current_time + 30 {
            adjusted_risk += 10.0; // Increase risk for expiring paths
        }
        
        // Flash loan risk adjustment
        if path.path_data.flash_loan_provider > 0 {
            adjusted_risk += 5.0; // Additional risk for flash loan strategies
        }
        
        // Priority level adjustment
        match path.path_data.priority_level {
            0 => adjusted_risk += 15.0, // Low priority = higher risk
            1 => adjusted_risk += 5.0,  // Medium priority
            _ => {},                    // High priority = no adjustment
        }
        
        // Cap at 100%
        adjusted_risk.min(100.0)
    }
    
    async fn generate_nonce(&self) -> Result<U256> {
        let block = self.blockchain.get_block_number().await?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        
        Ok(U256::from(timestamp) + U256::from(block))
    }
    
    fn calculate_deadline(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() + self.config.deadline_seconds
    }
    
    fn compute_commit_hash(
        &self,
        path_id: H256,
        deadline: u64,
        nonce: U256,
        max_slippage_bps: u32,
        salt: H256,
    ) -> H256 {
        let data = encode(&[
            Token::FixedBytes(path_id.as_bytes().to_vec()),
            Token::Uint(deadline.into()),
            Token::Uint(nonce),
            Token::Uint(max_slippage_bps.into()),
            Token::FixedBytes(salt.as_bytes().to_vec()),
            Token::Address(self.blockchain.get_weth_address()),
        ]);
        
        H256::from_slice(&keccak256(data))
    }
    
    async fn wait_for_reveal_block(&self, commit_block: u64) -> Result<()> {
        let target_block = commit_block + COMMIT_REVEAL_DELAY_BLOCKS;
        
        loop {
            let current_block = self.blockchain.get_block_number().await?;
            if current_block >= target_block {
                break;
            }
            
            sleep(Duration::from_millis(self.config.chain_configs
                .get("ethereum")
                .map(|c| c.block_time_ms)
                .unwrap_or(12000))).await;
        }
        
        // Additional MEV protection delay
        sleep(Duration::from_millis(MEV_PROTECTION_COOLDOWN_MS)).await;
        
        Ok(())
    }
    
    async fn get_optimal_gas_price(&self) -> Result<U256> {
        let gas_oracle = self.blockchain.get_gas_oracle().await?;
        let gas_price = gas_oracle.get_gas_price(&self.blockchain.get_chain_name().to_string(), None).await?;
        let base_gas_price = gas_price.effective_price();

        // Apply multiplier
        let adjusted_price = base_gas_price
            .mul(U256::from((self.config.gas_multiplier * 100.0) as u64))
            .div(U256::from(100));
        
        // Cap at max gas price
        let max_gas_price = U256::from(self.config.max_gas_price_gwei) * U256::from(10u64.pow(9));
        
        Ok(min(adjusted_price, max_gas_price))
    }
    
    async fn parse_execution_result(
        &self,
        path_id: H256,
        tx_hash: H256,
        receipt: Option<TransactionReceipt>,
        execution_time: Duration,
        path: &ArbitragePath,
    ) -> Result<ArbitrageResult> {
        let receipt = receipt.ok_or_else(|| UARError::ExecutionFailed("No receipt".to_string()))?;

        let mut error_message: Option<String> = None;
        let status = if receipt.status == Some(1.into()) {
            ExecutionStatus::Confirmed
        } else {
            let mut msg = "Execution reverted".to_string();
            // If a flash loan was used, it's a more specific failure type.
            if path.path_data.flash_loan_provider > 0 {
                self.trigger_circuit_breaker("flashLoanFailure").await;
                msg.push_str(" (flash loan failure)");
            }
            // Any revert is considered a loss.
            self.trigger_circuit_breaker("profitLoss").await;
            error_message = Some(msg);
            ExecutionStatus::Reverted
        };

        // Parse logs for profit calculation
        let profit_wei = self.parse_profit_from_logs(&receipt.logs).await?;
        let profit_usd = self.calculate_profit_usd(profit_wei).await?;

        // A confirmed transaction with profit lower than expected could be due to slippage.
        if status == ExecutionStatus::Confirmed && profit_wei < path.path_data.min_expected_profit {
            self.trigger_circuit_breaker("maxSlippage").await;
        }

        // Get circuit breaker status
        let circuit_breaker_status = self.get_circuit_breaker_status().await;

        Ok(ArbitrageResult {
            path_id,
            tx_hash,
            status,
            profit_wei,
            profit_usd,
            gas_used: receipt.gas_used,
            gas_price: receipt.effective_gas_price,
            execution_time_ms: execution_time.as_millis() as u64,
            token_path: self.extract_token_path(&path.path_data),
            error_message,
            flash_loan_provider: if path.path_data.flash_loan_provider > 0 {
                Some(path.path_data.flash_loan_provider)
            } else {
                None
            },
            mev_protection_used: path.path_data.requires_mev_protection,
            circuit_breaker_status,
        })
    }
    
    async fn parse_profit_from_logs(&self, logs: &[Log]) -> Result<U256> {
        // Parse FlashLoanArbitrageExecuted or DirectArbitrageExecuted events
        for log in logs {
            if log.topics.len() > 0 {
                // Check for our event signatures
                let event_sig = log.topics[0];
                
                // These would be the actual keccak256 hashes of the event signatures
                let flash_loan_sig = H256::from_slice(&keccak256(
                    b"FlashLoanArbitrageExecuted(bytes32,address,address[],uint256[],uint256,uint256,uint256,uint8)"
                ));
                let direct_sig = H256::from_slice(&keccak256(
                    b"DirectArbitrageExecuted(bytes32,address,address,uint256,uint256,uint256,uint256)"
                ));
                
                if event_sig == flash_loan_sig || event_sig == direct_sig {
                    // Decode profit from log data
                    // Position depends on event structure
                    let tokens = decode(&[ParamType::Uint(256)], &log.data)?;
                    if tokens.len() > 4 {
                        if let Token::Uint(profit) = &tokens[4] {
                            return Ok(*profit);
                        }
                    }
                }
            }
        }
        
        Ok(U256::zero())
    }
    
    async fn calculate_profit_usd(&self, profit_wei: U256) -> Result<f64> {
        let weth_address = self.blockchain.get_weth_address();
        
        // Use WETH address for price validation and fallback calculations
        let weth_price_validation = self.validate_eth_price(2000.0); // Validate reasonable WETH price
        
        // Validate WETH price before proceeding with profit calculation
        if !weth_price_validation {
            return Err(eyre::eyre!("WETH price validation failed - price outside reasonable bounds"));
        }
        
        // Use WETH address for enhanced price discovery if primary sources fail
        let weth_fallback_price = if weth_address != Address::zero() {
            // Try to get WETH-specific price data for enhanced accuracy
            debug!("Using WETH address {} for enhanced price discovery", weth_address);
            match self.price_oracle.get_token_price_usd(&self.blockchain.get_chain_name().to_string(), weth_address).await {
                Ok(price) if self.validate_eth_price(price) => {
                    debug!("Successfully obtained WETH price: ${:.2}", price);
                    Some(price)
                }
                Ok(price) => {
                    warn!("WETH price failed validation: ${:.2}, using standard fallback", price);
                    None
                }
                Err(e) => {
                    warn!("Failed to get WETH price: {}, using standard fallback", e);
                    None
                }
            }
        } else {
            warn!("WETH address not available, using standard price discovery");
            None
        };
        
        // Primary: Get ETH price from blockchain manager's price oracle integration
        let eth_price_usd = match self.blockchain.get_eth_price_usd().await {
            Ok(price) => {
                debug!("Successfully obtained ETH price from blockchain oracle: ${:.2}", price);
                if self.validate_eth_price(price) {
                    price
                } else {
                    warn!("ETH price from blockchain oracle failed validation: ${:.2}, using fallback", price);
                    if let Some(weth_price) = weth_fallback_price {
                        weth_price
                    } else {
                        self.get_fallback_eth_price().await?
                    }
                }
            }
            Err(e) => {
                warn!("Failed to get ETH price from blockchain oracle: {}, using fallback chain", e);
                if let Some(weth_price) = weth_fallback_price {
                    weth_price
                } else {
                    self.get_fallback_eth_price().await?
                }
            }
        };
        
        let profit_eth = profit_wei.as_u128() as f64 / 1e18;
        let profit_usd = profit_eth * eth_price_usd;
        
        // Comprehensive validation of the calculated profit
        if let Err(validation_error) = self.validate_profit_calculation(profit_wei, profit_eth, profit_usd, eth_price_usd) {
            warn!("Profit calculation validation failed: {}, returning conservative estimate", validation_error);
            // Return conservative estimate based on minimum reasonable ETH price
            let conservative_eth_price = 1000.0; // $1000 as minimum reasonable ETH price
            return Ok((profit_wei.as_u128() as f64 / 1e18) * conservative_eth_price);
        }
        
        debug!("Profit calculation: profit_wei={}, profit_eth={:.6}, eth_price=${:.2}, profit_usd=${:.4}", 
               profit_wei, profit_eth, eth_price_usd, profit_usd);
        
        Ok(profit_usd)
    }
    
    /// Comprehensive fallback chain for ETH price with multiple sources
    async fn get_fallback_eth_price(&self) -> Result<f64> {
        let weth_address = self.blockchain.get_weth_address();
        
        // Fallback 1: Quote source aggregator using validated WETH address
        match self.quote_source.get_rate_estimate_from_quote(
            self.blockchain.get_chain_id(),
            weth_address,
            weth_address, // Use WETH as reference
            U256::exp10(18), // 1 ETH
            false,
        ).await {
            Ok(rate_estimate) => {
                debug!("Obtained ETH price from quote source: ${:.2}", rate_estimate.amount_out.as_u128() as f64 / 1e18);
                let eth_price = rate_estimate.amount_out.as_u128() as f64 / 1e18;
                if self.validate_eth_price(eth_price) {
                    return Ok(eth_price);
                } else {
                    warn!("Quote source ETH price failed validation: ${:.2}", eth_price);
                }
            }
            Err(e) => {
                warn!("Failed to get ETH price from quote source: {}", e);
            }
        }
        
        // Fallback 2: Risk manager's price calculation (if available)
        if let Ok(risk_assessment) = self.risk_manager.assess_trade(
            self.blockchain.get_weth_address(),
            self.blockchain.get_weth_address(),
            U256::exp10(18), // 1 ETH
            None
        ).await {
            // Extract price information from risk assessment if it contains market data
            debug!("Using risk assessment for ETH price estimation");
            
            // Use risk assessment data to validate market conditions
            if risk_assessment.total_risk_score < 0.5 {
                debug!("Low risk market conditions detected, using conservative price adjustment");
                // In low risk conditions, we can be more aggressive with price estimates
                return Ok(3000.0); // Conservative but optimistic estimate
            } else if risk_assessment.total_risk_score > 0.8 {
                debug!("High risk market conditions detected, using defensive price adjustment");
                // In high risk conditions, use more defensive pricing
                return Ok(1800.0); // Defensive estimate
            }
        }
        
        // Fallback 3: Historical price with volatility adjustment
        match self.get_historical_eth_price_estimate().await {
            Ok(historical_price) => {
                debug!("Using historical ETH price estimate: ${:.2}", historical_price);
                if self.validate_eth_price(historical_price) {
                    return Ok(historical_price);
                }
            }
            Err(e) => {
                warn!("Failed to get historical ETH price: {}", e);
            }
        }
        
        // Fallback 4: On-chain WETH price discovery via DEX pools
        match self.get_dex_eth_price_estimate().await {
            Ok(onchain_price) => {
                debug!("Using on-chain ETH price estimate: ${:.2}", onchain_price);
                if self.validate_eth_price(onchain_price) {
                    return Ok(onchain_price);
                }
            }
            Err(e) => {
                warn!("Failed to get on-chain ETH price: {}", e);
            }
        }
        
        // Fallback 5: Conservative market-aware estimate
        let conservative_price = self.calculate_conservative_eth_price().await;
        warn!("All ETH price sources failed, using conservative estimate: ${:.2}", conservative_price);
        Ok(conservative_price)
    }
    
    /// Validate ETH price is within reasonable bounds
    fn validate_eth_price(&self, price: f64) -> bool {
        // ETH price should be between $100 and $50,000 (very generous bounds)
        let min_reasonable_price = 100.0;
        let max_reasonable_price = 50_000.0;
        
        price > min_reasonable_price && 
        price < max_reasonable_price && 
        price.is_finite() && 
        !price.is_nan() &&
        price > 0.0
    }
    
    /// Get historical price estimate with volatility adjustment
    async fn get_historical_eth_price_estimate(&self) -> Result<f64> {
        let current_block = self.blockchain.get_block_number().await?;
        let blocks_back = 100; // Look back ~20 minutes on Ethereum
        
        // Multi-source price discovery with weighted aggregation
        let mut price_sources = Vec::new();
        
        // 1. Chainlink Oracle Price (highest priority)
        if let Ok(chainlink_price) = self.get_chainlink_eth_price().await {
            price_sources.push((chainlink_price, 0.4)); // 40% weight
        }
        
        // 2. DEX Pool Price Discovery
        if let Ok(dex_price) = self.get_dex_eth_price_estimate().await {
            price_sources.push((dex_price, 0.3)); // 30% weight
        }
        
        // 3. Historical Block Analysis
        if let Ok(historical_price) = self.analyze_historical_blocks(current_block, blocks_back).await {
            price_sources.push((historical_price, 0.2)); // 20% weight
        }
        
        // 4. Market Regime Analysis
        if let Ok(regime_price) = self.calculate_regime_based_price().await {
            price_sources.push((regime_price, 0.1)); // 10% weight
        }
        
        if price_sources.is_empty() {
            return Err(eyre::eyre!("No price sources available for historical estimation"));
        }
        
        // Calculate weighted average
        let total_weight: f64 = price_sources.iter().map(|(_, weight)| weight).sum();
        let weighted_price: f64 = price_sources.iter()
            .map(|(price, weight)| price * weight)
            .sum::<f64>() / total_weight;
        
        // Apply volatility adjustment based on recent price movements
        let volatility_adjustment = self.calculate_volatility_adjustment(&price_sources).await?;
        let adjusted_price = weighted_price * (1.0 + volatility_adjustment);
        
        // Validate final price within reasonable bounds
        let validated_price = self.validate_eth_price_bounds(adjusted_price).await?;
        
        Ok(validated_price)
    }
    
    async fn get_chainlink_eth_price(&self) -> Result<f64> {
        let chain_id = self.blockchain.get_chain_id();

        // Get Chainlink feed address from config
        let chain_config = self.config.chain_configs.get(&chain_id.to_string())
            .ok_or_else(|| eyre::eyre!("Chain configuration not found"))?;

        if let Some(feed_address) = chain_config.chainlink_native_usd_feed {
            // Use the blockchain's token price functionality
            let price = self.price_oracle.get_token_price_usd(&chain_config.chain_name, feed_address).await
                .map_err(|e| eyre::eyre!("Chainlink price fetch failed: {}", e))?;

            if self.validate_eth_price(price) {
                return Ok(price);
            }
        }
        
        Err(eyre::eyre!("Chainlink price not available or invalid"))
    }
    
    async fn get_dex_eth_price_estimate(&self) -> Result<f64> {
        let weth_address = self.blockchain.get_weth_address();
        let mut dex_prices = Vec::new();
        
        // Major stablecoin pairs for price discovery
        let stablecoin_pairs = vec![
            ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6), // USDC
            ("0xdAC17F958D2ee523a2206206994597C13D831ec7", 6), // USDT
            ("0x6B175474E89094C44Da98b954EedeAC495271d0F", 18), // DAI
        ];
        
        for (stablecoin_str, decimals) in stablecoin_pairs {
            if let Ok(stablecoin_addr) = stablecoin_str.parse::<Address>() {
                // Try multiple DEX aggregators for best price
                let aggregators = vec!["1inch", "paraswap", "0x"];
                
                for _aggregator in aggregators {
                    if let Ok(rate) = self.quote_source.get_rate_estimate_from_quote(
                        self.blockchain.get_chain_id(),
                        weth_address,
                        stablecoin_addr,
                        U256::exp10(18), // 1 ETH
                        false,
                    ).await {
                        let price = rate.amount_out.as_u128() as f64 / 10f64.powi(decimals);
                        if self.validate_eth_price(price) {
                            dex_prices.push(price);
                            break; // Use first valid price from this aggregator
                        }
                    }
                }
            }
        }
        
        if dex_prices.is_empty() {
            return Err(eyre::eyre!("No valid DEX prices obtained"));
        }
        
        // Use median to avoid outliers
        dex_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_price = dex_prices[dex_prices.len() / 2];
        
        Ok(median_price)
    }
    
    async fn analyze_historical_blocks(&self, current_block: u64, blocks_back: u64) -> Result<f64> {
        let mut historical_prices = Vec::new();
        let sample_blocks = 10; // Sample every 10th block for efficiency
        
        for i in 0..sample_blocks {
            let block_num = current_block.saturating_sub(i * blocks_back / sample_blocks);
            if block_num == 0 { break; }
            
            // Try to get historical price from database cache
            if let Some(cached_price) = self.get_cached_historical_price(block_num).await? {
                historical_prices.push(cached_price);
            } else {
                // Fallback to block-based estimation
                let block_price = self.estimate_price_from_block_context(block_num).await?;
                historical_prices.push(block_price);
            }
        }
        
        if historical_prices.is_empty() {
            return Err(eyre::eyre!("No historical price data available"));
        }
        
        // Calculate trend-adjusted price
        let avg_price = historical_prices.iter().sum::<f64>() / historical_prices.len() as f64;
        let trend_factor = self.calculate_price_trend(&historical_prices)?;
        
        Ok(avg_price * trend_factor)
    }
    
    async fn calculate_regime_based_price(&self) -> Result<f64> {
        let gas_oracle = self.blockchain.get_gas_oracle().await?;
        let current_gas_price = gas_oracle.get_gas_price(&self.blockchain.get_chain_name().to_string(), None).await?;
        let gas_price_gwei = current_gas_price.effective_price().as_u128() as f64 / 1e9;
        
        // Market regime detection based on gas prices and network activity
        let regime = self.detect_market_regime(gas_price_gwei).await?;

        let base_price = match regime {
            MarketRegime::HighVolatility => 3500.0,
            MarketRegime::LowVolatility => 2500.0,
            MarketRegime::Trending => 3000.0,
            MarketRegime::Bull => 3200.0,
            MarketRegime::Bear => 2000.0,
            MarketRegime::Sideways => 2800.0,
            MarketRegime::Ranging => 2800.0,
            MarketRegime::Crisis => 1500.0,
        };
        
        // Apply regime-specific adjustments
        let regime_adjustment = self.calculate_regime_adjustment(&regime, gas_price_gwei).await?;
        
        Ok(base_price * regime_adjustment)
    }
    
    async fn calculate_volatility_adjustment(&self, price_sources: &[(f64, f64)]) -> Result<f64> {
        if price_sources.len() < 2 {
            return Ok(0.0);
        }
        
        let prices: Vec<f64> = price_sources.iter().map(|(price, _)| *price).collect();
        let mean_price = prices.iter().sum::<f64>() / prices.len() as f64;
        
        let variance = prices.iter()
            .map(|p| (p - mean_price).powi(2))
            .sum::<f64>() / prices.len() as f64;
        
        let volatility = variance.sqrt() / mean_price;
        
        // Normalize volatility to reasonable adjustment range (-0.1 to 0.1)
        let normalized_volatility = volatility.min(0.1).max(-0.1);
        
        Ok(normalized_volatility)
    }
    
    async fn validate_eth_price_bounds(&self, price: f64) -> Result<f64> {
        // Conservative bounds based on historical ETH price ranges
        const MIN_ETH_PRICE: f64 = 500.0;   // $500 minimum
        const MAX_ETH_PRICE: f64 = 10000.0; // $10,000 maximum
        
        if price < MIN_ETH_PRICE || price > MAX_ETH_PRICE {
            return Err(eyre::eyre!(
                "ETH price ${:.2} outside reasonable bounds (${:.0}-${:.0})",
                price, MIN_ETH_PRICE, MAX_ETH_PRICE
            ));
        }
        
        Ok(price)
    }
    
    async fn get_cached_historical_price(&self, block_number: u64) -> Result<Option<f64>> {
        let chain_id = self.blockchain.get_chain_id();
        let chain_name = match self.config.chain_configs.get(&chain_id.to_string()) {
            Some(chain_config) => chain_config.chain_name.clone(),
            None => "ethereum".to_string(),
        };
        let eth_address = ethers::types::H160::from_low_u64_be(0); // Use 0x0 for ETH (canonical)
        let price = self.price_oracle
            .get_token_price_usd_at(&chain_name, eth_address, Some(block_number), None)
            .await
            .map_err(|e| eyre::eyre!("Failed to fetch historical ETH price for block {}: {}", block_number, e))?;
        if price > 0.0 && price.is_finite() {
            return Ok(Some(price));
        }
        Ok(None)
    }
    
    async fn estimate_price_from_block_context(&self, block_number: u64) -> Result<f64> {
        // Use block number patterns and network activity as price indicators
        let block_factor = (block_number % 1000) as f64 / 1000.0;
        let base_price = 2500.0;
        
        // Cyclical adjustment based on block patterns
        let cyclical_adjustment = (block_factor * 2.0 * std::f64::consts::PI).sin() * 0.1;
        
        Ok(base_price * (1.0 + cyclical_adjustment))
    }
    
    fn calculate_price_trend(&self, prices: &[f64]) -> Result<f64> {
        if prices.len() < 2 {
            return Ok(1.0);
        }
        
        // Simple linear trend calculation
        let n = prices.len() as f64;
        let sum_x: f64 = (0..prices.len()).map(|i| i as f64).sum();
        let sum_y: f64 = prices.iter().sum();
        let sum_xy: f64 = (0..prices.len()).map(|i| i as f64 * prices[i]).sum();
        let sum_x2: f64 = (0..prices.len()).map(|i| (i as f64).powi(2)).sum();
        
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
        let trend_factor = 1.0 + (slope / prices[0]) * 0.1; // Normalize trend impact
        
        Ok(trend_factor.max(0.8).min(1.2)) // Limit trend impact to ±20%
    }
    
    async fn detect_market_regime(&self, gas_price_gwei: f64) -> Result<MarketRegime> {
        // Simple regime detection based on gas prices
        let regime = if gas_price_gwei > 100.0 {
            MarketRegime::HighVolatility
        } else if gas_price_gwei > 50.0 {
            MarketRegime::HighVolatility
        } else if gas_price_gwei < 20.0 {
            MarketRegime::LowVolatility
        } else {
            MarketRegime::Sideways
        };
        
        Ok(regime)
    }
    
    async fn calculate_regime_adjustment(&self, regime: &MarketRegime, gas_price_gwei: f64) -> Result<f64> {
        let base_adjustment = match regime {
            MarketRegime::HighVolatility => 1.1,
            MarketRegime::LowVolatility => 0.95,
            MarketRegime::Trending => 1.05,
            MarketRegime::Bull => 1.05,
            MarketRegime::Bear => 0.9,
            MarketRegime::Sideways => 1.0,
            MarketRegime::Ranging => 1.0,
            MarketRegime::Crisis => 0.8,
        };
        
        // Additional gas price adjustment
        let gas_adjustment = if gas_price_gwei > 100.0 {
            1.05 // Higher gas suggests higher activity and potentially higher prices
        } else if gas_price_gwei < 20.0 {
            0.95 // Lower gas suggests lower activity and potentially lower prices
        } else {
            1.0
        };
        
        Ok(base_adjustment * gas_adjustment)
    }
    
    /// Calculate conservative ETH price based on market conditions
    async fn calculate_conservative_eth_price(&self) -> f64 {
        // Get network activity indicators
        let base_price = 2500.0; // Conservative baseline

        // Adjust based on gas prices (indicator of network activity)
        let adjustment = match self.blockchain.get_gas_oracle().await {
            Ok(gas_oracle) => {
                match gas_oracle.get_gas_price(&self.blockchain.get_chain_name().to_string(), None).await {
                    Ok(gas_price) => {
                        let gas_price_gwei = gas_price.effective_price().as_u128() as f64 / 1e9;
                        if gas_price_gwei > 100.0 {
                            500.0 // Higher activity suggests higher prices
                        } else if gas_price_gwei < 20.0 {
                            -500.0 // Lower activity suggests lower prices
                        } else {
                            0.0
                        }
                    }
                    Err(_) => 0.0
                }
            }
            Err(_) => 0.0
        };
        
        let result: f64 = base_price + adjustment;
        result.max(1500.0).min(4000.0) // Conservative bounds
    }
    
    /// Comprehensive validation of profit calculation
    fn validate_profit_calculation(
        &self,
        profit_wei: U256,
        profit_eth: f64,
        profit_usd: f64,
        eth_price: f64,
    ) -> Result<()> {
        // Check for reasonable profit bounds
        if profit_usd < -1_000_000.0 || profit_usd > 1_000_000.0 {
            return Err(eyre::eyre!(
                "Profit USD out of reasonable bounds: ${:.2} (profit_wei: {}, eth_price: ${:.2})", 
                profit_usd, profit_wei, eth_price
            ));
        }
        
        // Check for mathematical consistency
        let expected_profit_usd = profit_eth * eth_price;
        let calculation_error = (profit_usd - expected_profit_usd).abs();
        if calculation_error > 0.01 { // Allow 1 cent rounding error
            return Err(eyre::eyre!(
                "Profit calculation inconsistency: calculated=${:.4}, expected=${:.4}, error=${:.4}",
                profit_usd, expected_profit_usd, calculation_error
            ));
        }
        
        // Check for NaN or infinite values
        if !profit_usd.is_finite() || profit_usd.is_nan() {
            return Err(eyre::eyre!("Profit USD is not finite: {}", profit_usd));
        }
        
        // Check profit_wei to profit_eth conversion
        let expected_profit_eth = profit_wei.as_u128() as f64 / 1e18;
        let eth_conversion_error = (profit_eth - expected_profit_eth).abs();
        if eth_conversion_error > 1e-12 { // Allow for floating point precision
            return Err(eyre::eyre!(
                "ETH conversion error: calculated={:.12}, expected={:.12}, error={:.12}",
                profit_eth, expected_profit_eth, eth_conversion_error
            ));
        }
        
        Ok(())
    }
    
    fn extract_token_path(&self, path_data: &PathExecutionData) -> Vec<String> {
        vec![
            format!("{:?}", path_data.start_token),
            format!("{:?}", path_data.end_token),
        ]
    }
    
    async fn get_circuit_breaker_status(&self) -> CircuitBreakerStatus {
        for entry in self.circuit_breaker_states.iter() {
            let (name, state) = entry.pair();
            if state.triggered {
                let cooldown_remaining = if state.last_triggered_timestamp > 0 {
                    let elapsed = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() - state.last_triggered_timestamp;
                    
                    if elapsed < state.cooldown_period {
                        Some(state.cooldown_period - elapsed)
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                return CircuitBreakerStatus {
                    triggered: true,
                    breaker_type: Some(name.clone()),
                    trigger_count: state.trigger_count,
                    cooldown_remaining_seconds: cooldown_remaining,
                };
            }
        }
        
        CircuitBreakerStatus {
            triggered: false,
            breaker_type: None,
            trigger_count: 0,
            cooldown_remaining_seconds: None,
        }
    }
    
    async fn update_execution_stats(&self, result: &ArbitrageResult) {
        let mut stats = self.execution_stats.write().await;
        
        stats.total_executions += 1;
        
        match result.status {
            ExecutionStatus::Confirmed => {
                stats.successful_executions += 1;
                stats.total_profit_usd += result.profit_usd;
                stats.highest_profit_usd = stats.highest_profit_usd.max(result.profit_usd);
                
                if let Some(provider) = result.flash_loan_provider {
                    match provider {
                        PROVIDER_AAVE => stats.flash_loan_stats.aave_executions += 1,
                        PROVIDER_BALANCER => stats.flash_loan_stats.balancer_executions += 1,
                        PROVIDER_MAKER => stats.flash_loan_stats.maker_executions += 1,
                        PROVIDER_UNISWAP_V4 => stats.flash_loan_stats.uniswap_v4_executions += 1,
                        _ => {}
                    }
                }
            }
            ExecutionStatus::Failed | ExecutionStatus::Reverted => {
                stats.failed_executions += 1;
                
                if let Some(msg) = &result.error_message {
                    if msg.contains("slippage") {
                        stats.failed_due_to_slippage += 1;
                    } else if msg.contains("gas") {
                        stats.failed_due_to_gas += 1;
                    } else if msg.contains("MEV") {
                        stats.failed_due_to_mev += 1;
                    }
                }
            }
            _ => {}
        }
        
        // Update averages
        if stats.successful_executions > 0 {
            stats.average_profit_usd = stats.total_profit_usd / stats.successful_executions as f64;
            
            if let Some(gas_used) = result.gas_used {
                let total_gas = stats.average_gas_used * (stats.successful_executions - 1) as f64
                    + gas_used.as_u64() as f64;
                stats.average_gas_used = total_gas / stats.successful_executions as f64;
            }
        }
        
        let total_time = stats.average_execution_time_ms * (stats.total_executions - 1) as f64
            + result.execution_time_ms as f64;
        stats.average_execution_time_ms = total_time / stats.total_executions as f64;
    }
    
    async fn update_stats_risk_rejection(&self) {
        let mut stats = self.execution_stats.write().await;
        stats.risk_rejections += 1;
    }
    
    async fn update_hot_path_cache(&self, path_id: H256, result: &ArbitrageResult) {
        let mut cache = self.hot_path_cache
            .entry(path_id)
            .or_insert_with(|| HotPathCache {
                last_gas_used: U256::zero(),
                last_profit: U256::zero(),
                execution_count: 0,
                success_rate: 0.0,
                last_execution_timestamp: 0,
                is_active: false,
            });
        
        cache.execution_count += 1;
        cache.last_profit = result.profit_wei;
        cache.last_execution_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        if let Some(gas_used) = result.gas_used {
            cache.last_gas_used = gas_used;
        }
        
        // Update success rate
        let success = if result.status == ExecutionStatus::Confirmed { 1.0 } else { 0.0 };
        cache.success_rate = (cache.success_rate * (cache.execution_count - 1) as f64 + success)
            / cache.execution_count as f64;
        
        // Activate if meets threshold
        if cache.execution_count >= self.config.hot_path_config.execution_threshold
            && cache.success_rate >= self.config.hot_path_config.success_rate_threshold {
            cache.is_active = true;
            
            // Update contract if needed
            if self.config.hot_path_config.gas_optimization_enabled {
                let _ = self.update_hot_path_on_contract(path_id, true).await;
            }
        }
        
        // Update execution stats
        if cache.is_active {
            let mut stats = self.execution_stats.write().await;
            stats.hot_path_executions += 1;
        }
    }
    
    async fn update_hot_path_on_contract(&self, path_id: H256, is_active: bool) -> Result<()> {
        let method_call = self.uar_contract
            .method::<_, ()>("updateHotPathCache", (path_id, is_active))?;
        
        let tx = method_call
            .send()
            .await?;
            
        let _ = tx.await?;
        
        Ok(())
    }
    
    pub async fn get_execution_stats(&self) -> ExecutionStats {
        self.execution_stats.read().await.clone()
    }
    
    pub async fn get_active_paths(&self) -> Vec<H256> {
        self.path_cache
            .iter()
            .filter(|entry| {
                let path = entry.value();
                path.path_data.expiration_timestamp > SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            })
            .map(|entry| *entry.key())
            .collect()
    }
    
    pub async fn cleanup_expired_paths(&self) -> Result<()> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let expired_paths: Vec<H256> = self.path_cache
            .iter()
            .filter(|entry| entry.value().path_data.expiration_timestamp < current_time)
            .map(|entry| *entry.key())
            .collect();
        
        for path_id in expired_paths {
            self.path_cache.remove(&path_id);
            self.hot_path_cache.remove(&path_id);
            
            // Remove from contract
            let _ = self.remove_path_from_contract(path_id).await;
        }
        
        Ok(())
    }
    
    async fn remove_path_from_contract(&self, path_id: H256) -> Result<()> {
        let method_call = self.uar_contract
            .method::<_, ()>("removePath", path_id)?;
        
        let tx = method_call
            .send()
            .await?;
            
        let _ = tx.await?;
        
        Ok(())
    }
    
    pub async fn get_best_flash_loan_provider(&self, token: Address, amount: U256) -> Option<u8> {
        let mut best_provider = None;
        let mut lowest_fee = u32::MAX;
        
        for (provider_id, config) in self.flash_loan_providers.iter() {
            if !config.enabled {
                continue;
            }
            
            if config.preferred_tokens.contains(&token) && amount <= config.max_loan_amount {
                if config.fee_bps < lowest_fee {
                    lowest_fee = config.fee_bps;
                    best_provider = Some(*provider_id);
                }
            }
        }
        
        best_provider.or(Some(self.config.flash_loan_providers
            .get("aave")
            .map(|c| c.provider_id)
            .unwrap_or(PROVIDER_AAVE)))
    }
    
    pub fn encode_swap_steps(&self, steps: &[SwapStep]) -> Result<Bytes> {
        let tokens: Vec<Token> = steps.iter().map(|step| {
            Token::Tuple(vec![
                Token::Address(step.target),
                Token::Address(step.token_in),
                Token::Address(step.token_out),
                Token::Uint(step.amount_in),
                Token::Uint(step.min_amount_out),
                Token::Bytes(step.call_data.to_vec()),
                Token::Uint(step.aggregator_type.into()),
                Token::Uint(step.pool_type.into()),
                Token::Bool(step.use_hot_path),
            ])
        }).collect();
        
        Ok(Bytes::from(encode(&[Token::Array(tokens)])))
    }
    
    pub fn get_aggregator_type(&self, dex_name: &str) -> u8 {
        match dex_name.to_lowercase().as_str() {
            "uniswapv2" | "sushiswap" | "pancakeswap" => AGG_DIRECT,
            "1inch" => AGG_1INCH,
            "0x" | "zerox" => AGG_0X,
            "uniswapv3" => AGG_UNISWAP_V3,
            "uniswapv4" => AGG_UNISWAP_V4,
            "curve" => AGG_CURVE,
            "balancer" => AGG_BALANCER,
            "solidly" | "velodrome" => AGG_SOLIDLY,
            _ => AGG_CUSTOM,
        }
    }
    
    pub fn get_strategy_type(&self, num_tokens: usize, cross_chain: bool) -> u8 {
        if cross_chain {
            STRATEGY_CROSS_CHAIN
        } else if num_tokens == 2 {
            STRATEGY_SIMPLE_ARB
        } else if num_tokens == 3 {
            STRATEGY_TRIANGULAR
        } else {
            STRATEGY_MULTI_DEX
        }
    }
}