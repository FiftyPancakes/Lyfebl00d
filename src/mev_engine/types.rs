use ethers::types::{H256, U256};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::mempool::MEVOpportunity;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MEVPlayStatus {
    Identified,
    Simulating,
    SimulationFailed(String),
    RiskAssessmentFailed(String),
    ReadyForSubmission,
    UrgentSubmission,
    SubmittingBundle,
    SubmittingTxs,
    SubmittedToRelay(H256),
    SubmittedToMempool(Vec<H256>),
    PendingInclusion,
    PendingConfirmation,
    ConfirmedSuccess(f64),
    ConfirmedFailure(String),
    BundleNotIncluded,
    Expired,
    Cancelled,
    MonitorError(String),
}

impl MEVPlayStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MEVPlayStatus::ConfirmedSuccess(_)
                | MEVPlayStatus::ConfirmedFailure(_)
                | MEVPlayStatus::BundleNotIncluded
                | MEVPlayStatus::Expired
                | MEVPlayStatus::Cancelled
        )
    }

    pub fn is_submitted(&self) -> bool {
        matches!(
            self,
            MEVPlayStatus::SubmittedToRelay(_)
                | MEVPlayStatus::SubmittedToMempool(_)
                | MEVPlayStatus::PendingInclusion
                | MEVPlayStatus::PendingConfirmation
        )
    }

    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    pub fn requires_monitoring(&self) -> bool {
        matches!(
            self,
            MEVPlayStatus::SubmittedToRelay(_)
                | MEVPlayStatus::SubmittedToMempool(_)
                | MEVPlayStatus::PendingInclusion
                | MEVPlayStatus::PendingConfirmation
        )
    }
}

#[derive(Debug, Clone)]
pub struct ActiveMEVPlay {
    pub play_id: Uuid,
    pub opportunity_type: String,
    pub original_signal: MEVOpportunity,
    pub status: MEVPlayStatus,
    pub submitted_bundle_hash: Option<H256>,
    pub submitted_tx_hashes: Vec<H256>,
    pub estimated_profit_usd: f64,
    pub actual_profit_usd: Option<f64>,
    pub created_at: Instant,
    pub last_updated_at: Instant,
    pub retry_count: u32,
    pub chain_id: u64,
    pub target_block: Option<u64>,
    pub strategy_data: Option<serde_json::Value>,
    pub inclusion_block_number: Option<u64>,
    pub gas_used: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub simulation_attempts: u32,
    pub last_error: Option<String>,
    pub metadata: PlayMetadata,
    /// Pre-transaction balances for profit calculation using balance delta method
    pub pre_tx_balances: Option<std::collections::HashMap<ethers::types::Address, U256>>,
    /// Post-transaction balances for profit calculation using balance delta method
    pub post_tx_balances: Option<std::collections::HashMap<ethers::types::Address, U256>>,
}

#[derive(Debug, Clone, Default)]
pub struct PlayMetadata {
    pub gas_price_at_submission: Option<U256>,
    pub mempool_congestion_at_submission: Option<f64>,
    pub competing_bots_detected: u32,
    pub flashbots_bundle_uuid: Option<String>,
    pub submission_timestamp: Option<Instant>,
    pub confirmation_timestamp: Option<Instant>,
    pub revert_reason: Option<String>,
    pub slippage_tolerance_bps: Option<u32>,
    pub actual_slippage_bps: Option<u32>,
}

impl ActiveMEVPlay {
    pub fn new(signal: &MEVOpportunity, chain_id: u64, estimated_profit_usd: f64) -> Self {
        let opportunity_type = match signal {
            MEVOpportunity::Sandwich(_) => "Sandwich",
            MEVOpportunity::Liquidation(_) => "Liquidation",
            MEVOpportunity::JITLiquidity(_) => "JITLiquidity",
            MEVOpportunity::Arbitrage(_) => "Arbitrage",
            MEVOpportunity::OracleUpdate(_) => "OracleUpdate",
            MEVOpportunity::FlashLoan(_) => "FlashLoan",
        }.to_string();

        let target_block = match signal {
            MEVOpportunity::Sandwich(s) => Some(s.block_number),
            MEVOpportunity::Liquidation(l) => Some(l.block_number),
            MEVOpportunity::JITLiquidity(j) => Some(j.block_number),
            MEVOpportunity::Arbitrage(_) => Some(0), // ArbitrageRoute doesn't have block_number
            MEVOpportunity::OracleUpdate(o) => Some(o.block_number),
            MEVOpportunity::FlashLoan(f) => Some(f.block_number),
        };

        Self {
            play_id: Uuid::new_v4(),
            opportunity_type,
            original_signal: signal.clone(),
            status: MEVPlayStatus::Identified,
            submitted_bundle_hash: None,
            submitted_tx_hashes: vec![],
            estimated_profit_usd,
            actual_profit_usd: None,
            created_at: Instant::now(),
            last_updated_at: Instant::now(),
            retry_count: 0,
            chain_id,
            target_block,
            strategy_data: None,
            inclusion_block_number: None,
            gas_used: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            simulation_attempts: 0,
            last_error: None,
            metadata: PlayMetadata::default(),
            pre_tx_balances: None,
            post_tx_balances: None,
        }
    }

    /// Submit play to relay with bundle hash - explicit state transition
    pub fn submit_to_relay(&mut self, bundle_hash: H256) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::ReadyForSubmission | MEVPlayStatus::UrgentSubmission => {
                self.status = MEVPlayStatus::SubmittedToRelay(bundle_hash);
                self.submitted_bundle_hash = Some(bundle_hash);
                self.metadata.submission_timestamp = Some(Instant::now());
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only submit to relay from ReadyForSubmission or UrgentSubmission states")
        }
    }

    /// Submit play to mempool with transaction hashes - explicit state transition
    pub fn submit_to_mempool(&mut self, tx_hashes: Vec<H256>) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::ReadyForSubmission | MEVPlayStatus::UrgentSubmission => {
                self.status = MEVPlayStatus::SubmittedToMempool(tx_hashes.clone());
                self.submitted_tx_hashes = tx_hashes;
                self.metadata.submission_timestamp = Some(Instant::now());
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only submit to mempool from ReadyForSubmission or UrgentSubmission states")
        }
    }

    /// Mark play as pending inclusion - explicit state transition
    pub fn mark_pending_inclusion(&mut self) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::SubmittedToRelay(_) | MEVPlayStatus::SubmittedToMempool(_) => {
                self.status = MEVPlayStatus::PendingInclusion;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only mark pending inclusion from submitted states")
        }
    }

    /// Mark play as pending confirmation - explicit state transition
    pub fn mark_pending_confirmation(&mut self, inclusion_block: u64) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::PendingInclusion => {
                self.status = MEVPlayStatus::PendingConfirmation;
                self.inclusion_block_number = Some(inclusion_block);
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only mark pending confirmation from PendingInclusion state")
        }
    }

    /// Mark play as confirmed success - explicit state transition
    pub fn confirm_success(&mut self, actual_profit_usd: f64) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::PendingConfirmation => {
                self.status = MEVPlayStatus::ConfirmedSuccess(actual_profit_usd);
                self.actual_profit_usd = Some(actual_profit_usd);
                self.metadata.confirmation_timestamp = Some(Instant::now());
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only confirm success from PendingConfirmation state")
        }
    }

    /// Mark play as confirmed failure - explicit state transition
    pub fn confirm_failure(&mut self, reason: String) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::SubmittedToRelay(_) | MEVPlayStatus::SubmittedToMempool(_)
            | MEVPlayStatus::PendingInclusion | MEVPlayStatus::PendingConfirmation => {
                self.status = MEVPlayStatus::ConfirmedFailure(reason.clone());
                self.last_error = Some(reason);
                self.metadata.confirmation_timestamp = Some(Instant::now());
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only confirm failure from active states")
        }
    }

    /// Mark play as expired - explicit state transition
    pub fn mark_expired(&mut self) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::Identified | MEVPlayStatus::Simulating | MEVPlayStatus::ReadyForSubmission
            | MEVPlayStatus::UrgentSubmission | MEVPlayStatus::SubmittedToRelay(_)
            | MEVPlayStatus::SubmittedToMempool(_) | MEVPlayStatus::PendingInclusion
            | MEVPlayStatus::PendingConfirmation => {
                self.status = MEVPlayStatus::Expired;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: cannot expire terminal states")
        }
    }

    /// Mark play as cancelled - explicit state transition
    pub fn cancel(&mut self, reason: Option<String>) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::Identified | MEVPlayStatus::Simulating | MEVPlayStatus::ReadyForSubmission
            | MEVPlayStatus::UrgentSubmission => {
                self.status = MEVPlayStatus::Cancelled;
                self.last_error = reason;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only cancel from initial states")
        }
    }

    /// Mark play as ready for submission - explicit state transition
    pub fn mark_ready_for_submission(&mut self) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::Identified | MEVPlayStatus::Simulating => {
                self.status = MEVPlayStatus::ReadyForSubmission;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only mark ready from Identified or Simulating states")
        }
    }

    /// Mark play as urgent submission - explicit state transition
    pub fn mark_urgent_submission(&mut self) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::ReadyForSubmission => {
                self.status = MEVPlayStatus::UrgentSubmission;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only mark urgent from ReadyForSubmission state")
        }
    }

    /// Record simulation failure - explicit state transition
    pub fn record_simulation_failure(&mut self, reason: String) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::Simulating => {
                self.status = MEVPlayStatus::SimulationFailed(reason.clone());
                self.last_error = Some(reason);
                self.simulation_attempts += 1;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only record simulation failure from Simulating state")
        }
    }

    /// Mark play as simulating - explicit state transition
    pub fn start_simulation(&mut self) -> Result<(), &'static str> {
        match self.status {
            MEVPlayStatus::Identified => {
                self.status = MEVPlayStatus::Simulating;
                self.simulation_attempts += 1;
                self.last_updated_at = Instant::now();
                Ok(())
            }
            _ => Err("Invalid state transition: can only start simulation from Identified state")
        }
    }

    // Legacy method for backward compatibility - deprecated
    #[deprecated(note = "Use explicit state transition methods instead")]
    pub fn update_status(&mut self, new_status: MEVPlayStatus) {
        self.status = new_status;
        self.last_updated_at = Instant::now();

        // Update metadata based on status
        match &self.status {
            MEVPlayStatus::SubmittedToRelay(_) | MEVPlayStatus::SubmittedToMempool(_) => {
                self.metadata.submission_timestamp = Some(Instant::now());
            }
            MEVPlayStatus::ConfirmedSuccess(_) | MEVPlayStatus::ConfirmedFailure(_) => {
                self.metadata.confirmation_timestamp = Some(Instant::now());
            }
            MEVPlayStatus::SimulationFailed(reason) => {
                self.last_error = Some(reason.clone());
                self.simulation_attempts += 1;
            }
            _ => {}
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    pub fn is_expired(&self, max_age_seconds: u64) -> bool {
        self.age().as_secs() > max_age_seconds
    }

    pub fn should_retry(&self, max_retries: u32) -> bool {
        !self.is_terminal() && self.retry_count < max_retries
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_updated_at = Instant::now();
    }

    pub fn execution_latency(&self) -> Option<std::time::Duration> {
        match (self.metadata.submission_timestamp, self.metadata.confirmation_timestamp) {
            (Some(submit), Some(confirm)) => Some(confirm.duration_since(submit)),
            _ => None,
        }
    }
}

// Thread-safe wrapper for ActiveMEVPlay
#[derive(Clone)]
pub struct SafeActiveMEVPlay {
    inner: Arc<RwLock<ActiveMEVPlay>>,
}

impl SafeActiveMEVPlay {
    pub fn new(play: ActiveMEVPlay) -> Self {
        Self {
            inner: Arc::new(RwLock::new(play)),
        }
    }

    pub async fn update_atomic<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ActiveMEVPlay) -> R,
    {
        let mut play = self.inner.write().await;
        f(&mut *play)
    }

    pub async fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ActiveMEVPlay) -> R,
    {
        let play = self.inner.read().await;
        f(&*play)
    }

    pub async fn clone_inner(&self) -> ActiveMEVPlay {
        let play = self.inner.read().await;
        play.clone()
    }

    /// Submit play to relay with bundle hash - thread-safe wrapper
    pub async fn submit_to_relay(&self, bundle_hash: H256) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.submit_to_relay(bundle_hash)
    }

    /// Submit play to mempool with transaction hashes - thread-safe wrapper
    pub async fn submit_to_mempool(&self, tx_hashes: Vec<H256>) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.submit_to_mempool(tx_hashes)
    }

    /// Mark play as pending inclusion - thread-safe wrapper
    pub async fn mark_pending_inclusion(&self) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.mark_pending_inclusion()
    }

    /// Mark play as pending confirmation - thread-safe wrapper
    pub async fn mark_pending_confirmation(&self, inclusion_block: u64) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.mark_pending_confirmation(inclusion_block)
    }

    /// Mark play as confirmed success - thread-safe wrapper
    pub async fn confirm_success(&self, actual_profit_usd: f64) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.confirm_success(actual_profit_usd)
    }

    /// Mark play as confirmed failure - thread-safe wrapper
    pub async fn confirm_failure(&self, reason: String) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.confirm_failure(reason)
    }

    /// Mark play as expired - thread-safe wrapper
    pub async fn mark_expired(&self) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.mark_expired()
    }

    /// Cancel play - thread-safe wrapper
    pub async fn cancel(&self, reason: Option<String>) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.cancel(reason)
    }

    /// Mark play as ready for submission - thread-safe wrapper
    pub async fn mark_ready_for_submission(&self) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.mark_ready_for_submission()
    }

    /// Mark play as urgent submission - thread-safe wrapper
    pub async fn mark_urgent_submission(&self) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.mark_urgent_submission()
    }

    /// Start simulation - thread-safe wrapper
    pub async fn start_simulation(&self) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.start_simulation()
    }

    /// Record simulation failure - thread-safe wrapper
    pub async fn record_simulation_failure(&self, reason: String) -> Result<(), &'static str> {
        let mut play = self.inner.write().await;
        play.record_simulation_failure(reason)
    }
}

#[derive(Debug, Clone)]
pub struct MEVPlayMetrics {
    pub total_plays: u64,
    pub successful_plays: u64,
    pub failed_plays: u64,
    pub total_profit_usd: f64,
    pub total_loss_usd: f64,
    pub average_latency_ms: f64,
    pub plays_by_type: std::collections::HashMap<String, u64>,
    pub profit_by_type: std::collections::HashMap<String, f64>,
}

impl Default for MEVPlayMetrics {
    fn default() -> Self {
        Self {
            total_plays: 0,
            successful_plays: 0,
            failed_plays: 0,
            total_profit_usd: 0.0,
            total_loss_usd: 0.0,
            average_latency_ms: 0.0,
            plays_by_type: std::collections::HashMap::new(),
            profit_by_type: std::collections::HashMap::new(),
        }
    }
}

impl MEVPlayMetrics {
    pub fn success_rate(&self) -> f64 {
        if self.total_plays == 0 {
            0.0
        } else {
            (self.successful_plays as f64) / (self.total_plays as f64)
        }
    }

    pub fn net_profit(&self) -> f64 {
        self.total_profit_usd - self.total_loss_usd
    }

    pub fn average_profit_per_play(&self) -> f64 {
        if self.successful_plays == 0 {
            0.0
        } else {
            self.total_profit_usd / (self.successful_plays as f64)
        }
    }
}

#[derive(Debug, Clone)]
pub enum MEVEngineError {
    StrategyError(String),
    SimulationError(String),
    ValidationError(String),
    PricingError(String),
    ArithmeticError(String),
    Timeout(String),
    CircuitBreakerOpen(String),
    InsufficientBalance(String),
    ResourceExhausted(String),
    PlayNotFound(String),
    ConfigError(String),
    NetworkError(String),
    UnknownError(String),
    MonitoringError(String),
    ProfitTooLow(String),
    ExecutionError(String),
}

impl From<crate::errors::MEVEngineError> for MEVEngineError {
    fn from(err: crate::errors::MEVEngineError) -> Self {
        match err {
            crate::errors::MEVEngineError::Configuration(msg) => MEVEngineError::ConfigError(msg),
            crate::errors::MEVEngineError::StrategyError(msg) => MEVEngineError::StrategyError(msg),
            crate::errors::MEVEngineError::SimulationError(msg) => MEVEngineError::SimulationError(msg),
            crate::errors::MEVEngineError::RiskAssessment(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::TransactionOptimization(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::BundleSubmission(msg) => MEVEngineError::NetworkError(msg),
            crate::errors::MEVEngineError::OpportunityProcessing(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::PlayManagement(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::GasEstimation(msg) => MEVEngineError::PricingError(msg),
            crate::errors::MEVEngineError::PriceOracle(msg) => MEVEngineError::PricingError(msg),
            crate::errors::MEVEngineError::Blockchain(msg) => MEVEngineError::NetworkError(msg),
            crate::errors::MEVEngineError::PoolManager(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::PathFinding(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::RiskError(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::SubmissionError(msg) => MEVEngineError::NetworkError(msg),
            crate::errors::MEVEngineError::PricingError(msg) => MEVEngineError::PricingError(msg),
            crate::errors::MEVEngineError::CalldataError(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::UnsupportedProtocol(msg) => MEVEngineError::UnknownError(msg),
            crate::errors::MEVEngineError::UnsupportedStrategy(msg) => MEVEngineError::UnknownError(msg),
            crate::errors::MEVEngineError::ResourceError(msg) => MEVEngineError::ResourceExhausted(msg),
            crate::errors::MEVEngineError::MonitorError(msg) => MEVEngineError::MonitoringError(msg),
            crate::errors::MEVEngineError::ProfitTooLow(msg) => MEVEngineError::ProfitTooLow(msg),
            crate::errors::MEVEngineError::MonitoringError(msg) => MEVEngineError::MonitoringError(msg),
            crate::errors::MEVEngineError::ConfigError(msg) => MEVEngineError::ConfigError(msg),
            crate::errors::MEVEngineError::ExecutionError(msg) => MEVEngineError::ExecutionError(msg),
            crate::errors::MEVEngineError::Timeout(msg) => MEVEngineError::Timeout(msg),
            crate::errors::MEVEngineError::ArithmeticError(msg) => MEVEngineError::ArithmeticError(msg),
            crate::errors::MEVEngineError::ValidationError(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::MEVEngineError::InsufficientBalance(msg) => MEVEngineError::InsufficientBalance(msg),
            crate::errors::MEVEngineError::Other(msg) => MEVEngineError::UnknownError(msg),
        }
    }
}

impl From<crate::errors::GasError> for MEVEngineError {
    fn from(err: crate::errors::GasError) -> Self {
        match err {
            crate::errors::GasError::GasPriceLookup(msg) => MEVEngineError::PricingError(msg),
            crate::errors::GasError::GasEstimation(msg) => MEVEngineError::PricingError(msg),
            crate::errors::GasError::Config(msg) => MEVEngineError::ConfigError(msg),
            crate::errors::GasError::HistoricalDataUnavailable { chain_name, block_number } => {
                MEVEngineError::PricingError(format!("Historical gas data unavailable for {} at block {}", chain_name, block_number))
            },
            crate::errors::GasError::InvalidRequest(msg) => MEVEngineError::ValidationError(msg),
            crate::errors::GasError::StateError(msg) => MEVEngineError::NetworkError(msg),
            crate::errors::GasError::DataSource(msg) => MEVEngineError::NetworkError(msg),
            crate::errors::GasError::Blockchain(e) => MEVEngineError::NetworkError(e.to_string()),
            crate::errors::GasError::Database(e) => MEVEngineError::NetworkError(e.to_string()),
            crate::errors::GasError::Other(msg) => MEVEngineError::UnknownError(msg),
        }
    }
}

impl From<crate::errors::OptimizerError> for MEVEngineError {
    fn from(err: crate::errors::OptimizerError) -> Self {
        MEVEngineError::NetworkError(err.to_string())
    }
}

impl From<crate::errors::BlockchainError> for MEVEngineError {
    fn from(err: crate::errors::BlockchainError) -> Self {
        MEVEngineError::NetworkError(err.to_string())
    }
}

impl std::fmt::Display for MEVEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MEVEngineError::StrategyError(msg) => write!(f, "Strategy error: {}", msg),
            MEVEngineError::SimulationError(msg) => write!(f, "Simulation error: {}", msg),
            MEVEngineError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            MEVEngineError::PricingError(msg) => write!(f, "Pricing error: {}", msg),
            MEVEngineError::ArithmeticError(msg) => write!(f, "Arithmetic error: {}", msg),
            MEVEngineError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            MEVEngineError::CircuitBreakerOpen(msg) => write!(f, "Circuit breaker open: {}", msg),
            MEVEngineError::InsufficientBalance(msg) => write!(f, "Insufficient balance: {}", msg),
            MEVEngineError::ResourceExhausted(msg) => write!(f, "Resource exhausted: {}", msg),
            MEVEngineError::PlayNotFound(msg) => write!(f, "Play not found: {}", msg),
            MEVEngineError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            MEVEngineError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            MEVEngineError::MonitoringError(msg) => write!(f, "Monitoring error: {}", msg),
            MEVEngineError::ProfitTooLow(msg) => write!(f, "Profit too low: {}", msg),
            MEVEngineError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            MEVEngineError::UnknownError(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for MEVEngineError {}