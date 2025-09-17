use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    blockchain::BlockchainManager,
    errors::MEVEngineError,
    price_oracle::PriceOracle,
    risk::RiskManager,
    transaction_optimizer::TransactionOptimizerTrait,
    pool_manager::PoolManagerTrait,
    execution::ExecutionBuilder,
    gas_oracle::GasOracleProvider,
};

pub mod sandwich;
pub mod liquidation;
pub mod jit_liquidity;
pub mod arbitrage;
pub mod discovery;
pub mod simulation;
pub mod analytics;

// Re-export discovery scanners
pub use discovery::{
    CyclicArbitrageScanner,
    StatisticalArbitrageScanner,
    CrossChainArbitrageScanner,
    MarketMakingScanner,
    DiscoveryScanner,
    DiscoveryConfig,
    ScannerContext,
    ScannerStats,
};

// ------------------------------------------------------------------------------------------------
// COMMON STRATEGY CONFIG
// ------------------------------------------------------------------------------------------------

/// Generic configuration for any MEV strategy.
///
/// * `min_profit_usd` — hard floor in USD below which the play is ignored.
/// * `ignore_risk`    — bypass risk-manager gating (useful for dry-run).
/// * `volatility_discount` — optional discount factor (0.0–1.0) applied to profit
///   estimates when the underlying pool is classified as highly volatile.
/// * `gas_escalation_factor` — scalar applied (in gwei) to priority fee to
///   out-bid competing bots under time pressure.
#[derive(Clone, Debug)]
pub struct MEVStrategyConfig {
    pub min_profit_usd: f64,
    pub ignore_risk: bool,
    pub volatility_discount: Option<f64>,
    pub gas_escalation_factor: Option<u128>,
}

// ------------------------------------------------------------------------------------------------
// STRATEGY TRAIT
// ------------------------------------------------------------------------------------------------

/// Every concrete MEV strategy must implement this trait.
#[async_trait]
pub trait MEVStrategy: Send + Sync {
    /// Human-readable strategy identifier.
    fn name(&self) -> &'static str;

    /// Predicate determining whether this strategy can handle `opportunity`.
    fn handles_opportunity_type(&self, opportunity: &crate::mempool::MEVOpportunity) -> bool;

    /// Core execution entry-point. Must update `active_play` status internally.
    async fn execute(
        &self,
        active_play: &crate::mev_engine::SafeActiveMEVPlay,
        opportunity: crate::mempool::MEVOpportunity,
        context: &StrategyContext,
    ) -> Result<(), MEVEngineError>;

    /// Accessor for the configured profit floor.
    fn min_profit_threshold_usd(&self) -> f64;

    /// Optional light-weight profit approximation for early filtering.
    /// Default is `0.0` (strategy will always request full simulation).
    async fn estimate_profit(
        &self,
        _opportunity: &crate::mempool::MEVOpportunity,
        _context: &StrategyContext,
    ) -> Result<f64, MEVEngineError> {
        Ok(0.0)
    }
}

// ------------------------------------------------------------------------------------------------
// ANALYTICS TRAIT
// ------------------------------------------------------------------------------------------------

/// Analytics trait for tracking strategy performance and metrics
#[async_trait]
pub trait AnalyticsTrait: Send + Sync {
    async fn track_opportunity(&self, opportunity_id: &str, metrics: &std::collections::HashMap<String, f64>);
    async fn get_performance_metrics(&self) -> std::collections::HashMap<String, f64>;
    async fn record_execution(&self, execution_id: &str, success: bool, profit_usd: f64);
}

// ------------------------------------------------------------------------------------------------
// STRATEGY CONTEXT
// ------------------------------------------------------------------------------------------------

pub struct StrategyContext {
    pub blockchain_manager: Arc<dyn BlockchainManager>,
    pub pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub risk_manager: Arc<dyn RiskManager + Send + Sync>,
    pub transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync>,
    pub simulation_engine: Arc<SimulationEngine>,
    pub execution_builder: Arc<ExecutionBuilder>,
    pub gas_oracle: tokio::sync::RwLock<Arc<dyn GasOracleProvider + Send + Sync>>,
    pub analytics: Arc<dyn AnalyticsTrait + Send + Sync>,
    pub quote_source: Arc<dyn crate::quote_source::AggregatorQuoteSource + Send + Sync>,
}

impl Clone for StrategyContext {
    fn clone(&self) -> Self {
        Self {
            blockchain_manager: self.blockchain_manager.clone(),
            pool_manager: self.pool_manager.clone(),
            price_oracle: self.price_oracle.clone(),
            risk_manager: self.risk_manager.clone(),
            transaction_optimizer: self.transaction_optimizer.clone(),
            simulation_engine: self.simulation_engine.clone(),
            execution_builder: self.execution_builder.clone(),
            gas_oracle: tokio::sync::RwLock::new(self.gas_oracle.try_read().unwrap().clone()),
            analytics: self.analytics.clone(),
            quote_source: self.quote_source.clone(),
        }
    }
}

// Re-export SimulationEngine from the dedicated simulation module
pub use simulation::SimulationEngine;
