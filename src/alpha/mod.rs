//! Alpha Engine Module
//!
//! This module contains the sophisticated alpha generation system that discovers 
//! and evaluates trading opportunities across multiple strategies and market regimes.
//! 
//! The alpha engine operates on the principle of multi-strategy diversification,
//! running various discovery scanners in parallel to identify profitable trades
//! while managing risk through circuit breakers and statistical analysis.

pub mod circuit_breaker;
pub mod types;
pub mod engine;
pub mod statistical_utils;
pub mod scanner_manager;
use std::sync::Arc;

// Re-export main components
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerStats};
pub use types::{
    StatisticalMetrics, TokenPairStats, LiquidationCandidate, YieldStrategy, 
    OptionsPosition, OptionType, OpportunityType, YieldStrategyType
};
pub use engine::AlphaEngine;
pub use statistical_utils::{
    mean, standard_deviation, calculate_garch_volatility, calculate_sharpe_ratio,
    calculate_sortino_ratio, calculate_max_drawdown, calculate_correlation,
    MovingAverage, ExponentialMovingAverage, BollingerBands, RSI, MACD
};

/// Alpha engine configuration parameters
#[derive(Debug, Clone)]
pub struct AlphaEngineConfig {
    pub min_profit_threshold_usd: f64,
    pub max_concurrent_ops: usize,
    pub circuit_breaker_threshold: u64,
    pub circuit_breaker_timeout_seconds: u64,
    pub min_scan_amount_wei: ethers::types::U256,
    pub min_pool_liquidity_usd: f64,
    pub statistical_arb_z_score_threshold: f64,
    pub market_regime_volatility_threshold: f64,
    pub max_position_size_usd: f64,
    pub risk_free_rate: f64,
    pub enable_cross_chain: bool,
    pub enable_options_arbitrage: bool,
    pub enable_yield_farming: bool,
    pub enable_liquidations: bool,
}

impl Default for AlphaEngineConfig {
    fn default() -> Self {
        Self {
            min_profit_threshold_usd: 10.0,
            max_concurrent_ops: 10,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_seconds: 300,
            min_scan_amount_wei: ethers::types::U256::from(1000000000000000000u64), // 1 ETH
            min_pool_liquidity_usd: 0.01,
            statistical_arb_z_score_threshold: 2.0,
            market_regime_volatility_threshold: 0.25,
            max_position_size_usd: 100000.0,
            risk_free_rate: 0.02, // 2% annual
            enable_cross_chain: true,
            enable_options_arbitrage: false, // Experimental
            enable_yield_farming: true,
            enable_liquidations: true,
        }
    }
}

impl AlphaEngineConfig {
    /// Construct AlphaEngineConfig from the global Config's module section
    pub fn from_config(cfg: &Arc<crate::config::Config>) -> Result<Self, crate::errors::BotError> {
        let mc = cfg.module_config.as_ref()
            .ok_or_else(|| crate::errors::BotError::Config("Missing module_config".to_string()))?;
        let alpha = mc.alpha_engine_settings.as_ref()
            .ok_or_else(|| crate::errors::BotError::Config("Missing alpha_engine_settings".to_string()))?;
        Ok(Self {
            min_profit_threshold_usd: alpha.min_profit_threshold_usd,
            max_concurrent_ops: alpha.max_concurrent_ops,
            circuit_breaker_threshold: alpha.circuit_breaker_threshold,
            circuit_breaker_timeout_seconds: alpha.circuit_breaker_timeout_seconds,
            min_scan_amount_wei: alpha.min_scan_amount_wei,
            min_pool_liquidity_usd: alpha.min_pool_liquidity_usd,
            statistical_arb_z_score_threshold: alpha.statistical_arb_z_score_threshold,
            market_regime_volatility_threshold: 0.25,
            max_position_size_usd: 100000.0,
            risk_free_rate: 0.02,
            enable_cross_chain: true,
            enable_options_arbitrage: false,
            enable_yield_farming: true,
            enable_liquidations: true,
        })
    }
}

/// Scanner types for dynamic management
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScannerType {
    Cyclic,
    Statistical,
    MarketMaking,
    CrossChain,
}

/// Configuration for active scanners per market regime
#[derive(Debug, Clone)]
pub struct RegimeScannerConfig {
    pub active_scanners: Vec<ScannerType>,
    pub priority_order: Vec<ScannerType>, // Execution priority (first = highest)
    pub resource_allocation: f64, // 0.0-1.0, fraction of resources for this regime
}

impl Default for RegimeScannerConfig {
    fn default() -> Self {
        Self {
            active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking],
            priority_order: vec![ScannerType::Cyclic, ScannerType::MarketMaking, ScannerType::Statistical],
            resource_allocation: 1.0,
        }
    }
}

/// Market regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    Bull,
    Bear,
    Sideways,
    HighVolatility,
    LowVolatility,
    Trending,
    Ranging,
    Crisis,
}

impl MarketRegime {
    /// Get the recommended strategy weights for this market regime
    pub fn strategy_weights(&self) -> StrategyWeights {
        match self {
            MarketRegime::Bull => StrategyWeights {
                momentum_weight: 0.4,
                mean_reversion_weight: 0.2,
                arbitrage_weight: 0.3,
                volatility_weight: 0.1,
            },
            MarketRegime::Bear => StrategyWeights {
                momentum_weight: 0.2,
                mean_reversion_weight: 0.4,
                arbitrage_weight: 0.3,
                volatility_weight: 0.1,
            },
            MarketRegime::Sideways => StrategyWeights {
                momentum_weight: 0.1,
                mean_reversion_weight: 0.5,
                arbitrage_weight: 0.3,
                volatility_weight: 0.1,
            },
            MarketRegime::HighVolatility => StrategyWeights {
                momentum_weight: 0.2,
                mean_reversion_weight: 0.2,
                arbitrage_weight: 0.4,
                volatility_weight: 0.2,
            },
            MarketRegime::LowVolatility => StrategyWeights {
                momentum_weight: 0.3,
                mean_reversion_weight: 0.4,
                arbitrage_weight: 0.2,
                volatility_weight: 0.1,
            },
            MarketRegime::Trending => StrategyWeights {
                momentum_weight: 0.5,
                mean_reversion_weight: 0.1,
                arbitrage_weight: 0.3,
                volatility_weight: 0.1,
            },
            MarketRegime::Ranging => StrategyWeights {
                momentum_weight: 0.1,
                mean_reversion_weight: 0.6,
                arbitrage_weight: 0.2,
                volatility_weight: 0.1,
            },
            MarketRegime::Crisis => StrategyWeights {
                momentum_weight: 0.1,
                mean_reversion_weight: 0.1,
                arbitrage_weight: 0.6,
                volatility_weight: 0.2,
            },
        }
    }

    /// Get the recommended scanner configuration for this market regime
    pub fn scanner_config(&self) -> RegimeScannerConfig {
        match self {
            MarketRegime::Bull => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::MarketMaking, ScannerType::Statistical],
                priority_order: vec![ScannerType::MarketMaking, ScannerType::Cyclic, ScannerType::Statistical],
                resource_allocation: 0.8,
            },
            MarketRegime::Bear => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical],
                priority_order: vec![ScannerType::Cyclic, ScannerType::Statistical],
                resource_allocation: 0.7,
            },
            MarketRegime::Sideways => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Statistical, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.9,
            },
            MarketRegime::HighVolatility => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::MarketMaking], // Disable complex strategies
                priority_order: vec![ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.6,
            },
            MarketRegime::LowVolatility => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking, ScannerType::CrossChain],
                priority_order: vec![ScannerType::Statistical, ScannerType::CrossChain, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 1.0,
            },
            MarketRegime::Trending => RegimeScannerConfig {
                active_scanners: vec![ScannerType::MarketMaking, ScannerType::Cyclic],
                priority_order: vec![ScannerType::MarketMaking, ScannerType::Cyclic],
                resource_allocation: 0.7,
            },
            MarketRegime::Ranging => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical],
                priority_order: vec![ScannerType::Statistical, ScannerType::Cyclic],
                resource_allocation: 0.8,
            },
            MarketRegime::Crisis => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic], // Only most reliable strategy
                priority_order: vec![ScannerType::Cyclic],
                resource_allocation: 0.5,
            },
        }
    }

    /// Get risk adjustment factor for this regime
    pub fn risk_adjustment_factor(&self) -> f64 {
        match self {
            MarketRegime::Bull => 0.8,
            MarketRegime::Bear => 1.2,
            MarketRegime::Sideways => 1.0,
            MarketRegime::HighVolatility => 1.5,
            MarketRegime::LowVolatility => 0.7,
            MarketRegime::Trending => 0.9,
            MarketRegime::Ranging => 1.0,
            MarketRegime::Crisis => 2.0,
        }
    }
}

/// Strategy allocation weights for different market regimes
#[derive(Debug, Clone)]
pub struct StrategyWeights {
    pub momentum_weight: f64,
    pub mean_reversion_weight: f64,
    pub arbitrage_weight: f64,
    pub volatility_weight: f64,
}

impl StrategyWeights {
    /// Normalize weights to sum to 1.0
    pub fn normalize(&mut self) {
        let total = self.momentum_weight + self.mean_reversion_weight 
                   + self.arbitrage_weight + self.volatility_weight;
        
        if total > 0.0 {
            self.momentum_weight /= total;
            self.mean_reversion_weight /= total;
            self.arbitrage_weight /= total;
            self.volatility_weight /= total;
        }
    }

    /// Get weight for a specific opportunity type
    pub fn weight_for_opportunity(&self, opportunity_type: OpportunityType) -> f64 {
        match opportunity_type {
            OpportunityType::MomentumTrading => self.momentum_weight,
            OpportunityType::MeanReversion | OpportunityType::StatisticalArbitrage 
            | OpportunityType::PairTrading => self.mean_reversion_weight,
            OpportunityType::SingleChainCyclic | OpportunityType::MultiDexTriangular 
            | OpportunityType::CrossChainArbitrage | OpportunityType::FlashLoanArbitrage 
            | OpportunityType::AtomicArbitrage => self.arbitrage_weight,
            OpportunityType::VolatilityArbitrage | OpportunityType::OptionsArbitrage => self.volatility_weight,
            _ => self.arbitrage_weight, // Default to arbitrage weight
        }
    }
}

/// Performance tracking for the alpha engine
#[derive(Debug, Clone)]
pub struct AlphaPerformanceMetrics {
    pub total_opportunities_found: u64,
    pub total_opportunities_executed: u64,
    pub total_profit_usd: f64,
    pub total_loss_usd: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub average_holding_time_seconds: f64,
    pub opportunities_by_type: std::collections::HashMap<OpportunityType, u64>,
    pub profit_by_type: std::collections::HashMap<OpportunityType, f64>,
    pub active_scanners: usize,
    pub scanner_performance: std::collections::HashMap<String, ScannerPerformanceMetrics>,
}

impl AlphaPerformanceMetrics {
    pub fn new() -> Self {
        Self {
            total_opportunities_found: 0,
            total_opportunities_executed: 0,
            total_profit_usd: 0.0,
            total_loss_usd: 0.0,
            win_rate: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
            average_holding_time_seconds: 0.0,
            opportunities_by_type: std::collections::HashMap::new(),
            profit_by_type: std::collections::HashMap::new(),
            active_scanners: 0,
            scanner_performance: std::collections::HashMap::new(),
        }
    }

    /// Calculate return on investment
    pub fn roi(&self) -> f64 {
        if self.total_loss_usd == 0.0 {
            if self.total_profit_usd > 0.0 { 100.0 } else { 0.0 }
        } else {
            (self.total_profit_usd / self.total_loss_usd - 1.0) * 100.0
        }
    }

    /// Calculate profit factor
    pub fn profit_factor(&self) -> f64 {
        if self.total_loss_usd == 0.0 {
            if self.total_profit_usd > 0.0 { f64::INFINITY } else { 0.0 }
        } else {
            self.total_profit_usd / self.total_loss_usd
        }
    }

    /// Get the most profitable opportunity type
    pub fn most_profitable_strategy(&self) -> Option<OpportunityType> {
        self.profit_by_type
            .iter()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(opportunity_type, _)| *opportunity_type)
    }
}

impl Default for AlphaPerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance metrics for individual scanners
#[derive(Debug, Clone)]
pub struct ScannerPerformanceMetrics {
    pub opportunities_found: u64,
    pub opportunities_executed: u64,
    pub total_profit_usd: f64,
    pub avg_scan_time_ms: f64,
    pub error_count: u64,
    pub last_error: Option<String>,
    pub uptime_percentage: f64,
}

impl ScannerPerformanceMetrics {
    pub fn new() -> Self {
        Self {
            opportunities_found: 0,
            opportunities_executed: 0,
            total_profit_usd: 0.0,
            avg_scan_time_ms: 0.0,
            error_count: 0,
            last_error: None,
            uptime_percentage: 100.0,
        }
    }

    /// Calculate the execution rate for this scanner
    pub fn execution_rate(&self) -> f64 {
        if self.opportunities_found == 0 {
            0.0
        } else {
            self.opportunities_executed as f64 / self.opportunities_found as f64
        }
    }

    /// Calculate profit per opportunity for this scanner
    pub fn profit_per_opportunity(&self) -> f64 {
        if self.opportunities_executed == 0 {
            0.0
        } else {
            self.total_profit_usd / self.opportunities_executed as f64
        }
    }
}

impl Default for ScannerPerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}