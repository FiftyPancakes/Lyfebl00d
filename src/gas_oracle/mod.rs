// src/gas_oracle/mod.rs

use crate::errors::GasError;
use async_trait::async_trait;
use ethers::types::U256;
use crate::types::FeeData;
use crate::config::GasOracleConfig;
use std::sync::Arc;
use crate::blockchain::BlockchainManager;
use crate::config::Config;
use crate::swap_loader::HistoricalSwapLoader;
pub mod historical_gas_provider;
pub mod live_gas_provider;

/// GasPrice represents the EIP-1559 gas fee structure: base_fee and priority_fee (tip).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GasPrice {
    pub base_fee: U256,
    pub priority_fee: U256,
}

const HIGH_GAS_GWEI: f64 = 150.0;
const LOW_GAS_GWEI: f64 = 10.0;

impl GasPrice {
    pub fn effective_price(&self) -> U256 {
        self.base_fee.saturating_add(self.priority_fee)
    }

    /// Converts the gas price into a normalized score from 0.0 (low) to 1.0 (high).
    /// This provides a consistent way to reason about gas costs across different chains
    /// and contexts. The thresholds are based on general observations and may need
    /// to be made configurable in the future for chain-specific tuning.
    pub fn normalized_level(&self) -> f64 {
        // Safe conversion with overflow protection
        let effective_price = self.effective_price();
        let effective_price_gwei = if effective_price > U256::from(u128::MAX) {
            // If price exceeds u128::MAX, it's definitely high gas
            return 1.0;
        } else {
            effective_price.as_u128() as f64 / 1_000_000_000.0
        };

        if effective_price_gwei <= LOW_GAS_GWEI {
            0.0
        } else if effective_price_gwei >= HIGH_GAS_GWEI {
            1.0
        } else {
            // Safe because HIGH_GAS_GWEI > LOW_GAS_GWEI (constants)
            (effective_price_gwei - LOW_GAS_GWEI) / (HIGH_GAS_GWEI - LOW_GAS_GWEI)
        }
    }

    /// Chain-aware normalization: callers should pass per-chain low/high thresholds in gwei.
    /// Returns a value in [0.0, 1.0] where 0.0 represents low gas and 1.0 high gas for that chain.
    pub fn normalized_level_for_chain(&self, low_gwei: f64, high_gwei: f64) -> f64 {
        let effective_price = self.effective_price();
        let effective_price_gwei = if effective_price > U256::from(u128::MAX) {
            return 1.0;
        } else {
            effective_price.as_u128() as f64 / 1_000_000_000.0
        };

        if high_gwei <= low_gwei {
            // Degenerate thresholds, treat as step function against high_gwei
            return if effective_price_gwei >= high_gwei { 1.0 } else { 0.0 };
        }

        if effective_price_gwei <= low_gwei {
            0.0
        } else if effective_price_gwei >= high_gwei {
            1.0
        } else {
            (effective_price_gwei - low_gwei) / (high_gwei - low_gwei)
        }
    }
}

/// GasOracleProvider defines the interface for all gas oracles (live or historical).
/// It enforces strict delegation, dependency injection, and mode-awareness.
/// All implementations must be thread-safe and stateless where possible.
#[async_trait]
pub trait GasOracleProvider: Send + Sync + std::fmt::Debug {
    /// Returns the gas price (base_fee and priority_fee) for a given chain.
    /// - In Live mode, block_number is ignored.
    /// - In Backtest mode, block_number is required and must be passed to all dependencies.
    async fn get_gas_price(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError>;

    /// Returns statistical data about gas fees for a given chain.
    async fn get_fee_data(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<FeeData, GasError>;
    
    fn get_chain_name_from_id(&self, chain_id: u64) -> Option<String>;
    
    fn estimate_protocol_gas(&self, protocol: &crate::types::ProtocolType) -> u64;

    /// Get gas price with specific block context
    async fn get_gas_price_with_block(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError>;

    /// Estimate gas cost in native currency
    async fn estimate_gas_cost_native(
        &self,
        chain_name: &str,
        gas_units: u64,
    ) -> Result<f64, GasError>;
}

/// Facade for gas oracle functionality, delegating to the injected provider implementation.
#[derive(Clone)]
pub struct GasOracle {
    inner: Arc<dyn GasOracleProvider + Send + Sync>,
}

impl GasOracle {
    pub fn new(inner: Arc<dyn GasOracleProvider + Send + Sync>) -> Self {
        Self { inner }
    }

    pub async fn get_gas_price(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        self.inner.get_gas_price(chain_name, block_number).await
    }
    
    pub async fn get_fee_data(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<FeeData, GasError> {
        self.inner.get_fee_data(chain_name, block_number).await
    }

    pub fn inner(&self) -> &Arc<dyn GasOracleProvider + Send + Sync> {
        &self.inner
    }

    pub async fn get_gas_price_with_block(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        self.inner.get_gas_price_with_block(chain_name, block_number).await
    }

    pub async fn estimate_gas_cost_native(
        &self,
        chain_name: &str,
        gas_units: u64,
    ) -> Result<f64, GasError> {
        self.inner.estimate_gas_cost_native(chain_name, gas_units).await
    }
}

impl std::fmt::Debug for GasOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GasOracle")
            .field("inner", &"Arc<dyn GasOracleProvider + Send + Sync>")
            .finish()
    }
}

pub use historical_gas_provider::HistoricalGasProvider;
pub use live_gas_provider::LiveGasProvider;

/// Create a gas oracle provider based on the configuration
pub async fn create_gas_oracle_provider(
    config: Arc<crate::config::Config>,
    blockchain_manager: Arc<dyn crate::blockchain::BlockchainManager>,
) -> Result<Arc<dyn GasOracleProvider + Send + Sync>, GasError> {
    // Default to live to preserve existing behavior
    create_gas_oracle_provider_with_mode(
        config,
        blockchain_manager,
        GasOracleMode::Live,
        None,
        None,
    ).await
}

/// Mode selector for gas oracle factory
#[derive(Debug, Clone)]
pub enum GasOracleMode {
    Live,
    Historical,
}

/// Mode-aware factory for gas oracle providers. When selecting Historical, the caller must
/// provide both a pinned block number and a historical loader.
pub async fn create_gas_oracle_provider_with_mode(
    config: Arc<Config>,
    blockchain_manager: Arc<dyn BlockchainManager>,
    mode: GasOracleMode,
    historical_block: Option<u64>,
    historical_loader: Option<Arc<HistoricalSwapLoader>>,
) -> Result<Arc<dyn GasOracleProvider + Send + Sync>, GasError> {
    let chain_name = blockchain_manager.get_chain_name().to_string();

    // Resolve gas oracle config from module_config or use safe defaults
    let gas_oracle_config = config
        .module_config
        .as_ref()
        .map(|m| m.gas_oracle_config.clone())
        .unwrap_or_else(|| GasOracleConfig {
            cache_ttl_secs: 60,
            backtest_default_priority_fee_gwei: 2,
            protocol_gas_estimates: std::collections::HashMap::new(),
            default_gas_estimate: 150_000,
            nearby_window_blocks: Some(50),
            block_distance_tolerance: Some(10),
        });

    match mode {
        GasOracleMode::Live => {
            let chain_id = config.chain_config.chains.get(&chain_name).unwrap().chain_id;
            let provider = LiveGasProvider::new(
                chain_name,
                chain_id,
                config.clone(),
                gas_oracle_config,
                blockchain_manager,
            );
            Ok(Arc::new(provider))
        }
        GasOracleMode::Historical => {
            let block = historical_block.ok_or_else(|| GasError::InvalidRequest(
                "Historical mode requires a block number".to_string(),
            ))?;
            let loader = historical_loader.ok_or_else(|| GasError::InvalidRequest(
                "Historical mode requires a HistoricalSwapLoader".to_string(),
            ))?;

            // Extract ChainConfig reference for the provider
            let chain_config = Arc::new(config.chain_config.clone());
            let provider = HistoricalGasProvider::new(
                chain_name,
                block,
                loader,
                gas_oracle_config,
                chain_config,
                config.strict_backtest,
            );
            Ok(Arc::new(provider))
        }
    }
}