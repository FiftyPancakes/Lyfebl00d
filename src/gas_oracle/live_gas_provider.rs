//! Live Gas Price Provider
//!
//! This module provides real-time gas price information for live trading.
//! It leverages blockchain provider connections to fetch current gas data using
//! EIP-1559 methods: latest block base_fee and fee_history for priority fee estimation.
//! Priority fees are calculated from recent fee_history data for stability.

use std::sync::Arc;
use moka::future::Cache;
use ethers::types::{U256, BlockNumber};
use ethers::prelude::Middleware;
use async_trait::async_trait;

use crate::{
    config::{Config, GasOracleConfig},
    errors::{GasError, BlockchainError},
    blockchain::BlockchainManager,
    gas_oracle::{GasPrice, GasOracleProvider},
    types::{FeeData, ProtocolType},
};

// Gas price validation constants (in wei)
const MIN_REASONABLE_BASE_FEE: u64 = 100_000_000; // 0.1 gwei
const MAX_REASONABLE_BASE_FEE: u64 = 1_000_000_000_000; // 1000 gwei  
const MIN_REASONABLE_PRIORITY_FEE: u64 = 100_000_000; // 0.1 gwei
const MAX_REASONABLE_PRIORITY_FEE: u64 = 100_000_000_000; // 100 gwei

#[derive(Debug)]
/// Live Gas Provider implementation that fetches real-time gas prices using proper EIP-1559 methods.
/// This provider is scoped to a single chain for simplicity and safety.
pub struct LiveGasProvider {
    chain_name: String,
    chain_id: u64,
    config: Arc<Config>,
    gas_oracle_config: GasOracleConfig,
    blockchain_manager: Arc<dyn BlockchainManager + Send + Sync>,
    gas_cache: Cache<String, GasPrice>,
    fee_data_cache: Cache<String, FeeData>,
}

impl LiveGasProvider {
    /// Creates a new LiveGasProvider instance for a specific chain.
    pub fn new(
        chain_name: String,
        chain_id: u64,
        config: Arc<Config>,
        gas_oracle_config: GasOracleConfig,
        blockchain_manager: Arc<dyn BlockchainManager + Send + Sync>,
    ) -> Self {
        let ttl = std::time::Duration::from_secs(gas_oracle_config.cache_ttl_secs as u64);
        Self {
            chain_name,
            chain_id,
            config,
            gas_oracle_config,
            blockchain_manager,
            gas_cache: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(10)
                .build(),
            fee_data_cache: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(10)
                .build(),
        }
    }

    /// Determines if we're in a test environment based on config and chain_id
    fn is_test_environment(&self) -> bool {
        // Check config for test environment flag first
        if let Ok(chain_config) = self.config.get_chain_config(&self.chain_name) {
            if let Some(is_test) = chain_config.is_test_environment {
                return is_test;
            }
        }

        // Use chain_id for reliable test environment detection
        // Common test chain IDs: 31337 (Anvil/Hardhat), 1337 (Ganache), 43113 (Fuji testnet)
        matches!(self.chain_id, 31337 | 1337 | 43113)
    }

    /// Validates gas price components against reasonable bounds.
    /// For test environments and L2 chains, logs warnings instead of erroring.
    fn validate_gas_components(
        &self,
        base_fee: U256,
        priority_fee: U256,
    ) -> Result<(), GasError> {
        let is_test = self.is_test_environment();

        // Use chain_id for reliable L2 detection
        // Known L2 chain IDs: 10 (Optimism), 42161 (Arbitrum One), 137 (Polygon), 8453 (Base), 324 (zkSync Era)
        let is_l2 = matches!(self.chain_id, 10 | 42161 | 137 | 8453 | 324);
        
        // For test environments and L2s, be much more lenient
        if is_test || is_l2 {
            // Allow any non-negative values for test/L2 environments
            // Just log warnings for unusual values
            if base_fee == U256::zero() {
                log::warn!(
                    target: "live_gas_provider",
                    "Zero base_fee on chain_id {} (test={}, L2={})",
                    self.chain_id, is_test, is_l2
                );
            }

            if base_fee > U256::from(10_000_000_000_000u64) { // > 10,000 gwei
                log::warn!(
                    target: "live_gas_provider",
                    "Very high base_fee on chain_id {}: {} wei",
                    self.chain_id, base_fee
                );
            }

            if priority_fee > U256::from(1_000_000_000_000u64) { // > 1,000 gwei
                log::warn!(
                    target: "live_gas_provider",
                    "Very high priority_fee on chain_id {}: {} wei",
                    self.chain_id, priority_fee
                );
            }
            
            return Ok(());
        }
        
        // For mainnet, enforce stricter bounds
        if base_fee < U256::from(MIN_REASONABLE_BASE_FEE) {
            log::warn!(
                target: "live_gas_provider",
                "Low base_fee on mainnet chain_id {}: {} is below expected minimum of {}",
                self.chain_id, base_fee, MIN_REASONABLE_BASE_FEE
            );
            // Don't error, just warn - some chains may have legitimate low fees
        }

        if base_fee > U256::from(MAX_REASONABLE_BASE_FEE) {
            return Err(GasError::Other(format!(
                "Unrealistic base_fee on chain_id {}: {} is above maximum of {}",
                self.chain_id, base_fee, MAX_REASONABLE_BASE_FEE
            )));
        }

        // Allow zero priority fee even on mainnet (can happen during low activity)
        if priority_fee > U256::from(MAX_REASONABLE_PRIORITY_FEE) {
            log::warn!(
                target: "live_gas_provider",
                "High priority_fee on chain_id {}: {} is above typical maximum of {}",
                self.chain_id, priority_fee, MAX_REASONABLE_PRIORITY_FEE
            );
            // Don't error - during congestion this can be legitimate
        }
        
        Ok(())
    }

    /// Fetches current gas price components using proper EIP-1559 methods.
    async fn fetch_current_gas_components(&self) -> Result<(U256, U256), GasError> {
        let provider = self.blockchain_manager.get_provider();
        
        log::debug!(
            target: "live_gas_provider",
            "Fetching live gas components for chain_id: {}", self.chain_id
        );

        // Fetch both base fee and priority fee concurrently using proper methods
        let base_fee_fut = provider.get_block(BlockNumber::Latest);
        
        // Use a longer look-back window for smoother priority fee calculation
        // Last 5 blocks, 60th percentile for more stable values
        let fee_history_fut = provider.fee_history(5, BlockNumber::Latest, &[60.0]);

        let (block_result, fee_history_result) = tokio::join!(base_fee_fut, fee_history_fut);

        // Extract base fee from latest block
        let latest_block = block_result.map_err(|e| {
            GasError::DataSource(format!("Failed to fetch latest block: {}", e))
        })?;

        let base_fee = latest_block
            .and_then(|block| block.base_fee_per_gas)
            .ok_or_else(|| {
                GasError::DataSource(format!(
                    "Chain_id {} does not support EIP-1559 (no base_fee_per_gas in latest block)",
                    self.chain_id
                ))
            })?;

        // Extract priority fee from fee history with smoothing
        let fee_history = fee_history_result.map_err(|e| {
            GasError::DataSource(format!("Failed to fetch fee history: {}", e))
        })?;
        
        // Calculate average priority fee from recent blocks, filtering out zeros
        let valid_priority_fees: Vec<U256> = fee_history.reward
            .iter()
            .filter_map(|block_rewards| {
                // Each block_rewards is a Vec<U256> for each percentile
                // We asked for 60th percentile, so get first element
                block_rewards.get(0).copied()
            })
            .filter(|&fee| !fee.is_zero())
            .collect();

        let priority_fee = if valid_priority_fees.is_empty() {
            // Fallback to a reasonable default if no valid fees found
            log::debug!(
                target: "live_gas_provider",
                "No valid priority fees from history, using fallback for chain_id {}",
                self.chain_id
            );
            U256::from(2_000_000_000u64) // 2 gwei fallback
        } else {
            // Calculate average of valid priority fees with overflow protection
            let sum: U256 = valid_priority_fees.iter().fold(U256::zero(), |acc, &fee| {
                acc.saturating_add(fee)
            });
            // Safe division - we know len > 0 from the isEmpty check above
            sum.checked_div(U256::from(valid_priority_fees.len()))
                .unwrap_or(U256::from(2_000_000_000u64)) // 2 gwei fallback
        };

        self.validate_gas_components(base_fee, priority_fee)?;

        Ok((base_fee, priority_fee))
    }

    /// Gets protocol-specific gas estimates from configuration.
    fn get_protocol_gas_estimate(&self, protocol: &ProtocolType) -> u64 {
        let protocol_str = format!("{:?}", protocol);
        self.gas_oracle_config
            .protocol_gas_estimates
            .get(&protocol_str)
            .copied()
            .unwrap_or_else(|| {
                // Fallback defaults if not in config
                match protocol {
                    ProtocolType::UniswapV2 => 120_000,
                    ProtocolType::UniswapV3 => 150_000,
                    ProtocolType::SushiSwap => 125_000,
                    ProtocolType::Curve => 250_000,
                    ProtocolType::Balancer => 300_000,
                    _ => self.gas_oracle_config.default_gas_estimate,
                }
            })
    }
}

#[async_trait]
impl GasOracleProvider for LiveGasProvider {
    async fn get_gas_price(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        // Harden chain validation - resolve requested chain name once and compare directly
        let req_id = self.config.chain_config.chains.get(chain_name)
            .map(|c| c.chain_id)
            .ok_or_else(|| GasError::Other(format!("Unknown chain '{}'", chain_name)))?;

        if req_id != self.chain_id {
            return Err(GasError::Other(format!(
                "Chain_id mismatch: provider configured for chain_id {} but requested {} (chain_id {})",
                self.chain_id, chain_name, req_id
            )));
        }

        // Live provider does not support historical queries
        if block_number.is_some() {
            return Err(GasError::Blockchain(BlockchainError::ModeError(format!(
                "LiveGasProvider does not support historical queries (requested block: {:?})",
                block_number
            ))));
        }

        // Check cache first
        let cache_key = format!("gas_price_{}", chain_name);
        if let Some(cached_price) = self.gas_cache.get(&cache_key).await {
            return Ok(cached_price);
        }

        // Fetch current gas components using proper EIP-1559 methods
        let (base_fee, priority_fee) = self.fetch_current_gas_components().await?;

        let gas_price = GasPrice {
            base_fee,
            priority_fee,
        };

        // Cache the result
        self.gas_cache.insert(cache_key, gas_price.clone()).await;

        log::debug!(
            target: "live_gas_provider",
            "Fetched gas price for chain_id {}: base_fee={} gwei, priority_fee={} gwei",
            self.chain_id,
            ethers::utils::format_units(base_fee, "gwei").unwrap_or_default(),
            ethers::utils::format_units(priority_fee, "gwei").unwrap_or_default()
        );

        Ok(gas_price)
    }

    async fn get_fee_data(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<FeeData, GasError> {
        // Harden chain validation - resolve requested chain name once and compare directly
        let req_id = self.config.chain_config.chains.get(chain_name)
            .map(|c| c.chain_id)
            .ok_or_else(|| GasError::Other(format!("Unknown chain '{}'", chain_name)))?;

        if req_id != self.chain_id {
            return Err(GasError::Other(format!(
                "Chain_id mismatch: provider configured for chain_id {} but requested {} (chain_id {})",
                self.chain_id, chain_name, req_id
            )));
        }

        // Live provider does not support historical queries
        if block_number.is_some() {
            return Err(GasError::Blockchain(BlockchainError::ModeError(format!(
                "LiveGasProvider does not support historical queries (requested block: {:?})",
                block_number
            ))));
        }

        // Check cache first
        let cache_key = format!("fee_data_{}", chain_name);
        if let Some(cached_data) = self.fee_data_cache.get(&cache_key).await {
            return Ok(cached_data);
        }

        // Get current gas price components
        let gas_price = self.get_gas_price(chain_name, None).await?;
        
        // Get current block number
        let provider = self.blockchain_manager.get_provider();
        let current_block = provider.get_block_number().await.map_err(|e| {
            GasError::DataSource(format!("Failed to fetch current block number: {}", e))
        })?;

        // Use safer max_fee_per_gas cap: base_fee * 2 + priority_fee
        // This accounts for potential base_fee increases (up to +12.5%/block)
        let doubled_base_fee = gas_price.base_fee.checked_mul(U256::from(2))
            .ok_or_else(|| {
                GasError::Other(format!(
                    "Arithmetic overflow calculating doubled base_fee: {} * 2",
                    gas_price.base_fee
                ))
            })?;

        let max_fee_per_gas = doubled_base_fee.checked_add(gas_price.priority_fee)
            .ok_or_else(|| {
                GasError::Other(format!(
                    "Arithmetic overflow calculating max_fee_per_gas: {} + {}",
                    doubled_base_fee, gas_price.priority_fee
                ))
            })?;

        // For legacy gas_price, use base_fee + priority_fee, not the max fee cap
        let legacy_gas_price = gas_price
            .base_fee
            .checked_add(gas_price.priority_fee)
            .ok_or_else(|| GasError::Other(format!(
                "Arithmetic overflow calculating legacy gas_price: {} + {}",
                gas_price.base_fee, gas_price.priority_fee
            )))?;

        let fee_data = FeeData {
            base_fee_per_gas: Some(gas_price.base_fee),
            max_priority_fee_per_gas: Some(gas_price.priority_fee),
            max_fee_per_gas: Some(max_fee_per_gas),
            gas_price: Some(legacy_gas_price),
            block_number: current_block.as_u64(),
            chain_name: chain_name.to_string(),
        };

        // Cache the result
        self.fee_data_cache.insert(cache_key, fee_data.clone()).await;

        Ok(fee_data)
    }
    
    fn get_chain_name_from_id(&self, chain_id: u64) -> Option<String> {
        self.config.chain_config.chains
            .iter()
            .find(|(_, chain_config)| chain_config.chain_id == chain_id)
            .map(|(name, _)| name.clone())
    }

    fn estimate_protocol_gas(&self, protocol: &ProtocolType) -> u64 {
        self.get_protocol_gas_estimate(protocol)
    }

    async fn get_gas_price_with_block(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        // Harden chain validation - resolve requested chain name once and compare directly
        let req_id = self.config.chain_config.chains.get(chain_name)
            .map(|c| c.chain_id)
            .ok_or_else(|| GasError::Other(format!("Unknown chain '{}'", chain_name)))?;

        if req_id != self.chain_id {
            return Err(GasError::Other(format!(
                "Chain_id mismatch: provider configured for chain_id {} but requested {} (chain_id {})",
                self.chain_id, chain_name, req_id
            )));
        }

        // Live provider does not support historical queries
        if block_number.is_some() {
            return Err(GasError::Blockchain(BlockchainError::ModeError(format!(
                "LiveGasProvider does not support historical queries (requested block: {:?})",
                block_number
            ))));
        }

        // Delegate to the main get_gas_price method
        self.get_gas_price(chain_name, None).await
    }

    async fn estimate_gas_cost_native(
        &self,
        chain_name: &str,
        gas_units: u64,
    ) -> Result<f64, GasError> {
        // Harden chain validation - resolve requested chain name once and compare directly
        let req_id = self.config.chain_config.chains.get(chain_name)
            .map(|c| c.chain_id)
            .ok_or_else(|| GasError::Other(format!("Unknown chain '{}'", chain_name)))?;

        if req_id != self.chain_id {
            return Err(GasError::Other(format!(
                "Chain_id mismatch: provider configured for chain_id {} but requested {} (chain_id {})",
                self.chain_id, chain_name, req_id
            )));
        }

        // Get current gas price
        let gas_price = self.get_gas_price(chain_name, None).await?;
        
        // Calculate total gas cost in Wei
        let gas_units_u256 = U256::from(gas_units);
        let total_cost_wei = gas_price.effective_price()
            .checked_mul(gas_units_u256)
            .ok_or_else(|| {
                GasError::Other(format!(
                    "Arithmetic overflow calculating gas cost: {} * {}",
                    gas_price.effective_price(), gas_units_u256
                ))
            })?;

        // Convert Wei to ETH (assuming 18 decimals) - optimize for hot path
        let cost_eth = if total_cost_wei > U256::from(u128::MAX) {
            // Rare case: very large value, fall back to string formatting
            let total_cost_eth = ethers::utils::format_units(total_cost_wei, "ether")
                .map_err(|e| GasError::Other(format!("Failed to format gas cost: {}", e)))?;
            total_cost_eth.parse::<f64>()
                .map_err(|e| GasError::Other(format!("Failed to parse gas cost: {}", e)))?
        } else {
            // Common case: direct f64 calculation
            total_cost_wei.as_u128() as f64 / 1e18
        };

        log::debug!(
            target: "live_gas_provider",
            "Estimated gas cost for {} gas units on chain_id {}: {} ETH ({} Wei)",
            gas_units, self.chain_id, cost_eth, total_cost_wei
        );

        Ok(cost_eth)
    }
}