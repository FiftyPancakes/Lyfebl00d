//! -----------------------------------------------------------------------------
//! Historical gas–price oracle (deterministic, back‑test optimised)
//! -----------------------------------------------------------------------------
//! • **Block‑pinned** — answers only at the provider’s `block_number`  
//! • **Batched DB read** — a single query per (chain, block)  
//!
//! Depends on queries in `HistoricalSwapLoader`:
//!   • `get_gas_info_for_block(chain, block)`   → `Option<GasPrice>`  
//!   • `get_fee_data_for_block(chain, block)`   → `FeeData`
//!   • `get_nearby_gas_price(chain, block, window)` → fallback data

use async_trait::async_trait;
use std::sync::Arc;
use ethers::types::U256;
use crate::{
    config::{GasOracleConfig, ChainConfig},
    errors::{BacktestError, GasError},
    gas_oracle::{GasOracleProvider, GasPrice},
    swap_loader::HistoricalSwapLoader,
    types::{FeeData, ProtocolType},
};
use tracing::{debug, warn, error};

impl From<BacktestError> for GasError {
    fn from(e: BacktestError) -> Self {
        GasError::DataSource(e.to_string())
    }
}

/// Historical gas oracle bound to a single (chain, block).
#[derive(Clone, Debug)]
pub struct HistoricalGasProvider {
    chain_name: String,
    block_number: u64,
    loader: Arc<HistoricalSwapLoader>,
    cfg: GasOracleConfig,
    chain_config: Arc<ChainConfig>,
    /// Strict mode disables any fallback or heuristic behavior
    strict: bool,
}

impl HistoricalGasProvider {
    pub fn new(
        chain_name:   String,
        block_number: u64,
        loader:       Arc<HistoricalSwapLoader>,
        cfg:          GasOracleConfig,
        chain_config: Arc<ChainConfig>,
        strict:       bool,
    ) -> Self {
        // Enable strict mode if the global config requests it; since we do not have
        // Config here, infer strict from chain_config presence in backtest via env flag.
        // The explicit strict flag will be propagated by callers in backtest engine.
        Self {
            chain_name,
            block_number,
            loader,
            cfg,
            chain_config,
            strict,
        }
    }

    /// Convenience helper; fetch timestamp first then create.
    ///
    /// This call to `get_fee_data_for_block` is used to "warm up" the loader's cache
    /// for the given block, ensuring that subsequent queries are fast and consistent.
    /// The result is not used directly here.
    pub async fn for_block(
        chain_name: String,
        block_number: u64,
        loader: Arc<HistoricalSwapLoader>,
        cfg: GasOracleConfig,
        chain_config: Arc<ChainConfig>,
    ) -> Result<Self, GasError> {
        // Warm up the loader's cache for this block (result is ignored)
        let _ = loader
            .get_fee_data_for_block(&chain_name, block_number as i64)
            .await
            .map_err(|e| GasError::DataSource(e.to_string()));
        let strict_env = std::env::var("STRICT_BACKTEST").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
        Ok(Self::new(chain_name, block_number, loader, cfg, chain_config, strict_env))
    }

    async fn price_at(&self) -> Result<GasPrice, GasError> {
        let block = self.block_number;
        debug!("Fetching gas price for provider's block: {}", block);

        // Try to get gas info for the specific block
        let gp_result = self
            .loader
            .get_gas_info_for_block(&self.chain_name, block as i64)
            .await
            .map_err(|e| GasError::DataSource(e.to_string()))?;

        match gp_result {
            Some(gp) => {
                if gp.base_fee == U256::zero() && gp.priority_fee == U256::zero() {
                    if self.strict {
                        return Err(GasError::DataSource(format!(
                            "Strict mode: zero gas values for block {}", block
                        )));
                    }
                    warn!("Gas price from database has zero values for block {}, trying nearby blocks", block);
                    self.get_fallback_gas_price(block).await
                } else {
                    Ok(gp)
                }
            }
            None => {
                if self.strict {
                    return Err(GasError::DataSource(format!(
                        "Strict mode: no gas price data for block {}", block
                    )));
                }
                warn!("No gas price data found for block {} in database, trying nearby blocks", block);
                self.get_fallback_gas_price(block).await
            }
        }
    }

    async fn get_fallback_gas_price(&self, block: u64) -> Result<GasPrice, GasError> {
        if self.strict {
            return Err(GasError::DataSource(format!(
                "Strict mode: fallback gas requested for block {}", block
            )));
        }
        let window: i64 = self.cfg.nearby_window_blocks.unwrap_or(50);
        debug!("Attempting to find nearby gas price within {} blocks for {}", window, block);

        match self.loader.get_nearby_gas_price(&self.chain_name, block as i64, window).await {
            Ok(Some(nearby_gp)) => Ok(nearby_gp),
            _ => self.get_config_default_gas_price(),
        }
    }

    fn get_config_default_gas_price(&self) -> Result<GasPrice, GasError> {
        // Get chain-specific configuration if available
        if let Some(chain_cfg) = self.chain_config.chains.get(&self.chain_name) {
            // Prefer explicit default_base_fee_gwei and default_priority_fee_gwei if set
            let base_fee = if let Some(base_fee_gwei) = chain_cfg.default_base_fee_gwei {
                U256::from(base_fee_gwei).saturating_mul(U256::from(1_000_000_000u64))
            } else {
                // Fallback: use 80% of max_gas_price as a heuristic
                chain_cfg
                    .max_gas_price
                    .saturating_mul(U256::from(80))
                    .checked_div(U256::from(100))
                    .unwrap_or(U256::from(25_000_000_000u64))
            };

            let priority_fee = if let Some(priority_fee_gwei) = chain_cfg.default_priority_fee_gwei {
                U256::from(priority_fee_gwei).saturating_mul(U256::from(1_000_000_000u64))
            } else {
                // Fallback: use 20% of base_fee as a heuristic
                base_fee
                    .saturating_mul(U256::from(20))
                    .checked_div(U256::from(100))
                    .unwrap_or(U256::from(1_500_000_000u64))
            };

            Ok(GasPrice {
                base_fee,
                priority_fee,
            })
        } else {
            // Global fallback if no chain config found
            warn!("No chain configuration found for {}, using global defaults", self.chain_name);
            let priority_fee_gwei = self.cfg.backtest_default_priority_fee_gwei;
            Ok(GasPrice {
                base_fee: U256::from(25_000_000_000u64), // 25 gwei
                priority_fee: U256::from(priority_fee_gwei).saturating_mul(U256::from(1_000_000_000u64)), // Convert gwei to wei
            })
        }
    }

    async fn fee_at(&self) -> Result<FeeData, GasError> {
        let block = self.block_number;
        debug!("Fetching fee data for provider's block: {}", block);

        // Try to get fee data for the specific block
        let fd_result = self
            .loader
            .get_fee_data_for_block(&self.chain_name, block as i64)
            .await
            .map_err(|e| GasError::DataSource(e.to_string()));

        match fd_result {
            Ok(fd) => Ok(fd),
            Err(e) => {
                error!("Error fetching fee data for block {}: {}", block, e);
                warn!("Attempting to construct fee data from gas price data for block {}", block);
                self.get_fallback_fee_data(block).await
            }
        }
    }

    async fn get_fallback_fee_data(&self, block: u64) -> Result<FeeData, GasError> {
        if self.strict {
            return Err(GasError::DataSource(format!(
                "Strict mode: fallback fee data requested for block {}", block
            )));
        }
        // Try to derive fee data from gas price information
        let gas_price_result = self.get_fallback_gas_price(block).await;

        match gas_price_result {
            Ok(gas_price) => {
                debug!("Constructing fee data from gas price for block {}", block);

                // Calculate max fee per gas (base + priority with some buffer)
                let max_fee_per_gas = gas_price.base_fee
                    .saturating_add(gas_price.priority_fee)
                    .saturating_mul(U256::from(110))
                    .checked_div(U256::from(100))
                    .unwrap_or_else(|| gas_price.base_fee.saturating_add(gas_price.priority_fee));

                Ok(FeeData {
                    base_fee_per_gas: Some(gas_price.base_fee),
                    max_fee_per_gas: Some(max_fee_per_gas),
                    max_priority_fee_per_gas: Some(gas_price.priority_fee),
                    gas_price: Some(gas_price.effective_price()),
                    block_number: block,
                    chain_name: self.chain_name.clone(),
                })
            }
            Err(_) => {
                // Final fallback: use config-driven defaults
                warn!("Unable to get gas price data, using config-driven fee defaults for block {}", block);
                self.get_config_default_fee_data(block)
            }
        }
    }

    fn get_config_default_fee_data(&self, block: u64) -> Result<FeeData, GasError> {
        if self.strict {
            return Err(GasError::DataSource(format!(
                "Strict mode: config default fee data requested for block {}", block
            )));
        }
        // Get default gas price from config
        let default_gas_price = self.get_config_default_gas_price()?;

        // Calculate fee structure based on gas price
        let max_fee_per_gas = default_gas_price.base_fee
            .saturating_add(default_gas_price.priority_fee)
            .saturating_mul(U256::from(110))
            .checked_div(U256::from(100))
            .unwrap_or_else(|| default_gas_price.base_fee.saturating_add(default_gas_price.priority_fee));

        Ok(FeeData {
            base_fee_per_gas: Some(default_gas_price.base_fee),
            max_fee_per_gas: Some(max_fee_per_gas),
            max_priority_fee_per_gas: Some(default_gas_price.priority_fee),
            gas_price: Some(default_gas_price.effective_price()),
            block_number: block,
            chain_name: self.chain_name.clone(),
        })
    }
}

#[async_trait]
impl GasOracleProvider for HistoricalGasProvider {
    async fn get_gas_price(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        if chain_name != self.chain_name {
            return Err(GasError::DataSource(format!(
                "Chain mismatch: requested '{}' but provider configured for '{}'",
                chain_name, self.chain_name
            )));
        }
        if let Some(requested_block) = block_number {
            let diff = requested_block.abs_diff(self.block_number);
            let tol = self.cfg.block_distance_tolerance.unwrap_or(10);
            if diff > tol {
                return Err(GasError::InvalidRequest(format!(
                    "Pinned at block {}, asked for {} (diff {} > {})",
                    self.block_number, requested_block, diff, tol
                )));
            }
        }
        self.price_at().await
    }

    async fn get_fee_data(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<FeeData, GasError> {
        if chain_name != self.chain_name {
            return Err(GasError::DataSource(format!(
                "Chain mismatch: requested '{}' but provider configured for '{}'",
                chain_name, self.chain_name
            )));
        }
        if let Some(requested_block) = block_number {
            // In backtest mode, allow requests for blocks within a reasonable range
            // This prevents the strict pinning that causes pathfinding failures
            let diff = requested_block.abs_diff(self.block_number);
            let tol = self.cfg.block_distance_tolerance.unwrap_or(10);
            if diff > tol {
                return Err(GasError::InvalidRequest(format!(
                    "Pinned at block {}, asked for {} (diff {} > {})",
                    self.block_number, requested_block, diff, tol
                )));
            }
        }
        self.fee_at().await
    }

    fn get_chain_name_from_id(&self, chain_id: u64) -> Option<String> {
        // Search through all configured chains to find matching chain ID
        for (chain_name, chain_config) in &self.chain_config.chains {
            if chain_config.chain_id == chain_id {
                return Some(chain_name.clone());
            }
        }
        None
    }

    fn estimate_protocol_gas(&self, proto: &ProtocolType) -> u64 {
        let key = format!("{proto:?}");
        self.cfg
            .protocol_gas_estimates
            .get(&key)
            .copied()
            .unwrap_or(self.cfg.default_gas_estimate)
    }

    async fn get_gas_price_with_block(&self, chain_name: &str, block_number: Option<u64>) -> Result<GasPrice, GasError> {
        self.get_gas_price(chain_name, block_number).await
    }

    async fn estimate_gas_cost_native(&self, chain_name: &str, gas_units: u64) -> Result<f64, GasError> {
        if chain_name != self.chain_name {
            return Err(GasError::DataSource(format!(
                "Chain mismatch: requested '{}' but provider configured for '{}'",
                chain_name, self.chain_name
            )));
        }
        let gas_price = self.get_gas_price(chain_name, Some(self.block_number)).await?;
        let effective_price = gas_price.effective_price();
        let gas_cost_wei = effective_price.saturating_mul(U256::from(gas_units));
        
        // Safe conversion with overflow protection
        let gas_cost_eth = if gas_cost_wei > U256::from(u128::MAX) {
            // If cost exceeds u128::MAX wei, convert via string formatting
            let cost_str = ethers::utils::format_units(gas_cost_wei, "ether")
                .map_err(|e| GasError::Other(format!("Failed to format gas cost: {}", e)))?;
            cost_str.parse::<f64>()
                .map_err(|e| GasError::Other(format!("Failed to parse gas cost: {}", e)))?
        } else {
            gas_cost_wei.as_u128() as f64 / 1e18
        };
        Ok(gas_cost_eth)
    }


}
