// src/price_oracle/mod.rs

use async_trait::async_trait;
use ethers::types::{Address, U256};
use std::{collections::HashMap};
use crate::errors::PriceError;
use crate::types::PoolInfo;

pub mod historical_price_provider;
pub mod live_price_provider;
pub mod types;
pub mod aggregator_quote_wrapper;

/// The PriceOracle trait defines the interface for all price oracles (live or historical).
/// It is designed for strict delegation, dependency injection, and mode-awareness.
/// All implementations must be thread-safe and stateless where possible.
#[async_trait]
pub trait PriceOracle: Send + Sync + std::fmt::Debug {
    /// Get token price in USD for the current context (live or block-specific).
    async fn get_token_price_usd(&self, chain_name: &str, token: Address) -> Result<f64, PriceError>;

    /// Get token price in USD at a specific block and/or timestamp.
    async fn get_token_price_usd_at(
        &self,
        chain_name: &str,
        token: Address,
        block_number: Option<u64>,
        unix_ts: Option<u64>,
    ) -> Result<f64, PriceError>;

    /// Get the price ratio between two tokens (token_a / token_b).
    async fn get_pair_price_ratio(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError>;

    /// Get a batch of historical prices for a set of tokens at a specific block.
    async fn get_historical_prices_batch(
        &self,
        chain_name: &str,
        tokens: &[Address],
        block_number: u64,
    ) -> Result<HashMap<Address, f64>, PriceError>;

    /// Get the price of a token (alias for get_token_price_usd for backward compatibility).
    async fn get_price(
        &self,
        chain_name: &str,
        token: Address,
    ) -> Result<f64, PriceError> {
        self.get_token_price_usd(chain_name, token).await
    }

    /// Estimate the value of a swap in USD.
    async fn estimate_swap_value_usd(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Result<f64, PriceError>;

    /// Calculate token volatility over a given timeframe (in seconds).
    async fn get_token_volatility(
        &self,
        chain_name: &str,
        token: Address,
        timeframe_seconds: u64,
    ) -> Result<f64, PriceError>;

    /// Calculate the USD liquidity value of a pool using this oracle's prices.
    async fn calculate_liquidity_usd(
        &self,
        pool_info: &PoolInfo,
    ) -> Result<f64, PriceError>;

    /// Get pair price at specific block
    async fn get_pair_price_at_block(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        block_number: u64,
    ) -> Result<f64, PriceError>;

    /// Get pair price for current context
    async fn get_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError>;

    /// Convert a token amount to USD value
    async fn convert_token_to_usd(
        &self,
        chain_name: &str,
        amount: ethers::types::U256,
        token: Address,
    ) -> Result<f64, PriceError>;

    /// Convert a native currency amount to USD value
    async fn convert_native_to_usd(
        &self,
        chain_name: &str,
        amount_native: ethers::types::U256,
    ) -> Result<f64, PriceError>;

    /// Estimate token conversion between two tokens
    async fn estimate_token_conversion(
        &self,
        chain_name: &str,
        amount_in: ethers::types::U256,
        token_in: Address,
        token_out: Address,
    ) -> Result<ethers::types::U256, PriceError>;

    /// Reset circuit breaker (administrative override)
    async fn reset_circuit_breaker(&self) {
        // Default implementation does nothing
    }

    /// Check if circuit breaker is tripped
    async fn is_circuit_breaker_tripped(&self) -> bool {
        false // Default implementation assumes no circuit breaker
    }
}