// src/bridge_health.rs

//! # Bridge Health Oracle
//!
//! This module provides an abstraction for querying the real-time health and
//! status of cross-chain bridges. A healthy bridge is critical for the successful
//! execution of a cross-chain arbitrage strategy.

use crate::blockchain::BlockchainManager;
use crate::config::Config;
use crate::errors::CrossChainError;
use crate::price_oracle::PriceOracle;
use async_trait::async_trait;
use ethers::prelude::*;
use moka::future::Cache;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

// A hypothetical, simplified ABI for a generic bridge contract.
// A real implementation would use specific ABIs for each supported bridge protocol.
abigen!(
    IBridge,
    r#"[
        function getPendingMessageCount(uint64 destChainId) external view returns (uint256)
        function getLiquidity(address token) external view returns (uint256)
        function estimateBridgingTime(uint64 destChainId) external view returns (uint256)
    ]"#
);

/// Represents key health metrics for a bridge protocol between two chains.
#[derive(Debug, Clone)]
pub struct BridgeHealthStats {
    /// Average time in seconds for a transaction to be confirmed on the destination chain.
    pub avg_bridging_time_secs: u64,
    /// Number of transactions currently pending or stuck in the bridge.
    pub pending_tx_count: u64,
    /// A normalized score (0.0 to 1.0) representing the liquidity depth of the bridge pools.
    /// Higher is better.
    pub liquidity_score: f64,
    /// A normalized score (0.0 to 1.0) representing the overall health.
    /// Higher is better.
    pub health_score: f64,
}

/// A trait for querying bridge health status.
#[async_trait]
pub trait BridgeHealthOracle: Send + Sync {
    /// Fetches the health statistics for a specific bridge protocol.
    async fn get_bridge_health(
        &self,
        source_chain: &str,
        dest_chain: &str,
        bridge_protocol: &str,
    ) -> Result<BridgeHealthStats, CrossChainError>;
}

/// A production-grade implementation of the `BridgeHealthOracle`.
/// It connects to bridge contracts to fetch live health metrics and caches them.
pub struct DefaultBridgeHealthOracle {
    config: Arc<Config>,
    blockchain_managers: Arc<HashMap<String, Arc<dyn BlockchainManager>>>,
    price_oracle: Arc<dyn PriceOracle>,
    health_cache: Cache<(String, String, String), BridgeHealthStats>,
}

impl DefaultBridgeHealthOracle {
    /// Creates a new `DefaultBridgeHealthOracle`.
    pub fn new(
        config: Arc<Config>,
        blockchain_managers: Arc<HashMap<String, Arc<dyn BlockchainManager>>>,
        price_oracle: Arc<dyn PriceOracle>,
    ) -> Self {
        Self {
            config,
            blockchain_managers,
            price_oracle,
            health_cache: Cache::builder()
                .time_to_live(Duration::from_secs(60)) // Cache health for 1 minute
                .max_capacity(1000)
                .build(),
        }
    }

    /// Fetches live on-chain data to calculate the health of a bridge.
    #[instrument(skip(self), fields(source=%source_chain, dest=%dest_chain, protocol=%bridge_protocol))]
    async fn fetch_live_health_stats(
        &self,
        source_chain: &str,
        dest_chain: &str,
        bridge_protocol: &str,
    ) -> Result<BridgeHealthStats, CrossChainError> {
        let source_manager = self.blockchain_managers.get(source_chain).ok_or_else(|| {
            CrossChainError::Configuration(format!("No BlockchainManager for source chain: {}", source_chain))
        })?;
        let dest_manager = self.blockchain_managers.get(dest_chain).ok_or_else(|| {
            CrossChainError::Configuration(format!("No BlockchainManager for dest chain: {}", dest_chain))
        })?;

        let source_bridge_address = self.get_bridge_address(source_chain, bridge_protocol)?;
        let dest_bridge_address = self.get_bridge_address(dest_chain, bridge_protocol)?;

        let source_contract = IBridge::new(source_bridge_address, source_manager.get_provider());
        let dest_contract = IBridge::new(dest_bridge_address, dest_manager.get_provider());
        let dest_chain_id = dest_manager.get_chain_id();

        // --- Fetch Metrics Concurrently ---
        let pending_tx_call = source_contract.get_pending_message_count(dest_chain_id);
        let bridging_time_call = source_contract.estimate_bridging_time(dest_chain_id);
        
        let pending_tx_future = pending_tx_call.call();
        let bridging_time_future = bridging_time_call.call();
        let source_liquidity_future = self.get_total_liquidity_usd(&source_contract, source_chain);
        let dest_liquidity_future = self.get_total_liquidity_usd(&dest_contract, dest_chain);

        let (
            pending_tx_result,
            bridging_time_result,
            source_liquidity_result,
            dest_liquidity_result,
        ) = tokio::join!(
            pending_tx_future,
            bridging_time_future,
            source_liquidity_future,
            dest_liquidity_future
        );

        let pending_tx_count = pending_tx_result.map_or(100, |c| c.as_u64()); // Default high on error
        let avg_bridging_time_secs = bridging_time_result.map_or(1800, |t| t.as_u64()); // Default high on error
        let source_liquidity_usd = source_liquidity_result.unwrap_or(0.0);
        let dest_liquidity_usd = dest_liquidity_result.unwrap_or(0.0);
        let total_liquidity_usd = source_liquidity_usd + dest_liquidity_usd;

        // --- Calculate Normalized Scores (0.0 = bad, 1.0 = good) ---
        let time_score = 1.0 - (avg_bridging_time_secs as f64 / 3600.0).min(1.0); // Penalize anything over 1 hour
        let pending_score = 1.0 - (pending_tx_count as f64 / 200.0).min(1.0); // Penalize > 200 pending txs
        let liquidity_score = (total_liquidity_usd / 5_000_000.0).min(1.0); // Normalize against $5M TVL

        // --- Weighted Composite Health Score ---
        let health_score = (0.5 * liquidity_score) + (0.3 * time_score) + (0.2 * pending_score);

        debug!(
            pending_tx_count,
            avg_bridging_time_secs,
            total_liquidity_usd,
            health_score,
            "Calculated bridge health"
        );

        Ok(BridgeHealthStats {
            avg_bridging_time_secs,
            pending_tx_count,
            liquidity_score,
            health_score,
        })
    }

    /// Helper to get the total USD liquidity of a bridge's token pools.
    async fn get_total_liquidity_usd<M: Middleware>(
        &self,
        contract: &IBridge<M>,
        chain_name: &str,
    ) -> Result<f64, CrossChainError> {
        let stablecoins = self
            .config
            .get_chain_config(chain_name)
            .ok()
            .and_then(|c| c.reference_stablecoins.clone())
            .unwrap_or_default();

        let mut total_usd: f64 = 0.0;
        for token in stablecoins {
            if let Ok(liquidity_raw) = contract.get_liquidity(token).call().await {
                let price = self
                    .price_oracle
                    .get_token_price_usd(chain_name, token)
                    .await
                    .unwrap_or(1.0);
                let decimals = self.blockchain_managers.get(chain_name).unwrap().get_token_decimals(token, None).await.unwrap_or(18);
                let liquidity_normalized = crate::types::normalize_units(liquidity_raw, decimals);
                total_usd += liquidity_normalized * price;
            }
        }
        Ok(total_usd)
    }

    /// Fetches a bridge address from the loaded configuration.
    fn get_bridge_address(
        &self,
        chain_name: &str,
        bridge_protocol: &str,
    ) -> Result<Address, CrossChainError> {
        self.config
            .get_chain_config(chain_name)
            .map_err(|e| CrossChainError::Configuration(e.to_string()))?
            .bridges
            .as_ref()
            .and_then(|b| b.get(bridge_protocol))
            .copied()
            .ok_or_else(|| {
                CrossChainError::Configuration(format!(
                    "Bridge address for protocol '{}' not found on chain '{}'",
                    bridge_protocol, chain_name
                ))
            })
    }
}

#[async_trait]
impl BridgeHealthOracle for DefaultBridgeHealthOracle {
    async fn get_bridge_health(
        &self,
        source_chain: &str,
        dest_chain: &str,
        bridge_protocol: &str,
    ) -> Result<BridgeHealthStats, CrossChainError> {
        let cache_key = (
            source_chain.to_string(),
            dest_chain.to_string(),
            bridge_protocol.to_string(),
        );

        if let Some(stats) = self.health_cache.get(&cache_key).await {
            return Ok(stats);
        }

        let stats = self
            .fetch_live_health_stats(source_chain, dest_chain, bridge_protocol)
            .await?;

        self.health_cache.insert(cache_key, stats.clone()).await;

        Ok(stats)
    }
} 