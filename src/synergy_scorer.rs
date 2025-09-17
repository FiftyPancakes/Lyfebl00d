// src/synergy_scorer.rs

//! # Cross-Chain Synergy Scorer
//!
//! This module provides a data-driven model for scoring the viability of
//! cross-chain arbitrage between two blockchains at a given moment. It analyzes
//! factors like gas price correlation, volatility spillover, and mempool
//! activity to produce a score that guides the `CrossChainEngine`.

use crate::{
    errors::CrossChainError,
    gas_oracle::{GasOracleProvider},
    price_oracle::PriceOracle,
};
use async_trait::async_trait;
use ethers::types::Address;
use std::sync::Arc;

#[async_trait]
pub trait SynergyScorer: Send + Sync + std::fmt::Debug {
    /// Scores the synergy between two chains for a specific token.
    /// Returns a score between 0.0 and 1.0.
    async fn score(
        &self,
        source_chain: &str,
        dest_chain: &str,
        token: Address,
    ) -> Result<f64, CrossChainError>;
}

pub struct DefaultSynergyScorer {
    gas_oracle: Arc<dyn GasOracleProvider>,
    price_oracle: Arc<dyn PriceOracle>,
}

impl std::fmt::Debug for DefaultSynergyScorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultSynergyScorer")
            .field("gas_oracle", &"Arc<dyn GasOracleProvider>")
            .field("price_oracle", &"Arc<dyn PriceOracle>")
            .finish()
    }
}

impl DefaultSynergyScorer {
    pub fn new(
        gas_oracle: Arc<dyn GasOracleProvider>,
        price_oracle: Arc<dyn PriceOracle>,
    ) -> Self {
        Self {
            gas_oracle,
            price_oracle,
        }
    }
}

#[async_trait]
impl SynergyScorer for DefaultSynergyScorer {
    async fn score(
        &self,
        source_chain: &str,
        dest_chain: &str,
        token: Address,
    ) -> Result<f64, CrossChainError> {
        // 1. Get gas price levels for both chains
        let gas_source = self
            .gas_oracle
            .get_gas_price(source_chain, None)
            .await
            .map_err(|e| CrossChainError::DependencyError(format!("Gas oracle failed for source chain: {}", e)))?;
        let gas_dest = self
            .gas_oracle
            .get_gas_price(dest_chain, None)
            .await
            .map_err(|e| CrossChainError::DependencyError(format!("Gas oracle failed for dest chain: {}", e)))?;

        // 2. Get volatility levels for the token on both chains
        let vol_source = self
            .price_oracle
            .get_token_volatility(source_chain, token, 3600) // 1-hour volatility
            .await
            .map_err(|e| CrossChainError::DependencyError(format!("Price oracle (vol) failed for source chain: {}", e)))?;
        let vol_dest = self
            .price_oracle
            .get_token_volatility(dest_chain, token, 3600) // 1-hour volatility
            .await
            .map_err(|e| CrossChainError::DependencyError(format!("Price oracle (vol) failed for dest chain: {}", e)))?;

        // 3. Simple scoring model:
        //    - High synergy if gas is low on both chains (cheap to execute).
        //    - High synergy if volatility is high on source and low on destination
        //      (price dislocation is likely to persist long enough to bridge).
        let gas_score =
            1.0 - (gas_source.normalized_level() + gas_dest.normalized_level()) / 2.0;
        let vol_score = vol_source * (1.0 - vol_dest);

        let final_score = (0.6 * gas_score + 0.4 * vol_score).max(0.0).min(1.0);

        Ok(final_score)
    }
}
