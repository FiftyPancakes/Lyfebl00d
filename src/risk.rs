// src/risk.rs

//! # Advanced Risk Management Engine
//!
//! This module provides a sophisticated, data-driven risk management system for the
//! arbitrage bot. It assesses the viability of potential trades by analyzing multiple
//! risk vectors in the context of the current market conditions.

use crate::{
    blockchain::BlockchainManager,
    config::{RiskScoreWeights, RiskSettings},
    dex_math,
    errors::{BlockchainError, DexMathError, RiskError},
    gas_oracle::GasOracleProvider,
    price_oracle::PriceOracle,
    types::{FeeData, ExecutionPriority},
    path::Path as TradePath,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ethers::types::{Address, U256};
use moka::future::Cache;
use smallvec::smallvec;
use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tracing::{debug, instrument, warn};
use crate::precision::PreciseDecimal;

//================================================================================================//
//                                         CONSTANTS                                             //
//================================================================================================//

const RISK_ASSESSMENT_CACHE_SIZE: u64 = 10_000;
const RISK_ASSESSMENT_CACHE_TTL: Duration = Duration::from_secs(10);

//================================================================================================//
//                                      CORE DATA STRUCTURES                                      //
//================================================================================================//

#[derive(Debug, Clone)]
pub struct RiskAssessment {
    pub total_risk_score: f64, // Normalized 0.0 (low risk) to 1.0 (high risk)
    pub volatility_score: f64,
    pub liquidity_score: f64,
    pub slippage_score: f64,
    pub mev_risk_score: f64,
    pub gas_risk_score: f64,
    pub is_safe: bool,
    pub mitigation: Option<String>,
    pub timestamp: DateTime<Utc>,
}

//================================================================================================//
//                                       TRAIT DEFINITION                                         //
//================================================================================================//

#[async_trait]
pub trait RiskManager: Send + Sync {
    /// Assesses the overall risk of executing a specific trade.
    ///
    /// # Arguments
    /// * `token_in` - The address of the input token.
    /// * `token_out` - The address of the output token.
    /// * `amount_in` - The amount of the input token for the trade.
    /// * `block_number` - `Some(number)` for backtesting at a specific block, `None` for live mode.
    ///
    /// # Returns
    /// A `Result` containing the `RiskAssessment` or a `RiskError`.
    async fn assess_trade(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError>;

    /// Assesses the overall risk of executing a specific trade path.
    ///
    /// # Arguments
    /// * `path` - The trade path to assess.
    /// * `block_number` - `Some(number)` for backtesting at a specific block, `None` for live mode.
    ///
    /// # Returns
    /// A `Result` containing the `RiskAssessment` or a `RiskError`.
    async fn assess_path(
        &self,
        path: &TradePath,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError>;
    
    /// Assesses the risk of executing an arbitrage opportunity.
    async fn assess_arbitrage_risk(
        &self,
        opportunity: &crate::mempool::MEVOpportunity,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError>;
    
    /// Assesses the risk of executing a liquidation opportunity.
    async fn assess_liquidation(
        &self,
        opportunity: &crate::mempool::PotentialLiquidationOpportunity,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError>;
}

//================================================================================================//
//                                     RISK MANAGER IMPL                                          //
//================================================================================================//

pub struct RiskManagerImpl {
    config: Arc<RiskSettings>,
    blockchain: Arc<dyn BlockchainManager>,
    pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait>,
    gas_oracle: Arc<dyn GasOracleProvider>,
    price_oracle: Arc<dyn PriceOracle>,
    assessments_cache: Cache<(Vec<Address>, Vec<Address>, Vec<u32>, Vec<crate::types::ProtocolType>, U256, String, Option<u64>), RiskAssessment>,
}

impl RiskManagerImpl {
    pub fn new(
        config: Arc<RiskSettings>,
        blockchain: Arc<dyn BlockchainManager>,
        pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait>,
        gas_oracle: Arc<dyn GasOracleProvider>,
        price_oracle: Arc<dyn PriceOracle>,
    ) -> Self {
        Self {
            config,
            blockchain,
            pool_manager,
            gas_oracle,
            price_oracle,
            assessments_cache: Cache::builder()
                .max_capacity(RISK_ASSESSMENT_CACHE_SIZE)
                .time_to_live(RISK_ASSESSMENT_CACHE_TTL)
                .build(),
        }
    }

    /// Calculates the expected output amount for a given input amount and pool.
    ///
    /// # Arguments
    /// * `chain_name` - The name of the blockchain chain.
    /// * `token_in` - The address of the input token.
    /// * `token_out` - The address of the output token.
    /// * `amount_in` - The amount of the input token.
    /// * `pool_address` - The address of the pool to use for the calculation.
    /// * `block_number` - `Some(number)` for backtesting at a specific block, `None` for live mode.
    ///
    /// # Returns
    /// A `Result` containing the expected output amount or a `RiskError`.
    async fn get_expected_output(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        pool_address: Address,
        block_number: Option<u64>,
    ) -> Result<U256, RiskError> {
        if amount_in.is_zero() {
            return Ok(U256::zero());
        }

        let pool_info = self
            .pool_manager
            .get_pool_info(chain_name, pool_address, block_number)
            .await
            .map_err(|e| RiskError::PoolManager(e.to_string()))?;

        // Validate that both tokens are in the pool
        if pool_info.token0 != token_in && pool_info.token1 != token_in {
            return Err(RiskError::InvalidInput(format!(
                "Token {} not found in pool {}",
                token_in, pool_address
            )));
        }

        if pool_info.token0 != token_out && pool_info.token1 != token_out {
            return Err(RiskError::InvalidInput(format!(
                "Token {} not found in pool {}",
                token_out, pool_address
            )));
        }

        // Validate that input and output tokens are different
        if token_in == token_out {
            return Err(RiskError::InvalidInput(format!(
                "Input and output tokens are the same: {}",
                token_in
            )));
        }

        // Use dex_math to calculate the expected output
        let expected_output = dex_math::get_amount_out(&pool_info, token_in, amount_in)
            .map_err(|e| RiskError::DexMath(e))?;

        debug!(
            chain_name = %chain_name,
            token_in = %token_in,
            token_out = %token_out,
            amount_in = %amount_in,
            pool_address = %pool_address,
            expected_output = %expected_output,
            block_number = ?block_number,
            "Calculated expected output using dex_math"
        );

        Ok(expected_output)
    }

    /// Calculates a score from 0.0 to 1.0 representing the volatility risk of a token path.
    ///
    /// If block_number is provided, attempts to use historical volatility data for each token in the path.
    /// Otherwise, uses live volatility data, which may introduce lookahead bias in backtesting.
    /// The score is based on the maximum volatility observed among all unique tokens in the path,
    /// normalized by the configured maximum volatility threshold.
    #[instrument(skip(self, path))]
    async fn get_volatility_score_path(
        &self,
        path: &TradePath,
        block_number: Option<u64>,
    ) -> Result<f64, RiskError> {
        use std::collections::HashSet;

        let chain_name = self.blockchain.get_chain_name();
        let mut max_volatility: f64 = 0.0;
        let mut tokens: HashSet<ethers::types::Address> = HashSet::new();

        for token in path.token_path.iter() {
            if !tokens.insert(*token) {
                continue;
            }

            let volatility = self.get_volatility_with_fallback(&chain_name, *token, 24 * 60 * 60, block_number).await?;

            if volatility.is_finite() && volatility > 0.0 {
                max_volatility = f64::max(max_volatility, volatility);
            } else {
                warn!(
                    token = ?token,
                    volatility,
                    "Non-finite or zero volatility returned for token"
                );
            }
        }

        if max_volatility == 0.0 {
            warn!("No valid volatility data for any token in path, assuming moderate risk.");
            return Ok(0.5);
        }

        let score = (max_volatility / self.config.max_volatility).min(1.0);
        debug!(
            max_volatility,
            score,
            "Calculated path volatility score"
        );
        Ok(score)
    }

    /// Calculates a score from 0.0 to 1.0 representing the liquidity risk for a trade.
    #[instrument(skip(self, path))]
    async fn get_liquidity_score_path(
        &self,
        path: &TradePath,
        block_number: Option<u64>,
    ) -> Result<f64, RiskError> {
        let chain_name = self.blockchain.get_chain_name();
        let mut worst_score: f64 = 0.0;
        let mut current_amount_in = path.initial_amount_in_wei;

        for (i, token) in path.token_path.iter().enumerate() {
            if i == path.token_path.len() - 1 {
                break;
            }

            let token_price_usd = if let Some(bn) = block_number {
                self.price_oracle
                    .get_token_price_usd_at(&chain_name, *token, Some(bn), None)
                    .await?
            } else {
                self.price_oracle
                    .get_token_price_usd(&chain_name, *token)
                    .await?
            };

            let decimals = if let Ok(pool_info) = self.pool_manager.get_pool_info(&chain_name, path.pool_path[i], block_number).await {
                if pool_info.token0 == *token {
                    pool_info.token0_decimals
                } else {
                    pool_info.token1_decimals
                }
            } else {
                match self
                .blockchain
                .get_token_decimals(*token, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into()))))
                .await {
                Ok(decimals) => decimals,
                Err(e) => {
                    warn!(
                        token = ?token,
                        error = ?e,
                        "Failed to get token decimals, using default value of 18"
                    );
                    18
                }
            }
            };

            let trade_size_usd = crate::types::normalize_units(current_amount_in, decimals) * token_price_usd;

            if trade_size_usd > self.config.max_exposure_usd {
                warn!(trade_size_usd, max_exposure_usd = self.config.max_exposure_usd, "Trade exceeds max exposure");
                return Ok(1.0);
            }

            let total_liquidity_units = self
                .pool_manager
                .get_total_liquidity_for_token_units(&chain_name, *token, block_number)
                .await?;
            
            let total_liquidity_usd = total_liquidity_units * token_price_usd;

            if total_liquidity_usd < self.config.min_liquidity_usd {
                warn!(total_liquidity_usd, min_liquidity_usd = self.config.min_liquidity_usd, "Token liquidity is below minimum threshold");
                return Ok(1.0);
            }

            let score = if total_liquidity_usd > 0.0 {
                (trade_size_usd / total_liquidity_usd).min(1.0)
            } else {
                1.0
            };

            worst_score = f64::max(worst_score, score);

            // Calculate the output of this hop to use as the input for the next
            if i < path.token_path.len() - 2 {
                let pool_address = path.pool_path[i];
                let pool_info = self
                    .pool_manager
                    .get_pool_info(&chain_name, pool_address, block_number)
                    .await?;

                match dex_math::get_amount_out(&pool_info, *token, current_amount_in) {
                    Ok(amount_out) => {
                        current_amount_in = amount_out;
                    }
                    Err(DexMathError::InsufficientLiquidity) => {
                        warn!(token_in = ?token, amount_in = ?current_amount_in, pool = ?pool_info.address, "Insufficient liquidity for trade, max liquidity risk");
                        return Ok(1.0);
                    }
                    Err(e) => return Err(RiskError::DexMath(e)),
                }
            }
        }

        debug!(worst_score, "Calculated path liquidity score");
        Ok(worst_score)
    }

    /// Calculates a score from 0.0 to 1.0 representing the slippage risk.
    #[instrument(skip(self, path))]
    async fn get_slippage_score_path(
        &self,
        path: &TradePath,
        block_number: Option<u64>,
    ) -> Result<f64, RiskError> {
        let chain_name = self.blockchain.get_chain_name();
        let mut max_price_impact_bps = 0.0;

        // carry forward the per-hop amount
        let mut current_in = path.initial_amount_in_wei;

        for (i, pool) in path.pool_path.iter().enumerate() {
            let token_in = path.token_path[i];
            let pool_info = self
                .pool_manager
                .get_pool_info(&chain_name, *pool, block_number)
                .await?;

            let amount_out = match dex_math::get_amount_out(&pool_info, token_in, current_in) {
                Ok(amount) => amount,
                Err(DexMathError::InsufficientLiquidity) => {
                    warn!(token_in = ?token_in, amount_in = ?current_in, pool = ?pool_info.address, "Insufficient liquidity for trade, max slippage risk");
                    return Ok(1.0);
                }
                Err(e) => return Err(RiskError::DexMath(e)),
            };

            let price_impact_bps = match dex_math::calculate_price_impact(&pool_info, token_in, current_in, amount_out) {
                Ok(impact) => impact,
                Err(DexMathError::InsufficientLiquidity) => {
                    warn!(token_in = ?token_in, amount_in = ?current_in, pool = ?pool_info.address, "Insufficient liquidity for trade, max slippage risk");
                    return Ok(1.0);
                }
                Err(e) => return Err(RiskError::DexMath(e)),
            };

            let price_impact_f64 = price_impact_bps.min(U256::from(u64::MAX)).as_u64() as f64;

            // Track the maximum price impact instead of averaging
            max_price_impact_bps = f64::max(max_price_impact_bps, price_impact_f64);

            // next hop input
            current_in = amount_out;
        }

        let score = (max_price_impact_bps / self.config.max_slippage_bps as f64).min(1.0);
        debug!(max_price_impact_bps, score, "Calculated path slippage score using maximum price impact");
        Ok(score)
    }

    /// Calculates a score from 0.0 to 1.0 representing the risk of gas price volatility.
    #[instrument(skip(self, path))]
    async fn get_gas_risk_path(&self, path: &TradePath, block_number: Option<u64>) -> Result<f64, RiskError> {
        let chain_name = self.blockchain.get_chain_name();
        let gas_price_stats: FeeData = self
            .gas_oracle
            .get_fee_data(&chain_name, block_number)
            .await?;
        let base_fee = gas_price_stats.base_fee_per_gas.unwrap_or(U256::zero());
        let priority_fee = gas_price_stats.max_priority_fee_per_gas.unwrap_or(U256::zero());
        let legacy_gas = gas_price_stats.gas_price.unwrap_or(U256::zero());
        let effective_gas = if base_fee > U256::zero() || priority_fee > U256::zero() {
            base_fee.saturating_add(priority_fee)
        } else {
            legacy_gas
        };
        let effective_gas_gwei = Self::u256_to_f64(effective_gas) / 1_000_000_000.0;
        let min_gas_gwei = self.config.min_gas_gwei;
        let max_gas_gwei = self.config.max_gas_gwei;
        
        let span = (max_gas_gwei - min_gas_gwei).max(1e-9);
        let base_score = if effective_gas_gwei <= min_gas_gwei {
            0.0
        } else if effective_gas_gwei >= max_gas_gwei {
            1.0
        } else {
            (effective_gas_gwei - min_gas_gwei) / span
        };
        
        let path_complexity_multiplier = if path.token_path.len() > 2 {
            let hop_count = path.token_path.len() - 1;
            let complexity_factor = (hop_count as f64).log2() / 3.0;
            (1.0 + complexity_factor).min(2.0)
        } else {
            1.0
        };
        
        let protocol_risk_multiplier = {
            let mut high_risk_protocols = 0;
            for protocol in path.protocol_path.iter() {
                match protocol {
                    crate::types::ProtocolType::UniswapV2 => high_risk_protocols += 1,
                    crate::types::ProtocolType::Curve => high_risk_protocols += 2,
                    crate::types::ProtocolType::Balancer => high_risk_protocols += 1,
                    _ => {}
                }
            }
            if !path.protocol_path.is_empty() {
                let risk_ratio = high_risk_protocols as f64 / path.protocol_path.len() as f64;
                1.0 + (risk_ratio * 0.5)
            } else {
                1.0
            }
        };
        
        let final_score = (base_score * path_complexity_multiplier * protocol_risk_multiplier).min(1.0);
        
        debug!(
            effective_gas_gwei, 
            base_score, 
            path_complexity_multiplier, 
            protocol_risk_multiplier, 
            final_score, 
            "Calculated gas risk score"
        );
        Ok(final_score)
    }

    /// Calculates a score from 0.0 to 1.0 representing the MEV risk.
    ///
    /// TODO: Future enhancement - incorporate historical data to track token/pool
    /// success rates for more data-driven MEV risk assessment. Some tokens are
    /// inherently more "toxic" due to frequent sandwich attacks, regardless of
    /// profit size.
    #[instrument(skip(self, path))]
    async fn get_mev_risk_score_path(
        &self,
        path: &TradePath,
        profit_usd: f64,
        gas_estimate: u64,
    ) -> f64 {
        let profit_score = (profit_usd / 1000.0).min(1.0); // Profits above $1k are high MEV risk
        let hop_penalty = 1.0 / (path.token_path.len() as f64).sqrt(); // More hops = lower risk
        let mut protocol_penalty = 0.0;
        let mut popular_protocols = 0;
        for protocol in path.protocol_path.iter() {
            match protocol {
                crate::types::ProtocolType::UniswapV2 | crate::types::ProtocolType::UniswapV3 | crate::types::ProtocolType::Curve | crate::types::ProtocolType::Balancer => {
                    popular_protocols += 1;
                }
                _ => {}
            }
        }
        if !path.protocol_path.is_empty() {
            protocol_penalty = (popular_protocols as f64) / (path.protocol_path.len() as f64);
        }
        let gas_score = if gas_estimate < 120_000 {
            1.0
        } else if gas_estimate > 500_000 {
            0.5
        } else {
            1.0 - ((gas_estimate as f64 - 120_000.0) / (500_000.0 - 120_000.0)) * 0.5
        };
        let score = 0.5 * profit_score + 0.2 * hop_penalty + 0.2 * protocol_penalty + 0.1 * gas_score;
        let final_score = score.min(1.0);
        debug!(final_score, profit_score, hop_penalty, protocol_penalty, gas_score, "Calculated MEV risk score");
        final_score
    }

    /// Dynamically calculates risk weights based on market conditions.
    async fn get_dynamic_weights(
        &self,
        volatility_score: f64,
        gas_risk_score: f64,
    ) -> RiskScoreWeights {
        let mut weights = self.config.risk_score_weights.clone();

        if volatility_score > 0.5 {
            weights.volatility *= 1.5;
            weights.slippage *= 1.2;
        }

        if gas_risk_score > 0.5 {
            weights.gas *= 1.5;
        }

        let total_weight =
            weights.volatility + weights.liquidity + weights.slippage + weights.mev + weights.gas;
        if total_weight > 0.0 {
            weights.volatility /= total_weight;
            weights.liquidity /= total_weight;
            weights.slippage /= total_weight;
            weights.mev /= total_weight;
            weights.gas /= total_weight;
        }

        debug!(?weights, "Calculated dynamic risk weights");
        weights
    }

    /// Gets token volatility with fallback from historical to live data
    async fn get_volatility_with_fallback(
        &self,
        chain: &str,
        token: Address,
        lookback_secs: u64,
        block_number: Option<u64>,
    ) -> Result<f64, RiskError> {
        // If block_number is provided, try to get historical volatility first
        if let Some(bn) = block_number {
            // Check if the price oracle has a historical volatility method
            // For now, fall back to live volatility with a warning
            // TODO: Implement historical volatility when oracle supports it
            warn!(
                token = ?token,
                block_number = ?bn,
                "Historical volatility not yet implemented, falling back to live volatility"
            );
        }

        // Get live volatility as fallback
        self.price_oracle
            .get_token_volatility(chain, token, lookback_secs)
            .await
            .map_err(|e| RiskError::PriceOracle(format!("Failed to get volatility: {}", e)))
    }

    /// Safely converts U256 to f64 to prevent truncation of large values
    fn u256_to_f64(u: U256) -> f64 {
        // Cheap path for small values that fit in u128
        if u <= U256::from(u128::MAX) {
            u.as_u128() as f64
        } else {
            // For very large values, use string parsing to avoid truncation
            u.to_string().parse::<f64>().unwrap_or(f64::INFINITY)
        }
    }
}

impl std::fmt::Debug for RiskManagerImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RiskManagerImpl")
            .field("config", &"Arc<RiskSettings>")
            .field("blockchain", &"Arc<dyn BlockchainManager>")
            .field("pool_manager", &"Arc<dyn PoolManagerTrait>")
            .field("gas_oracle", &"Arc<dyn GasOracleProvider>")
            .field("price_oracle", &"Arc<dyn PriceOracle>")
            .field("assessments_cache", &"Cache<(Vec<Address>, Vec<Address>, Vec<u32>, Vec<ProtocolType>, U256, String, Option<u64>), RiskAssessment>")
            .finish()
    }
}

#[async_trait]
impl RiskManager for RiskManagerImpl {
    #[instrument(skip(self), fields(token_in=%token_in, token_out=%token_out, amount_in=%amount_in, ?block_number))]
    async fn assess_trade(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError> {
        let chain_name = self.blockchain.get_chain_name();

        let pools = self
            .pool_manager
            .get_pools_for_pair(&chain_name, token_in, token_out)
            .await?;

        let best_pool_address = if pools.is_empty() {
            return Err(RiskError::NoPoolsAvailable);
        } else if pools.len() == 1 {
            pools[0]
        } else {
            // Pick the pool with the highest expected output
            let mut best: Option<(Address, U256)> = None;

            for pool in pools.iter().copied() {
                let pool_info = self
                    .pool_manager
                    .get_pool_info(&chain_name, pool, block_number)
                    .await?;

                match dex_math::get_amount_out(&pool_info, token_in, amount_in) {
                    Ok(output) => {
                        if output > U256::zero() {
                            if let Some((_, best_out)) = best {
                                if output > best_out {
                                    best = Some((pool, output));
                                }
                            } else {
                                best = Some((pool, output));
                            }
                        }
                    }
                    Err(DexMathError::InsufficientLiquidity) => {
                        // Skip pools with insufficient liquidity
                        continue;
                    }
                    Err(e) => {
                        warn!(pool = %pool, error = ?e, "Error calculating output for pool");
                        continue;
                    }
                }
            }

            match best {
                Some((pool, _)) => pool,
                None => return Err(RiskError::NoPoolsAvailable),
            }
        };

        let best_pool_info = self
            .pool_manager
            .get_pool_info(&chain_name, best_pool_address, block_number)
            .await?;

        let fee = if best_pool_info.fee_bps > 0 { best_pool_info.fee_bps } else { best_pool_info.fee.unwrap_or(3000) };
        let protocol_type = best_pool_info.protocol_type.clone();

        let path = TradePath {
            token_path: smallvec![token_in, token_out],
            pool_path: smallvec![best_pool_address],
            fee_path: smallvec![fee],
            protocol_path: smallvec![protocol_type],
            initial_amount_in_wei: amount_in,
            expected_output: U256::zero(),
            min_output: U256::zero(),
            price_impact_bps: 0,
            gas_estimate: 0,
            gas_cost_native: 0.0,
            profit_estimate_native: 0.0,
            profit_estimate_usd: 0.0,
            execution_priority: ExecutionPriority::Normal,
            tags: smallvec![],
            source: None,
            discovery_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            confidence_score: 0.0,
            mev_resistance_score: 0.0,
            token_in,
            token_out,
            legs: vec![],
            estimated_gas_cost_wei: U256::zero(),
        };

        let volatility_score = self.get_volatility_score_path(&path, block_number).await?;
        let liquidity_score = self.get_liquidity_score_path(&path, block_number).await?;
        let slippage_score = self.get_slippage_score_path(&path, block_number).await?;
        let gas_risk_score = self.get_gas_risk_path(&path, block_number).await?;

        let profit_estimate = if amount_in > U256::zero() {
            let block_id = block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into())));

            // Parallelize RPC calls for better performance
            let in_dec_result = self.blockchain.get_token_decimals(token_in, block_id).await?;
            let out_dec_result = self.blockchain.get_token_decimals(token_out, block_id).await?;
            let output_estimate_result = self.get_expected_output(&chain_name, token_in, token_out, amount_in, best_pool_address, block_number).await?;
            let price_in_result = self.price_oracle.get_token_price_usd_at(&chain_name, token_in, block_number, None).await
                .map_err(BlockchainError::from)?;
            let price_out_result = self.price_oracle.get_token_price_usd_at(&chain_name, token_out, block_number, None).await
                .map_err(BlockchainError::from)?;

            let in_dec = in_dec_result;
            let out_dec = out_dec_result;
            let output_estimate = output_estimate_result;
            let price_in = price_in_result;
            let price_out = price_out_result;

            let amount_in_f64 = PreciseDecimal::new(amount_in, in_dec.into()).to_f64();
            let output_f64 = PreciseDecimal::new(output_estimate, out_dec.into()).to_f64();
            let input_value_usd = amount_in_f64 * price_in;
            let output_value_usd = output_f64 * price_out;
            if output_value_usd > input_value_usd {
                output_value_usd - input_value_usd
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Calculate approximate gas estimate based on path complexity
        let approx_gas_units = 80_000u64 + 45_000u64 * (path.pool_path.len().saturating_sub(1) as u64);
        let mev_risk_score = self.get_mev_risk_score_path(&path, profit_estimate, approx_gas_units).await;

        let weights = self.get_dynamic_weights(volatility_score, gas_risk_score).await;
        let total_risk_score = (volatility_score * weights.volatility)
            + (liquidity_score * weights.liquidity)
            + (slippage_score * weights.slippage)
            + (mev_risk_score * weights.mev)
            + (gas_risk_score * weights.gas);

        let is_safe = total_risk_score <= self.config.max_risk_score;
        let mitigation = if !is_safe {
            Some("Risk score exceeds threshold. Consider reducing trade size or waiting for better market conditions.".to_string())
        } else {
            None
        };

        let assessment = RiskAssessment {
            total_risk_score,
            volatility_score,
            liquidity_score,
            slippage_score,
            mev_risk_score,
            gas_risk_score,
            is_safe,
            mitigation,
            timestamp: Utc::now(),
        };

        Ok(assessment)
    }

    #[instrument(skip(self, path))]
    async fn assess_path(
        &self,
        path: &TradePath,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError> {
        let chain_name = self.blockchain.get_chain_name();
        let cache_key = (
            path.token_path.clone().to_vec(),
            path.pool_path.clone().to_vec(),
            path.fee_path.clone().to_vec(),
            path.protocol_path.clone().to_vec(),
            path.initial_amount_in_wei,
            chain_name.to_string(),
            block_number
        );
        if let Some(cached) = self.assessments_cache.get(&cache_key).await {
            debug!("Returning cached risk assessment for path");
            return Ok(cached);
        }
        let volatility_score = self.get_volatility_score_path(path, block_number).await?;
        let liquidity_score = self.get_liquidity_score_path(path, block_number).await?;
        let slippage_score = self.get_slippage_score_path(path, block_number).await?;
        let gas_risk_score = self.get_gas_risk_path(path, block_number).await?;
        let mev_risk_score = self.get_mev_risk_score_path(
            path,
            {
                // Calculate profit locally since Path no longer stores profit fields
                let in_token = path.token_path.first().copied()
                    .ok_or_else(|| RiskError::InvalidPath("Empty token path".to_string()))?;
                let out_token = *path.token_path.last()
                    .ok_or_else(|| RiskError::InvalidPath("Empty token path".to_string()))?;

                let in_dec = self.blockchain.get_token_decimals(in_token, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into())))).await.unwrap_or(18);
                let out_dec = self.blockchain.get_token_decimals(out_token, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into())))).await.unwrap_or(18);

                let p_in = self.price_oracle.get_token_price_usd_at(&chain_name, in_token, block_number, None).await.unwrap_or(0.0);
                let p_out = self.price_oracle.get_token_price_usd_at(&chain_name, out_token, block_number, None).await.unwrap_or(0.0);

                let gross_profit_usd = (PreciseDecimal::new(path.expected_output, out_dec.into()).to_f64() * p_out)
                                     - (PreciseDecimal::new(path.initial_amount_in_wei, in_dec.into()).to_f64() * p_in);
                gross_profit_usd.max(0.0)
            },
            path.gas_estimate,
        ).await;
        let weights = self.get_dynamic_weights(volatility_score, gas_risk_score).await;
        let total_risk_score = (volatility_score * weights.volatility)
            + (liquidity_score * weights.liquidity)
            + (slippage_score * weights.slippage)
            + (mev_risk_score * weights.mev)
            + (gas_risk_score * weights.gas);
        let is_safe = total_risk_score <= self.config.max_risk_score;
        let mitigation = if !is_safe {
            Some("Risk score exceeds threshold. Consider reducing trade size or waiting for better market conditions.".to_string())
        } else {
            None
        };
        let assessment = RiskAssessment {
            total_risk_score,
            volatility_score,
            liquidity_score,
            slippage_score,
            mev_risk_score,
            gas_risk_score,
            is_safe,
            mitigation,
            timestamp: Utc::now(),
        };
        self.assessments_cache.insert(cache_key, assessment.clone()).await;
        Ok(assessment)
    }

    #[instrument(skip(self, opportunity))]
    async fn assess_arbitrage_risk(
        &self,
        opportunity: &crate::mempool::MEVOpportunity,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError> {
        let chain_name = self.blockchain.get_chain_name();
        match opportunity {
            crate::mempool::MEVOpportunity::Arbitrage(route) => {
                // Validate that the arbitrage route has legs
                if route.legs.is_empty() {
                    return Err(RiskError::InvalidPath("Arbitrage route has no legs".into()));
                }

                // Convert ArbitrageRoute to TradePath for risk assessment
                // compute gas units ≈ cost_wei / effective_gas_price_wei
                let fee = self.gas_oracle.get_fee_data(&chain_name, block_number).await?;
                let eff = fee.base_fee_per_gas.unwrap_or_default().saturating_add(fee.max_priority_fee_per_gas.unwrap_or_default());
                let gas_units = if !eff.is_zero() { 
                    route.estimated_gas_cost_wei.checked_div(eff).unwrap_or_default().as_u64() 
                } else { 
                    0 
                };

                let path = TradePath {
                    token_path: route.legs.iter().map(|leg| leg.token_in).chain(
                        route.legs.last().map(|leg| leg.token_out).into_iter()
                    ).collect(),
                    pool_path: route.legs.iter().map(|leg| leg.pool_address).collect(),
                    fee_path: route.legs.iter().map(|leg| leg.fee_bps).collect(),
                    protocol_path: route.legs.iter().map(|leg| leg.protocol_type.clone()).collect(),
                    initial_amount_in_wei: route.amount_in,
                    expected_output: route.amount_out_wei,
                    min_output: route.amount_out,
                    price_impact_bps: route.price_impact_bps,
                    gas_estimate: gas_units,
                    gas_cost_native: 0.0, // Calculate from gas estimate
                    profit_estimate_native: 0.0, // Calculate from USD profit
                    profit_estimate_usd: route.profit_usd,
                    execution_priority: ExecutionPriority::Normal,
                    tags: smallvec![],
                    source: None,
                    discovery_timestamp: (route.created_at_ns / 1_000_000_000), // Convert from ns to seconds
                    confidence_score: 0.8, // Default confidence
                    mev_resistance_score: route.mev_risk_score,
                    token_in: route.token_in,
                    token_out: route.token_out,
                    legs: route.legs.iter().map(|leg| crate::path::PathLeg {
                        protocol_type: leg.protocol_type.clone(),
                        pool_address: leg.pool_address,
                        from_token: leg.token_in,
                        to_token: leg.token_out,
                        amount_in: leg.amount_in,
                        amount_out: leg.expected_out.unwrap_or(leg.min_amount_out),
                        fee: leg.fee_bps,
                        requires_approval: false,
                        wrap_native: false,
                        unwrap_native: false,
                    }).collect(),
                    estimated_gas_cost_wei: route.estimated_gas_cost_wei,
                };

                // Perform standard path risk assessment
                let base_assessment = self.assess_path(&path, block_number).await?;

                // Add arbitrage-specific risk factors
                let arbitrage_complexity_multiplier = {
                    let leg_count = route.legs.len();
                    match leg_count {
                        1 => 1.0,
                        2..=3 => 1.2,
                        4..=5 => 1.5,
                        _ => 2.0,
                    }
                };

                let cross_protocol_risk = {
                    let unique_protocols: std::collections::HashSet<_> = 
                        route.legs.iter().map(|leg| &leg.protocol_type).collect();
                    if unique_protocols.len() > 1 {
                        1.3
                    } else {
                        1.0
                    }
                };

                let timing_risk = if route.profit_usd > 1000.0 {
                    1.4 // High-value arbs attract more competition
                } else if route.profit_usd > 100.0 {
                    1.2
                } else {
                    1.0
                };

                let adjusted_total_risk = (base_assessment.total_risk_score 
                    * arbitrage_complexity_multiplier 
                    * cross_protocol_risk 
                    * timing_risk).min(1.0);

                let adjusted_mev_risk = (base_assessment.mev_risk_score 
                    * timing_risk).min(1.0);

                let is_safe = adjusted_total_risk <= self.config.max_risk_score;
                let mitigation = if !is_safe {
                    Some(format!(
                        "High arbitrage risk ({}). Consider: reducing position size, using private mempool, or waiting for better market conditions.",
                        adjusted_total_risk
                    ))
                } else {
                    None
                };

                Ok(RiskAssessment {
                    total_risk_score: adjusted_total_risk,
                    volatility_score: base_assessment.volatility_score,
                    liquidity_score: base_assessment.liquidity_score,
                    slippage_score: base_assessment.slippage_score,
                    mev_risk_score: adjusted_mev_risk,
                    gas_risk_score: base_assessment.gas_risk_score,
                    is_safe,
                    mitigation,
                    timestamp: Utc::now(),
                })
            },
            crate::mempool::MEVOpportunity::Sandwich(sandwich) => {
                // Check if sandwich attacks are disabled in config
                if self.config.disable_sandwich {
                    return Ok(RiskAssessment {
                        total_risk_score: 1.0,
                        volatility_score: 0.0,
                        liquidity_score: 0.0,
                        slippage_score: 0.0,
                        mev_risk_score: 1.0,
                        gas_risk_score: 0.0,
                        is_safe: false,
                        mitigation: Some("Sandwich attacks are disabled by configuration.".to_string()),
                        timestamp: Utc::now(),
                    });
                }

                // Assess sandwich opportunity as high-risk arbitrage
                let token_in = sandwich.token_in;
                let token_out = sandwich.token_out;
                let amount_in = sandwich.amount_in;

                let base_assessment = self.assess_trade(token_in, token_out, amount_in, block_number).await?;

                // Sandwich attacks are inherently high-risk and ethically questionable
                let sandwich_risk_multiplier = 2.5;
                let adjusted_total_risk = (base_assessment.total_risk_score * sandwich_risk_multiplier).min(1.0);

                Ok(RiskAssessment {
                    total_risk_score: adjusted_total_risk,
                    volatility_score: base_assessment.volatility_score,
                    liquidity_score: base_assessment.liquidity_score,
                    slippage_score: base_assessment.slippage_score,
                    mev_risk_score: 1.0, // Maximum MEV risk for sandwich attacks
                    gas_risk_score: base_assessment.gas_risk_score,
                    is_safe: false, // Sandwich attacks are never considered "safe"
                    mitigation: Some("Sandwich attack detected. This strategy may harm other users and should be avoided.".to_string()),
                    timestamp: Utc::now(),
                })
            },
            crate::mempool::MEVOpportunity::Liquidation(liquidation) => {
                // For liquidation MEV opportunities, delegate to assess_liquidation
                self.assess_liquidation(liquidation, block_number).await
            },
            crate::mempool::MEVOpportunity::JITLiquidity(jit) => {
                let base_assessment = self.assess_trade(jit.token_in, jit.token_out, jit.amount0_desired, block_number).await?;

                // JIT liquidity has moderate additional risk due to timing sensitivity
                let jit_risk_multiplier = 1.3;
                let adjusted_total_risk = (base_assessment.total_risk_score * jit_risk_multiplier).min(1.0);

                Ok(RiskAssessment {
                    total_risk_score: adjusted_total_risk,
                    volatility_score: base_assessment.volatility_score,
                    liquidity_score: base_assessment.liquidity_score,
                    slippage_score: base_assessment.slippage_score,
                    mev_risk_score: (base_assessment.mev_risk_score * 1.2).min(1.0),
                    gas_risk_score: base_assessment.gas_risk_score,
                    is_safe: adjusted_total_risk <= self.config.max_risk_score,
                    mitigation: if adjusted_total_risk > self.config.max_risk_score {
                        Some("JIT liquidity strategy has elevated timing risk. Monitor gas prices and block inclusion carefully.".to_string())
                    } else {
                        None
                    },
                    timestamp: Utc::now(),
                })
            },
            crate::mempool::MEVOpportunity::OracleUpdate(oracle) => {
                // Oracle update opportunities have high execution risk
                let oracle_risk_base = 0.6; // Base risk for oracle update strategies
                let price_deviation_risk = if oracle.old_price > 0.0 {
                    let price_change_pct = ((oracle.new_price - oracle.old_price) / oracle.old_price).abs();
                    (price_change_pct / 0.1).min(1.0) // Risk increases with price deviation
                } else {
                    1.0 // Treat as maximum risk if baseline price is unknown/zero
                };

                let total_risk = (oracle_risk_base + price_deviation_risk * 0.4).min(1.0);

                Ok(RiskAssessment {
                    total_risk_score: total_risk,
                    volatility_score: price_deviation_risk,
                    liquidity_score: 0.3, // Oracle updates usually have sufficient liquidity
                    slippage_score: 0.2, // Low slippage risk for oracle-based strategies
                    mev_risk_score: 0.8, // High MEV competition
                    gas_risk_score: 0.4, // Moderate gas risk
                    is_safe: total_risk <= self.config.max_risk_score,
                    mitigation: if total_risk > self.config.max_risk_score {
                        Some("Oracle update strategy has high execution risk due to competition and timing sensitivity.".to_string())
                    } else {
                        None
                    },
                    timestamp: Utc::now(),
                })
            },
            crate::mempool::MEVOpportunity::FlashLoan(flashloan) => {
                // Flash loan opportunities have unique risk characteristics
                let base_risk = 0.4; // Base risk for flash loan strategies
                let flash_fee_risk = (flashloan.flash_fee_bps as f64 / 10000.0).min(0.3); // Risk from flash loan fees

                let amount_risk = {
                    let chain_name = self.blockchain.get_chain_name();
                    let token_price = self.price_oracle
                        .get_token_price_usd_at(&chain_name, flashloan.token_to_flash, block_number, None)
                        .await
                        .unwrap_or(0.0);
                    let decimals = self.blockchain
                        .get_token_decimals(flashloan.token_to_flash, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into()))))
                        .await
                        .unwrap_or(18);
                    let amount_usd = crate::types::normalize_units(flashloan.amount_to_flash, decimals) * token_price;
                    
                    if amount_usd > 1_000_000.0 {
                        0.4 // High risk for large flash loans
                    } else if amount_usd > 100_000.0 {
                        0.2
                    } else {
                        0.1
                    }
                };

                let total_risk = (base_risk + flash_fee_risk + amount_risk).min(1.0);

                Ok(RiskAssessment {
                    total_risk_score: total_risk,
                    volatility_score: 0.3,
                    liquidity_score: amount_risk,
                    slippage_score: 0.2,
                    mev_risk_score: 0.6, // Moderate MEV risk
                    gas_risk_score: 0.4, // Flash loans have complex gas requirements
                    is_safe: total_risk <= self.config.max_risk_score,
                    mitigation: if total_risk > self.config.max_risk_score {
                        Some("Flash loan strategy has elevated risk due to complex execution requirements and fee costs.".to_string())
                    } else {
                        None
                    },
                    timestamp: Utc::now(),
                })
            }
        }
    }

    #[instrument(skip(self, opportunity))]
    async fn assess_liquidation(
        &self,
        opportunity: &crate::mempool::PotentialLiquidationOpportunity,
        block_number: Option<u64>,
    ) -> Result<RiskAssessment, RiskError> {
        let chain_name = self.blockchain.get_chain_name();

        // Health factor risk - lower health factor = higher urgency but lower execution risk
        let health_factor_score = if opportunity.health_factor < 1.0 {
            (1.0 - opportunity.health_factor).min(1.0)
        } else {
            0.0 // No liquidation risk if health factor >= 1.0
        };

        // Health factor urgency score - affects timing and execution priority
        let health_factor_urgency = if opportunity.health_factor < 0.95 {
            0.8 // Very urgent - high priority execution
        } else if opportunity.health_factor < 0.98 {
            0.5 // Moderately urgent
        } else {
            0.2 // Less urgent - more time to execute carefully
        };

        // Collateral token volatility and liquidity assessment
        // TODO: Use historical volatility when block_number is provided to avoid lookahead bias in backtesting
        let collateral_volatility = self.price_oracle
            .get_token_volatility(&chain_name, opportunity.collateral_token, 24 * 60 * 60)
            .await
            .unwrap_or(0.5);
        let collateral_volatility_score = (collateral_volatility / self.config.max_volatility).min(1.0);

        let debt_volatility = self.price_oracle
            .get_token_volatility(&chain_name, opportunity.debt_token, 24 * 60 * 60)
            .await
            .unwrap_or(0.5);
        let debt_volatility_score = (debt_volatility / self.config.max_volatility).min(1.0);

        let overall_volatility_score = f64::max(collateral_volatility_score, debt_volatility_score);

        // Liquidity assessment for both tokens
        let collateral_price = self.price_oracle
            .get_token_price_usd_at(&chain_name, opportunity.collateral_token, block_number, None)
            .await
            .unwrap_or(0.0);
        let debt_price = self.price_oracle
            .get_token_price_usd_at(&chain_name, opportunity.debt_token, block_number, None)
            .await
            .unwrap_or(0.0);

        let collateral_liquidity_units = self.pool_manager
            .get_total_liquidity_for_token_units(&chain_name, opportunity.collateral_token, block_number)
            .await
            .unwrap_or(0.0);
        let collateral_liquidity = collateral_liquidity_units * collateral_price;

        let debt_liquidity_units = self.pool_manager
            .get_total_liquidity_for_token_units(&chain_name, opportunity.debt_token, block_number)
            .await
            .unwrap_or(0.0);
        let debt_liquidity = debt_liquidity_units * debt_price;

        let min_liquidity = f64::min(collateral_liquidity, debt_liquidity);
        let liquidity_score = if min_liquidity < self.config.min_liquidity_usd {
            1.0
        } else {
            let debt_decimals = self.blockchain
                .get_token_decimals(opportunity.debt_token, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into()))))
                .await
                .unwrap_or(18);
            let debt_token_price = self.price_oracle
                .get_token_price_usd_at(&chain_name, opportunity.debt_token, block_number, None)
                .await
                .unwrap_or(0.0);
            let debt_amount_usd = crate::types::normalize_units(opportunity.debt_to_cover, debt_decimals) * debt_token_price;
            
            (debt_amount_usd / min_liquidity).min(1.0)
        };

        // Slippage risk assessment based on liquidation size vs available liquidity
        let slippage_score = {
            let collateral_decimals = self.blockchain
                .get_token_decimals(opportunity.collateral_token, block_number.map(|bn| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(bn.into()))))
                .await
                .unwrap_or(18);
            let collateral_price = self.price_oracle
                .get_token_price_usd_at(&chain_name, opportunity.collateral_token, block_number, None)
                .await
                .unwrap_or(0.0);
            let collateral_amount_usd = crate::types::normalize_units(opportunity.collateral_amount, collateral_decimals) * collateral_price;
            
            if collateral_liquidity > 0.0 {
                (collateral_amount_usd / collateral_liquidity).min(1.0)
            } else {
                1.0
            }
        };

        // Gas risk for liquidation transactions (typically higher than normal trades)
        let gas_risk_score = {
            let fee_data = self.gas_oracle
                .get_fee_data(&chain_name, block_number)
                .await
                .unwrap_or(FeeData {
                    base_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_gas: None,
                    gas_price: Some(U256::from(20_000_000_000u64)), // 20 gwei default
                    block_number: block_number.unwrap_or(0),
                    chain_name: chain_name.to_string(),
                });
            
            let effective_gas_price = if let (Some(base), Some(priority)) = (fee_data.base_fee_per_gas, fee_data.max_priority_fee_per_gas) {
                base.saturating_add(priority)
            } else {
                fee_data.gas_price.unwrap_or(U256::zero())
            };

            let gas_price_gwei = effective_gas_price.as_u128() as f64 / 1_000_000_000.0;
            
            // Liquidations typically use 200k-400k gas
            let liquidation_gas_multiplier = if opportunity.requires_flashloan { 1.8 } else { 1.3 };
            let adjusted_gas_score = if gas_price_gwei <= self.config.min_gas_gwei {
                0.0
            } else if gas_price_gwei >= self.config.max_gas_gwei {
                1.0
            } else {
                (gas_price_gwei - self.config.min_gas_gwei) / (self.config.max_gas_gwei - self.config.min_gas_gwei)
            };
            
            (adjusted_gas_score * liquidation_gas_multiplier).min(1.0)
        };

        // MEV competition risk - liquidations face high MEV competition
        let mev_risk_score = {
            let base_mev_risk = 0.7f64; // Liquidations always have high MEV risk
            let profit_multiplier = if opportunity.estimated_profit_usd > 10000.0 {
                1.0 // Maximum competition for high-profit liquidations
            } else if opportunity.estimated_profit_usd > 1000.0 {
                0.8
            } else {
                0.6
            };
            
            let protocol_risk_multiplier = match opportunity.protocol_name.as_str() {
                "Aave" | "Compound" | "MakerDAO" => 1.0, // Well-known protocols attract more competition
                _ => 0.8,
            };
            
            (base_mev_risk * profit_multiplier * protocol_risk_multiplier).min(1.0_f64)
        };

        // Liquidation-specific risks
        let protocol_risk = {
            // Risk varies by lending protocol
            match opportunity.protocol_name.as_str() {
                "Aave" => 0.1,
                "Compound" => 0.15,
                "MakerDAO" => 0.1,
                _ => 0.3, // Higher risk for unknown protocols
            }
        };

        let liquidation_bonus_risk = {
            // Higher bonus might indicate higher risk position
            let bonus_pct = opportunity.liquidation_bonus_bps as f64 / 10000.0;
            if bonus_pct > 0.15 {
                0.3 // High bonus suggests risky collateral
            } else if bonus_pct > 0.10 {
                0.2
            } else {
                0.1
            }
        };

        let timing_risk = if opportunity.health_factor < 0.95 {
            0.1 // Urgent liquidations have less timing risk
        } else {
            0.4 // Health factor close to 1.0 means liquidation window might be short
        };

        // Calculate dynamic weights for liquidation assessment
        let weights = {
            let mut w = self.config.risk_score_weights.clone();
            // Adjust weights for liquidation-specific risks
            w.volatility *= 1.2; // Volatility is critical for liquidations
            w.liquidity *= 1.3; // Liquidity is crucial for execution
            w.slippage *= 1.1;
            w.mev *= 1.4; // MEV competition is very high
            w.gas *= 1.2; // Gas costs can eat into profits quickly
            
            // Normalize weights
            let total = w.volatility + w.liquidity + w.slippage + w.mev + w.gas;
            if total > 0.0 {
                w.volatility /= total;
                w.liquidity /= total;
                w.slippage /= total;
                w.mev /= total;
                w.gas /= total;
            }
            w
        };

        let base_risk_score = (overall_volatility_score * weights.volatility)
            + (liquidity_score * weights.liquidity)
            + (slippage_score * weights.slippage)
            + (mev_risk_score * weights.mev)
            + (gas_risk_score * weights.gas);

        // Health factor affects overall risk - more urgent liquidations have higher execution risk
        let health_factor_risk_adjustment = health_factor_urgency * 0.3; // Health factor contributes up to 30% to total risk
        
        // Health factor score directly impacts liquidation urgency and execution risk
        let health_factor_execution_risk = health_factor_score * 0.4; // Health factor score contributes up to 40% to execution risk
        
        let total_risk_score = (base_risk_score + protocol_risk + liquidation_bonus_risk + timing_risk + health_factor_risk_adjustment + health_factor_execution_risk).min(1.0);

        let is_safe = total_risk_score <= self.config.max_risk_score && opportunity.health_factor < 1.0;
        
        let mitigation = if !is_safe {
            if opportunity.health_factor >= 1.0 {
                Some("Position is not liquidatable (health factor >= 1.0).".to_string())
            } else if total_risk_score > self.config.max_risk_score {
                Some(format!(
                    "High liquidation risk ({}). Consider: monitoring health factor changes, using flashloan if profitable, or waiting for better gas conditions. Protocol: {}",
                    total_risk_score,
                    opportunity.protocol_name
                ))
            } else {
                None
            }
        } else {
            None
        };

        debug!(
            health_factor = opportunity.health_factor,
            health_factor_urgency,
            health_factor_risk_adjustment,
            health_factor_execution_risk,
            collateral_token = %opportunity.collateral_token,
            debt_token = %opportunity.debt_token,
            estimated_profit_usd = opportunity.estimated_profit_usd,
            protocol = %opportunity.protocol_name,
            total_risk_score,
            is_safe,
            "Assessed liquidation risk"
        );

        Ok(RiskAssessment {
            total_risk_score,
            volatility_score: overall_volatility_score,
            liquidity_score,
            slippage_score,
            mev_risk_score,
            gas_risk_score,
            is_safe,
            mitigation,
            timestamp: Utc::now(),
        })
    }
}