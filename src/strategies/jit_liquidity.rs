use async_trait::async_trait;
use ethers::types::{Address, U256};
use eyre::Result;
use tracing::{info, warn, debug};
use std::time::Duration;

use crate::{
    strategies::MEVEngineError,
    mempool::{JITLiquidityOpportunity, MEVOpportunity},
    mev_engine::{SafeActiveMEVPlay, MEVPlayStatus},
    types::ProtocolType,
};

use super::{
    MEVStrategy, MEVStrategyConfig, StrategyContext,
};

pub struct JITLiquidityStrategy {
    pub min_profit_usd: f64,
    pub ignore_risk: bool,
    pub volatility_discount: f64,
    pub gas_escalation_factor: u128,
}

impl JITLiquidityStrategy {
    pub fn new(config: MEVStrategyConfig) -> Self {
        Self {
            min_profit_usd: config.min_profit_usd,
            ignore_risk: config.ignore_risk,
            volatility_discount: config.volatility_discount.unwrap_or(0.80),
            gas_escalation_factor: config.gas_escalation_factor.unwrap_or(5),
        }
    }

    async fn is_high_impact_swap(
        &self,
        opp: &JITLiquidityOpportunity,
        context: &StrategyContext,
    ) -> Result<bool, MEVEngineError> {
        let pool_state = context
            .pool_manager
            .get_pool_info(
                &context.blockchain_manager.get_chain_name(),
                opp.pool_address,
                None
            )
            .await?;

        let amount_in = if opp.amount0_desired > U256::zero() {
            opp.amount0_desired
        } else {
            opp.amount1_desired
        };
        
        let swap_value_usd = context
            .price_oracle
            .estimate_swap_value_usd(
                &context.blockchain_manager.get_chain_name(),
                opp.token_in,
                opp.token_out,
                amount_in,
            )
            .await?;

        let mut metrics = std::collections::HashMap::new();
        metrics.insert("swap_value_usd".to_string(), swap_value_usd);
        metrics.insert("pool_liquidity_usd".to_string(), pool_state.liquidity_usd);
        
        context.analytics.track_opportunity(
            &format!("jit_swap_{:?}", opp.target_tx_hash),
            &metrics
        ).await;

        let liquidity_usd = pool_state.liquidity_usd;
        let is_large_swap = swap_value_usd > 100_000.0;
        let is_low_liquidity = liquidity_usd < 1_000_000.0;
        let price_impact = swap_value_usd / liquidity_usd.max(1.0);
        let is_high_impact = price_impact > 0.02;

        Ok(is_large_swap && (is_low_liquidity || is_high_impact))
    }

    fn apply_sandwich_adjustments(&self, opp: &mut JITLiquidityOpportunity) {
        opp.liquidity_amount = opp
            .liquidity_amount
            .saturating_mul(U256::from(120u128))
            .checked_div(U256::from(100u128))
            .unwrap_or(opp.liquidity_amount);
        opp.timeout_blocks = opp.timeout_blocks.saturating_sub(1);
    }

    async fn calculate_optimal_gas_bidding(
        &self,
        context: &StrategyContext,
        is_sandwich: bool,
    ) -> Result<(U256, U256), MEVEngineError> {
        let gas_price = context.gas_oracle.read().await.get_gas_price(
            context.blockchain_manager.get_chain_name(),
            None,
        ).await
        .map_err(|e| MEVEngineError::SimulationError(format!("gas price fetch failed: {}", e)))?;

        let base_fee = gas_price.base_fee;
        let priority_fee = gas_price.priority_fee;

        let escalation = if is_sandwich {
            self.gas_escalation_factor.saturating_mul(2)
        } else {
            self.gas_escalation_factor
        };

        let offset = U256::from(1u64) * U256::exp10(9);
        let extra_prio = offset
            .checked_mul(U256::from(escalation))
            .ok_or_else(|| MEVEngineError::SimulationError("gas escalation overflow".into()))?;

        let max_fee_per_gas = base_fee
            .checked_mul(U256::from(3))
            .and_then(|v| v.checked_add(priority_fee))
            .and_then(|v| v.checked_add(extra_prio))
            .ok_or_else(|| MEVEngineError::SimulationError("gas calculation overflow".into()))?;

        let max_priority_fee_per_gas = priority_fee
            .checked_add(extra_prio)
            .ok_or_else(|| MEVEngineError::SimulationError("gas calculation overflow".into()))?;

        Ok((max_fee_per_gas, max_priority_fee_per_gas))
    }

    async fn verify_block_timing(
        &self,
        context: &StrategyContext,
        opp: &JITLiquidityOpportunity,
    ) -> Result<bool, MEVEngineError> {
        let current_block = context.blockchain_manager.get_current_block_number().await?;
        let blocks_to_target = opp.block_number.saturating_sub(current_block);
        let remaining = context
            .blockchain_manager
            .estimate_time_to_target(opp.block_number)
            .await?;
        
        let tolerance = if context.blockchain_manager.is_l2() {
            Duration::from_millis(2_000)
        } else {
            Duration::from_millis(6_000)
        };
        
        Ok(remaining < tolerance || blocks_to_target <= 2)
    }

    fn u256_to_f64_with_decimals(x: U256, decimals: u8) -> f64 {
        let divisor = U256::exp10(decimals as usize);
        let (quotient, remainder) = (x / divisor, x % divisor);
        quotient.as_u128() as f64 + (remainder.as_u128() as f64) / 10f64.powi(decimals as i32)
    }
}

#[async_trait]
impl MEVStrategy for JITLiquidityStrategy {
    fn name(&self) -> &'static str {
        "JITLiquidity"
    }

    fn handles_opportunity_type(&self, opportunity: &MEVOpportunity) -> bool {
        matches!(opportunity, MEVOpportunity::JITLiquidity(_))
    }

    fn min_profit_threshold_usd(&self) -> f64 {
        self.min_profit_usd
    }

    async fn execute(
        &self,
        active_play: &SafeActiveMEVPlay,
        opportunity: MEVOpportunity,
        context: &StrategyContext,
    ) -> Result<(), MEVEngineError> {
        let mut opp = match opportunity {
            MEVOpportunity::JITLiquidity(j) => j,
            _ => {
                return Err(MEVEngineError::StrategyError(
                    "received non-JIT opportunity".into(),
                ))
            }
        };

        let play_id = active_play.read(|p| p.play_id).await;
        
        info!(
            play_id = %play_id,
            pool = %opp.pool_address,
            target = %opp.target_tx_hash,
            "evaluating JIT-liquidity opportunity"
        );

        active_play.update_atomic(|p| p.status = MEVPlayStatus::Simulating).await;

        let (
            sim_res,
            price0_usd,
            price1_usd,
            sandwich_possible,
            native_price_usd,
            decimals0,
            decimals1,
        ) = tokio::join!(
            context.simulation_engine.simulate_jit_liquidity(&opp),
            context.price_oracle.get_token_price_usd(
                &context.blockchain_manager.get_chain_name(),
                opp.token0
            ),
            context.price_oracle.get_token_price_usd(
                &context.blockchain_manager.get_chain_name(),
                opp.token1
            ),
            self.is_high_impact_swap(&opp, context),
            context.price_oracle.get_token_price_usd(
                &context.blockchain_manager.get_chain_name(),
                context.blockchain_manager.get_native_token_address()
            ),
            context.blockchain_manager.get_token_decimals(opp.token0, None),
            context.blockchain_manager.get_token_decimals(opp.token1, None),
        );

        let (fees0, fees1) = sim_res
            .map_err(|e| MEVEngineError::SimulationError(format!("simulation failed: {e}")))?;

        let token0_price = price0_usd?;
        let token1_price = price1_usd?;
        let native_price = native_price_usd?;
        let token0_decimals = decimals0?;
        let token1_decimals = decimals1?;
        
        let fees0_usd = Self::u256_to_f64_with_decimals(fees0, token0_decimals) * token0_price;
        let fees1_usd = Self::u256_to_f64_with_decimals(fees1, token1_decimals) * token1_price;
        let mut total_fees_usd = fees0_usd + fees1_usd;

        if is_high_volatility_pool(&opp.pool_address, context).await {
            total_fees_usd *= self.volatility_discount;
        }

        let is_sandwich_possible = sandwich_possible?;
        
        let gas_limit = determine_jit_gas_limit(&opp, context).await?;
        let (max_fee_per_gas, max_priority_fee_per_gas) =
            self.calculate_optimal_gas_bidding(context, is_sandwich_possible).await?;

        active_play.update_atomic(|p| {
            p.max_fee_per_gas = Some(max_fee_per_gas);
            p.max_priority_fee_per_gas = Some(max_priority_fee_per_gas);
        }).await;

        let gas_cost_native = gas_limit
            .checked_mul(max_fee_per_gas)
            .ok_or_else(|| MEVEngineError::SimulationError("gas cost overflow".into()))?;
        let gas_cost_usd = native_price * gas_cost_native.as_u128() as f64 / 1e18_f64;

        if is_sandwich_possible {
            self.apply_sandwich_adjustments(&mut opp);
            total_fees_usd *= 1.20;
        }

        let net_profit_usd = total_fees_usd - gas_cost_usd;
        active_play.update_atomic(|p| p.estimated_profit_usd = net_profit_usd).await;

        info!(
            play_id = %play_id,
            "fees_usd={:.6}, gas_usd={:.6}, net_usd={:.6}",
            total_fees_usd,
            gas_cost_usd,
            net_profit_usd
        );

        if net_profit_usd < self.min_profit_usd {
            debug!(
                play_id = %play_id,
                "profit {:.6} below threshold {:.6}",
                net_profit_usd,
                self.min_profit_usd
            );
            active_play.record_simulation_failure(format!(
                "profit {:.6} < threshold",
                net_profit_usd
            )).await?;
            return Ok(());
        }

        if !self.ignore_risk {
            let amount_in = if opp.amount0_desired > U256::zero() {
                opp.amount0_desired
            } else {
                opp.amount1_desired
            };
            
            let risk = context
                .risk_manager
                .assess_trade(
                    opp.token_in,
                    opp.token_out,
                    amount_in,
                    Some(opp.block_number),
                )
                .await?;
                
            if !risk.is_safe {
                warn!(play_id = %play_id, ?risk, "risk too high");
                active_play.update_atomic(|p| {
                    p.status = MEVPlayStatus::RiskAssessmentFailed(
                        risk.mitigation
                            .clone()
                            .unwrap_or_else(|| "no mitigation".into()),
                    );
                }).await;
                return Ok(());
            }
        }

        let urgent = self.verify_block_timing(context, &opp).await?;
        if urgent {
            active_play.mark_urgent_submission().await?;
        } else {
            active_play.mark_ready_for_submission().await?;
        }

        info!(play_id = %play_id, "JIT-liquidity validated");
        Ok(())
    }
}

async fn determine_jit_gas_limit(
    opp: &JITLiquidityOpportunity,
    context: &StrategyContext,
) -> Result<U256, MEVEngineError> {
    let mut gas_limit = U256::from(400_000u64);

    let pool_type = context
        .pool_manager
        .get_pool_info(
            &context.blockchain_manager.get_chain_name(),
            opp.pool_address,
            None
        )
        .await
        .map(|info| info.protocol_type)
        .unwrap_or(ProtocolType::Other("unknown".to_string()));

    let (decimals0, decimals1) = tokio::join!(
        context.blockchain_manager.get_token_decimals(opp.token0, None),
        context.blockchain_manager.get_token_decimals(opp.token1, None),
    );

    let _token0_decimals = match decimals0 {
        Ok(d) if d > 0 => d,
        _ => return Err(MEVEngineError::Blockchain("missing token0 decimals".into())),
    };
    let _token1_decimals = match decimals1 {
        Ok(d) if d > 0 => d,
        _ => return Err(MEVEngineError::Blockchain("missing token1 decimals".into())),
    };

    if opp.liquidity_amount > U256::from(1_000_000_000_000_000_000u128) {
        gas_limit = gas_limit.checked_add(U256::from(100_000u64))
            .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
    }

    match pool_type {
        ProtocolType::UniswapV3 => {
            gas_limit = gas_limit.checked_add(U256::from(80_000u64))
                .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
        }
        ProtocolType::Other(ref s) if s.contains("v3") => {
            gas_limit = gas_limit.checked_add(U256::from(80_000u64))
                .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
        }
        _ => {}
    }

    let mut metrics = std::collections::HashMap::new();
    metrics.insert("gas_limit".to_string(), gas_limit.as_u128() as f64);
    metrics.insert("pool_fee".to_string(), opp.fee as f64);
    context.analytics.track_opportunity(
        &format!("gas_estimation_jit_{:?}", opp.pool_address),
        &metrics
    ).await;

    Ok(gas_limit)
}

fn calculate_price_deviation(price1: f64, price2: f64) -> f64 {
    if price1 == 0.0 || price2 == 0.0 {
        return 0.0;
    }
    let avg_price = (price1 + price2) / 2.0;
    ((price1 - price2).abs() / avg_price) * 100.0
}

async fn is_competing_pool(pool_address: &Address, context: &StrategyContext) -> bool {
    let performance = context.analytics.get_performance_metrics().await;
    
    let pool_key = format!("pool_competition_{:?}", pool_address);
    let competition_score = performance.get(&pool_key).copied().unwrap_or(0.0);
    
    let mut metrics = std::collections::HashMap::new();
    metrics.insert("competition_check".to_string(), 1.0);
    metrics.insert("competition_score".to_string(), competition_score);
    context.analytics.track_opportunity(
        &format!("competition_check_jit_{:?}", pool_address),
        &metrics
    ).await;
    
    competition_score > 0.5
}

async fn is_high_volatility_pool(pool_address: &Address, context: &StrategyContext) -> bool {
    match context.pool_manager.get_pool_info(
        &context.blockchain_manager.get_chain_name(),
        *pool_address,
        None
    ).await {
        Ok(pool_state) => {
            match pool_state.protocol_type {
                ProtocolType::UniswapV2 => {
                    if let (Some(reserve0), Some(reserve1)) = (pool_state.v2_reserve0, pool_state.v2_reserve1) {
                        let has_reserves = reserve0 > U256::zero() && reserve1 > U256::zero();
                        let low_reserves = reserve0 < U256::from(1_000_000_000_000_000_000u128) 
                            || reserve1 < U256::from(1_000_000_000_000_000_000u128);
                        has_reserves && low_reserves
                    } else {
                        false
                    }
                }
                ProtocolType::UniswapV3 => {
                    if let Some(liquidity) = pool_state.v3_liquidity {
                        let low_liquidity = liquidity < 1_000_000_000_000_000_000u128;
                        
                        let near_tick_boundary = if let (Some(tick), Some(tick_data)) = 
                            (pool_state.v3_tick_current, &pool_state.v3_tick_data) {
                            let near_major_tick = (tick % 100).abs() < 10;
                            
                            let tick_density_high = if !tick_data.is_empty() {
                                let total_ticks = tick_data.len();
                                let active_range = 200;
                                let ticks_in_range = tick_data.iter()
                                    .filter(|(tick_index, _)| (**tick_index - tick).abs() <= active_range)
                                    .count();
                                
                                let total_ticks_safe = if total_ticks == 0 { 1 } else { total_ticks };
                                let density_ratio = ticks_in_range as f64 / total_ticks_safe as f64;
                                density_ratio > 0.3 || ticks_in_range < 5
                            } else {
                                true
                            };
                            
                            near_major_tick || tick_density_high
                        } else {
                            false
                        };
                        
                        low_liquidity || near_tick_boundary
                    } else {
                        false
                    }
                }
                _ => {
                    pool_state.liquidity_usd < 1_000_000.0
                }
            }
        }
        Err(_) => false,
    }
}