use async_trait::async_trait;
use ethers::types::U256;
use eyre::Result;
use tracing::{debug, info, warn};
use std::time::Duration;

use crate::{
    strategies::{MEVEngineError, MEVStrategyConfig},
    mempool::{MEVOpportunity, PotentialLiquidationOpportunity},
};

use crate::mev_engine::{SafeActiveMEVPlay, MEVPlayStatus};
use super::{MEVStrategy, StrategyContext};

pub struct LiquidationStrategy {
    pub min_profit_usd: f64,
    pub ignore_risk: bool,
    pub gas_escalation_factor: u128,
}

impl LiquidationStrategy {
    pub fn new(config: MEVStrategyConfig) -> Result<Self, MEVEngineError> {
        // Validate min_profit_usd
        if !config.min_profit_usd.is_finite() || config.min_profit_usd < 0.0 {
            return Err(MEVEngineError::StrategyError(
                format!("Invalid min_profit_usd: {}. Must be finite and non-negative.", config.min_profit_usd)
            ));
        }

        // Validate volatility_discount if provided
        if let Some(discount) = config.volatility_discount {
            if discount < 0.0 || discount > 1.0 {
                return Err(MEVEngineError::StrategyError(
                    format!("Invalid volatility_discount: {}. Must be between 0.0 and 1.0.", discount)
                ));
            }
        }

        // Validate gas_escalation_factor if provided
        if let Some(factor) = config.gas_escalation_factor {
            if factor == 0 || factor > 1000 {
                return Err(MEVEngineError::StrategyError(
                    format!("Invalid gas_escalation_factor: {}. Must be between 1 and 1000.", factor)
                ));
            }
        }

        Ok(Self {
            min_profit_usd: config.min_profit_usd,
            ignore_risk: config.ignore_risk,
            gas_escalation_factor: config.gas_escalation_factor.unwrap_or(4),
        })
    }

    async fn gas_bid(
        &self,
        context: &StrategyContext,
    ) -> Result<(U256, U256), MEVEngineError> {
        let chain_name = context.blockchain_manager.get_chain_name();
        let gas_price = context.gas_oracle.read().await
            .get_gas_price(&chain_name, None)
            .await
            .map_err(|e| MEVEngineError::GasEstimation(format!("Failed to get gas price: {}", e)))?;
        
        let base_fee = gas_price.base_fee;
        let prio_fee = gas_price.priority_fee;
        
        let offset = U256::from(1u64) * U256::exp10(9);
        let extra_prio = offset
            .checked_mul(U256::from(self.gas_escalation_factor))
            .ok_or_else(|| MEVEngineError::SimulationError("gas escalation overflow".into()))?;
        let max_fee = base_fee
            .checked_mul(U256::from(3))
            .and_then(|v| v.checked_add(prio_fee))
            .and_then(|v| v.checked_add(extra_prio))
            .ok_or_else(|| MEVEngineError::SimulationError("gas calculation overflow".into()))?;
        let max_priority = prio_fee
            .checked_add(extra_prio)
            .ok_or_else(|| MEVEngineError::SimulationError("gas calculation overflow".into()))?;
        Ok((max_fee, max_priority))
    }

    async fn block_deadline(
        &self,
        context: &StrategyContext,
        opp: &PotentialLiquidationOpportunity,
    ) -> Result<bool, MEVEngineError> {
        let remaining_duration = context
            .blockchain_manager
            .estimate_time_to_target(opp.block_number)
            .await
            .map_err(|e| MEVEngineError::Blockchain(format!("Failed to estimate time to target: {}", e)))?;
        
        let tolerance_duration = if context.blockchain_manager.is_l2() {
            Duration::from_millis(2_000)
        } else {
            Duration::from_millis(6_000)
        };
        Ok(remaining_duration < tolerance_duration)
    }

    fn u256_to_f64_with_decimals(x: U256, decimals: u8) -> f64 {
        let divisor = U256::exp10(decimals as usize);
        let (quotient, remainder) = (x / divisor, x % divisor);
        quotient.as_u128() as f64 + (remainder.as_u128() as f64) / 10f64.powi(decimals as i32)
    }
}

#[async_trait]
impl MEVStrategy for LiquidationStrategy {
    fn name(&self) -> &'static str {
        "Liquidation"
    }

    fn handles_opportunity_type(&self, opportunity: &MEVOpportunity) -> bool {
        matches!(opportunity, MEVOpportunity::Liquidation(_))
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
        let opp = match opportunity {
            MEVOpportunity::Liquidation(l) => l,
            _ => {
                return Err(MEVEngineError::StrategyError(
                    "received non-liquidation opportunity".into(),
                ))
            }
        };

        let play_id = active_play.read(|p| p.play_id).await;
        info!(
            play_id = %play_id,
            user = %opp.user_address,
            protocol = %opp.lending_protocol_address,
            "simulating liquidation"
        );
        active_play.update_atomic(|play| {
            if let Err(e) = play.start_simulation() {
                warn!("Failed to start simulation: {}", e);
            }
        }).await;

        let simulation_result = context
            .simulation_engine
            .simulate_liquidation_and_swap(&opp);
            
        let native_price_future = context
            .price_oracle
            .get_token_price_usd(
                &context.blockchain_manager.get_chain_name(),
                context.blockchain_manager.get_native_token_address()
            );
            
        let profit_token_price_future = context
            .price_oracle
            .get_token_price_usd(&context.blockchain_manager.get_chain_name(), opp.profit_token);

        let (
            collateral_result,
            native_price_result,
            profit_token_price_result,
        ) = tokio::join!(
            simulation_result,
            native_price_future,
            profit_token_price_future
        );

        let (collateral_received, debt_repaid_token, flashloan_fee_token, profit_token_addr) =
            collateral_result.map_err(|e| MEVEngineError::SimulationError(format!("liquidation simulation failed: {e}")))?;
            
        let native_price_usd = native_price_result
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get native token price: {}", e)))?;
            
        let profit_token_price_usd = profit_token_price_result
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get profit token price: {}", e)))?;

        let decimals = context
            .blockchain_manager
            .get_token_decimals(profit_token_addr, None)
            .await
            .map_err(|e| MEVEngineError::Blockchain(e.to_string()))?;

        let net_profit_token = collateral_received
            .saturating_sub(debt_repaid_token)
            .saturating_sub(flashloan_fee_token);

        let net_profit_token_f64 = Self::u256_to_f64_with_decimals(net_profit_token, decimals);
        let mut net_profit_usd = net_profit_token_f64 * profit_token_price_usd;

        let gas_limit = determine_liquidation_gas_limit(&opp).await?;
        let (max_fee, max_priority_fee) = self.gas_bid(context).await?;
        
        // Set gas parameters on active play for transaction building
        active_play.update_atomic(|play| {
            play.max_fee_per_gas = Some(max_fee);
            play.max_priority_fee_per_gas = Some(max_priority_fee);
        }).await;
        let gas_cost_wei = gas_limit
            .checked_mul(max_fee)
            .ok_or_else(|| MEVEngineError::SimulationError("gas overflow".into()))?;
        let gas_cost_usd = native_price_usd * gas_cost_wei.as_u128() as f64 / 1e18_f64;

        net_profit_usd -= gas_cost_usd;
        // Simple race model: success probability based on analytics competition proxy
        let performance = context.analytics.get_performance_metrics().await;
        let contention = performance.get("mempool_congestion").copied().unwrap_or(0.0).max(0.0).min(1.0);
        let bump_cost_factor = 1.0 + contention * 0.1;
        let success_prob = (1.0 - contention * 0.6).max(0.0).min(1.0);
        let ev_usd = net_profit_usd * success_prob - gas_cost_usd * (bump_cost_factor - 1.0);
        if ev_usd <= 0.0 {
            active_play.update_atomic(|play| {
                play.status = MEVPlayStatus::RiskAssessmentFailed(
                    "negative expected value under contention model".into(),
                );
            }).await;
            return Ok(());
        }
        active_play.update_atomic(|play| {
            play.estimated_profit_usd = net_profit_usd;
        }).await;

        let play_id = active_play.read(|p| p.play_id).await;
        info!(
            play_id = %play_id,
            "profit_token={:?}, net_profit_token={}, gas_usd={:.6}, net_usd={:.6}",
            profit_token_addr,
            net_profit_token,
            gas_cost_usd,
            net_profit_usd
        );

        if net_profit_usd < self.min_profit_usd {
            let play_id = active_play.read(|p| p.play_id).await;
            debug!(
                play_id = %play_id,
                "profit {:.6} below threshold {:.2}",
                net_profit_usd,
                self.min_profit_usd
            );
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure(format!(
                    "profit {:.6} < {:.2}",
                    net_profit_usd, self.min_profit_usd
                )) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        if !self.ignore_risk {
            let risk = context
                .risk_manager
                .assess_liquidation(&opp, Some(opp.block_number))
                .await
                .map_err(|e| MEVEngineError::RiskAssessment(e.to_string()))?;
            if !risk.is_safe {
                let play_id = active_play.read(|p| p.play_id).await;
                warn!(play_id = %play_id, ?risk, "risk too high");
                active_play.update_atomic(|play| {
                    play.status = MEVPlayStatus::RiskAssessmentFailed(
                        risk.mitigation
                            .clone()
                            .unwrap_or_else(|| "no mitigation".into()),
                    );
                }).await;
                return Ok(());
            }
        }

        let urgent = self.block_deadline(context, &opp).await?;
        active_play.update_atomic(|play| {
            if urgent {
                if let Err(e) = play.mark_ready_for_submission() {
                    warn!("Failed to mark ready for submission: {}", e);
                }
                if let Err(e) = play.mark_urgent_submission() {
                    warn!("Failed to mark urgent submission: {}", e);
                }
            } else {
                if let Err(e) = play.mark_ready_for_submission() {
                    warn!("Failed to mark ready for submission: {}", e);
                }
            }
        }).await;

        let play_id = active_play.read(|p| p.play_id).await;
        info!(play_id = %play_id, "liquidation validated");
        Ok(())
    }
}

async fn determine_liquidation_gas_limit(opp: &PotentialLiquidationOpportunity) -> Result<U256, MEVEngineError> {
    let mut gas_limit = U256::from(300_000u64);
    
    if opp.requires_flashloan {
        gas_limit = gas_limit.checked_add(U256::from(100_000u64))
            .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
    }
    
    if opp.profit_token != opp.collateral_token {
        gas_limit = gas_limit.checked_add(U256::from(150_000u64))
            .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
    }
    
    if opp.debt_amount > U256::from(1_000_000_000_000_000_000u128) {
        gas_limit = gas_limit.checked_add(U256::from(50_000u64))
            .ok_or_else(|| MEVEngineError::SimulationError("gas limit overflow".into()))?;
    }
    
    Ok(gas_limit)
}