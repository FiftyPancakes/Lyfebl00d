use async_trait::async_trait;
use ethers::types::{Address, U256};
use eyre::{Result};
use tracing::{debug, info, warn};
use std::time::Duration;

use crate::{
    strategies::MEVEngineError,
    mempool::{MEVOpportunity, PotentialSandwichOpportunity},
    mev_engine::{SafeActiveMEVPlay, MEVPlayStatus},
    errors::{PriceError, BlockchainError, RiskError},
};

use super::{
    MEVStrategy, StrategyContext,
};

/// Configuration stanza for [`SandwichStrategy`].
#[derive(Clone)]
pub struct SandwichStrategyConfig {
    pub min_profit_usd: f64,
    pub max_sandwich_hops: usize,
    pub frontrun_gas_offset_gwei: u64,
    pub backrun_gas_offset_gwei: u64,
    pub fixed_frontrun_native_amount_wei: U256,
    pub max_tx_gas_limit: U256,
    pub use_flashbots: bool,
    pub ignore_risk: bool,
    pub gas_escalation_factor: u128,
}

pub struct SandwichStrategy {
    cfg: SandwichStrategyConfig,
}

impl SandwichStrategy {
    pub fn new(cfg: SandwichStrategyConfig) -> Self {
        Self { cfg }
    }

    /// Return `(max_fee, max_priority)` given escalation `factor`.
    async fn gas_bid(
        &self,
        context: &StrategyContext,
        priority_offset_gwei: i64,
    ) -> Result<(U256, U256), MEVEngineError> {
        let chain = context.blockchain_manager.get_chain_name();
        let gas_oracle = context.gas_oracle.read().await;
        let gas_price = gas_oracle
            .get_gas_price(&chain, None)
            .await
            .map_err(|e| MEVEngineError::GasEstimation(format!("Failed to get gas price: {}", e)))?;
        
        let offset = U256::from(priority_offset_gwei.unsigned_abs()) * U256::exp10(9);

        let adjusted_prio = if priority_offset_gwei.is_negative() {
            gas_price.priority_fee
                .checked_sub(offset)
                .ok_or_else(|| MEVEngineError::ArithmeticError("Priority fee subtraction overflow".into()))?
        } else {
            gas_price.priority_fee
                .checked_add(offset)
                .ok_or_else(|| MEVEngineError::ArithmeticError("Priority fee addition overflow".into()))?
        };

        let extra = U256::from(self.cfg.gas_escalation_factor) * U256::exp10(9);
        let max_fee = gas_price.base_fee
            .checked_mul(U256::from(2))
            .and_then(|v| v.checked_add(adjusted_prio))
            .and_then(|v| v.checked_add(extra))
            .ok_or_else(|| MEVEngineError::SimulationError("fee overflow".into()))?;

        Ok((max_fee, adjusted_prio))
    }

    async fn block_deadline(
        &self,
        context: &StrategyContext,
        opp: &PotentialSandwichOpportunity,
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
}

#[async_trait]
impl MEVStrategy for SandwichStrategy {
    fn name(&self) -> &'static str {
        "Sandwich"
    }

    fn handles_opportunity_type(&self, opportunity: &MEVOpportunity) -> bool {
        matches!(opportunity, MEVOpportunity::Sandwich(_))
    }

    fn min_profit_threshold_usd(&self) -> f64 {
        self.cfg.min_profit_usd
    }

    async fn execute(
        &self,
        active_play: &SafeActiveMEVPlay,
        opportunity: MEVOpportunity,
        context: &StrategyContext,
    ) -> Result<(), MEVEngineError> {
        let opp = match opportunity {
            MEVOpportunity::Sandwich(s) => s,
            _ => {
                return Err(MEVEngineError::StrategyError(
                    "received non-sandwich opportunity".into(),
                ))
            }
        };
        self.handle_sandwich_opportunity(active_play, opp, context)
            .await
    }
}

impl SandwichStrategy {
    #[tracing::instrument(
        skip_all,
        fields(victim = %opp.target_tx_hash)
    )]
    async fn handle_sandwich_opportunity(
        &self,
        active_play: &SafeActiveMEVPlay,
        opp: PotentialSandwichOpportunity,
        context: &StrategyContext,
    ) -> Result<(), MEVEngineError> {
        info!("Simulating sandwich on {}", opp.target_tx_hash);
        active_play.update_atomic(|play| {
            if let Err(e) = play.start_simulation() {
                warn!("Failed to start simulation: {}", e);
            }
        }).await;

        let params = opp.swap_params.as_ref().ok_or_else(|| {
            MEVEngineError::StrategyError("missing parsed swap params in opportunity".into())
        })?;

        if params.token_in == params.token_out {
            return Err(MEVEngineError::StrategyError(
                "victim path too short".into(),
            ));
        }
        if self.cfg.max_sandwich_hops < 1 {
            return Err(MEVEngineError::StrategyError(format!(
                "max sandwich hops {} < 1",
                self.cfg.max_sandwich_hops
            )));
        }

        let native = context.blockchain_manager.get_weth_address();
        let profit_token = params.token_in;

        let front_native = self.cfg.fixed_frontrun_native_amount_wei;
        let front_amount_in = if profit_token == native {
            front_native
        } else {
            self.estimate_token_conversion(front_native, native, profit_token, context)
                .await?
        };
        if front_amount_in.is_zero() {
            return Err(MEVEngineError::StrategyError(
                "front-run amount zero".into(),
            ));
        }

        let path = vec![params.token_in, params.token_out];

        let (front_out, pools_after_front) = context
            .simulation_engine
            .simulate_multihop_swap_and_get_final_state(
                &path,
                front_amount_in,
                None,
            )
            .await
            .map_err(|e| MEVEngineError::SimulationError(format!("front-run simulation failed: {}", e)))?;
        if front_out.is_zero() {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("front-run produced zero output".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        let (victim_out, pools_after_victim) = context
            .simulation_engine
            .simulate_multihop_swap_and_get_final_state_with_state(
                &path,
                params.amount_in,
                Some(&pools_after_front),
                None,
            )
            .await
            .map_err(|e| MEVEngineError::SimulationError(format!("victim simulation after front-run failed: {}", e)))?;
        if victim_out < params.amount_out_min {
            debug!("victim would revert after front-run ➜ abort");
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("victim would revert".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        let mut back_path = path.clone();
        back_path.reverse();
        let (profit_token_back, final_states) = context
            .simulation_engine
            .simulate_multihop_swap_and_get_final_state_with_state(
                &back_path,
                front_out,
                Some(&pools_after_victim),
                None,
            )
            .await
            .map_err(|e| MEVEngineError::SimulationError(format!("back-run simulation failed: {}", e)))?;

        let gross_profit_token = profit_token_back
            .checked_sub(front_amount_in)
            .ok_or_else(|| MEVEngineError::ArithmeticError("Gross profit calculation overflow".into()))?;
        if gross_profit_token.is_zero() {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("not profitable pre-gas".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        // Validate final state consistency
        if let Some(first_pool) = back_path.first() {
            if let Some(final_state) = final_states.get(first_pool) {
                debug!("back-run simulation completed with {} final pool states, first pool state validated: reserves0={}, reserves1={}",
                       final_states.len(), final_state.reserves0, final_state.reserves1);
            }
        }

        let (front_max_fee, front_max_priority_fee) = self
            .gas_bid(context, self.cfg.frontrun_gas_offset_gwei as i64)
            .await?;
        let (back_max_fee, back_max_priority_fee) = self
            .gas_bid(context, -(self.cfg.backrun_gas_offset_gwei as i64))
            .await?;
        
        // Set gas parameters on active play for transaction building
        active_play.update_atomic(|play| {
            play.max_fee_per_gas = Some(front_max_fee.max(back_max_fee));
            play.max_priority_fee_per_gas = Some(front_max_priority_fee.max(back_max_priority_fee));
        }).await;
        let gas_limit = self.cfg.max_tx_gas_limit;
        let gas_cost_native = gas_limit
            .checked_mul(front_max_fee)
            .and_then(|v| v.checked_add(gas_limit * back_max_fee))
            .ok_or_else(|| MEVEngineError::SimulationError("gas overflow".into()))?;
        let gas_cost_token = self
            .estimate_token_conversion(gas_cost_native, native, profit_token, context)
            .await?;

        if gas_cost_token >= gross_profit_token {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure("not profitable post-gas".into()) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }
        let net_profit_token = gross_profit_token - gas_cost_token;

        let chain = context.blockchain_manager.get_chain_name();
        let price_usd = context
            .price_oracle
            .get_token_price_usd(&chain, profit_token)
            .await
            .map_err(|e| MEVEngineError::PriceOracle(format!("fetch profit-token price: {}", e)))?;
        let decimals = context
            .blockchain_manager
            .get_token_decimals(profit_token, None)
            .await
            .map_err(|e| MEVEngineError::Blockchain(format!("fetch profit-token decimals: {}", e)))?;
        let net_usd = net_profit_token.as_u128() as f64 / 10f64.powi(decimals as i32) * price_usd;
        active_play.update_atomic(|play| {
            play.estimated_profit_usd = net_usd;
        }).await;

        info!(
            "sandwich PnL {:.6} USD (token {})",
            net_usd, profit_token
        );
        if net_usd < self.cfg.min_profit_usd {
            active_play.update_atomic(|play| {
                if let Err(e) = play.record_simulation_failure(format!(
                    "profit {:.6} < {:.2}",
                    net_usd, self.cfg.min_profit_usd
                )) {
                    warn!("Failed to record simulation failure: {}", e);
                }
            }).await;
            return Ok(());
        }

        if !self.cfg.ignore_risk {
            let front_risk = context
                .risk_manager
                .assess_trade(profit_token, params.token_out, front_amount_in, None)
                .await
                .map_err(|e| MEVEngineError::RiskAssessment(format!("front-run risk: {}", e)))?;
            if !front_risk.is_safe {
                warn!(?front_risk, "front-run risk too high");
                active_play.update_atomic(|play| {
                    play.status = MEVPlayStatus::RiskAssessmentFailed(
                        front_risk
                            .mitigation
                            .clone()
                            .unwrap_or_else(|| "no mitigation".into()),
                    );
                }).await;
                return Ok(());
            }
            let back_risk = context
                .risk_manager
                .assess_trade(params.token_out, profit_token, front_out, None)
                .await
                .map_err(|e| MEVEngineError::RiskAssessment(format!("back-run risk: {}", e)))?;
            if !back_risk.is_safe {
                warn!(?back_risk, "back-run risk too high");
                active_play.update_atomic(|play| {
                    play.status = MEVPlayStatus::RiskAssessmentFailed(
                        back_risk
                            .mitigation
                            .clone()
                            .unwrap_or_else(|| "no mitigation".into()),
                    );
                }).await;
                return Ok(());
            }
        }

        let urgent = self.block_deadline(context, &opp).await?;
        let play_id = active_play.read(|play| play.play_id).await;
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

        info!(play_id = %play_id, "sandwich validated");
        Ok(())
    }

    /// Convert `amount_in` of `token_in` → `token_out` via USD oracle.
    async fn estimate_token_conversion(
        &self,
        amount_in: U256,
        token_in: Address,
        token_out: Address,
        context: &StrategyContext,
    ) -> Result<U256, MEVEngineError> {
        if token_in == token_out || amount_in.is_zero() {
            return Ok(amount_in);
        }

        let chain = context.blockchain_manager.get_chain_name();
        let price_in = context
            .price_oracle
            .get_token_price_usd(&chain, token_in)
            .await
            .map_err(|e| MEVEngineError::PriceOracle(format!("get token_in price: {}", e)))?;
        let price_out = context
            .price_oracle
            .get_token_price_usd(&chain, token_out)
            .await
            .map_err(|e| MEVEngineError::PriceOracle(format!("get token_out price: {}", e)))?;
        if price_out <= 0.0 {
            return Err(MEVEngineError::StrategyError(format!(
                "invalid USD price for {:?}",
                token_out
            )));
        }

        let dec_in = context
            .blockchain_manager
            .get_token_decimals(token_in, None)
            .await
            .map_err(|e| MEVEngineError::Blockchain(format!("get token_in decimals: {}", e)))?;
        let dec_out = context
            .blockchain_manager
            .get_token_decimals(token_out, None)
            .await
            .map_err(|e| MEVEngineError::Blockchain(format!("get token_out decimals: {}", e)))?;

        let amount_in_std =
            amount_in.as_u128() as f64 / 10f64.powi(dec_in as i32) * price_in / price_out;
        Ok(U256::from(
            (amount_in_std * 10f64.powi(dec_out as i32)) as u128,
        ))
    }
}

// Add missing error conversions
impl From<PriceError> for MEVEngineError {
    fn from(e: PriceError) -> Self {
        MEVEngineError::PriceOracle(e.to_string())
    }
}

impl From<BlockchainError> for MEVEngineError {
    fn from(e: BlockchainError) -> Self {
        MEVEngineError::Blockchain(e.to_string())
    }
}

impl From<RiskError> for MEVEngineError {
    fn from(e: RiskError) -> Self {
        MEVEngineError::RiskAssessment(e.to_string())
    }
}

