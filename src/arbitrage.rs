//! High‑performance arbitrage engine
//!
//! • fully concurrent, event‑driven opportunity discovery  
//! • deterministic, block‑pinned back‑testing support  
//! • profit evaluation uses *token‑aware* decimals (batch RPC)  
//! • Flashbots‑first execution with gas/ETH‑USD simulation guardrails

use crate::path::Path;

use async_trait::async_trait;
use ethers::types::{Address, H256, U256};
use tracing::{instrument, debug, error, info, warn};

use crate::{
    config::{ExecutionMode},
    errors::ArbitrageError,
    types::{
        ArbitrageEngine, ArbitrageOpportunity, ArbitrageRoute, ExecutionResult,
        ExecutionStatus, RouteInfo,
    },
};

#[async_trait]
pub trait ArbitrageEngineTrait: Send + Sync {
    async fn execute_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<ExecutionResult, ArbitrageError>;
}

#[async_trait]
impl ArbitrageEngineTrait for ArbitrageEngine {
    #[instrument(skip(self, opportunity))]
    async fn execute_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<ExecutionResult, ArbitrageError> {
        debug!("Starting execution of opportunity with ID: {} for block_number: {}", opportunity.id(), opportunity.block_number);
        let transaction_optimizer = self.get_transaction_optimizer().await?;
        debug!("Transaction optimizer acquired for opportunity ID: {}", opportunity.id());

        let (gas_used_est, gas_cost_usd) = transaction_optimizer
            .simulate_trade_submission(&opportunity.route, opportunity.block_number)
            .await
            .map_err(|e| {
                error!("Simulation failed for opportunity ID: {} with error: {}", opportunity.id(), e);
                ArbitrageError::Submission(format!("simulation: {e}"))
            })?;
        debug!("Simulation completed for opportunity ID: {}. Gas used estimate: {}, Gas cost USD: {}", opportunity.id(), gas_used_est, gas_cost_usd);

        if gas_cost_usd >= opportunity.profit_usd {
            warn!("Opportunity ID: {} is unprofitable. Gas cost ({}) exceeds profit ({}).", opportunity.id(), gas_cost_usd, opportunity.profit_usd);
            return Ok(unprofitable_result(opportunity, gas_used_est, gas_cost_usd));
        }

        let is_paper_trading = matches!(self.full_config.mode, ExecutionMode::PaperTrading);
        debug!("Execution mode for opportunity ID: {} is {}", opportunity.id(), if is_paper_trading { "PaperTrading" } else { "Live" });

        let tx_hash = if is_paper_trading {
            transaction_optimizer
                .paper_trade(
                    &opportunity.id(),
                    &opportunity.route,
                    opportunity.block_number,
                    opportunity.profit_usd,
                    gas_cost_usd,
                )
                .await
                .map_err(|e| {
                    error!("Paper trade failed for opportunity ID: {} with error: {}", opportunity.id(), e);
                    ArbitrageError::Submission(format!("paper_trade: {e}"))
                })?
        } else {
            let wallet = transaction_optimizer.get_wallet_address();
            debug!("Wallet address for live trade: {:?}", wallet);
            let tx_req = opportunity.route.to_tx(wallet);
            transaction_optimizer
                .auto_optimize_and_submit(tx_req, Some(true))
                .await
                .map_err(|e| {
                    error!("Submission failed for opportunity ID: {} with error: {}", opportunity.id(), e);
                    ArbitrageError::Submission(format!("submit: {e}"))
                })?
        };
        info!("Opportunity ID: {} executed successfully, tx_hash: {:?}", opportunity.id(), tx_hash);

        let predicted_profit = (opportunity.profit_usd - gas_cost_usd).max(0.0);

        Ok(ExecutionResult {
            tx_hash,
            status: ExecutionStatus::Submitted,
            gas_used: Some(gas_used_est),
            effective_gas_price: None,
            block_number: Some(opportunity.block_number),
            execution_time_ms: 0,
            route_info: Some(RouteInfo {
                description: opportunity.route.route_description.clone(),
                profit_usd: opportunity.profit_usd, // Gross profit before gas costs
            }),
            error: None,
            predicted_profit_usd: predicted_profit,
            prediction_id: H256::zero(),
            success: true,
            actual_profit_usd: None,
            failure_reason: None,
        })
    }
}

// ══════════════════════════════════════════════════════════════════════════
// idiomatic conversion – Path → ArbitrageRoute
// ══════════════════════════════════════════════════════════════════════════
impl ArbitrageRoute {
    pub fn from_path(p: &Path, chain_id: u64) -> Self {
        debug!("Converting Path to ArbitrageRoute. Initial amount in wei: {}, Expected output: {}, Chain ID: {}", p.initial_amount_in_wei, p.expected_output, chain_id);
        let legs: Vec<crate::types::ArbitrageRouteLeg> = p.legs.iter().map(|leg| {
            debug!("Creating ArbitrageRouteLeg for pool: {:?}, tokens: {:?} -> {:?}, amount_in: {}, expected_out: {}", leg.pool_address, leg.from_token, leg.to_token, leg.amount_in, leg.amount_out);
            let default_slippage_bps: u32 = 50; // TODO: from config
            let min_out = if leg.amount_out.is_zero() {
                U256::zero()
            } else {
                leg.amount_out
                    .saturating_mul(U256::from(10_000 - default_slippage_bps))
                    / U256::from(10_000)
            };

            crate::types::ArbitrageRouteLeg {
                dex_name: "Unknown".to_string(),
                protocol_type: leg.protocol_type.clone(),
                pool_address: leg.pool_address,
                token_in: leg.from_token,
                token_out: leg.to_token,
                amount_in: leg.amount_in,
                min_amount_out: min_out,
                fee_bps: leg.fee,
                expected_out: Some(leg.amount_out),
            }
        }).collect();

        let created_at_ns_u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let created_at_ns = u64::try_from(created_at_ns_u128).unwrap_or(u64::MAX);

        debug!("ArbitrageRoute created with {} legs.", legs.len());
        Self {
            legs,
            initial_amount_in_wei: p.initial_amount_in_wei,
            amount_out_wei: p.expected_output,
            profit_usd: 0.0, // Will be calculated later
            estimated_gas_cost_wei: p.estimated_gas_cost_wei,
            price_impact_bps: p.price_impact_bps,
            route_description: format!("{} → {}", p.token_path.first().unwrap_or(&Address::zero()), p.token_path.last().unwrap_or(&Address::zero())),
            risk_score: 0.0,
            chain_id,
            created_at_ns,
            mev_risk_score: 0.0,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: *p.token_path.first().unwrap_or(&Address::zero()),
            token_out: *p.token_path.last().unwrap_or(&Address::zero()),
            amount_in: p.initial_amount_in_wei,
            amount_out: p.expected_output,
        }
    }
}

// Backward compatibility
impl From<&Path> for ArbitrageRoute {
    fn from(p: &Path) -> Self {
        Self::from_path(p, 1) // Default to Ethereum mainnet
    }
}

// ══════════════════════════════════════════════════════════════════════════
// helpers
// ══════════════════════════════════════════════════════════════════════════
fn unprofitable_result(
    opp:            &ArbitrageOpportunity,
    gas_estimate:   U256,
    gas_cost_usd:   f64,
) -> ExecutionResult {
    debug!("Creating unprofitable result for opportunity ID: {} at block_number: {}. Gas estimate: {}, Gas cost USD: {}", opp.id(), opp.block_number, gas_estimate, gas_cost_usd);
    let predicted = (opp.profit_usd - gas_cost_usd).max(0.0);
    ExecutionResult {
        tx_hash:              H256::zero(),
        status:               ExecutionStatus::Failed,
        gas_used:             Some(gas_estimate),
        effective_gas_price:  None,
        block_number:         Some(opp.block_number),
        execution_time_ms:    0,
        route_info:           Some(RouteInfo {
            description: opp.route.route_description.clone(),
            profit_usd: opp.profit_usd, // Gross profit before gas costs
        }),
        error:                Some("gas exceeds profit".to_string()),
        predicted_profit_usd: predicted,
        prediction_id:        H256::zero(),
        success:              false,
        actual_profit_usd:    None,
        failure_reason:       Some("Unprofitable".to_string()),
    }
}