use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, instrument};
use std::time::Instant;

use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    types::ArbitrageOpportunity,
};

use super::{DiscoveryConfig, DiscoveryScanner, ScannerContext, ScannerStats};

pub struct CrossChainArbitrageScanner {
    context: ScannerContext,
    base_scanner: super::BaseScanner,
    min_scan_amount_wei: ethers::types::U256,
    min_profit_threshold_usd: f64,
    cross_chain_pathfinder: Arc<crate::cross::CrossChainPathfinder>,
    opportunity_cache: Arc<RwLock<HashMap<String, ArbitrageOpportunity>>>,
}

impl CrossChainArbitrageScanner {
    pub fn new(
        context: ScannerContext,
        min_scan_amount_wei: ethers::types::U256,
        min_profit_threshold_usd: f64,
        cross_chain_pathfinder: Arc<crate::cross::CrossChainPathfinder>,
        opportunity_cache: Arc<RwLock<HashMap<String, ArbitrageOpportunity>>>,
    ) -> Self {
        Self {
            context,
            base_scanner: super::BaseScanner::new(),
            min_scan_amount_wei,
            min_profit_threshold_usd,
            cross_chain_pathfinder,
            opportunity_cache,
        }
    }

    async fn scan_cross_chain_opportunities(
        &self,
        block_number: Option<u64>,
    ) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        let mut opportunities = Vec::new();

        let token_in = self.context.chain_infra.blockchain_manager.get_weth_address();
        let min_scan_amount_wei = self.min_scan_amount_wei;
        let _min_profit_threshold_usd = self.min_profit_threshold_usd;

        let cross_chain_paths = self
            .cross_chain_pathfinder
            .find_routes(token_in, min_scan_amount_wei, block_number)
            .await
            .map_err(ArbitrageError::CrossChain)?;

        for execution_plan in cross_chain_paths {
            let source_amount = execution_plan
                .source_swap
                .as_ref()
                .and_then(|s| s.amounts.get(0).copied())
                .unwrap_or_else(|| ethers::types::U256::zero());

            let bridge_amount = execution_plan.bridge_quote.amount_out;

            let dest_amount = execution_plan
                .dest_swap
                .as_ref()
                .and_then(|s| s.amounts.last().copied())
                .unwrap_or_else(|| ethers::types::U256::zero());

            let source_chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();
            let dest_chain_name = match self.context.chain_infra.gas_oracle.get_chain_name_from_id(execution_plan.bridge_quote.dest_chain_id) {
                Some(name) => name,
                None => format!("chain_{}", execution_plan.bridge_quote.dest_chain_id),
            };

            let from_token = execution_plan.bridge_quote.from_token;
            let to_token = execution_plan.bridge_quote.to_token;

            let source_token_price = match block_number {
                Some(block) => match self
                    .context
                    .price_oracle
                    .get_token_price_usd_at(source_chain_name, from_token, Some(block), None)
                    .await
                {
                    Ok(p) if p > 0.0 => p,
                    _ => {
                        tracing::warn!("invalid or missing source token price for {:?} on {}", from_token, source_chain_name);
                        continue;
                    }
                },
                None => match self
                    .context
                    .price_oracle
                    .get_token_price_usd(source_chain_name, from_token)
                    .await
                {
                    Ok(p) if p > 0.0 => p,
                    _ => {
                        tracing::warn!("invalid or missing source token price for {:?} on {}", from_token, source_chain_name);
                        continue;
                    }
                },
            };

            let dest_token_price = match block_number {
                Some(block) => match self
                    .context
                    .price_oracle
                    .get_token_price_usd_at(&dest_chain_name, to_token, Some(block), None)
                    .await
                {
                    Ok(p) if p > 0.0 => p,
                    _ => {
                        tracing::warn!("invalid or missing dest token price for {:?} on {}", to_token, dest_chain_name);
                        continue;
                    }
                },
                None => match self
                    .context
                    .price_oracle
                    .get_token_price_usd(&dest_chain_name, to_token)
                    .await
                {
                    Ok(p) if p > 0.0 => p,
                    _ => {
                        tracing::warn!("invalid or missing dest token price for {:?} on {}", to_token, dest_chain_name);
                        continue;
                    }
                },
            };

            let bridge_fee_wei = bridge_amount
                .checked_sub(source_amount)
                .unwrap_or_else(ethers::types::U256::zero);

            // Fetch token decimals from chain; skip opportunity if unavailable
            let block_id = block_number.map(|b| ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(b.into())));
            let source_token_decimals = match self
                .context
                .blockchain_manager
                .get_token_decimals(from_token, block_id)
                .await
            {
                Ok(d) if d > 0 => d,
                _ => {
                    tracing::warn!("missing source token decimals for {:?}", from_token);
                    continue;
                }
            } as i32;
            let dest_token_decimals = match self
                .context
                .blockchain_manager
                .get_token_decimals(to_token, block_id)
                .await
            {
                Ok(d) if d > 0 => d,
                _ => {
                    tracing::warn!("missing dest token decimals for {:?}", to_token);
                    continue;
                }
            } as i32;

            let bridge_fee_usd = (bridge_fee_wei.as_u128() as f64 / 10f64.powi(source_token_decimals)) * source_token_price;

            let dest_amount_usd = (dest_amount.as_u128() as f64 / 10f64.powi(dest_token_decimals)) * dest_token_price;
            let source_amount_usd = (source_amount.as_u128() as f64 / 10f64.powi(source_token_decimals)) * source_token_price;
            let _price_disparity_ratio = if source_amount_usd > 0.0 {
                dest_amount_usd / source_amount_usd
            } else {
                1.0
            };

            let bridge_fee_impact_on_profitability = bridge_fee_usd / source_amount_usd.max(0.01);
            let _min_profitability_threshold = 0.05 + bridge_fee_impact_on_profitability;

            // Check bridge liquidity
            let bridge_liquidity_sufficient = self.check_bridge_liquidity(
                &execution_plan.bridge_quote,
                source_amount,
                dest_amount,
            ).await?;

            if !bridge_liquidity_sufficient {
                tracing::debug!(
                    bridge = %execution_plan.bridge_quote.bridge_name,
                    source = %source_chain_name,
                    dest = %dest_chain_name,
                    source_amount = %source_amount,
                    "Skipping cross-chain plan due to insufficient bridge liquidity"
                );
                continue;
            }

            // Find reverse route for complete arbitrage cycle
            let reverse_execution_plan = match self.find_reverse_route(
                &execution_plan.bridge_quote,
                dest_amount,
                source_amount,
            ).await {
                Ok(plan) => plan,
                Err(e) => {
                    tracing::debug!(
                        bridge = %execution_plan.bridge_quote.bridge_name,
                        source = %source_chain_name,
                        dest = %dest_chain_name,
                        error = %e,
                        "Skipping cross-chain plan due to missing reverse route"
                    );
                    continue;
                }
            };

            // Calculate complete arbitrage profitability
            let reverse_source_amount = reverse_execution_plan
                .source_swap
                .as_ref()
                .and_then(|s| s.amounts.get(0).copied())
                .unwrap_or_else(|| ethers::types::U256::zero());

            let reverse_bridge_amount = reverse_execution_plan.bridge_quote.amount_out;
            let _reverse_dest_amount = reverse_execution_plan
                .dest_swap
                .as_ref()
                .and_then(|s| s.amounts.last().copied())
                .unwrap_or_else(|| ethers::types::U256::zero());

            // Calculate total arbitrage profit (forward - reverse costs)
            let total_bridge_fee_wei = bridge_fee_wei.saturating_add(
                reverse_bridge_amount.saturating_sub(reverse_source_amount)
            );

            let _total_source_amount_usd = (source_amount.as_u128() as f64 / 10f64.powi(source_token_decimals)) * source_token_price;
            let total_bridge_fee_usd = (total_bridge_fee_wei.as_u128() as f64 / 10f64.powi(source_token_decimals)) * source_token_price;

            let net_profit_usd = dest_amount_usd - source_amount_usd - total_bridge_fee_usd;

            // Apply minimum profit threshold for cross-chain arbitrage
            if net_profit_usd < self.min_profit_threshold_usd {
                tracing::debug!(
                    bridge = %execution_plan.bridge_quote.bridge_name,
                    net_profit_usd = %net_profit_usd,
                    threshold = %self.min_profit_threshold_usd,
                    "Skipping cross-chain plan due to insufficient net profit"
                );
                continue;
            }

            // Create cross-chain arbitrage route
            let route = self.create_cross_chain_arbitrage_route(
                &execution_plan,
                &reverse_execution_plan,
                net_profit_usd,
                total_bridge_fee_wei,
            ).await?;

            tracing::info!(
                bridge = %execution_plan.bridge_quote.bridge_name,
                net_profit_usd = %net_profit_usd,
                source_chain = %source_chain_name,
                dest_chain = %dest_chain_name,
                "Found viable cross-chain arbitrage opportunity"
            );

            opportunities.push(MEVOpportunity::Arbitrage(route));
        }

        debug!(
            opportunities_found = opportunities.len(),
            "Cross-chain arbitrage scan completed"
        );

        Ok(opportunities)
    }

    /// Create a cross-chain arbitrage route from execution plans
    async fn create_cross_chain_arbitrage_route(
        &self,
        forward_plan: &crate::types::CrossChainExecutionPlan,
        reverse_plan: &crate::types::CrossChainExecutionPlan,
        net_profit_usd: f64,
        total_bridge_fee_wei: ethers::types::U256,
    ) -> Result<crate::types::ArbitrageRoute, ArbitrageError> {
        let mut legs = Vec::new();

        // Add forward swap legs (source chain)
        if let Some(source_swap) = &forward_plan.source_swap {
            for (i, (amount_in, amount_out)) in source_swap.amounts.iter().zip(source_swap.amounts.iter().skip(1)).enumerate() {
                if i < source_swap.path.len().saturating_sub(1) {
                    legs.push(crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossChainSource".to_string(),
                        protocol_type: crate::types::ProtocolType::Other("CrossChain".to_string()),
                        pool_address: source_swap.path[i], // Use pool address from path
                        token_in: source_swap.path[i], // This should be the token address
                        token_out: source_swap.path[i + 1],
                        amount_in: *amount_in,
                        min_amount_out: amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(*amount_out),
                        fee_bps: 30, // Default cross-chain fee
                        expected_out: Some(*amount_out),
                    });
                }
            }
        }

        // Add bridge leg
        legs.push(crate::types::ArbitrageRouteLeg {
            dex_name: forward_plan.bridge_quote.bridge_name.clone(),
            protocol_type: crate::types::ProtocolType::Other("Bridge".to_string()),
            pool_address: ethers::types::Address::zero(), // Bridge contracts don't have pools
            token_in: forward_plan.bridge_quote.from_token,
            token_out: forward_plan.bridge_quote.to_token,
            amount_in: forward_plan.bridge_quote.amount_in,
            min_amount_out: forward_plan.bridge_quote.amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(forward_plan.bridge_quote.amount_out),
            fee_bps: 0, // Bridge fees are separate
            expected_out: Some(forward_plan.bridge_quote.amount_out),
        });

        // Add destination swap legs (destination chain)
        if let Some(dest_swap) = &forward_plan.dest_swap {
            for (i, (amount_in, amount_out)) in dest_swap.amounts.iter().zip(dest_swap.amounts.iter().skip(1)).enumerate() {
                if i < dest_swap.path.len().saturating_sub(1) {
                    legs.push(crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossChainDest".to_string(),
                        protocol_type: crate::types::ProtocolType::Other("CrossChain".to_string()),
                        pool_address: dest_swap.path[i],
                        token_in: dest_swap.path[i],
                        token_out: dest_swap.path[i + 1],
                        amount_in: *amount_in,
                        min_amount_out: amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(*amount_out),
                        fee_bps: 30, // Default cross-chain fee
                        expected_out: Some(*amount_out),
                    });
                }
            }
        }

        // Add reverse legs (return trip)
        if let Some(reverse_source_swap) = &reverse_plan.source_swap {
            for (i, (amount_in, amount_out)) in reverse_source_swap.amounts.iter().zip(reverse_source_swap.amounts.iter().skip(1)).enumerate() {
                if i < reverse_source_swap.path.len().saturating_sub(1) {
                    legs.push(crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossChainReturn".to_string(),
                        protocol_type: crate::types::ProtocolType::Other("CrossChain".to_string()),
                        pool_address: reverse_source_swap.path[i],
                        token_in: reverse_source_swap.path[i],
                        token_out: reverse_source_swap.path[i + 1],
                        amount_in: *amount_in,
                        min_amount_out: amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(*amount_out),
                        fee_bps: 30,
                        expected_out: Some(*amount_out),
                    });
                }
            }
        }

        // Add reverse bridge leg
        legs.push(crate::types::ArbitrageRouteLeg {
            dex_name: reverse_plan.bridge_quote.bridge_name.clone(),
            protocol_type: crate::types::ProtocolType::Other("Bridge".to_string()),
            pool_address: ethers::types::Address::zero(),
            token_in: reverse_plan.bridge_quote.from_token,
            token_out: reverse_plan.bridge_quote.to_token,
            amount_in: reverse_plan.bridge_quote.amount_in,
            min_amount_out: reverse_plan.bridge_quote.amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(reverse_plan.bridge_quote.amount_out),
            fee_bps: 0,
            expected_out: Some(reverse_plan.bridge_quote.amount_out),
        });

        if let Some(reverse_dest_swap) = &reverse_plan.dest_swap {
            for (i, (amount_in, amount_out)) in reverse_dest_swap.amounts.iter().zip(reverse_dest_swap.amounts.iter().skip(1)).enumerate() {
                if i < reverse_dest_swap.path.len().saturating_sub(1) {
                    legs.push(crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossChainReturn".to_string(),
                        protocol_type: crate::types::ProtocolType::Other("CrossChain".to_string()),
                        pool_address: reverse_dest_swap.path[i],
                        token_in: reverse_dest_swap.path[i],
                        token_out: reverse_dest_swap.path[i + 1],
                        amount_in: *amount_in,
                        min_amount_out: amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(*amount_out),
                        fee_bps: 30,
                        expected_out: Some(*amount_out),
                    });
                }
            }
        }

        let initial_amount = forward_plan.bridge_quote.amount_in;
        let final_amount = reverse_plan.bridge_quote.amount_out;

        // Estimate gas costs for cross-chain transaction
        let estimated_gas_cost_wei = self.estimate_cross_chain_gas_cost(&legs).await;

        let route = crate::types::ArbitrageRoute {
            legs,
            initial_amount_in_wei: initial_amount,
            amount_out_wei: final_amount,
            profit_usd: net_profit_usd,
            estimated_gas_cost_wei,
            price_impact_bps: 100, // Higher impact for cross-chain
            route_description: format!(
                "Cross-chain arbitrage: {} -> {} via {}",
                forward_plan.bridge_quote.bridge_name,
                reverse_plan.bridge_quote.bridge_name,
                forward_plan.bridge_quote.bridge_name
            ),
            risk_score: 0.4, // Higher risk for cross-chain
            chain_id: self.context.blockchain_manager.get_chain_id(),
            created_at_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            mev_risk_score: 0.5, // Higher MEV risk for cross-chain
            flashloan_amount_wei: Some(initial_amount),
            flashloan_fee_wei: Some(total_bridge_fee_wei),
            token_in: forward_plan.bridge_quote.from_token,
            token_out: reverse_plan.bridge_quote.to_token,
            amount_in: initial_amount,
            amount_out: final_amount,
        };

        Ok(route)
    }

    /// Estimate gas costs for cross-chain arbitrage
    async fn estimate_cross_chain_gas_cost(&self, legs: &[crate::types::ArbitrageRouteLeg]) -> ethers::types::U256 {
        // Base gas for cross-chain transaction
        let base_gas = 500_000u64; // Higher base gas for cross-chain
        let per_swap_gas = 200_000u64; // Higher per swap gas
        let bridge_gas = 300_000u64; // Additional gas for bridge operations

        let swap_count = legs.iter()
            .filter(|leg| leg.protocol_type != crate::types::ProtocolType::Other("Bridge".to_string()))
            .count() as u64;

        let bridge_count = legs.iter()
            .filter(|leg| leg.protocol_type == crate::types::ProtocolType::Other("Bridge".to_string()))
            .count() as u64;

        let total_gas = base_gas
            .saturating_add(per_swap_gas.saturating_mul(swap_count))
            .saturating_add(bridge_gas.saturating_mul(bridge_count));

        // Get current gas price
        let chain_name = self.context.get_chain_name();
        let gas_price = match self.context.chain_infra.gas_oracle.get_gas_price(&chain_name, None).await {
            Ok(price) => price.effective_price(),
            Err(_) => ethers::types::U256::from(50_000_000_000u64), // 50 gwei fallback
        };

        ethers::types::U256::from(total_gas).saturating_mul(gas_price)
    }

    async fn get_cached_opportunity(
        &self,
        opportunity_id: &str,
    ) -> Option<ArbitrageOpportunity> {
        let cache = self.opportunity_cache.read().await;
        cache.get(opportunity_id).cloned()
    }

    /// Check if bridge has sufficient liquidity for the given amounts
    async fn check_bridge_liquidity(
        &self,
        bridge_quote: &crate::types::BridgeQuote,
        source_amount: ethers::types::U256,
        dest_amount: ethers::types::U256,
    ) -> Result<bool, ArbitrageError> {
        // In a real implementation, you would:
        // 1. Query the bridge contract for available liquidity
        // 2. Check against minimum reserve requirements
        // 3. Consider pending transactions that might affect liquidity

        // For now, implement a basic check using estimated liquidity
        // This should be replaced with actual bridge contract calls

        // Get bridge liquidity from the quote (if available)
        // For now, use a reasonable default based on the bridge quote amount
        // In a real implementation, this would be fetched from the bridge contract
        let bridge_liquidity = bridge_quote.amount_in.saturating_mul(ethers::types::U256::from(1000)); // Assume 1000x the transaction amount

        // Check if bridge has enough liquidity for both directions
        let forward_liquidity_required = source_amount;
        let reverse_liquidity_required = dest_amount;

        let forward_sufficient = bridge_liquidity >= forward_liquidity_required;
        let reverse_sufficient = bridge_liquidity >= reverse_liquidity_required;

        if forward_sufficient && reverse_sufficient {
            tracing::debug!(
                bridge = %bridge_quote.bridge_name,
                liquidity = %bridge_liquidity,
                forward_required = %forward_liquidity_required,
                reverse_required = %reverse_liquidity_required,
                "Bridge liquidity check passed"
            );
            Ok(true)
        } else {
            tracing::debug!(
                bridge = %bridge_quote.bridge_name,
                liquidity = %bridge_liquidity,
                forward_required = %forward_liquidity_required,
                reverse_required = %reverse_liquidity_required,
                forward_sufficient = %forward_sufficient,
                reverse_sufficient = %reverse_sufficient,
                "Bridge liquidity check failed"
            );
            Ok(false)
        }
    }

    /// Find reverse route for complete arbitrage cycle
    async fn find_reverse_route(
        &self,
        original_bridge_quote: &crate::types::BridgeQuote,
        dest_amount: ethers::types::U256,
        target_return_amount: ethers::types::U256,
    ) -> Result<crate::types::CrossChainExecutionPlan, ArbitrageError> {
        // Create reverse bridge quote (swap source and destination)
        let reverse_bridge_quote = crate::types::BridgeQuote {
            bridge_name: original_bridge_quote.bridge_name.clone(),
            from_token: original_bridge_quote.to_token,
            to_token: original_bridge_quote.from_token,
            dest_chain_id: original_bridge_quote.source_chain_id,
            amount_in: dest_amount,
            amount_out: target_return_amount,
            fee: ethers::types::U256::zero(),
            estimated_gas: ethers::types::U256::zero(),
            bridge_specific_data: Default::default(),
            source_chain_id: original_bridge_quote.source_chain_id,
            fee_usd: None,
        };

        // Find reverse execution plan
        let reverse_execution_plans = self
            .cross_chain_pathfinder
            .find_routes(reverse_bridge_quote.from_token, dest_amount, None)
            .await
            .map_err(ArbitrageError::CrossChain)?;

        // Find the plan that gives us the best return
        let mut best_plan: Option<crate::types::CrossChainExecutionPlan> = None;
        let mut best_return_amount = ethers::types::U256::zero();

        for plan in reverse_execution_plans {
            let return_amount = plan
                .dest_swap
                .as_ref()
                .and_then(|s| s.amounts.last().copied())
                .unwrap_or_else(|| ethers::types::U256::zero());

            if return_amount > best_return_amount {
                best_return_amount = return_amount;
                best_plan = Some(plan);
            }
        }

        match best_plan {
            Some(plan) => {
                tracing::debug!(
                    bridge = %original_bridge_quote.bridge_name,
                    dest_amount = %dest_amount,
                    best_return = %best_return_amount,
                    target = %target_return_amount,
                    "Found reverse route"
                );
                Ok(plan)
            }
            None => {
                Err(ArbitrageError::CrossChain(crate::errors::CrossChainError::Other(
                    "No suitable reverse route found".to_string()
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl DiscoveryScanner for CrossChainArbitrageScanner {
    fn name(&self) -> &'static str {
        "CrossChainArbitrage"
    }

    async fn initialize(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        self.base_scanner.set_config(config).await;
        self.base_scanner.start().await;
        debug!("CrossChainArbitrageScanner initialized");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ArbitrageError> {
        self.base_scanner.stop().await;
        debug!("CrossChainArbitrageScanner shutdown");
        Ok(())
    }

    async fn is_running(&self) -> bool {
        self.base_scanner.is_running().await
    }

    async fn get_stats(&self) -> ScannerStats {
        self.base_scanner.get_stats().await
    }

    async fn get_config(&self) -> DiscoveryConfig {
        self.base_scanner.get_config().await
    }

    #[instrument(skip(self), fields(scanner = "CrossChainArbitrage"))]
    async fn scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        if !self.base_scanner.is_running().await {
            return Ok(Vec::new());
        }

        let start_time = Instant::now();

        let opportunities = match self.scan_cross_chain_opportunities(block_number).await {
            Ok(opps) => {
                let scan_duration = start_time.elapsed().as_millis() as f64;
                self.base_scanner.record_scan_result(opps.len(), scan_duration).await;
                opps
            }
            Err(e) => {
                self.base_scanner.record_error().await;
                return Err(e);
            }
        };

        debug!(
            scanner = "CrossChainArbitrage",
            opportunities_found = opportunities.len(),
            scan_duration_ms = start_time.elapsed().as_millis(),
            "Scan completed"
        );

        Ok(opportunities)
    }
}

impl CrossChainArbitrageScanner {
    pub async fn run_scan(
        &self,
        block_number: Option<u64>,
    ) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        if !self.base_scanner.is_running().await {
            return Ok(Vec::new());
        }

        let start_time = std::time::Instant::now();

        let opportunities = self.scan_cross_chain_opportunities(block_number).await?;

        self.base_scanner
            .update_stats(|stats| {
                stats.opportunities_found += opportunities.len() as u64;
                stats.last_scan_time = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );
                stats.avg_scan_duration_ms = start_time.elapsed().as_millis() as f64;
            })
            .await;

        Ok(opportunities)
    }
}