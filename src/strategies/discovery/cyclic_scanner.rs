use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
};

use super::{DiscoveryScanner, ScannerContext};

pub struct CyclicArbitrageScanner {
    context: ScannerContext,
    base_scanner: super::BaseScanner,
    min_scan_amount_wei: ethers::types::U256,
    min_profit_threshold_usd: f64,
    strict_mode: bool,
}

impl CyclicArbitrageScanner {
    pub fn new(
        context: ScannerContext,
        min_scan_amount_wei: ethers::types::U256,
        min_profit_threshold_usd: f64,
    ) -> Self {
        let strict_mode = std::env::var("STRICT_BACKTEST_MODE")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        Self {
            context,
            base_scanner: super::BaseScanner::new(),
            min_scan_amount_wei,
            min_profit_threshold_usd,
            strict_mode,
        }
    }

    async fn scan_cyclic_opportunities(
        &self,
        block_number: Option<u64>,
    ) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        let mut opportunities = Vec::new();

        // Get tokens that are active at the current block to avoid time-traveling
        let token_addresses = if let Some(block) = block_number {
            let chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();
            match self.context.chain_infra.pool_manager.get_tokens_active_at_block(&chain_name, block).await {
                Ok(addresses) => {
                    if self.strict_mode && addresses.is_empty() {
                        return Err(ArbitrageError::Other(format!("Strict mode: No tokens active at block {} on chain {}", block, chain_name)));
                    }
                    addresses
                },
                Err(e) => {
                    if self.strict_mode {
                        return Err(ArbitrageError::Other(format!("Strict mode: Failed to get tokens active at block {}: {}", block, e)));
                    } else {
                        tracing::warn!(error = ?e, block = ?block, "Failed to get tokens active at block for cyclic scan");
                        return Ok(opportunities);
                    }
                }
            }
        } else {
            // Fallback to all tokens if no block number is provided
            match self.context.chain_infra.token_registry.get_all_tokens().await {
                Ok(tokens) => tokens.into_iter().map(|t| t.address).collect(),
                Err(e) => {
                    if self.strict_mode {
                        return Err(ArbitrageError::Other(format!("Strict mode: Failed to get tokens for cyclic scan: {}", e)));
                    } else {
                        tracing::warn!(error = ?e, "Failed to get tokens for cyclic scan");
                        return Ok(opportunities);
                    }
                }
            }
        };

        let chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();

        // Use the PathFinder to find all negative cycles (cyclic arbitrage opportunities)
        let path_finder = &self.context.path_finder;

        let mut eligible_tokens = Vec::new();
        for token_address in &token_addresses {
            let token_info = match self.context.chain_infra.token_registry.get_token(*token_address).await {
                Ok(Some(info)) => info,
                Ok(None) => {
                    if self.strict_mode {
                        return Err(ArbitrageError::Other(format!("Strict mode: Token info not found for token: {:?}", token_address)));
                    } else {
                        tracing::debug!("Token info not found for token: {:?}", token_address);
                        continue;
                    }
                }
                Err(e) => {
                    if self.strict_mode {
                        return Err(ArbitrageError::Other(format!("Strict mode: Error fetching token info for {:?}: {:?}", token_address, e)));
                    } else {
                        tracing::debug!("Error fetching token info for {:?}: {:?}", token_address, e);
                        continue;
                    }
                }
            };
            if token_info.decimals == 0 {
                if self.strict_mode {
                    return Err(ArbitrageError::Other(format!("Strict mode: Token with 0 decimals: {:?}", token_address)));
                } else {
                    tracing::debug!("Skipping token with 0 decimals: {:?}", token_address);
                    continue;
                }
            }
            eligible_tokens.push(token_info);
        }
        tracing::debug!("Eligible tokens for cyclic scan: {:?}", eligible_tokens);

        // Deduplicate by pool-set + direction using a hash set
        use std::collections::HashSet;
        let mut seen_cycles: HashSet<String> = HashSet::new();
        for start_token in &eligible_tokens {
            let scan_amount = self.min_scan_amount_wei;
            tracing::debug!("Scanning for cyclic paths from token: {:?}, scan_amount: {}", start_token, scan_amount);

            let paths = match path_finder
                .find_circular_arbitrage_paths(
                    start_token.address,
                    scan_amount,
                    block_number,
                )
                .await
            {
                Ok(paths) => {
                    tracing::debug!("Found {} cyclic paths for token {:?}", paths.len(), start_token);
                    paths
                }
                Err(e) => {
                    tracing::debug!("PathFinder failed for token {:?}: {:?}", start_token, e);
                    continue;
                }
            };

            for path in paths {
                // Canonicalize the cycle by rotating to lexicographically smallest tuple
                let canonical_key = Self::canonicalize_cycle_key(&path);
                if !seen_cycles.insert(canonical_key) {
                    tracing::debug!("duplicate cycle encountered, skipping");
                    continue;
                }
                tracing::debug!("Evaluating path: {:?}", path);

                let profit_wei = path.expected_output.saturating_sub(scan_amount);
                tracing::debug!("Profit in wei: {}", profit_wei);

                if profit_wei.is_zero() {
                    tracing::debug!("Profit is zero for path, skipping.");
                    continue;
                }

                let profit_usd = match self
                    .calculate_profit_usd_direct(start_token.address, profit_wei, block_number)
                    .await
                {
                    Ok(p) => {
                        tracing::debug!("Profit in USD: {}", p);
                        p
                    }
                    Err(e) => {
                        tracing::debug!("Failed to calculate profit in USD for path: {:?}", e);
                        continue;
                    }
                };

                if profit_usd < self.min_profit_threshold_usd {
                    tracing::debug!(
                        "Profit below threshold ({} < {}), skipping.",
                        profit_usd,
                        self.min_profit_threshold_usd
                    );
                    continue;
                }

                let mut legs = Vec::new();
                for leg in path.legs.iter() {
                    let min_amount_out = leg.amount_out
                        .saturating_mul(95.into())
                        .checked_div(100.into())
                        .unwrap_or(leg.amount_out);
                    tracing::debug!(
                        "Leg: protocol={:?}, pool={:?}, from={:?}, to={:?}, amount_in={}, amount_out={}, min_amount_out={}",
                        leg.protocol_type, leg.pool_address, leg.from_token, leg.to_token, leg.amount_in, leg.amount_out, min_amount_out
                    );
                    legs.push(crate::types::ArbitrageRouteLeg {
                        dex_name: leg.protocol_type.to_string(),
                        protocol_type: leg.protocol_type.clone(),
                        pool_address: leg.pool_address,
                        token_in: leg.from_token,
                        token_out: leg.to_token,
                        amount_in: leg.amount_in,
                        min_amount_out,
                        fee_bps: leg.fee,
                        expected_out: Some(leg.amount_out),
                    });
                }

                let price_impact_bps = self
                    .calculate_total_price_impact(
                        &legs
                            .iter()
                            .map(|l| l.amount_in)
                            .collect::<Vec<ethers::types::U256>>(),
                    )
                    .await;
                tracing::debug!("Calculated price impact (bps): {}", price_impact_bps);

                let route_description = {
                    let mut desc = String::from("Cyclic: ");
                    for (i, leg) in legs.iter().enumerate() {
                        let symbol = self.format_token_symbol(leg.token_in).await;
                        desc.push_str(&symbol);
                        if i < legs.len() - 1 {
                            desc.push_str(" -> ");
                        }
                    }
                    // Append the final token_out of the last leg to complete the cycle
                    desc.push_str(" -> ");
                    let last_symbol = match legs.last() {
                        Some(last_leg) => self.format_token_symbol(last_leg.token_out).await,
                        None => {
                            tracing::warn!("No legs found in arbitrage path");
                            "UNKNOWN".to_string()
                        }
                    };
                    desc.push_str(&last_symbol);
                    desc
                };
                tracing::debug!("Route description: {}", route_description);

                // Estimate gas cost using chain gas oracle for current block context
                let gas_price = match self
                    .context
                    .chain_infra
                    .gas_oracle
                    .get_gas_price(&chain_name, block_number)
                    .await
                {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!("failed to fetch gas price: {}", e);
                        continue;
                    }
                };
                let effective_gas_price = gas_price.base_fee.saturating_add(gas_price.priority_fee);
                // Dynamic gas estimation: base + per-swap with 20% buffer
                let base_gas: u64 = 21_000;
                let per_swap_gas: u64 = 150_000;
                let num_swaps = path.legs.len() as u64;
                let total_units = base_gas.saturating_add(per_swap_gas.saturating_mul(num_swaps));
                let total_units_with_buffer = total_units.saturating_mul(120).saturating_div(100);
                let estimated_gas_cost_wei = ethers::types::U256::from(total_units_with_buffer)
                    .saturating_mul(effective_gas_price);
                tracing::debug!("Estimated gas cost (wei): {}", estimated_gas_cost_wei);

                let route = crate::types::ArbitrageRoute {
                    legs,
                    initial_amount_in_wei: scan_amount,
                    amount_out_wei: path.expected_output,
                    profit_usd,
                    estimated_gas_cost_wei,
                    price_impact_bps,
                    route_description,
                    risk_score: 0.2,
                    chain_id: self.context.blockchain_manager.get_chain_id(),
                    created_at_ns: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64,
                    mev_risk_score: 0.3,
                    flashloan_amount_wei: Some(scan_amount),
                    flashloan_fee_wei: Some(
                        scan_amount
                            .saturating_mul(9.into())
                            .checked_div(10000.into())
                            .unwrap_or_default(),
                    ),
                    token_in: start_token.address,
                    token_out: start_token.address,
                    amount_in: scan_amount,
                    amount_out: path.expected_output,
                };

                tracing::debug!("Pushing MEVOpportunity::Arbitrage: {:?}", route);

                opportunities.push(MEVOpportunity::Arbitrage(route));

                if opportunities.len() >= 10 {
                    tracing::debug!("Reached max opportunity count (10), breaking.");
                    break;
                }
            }

            if opportunities.len() >= 10 {
                tracing::debug!("Reached max opportunity count (10) for eligible tokens, breaking.");
                break;
            }
        }

        tracing::debug!(
            "Found {} cyclic arbitrage opportunities using PathFinder",
            opportunities.len()
        );
        Ok(opportunities)
    }

    async fn calculate_profit_usd_direct(
        &self,
        profit_token: ethers::types::Address,
        profit_amount: ethers::types::U256,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let chain_name = self.context.blockchain_manager.get_chain_name();
        tracing::debug!(
            "Calculating profit in USD for token: {:?}, amount: {}, block_number: {:?}",
            profit_token,
            profit_amount,
            block_number
        );

        let price = match block_number {
            Some(block) => {
                tracing::debug!("Fetching historical price for token {:?} at block {}", profit_token, block);
                self.context
                    .price_oracle
                    .get_token_price_usd_at(&chain_name, profit_token, Some(block), None)
                    .await
            }
            None => {
                tracing::debug!("Fetching current price for token {:?}", profit_token);
                self.context
                    .price_oracle
                    .get_token_price_usd(&chain_name, profit_token)
                    .await
            }
        }
        .map_err(|e| {
            tracing::debug!("Failed to get token price: {:?}", e);
            ArbitrageError::PriceOracle(format!("Failed to get token price: {}", e))
        })?;

        let token_info = self
            .context
            .chain_infra
            .token_registry
            .get_token(profit_token)
            .await
            .map_err(|e| {
                tracing::debug!("Failed to get token info: {:?}", e);
                ArbitrageError::TokenRegistry(format!("Failed to get token info: {}", e))
            })?
            .ok_or_else(|| {
                tracing::debug!("Token not found in registry: {:?}", profit_token);
                ArbitrageError::TokenRegistry(format!("Token not found: {:?}", profit_token))
            })?;

        let decimals_factor = 10f64.powi(token_info.decimals as i32);
        let profit_usd = price * profit_amount.as_u128() as f64 / decimals_factor;

        tracing::debug!(
            "Calculated profit_usd: {} (price: {}, amount: {}, decimals: {})",
            profit_usd,
            price,
            profit_amount,
            token_info.decimals
        );

        Ok(profit_usd)
    }

    async fn calculate_total_price_impact(&self, amounts: &[ethers::types::U256]) -> u32 {
        let max_amount = amounts.iter().cloned().max().unwrap_or_else(ethers::types::U256::zero);
        tracing::debug!("Calculating price impact for amounts: {:?}, max_amount: {}", amounts, max_amount);

        if max_amount > ethers::types::U256::exp10(20) {
            tracing::debug!("Price impact: 500 bps");
            500
        } else if max_amount > ethers::types::U256::exp10(19) {
            tracing::debug!("Price impact: 200 bps");
            200
        } else {
            tracing::debug!("Price impact: 50 bps");
            50
        }
    }

    async fn format_token_symbol(&self, token: ethers::types::Address) -> String {
        match self
            .context
            .chain_infra
            .token_registry
            .get_token(token)
            .await
        {
            Ok(Some(info)) => {
                tracing::debug!("Token symbol for {:?}: {}", token, info.symbol);
                info.symbol
            }
            Ok(None) => {
                tracing::debug!("Token info not found for {:?}, using address as symbol", token);
                format!("{:?}", token)
            }
            Err(e) => {
                tracing::debug!("Error fetching token info for {:?}: {:?}", token, e);
                format!("{:?}", token)
            }
        }
    }

    /// Canonicalize a cycle path by rotating to start with the lexicographically smallest tuple
    /// This ensures cycles like A->B->C->A and B->C->A->B are treated as the same cycle
    fn canonicalize_cycle_key(path: &crate::path::Path) -> String {
        if path.legs.is_empty() {
            return String::new();
        }

        // Create tuples of (pool, from_token, to_token) for each leg
        let tuples: Vec<(String, String, String)> = path.legs.iter()
            .map(|leg| (
                format!("{:?}", leg.pool_address),
                format!("{:?}", leg.from_token),
                format!("{:?}", leg.to_token)
            ))
            .collect();

        // Find the lexicographically smallest tuple and its position
        let mut min_tuple = &tuples[0];
        let mut min_index = 0;

        for (i, tuple) in tuples.iter().enumerate() {
            if tuple < min_tuple {
                min_tuple = tuple;
                min_index = i;
            }
        }

        // Rotate the tuples so the smallest one is first
        let rotated_tuples = tuples.iter()
            .cycle()
            .skip(min_index)
            .take(tuples.len())
            .cloned()
            .collect::<Vec<_>>();

        // Create the canonical key
        let mut key = String::new();
        for (pool, from, to) in rotated_tuples {
            key.push_str(&format!("{}>{}>{}-", pool, from, to));
        }

        key
    }
}

#[async_trait::async_trait]
impl DiscoveryScanner for CyclicArbitrageScanner {
    fn name(&self) -> &'static str {
        "CyclicArbitrageScanner"
    }

    async fn initialize(&self, config: super::DiscoveryConfig) -> Result<(), ArbitrageError> {
        tracing::debug!("Initializing CyclicArbitrageScanner with config: {:?}", config);
        self.base_scanner.set_config(config).await;
        self.base_scanner.start().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ArbitrageError> {
        tracing::debug!("Shutting down CyclicArbitrageScanner");
        self.base_scanner.stop().await;
        Ok(())
    }

    async fn is_running(&self) -> bool {
        let running = self.base_scanner.is_running().await;
        tracing::debug!("CyclicArbitrageScanner is_running: {}", running);
        running
    }

    async fn get_stats(&self) -> super::ScannerStats {
        let stats = self.base_scanner.get_stats().await;
        tracing::debug!("CyclicArbitrageScanner stats: {:?}", stats);
        stats
    }

    async fn get_config(&self) -> super::DiscoveryConfig {
        let config = self.base_scanner.get_config().await;
        tracing::debug!("CyclicArbitrageScanner config: {:?}", config);
        config
    }

    #[tracing::instrument(skip(self), level = "debug", fields(block_number = ?block_number))]
    async fn scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        if !self.base_scanner.is_running().await {
            tracing::debug!("CyclicArbitrageScanner not running, returning empty opportunities.");
            return Ok(Vec::new());
        }

        let start_time = std::time::Instant::now();
        tracing::debug!("Starting scan at {:?}", start_time);

        let opportunities = self.scan_cyclic_opportunities(block_number).await?;

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
                tracing::debug!(
                    "Updated stats: opportunities_found={}, last_scan_time={:?}, avg_scan_duration_ms={}",
                    stats.opportunities_found,
                    stats.last_scan_time,
                    stats.avg_scan_duration_ms
                );
            })
            .await;

        tracing::debug!("Scan complete. Found {} opportunities.", opportunities.len());
        Ok(opportunities)
    }
}