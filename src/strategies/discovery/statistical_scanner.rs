use std::sync::Arc;
use tracing::{debug, warn, instrument};
use std::time::Instant;
use std::collections::HashMap;

use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    path::Path,
    types::{ArbitrageRoute, ProtocolType, TokenPairStats},
};

use super::{DiscoveryConfig, DiscoveryScanner, ScannerContext, ScannerStats};

use ethers::types::{BlockId, BlockNumber};

pub struct StatisticalArbitrageScanner {
    context: ScannerContext,
    base_scanner: super::BaseScanner,
    min_scan_amount_wei: ethers::types::U256,
    min_profit_threshold_usd: f64,
    statistical_arb_z_score_threshold: f64,
    statistical_pair_stats: Arc<tokio::sync::RwLock<HashMap<(ethers::types::Address, ethers::types::Address), TokenPairStats>>>,
    strict_mode: bool,
}

impl StatisticalArbitrageScanner {
    pub fn new(
        context: ScannerContext,
        min_scan_amount_wei: ethers::types::U256,
        min_profit_threshold_usd: f64,
        statistical_arb_z_score_threshold: f64,
        statistical_pair_stats: Arc<tokio::sync::RwLock<HashMap<(ethers::types::Address, ethers::types::Address), TokenPairStats>>>,
    ) -> Self {
        let strict_mode = std::env::var("STRICT_BACKTEST_MODE")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        Self {
            context,
            base_scanner: super::BaseScanner::new(),
            min_scan_amount_wei,
            min_profit_threshold_usd,
            statistical_arb_z_score_threshold,
            statistical_pair_stats,
            strict_mode,
        }
    }

    #[instrument(skip(self), fields(scanner = "StatisticalArbitrage"))]
    async fn scan_statistical_arbitrage_opportunities(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        let mut opportunities = Vec::new();
        let config = self.base_scanner.get_config().await;

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
                        warn!(error = ?e, block = ?block, "Failed to get tokens active at block for statistical arbitrage scan");
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
                        return Err(ArbitrageError::Other(format!("Strict mode: Failed to get tokens for statistical arbitrage scan: {}", e)));
                    } else {
                        warn!(error = ?e, "Failed to get tokens for statistical arbitrage scan");
                        return Ok(opportunities);
                    }
                }
            }
        };

        let token_sample = token_addresses.iter().take(config.batch_size.min(50)).collect::<Vec<_>>();

        // Update statistical_pair_stats with fresh historical data for all pairs in token_sample
        let token_refs: Vec<&ethers::types::Address> = token_addresses.iter().collect();
        self.update_all_token_pair_stats(&token_refs, block_number).await?;

        for &token_a in &token_addresses {
            for &token_b in &token_addresses {
                if token_a == token_b { continue; }

                if let Some(stats) = self.get_token_pair_stats(token_a, token_b).await {
                    let volatility_threshold = stats.volatility * 2.0;
                    let cointegration_threshold = config.min_confidence_score.max(0.7);

                    let z_score = match self.calculate_price_z_score(token_a, token_b, block_number).await {
                        Ok(score) => score,
                        Err(e) => {
                            warn!("Failed to calculate z-score for tokens {:?} <-> {:?}: {}", token_a, token_b, e);
                            continue;
                        }
                    };

                    if z_score.abs() > self.statistical_arb_z_score_threshold
                        && stats.volatility > volatility_threshold
                        && stats.cointegration_score > cointegration_threshold
                        && stats.mean_abs_diff > 0.0
                        && stats.mean_sq_diff >= stats.mean_abs_diff.powi(2)
                    {
                        if let Ok(Some(path)) = self.context.path_finder.find_optimal_path(
                            token_a,
                            token_b,
                            self.min_scan_amount_wei,
                            block_number
                        ).await {
                             let profit = path.expected_output.saturating_sub(path.initial_amount_in_wei);
                            if profit > ethers::types::U256::zero() {
                                let profit_usd = match self.calculate_profit_usd(&path, profit, block_number).await {
                                    Ok(v) if v.is_finite() && v > 0.0 => v,
                                    _ => continue,
                                };
                                if profit_usd > self.min_profit_threshold_usd.max(config.min_profit_usd)
                                    && path.price_impact_bps <= config.max_price_impact_bps {
                                    let route = self.path_to_arbitrage_route(path, profit_usd).await?;
                                    opportunities.push(MEVOpportunity::Arbitrage(route));
                                }
                            }
                        }
                    }
                }

                if opportunities.len() >= config.batch_size {
                    break;
                }
            }
            if opportunities.len() >= config.batch_size {
                break;
            }
        }

        debug!(
            opportunities_found = opportunities.len(),
            pairs_analyzed = token_sample.len() * token_sample.len(),
            "Statistical arbitrage scan completed"
        );
        Ok(opportunities)
    }

    /// Update the statistical_pair_stats map for all token pairs in the sample.
    async fn update_all_token_pair_stats(
        &self,
        token_sample: &[&ethers::types::Address],
        block_number: Option<u64>,
    ) -> Result<(), ArbitrageError> {
        // Resolve the current block number if not provided
        let current_block = match block_number {
            Some(block) => block,
            None => self.context.blockchain_manager.get_block_number().await
                .map_err(|e| ArbitrageError::BlockchainQueryFailed(format!("Failed to get block number: {}", e)))?,
        };
        
        let mut stats_map = self.statistical_pair_stats.write().await;
        for &token_a in token_sample {
            for &token_b in token_sample {
                if token_a == token_b {
                    continue;
                }
                let pair_key = (*token_a, *token_b);
                let historical_ratios = self.fetch_historical_price_ratios(*token_a, *token_b, block_number).await?;
                if historical_ratios.len() < 30 {
                    continue;
                }
                let (mean_price, std_dev, volatility) = Self::compute_mean_std_volatility(&historical_ratios);
                let (cointegration_score, mean_abs_diff, mean_sq_diff) = Self::compute_cointegration_score(&historical_ratios);
                let total_trades = historical_ratios.len() as u64;
                let total_volume_usd = historical_ratios.iter().sum::<f64>();
                let avg_trade_size_usd = if total_trades > 0 {
                    total_volume_usd / total_trades as f64
                } else {
                    0.0
                };
                let price_correlation = cointegration_score;
                let last_updated = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let price_momentum = if historical_ratios.len() >= 2 {
                    if let (Some(last), Some(first)) = (historical_ratios.last(), historical_ratios.first()) {
                        last - first
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                let now = std::time::Instant::now();
                let price_observations: Vec<(f64, u64)> = historical_ratios
                    .iter()
                    .enumerate()
                    .map(|(i, &price)| {
                        let block = if i < historical_ratios.len() {
                            let block_offset = i as u64 * 10;
                            if current_block > block_offset {
                                current_block - block_offset
                            } else {
                                0
                            }
                        } else {
                            0
                        };
                        (price, block)
                    })
                    .collect();
                let volume_observations: Vec<(f64, u64)> = historical_ratios
                    .iter()
                    .enumerate()
                    .map(|(i, &volume)| {
                        let block = if i < historical_ratios.len() {
                            let block_offset = i as u64 * 10;
                            if current_block > block_offset {
                                current_block - block_offset
                            } else {
                                0
                            }
                        } else {
                            0
                        };
                        (volume, block)
                    })
                    .collect();
                let stats = TokenPairStats {
                    total_volume_usd,
                    total_trades,
                    avg_trade_size_usd,
                    volatility_24h: volatility,
                    price_correlation,
                    last_updated,
                    volatility,
                    cointegration_score,
                    mean_price,
                    std_dev,
                    price_momentum,
                    price_observations,
                    volume_observations,
                    last_update: now,
                    mean_abs_diff,
                    mean_sq_diff,
                };
                stats_map.insert(pair_key, stats);
            }
        }
        Ok(())
    }

    /// Fetch historical price ratios for a token pair.
    async fn fetch_historical_price_ratios(
        &self,
        token_a: ethers::types::Address,
        token_b: ethers::types::Address,
        block_number: Option<u64>,
    ) -> Result<Vec<f64>, ArbitrageError> {
        let chain_name = self.context.get_chain_name();
        let history_len = 60;
        let mut ratios = Vec::with_capacity(history_len);

        // Get the current block context or use the provided block
        let current_block = if let Some(block) = block_number {
            block
        } else {
            self.context.blockchain_manager.get_current_block_number().await.unwrap_or(0)
        };

        // Calculate historical blocks (going back in time)
        let mut historical_blocks = Vec::with_capacity(history_len);
        for i in 0..history_len {
            let block_offset = i as u64 * 10; // Sample every 10 blocks for efficiency
            if current_block > block_offset {
                historical_blocks.push(current_block - block_offset);
            }
        }

        // Fetch prices for both tokens at each historical block
        let mut price_history_a = Vec::with_capacity(historical_blocks.len());
        let mut price_history_b = Vec::with_capacity(historical_blocks.len());

        for block in &historical_blocks {
            match tokio::try_join!(
                self.context.price_oracle.get_token_price_usd_at(&chain_name, token_a, Some(*block), None),
                self.context.price_oracle.get_token_price_usd_at(&chain_name, token_b, Some(*block), None)
            ) {
                Ok((price_a, price_b)) => {
                    price_history_a.push(price_a);
                    price_history_b.push(price_b);
                }
                Err(e) => {
                    // Log warning but continue with other blocks
                    tracing::warn!("Failed to get prices for block {}: {}", block, e);
                    continue;
                }
            }
        }

        // Calculate price ratios
        let len = price_history_a.len().min(price_history_b.len());
        for i in 0..len {
            let pa = price_history_a[i];
            let pb = price_history_b[i];
            if pa > 0.0 && pb > 0.0 {
                ratios.push(pa / pb);
            }
        }
        Ok(ratios)
    }

    /// Compute mean, std deviation, and volatility for a vector of price ratios.
    fn compute_mean_std_volatility(ratios: &[f64]) -> (f64, f64, f64) {
        let n = ratios.len() as f64;
        if n == 0.0 {
            return (0.0, 0.0, 0.0);
        }
        let mean = ratios.iter().sum::<f64>() / n;
        let std_dev = (ratios.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n).sqrt();
        let mut returns = Vec::with_capacity(ratios.len().saturating_sub(1));
        for i in 1..ratios.len() {
            if ratios[i - 1] > 0.0 {
                returns.push((ratios[i] / ratios[i - 1]).ln());
            }
        }
        let volatility = if !returns.is_empty() {
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            (returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / returns.len() as f64).sqrt()
        } else {
            0.0
        };
        (mean, std_dev, volatility)
    }

    /// Compute a simple cointegration score, mean absolute difference, and mean squared difference for a vector of price ratios.
    fn compute_cointegration_score(ratios: &[f64]) -> (f64, f64, f64) {
        if ratios.len() < 2 {
            return (0.0, 0.0, 0.0);
        }
        let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
        let mut sum_num = 0.0;
        let mut sum_den = 0.0;
        let mut sum_abs_diff = 0.0;
        let mut sum_sq_diff = 0.0;
        for i in 1..ratios.len() {
            let diff = ratios[i] - ratios[i - 1];
            sum_num += (ratios[i] - mean) * (ratios[i - 1] - mean);
            sum_den += (ratios[i - 1] - mean).powi(2);
            // Use diff directly in both accumulations
            sum_abs_diff += diff.abs();
            sum_sq_diff += diff * diff;
            // Prevent unused warning by referencing diff in a no-op if all else fails
            std::hint::black_box(diff);
        }
        let mean_abs_diff = sum_abs_diff / (ratios.len() as f64 - 1.0);
        let mean_sq_diff = sum_sq_diff / (ratios.len() as f64 - 1.0);
        let cointegration_score = if sum_den.abs() < 1e-8 {
            0.0
        } else {
            (sum_num / sum_den).abs().min(1.0)
        };
        (cointegration_score, mean_abs_diff, mean_sq_diff)
    }

    async fn get_token_pair_stats(&self, token_a: ethers::types::Address, token_b: ethers::types::Address) -> Option<TokenPairStats> {
        let stats = self.statistical_pair_stats.read().await;
        stats.get(&(token_a, token_b)).cloned()
    }

    async fn calculate_price_z_score(
        &self,
        token_a: ethers::types::Address,
        token_b: ethers::types::Address,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let chain_name = self.context.get_chain_name();

        let price_a = if let Some(block) = block_number {
            self.context.price_oracle.get_token_price_usd_at(&chain_name, token_a, Some(block), None).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for token A: {}", e)))?
        } else {
            self.context.price_oracle.get_token_price_usd(&chain_name, token_a).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for token A: {}", e)))?
        };

        let price_b = if let Some(block) = block_number {
            self.context.price_oracle.get_token_price_usd_at(&chain_name, token_b, Some(block), None).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for token B: {}", e)))?
        } else {
            self.context.price_oracle.get_token_price_usd(&chain_name, token_b).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for token B: {}", e)))?
        };

        if price_a <= 0.0 || price_b <= 0.0 {
            return Err(ArbitrageError::PriceOracle(
                format!("Invalid prices: token_a={}, token_b={}", price_a, price_b)
            ));
        }

        if let Some(stats) = self.get_token_pair_stats(token_a, token_b).await {
            let current_ratio = price_a / price_b;
            let mean_ratio = stats.mean_price;
            let std_dev = stats.std_dev;

            if std_dev > 0.0 {
                let z_score = (current_ratio - mean_ratio) / std_dev;
                Ok(z_score)
            } else {
                Ok(0.0)
            }
        } else {
            Ok(0.0)
        }
    }

    async fn calculate_profit_usd(
        &self,
        path: &Path,
        profit_amount: ethers::types::U256,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let chain_name = self.context.get_chain_name();

        let profit_token = path.token_out;
        let price = if let Some(block) = block_number {
            self.context.price_oracle.get_token_price_usd_at(&chain_name, profit_token, Some(block), None).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for profit token: {}", e)))?
        } else {
            self.context.price_oracle.get_token_price_usd(&chain_name, profit_token).await
                .map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for profit token: {}", e)))?
        };

        if price <= 0.0 {
            return Err(ArbitrageError::PriceOracle(
                format!("Invalid price for profit token: {}", price)
            ));
        }

        let block_id = block_number.map(|b| BlockId::Number(BlockNumber::Number(b.into())));
        let decimals = self.context.blockchain_manager.get_token_decimals(profit_token, block_id).await
            .map_err(|e| ArbitrageError::BlockchainQueryFailed(format!("Failed to get token decimals: {}", e)))?;

        // Safer unit normalization using shared helper
        let profit_tokens = crate::types::normalize_units(profit_amount, decimals);
        let profit_usd = profit_tokens * price;

        if !profit_usd.is_finite() || profit_usd < 0.0 {
            return Err(ArbitrageError::PriceOracle(
                format!("Invalid profit calculation: {}", profit_usd)
            ));
        }

        tracing::debug!(
            profit_tokens = profit_tokens,
            price_usd = price,
            profit_usd = profit_usd,
            "Profit calculation details"
        );
        Ok(profit_usd)
    }

    async fn path_to_arbitrage_route(
        &self,
        path: Path,
        profit_usd: f64,
    ) -> Result<ArbitrageRoute, ArbitrageError> {
        let legs = path.legs.into_iter().map(|leg| {
            let dex_name = self.get_dex_name_from_protocol(&leg.protocol_type);

            crate::types::ArbitrageRouteLeg {
                dex_name,
                protocol_type: leg.protocol_type,
                pool_address: leg.pool_address,
                token_in: leg.from_token,
                token_out: leg.to_token,
                amount_in: leg.amount_in,
                min_amount_out: leg.amount_out
                    .checked_mul(95.into())
                    .and_then(|v| v.checked_div(100.into()))
                    .unwrap_or(leg.amount_out),
                fee_bps: leg.fee,
                expected_out: Some(leg.amount_out),
            }
        }).collect();

        let route = ArbitrageRoute {
            legs,
            initial_amount_in_wei: path.initial_amount_in_wei,
            amount_out_wei: path.expected_output,
            profit_usd,
            estimated_gas_cost_wei: path.estimated_gas_cost_wei,
            price_impact_bps: path.price_impact_bps,
            route_description: format!("Statistical arbitrage: {} -> {}", path.token_in, path.token_out),
            risk_score: 0.0,
                                chain_id: self.context.blockchain_manager.get_chain_id(),
            created_at_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            mev_risk_score: 0.0,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: path.token_in,
            token_out: path.token_out,
            amount_in: path.initial_amount_in_wei,
            amount_out: path.expected_output,
        };

        Ok(route)
    }

    fn get_dex_name_from_protocol(&self, protocol_type: &ProtocolType) -> String {
        match protocol_type {
            ProtocolType::UniswapV2 => "Uniswap V2".to_string(),
            ProtocolType::UniswapV3 => "Uniswap V3".to_string(),
            ProtocolType::UniswapV4 => "Uniswap V4".to_string(),
            ProtocolType::SushiSwap => "SushiSwap".to_string(),
            ProtocolType::Curve => "Curve".to_string(),
            ProtocolType::Balancer => "Balancer".to_string(),
            ProtocolType::Unknown => "Unknown DEX".to_string(),
            ProtocolType::Other(name) => format!("DEX({})", name),
        }
    }
}

#[async_trait::async_trait]
impl DiscoveryScanner for StatisticalArbitrageScanner {
    fn name(&self) -> &'static str {
        "StatisticalArbitrage"
    }

    #[instrument(skip(self), fields(scanner = "StatisticalArbitrage"))]
    async fn initialize(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        self.base_scanner.set_config(config).await;
        self.base_scanner.start().await;
        debug!("StatisticalArbitrageScanner initialized");
        Ok(())
    }

    #[instrument(skip(self), fields(scanner = "StatisticalArbitrage"))]
    async fn shutdown(&self) -> Result<(), ArbitrageError> {
        self.base_scanner.stop().await;
        debug!("StatisticalArbitrageScanner shutdown");
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

    #[instrument(skip(self), fields(scanner = "StatisticalArbitrage"))]
    async fn scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        if !self.base_scanner.is_running().await {
            return Ok(Vec::new());
        }

        let start_time = Instant::now();

        let opportunities = match self.scan_statistical_arbitrage_opportunities(block_number).await {
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
            scanner = "StatisticalArbitrage",
            opportunities_found = opportunities.len(),
            scan_duration_ms = start_time.elapsed().as_millis(),
            "Scan completed"
        );

        Ok(opportunities)
    }
}

impl StatisticalArbitrageScanner {
    pub async fn run_scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        self.scan(block_number).await
    }
}