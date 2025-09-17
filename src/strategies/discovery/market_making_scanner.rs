use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    path::Path,
    types::ArbitrageRoute,
};
use tokio::sync::{broadcast, RwLock};
use super::{DiscoveryScanner, ScannerContext, DiscoveryConfig};

pub struct MarketMakingScanner {
    context: ScannerContext,
    base_scanner: super::BaseScanner,
    opportunity_sender: Option<broadcast::Sender<MEVOpportunity>>,
    min_scan_amount_wei: ethers::types::U256,
    min_profit_threshold_usd: f64,
    config: RwLock<DiscoveryConfig>,
    strict_mode: bool,
}

impl MarketMakingScanner {
    pub fn new(
        context: ScannerContext,
        min_scan_amount_wei: ethers::types::U256,
        min_profit_threshold_usd: f64,
    ) -> Self {
        let strict_mode = std::env::var("STRICT_BACKTEST_MODE")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let default_config = DiscoveryConfig {
            min_profit_usd: min_profit_threshold_usd,
            max_gas_cost_usd: 50.0,
            min_liquidity_usd: 10000.0,
            max_price_impact_bps: 500,
            scan_interval_ms: 1000,
            batch_size: 20,
            max_hops: 4,
            timeout_ms: 30000,
            enable_statistical_filtering: false,
            min_confidence_score: 0.5,
        };

        Self {
            context,
            base_scanner: super::BaseScanner::new(),
            opportunity_sender: None,
            min_scan_amount_wei,
            min_profit_threshold_usd,
            config: RwLock::new(default_config),
            strict_mode,
        }
    }

    async fn scan_market_making_opportunities(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        let mut opportunities = Vec::new();
        let config = self.config.read().await;
        let batch_size = config.batch_size;
        let min_profit_usd = config.min_profit_usd;
        let max_price_impact_bps = config.max_price_impact_bps;
        let max_gas_cost_usd = config.max_gas_cost_usd;
        let min_liquidity_usd = config.min_liquidity_usd;
        drop(config);

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
                        return Err(ArbitrageError::TokenRegistry(format!("Failed to get tokens active at block {} for market making scan: {}", block, e)));
                    }
                }
            }
        } else {
            // Fallback to all tokens if no block number is provided
            match self.context.chain_infra.token_registry.get_all_tokens().await {
                Ok(tokens) => tokens.into_iter().map(|t| t.address).collect(),
                Err(e) => {
                    if self.strict_mode {
                        return Err(ArbitrageError::Other(format!("Strict mode: Failed to get tokens for market making scan: {}", e)));
                    } else {
                        return Err(ArbitrageError::TokenRegistry(format!("Failed to get tokens for market making scan: {}", e)));
                    }
                }
            }
        };

        let stable_addresses: Vec<_> = token_addresses.iter().take(batch_size).collect();

        for &token_address in &stable_addresses {
            let optimal_path = self.context.path_finder.find_optimal_path(
                *token_address,
                *token_address,
                self.min_scan_amount_wei,
                block_number,
            ).await;

            if let Ok(Some(path)) = optimal_path {
                if path.price_impact_bps > max_price_impact_bps {
                    continue;
                }

                let profit = path.expected_output.checked_sub(path.initial_amount_in_wei)
                    .unwrap_or_else(|| ethers::types::U256::zero());
                
                if profit > ethers::types::U256::zero() {
                    // Strictly require valid price and decimals; skip on any error
                    let profit_usd = match self.calculate_profit_usd(&path, profit, block_number).await {
                        Ok(p) if p.is_finite() && p > 0.0 => p,
                        _ => {
                            tracing::warn!("skipping pair due to invalid profit_usd computation");
                            continue;
                        }
                    };
                    let gas_cost_usd = match self.calculate_gas_cost_usd(&path, block_number).await {
                        Ok(g) if g.is_finite() && g >= 0.0 => g,
                        _ => {
                            tracing::warn!("skipping pair due to invalid gas_cost_usd computation");
                            continue;
                        }
                    };
                    let net_profit_usd = profit_usd - gas_cost_usd;
                    
                    if net_profit_usd > min_profit_usd && gas_cost_usd <= max_gas_cost_usd {
                        let liquidity_usd = match self.calculate_liquidity_usd(&path, block_number).await {
                            Ok(l) if l.is_finite() && l >= 0.0 => l,
                            _ => {
                                tracing::warn!("skipping pair due to invalid liquidity_usd computation");
                                continue;
                            }
                        };
                        if liquidity_usd >= min_liquidity_usd {
                            let route = self.path_to_arbitrage_route(path, net_profit_usd, block_number).await?;
                            opportunities.push(MEVOpportunity::Arbitrage(route));
                        }
                    }
                }
            }
        }

        Ok(opportunities)
    }

    async fn calculate_profit_usd(
        &self,
        path: &Path,
        profit_amount: ethers::types::U256,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let profit_token = path.token_out;
        let chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();
        let price = if let Some(block) = block_number {
            self.context.price_oracle.get_token_price_usd_at(
                chain_name,
                profit_token,
                Some(block),
                None
            ).await
        } else {
            self.context.price_oracle.get_token_price_usd(
                chain_name,
                profit_token
            ).await
        }.map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get price for profit token: {}", e)))?;

        let profit_wei = profit_amount.as_u128() as f64;
        let profit_usd = price * profit_wei / 1e18;
        Ok(profit_usd)
    }

    async fn calculate_gas_cost_usd(
        &self,
        path: &Path,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();
        
        // Prefer path estimate; otherwise compute dynamic estimate based on hops and current gas
        let gas_cost_wei = if path.estimated_gas_cost_wei.is_zero() {
            let base_gas: u64 = 21_000;
            let per_swap_gas: u64 = 150_000;
            let num_swaps = path.legs.len() as u64;
            let total_units = base_gas.saturating_add(per_swap_gas.saturating_mul(num_swaps));
            let total_units_with_buffer = total_units.saturating_mul(120).saturating_div(100);
            let gas_price = self.context.chain_infra.gas_oracle
                .get_gas_price(
                    self.context.chain_infra.blockchain_manager.get_chain_name(),
                    block_number
                )
                .await
                .unwrap_or_else(|_| crate::gas_oracle::GasPrice { base_fee: 20_000_000_000u64.into(), priority_fee: 0u64.into() });
            ethers::types::U256::from(total_units_with_buffer)
                .saturating_mul(gas_price.effective_price())
        } else {
            path.estimated_gas_cost_wei
        };
        let weth_address = self.context.chain_infra.blockchain_manager.get_weth_address();
        let eth_price = if let Some(block) = block_number {
            self.context.price_oracle.get_token_price_usd_at(
                chain_name,
                weth_address,
                Some(block),
                None
            ).await
        } else {
            self.context.price_oracle.get_token_price_usd(
                chain_name,
                weth_address
            ).await
        }.map_err(|e| ArbitrageError::PriceOracle(format!("Failed to get ETH price: {}", e)))?;

        let gas_cost_eth = gas_cost_wei.as_u128() as f64 / 1e18;
        let gas_cost_usd = gas_cost_eth * eth_price;
        Ok(gas_cost_usd)
    }

    async fn calculate_liquidity_usd(
        &self,
        path: &Path,
        block_number: Option<u64>,
    ) -> Result<f64, ArbitrageError> {
        let mut total_liquidity_usd = f64::MAX;
        let chain_name = self.context.chain_infra.blockchain_manager.get_chain_name();

        for leg in &path.legs {
            let pool_state = self.context.pool_state_oracle.get_pool_state(
                chain_name,
                leg.pool_address,
                block_number
            ).await.map_err(|e| ArbitrageError::Other(format!("Failed to get pool state: {}", e)))?;

            let token0_price = if let Some(block) = block_number {
                self.context.price_oracle.get_token_price_usd_at(
                    chain_name,
                    pool_state.pool_state.token0,
                    Some(block),
                    None
                ).await
            } else {
                self.context.price_oracle.get_token_price_usd(
                    chain_name,
                    pool_state.pool_state.token0
                ).await
            }.unwrap_or(0.0);

            let token1_price = if let Some(block) = block_number {
                self.context.price_oracle.get_token_price_usd_at(
                    chain_name,
                    pool_state.pool_state.token1,
                    Some(block),
                    None
                ).await
            } else {
                self.context.price_oracle.get_token_price_usd(
                    chain_name,
                    pool_state.pool_state.token1
                ).await
            }.unwrap_or(0.0);

            // Use token-specific decimals for accurate liquidity calculation
            let token0_decimals = pool_state.pool_state.token0_decimals;
            let token1_decimals = pool_state.pool_state.token1_decimals;
            
            let token0_liquidity_usd = (pool_state.pool_state.reserve0.as_u128() as f64 / 10f64.powi(token0_decimals as i32)) * token0_price;
            let token1_liquidity_usd = (pool_state.pool_state.reserve1.as_u128() as f64 / 10f64.powi(token1_decimals as i32)) * token1_price;
            let pool_liquidity_usd = token0_liquidity_usd + token1_liquidity_usd;

            total_liquidity_usd = total_liquidity_usd.min(pool_liquidity_usd);
        }

        Ok(total_liquidity_usd)
    }

    async fn path_to_arbitrage_route(
        &self,
        path: Path,
        profit_usd: f64,
        block_number: Option<u64>,
    ) -> Result<ArbitrageRoute, ArbitrageError> {
        let legs = path.legs.into_iter().map(|leg| crate::types::ArbitrageRouteLeg {
            dex_name: Self::normalize_dex_name(leg.protocol_type.clone()),
            protocol_type: leg.protocol_type,
            pool_address: leg.pool_address,
            token_in: leg.from_token,
            token_out: leg.to_token,
            amount_in: leg.amount_in,
            min_amount_out: leg.amount_out.saturating_mul(ethers::types::U256::from(95)).checked_div(ethers::types::U256::from(100)).unwrap_or(leg.amount_out),
            fee_bps: leg.fee,
            expected_out: Some(leg.amount_out),
        }).collect();

        let route = ArbitrageRoute {
            legs,
            initial_amount_in_wei: path.initial_amount_in_wei,
            amount_out_wei: path.expected_output,
            profit_usd,
            estimated_gas_cost_wei: path.estimated_gas_cost_wei,
            price_impact_bps: path.price_impact_bps,
            route_description: format!("Market making: {} -> {}", path.token_in, path.token_out),
            risk_score: 0.0,
            chain_id: self.context.chain_infra.blockchain_manager.get_chain_id(),
            created_at_ns: if let Some(block) = block_number {
                if let Ok(block_data) = self.context.chain_infra.blockchain_manager
                    .get_block(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(block.into())))
                    .await
                {
                    if let Some(b) = block_data {
                        b.timestamp.as_u64() * 1_000_000_000
                    } else {
                        block * 12 * 1_000_000_000 + 1609459200_000_000_000
                    }
                } else {
                    block * 12 * 1_000_000_000 + 1609459200_000_000_000
                }
            } else {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64
            },
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
}

#[async_trait::async_trait]
impl DiscoveryScanner for MarketMakingScanner {
    fn name(&self) -> &'static str {
        "market_making"
    }

    async fn initialize(&self, config: super::DiscoveryConfig) -> Result<(), ArbitrageError> {
        let mut stored_config = self.config.write().await;
        *stored_config = config;
        drop(stored_config);
        
        self.base_scanner.start().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ArbitrageError> {
        self.base_scanner.stop().await;
        Ok(())
    }

    async fn is_running(&self) -> bool {
        self.base_scanner.is_running().await
    }

    async fn get_stats(&self) -> super::ScannerStats {
        self.base_scanner.get_stats().await
    }

    async fn scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        self.run_scan(block_number).await
    }

    async fn get_config(&self) -> DiscoveryConfig {
        self.config.read().await.clone()
    }
}

impl MarketMakingScanner {
    pub async fn run_scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        if !self.base_scanner.is_running().await {
            return Ok(Vec::new());
        }

        let start_time = std::time::Instant::now();

        let opportunities = self.scan_market_making_opportunities(block_number).await?;

        self.base_scanner.update_stats(|stats| {
            stats.opportunities_found += opportunities.len() as u64;
            stats.last_scan_time = Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs());
            stats.avg_scan_duration_ms = start_time.elapsed().as_millis() as f64;
        }).await;

        Ok(opportunities)
    }

    /// Normalize DEX name for consistent display
    fn normalize_dex_name(protocol_type: crate::types::ProtocolType) -> String {
        match protocol_type {
            crate::types::ProtocolType::UniswapV2 => "Uniswap V2".to_string(),
            crate::types::ProtocolType::UniswapV3 => "Uniswap V3".to_string(),
            crate::types::ProtocolType::UniswapV4 => "Uniswap V4".to_string(),
            crate::types::ProtocolType::SushiSwap => "SushiSwap".to_string(),
            crate::types::ProtocolType::Curve => "Curve".to_string(),
            crate::types::ProtocolType::Balancer => "Balancer".to_string(),
            crate::types::ProtocolType::Unknown => "DEX".to_string(),
            crate::types::ProtocolType::Other(name) => name,
        }
    }
}