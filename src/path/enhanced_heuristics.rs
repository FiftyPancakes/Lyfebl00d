use ethers::types::Address;
use std::collections::HashMap;
use std::sync::Arc;
use crate::types::ProtocolType;

/// Enhanced heuristic calculator for A* pathfinding
/// Combines multiple signals for more accurate path estimation
pub struct EnhancedHeuristics {
    /// Historical profitability by pool
    pool_profitability: Arc<dashmap::DashMap<Address, f64>>,
    /// Gas cost predictions by protocol
    protocol_gas_costs: HashMap<ProtocolType, u64>,
    /// MEV resistance scores by pool
    mev_resistance: Arc<dashmap::DashMap<Address, f64>>,
    /// Liquidity depth scores
    liquidity_scores: Arc<dashmap::DashMap<Address, f64>>,
    /// Cross-pool correlation matrix
    pool_correlations: Arc<dashmap::DashMap<(Address, Address), f64>>,
    /// Market volatility factor
    volatility_factor: Arc<tokio::sync::RwLock<f64>>,
    /// Time-weighted average prices
    twap_cache: Arc<dashmap::DashMap<(Address, Address), f64>>,
}

impl EnhancedHeuristics {
    pub fn new() -> Self {
        Self {
            pool_profitability: Arc::new(dashmap::DashMap::new()),
            protocol_gas_costs: Self::initialize_gas_costs(),
            mev_resistance: Arc::new(dashmap::DashMap::new()),
            liquidity_scores: Arc::new(dashmap::DashMap::new()),
            pool_correlations: Arc::new(dashmap::DashMap::new()),
            volatility_factor: Arc::new(tokio::sync::RwLock::new(1.0)),
            twap_cache: Arc::new(dashmap::DashMap::new()),
        }
    }

    fn initialize_gas_costs() -> HashMap<ProtocolType, u64> {
        let mut costs = HashMap::new();
        costs.insert(ProtocolType::UniswapV2, 120_000);
        costs.insert(ProtocolType::UniswapV3, 180_000);
        costs.insert(ProtocolType::SushiSwap, 125_000);
        costs.insert(ProtocolType::Curve, 250_000);
        costs.insert(ProtocolType::Balancer, 200_000);
        costs.insert(ProtocolType::Other("PancakeSwap".to_string()), 120_000);
        costs.insert(ProtocolType::Other("DODO".to_string()), 160_000);
        costs.insert(ProtocolType::Other("Bancor".to_string()), 180_000);
        costs.insert(ProtocolType::Other("Kyber".to_string()), 140_000);
        costs.insert(ProtocolType::Other("ZeroEx".to_string()), 150_000);
        costs
    }

    /// Calculate sophisticated heuristic combining multiple factors
    pub async fn calculate_advanced_heuristic(
        &self,
        current_token: Address,
        target_token: Address,
        current_amount_native: f64,
        expected_output_native: f64,
        pools_in_path: &[Address],
        depth: u8,
        strategy: &crate::path::types::SearchStrategy,
    ) -> f64 {
        let base_profit = expected_output_native - current_amount_native;
        
        // Liquidity-adjusted profit
        let liquidity_factor = self.calculate_liquidity_factor(pools_in_path).await;
        let adjusted_profit = base_profit * liquidity_factor;
        
        // MEV resistance factor (higher is better)
        let mev_score = self.calculate_mev_resistance_score(pools_in_path).await;
        
        // Historical success rate of pools
        let historical_score = self.calculate_historical_score(pools_in_path).await;
        
        // Correlation penalty (avoid highly correlated pools)
        let correlation_penalty = self.calculate_correlation_penalty(pools_in_path).await;
        
        // Market volatility adjustment
        let volatility = *self.volatility_factor.read().await;
        let volatility_adjustment = 1.0 / (1.0 + volatility * 0.1);
        
        // Path complexity penalty
        let complexity_penalty = (depth as f64) * 0.05;
        
        // Strategy-specific weighting
        let (profit_weight, mev_weight, liquidity_weight) = match strategy {
            crate::path::types::SearchStrategy::HighLiquidity => (0.3, 0.2, 0.5),
            crate::path::types::SearchStrategy::LowFee => (0.5, 0.2, 0.3),
            crate::path::types::SearchStrategy::ShortPath => (0.6, 0.1, 0.3),
            crate::path::types::SearchStrategy::Balanced => (0.4, 0.3, 0.3),
        };
        
        // Token pair familiarity adjustment using TWAP cache when available
        let pair_adjustment = if let Some(ratio) = self.get_twap_ratio(current_token, target_token).await {
            // Prefer pairs where current to target conversion is historically favorable
            // Normalize around 1.0 so that extreme ratios contribute proportionally
            (ratio.max(0.1)).ln()
        } else {
            0.0
        };

        // Combine all factors
        let combined_score =
            adjusted_profit * profit_weight +
            mev_score * mev_weight * 100.0 +
            historical_score * liquidity_weight * 50.0 -
            correlation_penalty * 10.0 -
            complexity_penalty * 20.0 +
            pair_adjustment * 5.0;
        
        // Apply volatility adjustment
        let final_score = combined_score * volatility_adjustment;
        
        // A* minimizes, so return negative for better paths
        -final_score
    }

    async fn calculate_liquidity_factor(&self, pools: &[Address]) -> f64 {
        if pools.is_empty() {
            return 1.0;
        }
        
        let mut total_score = 0.0;
        for pool in pools {
            if let Some(score) = self.liquidity_scores.get(pool) {
                total_score += *score;
            } else {
                total_score += 0.5; // Default medium liquidity
            }
        }
        
        total_score / pools.len() as f64
    }

    async fn calculate_mev_resistance_score(&self, pools: &[Address]) -> f64 {
        if pools.is_empty() {
            return 0.5;
        }
        
        let mut total_score = 0.0;
        for pool in pools {
            if let Some(score) = self.mev_resistance.get(pool) {
                total_score += *score;
            } else {
                total_score += 0.3; // Default low resistance
            }
        }
        
        total_score / pools.len() as f64
    }

    async fn calculate_historical_score(&self, pools: &[Address]) -> f64 {
        if pools.is_empty() {
            return 0.5;
        }
        
        let mut total_score = 0.0;
        for pool in pools {
            if let Some(score) = self.pool_profitability.get(pool) {
                total_score += (*score).max(0.0).min(1.0);
            } else {
                total_score += 0.5; // Unknown pool
            }
        }
        
        total_score / pools.len() as f64
    }

    async fn calculate_correlation_penalty(&self, pools: &[Address]) -> f64 {
        if pools.len() < 2 {
            return 0.0;
        }
        
        let mut total_correlation = 0.0;
        let mut pairs_checked = 0;
        
        for i in 0..pools.len() - 1 {
            for j in i + 1..pools.len() {
                let key = if pools[i] < pools[j] {
                    (pools[i], pools[j])
                } else {
                    (pools[j], pools[i])
                };
                
                if let Some(correlation) = self.pool_correlations.get(&key) {
                    total_correlation += (*correlation).abs();
                    pairs_checked += 1;
                }
            }
        }
        
        if pairs_checked > 0 {
            total_correlation / pairs_checked as f64
        } else {
            0.0
        }
    }

    /// Update historical profitability for a pool
    pub async fn update_pool_profitability(
        &self,
        pool: Address,
        profit_ratio: f64,
    ) {
        let mut entry = self.pool_profitability.entry(pool).or_insert(0.5);
        // Exponential moving average
        *entry = *entry * 0.9 + profit_ratio * 0.1;
    }

    /// Update MEV resistance score based on sandwich attack success rate
    pub async fn update_mev_resistance(
        &self,
        pool: Address,
        resistance_score: f64,
    ) {
        self.mev_resistance.insert(pool, resistance_score.max(0.0).min(1.0));
    }

    /// Update liquidity score based on depth and volume
    pub async fn update_liquidity_score(
        &self,
        pool: Address,
        score: f64,
    ) {
        self.liquidity_scores.insert(pool, score.max(0.0).min(1.0));
    }

    /// Update correlation between pools
    pub async fn update_pool_correlation(
        &self,
        pool_a: Address,
        pool_b: Address,
        correlation: f64,
    ) {
        let key = if pool_a < pool_b {
            (pool_a, pool_b)
        } else {
            (pool_b, pool_a)
        };
        self.pool_correlations.insert(key, correlation.max(-1.0).min(1.0));
    }

    /// Update market volatility factor
    pub async fn update_volatility(&self, volatility: f64) {
        let mut vol = self.volatility_factor.write().await;
        *vol = volatility.max(0.0);
    }

    /// Get estimated gas cost for a protocol
    pub fn get_protocol_gas_cost(&self, protocol: &ProtocolType) -> u64 {
        self.protocol_gas_costs
            .get(protocol)
            .copied()
            .unwrap_or(150_000)
    }

    /// Calculate time-weighted average price ratio
    pub async fn get_twap_ratio(
        &self,
        token_a: Address,
        token_b: Address,
    ) -> Option<f64> {
        let key = if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        };
        self.twap_cache.get(&key).map(|v| *v)
    }

    /// Update TWAP cache
    pub async fn update_twap(
        &self,
        token_a: Address,
        token_b: Address,
        ratio: f64,
    ) {
        let key = if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        };
        self.twap_cache.insert(key, ratio);
    }
}