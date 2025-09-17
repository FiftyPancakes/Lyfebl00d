// src/path/utils.rs

use super::types::{Path, PathSearchNode};
use crate::{
    gas_oracle::GasOracle,
    pool_manager::PoolManagerTrait,
    types::{normalize_units, ProtocolType},
};
use ahash::AHashSet;
use ethers::types::U256;
use futures::{
    stream::{FuturesUnordered, StreamExt},
};
use std::cmp::Ordering;
use tracing::{debug, warn};

impl Path {
    /// Get the number of hops in the path
    pub fn hop_count(&self) -> usize {
        self.token_path.len().saturating_sub(1)
    }

    /// Get a short description of the path for logging
    pub fn short_desc(&self) -> String {
        if self.token_path.is_empty() {
            return "empty".to_string();
        }
        
        self.token_path
            .iter()
            .map(|a| format!("{:#x}", a))
            .collect::<Vec<_>>()
            .join("->")
    }

    /// Check if this path is similar to another based on pool and protocol overlap
    ///
    /// Returns true if the Jaccard similarity of (pool, protocol) pairs exceeds threshold.
    /// This prevents discarding paths that use similar pools but different protocols.
    pub fn is_similar_to(&self, other: &Path, threshold: f64) -> bool {
        if threshold < 0.0 || threshold > 1.0 {
            warn!(target: "path_utils", "Invalid similarity threshold: {}", threshold);
            return false;
        }

        if self.pool_path.is_empty() || other.pool_path.is_empty() {
            return false;
        }

        // Create canonical keys combining pool address and protocol type
        let a: AHashSet<String> = self.pool_path.iter()
            .zip(self.protocol_path.iter())
            .map(|(pool, protocol)| format!("{:?}:{:?}", pool, protocol))
            .collect();

        let b: AHashSet<String> = other.pool_path.iter()
            .zip(other.protocol_path.iter())
            .map(|(pool, protocol)| format!("{:?}:{:?}", pool, protocol))
            .collect();

        let intersection_size = a.intersection(&b).count() as f64;
        let union_size = a.union(&b).count() as f64;

        if union_size == 0.0 {
            return false;
        }

        let similarity = intersection_size / union_size;
        similarity >= threshold
    }

    /// Calculate minimum output with slippage tolerance
    /// 
    /// Updates the min_output field based on expected output and slippage
    pub fn calculate_min_output(&mut self, slippage_bps: u32) {
        // Cap slippage at 100%
        let slippage = U256::from(slippage_bps.min(10000));
        
        // Calculate minimum acceptable output
        let min_out = self.expected_output
            .saturating_mul(U256::from(10000).saturating_sub(slippage))
            .checked_div(U256::from(10000))
            .unwrap_or(U256::zero());
        
        // Ensure at least 1 unit minimum
        self.min_output = min_out.max(U256::one());
    }

    /// Update gas estimate based on protocol types
    pub fn update_gas_estimate(&mut self, gas_oracle: &std::sync::Arc<GasOracle>) {
        const MAX_GAS_PER_HOP: u64 = 500_000;
        const MAX_TOTAL_GAS: u64 = 2_000_000;
        const BASE_TX_GAS: u64 = 21_000;
        
        let mut total_gas = BASE_TX_GAS;
        
        for protocol in &self.protocol_path {
            let hop_gas = gas_oracle.inner().estimate_protocol_gas(protocol);
            let capped_hop_gas = hop_gas.min(MAX_GAS_PER_HOP);
            total_gas = total_gas.saturating_add(capped_hop_gas);
            
            if total_gas > MAX_TOTAL_GAS {
                total_gas = MAX_TOTAL_GAS;
                break;
            }
        }
        
        self.gas_estimate = total_gas;
    }

    /// Calculate confidence score based on pool liquidity and other factors
    /// 
    /// Score ranges from 0.0 (no confidence) to ~1.0 (high confidence)
    pub async fn calculate_confidence_score(
        &mut self,
        pool_manager: &std::sync::Arc<dyn PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        block_number: Option<u64>,
    ) {
        let mut score = 1.0;
        
        if self.pool_path.is_empty() {
            self.confidence_score = 0.0;
            return;
        }

        // Fetch pool info in parallel
        let mut futures = FuturesUnordered::new();
        for pool_address in &self.pool_path {
            let fut = pool_manager.get_pool_info(chain_name, *pool_address, block_number);
            futures.push(fut);
        }

        let mut pool_count = 0;
        let mut failed_pools = 0;
        
        while let Some(result) = futures.next().await {
            pool_count += 1;
            
            match result {
                Ok(pool_info) => {
                    // Factor in liquidity depth
                    let liquidity_factor = if pool_info.liquidity_usd > 0.0 {
                        // Normalize to 1M USD baseline
                        (pool_info.liquidity_usd / 1_000_000.0)
                            .min(1.0)
                            .max(0.1)
                    } else {
                        // Estimate liquidity from reserves if USD value not available
                        let reserve0_f64 = normalize_units(pool_info.reserves0, pool_info.token0_decimals);
                        let reserve1_f64 = normalize_units(pool_info.reserves1, pool_info.token1_decimals);
                        
                        // Very rough USD estimate (assumes $1000 per unit average)
                        let estimated_liquidity = (reserve0_f64 + reserve1_f64) * 1000.0;
                        (estimated_liquidity / 1_000_000.0)
                            .min(1.0)
                            .max(0.1)
                    };

                    // Protocol reliability bonus
                    let protocol_bonus = match pool_info.protocol_type {
                        ProtocolType::UniswapV4 => 1.1,
                        ProtocolType::UniswapV3 => 1.05,
                        ProtocolType::UniswapV2 => 1.0,
                        ProtocolType::Curve => 1.02,
                        ProtocolType::Balancer => 1.03,
                        ProtocolType::SushiSwap => 0.98,
                        _ => 0.95,
                    };

                    // Combine factors
                    let pool_score = (0.7 + 0.3 * liquidity_factor) * protocol_bonus;
                    score *= pool_score;
                }
                Err(e) => {
                    debug!(target: "path_utils", "Failed to get pool info: {}", e);
                    failed_pools += 1;
                    score *= 0.8; // Penalty for missing pool data
                }
            }
        }

        // Penalize for too many hops
        let hop_penalty = 1.0 / (1.0 + 0.1 * self.hop_count() as f64);
        score *= hop_penalty;

        // Penalize for high price impact
        let impact_penalty = 1.0 / (1.0 + self.price_impact_bps as f64 / 10000.0);
        score *= impact_penalty;
        
        // Penalize if many pools failed
        if pool_count > 0 {
            let failure_rate = failed_pools as f64 / pool_count as f64;
            score *= 1.0 - (failure_rate * 0.5);
        }
        
        // Ensure score is in valid range
        score = score.max(0.0).min(2.0);
        
        debug!(
            target: "path_utils", 
            "Path confidence score for '{}': {:.4} (hops={}, impact={}bps, failed_pools={})", 
            self.short_desc(), 
            score,
            self.hop_count(),
            self.price_impact_bps,
            failed_pools
        );
        
        self.confidence_score = score;
    }

    /// Calculate MEV resistance score
    /// 
    /// Higher scores indicate better resistance to MEV attacks
    pub fn calculate_mev_resistance(&mut self) {
        let mut resistance = 1.0;

        // High-value transactions are more likely to be targeted
        let value_factor = (self.profit_estimate_usd / 10000.0)
            .min(1.0)
            .max(0.0);
        resistance *= 1.0 - value_factor * 0.5;

        // More hops make sandwich attacks harder
        let hop_bonus = 1.0 + 0.1 * self.hop_count() as f64;
        resistance *= hop_bonus;

        // Uncommon protocols are harder to exploit
        let common_protocols = [
            ProtocolType::UniswapV2, 
            ProtocolType::UniswapV3,
            ProtocolType::SushiSwap,
        ];
        
        let uncommon_count = self.protocol_path
            .iter()
            .filter(|p| !common_protocols.contains(p))
            .count();
        
        let protocol_bonus = 1.0 + 0.15 * uncommon_count as f64;
        resistance *= protocol_bonus;
        
        // Paths through multiple protocols are harder to sandwich
        let unique_protocols: AHashSet<_> = self.protocol_path.iter().collect();
        let diversity_bonus = 1.0 + 0.1 * unique_protocols.len() as f64;
        resistance *= diversity_bonus;

        // Cap resistance score
        resistance = resistance.min(3.0).max(0.1);

        debug!(
            target: "path_utils",
            "MEV resistance for '{}': {:.4} (value=${:.2}, hops={}, uncommon={}, unique_protocols={})",
            self.short_desc(),
            resistance,
            self.profit_estimate_usd,
            self.hop_count(),
            uncommon_count,
            unique_protocols.len()
        );
        
        self.mev_resistance_score = resistance;
    }
    
    /// Validate path structure
    pub fn validate_structure(&self) -> Result<(), String> {
        // Check token path
        if self.token_path.is_empty() {
            return Err("Empty token path".to_string());
        }
        
        // Check path consistency
        if self.pool_path.len() != self.token_path.len() - 1 {
            return Err(format!(
                "Inconsistent path: {} tokens but {} pools", 
                self.token_path.len(), 
                self.pool_path.len()
            ));
        }
        
        if self.fee_path.len() != self.pool_path.len() {
            return Err(format!(
                "Inconsistent fees: {} pools but {} fees", 
                self.pool_path.len(), 
                self.fee_path.len()
            ));
        }
        
        if self.protocol_path.len() != self.pool_path.len() {
            return Err(format!(
                "Inconsistent protocols: {} pools but {} protocols", 
                self.pool_path.len(), 
                self.protocol_path.len()
            ));
        }
        
        // Check for duplicate pools
        let unique_pools: AHashSet<_> = self.pool_path.iter().collect();
        if unique_pools.len() != self.pool_path.len() {
            return Err("Path contains duplicate pools".to_string());
        }
        
        // Check amounts
        if self.initial_amount_in_wei.is_zero() {
            return Err("Zero input amount".to_string());
        }
        
        if self.expected_output.is_zero() {
            return Err("Zero expected output".to_string());
        }
        
        Ok(())
    }
}

/// Deduplicate paths based on similarity
/// 
/// Keeps the most profitable path from each group of similar paths
pub fn deduplicate_paths(paths: &[Path], similarity_threshold: f64) -> Vec<Path> {
    if paths.is_empty() {
        return vec![];
    }
    
    if similarity_threshold < 0.0 || similarity_threshold > 1.0 {
        warn!(target: "path_utils", "Invalid similarity threshold: {}, using 0.95", similarity_threshold);
        let threshold = 0.95;
        return deduplicate_paths(paths, threshold);
    }

    let mut unique_paths: Vec<Path> = Vec::with_capacity(paths.len());

    for path in paths {
        // Validate path structure
        if let Err(e) = path.validate_structure() {
            debug!(target: "path_utils", "Skipping invalid path: {}", e);
            continue;
        }
        
        let mut is_duplicate = false;
        let mut duplicate_index = None;

        // Check against existing unique paths
        for (i, unique_path) in unique_paths.iter().enumerate() {
            if path.is_similar_to(unique_path, similarity_threshold) {
                is_duplicate = true;
                duplicate_index = Some(i);
                break;
            }
        }

        if let Some(idx) = duplicate_index {
            // Replace if this path is better
            let existing = &unique_paths[idx];
            let should_replace = 
                path.profit_estimate_usd > existing.profit_estimate_usd ||
                (path.profit_estimate_usd == existing.profit_estimate_usd && 
                 path.confidence_score > existing.confidence_score) ||
                (path.profit_estimate_usd == existing.profit_estimate_usd && 
                 path.confidence_score == existing.confidence_score &&
                 path.gas_estimate < existing.gas_estimate);
                 
            if should_replace {
                unique_paths[idx] = path.clone();
            }
        } else if !is_duplicate {
            unique_paths.push(path.clone());
        }
    }

    debug!(
        target: "path_utils",
        "Deduplicated paths: {} -> {} (threshold={})",
        paths.len(),
        unique_paths.len(),
        similarity_threshold
    );
    
    unique_paths
}

impl Ord for PathSearchNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // A* uses f = g + h, where lower is better
        let f_self = self.g_score + self.h_score;
        let f_other = other.g_score + other.h_score;
        
        match (f_self.is_finite(), f_other.is_finite()) {
            (true, true) => f_self.partial_cmp(&f_other).unwrap_or(Ordering::Equal),
            (true, false) => Ordering::Less,  // Finite is better than infinite
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        }
    }
}

impl PartialOrd for PathSearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PathSearchNode {
    fn eq(&self, other: &Self) -> bool {
        let f_score_self = self.g_score + self.h_score;
        let f_score_other = other.g_score + other.h_score;
        
        match (f_score_self.is_finite(), f_score_other.is_finite()) {
            (true, true) => (f_score_self - f_score_other).abs() < 1e-9,
            (false, false) => true,
            _ => false,
        }
    }
}

impl Eq for PathSearchNode {}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    
    fn create_test_path() -> Path {
        let addr1 = ethers::types::Address::random();
        let addr2 = ethers::types::Address::random();
        let pool = ethers::types::Address::random();
        
        Path {
            token_path: smallvec![addr1, addr2],
            pool_path: smallvec![pool],
            fee_path: smallvec![3000],
            protocol_path: smallvec![ProtocolType::UniswapV3],
            initial_amount_in_wei: U256::from(1000000),
            expected_output: U256::from(900000),
            min_output: U256::zero(),
            price_impact_bps: 100,
            gas_estimate: 150000,
            gas_cost_native: 0.01,
            profit_estimate_native: 0.001,
            profit_estimate_usd: 2.0,
            execution_priority: crate::types::ExecutionPriority::Default,
            tags: smallvec![],
            source: Some("test".to_string()),
            discovery_timestamp: 0,
            confidence_score: 0.8,
            mev_resistance_score: 1.2,
            token_in: addr1,
            token_out: addr2,
            legs: vec![],
            estimated_gas_cost_wei: U256::from(1000000000000000u64),
        }
    }
    
    #[test]
    fn test_path_validation() {
        let mut path = create_test_path();
        assert!(path.validate_structure().is_ok());
        
        // Test invalid structure
        path.pool_path.push(ethers::types::Address::random());
        assert!(path.validate_structure().is_err());
    }
    
    #[test]
    fn test_similarity_calculation() {
        let path1 = create_test_path();
        let mut path2 = path1.clone();
        
        // Same pools = 100% similar
        assert!(path1.is_similar_to(&path2, 0.99));
        
        // Different pools = 0% similar
        path2.pool_path[0] = ethers::types::Address::random();
        assert!(!path1.is_similar_to(&path2, 0.01));
    }
    
    #[test]
    fn test_min_output_calculation() {
        let mut path = create_test_path();
        path.expected_output = U256::from(1000000);
        
        // 1% slippage
        path.calculate_min_output(100);
        assert_eq!(path.min_output, U256::from(990000));
        
        // 50% slippage
        path.calculate_min_output(5000);
        assert_eq!(path.min_output, U256::from(500000));
        
        // 100% slippage (capped)
        path.calculate_min_output(15000);
        assert_eq!(path.min_output, U256::one());
    }
}