// src/path/types.rs

use crate::types::{ExecutionPriority, ProtocolType};
use ethers::types::{Address, U256};
use petgraph::graph::UnGraph;
use smallvec::SmallVec;
use std::sync::atomic::AtomicU64;

/// Centralized configuration used across the `path` module
#[derive(Clone, Debug)]
pub struct PathConfig {
    pub strict_backtest: bool,

    // Search caps
    pub max_search_nodes: usize,
    pub max_frontier_size: usize,
    pub beam_width_per_depth: usize,
    pub max_no_improve_secs_live: u64,
    pub max_no_improve_secs_backtest: u64,

    // Graph build
    pub max_graph_nodes: usize,
    pub max_graph_edges: usize,
    pub incremental_block_window: u64,
    pub build_retry_backoff_ms: u64,    // base
    pub build_retry_backoff_max_ms: u64,

    // Heuristic weights
    pub w_liquidity: f64,
    pub w_mev_resistance: f64,
    pub w_protocol_gas_penalty: f64,
    pub w_price_impact_penalty: f64,

    // MEV & scoring clamps
    pub mev_score_min: f64,             // e.g. 0.1
    pub mev_score_max: f64,             // e.g. 3.0

    // Circular arb
    pub min_cycle_len: usize,
    pub max_cycle_len: usize,
    pub profit_history_len: usize,
    pub success_rate_threshold: f64,    // e.g. 0.55
}

impl Default for PathConfig {
    fn default() -> Self {
        Self {
            strict_backtest: false,

            // Search caps
            max_search_nodes: 10_000,
            max_frontier_size: 5_000,
            beam_width_per_depth: 32,
            max_no_improve_secs_live: 1,
            max_no_improve_secs_backtest: 3,

            // Graph build
            max_graph_nodes: 500_000,
            max_graph_edges: 1_000_000,
            incremental_block_window: 200,
            build_retry_backoff_ms: 100,
            build_retry_backoff_max_ms: 5_000,

            // Heuristic weights
            w_liquidity: 0.4,
            w_mev_resistance: 0.3,
            w_protocol_gas_penalty: 0.2,
            w_price_impact_penalty: 0.3,

            // MEV & scoring clamps
            mev_score_min: 0.1,
            mev_score_max: 3.0,

            // Circular arb
            min_cycle_len: 3,
            max_cycle_len: 6,
            profit_history_len: 100,
            success_rate_threshold: 0.55,
        }
    }
}

/// Individual leg of a path representing a single swap
/// 
/// Each leg represents one hop in a multi-hop swap path
#[derive(Debug, Clone)]
pub struct PathLeg {
    /// Source token address
    pub from_token: Address,
    /// Destination token address
    pub to_token: Address,
    /// DEX pool address for this swap
    pub pool_address: Address,
    /// Protocol type (Uniswap V2/V3, Curve, etc.)
    pub protocol_type: ProtocolType,
    /// Fee in basis points (e.g., 3000 = 0.3%)
    pub fee: u32,
    /// Input amount for this leg
    pub amount_in: U256,
    /// Expected output amount for this leg
    pub amount_out: U256,
    /// Whether this hop requires an approval transaction (affects gas)
    pub requires_approval: bool,
    /// Whether this hop wraps native token
    pub wrap_native: bool,
    /// Whether this hop unwraps native token
    pub unwrap_native: bool,
}

/// Search strategies for parallel workers
/// 
/// Different strategies optimize for different objectives
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchStrategy {
    /// Prioritize pools with high liquidity to minimize slippage
    HighLiquidity,
    /// Prioritize pools with low fees
    LowFee,
    /// Prioritize paths with fewer hops
    ShortPath,
    /// Balance between profit, liquidity, and gas costs
    Balanced,
}

impl SearchStrategy {
    /// Get all available strategies
    pub fn all() -> Vec<Self> {
        vec![
            Self::Balanced,
            Self::HighLiquidity,
            Self::LowFee,
            Self::ShortPath,
        ]
    }
    
    /// Get a human-readable name for the strategy
    pub fn name(&self) -> &'static str {
        match self {
            Self::HighLiquidity => "high_liquidity",
            Self::LowFee => "low_fee",
            Self::ShortPath => "short_path",
            Self::Balanced => "balanced",
        }
    }
}

/// Enhanced validation result with detailed metrics
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the path is valid for execution
    pub is_valid: bool,
    /// Expected output amount
    pub expected_amount_out: U256,
    /// Minimum acceptable output amount
    pub min_amount_out: U256,
    /// Current price impact in basis points
    pub current_price_impact_bps: u32,
    /// List of validation errors
    pub errors: Vec<String>,
    /// Estimated gas cost in USD
    pub gas_cost_usd: f64,
    /// List of validation warnings
    pub warnings: Vec<String>,
    /// Estimated execution time in milliseconds
    pub estimated_execution_time_ms: u64,
    /// Token addresses that need approval
    pub token_approvals_needed: Vec<Address>,
    /// Liquidity depth score (0.0 = shallow, 1.0 = deep)
    pub liquidity_depth_score: f64,
    /// Slippage tolerance in basis points
    pub slippage_tolerance_bps: u32,
    /// MEV risk score (0.0 = low risk, 1.0 = high risk)
    pub mev_risk_score: f64,
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self {
            is_valid: true,
            expected_amount_out: U256::zero(),
            min_amount_out: U256::zero(),
            current_price_impact_bps: 0,
            errors: Vec::new(),
            gas_cost_usd: 0.0,
            warnings: Vec::new(),
            estimated_execution_time_ms: 0,
            token_approvals_needed: Vec::new(),
            liquidity_depth_score: 0.0,
            slippage_tolerance_bps: 0,
            mev_risk_score: 0.0,
        }
    }
}

impl ValidationResult {
    /// Create a new invalid result with an error message
    pub fn invalid(error: String) -> Self {
        Self {
            is_valid: false,
            errors: vec![error],
            ..Default::default()
        }
    }
    
    /// Add an error to the validation result
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.is_valid = false;
    }
    
    /// Add a warning to the validation result
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }
}

/// Thread-safe metrics collection for path finding operations
#[derive(Debug, Default)]
pub struct PathFinderMetrics {
    /// Total paths found
    pub paths_found: AtomicU64,
    /// Cache hits
    pub cache_hits: AtomicU64,
    /// Cache misses
    pub cache_misses: AtomicU64,
    /// Total search time in milliseconds
    pub search_time_ms: AtomicU64,
    /// Number of graph updates
    pub graph_updates: AtomicU64,
    /// Number of concurrent operations
    pub concurrent_operations: AtomicU64,
    /// Token metadata loads
    pub token_metadata_loads: AtomicU64,
    /// Price discovery time in milliseconds
    pub price_discovery_time_ms: AtomicU64,
    /// Validation time in milliseconds
    pub validation_time_ms: AtomicU64,
    /// Graph build time in milliseconds
    pub graph_build_time_ms: AtomicU64,
}

impl PathFinderMetrics {
    /// Reset all metrics to zero
    pub fn reset(&self) {
        use std::sync::atomic::Ordering;
        self.paths_found.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.search_time_ms.store(0, Ordering::Relaxed);
        self.graph_updates.store(0, Ordering::Relaxed);
        self.concurrent_operations.store(0, Ordering::Relaxed);
        self.token_metadata_loads.store(0, Ordering::Relaxed);
        self.price_discovery_time_ms.store(0, Ordering::Relaxed);
        self.validation_time_ms.store(0, Ordering::Relaxed);
        self.graph_build_time_ms.store(0, Ordering::Relaxed);
    }
    
    /// Get cache hit rate as a percentage
    pub fn cache_hit_rate(&self) -> f64 {
        use std::sync::atomic::Ordering;
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 {
            (hits / total) * 100.0
        } else {
            0.0
        }
    }
}

/// Optimized path representation using SmallVec for better cache locality
/// 
/// SmallVec avoids heap allocation for paths with <= 8 hops
#[derive(Debug, Clone)]
pub struct Path {
    /// Sequence of token addresses in the path
    pub token_path: SmallVec<[Address; 8]>,
    /// Sequence of pool addresses for each hop
    pub pool_path: SmallVec<[Address; 8]>,
    /// Fee for each pool in basis points
    pub fee_path: SmallVec<[u32; 8]>,
    /// Protocol type for each pool
    pub protocol_path: SmallVec<[ProtocolType; 8]>,
    /// Initial input amount in smallest units
    pub initial_amount_in_wei: U256,
    /// Expected output amount in smallest units
    pub expected_output: U256,
    /// Minimum acceptable output amount
    pub min_output: U256,
    /// Total price impact in basis points
    pub price_impact_bps: u32,
    /// Estimated gas cost in units
    pub gas_estimate: u64,
    /// Gas cost in native token (ETH)
    pub gas_cost_native: f64,
    /// Profit estimate in native token
    pub profit_estimate_native: f64,
    /// Profit estimate in USD
    pub profit_estimate_usd: f64,
    /// Execution priority level
    pub execution_priority: ExecutionPriority,
    /// Tags for categorization
    pub tags: SmallVec<[String; 4]>,
    /// Source identifier (e.g., "a_star", "manual")
    pub source: Option<String>,
    /// Unix timestamp when path was discovered
    pub discovery_timestamp: u64,
    /// Confidence score (0.0 = low, 1.0 = high)
    pub confidence_score: f64,
    /// MEV resistance score (higher = more resistant)
    pub mev_resistance_score: f64,
    /// Input token address (denormalized for convenience)
    pub token_in: Address,
    /// Output token address (denormalized for convenience)
    pub token_out: Address,
    /// Detailed information for each leg
    pub legs: Vec<PathLeg>,
    /// Estimated gas cost in wei
    pub estimated_gas_cost_wei: U256,
}

impl Path {
    /// Create a new empty path
    pub fn new() -> Self {
        Self {
            token_path: SmallVec::new(),
            pool_path: SmallVec::new(),
            fee_path: SmallVec::new(),
            protocol_path: SmallVec::new(),
            initial_amount_in_wei: U256::zero(),
            expected_output: U256::zero(),
            min_output: U256::zero(),
            price_impact_bps: 0,
            gas_estimate: 0,
            gas_cost_native: 0.0,
            profit_estimate_native: 0.0,
            profit_estimate_usd: 0.0,
            execution_priority: ExecutionPriority::Default,
            tags: SmallVec::new(),
            source: None,
            discovery_timestamp: 0,
            confidence_score: 0.0,
            mev_resistance_score: 0.0,
            token_in: Address::zero(),
            token_out: Address::zero(),
            legs: Vec::new(),
            estimated_gas_cost_wei: U256::zero(),
        }
    }
    
    /// Check if this is a circular arbitrage path
    pub fn is_circular(&self) -> bool {
        !self.token_path.is_empty() && 
        self.token_path.first() == self.token_path.last()
    }
}

impl Default for Path {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimized search node for A* algorithm with better memory layout
/// 
/// Uses SmallVec to minimize allocations during search
#[derive(Debug, Clone)]
pub struct PathSearchNode {
    /// Current token address
    pub token: Address,
    /// Current amount at this node
    pub amount: U256,
    /// Path of tokens visited so far
    pub tokens: SmallVec<[Address; 8]>,
    /// Pools used so far
    pub pools: SmallVec<[Address; 8]>,
    /// Fees for each pool
    pub fees: SmallVec<[u32; 8]>,
    /// Protocol types for each pool
    pub protocols: SmallVec<[ProtocolType; 8]>,
    /// Cost from start to this node (g-score in A*)
    pub g_score: f64,
    /// Heuristic cost from this node to goal (h-score in A*)
    pub h_score: f64,
    /// Initial amount input to the path
    pub initial_amount_in_wei: U256,
    /// Current depth in the search tree
    pub depth: u8,
}

impl PathSearchNode {
    /// Get the f-score for A* (g + h)
    pub fn f_score(&self) -> f64 {
        self.g_score + self.h_score
    }
    
    /// Check if this node has visited a specific pool
    pub fn has_visited_pool(&self, pool: &Address) -> bool {
        self.pools.contains(pool)
    }
    
    /// Check if this node has visited a specific token
    pub fn has_visited_token(&self, token: &Address) -> bool {
        self.tokens.contains(token)
    }
}

/// Edge in the token graph representing a pool
#[derive(Debug, Clone, Copy)]
pub struct PoolEdge {
    /// Address of the pool connecting two tokens
    pub pool_address: Address,
}

/// Immutable snapshot of the token graph at a point in time
#[derive(Clone)]
pub struct GraphSnapshot {
    /// Number of nodes (tokens) in the graph
    pub node_count: usize,
    /// Number of edges (pools) in the graph
    pub edge_count: usize,
    /// The actual graph structure
    pub graph: UnGraph<Address, PoolEdge>,
}

impl GraphSnapshot {
    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.node_count == 0
    }
    
    /// Get the density of the graph (edges / possible edges)
    pub fn density(&self) -> f64 {
        if self.node_count <= 1 {
            return 0.0;
        }
        let max_edges = (self.node_count * (self.node_count - 1)) / 2;
        self.edge_count as f64 / max_edges as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_search_strategy_names() {
        assert_eq!(SearchStrategy::Balanced.name(), "balanced");
        assert_eq!(SearchStrategy::HighLiquidity.name(), "high_liquidity");
        assert_eq!(SearchStrategy::LowFee.name(), "low_fee");
        assert_eq!(SearchStrategy::ShortPath.name(), "short_path");
    }
    
    #[test]
    fn test_validation_result_builder() {
        let mut result = ValidationResult::default();
        assert!(result.is_valid);
        
        result.add_error("Test error".to_string());
        assert!(!result.is_valid);
        assert_eq!(result.errors.len(), 1);
        
        result.add_warning("Test warning".to_string());
        assert_eq!(result.warnings.len(), 1);
    }
    
    #[test]
    fn test_metrics_cache_hit_rate() {
        let metrics = PathFinderMetrics::default();
        use std::sync::atomic::Ordering;
        
        assert_eq!(metrics.cache_hit_rate(), 0.0);
        
        metrics.cache_hits.store(75, Ordering::Relaxed);
        metrics.cache_misses.store(25, Ordering::Relaxed);
        assert_eq!(metrics.cache_hit_rate(), 75.0);
    }
    
    #[test]
    fn test_path_circular_detection() {
        let mut path = Path::new();
        assert!(!path.is_circular());
        
        let addr = Address::random();
        path.token_path.push(addr);
        path.token_path.push(Address::random());
        path.token_path.push(addr);
        assert!(path.is_circular());
    }
    
    #[test]
    fn test_search_node_f_score() {
        let node = PathSearchNode {
            token: Address::random(),
            amount: U256::from(1000),
            tokens: SmallVec::new(),
            pools: SmallVec::new(),
            fees: SmallVec::new(),
            protocols: SmallVec::new(),
            g_score: 10.0,
            h_score: 5.0,
            initial_amount_in_wei: U256::from(1000),
            depth: 0,
        };
        
        assert_eq!(node.f_score(), 15.0);
    }
}