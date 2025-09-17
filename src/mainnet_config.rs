use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Mainnet-scale configuration for the arbitrage bot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainnetConfig {
    /// Path finding configuration
    pub pathfinding: PathfindingConfig,
    
    /// Graph configuration
    pub graph: GraphConfig,
    
    /// Discovery configuration
    pub discovery: DiscoveryConfig,
    
    /// Execution configuration
    pub execution: ExecutionConfig,
    
    /// Chain-specific configurations
    pub chains: HashMap<String, ChainConfig>,
    
    /// Performance tuning
    pub performance: PerformanceConfig,
    
    /// Risk management
    pub risk: RiskConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathfindingConfig {
    /// Maximum nodes in graph (increased for mainnet)
    pub max_graph_nodes: usize,
    
    /// Maximum edges in graph (increased for mainnet)
    pub max_graph_edges: usize,
    
    /// Maximum hops in a path
    pub max_path_hops: usize,
    
    /// Minimum cycle length for circular arbitrage
    pub min_cycle_length: usize,
    
    /// Maximum cycle length for circular arbitrage
    pub max_cycle_length: usize,
    
    /// Path cache size
    pub path_cache_size: usize,
    
    /// Path cache TTL in seconds
    pub path_cache_ttl_secs: u64,
    
    /// Enable enhanced heuristics
    pub use_enhanced_heuristics: bool,
    
    /// Enable circular arbitrage detection
    pub enable_circular_detection: bool,
    
    /// A* search parameters
    pub astar_params: AStarParameters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AStarParameters {
    /// Maximum nodes to explore per search
    pub max_nodes_to_explore: usize,
    
    /// Search timeout in milliseconds
    pub search_timeout_ms: u64,
    
    /// Frontier pruning threshold
    pub frontier_size_limit: usize,
    
    /// Enable parallel search for multiple strategies
    pub enable_parallel_search: bool,
    
    /// Number of parallel search threads
    pub parallel_search_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// Graph rebuild interval in blocks
    pub rebuild_interval_blocks: u64,
    
    /// Incremental update batch size
    pub incremental_update_batch: usize,
    
    /// Enable graph persistence
    pub enable_persistence: bool,
    
    /// Graph snapshot interval in blocks
    pub snapshot_interval_blocks: u64,
    
    /// Maximum pool age in blocks before removal
    pub max_pool_age_blocks: u64,
    
    /// Minimum liquidity for pool inclusion (USD)
    pub min_pool_liquidity_usd: f64,
    
    /// Pool filtering criteria
    pub pool_filters: PoolFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolFilters {
    /// Include only verified pools
    pub require_verified: bool,
    
    /// Minimum 24h volume (USD)
    pub min_24h_volume_usd: f64,
    
    /// Maximum fee in basis points
    pub max_fee_bps: u32,
    
    /// Blacklisted pool addresses
    pub blacklist: Vec<String>,
    
    /// Whitelisted protocols
    pub protocol_whitelist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Number of discovery scanners per chain
    pub scanners_per_chain: usize,
    
    /// Discovery cycle interval in milliseconds
    pub discovery_interval_ms: u64,
    
    /// Maximum opportunities per block
    pub max_opportunities_per_block: usize,
    
    /// Opportunity expiry time in seconds
    pub opportunity_expiry_secs: u64,
    
    /// Enable statistical arbitrage
    pub enable_statistical_arb: bool,
    
    /// Statistical arbitrage parameters
    pub statistical_params: StatisticalParameters,
    
    /// Enable cross-chain arbitrage
    pub enable_cross_chain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalParameters {
    /// Z-score threshold for mean reversion
    pub z_score_threshold: f64,
    
    /// Minimum correlation for pair trading
    pub min_correlation: f64,
    
    /// Historical data window in blocks
    pub history_window_blocks: u64,
    
    /// Minimum samples for statistics
    pub min_samples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Maximum concurrent MEV plays
    pub max_concurrent_plays: usize,
    
    /// MEV play timeout in seconds
    pub play_timeout_secs: u64,
    
    /// Enable flashloans
    pub enable_flashloans: bool,
    
    /// Flashloan providers
    pub flashloan_providers: Vec<String>,
    
    /// Gas bidding strategy
    pub gas_strategy: GasStrategy,
    
    /// Bundle submission configuration
    pub bundle_config: BundleConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasStrategy {
    /// Base gas price multiplier
    pub base_multiplier: f64,
    
    /// Priority fee multiplier
    pub priority_multiplier: f64,
    
    /// Maximum gas price in Gwei
    pub max_gas_price_gwei: u64,
    
    /// Gas escalation factor per retry
    pub escalation_factor: f64,
    
    /// Enable EIP-1559 transactions
    pub use_eip1559: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleConfig {
    /// Use Flashbots
    pub use_flashbots: bool,
    
    /// Flashbots relay URLs
    pub flashbots_relays: Vec<String>,
    
    /// Maximum bundle size
    pub max_bundle_size: usize,
    
    /// Bundle submission retries
    pub submission_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    /// Chain ID
    pub chain_id: u64,
    
    /// RPC endpoints (multiple for redundancy)
    pub rpc_endpoints: Vec<String>,
    
    /// WebSocket endpoints
    pub ws_endpoints: Vec<String>,
    
    /// Block time in seconds
    pub block_time_secs: u64,
    
    /// Chain-specific token addresses
    pub tokens: ChainTokens,
    
    /// Supported DEX protocols
    pub supported_protocols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainTokens {
    /// Wrapped native token
    pub wrapped_native: String,
    
    /// Major stablecoins
    pub stablecoins: Vec<String>,
    
    /// High-liquidity tokens
    pub blue_chips: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Database connection pool size
    pub db_pool_size: u32,
    
    /// HTTP connection pool size
    pub http_pool_size: u32,
    
    /// WebSocket reconnect interval
    pub ws_reconnect_interval_secs: u64,
    
    /// Cache sizes
    pub cache_sizes: CacheSizes,
    
    /// Thread pool sizes
    pub thread_pools: ThreadPoolConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSizes {
    /// Token metadata cache
    pub token_metadata: usize,
    
    /// Pool info cache
    pub pool_info: usize,
    
    /// Price cache
    pub prices: usize,
    
    /// Gas price cache
    pub gas_prices: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadPoolConfig {
    /// Discovery thread pool size
    pub discovery: usize,
    
    /// Simulation thread pool size
    pub simulation: usize,
    
    /// Execution thread pool size
    pub execution: usize,
    
    /// Database thread pool size
    pub database: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum position size in USD
    pub max_position_size_usd: f64,
    
    /// Maximum slippage tolerance in basis points
    pub max_slippage_bps: u32,
    
    /// Minimum profit threshold in USD
    pub min_profit_usd: f64,
    
    /// Maximum profit threshold in USD (sanity check)
    pub max_profit_usd: f64,
    
    /// Circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Failure threshold before tripping
    pub failure_threshold: u32,
    
    /// Timeout duration in seconds
    pub timeout_secs: u64,
    
    /// Half-open test count
    pub half_open_tests: u32,
    
    /// Success threshold to reset
    pub success_threshold: u32,
}

impl Default for MainnetConfig {
    fn default() -> Self {
        Self {
            pathfinding: PathfindingConfig {
                max_graph_nodes: 500_000,  // Increased from 100k
                max_graph_edges: 2_000_000, // Increased from 500k
                max_path_hops: 5,
                min_cycle_length: 3,
                max_cycle_length: 7,
                path_cache_size: 10_000,
                path_cache_ttl_secs: 60,
                use_enhanced_heuristics: true,
                enable_circular_detection: true,
                astar_params: AStarParameters {
                    max_nodes_to_explore: 50_000,
                    search_timeout_ms: 1000,
                    frontier_size_limit: 10_000,
                    enable_parallel_search: true,
                    parallel_search_threads: 4,
                },
            },
            graph: GraphConfig {
                rebuild_interval_blocks: 100,
                incremental_update_batch: 50,
                enable_persistence: true,
                snapshot_interval_blocks: 1000,
                max_pool_age_blocks: 100_000,
                min_pool_liquidity_usd: 0.01,
                pool_filters: PoolFilters {
                    require_verified: false,
                    min_24h_volume_usd: 1_000.0,
                    max_fee_bps: 10000, // 100%
                    blacklist: Vec::new(),
                    protocol_whitelist: vec![
                        "UniswapV2".to_string(),
                        "UniswapV3".to_string(),
                        "SushiSwap".to_string(),
                        "Curve".to_string(),
                        "Balancer".to_string(),
                    ],
                },
            },
            discovery: DiscoveryConfig {
                scanners_per_chain: 4,
                discovery_interval_ms: 500,
                max_opportunities_per_block: 200,
                opportunity_expiry_secs: 10,
                enable_statistical_arb: true,
                statistical_params: StatisticalParameters {
                    z_score_threshold: 2.5,
                    min_correlation: 0.7,
                    history_window_blocks: 1000,
                    min_samples: 100,
                },
                enable_cross_chain: true,
            },
            execution: ExecutionConfig {
                max_concurrent_plays: 50,
                play_timeout_secs: 30,
                enable_flashloans: true,
                flashloan_providers: vec![
                    "AAVE".to_string(),
                    "Balancer".to_string(),
                    "dYdX".to_string(),
                ],
                gas_strategy: GasStrategy {
                    base_multiplier: 1.1,
                    priority_multiplier: 1.2,
                    max_gas_price_gwei: 1000,
                    escalation_factor: 1.15,
                    use_eip1559: true,
                },
                bundle_config: BundleConfig {
                    use_flashbots: true,
                    flashbots_relays: vec![
                        "https://relay.flashbots.net".to_string(),
                    ],
                    max_bundle_size: 10,
                    submission_retries: 3,
                },
            },
            chains: Self::default_chains(),
            performance: PerformanceConfig {
                db_pool_size: 50,
                http_pool_size: 100,
                ws_reconnect_interval_secs: 5,
                cache_sizes: CacheSizes {
                    token_metadata: 10_000,
                    pool_info: 50_000,
                    prices: 100_000,
                    gas_prices: 1_000,
                },
                thread_pools: ThreadPoolConfig {
                    discovery: 8,
                    simulation: 16,
                    execution: 8,
                    database: 4,
                },
            },
            risk: RiskConfig {
                max_position_size_usd: 1_000_000.0,
                max_slippage_bps: 200,
                min_profit_usd: 10.0,
                max_profit_usd: 1_000_000.0,
                circuit_breaker: CircuitBreakerConfig {
                    failure_threshold: 5,
                    timeout_secs: 300,
                    half_open_tests: 3,
                    success_threshold: 2,
                },
            },
        }
    }
}

impl MainnetConfig {
    fn default_chains() -> HashMap<String, ChainConfig> {
        let mut chains = HashMap::new();
        
        // Ethereum Mainnet
        chains.insert("ethereum".to_string(), ChainConfig {
            chain_id: 1,
            rpc_endpoints: vec![
                "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY".to_string(),
                "https://mainnet.infura.io/v3/YOUR_KEY".to_string(),
            ],
            ws_endpoints: vec![
                "wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY".to_string(),
            ],
            block_time_secs: 12,
            tokens: ChainTokens {
                wrapped_native: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(),
                stablecoins: vec![
                    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(), // USDC
                    "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(), // USDT
                    "0x6B175474E89094C44Da98b954EedeAC495271d0F".to_string(), // DAI
                ],
                blue_chips: vec![
                    "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".to_string(), // WBTC
                    "0x514910771AF9Ca656af840dff83E8264EcF986CA".to_string(), // LINK
                ],
            },
            supported_protocols: vec![
                "UniswapV2".to_string(),
                "UniswapV3".to_string(),
                "SushiSwap".to_string(),
                "Curve".to_string(),
                "Balancer".to_string(),
            ],
        });
        
        // Arbitrum
        chains.insert("arbitrum".to_string(), ChainConfig {
            chain_id: 42161,
            rpc_endpoints: vec![
                "https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY".to_string(),
            ],
            ws_endpoints: vec![
                "wss://arb-mainnet.g.alchemy.com/v2/YOUR_KEY".to_string(),
            ],
            block_time_secs: 2,
            tokens: ChainTokens {
                wrapped_native: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1".to_string(),
                stablecoins: vec![
                    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8".to_string(), // USDC
                ],
                blue_chips: vec![],
            },
            supported_protocols: vec![
                "UniswapV3".to_string(),
                "SushiSwap".to_string(),
                "Camelot".to_string(),
            ],
        });
        
        chains
    }

    /// Load configuration from file
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&contents)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let contents = serde_json::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate graph limits
        if self.pathfinding.max_graph_nodes < 100_000 {
            return Err("max_graph_nodes too small for mainnet".to_string());
        }
        
        if self.pathfinding.max_graph_edges < 500_000 {
            return Err("max_graph_edges too small for mainnet".to_string());
        }
        
        // Validate path parameters
        if self.pathfinding.max_path_hops > 10 {
            return Err("max_path_hops too large, will impact performance".to_string());
        }
        
        // Validate discovery parameters
        if self.discovery.max_opportunities_per_block > 1000 {
            return Err("max_opportunities_per_block too large".to_string());
        }
        
        // Validate execution parameters
        if self.execution.max_concurrent_plays > 100 {
            return Err("max_concurrent_plays too large".to_string());
        }
        
        Ok(())
    }
}