use ethers::types::{Address, U256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::errors::PathError;
use crate::path::types::{Path, PathLeg};
use crate::types::ProtocolType;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use smallvec::SmallVec;
use tracing::debug;

/// Safety state for circular arbitrage search to prevent unbounded exploration
#[derive(Debug, Clone)]
pub struct CircularSearchState {
    /// When the search started
    pub start_time: Instant,
    /// Whether this is a backtest (affects timeout values)
    pub is_backtesting: bool,
    /// Maximum time allowed for the search
    pub max_search_time: Duration,
    /// Maximum nodes to explore before terminating
    pub max_nodes_to_explore: usize,
    /// Maximum cycles to find before terminating
    pub max_cycles_to_find: usize,
    /// Current number of nodes explored
    pub nodes_explored: usize,
    /// Current number of cycles found
    pub cycles_found: usize,
    /// Time of last cycle found
    pub last_cycle_time: Instant,
    /// Maximum time without finding a cycle before terminating
    pub max_time_without_cycle: Duration,
}

impl CircularSearchState {
    /// Create a new search state
    pub fn new(is_backtesting: bool) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            is_backtesting,
            max_search_time: if is_backtesting { Duration::from_secs(30) } else { Duration::from_secs(5) },
            max_nodes_to_explore: if is_backtesting { 100_000 } else { 50_000 },
            max_cycles_to_find: 100,
            nodes_explored: 0,
            cycles_found: 0,
            last_cycle_time: now,
            max_time_without_cycle: if is_backtesting { Duration::from_secs(10) } else { Duration::from_secs(2) },
        }
    }

    /// Check if search should terminate based on safety conditions
    pub fn should_terminate(&self) -> bool {
        // Check time limit
        if self.start_time.elapsed() > self.max_search_time {
            debug!(target: "circular_arbitrage", "Circular search terminated: max time reached ({:?})", self.max_search_time);
            return true;
        }

        // Check node exploration limit
        if self.nodes_explored > self.max_nodes_to_explore {
            debug!(target: "circular_arbitrage", "Circular search terminated: max nodes explored ({})", self.nodes_explored);
            return true;
        }

        // Check cycle limit
        if self.cycles_found > self.max_cycles_to_find {
            debug!(target: "circular_arbitrage", "Circular search terminated: max cycles found ({})", self.cycles_found);
            return true;
        }

        // Check stagnation (no cycles found recently)
        if self.last_cycle_time.elapsed() > self.max_time_without_cycle && self.cycles_found > 0 {
            debug!(target: "circular_arbitrage", "Circular search terminated: stagnation detected ({:?} since last cycle)", self.max_time_without_cycle);
            return true;
        }

        false
    }

    /// Increment the nodes explored counter
    pub fn increment_nodes_explored(&mut self) {
        self.nodes_explored += 1;
    }

    /// Record that a cycle was found
    pub fn cycle_found(&mut self) {
        self.cycles_found += 1;
        self.last_cycle_time = Instant::now();
    }
}

/// Advanced circular arbitrage detector that finds complex cycles
pub struct CircularArbitrageDetector {
    /// Minimum cycle length (typically 3 for triangular)
    min_cycle_length: usize,
    /// Maximum cycle length to prevent exponential search
    max_cycle_length: usize,
    /// Profit threshold in basis points
    min_profit_bps: u32,
    /// Cache of profitable cycles
    cycle_cache: Arc<dashmap::DashMap<Vec<Address>, CycleInfo>>,
}

#[derive(Clone)]
struct CycleInfo {
    tokens: Vec<Address>,
    pools: Vec<Address>,
    last_profitable_block: u64,
    profit_history: VecDeque<f64>,
    success_rate: f64,
}

impl CircularArbitrageDetector {
    pub fn new(
        min_cycle_length: usize,
        max_cycle_length: usize,
        min_profit_bps: u32,
    ) -> Self {
        Self {
            min_cycle_length,
            max_cycle_length,
            min_profit_bps,
            cycle_cache: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Find all profitable circular arbitrage paths
    pub async fn find_all_cycles(
        &self,
        graph: &petgraph::Graph<Address, crate::path::types::PoolEdge, petgraph::Undirected>,
        token_to_node: &dashmap::DashMap<Address, NodeIndex>,
        pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        block_number: Option<u64>,
        is_backtesting: bool,
    ) -> Result<Vec<Path>, PathError> {
        let mut all_cycles = Vec::new();
        let mut search_state = CircularSearchState::new(is_backtesting);

        // Get high-liquidity tokens as starting points
        let start_tokens = self.get_high_liquidity_tokens(token_to_node, chain_name).await;

        for token in start_tokens {
            // Check if we should terminate before starting a new token
            if search_state.should_terminate() {
                debug!(target: "circular_arbitrage", "Terminating cycle search early due to safety limits");
                break;
            }

            if let Some(node_idx) = token_to_node.get(&token) {
                let cycles = self.find_cycles_from_node(
                    graph,
                    *node_idx,
                    token,
                    pool_manager.clone(),
                    chain_name,
                    block_number,
                    &mut search_state,
                ).await?;

                all_cycles.extend(cycles);
            }
        }

        debug!(target: "circular_arbitrage", "Found {} cycles, explored {} nodes in {:?}", all_cycles.len(), search_state.nodes_explored, search_state.start_time.elapsed());

        // Deduplicate cycles
        self.deduplicate_cycles(all_cycles)
    }

    /// Find cycles starting from a specific node using modified DFS
    async fn find_cycles_from_node(
        &self,
        graph: &petgraph::Graph<Address, crate::path::types::PoolEdge, petgraph::Undirected>,
        start_node: NodeIndex,
        start_token: Address,
        pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        block_number: Option<u64>,
        search_state: &mut CircularSearchState,
    ) -> Result<Vec<Path>, PathError> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut pool_path = Vec::new();

        self.dfs_find_cycles(
            graph,
            start_node,
            start_node,
            start_token,
            &mut visited,
            &mut path,
            &mut pool_path,
            &mut cycles,
            pool_manager,
            chain_name,
            block_number,
            0,
            search_state,
        ).await?;

        Ok(cycles)
    }

    /// Depth-first search for cycle detection
    #[async_recursion::async_recursion]
    async fn dfs_find_cycles(
        &self,
        graph: &petgraph::Graph<Address, crate::path::types::PoolEdge, petgraph::Undirected>,
        current_node: NodeIndex,
        start_node: NodeIndex,
        start_token: Address,
        visited: &mut HashSet<NodeIndex>,
        path: &mut Vec<Address>,
        pool_path: &mut Vec<Address>,
        cycles: &mut Vec<Path>,
        pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        block_number: Option<u64>,
        depth: usize,
        search_state: &mut CircularSearchState,
    ) -> Result<(), PathError> {
        // Check max depth
        if depth > self.max_cycle_length {
            return Ok(());
        }

        // Check safety termination conditions
        if search_state.should_terminate() {
            debug!(target: "circular_arbitrage", "DFS terminated early due to safety limits");
            return Ok(());
        }

        // Increment nodes explored counter
        search_state.increment_nodes_explored();
        
        // Add current node to path
        if let Some(current_token) = graph.node_weight(current_node) {
            path.push(*current_token);
            visited.insert(current_node);
            
            // Check if we've found a cycle
            if depth >= self.min_cycle_length - 1 && current_node == start_node && depth > 0 {
                // Validate and create the cycle
                if let Some(cycle_path) = self.validate_cycle(
                    path,
                    pool_path,
                    pool_manager.clone(),
                    chain_name,
                    block_number,
                ).await? {
                    cycles.push(cycle_path);
                    search_state.cycle_found(); // Record cycle found for safety tracking
                }
            } else if depth < self.max_cycle_length {
                // Continue exploring
                for edge in graph.edges(current_node) {
                    let neighbor = edge.target();
                    let pool = &edge.weight().pool_address;
                    
                    // Allow revisiting start node to complete cycle
                    if neighbor == start_node && depth >= self.min_cycle_length - 1 {
                        pool_path.push(*pool);
                        
                        // Try to complete the cycle
                        self.dfs_find_cycles(
                            graph,
                            neighbor,
                            start_node,
                            start_token,
                            visited,
                            path,
                            pool_path,
                            cycles,
                            pool_manager.clone(),
                            chain_name,
                            block_number,
                            depth + 1,
                            search_state,
                        ).await?;
                        
                        pool_path.pop();
                    } else if !visited.contains(&neighbor) {
                        pool_path.push(*pool);
                        
                        // Continue DFS
                        self.dfs_find_cycles(
                            graph,
                            neighbor,
                            start_node,
                            start_token,
                            visited,
                            path,
                            pool_path,
                            cycles,
                            pool_manager.clone(),
                            chain_name,
                            block_number,
                            depth + 1,
                            search_state,
                        ).await?;
                        
                        pool_path.pop();
                    }
                }
            }
            
            // Backtrack
            path.pop();
            visited.remove(&current_node);
        }
        
        Ok(())
    }

    /// Validate a cycle and calculate profitability
    async fn validate_cycle(
        &self,
        token_path: &[Address],
        pool_path: &[Address],
        pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<Option<Path>, PathError> {
        if token_path.len() < self.min_cycle_length || pool_path.len() < self.min_cycle_length - 1 {
            return Ok(None);
        }
        
        // Simulate the cycle with a test amount
        let test_amount = U256::from(10).pow(U256::from(18)); // 1 ETH equivalent
        let mut current_amount = test_amount;
        let mut legs = Vec::new();
        let mut fee_path = SmallVec::new();
        let mut protocol_path = SmallVec::new();
        
        for i in 0..pool_path.len() {
            let pool_address = pool_path[i];
            let token_in = token_path[i];
            let token_out = token_path[(i + 1) % token_path.len()];
            
            // Get pool info
            let pool_info = pool_manager
                .get_pool_info(chain_name, pool_address, block_number)
                .await
                .map_err(|e| PathError::PoolManager(e.to_string()))?;
            
            // Calculate output using dex_math
            let output = crate::dex_math::get_amount_out(&pool_info, token_in, current_amount)
                .map_err(|e| PathError::Calculation(e.to_string()))?;
            
            legs.push(PathLeg {
                from_token: token_in,
                to_token: token_out,
                pool_address,
                protocol_type: pool_info.protocol_type.clone(),
                fee: pool_info.fee_bps,
                amount_in: current_amount,
                amount_out: output,
                requires_approval: false,
                wrap_native: false,
                unwrap_native: false,
            });
            
            fee_path.push(pool_info.fee_bps);
            protocol_path.push(pool_info.protocol_type);
            current_amount = output;
        }
        
        // Check profitability
        let profit_bps = if current_amount > test_amount {
            ((current_amount - test_amount) * U256::from(10000) / test_amount).as_u32()
        } else {
            0
        };
        
        if profit_bps < self.min_profit_bps {
            return Ok(None);
        }
        
        // Create the path
        let mut path = Path::new();
        path.token_path = token_path.iter().cloned().collect();
        path.pool_path = pool_path.iter().cloned().collect();
        path.fee_path = fee_path;
        path.protocol_path = protocol_path;
        path.initial_amount_in_wei = test_amount;
        path.expected_output = current_amount;
        path.min_output = current_amount * U256::from(99) / U256::from(100); // 1% slippage
        path.price_impact_bps = 0; // Will be calculated separately
        path.gas_estimate = self.estimate_cycle_gas(&legs);
        path.token_in = token_path[0];
        path.token_out = token_path[0]; // Circular
        path.legs = legs;
        path.confidence_score = 0.8; // Base confidence for cycles
        path.source = Some("circular_detector".to_string());
        
        // Update cache
        self.update_cycle_cache(token_path.to_vec(), pool_path.to_vec(), profit_bps, block_number.unwrap_or(0)).await;
        
        Ok(Some(path))
    }

    /// Get high-liquidity tokens to start cycle search
    async fn get_high_liquidity_tokens(
        &self,
        token_to_node: &dashmap::DashMap<Address, NodeIndex>,
        chain_name: &str,
    ) -> Vec<Address> {
        // Chain-specific base tokens
        let base_tokens = match chain_name {
            "ethereum" | "mainnet" => vec![
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7", // USDT
                "0x6B175474E89094C44Da98b954EedeAC495271d0F", // DAI
                "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", // WBTC
            ],
            "arbitrum" => vec![
                "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", // WETH (Arbitrum)
                "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", // USDC (Arbitrum)
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", // USDT (Arbitrum)
                "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", // DAI (Arbitrum)
            ],
            "polygon" => vec![
                "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", // WMATIC
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", // USDC (Polygon)
                "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", // USDT (Polygon)
                "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063", // DAI (Polygon)
            ],
            "optimism" => vec![
                "0x4200000000000000000000000000000000000006", // WETH (Optimism)
                "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", // USDC (Optimism)
                "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58", // USDT (Optimism)
            ],
            // Default to Ethereum mainnet tokens for unknown chains
            _ => vec![
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7", // USDT
                "0x6B175474E89094C44Da98b954EedeAC495271d0F", // DAI
            ],
        };
        
        let mut tokens = Vec::new();
        for token_str in base_tokens {
            if let Ok(addr) = token_str.parse::<Address>() {
                if token_to_node.contains_key(&addr) {
                    tokens.push(addr);
                }
            }
        }
        
        // Also add tokens from profitable cycles in cache
        for entry in self.cycle_cache.iter() {
            if let Some(first_token) = entry.value().tokens.first() {
                if !tokens.contains(first_token) {
                    tokens.push(*first_token);
                }
            }
        }
        
        tokens
    }

    /// Deduplicate cycles (same tokens in different order)
    fn deduplicate_cycles(&self, cycles: Vec<Path>) -> Result<Vec<Path>, PathError> {
        let mut unique_cycles: HashMap<String, Path> = HashMap::new();
        
        for cycle in cycles {
            // Create a canonical representation of the cycle
            let mut tokens = cycle.token_path.to_vec();
            
            // Direction-aware key: include first token anchor to preserve directionality
            let direction_anchor = tokens.first().copied().unwrap_or_default();
            // Normalize by rotation but keep direction anchor
            if let Some(anchor_pos) = tokens.iter().position(|t| *t == direction_anchor) {
                tokens.rotate_left(anchor_pos);
            }
            
            let key = format!(
                "{}:{}",
                direction_anchor,
                tokens.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join("-")
            );
            
            // Keep the most profitable version
            match unique_cycles.get(&key) {
                Some(existing) => {
                    if cycle.profit_estimate_usd > existing.profit_estimate_usd {
                        unique_cycles.insert(key, cycle);
                    }
                }
                None => {
                    unique_cycles.insert(key, cycle);
                }
            }
        }
        
        Ok(unique_cycles.into_values().collect())
    }

    /// Estimate gas cost for a cycle
    fn estimate_cycle_gas(&self, legs: &[PathLeg]) -> u64 {
        let base_gas = 21000u64;
        let per_swap_gas = legs.iter().map(|leg| {
            match leg.protocol_type {
                ProtocolType::UniswapV2 => 120_000,
                ProtocolType::UniswapV3 => 180_000,
                ProtocolType::Curve => 250_000,
                ProtocolType::Balancer => 200_000,
                _ => 150_000,
            }
        }).sum::<u64>();
        
        base_gas + per_swap_gas
    }

    /// Update cycle cache with profitability info
    async fn update_cycle_cache(
        &self,
        tokens: Vec<Address>,
        pools: Vec<Address>,
        profit_bps: u32,
        block_number: u64,
    ) {
        let profit = profit_bps as f64 / 10000.0;
        
        self.cycle_cache
            .entry(tokens.clone())
            .and_modify(|info| {
                info.last_profitable_block = block_number;
                info.pools = pools.clone();
                info.profit_history.push_back(profit);
                if info.profit_history.len() > 100 {
                    info.profit_history.pop_front();
                }
                let successful = info.profit_history.iter().filter(|&&p| p > 0.0).count();
                info.success_rate = successful as f64 / info.profit_history.len() as f64;
            })
            .or_insert_with(|| {
                let mut profit_history = VecDeque::new();
                profit_history.push_back(profit);
                CycleInfo {
                    tokens,
                    pools,
                    last_profitable_block: block_number,
                    profit_history,
                    success_rate: if profit > 0.0 { 1.0 } else { 0.0 },
                }
            });
    }

    /// Get cached profitable cycles
    pub async fn get_profitable_cycles(&self, min_success_rate: f64) -> Vec<Vec<Address>> {
        self.cycle_cache
            .iter()
            .filter(|entry| entry.value().success_rate >= min_success_rate)
            .map(|entry| entry.value().tokens.clone())
            .collect()
    }

    /// Get cached profitable cycles with complete information
    pub async fn get_profitable_cycles_with_pools(&self, min_success_rate: f64) -> Vec<(Vec<Address>, Vec<Address>)> {
        self.cycle_cache
            .iter()
            .filter(|entry| entry.value().success_rate >= min_success_rate)
            .map(|entry| {
                let info = entry.value();
                (info.tokens.clone(), info.pools.clone())
            })
            .collect()
    }

    /// Check if a specific cycle (by tokens and pools) was recently profitable
    pub async fn is_cycle_recently_profitable(&self, tokens: &[Address], block_number: u64, max_blocks_ago: u64) -> bool {
        if let Some(entry) = self.cycle_cache.get(tokens) {
            let info = entry.value();
            return info.last_profitable_block + max_blocks_ago >= block_number
                && info.success_rate > 0.5;
        }
        false
    }
}