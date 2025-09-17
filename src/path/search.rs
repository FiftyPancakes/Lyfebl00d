// src/path/search.rs

use super::{
    finder::AStarPathFinder,
    types::{Path, PathSearchNode, SearchStrategy, PoolEdge},
    PathFinder,
};
use crate::{
    errors::PathError,
    types::normalize_units,
    dex_math,
};
use crate::types::PoolInfo;
use tracing::{info, trace};
use ethers::types::{Address, U256};
use smallvec::smallvec;
use std::{
    cmp::{Reverse, Ordering},
    time::{Duration, Instant},
};
use tracing::{debug, instrument, warn};
use ahash::AHashMap;
use ahash::AHashSet;
use std::sync::atomic::Ordering as AtomicOrdering;

// Defaults retained but overridden by config at runtime
const DEFAULT_MAX_SEARCH_NODES: usize = 10_000;
const DEFAULT_MAX_FRONTIER_SIZE: usize = 5_000;

/// Search parameters for A* algorithm
#[derive(Debug, Clone)]
pub(super) struct SearchParameters {
    pub(super) max_routes: usize,
    pub(super) is_backtesting: bool,
}

/// Search state for tracking A* algorithm progress
#[derive(Debug)]
pub(super) struct SearchState {
    pub(super) start_time: Instant,
    pub(super) is_backtesting: bool,
    pub(super) nodes_without_improvement: usize,
    pub(super) best_profit_so_far: f64,
    pub(super) last_improvement_time: Instant,
    pub(super) nodes_processed: usize,
}

impl SearchState {
    /// Create a new search state
    pub(super) fn new(is_backtesting: bool) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            is_backtesting,
            nodes_without_improvement: 0,
            best_profit_so_far: f64::NEG_INFINITY,
            last_improvement_time: now,
            nodes_processed: 0,
        }
    }
    
    /// Update the best profit found so far
    pub(super) fn update_best_profit(&mut self, profit: f64) {
        if !profit.is_finite() {
            return;
        }
        
        if profit > self.best_profit_so_far {
            self.best_profit_so_far = profit;
            self.nodes_without_improvement = 0;
            self.last_improvement_time = Instant::now();
        } else {
            self.nodes_without_improvement += 1;
        }
    }
    
    /// Increment nodes processed counter
    pub(super) fn increment_nodes_processed(&mut self) {
        self.nodes_processed += 1;
    }
    
    /// Check if search should terminate based on various conditions
    pub(super) fn should_terminate(
        &self, 
        found_paths: usize, 
        config: &crate::config::PathFinderSettings
    ) -> bool {
        // Check time limit
        let max_time = if self.is_backtesting { Duration::from_secs(config.max_search_time_seconds_backtest) } else { Duration::from_secs(config.max_search_time_seconds) };
        
        if self.start_time.elapsed() > max_time {
            debug!(target: "path_finder", "Search terminated: max time reached ({:?})", max_time);
            return true;
        }
        
        // Check node limit using configured ceiling with a safety default
        let max_nodes_ceiling = if config.max_search_nodes_ceiling == 0 { DEFAULT_MAX_SEARCH_NODES } else { config.max_search_nodes_ceiling };
        if self.nodes_processed > config.max_processed_nodes.min(max_nodes_ceiling) {
            debug!(target: "path_finder", "Search terminated: max nodes processed ({})", self.nodes_processed);
            return true;
        }
        
        // Check stagnation
        let stagnation_threshold = if self.is_backtesting { 
            config.stagnation_threshold_backtest 
        } else { 
            config.stagnation_threshold_live 
        };
        
        if self.nodes_without_improvement > stagnation_threshold && found_paths > 0 {
            debug!(target: "path_finder", "Search terminated: stagnation detected ({} nodes without improvement)", self.nodes_without_improvement);
            return true;
        }
        
        // Check patience duration (config-driven)
        let patience_secs = if self.is_backtesting { config.patience_seconds_backtest } else { config.patience_seconds_live };
        let patience_duration = Duration::from_secs(patience_secs);
        
        if self.last_improvement_time.elapsed() > patience_duration && found_paths > 0 {
            debug!(target: "path_finder", "Search terminated: patience exhausted ({:?} since last improvement)", patience_duration);
            return true;
        }
        
        false
    }
}

impl AStarPathFinder {
    /// Enhanced A* search with parallel exploration
    /// 
    /// This method implements the A* pathfinding algorithm optimized for DeFi routing:
    /// - Uses multiple search strategies
    /// - Implements early termination to prevent runaway searches
    /// - Validates all inputs and outputs
    /// - Tracks metrics for performance monitoring
    #[instrument(skip(self), fields(token_in=%token_in, token_out=%token_out, amount_in=%amount_in, max_hops=%max_hops, block_number=?block_number), level = "debug")]
    pub(super) async fn a_star_search(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        max_hops: usize,
        strategy: SearchStrategy,
        block_number: Option<u64>,
    ) -> Result<Vec<Path>, PathError> {
        // Input validation
        if amount_in.is_zero() {
            debug!(target: "path_finder", "A* search called with zero amount_in");
            return Ok(Vec::new());
        }

        if max_hops == 0 || max_hops > 10 {
            return Err(PathError::InvalidInput(format!("Invalid max_hops: {}", max_hops)));
        }
        
        debug!(target: "path_finder", ?token_in, ?token_out, ?amount_in, max_hops, ?strategy, ?block_number, "Starting A* search");
        
        // Ensure graph is built for the target block
        self.build_graph_for_block(block_number).await?;
        
        // DEBUG: Validate tokens exist in graph with detailed logging
        let graph_data = self.graph.lock().await;
        let graph_tokens_count = graph_data.token_to_node.len();
        let graph_tokens_sample: Vec<_> = graph_data.token_to_node.iter().map(|entry| *entry.key()).take(5).collect();

        if !graph_data.token_to_node.contains_key(&token_in) {
            trace!(target: "path_finder", "PATHFIND_FAIL: Start token {:?} not present in graph (block: {:?}, graph has {} tokens)",
                  token_in, block_number, graph_tokens_count);
            trace!(target: "path_finder", "PATHFIND_FAIL: Graph token samples: {:?}", graph_tokens_sample);
            trace!(target: "path_finder", "PATHFIND_FAIL: Looking for token_in: {:?}", token_in);

            // DEBUG: Check if token was in any recent pools
            trace!(target: "path_finder", "PATHFIND_FAIL: Checking if token was recently active...");

            return Ok(vec![]);
        }

        if !graph_data.token_to_node.contains_key(&token_out) {
            warn!(target: "path_finder", "PATHFIND_FAIL: Target token {:?} not present in graph (block: {:?}, graph has {} tokens)",
                  token_out, block_number, graph_tokens_count);
            debug!(target: "path_finder", "PATHFIND_FAIL: Graph token samples: {:?}", graph_tokens_sample);
            debug!(target: "path_finder", "PATHFIND_FAIL: Looking for token_out: {:?}", token_out);

            return Ok(vec![]);
        }

        // DEBUG: Log successful token validation
        debug!(target: "path_finder", "PATHFIND_SUCCESS: Both tokens found in graph - in: {:?}, out: {:?} (graph size: {} tokens)",
               token_in, token_out, graph_tokens_count);

        // Acquire semaphore permit for rate limiting and hold until function exit
        let search_permit = self.semaphore.acquire().await
            .map_err(|e| PathError::Unexpected(format!("Failed to acquire search semaphore: {e}")))?;

        self.metrics.concurrent_operations.fetch_add(1, AtomicOrdering::Relaxed);

        // Get search parameters and pricing data
        let search_params = self.get_search_parameters().await;
        let (gas_price, eth_usd_price) = self.get_gas_and_eth_prices(
            search_params.is_backtesting,
            block_number,
            None
        ).await?;

        // DEBUG: Check frontier initialization
        let mut frontier = self.initialize_frontier(
            token_in,
            token_out,
            amount_in,
            block_number,
            &strategy,
            gas_price,
            eth_usd_price,
            search_params.is_backtesting,
        ).await?;

        debug!(target: "path_finder", "PATHFIND_FRONTIER: Initialized frontier with {} nodes", frontier.len());

        if frontier.is_empty() {
            debug!(target: "path_finder", "PATHFIND_FRONTIER: Frontier is empty, cannot start search");
            return Ok(Vec::new());
        }

        let start_time = Instant::now();
        
        let mut cost_so_far: AHashMap<(Address, u8), f64> = AHashMap::with_capacity(4096);
        cost_so_far.insert((token_in, 0), 0.0);

        let mut found_paths: Vec<Path> = Vec::with_capacity(search_params.max_routes);
        let mut state = SearchState::new(search_params.is_backtesting);
        let mut depth_expansion_counts: AHashMap<u8, usize> = AHashMap::with_capacity(64);
        let mut visited: AHashSet<(Address, u8, u64)> = AHashSet::with_capacity(4096);

        // Main search loop
        while let Some(Reverse(current)) = frontier.pop() {
            state.increment_nodes_processed();

            // Process current node
            // per-depth beam control is enforced in explore phase via depth_expansion_counts

            // visited signature: (last_token, depth, hash of pools)
            let poolset_hash: u64 = {
                use std::hash::{Hash, Hasher};
                let mut h = ahash::AHasher::default();
                for p in &current.pools { p.hash(&mut h); }
                h.finish()
            } as u64;
            let sig = (current.token, current.depth, poolset_hash);
            if !visited.insert(sig) {
                continue;
            }

            if let Some(path) = self.process_search_node(
                current.clone(),
                token_out,
                &mut cost_so_far,
                &mut state,
                &mut frontier,
                strategy,
                gas_price,
                eth_usd_price,
                block_number,
                max_hops,
                &mut depth_expansion_counts,
            ).await? {
                state.update_best_profit(path.profit_estimate_usd);
                found_paths.push(path);

                if found_paths.len() >= search_params.max_routes {
                    debug!(target: "path_finder", "Found maximum number of routes");
                    break;
                }
            }

            // Check termination conditions
            if self.should_terminate_search(&mut state, &found_paths, &mut frontier, start_time) {
                break;
            }

            // Prevent frontier from growing too large
            let max_frontier_size = if self.config.max_frontier_size == 0 { DEFAULT_MAX_FRONTIER_SIZE } else { self.config.max_frontier_size };
            if frontier.len() > max_frontier_size {
                debug!(target: "path_finder", "Frontier size limit reached, pruning");
                self.prune_frontier(&mut frontier, max_frontier_size / 2);
            }
        }

        // Sort paths by profit
        found_paths.sort_by(|a, b| {
            b.profit_estimate_usd
                .partial_cmp(&a.profit_estimate_usd)
                .unwrap_or(Ordering::Equal)
        });
        
        // Update metrics
        self.metrics.concurrent_operations.fetch_sub(1, AtomicOrdering::Relaxed);
        self.metrics.paths_found.fetch_add(found_paths.len() as u64, AtomicOrdering::Relaxed);
        self.metrics.search_time_ms.fetch_add(start_time.elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
        
        debug!(
            target: "path_finder", 
            "A* search complete: {} paths found in {:?}, {} nodes processed", 
            found_paths.len(), 
            start_time.elapsed(),
            state.nodes_processed
        );
        
        // Explicitly release concurrency permit at the end of the search
        drop(search_permit);
        Ok(found_paths)
    }

    /// Process a single search node
    pub(super) async fn process_search_node(
        &self,
        current: PathSearchNode,
        token_out: Address,
        cost_so_far: &mut AHashMap<(Address, u8), f64>,
        state: &mut SearchState,
        frontier: &mut std::collections::BinaryHeap<Reverse<PathSearchNode>>,
        strategy: SearchStrategy,
        gas_price: U256,
        eth_usd_price: f64,
        block_number: Option<u64>,
        max_hops: usize,
        depth_expansion_counts: &mut AHashMap<u8, usize>,
    ) -> Result<Option<Path>, PathError> {
        // Check if we've reached the target
        // For circular paths (when token_in == token_out), require at least 2 hops
        let min_depth_for_circular = if current.initial_amount_in_wei > U256::zero() && 
            !current.tokens.is_empty() && 
            current.tokens[0] == token_out { 
            2 
        } else { 
            1 
        };
        
        if current.token == token_out && current.depth >= min_depth_for_circular {
            return Ok(Some(self.construct_path_from_node_with_block(&current, block_number).await?));
        }

        // Check depth limit
        if current.depth as usize >= max_hops {
            return Ok(None);
        }
        
        // Get neighboring pools
        let neighbor_pools = self.get_neighbor_pools(&current.token).await?;
        
            // Limit neighbors adaptively based on search state to balance breadth and depth
            let max_neighbors = if state.is_backtesting { 30 } else { 20 };
            let neighbors_to_explore = neighbor_pools.len().min(max_neighbors);

        // Beam control per next depth
        let next_depth = current.depth + 1;
        let high_conn = neighbor_pools.len() >= self.config.high_connectivity_threshold;
        let beam_limit = if self.config.adaptive_beam_width {
            if high_conn { self.config.beam_width_high_connectivity } else { self.config.beam_width }
        } else {
            self.config.beam_width
        };

        for &pool_address in neighbor_pools.iter().take(neighbors_to_explore) {
            // Skip if we've already used this pool in current path
            if current.pools.contains(&pool_address) {
                continue;
            }
            
            match self.explore_neighbor(
                &current,
                pool_address,
                token_out,
                &strategy,
                gas_price,
                eth_usd_price,
                block_number,
                state.is_backtesting,
            ).await {
                Ok(Some((new_g_score, new_node))) => {
                    // Enforce beam width per depth
                    let count = depth_expansion_counts.entry(next_depth).or_insert(0);
                    if *count >= beam_limit { continue; }
                    let state_key = (new_node.token, new_node.depth);
                    
                    // Only add if this is a better path to this state
                    if new_g_score < *cost_so_far.get(&state_key).unwrap_or(&f64::MAX) {
                        cost_so_far.insert(state_key, new_g_score);
                        frontier.push(Reverse(new_node));
                        *count += 1;
                    }
                }
                Ok(None) => {
                    // Skip this neighbor
                }
                Err(e) => {
                    debug!(target: "path_finder", "Error exploring neighbor pool {:?}: {}", pool_address, e);
                }
            }
        }

        Ok(None)
    }

    /// Explore a neighbor node in the search
    pub(super) async fn explore_neighbor(
        &self,
        current: &PathSearchNode,
        pool_address: Address,
        target_token: Address,
        strategy: &SearchStrategy,
        gas_price: U256,
        eth_usd_price: f64,
        block_number: Option<u64>,
        is_backtesting: bool,
    ) -> Result<Option<(f64, PathSearchNode)>, PathError> {
        // Enforce strict mode: require historical context
        if self.strict_backtest && block_number.is_none() {
            return Err(PathError::InvalidInput("Block number required in strict backtest".to_string()));
        }
        // Calculate output for this hop
        let (output_amount, output_token, fee_bps, protocol) = match self
            .calculate_hop_output(pool_address, current.token, current.amount, block_number)
            .await 
        {
            Ok(res) => res,
            Err(e) => {
                info!(target: "path_finder", "SKIP_NEIGHBOR: Pool {:?} cannot be used for hop: {}", pool_address, e);
                return Ok(None);
            }
        };

        // Skip if output is zero or too small
        // In test environments, be more permissive with minimum output amounts
        let min_output_threshold = if is_backtesting {
            U256::from(10)  // Much more permissive for test environments
        } else {
            U256::from(1000)  // Keep strict for production
        };

        if output_amount.is_zero() || output_amount < min_output_threshold {
            info!(target: "path_finder", "SKIP_NEIGHBOR: Pool {:?} output is zero or below threshold (output: {}, threshold: {})", pool_address, output_amount, min_output_threshold);
            return Ok(None);
        }

        // Get pool info for price impact calculation
        let chain_name = self.blockchain.get_chain_name();
        let pool_info = match self.pool_manager.get_pool_info(&chain_name, pool_address, block_number).await {
            Ok(info) => info,
            Err(e) => {
                info!(target: "path_finder", "SKIP_NEIGHBOR: Pool {:?} failed to get pool info: {}", pool_address, e);
                return Ok(None);
            }
        };
        
        // Calculate and check price impact
        let price_impact = dex_math::calculate_price_impact(
            &pool_info, 
            current.token, 
            current.amount, 
            output_amount
        )?;
        
        if price_impact > U256::from(self.config.max_slippage_bps) {
            info!(target: "path_finder", "SKIP_NEIGHBOR: Pool {:?} price impact too high (impact: {}, max: {})", pool_address, price_impact, self.config.max_slippage_bps);
            return Ok(None);
        }

        // Calculate cost for this hop
        let cost = self.calculate_cost(
            current, 
            output_token, 
            output_amount, 
            &protocol, 
            gas_price, 
            eth_usd_price, 
            block_number, 
            strategy, 
            pool_info.liquidity_usd, 
            fee_bps
        ).await?;
        
        if !cost.is_finite() {
            info!(target: "path_finder", "SKIP_NEIGHBOR: Pool {:?} cost is not finite (cost: {})", pool_address, cost);
            return Ok(None);
        }

        // Calculate scores
        let new_g_score = current.g_score + cost;
        let decimals = match self.get_token_decimals(output_token, block_number).await {
            Ok(d) => d as u8,
            Err(_) => {
                // In test environments, use a reasonable default if decimals can't be retrieved
                if is_backtesting {
                    18  // Default to 18 decimals for test tokens
                } else {
                    return Ok(None);
                }
            },
        };
        let h_score = self.calculate_heuristic(
            output_token, 
            target_token, 
            normalize_units(output_amount, decimals), 
            strategy, 
            block_number
        ).await;

        // Build new node
        let mut new_tokens = current.tokens.clone();
        new_tokens.push(output_token);
        
        let mut new_pools = current.pools.clone();
        new_pools.push(pool_address);
        
        let mut new_fees = current.fees.clone();
        new_fees.push(fee_bps);
        
        let mut new_protocols = current.protocols.clone();
        new_protocols.push(protocol);

        let new_node = PathSearchNode {
            token: output_token,
            amount: output_amount,
            tokens: new_tokens,
            pools: new_pools,
            fees: new_fees,
            protocols: new_protocols,
            g_score: new_g_score,
            h_score,
            initial_amount_in_wei: current.initial_amount_in_wei,
            depth: current.depth + 1,
        };

        Ok(Some((new_g_score, new_node)))
    }

    /// Calculate heuristic score for A*
    /// 
    /// The heuristic estimates the remaining cost/profit from current to target
    pub(super) async fn calculate_heuristic(
        &self,
        current_token: Address,
        target_token: Address,
        current_amount_native: f64,
        strategy: &SearchStrategy,
        block_number: Option<u64>,
    ) -> f64 {
        const UNKNOWN_PRICE_PENALTY_PER_HOP: f64 = 1000.0;
        const MAX_PENALTY_HOPS: u32 = 5;

        if current_token == target_token {
            return 0.0;
        }

        match strategy {
            SearchStrategy::ShortPath => 1.0,
            _ => {
                let price_ratio_res = self
                    .get_price_ratio_cached(current_token, target_token, block_number)
                    .await;

                let price_ratio = match price_ratio_res {
                    Ok(r) if r.is_finite() && r > 0.0 => r,
                    _ => {
                        debug!(target: "path_finder", "Price oracle failed for tokens {:?} -> {:?}, calculating penalty.", current_token, target_token);
                        let hops_to_known_price = self
                            .find_hops_to_known_price(current_token, block_number, MAX_PENALTY_HOPS)
                            .await;
                        
                        // Apply penalty based on distance to a token with a known price
                        return UNKNOWN_PRICE_PENALTY_PER_HOP * hops_to_known_price as f64;
                    }
                };
                
                let expected_output_native = current_amount_native * price_ratio;
                let estimated_profit = expected_output_native - current_amount_native;
                
                -estimated_profit
            }
        }
    }

    /// Finds the minimum number of hops from a start token to any token with a known price.
    ///
    /// This function performs a breadth-first search (BFS) on the token graph to find the
    /// shortest path to a token for which the price oracle can return a valid price. It's used
    /// as a fallback for the A* heuristic when a direct price ratio cannot be found.
    ///
    /// # Arguments
    ///
    /// * `start_token` - The token to start the search from.
    /// * `block_number` - The block number for historical price checks.
    /// * `max_hops` - The maximum depth for the BFS to prevent excessively long searches.
    ///
    /// # Returns
    ///
    /// The number of hops to the nearest token with a known price, or `max_hops` if none is found.
    async fn find_hops_to_known_price(
        &self,
        start_token: Address,
        block_number: Option<u64>,
        max_hops: u32,
    ) -> u32 {
        let mut queue: std::collections::VecDeque<(Address, u32)> = std::collections::VecDeque::new();
        let mut visited: AHashSet<Address> = AHashSet::new();

        queue.push_back((start_token, 0));
        visited.insert(start_token);

        while let Some((current_token, hops)) = queue.pop_front() {
            if hops >= max_hops {
                continue;
            }

            // Check if the current token has a known price
            if self.price_oracle.get_token_price_usd_at(&self.blockchain.get_chain_name(), current_token, block_number, None).await.is_ok() {
                return hops;
            }

            // Explore neighbors
            if let Ok(neighbors) = self.get_neighbor_pools(&current_token).await {
                for &pool_address in &neighbors {
                    if let Ok((_, neighbor_token, _, _)) = self.calculate_hop_output(pool_address, current_token, U256::one(), block_number).await {
                        if visited.insert(neighbor_token) {
                            queue.push_back((neighbor_token, hops + 1));
                        }
                    }
                }
            }
        }

        max_hops
    }
    
    /// Initialize the search frontier with the starting node
    pub(super) async fn initialize_frontier(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        block_number: Option<u64>,
        strategy: &SearchStrategy,
        gas_price: U256,
        eth_usd_price: f64,
        is_backtesting: bool,
    ) -> Result<std::collections::BinaryHeap<Reverse<PathSearchNode>>, PathError> {
        debug!(target: "path_finder", "FRONTIER_INIT: Initializing frontier for token_in={:?}, token_out={:?}, amount_in={}", token_in, token_out, amount_in);

        let mut frontier = std::collections::BinaryHeap::new();

        let decimals = match self.get_token_decimals(token_in, block_number).await {
            Ok(d) => {
                debug!(target: "path_finder", "FRONTIER_INIT: Got decimals for token_in: {}", d);
                d as u8
            },
            Err(e) => {
                debug!(target: "path_finder", "FRONTIER_INIT: Failed to get decimals for token_in: {:?}", e);
                return Err(PathError::InvalidInput("Unknown token decimals".to_string()));
            }
        };

        let normalized_amount = normalize_units(amount_in, decimals);
        debug!(target: "path_finder", "FRONTIER_INIT: Normalized amount_in: {}", normalized_amount);

        let h_score = self.calculate_heuristic(
            token_in,
            token_out,
            normalized_amount,
            strategy,
            block_number,
        ).await;

        debug!(target: "path_finder", "FRONTIER_INIT: Calculated heuristic score: {}", h_score);

        let start_node = PathSearchNode {
            token: token_in,
            amount: amount_in,
            tokens: smallvec![token_in],
            pools: smallvec![],
            fees: smallvec![],
            protocols: smallvec![],
            g_score: 0.0,
            h_score,
            initial_amount_in_wei: amount_in,
            depth: 0,
        };

        // Initialize the frontier with all valid starting hops from the start_node
        let starting_pools = self.get_neighbor_pools(&start_node.token).await?;
        let token_graph = self.graph.lock().await;
        let graph_tokens: Vec<_> = token_graph.token_to_node.iter().map(|entry| *entry.key()).collect();
        info!(target: "path_finder", "FRONTIER_INIT: Graph tokens: {:?}", graph_tokens);
        info!(target: "path_finder", "FRONTIER_INIT: Neighbor pools for token_in {:?}: {:?}", start_node.token, starting_pools);
        debug!(target: "path_finder", "FRONTIER_INIT: Found {} starting pools for token_in", starting_pools.len());

        for &pool_address in starting_pools.iter() {
            if let Ok(Some((g_score, node))) = self.explore_neighbor(
                &start_node,
                pool_address,
                token_out,
                strategy,
                gas_price,
                eth_usd_price,
                block_number,
                is_backtesting,
            ).await {
                if g_score.is_finite() && node.h_score.is_finite() {
                    frontier.push(Reverse(node));
                }
            }
        }
        
        debug!(target: "path_finder", "FRONTIER_INIT: Frontier initialized with {} nodes", frontier.len());

        Ok(frontier)
    }

    /// Check if search should terminate
    pub(super) fn should_terminate_search(
        &self,
        state: &mut SearchState,
        found_paths: &[Path],
        frontier: &mut std::collections::BinaryHeap<Reverse<PathSearchNode>>,
        _start_time: Instant,
    ) -> bool {
        // Check if frontier is empty
        if frontier.is_empty() {
            debug!(target: "path_finder", "Search terminated: frontier is empty");
            return true;
        }

        // Check if we've found enough paths
        if found_paths.len() >= self.config.max_routes_to_consider {
            debug!(target: "path_finder", "Search terminated: max routes found");
            return true;
        }

        // Check state-based termination conditions
        if state.should_terminate(found_paths.len(), &self.config) {
            return true;
        }

        false
    }
    
    /// Prune the search frontier to prevent memory exhaustion
    /// 
    /// Keeps only the best nodes based on f-score
    fn prune_frontier(
        &self,
        frontier: &mut std::collections::BinaryHeap<Reverse<PathSearchNode>>,
        target_size: usize,
    ) {
        if frontier.len() <= target_size {
            return;
        }
        
        // Extract all nodes
        let mut nodes: Vec<PathSearchNode> = Vec::with_capacity(frontier.len());
        while let Some(Reverse(node)) = frontier.pop() {
            nodes.push(node);
        }
        
        // Sort by f-score and keep best
        nodes.sort_by(|a, b| {
            let f_a = a.g_score + a.h_score;
            let f_b = b.g_score + b.h_score;
            f_a.partial_cmp(&f_b).unwrap_or(Ordering::Equal)
        });
        
        // Re-add best nodes to frontier
        for node in nodes.into_iter().take(target_size) {
            frontier.push(Reverse(node));
        }
        
        debug!(target: "path_finder", "Pruned frontier to {} nodes", frontier.len());
    }

    /// Builds the graph with a given set of pools and ensures a specific set of tokens are included.
    pub async fn build_graph_with_pools_and_tokens(&self, pools: &[PoolInfo], tokens: Vec<Address>, block_number: u64) -> Result<(), PathError> {
        let mut graph_data = self.graph.lock().await;
        let mut graph = graph_data.graph.write().await;
        
        graph.clear();
        graph_data.token_to_node.clear();
        graph_data.pool_to_edge.clear();


        // First, ensure all tokens from the list are added to the graph
        for &token in &tokens {
            if !graph_data.token_to_node.contains_key(&token) {
                let node_index = graph.add_node(token);
                graph_data.token_to_node.insert(token, node_index);
            }
        }

        // Now add nodes and edges from pools
        for pool in pools {
            let token0_index = *graph_data
                .token_to_node
                .entry(pool.token0)
                .or_insert_with(|| graph.add_node(pool.token0));

            let token1_index = *graph_data
                .token_to_node
                .entry(pool.token1)
                .or_insert_with(|| graph.add_node(pool.token1));

            let edge = graph.add_edge(token0_index, token1_index, PoolEdge { pool_address: pool.address });
            graph_data.pool_to_edge.insert(pool.address, edge);
        }

        debug!(target: "path_finder", "Built graph for block {} with {} nodes and {} edges, including {} explicit tokens",
               block_number,
               graph.node_count(),
               graph.edge_count(),
               tokens.len());

        Ok(())
    }
}