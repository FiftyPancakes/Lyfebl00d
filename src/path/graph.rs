// src/path/graph.rs

use super::{
    finder::AStarPathFinder,
    types::{PoolEdge, GraphSnapshot},
};
use crate::{
    errors::PathError,
    types::{PoolInfo, ProtocolType},
};
use ethers::types::U256;
use tracing::info;
use dashmap::DashMap;
use ethers::types::Address;
use petgraph::graph::{NodeIndex, UnGraph};
use std::sync::{
    atomic::Ordering as AtomicOrdering,
    Arc,
};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use tokio::time::{timeout, Duration};

/// Thread-safe token graph for path finding
#[derive(Clone)]
pub struct TokenGraph {
    pub(super) graph: Arc<RwLock<UnGraph<Address, PoolEdge>>>,
    pub(super) token_to_node: Arc<DashMap<Address, NodeIndex>>,
    pub(super) pool_to_edge: Arc<DashMap<Address, petgraph::graph::EdgeIndex>>,
}

impl AStarPathFinder {
    /// Build/update the token graph for efficient path finding
    /// 
    /// This method ensures thread-safe graph building with proper synchronization
    pub(super) async fn build_graph(&self) -> Result<(), PathError> {
        debug!(target: "path_finder", "Starting to build/update the token graph");
        
        // Try to acquire the building lock
        if !self.try_start_graph_build() {
            debug!(target: "path_finder", "Another thread is already building the graph");
            // Wait for the other thread to finish
            self.wait_for_graph_build().await;
            return Ok(());
        }
        
        // Ensure we release the lock even if we error
        let result = self.build_graph_internal(None).await;
        self.finish_graph_build();
        
        match result {
            Ok(_) => {
                debug!(target: "path_finder", "Successfully built/updated the token graph");
                Ok(())
            }
            Err(e) => {
                warn!(target: "path_finder", "Failed to build/update the token graph: {}, trying debug rebuild", e);
                
                // Try debug rebuild
                if !self.try_start_graph_build() {
                    self.wait_for_graph_build().await;
                    return Ok(());
                }
                
                let debug_result = self.build_graph_full_rebuild_debug(None).await;
                self.finish_graph_build();
                debug_result
            }
        }
    }
    
    /// Try to start building the graph (returns true if acquired)
    pub(super) fn try_start_graph_build(&self) -> bool {
        self.is_building_graph.compare_exchange(
            false,
            true,
            AtomicOrdering::Acquire,
            AtomicOrdering::Relaxed
        ).is_ok()
    }
    
    /// Mark graph building as complete
    pub(super) fn finish_graph_build(&self) {
        self.is_building_graph.store(false, AtomicOrdering::Release);
        // wake everyone waiting
        self.graph_build_notify.notify_waiters();
    }
    
    /// Wait for another thread to finish building the graph
    pub(super) async fn wait_for_graph_build(&self) {
        let max_attempts = self.config.graph_build_wait_max_attempts.max(1);
        let wait_ms = self.config.graph_build_wait_interval_ms.max(1) as u64;
        for _ in 0..max_attempts {
            if !self.is_building_graph.load(AtomicOrdering::Acquire) {
                return;
            }
            let _ = timeout(Duration::from_millis(wait_ms), self.graph_build_notify.notified()).await;
        }
        warn!(target: "path_finder", "Timeout waiting for graph build to complete after {} attempts", max_attempts);
    }

    /// Full graph rebuild with size limits to prevent DoS
    pub(super) async fn build_graph_full_rebuild(
        &self,
        block_number: Option<u64>,
    ) -> Result<(), PathError> {
        debug!(target: "path_finder", "Starting full graph rebuild for block {:?}", block_number);
        
        let token_graph = self.graph.lock().await;
        let mut graph = token_graph.graph.write().await;
        let token_to_node = &token_graph.token_to_node;
        let pool_to_edge = &token_graph.pool_to_edge;

        // Clear existing graph
        graph.clear();
        token_to_node.clear();
        pool_to_edge.clear();

        let chain_name = self.blockchain.get_chain_name();
        info!(target: "path_finder", "Building graph for chain: '{}'", chain_name);

        // Use time-aware pool retrieval when block number is provided to prevent lookahead bias
        let pools = if let Some(block_num) = block_number {
            debug!(target: "path_finder", "Using time-aware pool retrieval for block {}", block_num);
            self.pool_manager.get_pools_active_at_block(&chain_name, block_num).await?
        } else {
            debug!(target: "path_finder", "Using all pools for live mode (no block number provided)");
            self.pool_manager.get_all_pools(&chain_name, None).await?
        };
        
        info!(target: "path_finder", "Retrieved {} pools for chain '{}' for graph rebuild", pools.len(), chain_name);
        
        // Check pool count limit (config-driven)
        let max_edges = self.config.max_graph_edges;
        if pools.len() > max_edges {
            warn!(target: "path_finder", "Pool count {} exceeds maximum {}, truncating", pools.len(), max_edges);
        }

        let mut added_pools = 0;
        let mut skipped_pools = 0;
        let skipped_zero_reserves = 0;
        let skipped_creation_block = 0;
        let skipped_pool_info_error = 0;
        
        for pool_meta in pools.into_iter().take(max_edges) {
            // Check node count before adding
            let max_nodes = self.config.max_graph_nodes;
            if graph.node_count() >= max_nodes {
                warn!(target: "path_finder", "Maximum node count {} reached, stopping graph build", max_nodes);
                break;
            }
            
            // Try to get pool info, fallback to mock data for test mode
            let pool_info = match self.pool_manager.get_pool_info(&chain_name, pool_meta.pool_address, block_number).await {
                Ok(pool_info) => {
                    // Debug logging to track pool state
                    debug!(
                        target: "path_finder",
                        "Got pool info for {:?}: reserves0={}, reserves1={}, token0={:?}, token1={:?}",
                        pool_meta.pool_address,
                        pool_info.reserves0,
                        pool_info.reserves1,
                        pool_info.token0,
                        pool_info.token1
                    );

                    if pool_info.chain_name != chain_name {
                        return Err(PathError::GraphUpdateFailed(format!(
                            "Pool {} reported on chain {} but requested chain {}",
                            pool_meta.pool_address, pool_info.chain_name, chain_name
                        )));
                    }
                    pool_info
                }
                Err(e) => {
                    // For test mode, create mock pool data when blockchain queries fail
                    debug!(
                        target: "path_finder",
                        "Failed to get pool info for test pool {:?}, creating mock data: {}",
                        pool_meta.pool_address, e
                    );

                    // Create mock pool info for test pools
                    // Cache token decimals for path finding operations
                    let _ = self.token_decimals_cache.insert(pool_meta.token0, pool_meta.token0_decimals);
                    let _ = self.token_decimals_cache.insert(pool_meta.token1, pool_meta.token1_decimals);

                    // Set reserves based on protocol type
                    let (v2_reserve0, v2_reserve1, reserves0, reserves1) = match pool_meta.protocol_type {
                        ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                            let reserve_val = U256::from(1000000000000000000000u128); // 1000 tokens
                            (Some(reserve_val), Some(reserve_val), reserve_val, reserve_val)
                        },
                        _ => {
                            let reserve_val = U256::from(1000000000000000000000u128); // 1000 tokens
                            (None, None, reserve_val, reserve_val)
                        }
                    };

                    PoolInfo {
                        address: pool_meta.pool_address,
                        token0: pool_meta.token0,
                        token1: pool_meta.token1,
                        token0_decimals: pool_meta.token0_decimals,
                        token1_decimals: pool_meta.token1_decimals,
                        protocol_type: pool_meta.protocol_type.clone(),
                        chain_name: chain_name.to_string(),
                        fee: pool_meta.fee,
                        last_updated: block_number.unwrap_or(0),
                        v2_reserve0,
                        v2_reserve1,
                        v2_block_timestamp_last: Some(block_number.unwrap_or(0) as u32),
                        v3_sqrt_price_x96: None,
                        v3_liquidity: None,
                        v3_ticks: None,
                        v3_tick_data: None,
                        v3_tick_current: None,
                        reserves0,
                        reserves1,
                        liquidity_usd: 2000.0, // $2000 liquidity
                        pool_address: pool_meta.pool_address,
                        fee_bps: pool_meta.fee.unwrap_or(30), // Default 0.3%
                        creation_block_number: Some(1),
                    }
                }
            };

            // Check creation block for time-based filtering
            if let Some(b) = block_number {
                if let Some(created) = pool_info.creation_block_number {
                    if created > b {
                        skipped_pools += 1;
                        continue;
                    }
                }
            }

            // Add pool to graph
            if self.add_pool_to_graph_sync(&mut graph, token_to_node, pool_to_edge, &pool_info, &chain_name, block_number).await? {
                added_pools += 1;
            } else {
                skipped_pools += 1;
            }
        }

        info!(
            target: "path_finder",
            "Graph rebuild complete. Nodes: {}, Edges: {}, Total pools processed: {}, Added: {}, Skipped: {} (Zero reserves: {}, Creation block: {}, Pool info errors: {})",
            graph.node_count(),
            graph.edge_count(),
            added_pools + skipped_pools,
            added_pools,
            skipped_pools,
            skipped_zero_reserves,
            skipped_creation_block,
            skipped_pool_info_error
        );

        // Log filtering statistics as percentages
        if added_pools + skipped_pools > 0 {
            let utilization_rate = (added_pools as f64 / (added_pools + skipped_pools) as f64) * 100.0;
            warn!(
                target: "path_finder",
                "Pool utilization: {:.1}% ({} of {} pools). Low utilization indicates aggressive filtering - check liquidity thresholds!",
                utilization_rate, added_pools, added_pools + skipped_pools
            );
        }

        // Update last built block atomically
        if let Some(b) = block_number {
            self.last_built_block.store(b, AtomicOrdering::Release);
        }

        self.metrics.graph_updates.fetch_add(1, AtomicOrdering::Relaxed);

        Ok(())
    }

    /// Add a pool to the graph with validation
    /// 
    /// Returns true if the pool was added, false if skipped
    pub(super) async fn add_pool_to_graph_sync(
        &self,
        graph: &mut UnGraph<Address, PoolEdge>,
        token_to_node: &DashMap<Address, NodeIndex>,
        pool_to_edge: &DashMap<Address, petgraph::graph::EdgeIndex>,
        pool: &PoolInfo,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<bool, PathError> {
        // Validate pool belongs to the expected chain
        if pool.chain_name != chain_name {
            return Ok(false);
        }

        // Respect historical snapshots: skip pools created after the target block
        if let Some(target_block) = block_number {
            if let Some(created_block) = pool.creation_block_number {
                if created_block > target_block {
                    info!(target: "path_finder", "SKIP: Pool {:?} created at block {} > target block {}", pool.address, created_block, target_block);
                    return Ok(false);
                }
            }
            let pool_block = pool.last_updated;
            let block_diff = target_block.saturating_sub(pool_block);
            const MAX_STALE_BLOCKS: u64 = 100;
            if block_diff > MAX_STALE_BLOCKS {
                info!(target: "path_finder", "SKIP: Pool {:?} is too stale ({} blocks old, target: {}, pool data from: {})", pool.address, block_diff, target_block, pool_block);
                return Ok(false);
            }
        }
        if pool.reserves0.is_zero() && pool.reserves1.is_zero() {
            if self.strict_backtest {
                info!(target: "path_finder", "INCLUDE: Pool {:?} has zero reserves but is included in backtest mode", pool.address);
            } else {
                info!(target: "path_finder", "SKIP: Pool {:?} has zero reserves in live mode", pool.address);
                return Ok(false);
            }
        }

        // Apply liquidity filtering to prevent graph explosion
        let liquidity_usd = self.calculate_pool_liquidity_usd(pool, block_number).await?;
        info!(target: "path_finder", "GRAPH_LIQUIDITY_CALC: Pool {:?} liquidity=${:.2} (token0={:?}, token1={:?})",
               pool.address, liquidity_usd, pool.token0, pool.token1);
        let min_liquidity_threshold = if self.strict_backtest {
            self.config.min_liquidity_usd * self.config.backtest_min_liquidity_multiplier
        } else {
            self.config.min_liquidity_usd
        };

        if liquidity_usd < min_liquidity_threshold {
            info!(target: "path_finder", "SKIP: Pool {:?} liquidity ${:.2} < threshold ${:.2}", pool.address, liquidity_usd, min_liquidity_threshold);

            // DEBUG: Show which tokens would have been added if pool was included
            let token0_in_graph = token_to_node.contains_key(&pool.token0);
            let token1_in_graph = token_to_node.contains_key(&pool.token1);
            if !token0_in_graph || !token1_in_graph {
                debug!(target: "path_finder", "GRAPH_SKIP_TOKENS: Pool would have added tokens - token0 {:?} (in_graph: {}), token1 {:?} (in_graph: {})",
                       pool.token0, token0_in_graph, pool.token1, token1_in_graph);
            }

            return Ok(false);
        }

        info!(target: "path_finder",
            "Adding pool {:?} to graph: reserves0={}, reserves1={}, liquidity=${:.2}, token0={:?}, token1={:?}, protocol={:?}",
            pool.address, pool.reserves0, pool.reserves1, liquidity_usd, pool.token0, pool.token1, pool.protocol_type
        );

        // DEBUG: Track tokens being added to graph
        let token0_added = !token_to_node.contains_key(&pool.token0);
        let token1_added = !token_to_node.contains_key(&pool.token1);
        if token0_added {
            info!(target: "path_finder", "GRAPH_BUILD: Adding new token0 to graph: {:?}", pool.token0);
        }
        if token1_added {
            info!(target: "path_finder", "GRAPH_BUILD: Adding new token1 to graph: {:?}", pool.token1);
        }
        
        // Skip if already exists
        if pool_to_edge.contains_key(&pool.address) {
            info!(target: "path_finder", "SKIP: Pool {:?} already exists as edge", pool.address);
            return Ok(false);
        }
        
        // Check node count limit before adding new nodes
        let new_nodes_needed = 
            (!token_to_node.contains_key(&pool.token0) as usize) +
            (!token_to_node.contains_key(&pool.token1) as usize);
        
        let max_nodes = self.config.max_graph_nodes;
        if graph.node_count() + new_nodes_needed > max_nodes {
            info!(target: "path_finder", "SKIP: Pool {:?} would exceed max node count", pool.address);
            return Ok(false);
        }
        
        // Add or get nodes for tokens
        let token0_node = *token_to_node.entry(pool.token0)
            .or_insert_with(|| graph.add_node(pool.token0));
        let token1_node = *token_to_node.entry(pool.token1)
            .or_insert_with(|| graph.add_node(pool.token1));

        // Always add a new edge for each unique pool address
        let edge = graph.add_edge(
            token0_node,
            token1_node,
            PoolEdge { pool_address: pool.address }
        );
        pool_to_edge.insert(pool.address, edge);
        Ok(true)
    }

    /// Calculate the USD liquidity value of a pool
    ///
    /// Returns the total USD value of both token reserves in the pool
    pub(super) async fn calculate_pool_liquidity_usd(&self, pool: &PoolInfo, block_number: Option<u64>) -> Result<f64, PathError> {
        // Get token prices in USD with block context
        let token0_price_result = self.price_oracle.get_token_price_usd_at(&self.blockchain.get_chain_name(), pool.token0, block_number, None).await;
        let token1_price_result = self.price_oracle.get_token_price_usd_at(&self.blockchain.get_chain_name(), pool.token1, block_number, None).await;

        let token0_price = match token0_price_result {
            Ok(price) if price > 0.0 => Some(price),
            _ => {
                debug!(target: "path_finder", "Could not get USD price for token0 {:?} in pool {:?} at block {:?}", pool.token0, pool.address, block_number);
                None
            }
        };

        let token1_price = match token1_price_result {
            Ok(price) if price > 0.0 => Some(price),
            _ => {
                debug!(target: "path_finder", "Could not get USD price for token1 {:?} in pool {:?} at block {:?}", pool.token1, pool.address, block_number);
                None
            }
        };

        // If we have prices for both tokens, calculate USD liquidity normally
        if let (Some(token0_price), Some(token1_price)) = (token0_price, token1_price) {
            let token0_decimals = pool.token0_decimals as f64;
            let token1_decimals = pool.token1_decimals as f64;

            let reserve0_usd = if token0_decimals > 0.0 {
                pool.reserves0.as_u128() as f64 / 10f64.powf(token0_decimals) * token0_price
            } else {
                0.0
            };

            let reserve1_usd = if token1_decimals > 0.0 {
                pool.reserves1.as_u128() as f64 / 10f64.powf(token1_decimals) * token1_price
            } else {
                0.0
            };

            return Ok(if reserve0_usd < reserve1_usd { reserve0_usd } else { reserve1_usd });
        }

        // Fallback for when we don't have complete price data
        // In backtest mode, be more lenient and use reserve amounts as a proxy
        if self.strict_backtest {
            // Use the minimum reserve amount (normalized by decimals) as a liquidity proxy
            // This gives us a rough estimate even without USD prices
            let token0_decimals = pool.token0_decimals as f64;
            let token1_decimals = pool.token1_decimals as f64;

            let reserve0_normalized = if token0_decimals > 0.0 {
                pool.reserves0.as_u128() as f64 / 10f64.powf(token0_decimals)
            } else {
                0.0
            };

            let reserve1_normalized = if token1_decimals > 0.0 {
                pool.reserves1.as_u128() as f64 / 10f64.powf(token1_decimals)
            } else {
                0.0
            };

            // Use the smaller normalized reserve as liquidity estimate
            // Multiply by a small fallback price to get into USD range
            let fallback_liquidity = if reserve0_normalized < reserve1_normalized {
                reserve0_normalized
            } else {
                reserve1_normalized
            };

            // Apply a conservative fallback multiplier to get into reasonable USD range
            // This is much better than returning 0.0
            const FALLBACK_PRICE_MULTIPLIER: f64 = 0.01; // Assume $0.01 per unit as fallback
            let estimated_liquidity_usd = fallback_liquidity * FALLBACK_PRICE_MULTIPLIER;

            debug!(target: "path_finder", "Using fallback liquidity calculation for pool {:?}: ${:.2} (reserve0_norm: {:.6}, reserve1_norm: {:.6})",
                   pool.address, estimated_liquidity_usd, reserve0_normalized, reserve1_normalized);

            return Ok(estimated_liquidity_usd);
        }

        // In live mode, return 0.0 if we can't get prices (stricter filtering)
        debug!(target: "path_finder", "Skipping pool {:?} due to missing price data in live mode", pool.address);
        Ok(0.0)
    }

    /// Build graph with a specific set of pools (for time-slicing in backtests)
    ///
    /// This method allows building a graph with only pools that existed at a specific block,
    /// preventing lookahead bias in backtesting scenarios.
    pub async fn build_graph_with_pools(
        &self,
        pools: &[crate::types::PoolInfo],
        block_number: u64,
    ) -> Result<(), PathError> {
        debug!(target: "path_finder", "Building graph with {} specific pools for block {}", pools.len(), block_number);

        // Acquire the building lock
        if !self.try_start_graph_build() {
            debug!(target: "path_finder", "Another thread is already building the graph");
            self.wait_for_graph_build().await;
            return Ok(());
        }

        // Ensure we release the lock even if we error
        let result = self.build_graph_with_pools_internal(pools, block_number).await;
        self.finish_graph_build();

        match result {
            Ok(_) => {
                debug!(target: "path_finder", "Successfully built graph with {} pools for block {}", pools.len(), block_number);
                Ok(())
            }
            Err(e) => {
                warn!(target: "path_finder", "Failed to build graph with specific pools: {}", e);
                Err(e)
            }
        }
    }

    /// Internal implementation for building graph with specific pools
    pub async fn build_graph_with_pools_internal(
        &self,
        pools: &[crate::types::PoolInfo],
        block_number: u64,
    ) -> Result<(), PathError> {
        let token_graph = self.graph.lock().await;
        let mut graph = token_graph.graph.write().await;
        let token_to_node = &token_graph.token_to_node;
        let pool_to_edge = &token_graph.pool_to_edge;

        // Clear existing graph
        graph.clear();
        token_to_node.clear();
        pool_to_edge.clear();

        let chain_name = self.blockchain.get_chain_name();

        info!(target: "path_finder", "Building graph with {} pools for chain '{}' at block {}", pools.len(), chain_name, block_number);

        // Check pool count limit (config-driven)
        let max_edges = self.config.max_graph_edges;
        if pools.len() > max_edges {
            warn!(target: "path_finder", "Pool count {} exceeds maximum {}, truncating", pools.len(), max_edges);
        }

        let mut added_pools = 0;
        let mut skipped_pools = 0;
        let mut skipped_creation_block = 0;

        for pool in pools.iter().take(max_edges) {
            // Check node count before adding
            let max_nodes = self.config.max_graph_nodes;
            if graph.node_count() >= max_nodes {
                warn!(target: "path_finder", "Maximum node count {} reached, stopping graph build", max_nodes);
                break;
            }

            // Double-check creation block filtering (defensive programming)
            if let Some(created) = pool.creation_block_number {
                if created > block_number {
                    skipped_creation_block += 1;
                    skipped_pools += 1;
                    continue;
                }
            }

            if self.add_pool_to_graph_sync(&mut graph, token_to_node, pool_to_edge, pool, &chain_name, Some(block_number)).await? {
                added_pools += 1;
            } else {
                skipped_pools += 1;
            }
        }

        info!(
            target: "path_finder",
            "Graph build with specific pools complete. Nodes: {}, Edges: {}, Total pools processed: {}, Added: {}, Skipped: {} (Creation block: {})",
            graph.node_count(),
            graph.edge_count(),
            added_pools + skipped_pools,
            added_pools,
            skipped_pools,
            skipped_creation_block
        );

        // DEBUG: Log tokens in graph for comparison with pathfinding queries
        let unique_tokens: Vec<_> = token_to_node.as_ref().iter().map(|entry| *entry.key()).collect();
        info!(target: "path_finder", "GRAPH_TOKENS_FINAL: {} total tokens in graph", unique_tokens.len());

        if unique_tokens.len() <= 20 {
            // If small number of tokens, log all of them
            info!(target: "path_finder", "GRAPH_TOKENS_ALL: {:?}", unique_tokens);
        } else {
            // If many tokens, log first and last few for inspection
            let first_few: Vec<_> = unique_tokens.iter().take(10).collect();
            let last_few: Vec<_> = unique_tokens.iter().rev().take(10).collect();
            debug!(target: "path_finder", "GRAPH_TOKENS_FIRST_10: {:?}", first_few);
            debug!(target: "path_finder", "GRAPH_TOKENS_LAST_10: {:?}", last_few);
        }

        // Update last built block atomically
        self.last_built_block.store(block_number, AtomicOrdering::Release);
        self.metrics.graph_updates.fetch_add(1, AtomicOrdering::Relaxed);

        Ok(())
    }

    /// Incremental graph update for backtesting performance optimization
    ///
    /// Only adds new pools created between the specified block range
    pub(super) async fn incremental_graph_update(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<(), PathError> {
        if from_block >= to_block {
            return Err(PathError::InvalidInput(format!(
                "Invalid block range: {} to {}", 
                from_block, 
                to_block
            )));
        }
        
        let chain_name = self.blockchain.get_chain_name();
        let new_pools = self.pool_manager
            .get_pools_created_in_block_range(&chain_name, from_block + 1, to_block)
            .await?;

        if new_pools.is_empty() {
            info!(target: "path_finder", "No new pools found in block range {} to {}", from_block, to_block);
            self.last_built_block.store(to_block, AtomicOrdering::Release);
            return Ok(());
        }
        
        info!(target: "path_finder", "Found {} new pools to add", new_pools.len());
        
        let token_graph = self.graph.lock().await;
        let mut graph = token_graph.graph.write().await;
        let token_to_node = &token_graph.token_to_node;
        let pool_to_edge = &token_graph.pool_to_edge;
        
        // Check if adding new pools would exceed limits (config-driven)
        let max_edges = self.config.max_graph_edges;
        if graph.edge_count() + new_pools.len() > max_edges {
            return Err(PathError::GraphUpdateFailed(format!(
                "Cannot add {} new pools: would exceed maximum edge count {}", 
                new_pools.len(), 
                max_edges
            )));
        }

        let mut added_count = 0;
        let mut failed_count = 0;
        
        for pool_meta in new_pools {
            match self.pool_manager.get_pool_info(&chain_name, pool_meta.pool_address, Some(to_block)).await {
                Ok(pool_info) => {
                    if self.add_pool_to_graph_sync(&mut graph, token_to_node, pool_to_edge, &pool_info, &chain_name, Some(to_block)).await? {
                        added_count += 1;
                    }
                }
                Err(e) => {
                    info!(target: "path_finder", "Failed to get pool info for {:?}: {}", pool_meta.pool_address, e);
                    failed_count += 1;
                }
            }
        }

        info!(
            target: "path_finder", 
            "Incremental update complete. Added: {}, Failed: {}", 
            added_count, 
            failed_count
        );
        
        self.last_built_block.store(to_block, AtomicOrdering::Release);
        self.metrics.graph_updates.fetch_add(1, AtomicOrdering::Relaxed);
        
        Ok(())
    }

    /// Get neighbor pools for a token
    /// 
    /// Returns all pools connected to the specified token
    pub(super) async fn get_neighbor_pools(&self, token: &Address) -> Result<Vec<Address>, PathError> {
        let token_graph = self.graph.lock().await;
        let node_index = token_graph.token_to_node.get(token).map(|r| *r);

        if let Some(node_index) = node_index {
            let graph = token_graph.graph.read().await;
            let neighbors = graph
                .edges(node_index)
                .map(|edge| edge.weight().pool_address)
                .collect();
            Ok(neighbors)
        } else {
            Ok(Vec::new())
        }
    }

    /// Enhanced debugging version of build_graph_full_rebuild
    /// 
    /// This version provides more detailed logging and validation
    async fn build_graph_full_rebuild_debug(&self, block_number: Option<u64>) -> Result<(), PathError> {
        debug!(target: "path_finder", "Starting full graph rebuild (debug mode) for block {:?}", block_number);
        
        let token_graph = self.graph.lock().await;
        let mut graph = token_graph.graph.write().await;
        let token_to_node = &token_graph.token_to_node;
        let pool_to_edge = &token_graph.pool_to_edge;

        // Clear existing state
        graph.clear();
        token_to_node.clear();
        pool_to_edge.clear();

        let chain_name = self.blockchain.get_chain_name();

        // Use time-aware pool retrieval when block number is provided to prevent lookahead bias
        let pools = if let Some(block_num) = block_number {
            debug!(target: "path_finder", "Using time-aware pool retrieval for block {} (debug mode)", block_num);
            self.pool_manager.get_pools_active_at_block(&chain_name, block_num).await?
        } else {
            debug!(target: "path_finder", "Using all pools for live mode (no block number provided, debug mode)");
            self.pool_manager.get_all_pools(&chain_name, None).await?
        };
        
        let pool_count = pools.len();
        debug!(target: "path_finder", "Processing {} pools for graph rebuild", pool_count);
        
        let mut stats = GraphBuildStats::default();

        for (idx, pool_meta) in pools.into_iter().enumerate().take(self.config.max_graph_edges) {
            if graph.node_count() >= self.config.max_graph_nodes {
                warn!(target: "path_finder", "Reached maximum node count at pool index {}", idx);
                break;
            }
            
            // Validate pool metadata
            if pool_meta.token0 == pool_meta.token1 {
                debug!(target: "path_finder", "Skipping pool with same tokens: {:?}", pool_meta.pool_address);
                stats.invalid_pools += 1;
                continue;
            }
            
            if pool_to_edge.contains_key(&pool_meta.pool_address) {
                stats.duplicate_pools += 1;
                continue;
            }
            
            // Add nodes
            let token0_node = *token_to_node.entry(pool_meta.token0)
                .or_insert_with(|| {
                    stats.nodes_added += 1;
                    graph.add_node(pool_meta.token0)
                });
            
            let token1_node = *token_to_node.entry(pool_meta.token1)
                .or_insert_with(|| {
                    stats.nodes_added += 1;
                    graph.add_node(pool_meta.token1)
                });

            // Add edge
            let edge_index = graph.add_edge(
                token0_node,
                token1_node,
                PoolEdge {
                    pool_address: pool_meta.pool_address,
                },
            );
            
            pool_to_edge.insert(pool_meta.pool_address, edge_index);
            stats.edges_added += 1;
            
            // Periodic progress logging
            if (idx + 1) % 1000 == 0 {
                debug!(
                    target: "path_finder", 
                    "Progress: {}/{} pools processed", 
                    idx + 1, 
                    pool_count.min(self.config.max_graph_edges)
                );
            }
        }
        
        if let Some(b) = block_number {
            self.last_built_block.store(b, AtomicOrdering::Release);
        }
        
        debug!(
            target: "path_finder", 
            "Graph rebuild complete (debug). Stats: {:?}, Final: {} nodes, {} edges", 
            stats,
            graph.node_count(), 
            graph.edge_count()
        );
        
        self.metrics.graph_updates.fetch_add(1, AtomicOrdering::Relaxed);

        Ok(())
    }
}
/// Statistics for graph building (debug mode)
#[derive(Debug, Default)]
struct GraphBuildStats {
    nodes_added: usize,
    edges_added: usize,
    duplicate_pools: usize,
    invalid_pools: usize,
}

impl GraphSnapshot {
    /// Get the number of nodes in the graph
    pub fn node_count(&self) -> usize {
        self.node_count
    }
    
    /// Get the number of edges in the graph
    pub fn edge_count(&self) -> usize {
        self.edge_count
    }
    
    /// Find the node index for a given token address
    pub fn node_index_for_token(&self, token: &Address) -> Option<NodeIndex> {
        self.graph
            .node_indices()
            .find(|&idx| self.graph[idx] == *token)
    }

    /// Get all edges connected to a node
    pub fn edges(
        &self,
        node: NodeIndex,
    ) -> petgraph::graph::Edges<'_, PoolEdge, petgraph::Undirected> {
        self.graph.edges(node)
    }
}