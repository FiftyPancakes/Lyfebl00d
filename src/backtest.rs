// src/backtest.rs

//! # High‑Fidelity Backtesting Engine
//!
//! Production‑grade framework for replaying historical blocks and simulating
//! the full arbitrage stack under realistic market conditions.  The engine
//! creates sandboxed per‑chain infrastructure, enforces strict block context
//! on every dependency, models sophisticated MEV/competition dynamics, and
//! produces detailed performance reports.

use std::{
    collections::HashMap,
    sync::Arc,
    time::Instant,
    ops::Div,
    str::FromStr,
};
use crate::types::ModelPerformance;
use crate::types::ProtocolType;
use anyhow::Result;
use async_trait::async_trait;

use ethers::types::{Address, H256, U256};

use rand::{Rng, SeedableRng, rngs::StdRng};
use rand_chacha::ChaCha20Rng;
// use rayon::prelude::*; // Removed in favor of Tokio-bounded async concurrency
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    sync::{RwLock, Mutex},
};
use parking_lot::RwLock as SyncRwLock;
use tracing::{info, debug, warn};
use futures;


use crate::{
    blockchain::{BlockchainManager, BlockchainManagerImpl},
    config::{BacktestConfig, Config, ExecutionMode, GasOracleConfig, ChainConfig},
    dex_math,
    errors::{BacktestError, ArbitrageError, PriceError, DexError},
    gas_oracle::{GasOracleProvider, HistoricalGasProvider, GasOracle},
    path::{AStarPathFinder, PathFinder},
    pool_manager::PoolManagerTrait,
    pool_states::PoolStateOracle,
    price_oracle::{PriceOracle, historical_price_provider::HistoricalPriceProvider},
    quote_source::AggregatorQuoteSource,
    risk::{RiskManager, RiskManagerImpl},
    strategies::SimulationEngine,
    swap_loader::HistoricalSwapLoader,
    transaction_optimizer::{TransactionOptimizerTrait, TransactionOptimizerV2},
    types::{
        ArbitrageEngine, ExecutionResult, MEVPrediction,
        RateEstimate, SwapEvent, TransactionContext, DexSwapEvent, ArbitrageMetrics, PoolInfo, ArbitrageOpportunity
    },
};

/// Infer token decimals from static pool metadata only. Returns None if not found.
async fn infer_token_decimals(
    pool_manager: &Arc<dyn PoolManagerTrait + Send + Sync>,
    chain_name: &str,
    token: Address,
) -> Option<u8> {
    if let Ok(pools) = pool_manager.get_all_pools(chain_name, None).await {
        for meta in pools {
            if meta.token0 == token { return Some(meta.token0_decimals); }
            if meta.token1 == token { return Some(meta.token1_decimals); }
        }
    }
    None
}

/// Outcome returned by discovery strategies, including accepted opportunities and rejection stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryOutcome {
    pub opportunities: Vec<ArbitrageOpportunity>,
    pub rejection_stats: std::collections::HashMap<String, u64>,
}

// Configuration constants to replace magic numbers
const MIN_PROFIT_THRESHOLD_USD: f64 = 0.10;
const MIN_CONFIDENCE_SCORE: f64 = 0.6;
const MIN_MEV_RESISTANCE: f64 = 0.5;
const DEFAULT_MAX_OPPORTUNITIES_PER_BLOCK: usize = 100;
const DEFAULT_MAX_TOKENS_PER_SCAN: usize = 50;
const DEFAULT_COMPETITION_BOT_COUNT: usize = 50;
#[allow(dead_code)]
const DEFAULT_MEV_ATTACK_PROBABILITY: f64 = 0.1;
const RESULT_STREAM_FLUSH_THRESHOLD: usize = 10;
#[allow(dead_code)]
const GRAPH_PRUNING_RETENTION_BLOCKS: u64 = 1000;
// Default USD notionals for scan sizing when not provided via config
const DEFAULT_SCAN_USD_NOTIONALS: [f64; 3] = [50.0, 200.0, 1000.0];
// Max number of token pairs to scan per block for cross-DEX strategy
const DEFAULT_MAX_PAIRS_PER_BLOCK: usize = 1000;

/// Bounded collection for opportunities to prevent unbounded memory growth
#[derive(Debug, Clone)]
pub struct BoundedOpportunityList {
    items: Vec<ArbitrageOpportunity>,
    max_size: usize,
}

impl BoundedOpportunityList {
    pub fn new(max_size: usize) -> Self {
        Self {
            items: Vec::with_capacity(max_size),
            max_size,
        }
    }
    
    pub fn push(&mut self, item: ArbitrageOpportunity) -> bool {
        if self.items.len() < self.max_size {
            self.items.push(item);
            true
        } else {
            false
        }
    }
    
    pub fn extend<I: IntoIterator<Item = ArbitrageOpportunity>>(&mut self, iter: I) -> usize {
        let mut added = 0;
        for item in iter {
            if self.push(item) {
                added += 1;
            } else {
                break;
            }
        }
        added
    }
    
    pub fn len(&self) -> usize {
        self.items.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    
    pub fn iter(&self) -> std::slice::Iter<'_, ArbitrageOpportunity> {
        self.items.iter()
    }
    
    pub fn into_vec(self) -> Vec<ArbitrageOpportunity> {
        self.items
    }
}

/// Configuration thresholds for backtest operations
#[derive(Debug, Clone)]
pub struct BacktestThresholds {
    pub min_profit_usd: f64,
    pub min_confidence_score: f64,
    pub min_mev_resistance: f64,
    pub max_opportunities_per_block: usize,
    pub max_tokens_per_scan: usize,
    pub scan_usd_amounts: Option<Vec<f64>>,
    pub max_pairs_per_block: usize,
}

impl Default for BacktestThresholds {
    fn default() -> Self {
        Self {
            min_profit_usd: MIN_PROFIT_THRESHOLD_USD,
            min_confidence_score: MIN_CONFIDENCE_SCORE,
            min_mev_resistance: MIN_MEV_RESISTANCE,
            max_opportunities_per_block: DEFAULT_MAX_OPPORTUNITIES_PER_BLOCK,
            max_tokens_per_scan: DEFAULT_MAX_TOKENS_PER_SCAN,
            scan_usd_amounts: Some(DEFAULT_SCAN_USD_NOTIONALS.to_vec()),
            max_pairs_per_block: DEFAULT_MAX_PAIRS_PER_BLOCK,
        }
    }
}

/// Async-safe random number generator
#[derive(Debug)]
pub struct AsyncRng {
    rng: Arc<Mutex<StdRng>>,
}

impl AsyncRng {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        }
    }
    
    pub async fn gen_range<T, R>(&self, range: R) -> T
    where
        T: rand::distributions::uniform::SampleUniform + PartialOrd,
        R: rand::distributions::uniform::SampleRange<T>,
    {
        let mut rng = self.rng.lock().await;
        rng.gen_range(range)
    }
}

// InfrastructurePool was removed to avoid confusion and potential resource leaks in backtests.

/// Result streaming for memory-efficient output
pub struct ResultStream {
    writer: Option<tokio::fs::File>,
    buffer: Vec<BlockResult>,
    flush_threshold: usize,
    output_path: std::path::PathBuf,
}

impl ResultStream {
    pub async fn new(output_path: std::path::PathBuf) -> Result<Self, BacktestError> {
        // Write streaming results to a distinct NDJSON file to avoid clobbering the summary
        let ndjson_path = output_path.with_extension("ndjson");
        let writer = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&ndjson_path)
            .await
            .map_err(|e| BacktestError::IO(format!("Failed to open output file: {}", e)))?;
        
        Ok(Self {
            writer: Some(writer),
            buffer: Vec::with_capacity(RESULT_STREAM_FLUSH_THRESHOLD),
            flush_threshold: RESULT_STREAM_FLUSH_THRESHOLD,
            output_path: ndjson_path,
        })
    }
    
    pub async fn push(&mut self, result: BlockResult) -> Result<(), BacktestError> {
        self.buffer.push(result);
        if self.buffer.len() >= self.flush_threshold {
            self.flush().await?;
        }
        Ok(())
    }
    
    pub async fn flush(&mut self) -> Result<(), BacktestError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        if let Some(writer) = &mut self.writer {
            use tokio::io::AsyncWriteExt;
            // NDJSON: one JSON object per line
            for item in self.buffer.drain(..) {
                let json = serde_json::to_string(&item)
                    .map_err(|e| BacktestError::Serialization(format!("Failed to serialize results: {}", e)))?;
                writer
                    .write_all(json.as_bytes())
                    .await
                    .map_err(|e| BacktestError::IO(format!("Failed to write results: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .await
                    .map_err(|e| BacktestError::IO(format!("Failed to write newline: {}", e)))?;
            }
            writer.flush().await
                .map_err(|e| BacktestError::IO(format!("Failed to flush writer: {}", e)))?;
        }
        
        Ok(())
    }
    
    pub async fn finalize(mut self) -> Result<(), BacktestError> {
        self.flush().await?;
        // Let the writer drop after flush; no explicit shutdown is required
        info!("Finalized result stream to: {:?}", self.output_path);
        Ok(())
    }
    
    pub fn output_path(&self) -> &std::path::Path {
        &self.output_path
    }
}

/// Checkpoint for progress persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestCheckpoint {
    pub chain: String,
    pub last_processed_block: u64,
    pub partial_results: Vec<BlockResult>,
    pub timestamp: u64,
}

impl BacktestCheckpoint {
    pub fn new(chain: String, block: u64, results: Vec<BlockResult>) -> Self {
        Self {
            chain,
            last_processed_block: block,
            partial_results: results,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Per‑chain back‑test configuration (start/end range and output file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestChainConfig {
    pub chain_name:          String,
    pub start_block:         u64,
    pub end_block:           u64,
    pub result_output_file:  std::path::PathBuf,
}

impl BacktestChainConfig {
    /// Validate the configuration parameters
    pub fn validate(&self) -> Result<(), BacktestError> {
        if self.start_block >= self.end_block {
            return Err(BacktestError::Validation(format!(
                "Invalid block range: start_block ({}) must be less than end_block ({})",
                self.start_block, self.end_block
            )));
        }
        
        if self.chain_name.is_empty() {
            return Err(BacktestError::Validation("Chain name cannot be empty".into()));
        }
        
        Ok(())
    }
}

/// Scan metrics for tracking effectiveness of different opportunity discovery methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanMetrics {
    pub proactive_opportunities: usize,
    pub reactive_opportunities: usize,
    pub hot_token_opportunities: usize,
    pub cross_dex_opportunities: usize,
    pub total_scan_time_ms: u64,
}

/// Per‑block execution report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResult {
    pub block_number:        u64,
    pub execution_time_ms:   u64,
    pub opportunities_found: usize,
    pub proactive_opportunities_found: usize,
    pub reactive_opportunities_found: usize,
    pub hot_token_opportunities_found: usize,
    pub cross_dex_opportunities_found: usize,
    pub trades_executed:     Vec<TradeDetail>,
    pub errors:              Vec<String>,
    pub mev_attacks:         Vec<MEVAttackResult>,
    pub competition_results: Vec<CompetitionResult>,
    pub scan_metrics:        ScanMetrics,
    pub rejection_stats:     std::collections::HashMap<String, u64>,
}

/// Trade‑level execution outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeDetail {
    pub opportunity:    ArbitrageOpportunity,
    pub success:        bool,
    pub profit_usd:     f64,
    pub gas_used:       U256,
    pub tx_hash:        Option<H256>,
    pub failure_reason: Option<String>,
    pub mev_prediction: Option<MEVPrediction>,
}

/// Outcome of a simulated MEV attack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEVAttackResult {
    pub opportunity_id:   String,
    pub attack_type:      String,
    pub success:          bool,
    pub profit_lost_usd:  f64,
}

/// Result of simulated bot competition for an opportunity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitionResult {
    pub opportunity_id:    String,
    pub we_win:            bool,
    pub winning_bot_skill: f64,
    pub latency_margin_ms: i64,
}

/// Strategy pattern for opportunity discovery
#[async_trait]
pub trait OpportunityDiscoveryStrategy: Send + Sync {
    async fn discover(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryOutcome, BacktestError>;

    /// Discover opportunities with current block pool states for intra-block simulation
    async fn discover_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        swap_index: usize,
    ) -> Result<DiscoveryOutcome, BacktestError>;

    fn name(&self) -> &str;
}

/// Proactive scanning strategy using graph analysis
pub struct ProactiveScanStrategy {
    thresholds: BacktestThresholds,
}

impl ProactiveScanStrategy {
    pub fn new(thresholds: BacktestThresholds) -> Self {
        Self { thresholds }
    }

    /// Filter pools to only include those created at or before the current block
    async fn get_block_valid_pools(
        &self,
        pool_manager: &Arc<dyn PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        current_block: u64,
    ) -> Result<Vec<crate::types::PoolInfo>, BacktestError> {
        let all_pools = pool_manager
            .get_all_pools(chain_name, None)
            .await
            .map_err(|e| BacktestError::PoolDiscovery(format!("Failed to get all pools: {}", e)))?;
        let total_count = all_pools.len();

        // Filter pools by creation block to prevent lookahead bias
        let valid_metadata: Vec<Arc<crate::pool_manager::PoolStaticMetadata>> = all_pools
            .into_iter()
            .filter(|pool| {
                // Only include pools that were created at or before the current block
                pool.creation_block <= current_block
            })
            .collect();

        debug!(
            "Filtered pools for block {}: {} total pools, {} valid (filtered {})",
            current_block,
            total_count,
            valid_metadata.len(),
            total_count.saturating_sub(valid_metadata.len())
        );

        // Convert metadata to full PoolInfo objects
        let mut valid_pools = Vec::new();
        for metadata in valid_metadata {
            match pool_manager.get_pool_info(chain_name, metadata.pool_address, Some(current_block)).await {
                Ok(pool_info) => valid_pools.push(pool_info),
                Err(e) => {
                    debug!("Failed to get pool info for {}: {}, skipping", metadata.pool_address, e);
                }
            }
        }

        Ok(valid_pools)
    }

    /// Infer token decimals from block-valid pool metadata only
    async fn infer_token_decimals_from_valid_pools(
        &self,
        valid_pools: &[crate::types::PoolInfo],
        token: Address,
    ) -> Option<u8> {
        for pool in valid_pools {
            if pool.token0 == token {
                return Some(pool.token0_decimals);
            }
            if pool.token1 == token {
                return Some(pool.token1_decimals);
            }
        }
        None
    }

    /// Comprehensive debug logging for troubleshooting token availability issues
    async fn log_comprehensive_debug_state(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        token_candidates: &[Address],
        _swap_tokens: &std::collections::HashSet<Address>,
        graph_snapshot: &crate::path::types::GraphSnapshot,
    ) {
        debug!(target: "backtest", "=== COMPREHENSIVE DEBUG STATE ===");

        // Pool state summary
        debug!(target: "backtest", "POOL_STATE: Block {} - {} pools processed", block, graph_snapshot.edge_count());

        // Token analysis
        let graph_token_set: std::collections::HashSet<_> = graph_snapshot.graph.node_weights().collect();
        let candidate_set: std::collections::HashSet<_> = token_candidates.iter().collect();

        let tokens_in_graph_not_candidates = graph_token_set.difference(&candidate_set).take(5);
        let candidates_not_in_graph = candidate_set.difference(&graph_token_set).take(5);

        debug!(target: "backtest", "TOKEN_ANALYSIS: Graph has {}, candidates has {}", graph_token_set.len(), candidate_set.len());
        debug!(target: "backtest", "TOKEN_ANALYSIS: Tokens in graph but not candidates: {:?}", tokens_in_graph_not_candidates.collect::<Vec<_>>());
        debug!(target: "backtest", "TOKEN_ANALYSIS: Candidates not in graph: {:?}", candidates_not_in_graph.collect::<Vec<_>>());

        // Price check for first few candidates using block-aware pricing
        let block_ts = infra
            .blockchain_manager
            .get_block(block.into())
            .await
            .ok()
            .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
            .unwrap_or(0);
        for (i, token) in token_candidates.iter().take(3).enumerate() {
            if let Ok(price) = infra
                .price_oracle
                .get_token_price_usd_at(&infra.blockchain_manager.get_chain_name(), *token, Some(block), Some(block_ts))
                .await
            {
                debug!(target: "backtest", "PRICE_CHECK: Candidate {} ({:?}) = ${:.6}", i, token, price);
            } else {
                debug!(target: "backtest", "PRICE_CHECK: Candidate {} ({:?}) = NO PRICE", i, token);
            }
        }

        debug!(target: "backtest", "=== END COMPREHENSIVE DEBUG STATE ===");
    }
}

#[async_trait]
impl OpportunityDiscoveryStrategy for ProactiveScanStrategy {
    async fn discover(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryOutcome, BacktestError> {
        // For backward compatibility, create empty pool states map
        let empty_pool_states = std::collections::HashMap::new();
        self.discover_with_pool_states(infra, block, swaps, &empty_pool_states, 0).await
    }

    async fn discover_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        _swap_index: usize,
    ) -> Result<DiscoveryOutcome, BacktestError> {
        let mut opportunities = BoundedOpportunityList::new(self.thresholds.max_opportunities_per_block);
        let rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        let chain_name = infra.blockchain_manager.get_chain_name().to_string();
        let chain_id = infra.blockchain_manager.get_chain_id();

        // Use current block pool states if available, otherwise get block-valid pools to prevent lookahead bias
        let valid_pools = if current_block_pool_states.is_empty() {
            self.get_block_valid_pools(&infra.pool_manager, &chain_name, block).await?
        } else {
            // Convert current block pool states to Vec for compatibility
            current_block_pool_states.values().cloned().collect()
        };

        // Graph is now built once per block in Backtester::process_block
        let path_finder_ref = infra
            .path_finder
            .as_any()
            .downcast_ref::<AStarPathFinder>()
            .ok_or_else(|| BacktestError::SwapLoad("PathFinder is not AStarPathFinder".into()))?;

        let graph_snapshot = path_finder_ref.get_graph_snapshot().await;
        if graph_snapshot.node_count() < 3 || graph_snapshot.edge_count() < 2 {
            return Ok(DiscoveryOutcome { opportunities: opportunities.into_vec(), rejection_stats });
        }

        // --- START FIX ---
        // The graph IS the source of truth for all tradeable tokens at this block.
        // Derive all candidate tokens directly from the graph's nodes.
        let mut token_candidates: Vec<Address> = graph_snapshot.graph.node_weights().copied().collect();

        // DEBUG: Log initial token candidates from graph
        info!(target: "backtest", "DISCOVERY_CANDIDATES: Found {} initial token candidates from graph nodes", token_candidates.len());

        // Optional: If you still want to prioritize, you can sort by degree,
        // but iterating over all nodes in the valid graph is often sufficient and simpler.
        // For example, you could still prioritize by swaps if you wish:
        let swap_tokens: std::collections::HashSet<Address> = swaps.iter().flat_map(|s| vec![s.token_in, s.token_out]).collect();
        token_candidates.sort_by_key(|token| !swap_tokens.contains(token)); // Prioritize tokens that were active in this block

        // DEBUG: Log swap activity for comparison
        info!(target: "backtest", "DISCOVERY_SWAPS: Found {} unique tokens in block swaps", swap_tokens.len());
        if swap_tokens.len() <= 10 {
            info!(target: "backtest", "DISCOVERY_SWAP_TOKENS: {:?}", swap_tokens);
        }

        // Limit the number of tokens to scan to avoid excessive processing time, using your configured threshold.
        let original_count = token_candidates.len();
        token_candidates.truncate(self.thresholds.max_tokens_per_scan);

        // DEBUG: Log final token candidates that will be scanned
        debug!(target: "backtest", "DISCOVERY_FINAL: Will scan {} tokens (truncated from {}, max_tokens_per_scan={})",
               token_candidates.len(), original_count, self.thresholds.max_tokens_per_scan);

        if token_candidates.len() <= 20 {
            debug!(target: "backtest", "DISCOVERY_TOKEN_LIST: {:?}", token_candidates);
        } else {
            let first_few: Vec<_> = token_candidates.iter().take(5).collect();
            let last_few: Vec<_> = token_candidates.iter().rev().take(5).collect();
            debug!(target: "backtest", "DISCOVERY_TOKEN_SAMPLE: first={:?}, last={:?}", first_few, last_few);
        }
        // --- END FIX ---

        // DEBUG: Summary of discovery setup
        info!(target: "backtest", "DISCOVERY_SETUP: Block {} - Graph: {} nodes, {} edges, {} token candidates, {} swap tokens",
              block, graph_snapshot.node_count(), graph_snapshot.edge_count(), token_candidates.len(), swap_tokens.len());

        // DEBUG: Log comprehensive state for troubleshooting
        if tracing::enabled!(tracing::Level::DEBUG) {
            self.log_comprehensive_debug_state(&infra, block, &token_candidates, &swap_tokens, &graph_snapshot).await;
        }

        // For each candidate token, compute scan amounts from USD notionals at this historical block
        for token in token_candidates.into_iter() {
            let block_ts = infra
                .blockchain_manager
                .get_block(block.into())
                .await
                .ok()
                .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
                .unwrap_or(0);

            // Prefer configured USD notionals; fallback to defaults
            let usd_notionals: Vec<f64> = self
                .thresholds
                .scan_usd_amounts
                .clone()
                .unwrap_or_else(|| DEFAULT_SCAN_USD_NOTIONALS.to_vec());

            // Resolve decimals from block-valid pool metadata only; no live queries in strict backtest
            let decimals = match self.infer_token_decimals_from_valid_pools(&valid_pools, token).await {
                Some(d) => d,
                None => { continue; }
            };

            // Resolve token USD price at block
            let token_price = match infra
                .price_oracle
                .get_token_price_usd_at(&chain_name, token, Some(block), Some(block_ts))
                .await
            {
                Ok(p) if p > 0.0 => p,
                _ => { continue; }
            };

            let mut scan_amounts: Vec<U256> = Vec::new();
            for usd in usd_notionals {
                if usd <= 0.0 { continue; }
                let normalized_tokens = usd / token_price;
                let units = normalized_tokens * (10u128.pow(decimals as u32) as f64);
                if !units.is_finite() || units <= 0.0 { continue; }
                scan_amounts.push(U256::from(units as u128));
            }

            for amount in &scan_amounts {
                if opportunities.len() >= self.thresholds.max_opportunities_per_block {
                    break;
                }

                match infra
                    .path_finder
                    .find_circular_arbitrage_paths(token, *amount, Some(block))
                    .await
                {
                    Ok(paths) => {
                        for mut path in paths {
                            // Per-path risk and validation at this block
                            path.calculate_confidence_score(&infra.pool_manager, &chain_name, Some(block)).await;
                            path.calculate_mev_resistance();
                            let validation = infra
                                .path_finder
                                .validate_path(&path, Some(block))
                                .await
                                .map_err(|e| BacktestError::PathValidation(format!("validate_path failed: {}", e)))?;
                            if !validation.is_valid {
                                continue;
                            }

                            if path.confidence_score < self.thresholds.min_confidence_score
                                || path.mev_resistance_score < self.thresholds.min_mev_resistance
                            {
                                continue;
                            }

                            if path.profit_estimate_usd <= self.thresholds.min_profit_usd {
                                continue;
                            }

                            // Convert to opportunity mirroring Backtester::path_to_opportunity
                            let token_in = path.token_path[0];
                            let estimated_gas_cost_wei = U256::from(path.gas_estimate);
                            let base_risk_score = 1.0 - path.confidence_score;
                            let mev_risk_score = 1.0 - path.mev_resistance_score;
                            let hop_risk_multiplier = 1.0 + (path.hop_count() as f64 - 1.0) * 0.1;
                            let volatility = match infra
                                .price_oracle
                                .get_token_volatility(&chain_name, token_in, 3600)
                                .await
                            {
                                Ok(v) => v,
                                Err(_) => { continue; }
                            };
                            let volatility_risk_multiplier = 1.0 + volatility.min(1.0);
                            let final_risk_score = ((base_risk_score + mev_risk_score)
                                * hop_risk_multiplier
                                * volatility_risk_multiplier
                                * 0.5)
                                .min(1.0);

                            let route = crate::types::ArbitrageRoute::from(&path);
                            let metadata = serde_json::json!({
                                "path_confidence_score": path.confidence_score,
                                "path_mev_resistance_score": path.mev_resistance_score,
                                "path_hop_count": path.hop_count(),
                                "strategy": "proactive_scan",
                                "chain_name": chain_name,
                            });

                            opportunities.push(ArbitrageOpportunity {
                                route,
                                amount_in: path.initial_amount_in_wei,
                                block_number: block,
                                block_timestamp: 0,
                                profit_usd: path.profit_estimate_usd,
                                profit_token_amount: path
                                    .expected_output
                                    .saturating_sub(path.initial_amount_in_wei),
                                profit_token: token_in,
                                estimated_gas_cost_wei,
                                risk_score: final_risk_score,
                                mev_risk_score: mev_risk_score.min(1.0),
                                simulation_result: None,
                                metadata: Some(metadata),
                                token_in,
                                token_out: path.token_path[0],
                                chain_id,
                            });
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }

        Ok(DiscoveryOutcome { opportunities: opportunities.into_vec(), rejection_stats })
    }
    
    fn name(&self) -> &str {
        "proactive_scan"
    }
}

/// Cross-DEX arbitrage discovery strategy
pub struct CrossDexStrategy {
    thresholds: BacktestThresholds,
}

impl CrossDexStrategy {
    pub fn new(thresholds: BacktestThresholds) -> Self {
        Self { thresholds }
    }

    /// Filter pools to only include those created at or before the current block
    async fn get_block_valid_pools(
        &self,
        pool_manager: &Arc<dyn PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        current_block: u64,
    ) -> Result<Vec<crate::types::PoolInfo>, BacktestError> {
        let all_pools = pool_manager
            .get_all_pools(chain_name, None)
            .await
            .map_err(|e| BacktestError::PoolDiscovery(format!("Failed to get all pools: {}", e)))?;
        let total_count = all_pools.len();

        // Filter pools by creation block to prevent lookahead bias
        let valid_metadata: Vec<Arc<crate::pool_manager::PoolStaticMetadata>> = all_pools
            .into_iter()
            .filter(|pool| {
                // Only include pools that were created at or before the current block
                pool.creation_block <= current_block
            })
            .collect();

        debug!(
            "CrossDexStrategy filtered pools for block {}: {} total pools, {} valid (filtered {})",
            current_block,
            total_count,
            valid_metadata.len(),
            total_count.saturating_sub(valid_metadata.len())
        );

        // Convert metadata to full PoolInfo objects
        let mut valid_pools = Vec::new();
        for metadata in valid_metadata {
            match pool_manager.get_pool_info(chain_name, metadata.pool_address, Some(current_block)).await {
                Ok(pool_info) => valid_pools.push(pool_info),
                Err(e) => {
                    debug!("Failed to get pool info for {}: {}, skipping", metadata.pool_address, e);
                }
            }
        }

        Ok(valid_pools)
    }

    /// Infer token decimals from block-valid pool metadata only
    async fn infer_token_decimals_from_valid_pools(
        &self,
        valid_pools: &[crate::types::PoolInfo],
        token: Address,
    ) -> Option<u8> {
        for pool in valid_pools {
            if pool.token0 == token {
                return Some(pool.token0_decimals);
            }
            if pool.token1 == token {
                return Some(pool.token1_decimals);
            }
        }
        None
    }
}

#[async_trait]
impl OpportunityDiscoveryStrategy for CrossDexStrategy {
    async fn discover(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryOutcome, BacktestError> {
        // For backward compatibility, create empty pool states map
        let empty_pool_states = std::collections::HashMap::new();
        self.discover_with_pool_states(infra, block, swaps, &empty_pool_states, 0).await
    }

    async fn discover_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        _current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        _swap_index: usize,
    ) -> Result<DiscoveryOutcome, BacktestError> {
        let mut opportunities = BoundedOpportunityList::new(self.thresholds.max_opportunities_per_block);
        let mut rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        let chain_name = infra.blockchain_manager.get_chain_name().to_string();
        let chain_id = infra.blockchain_manager.get_chain_id();

        // Build token universe from current block swaps
        let mut tokens: Vec<Address> = Vec::new();
        let mut seen: std::collections::HashSet<Address> = std::collections::HashSet::new();
        for sw in swaps {
            if seen.insert(sw.token_in) {
                tokens.push(sw.token_in);
            }
            if seen.insert(sw.token_out) {
                tokens.push(sw.token_out);
            }
        }

        // Fallback: if no swaps, derive candidates from current graph snapshot (top-degree nodes)
        if tokens.is_empty() {
            if let Some(path_finder_ref) = infra
                .path_finder
                .as_any()
                .downcast_ref::<AStarPathFinder>()
            {
                let graph_snapshot = path_finder_ref.get_graph_snapshot().await;
                let mut degree_by_token: Vec<(Address, usize)> = graph_snapshot
                    .graph
                    .node_indices()
                    .map(|idx| {
                        let token = graph_snapshot.graph[idx];
                        let degree = graph_snapshot.graph.edges(idx).count();
                        (token, degree)
                    })
                    .collect();
                degree_by_token.sort_by(|a, b| b.1.cmp(&a.1));
                for (token, _) in degree_by_token
                    .into_iter()
                    .take(self.thresholds.max_tokens_per_scan)
                {
                    if seen.insert(token) {
                        tokens.push(token);
                    }
                }
            }
        }

        // Pre-fetch block-valid pools once (prevents look-ahead and reduces IO)
        let valid_pools = match self.get_block_valid_pools(&infra.pool_manager, &chain_name, block).await {
            Ok(p) => p,
            Err(e) => {
                debug!("Failed to get valid pools for CrossDex: {}", e);
                Vec::new()
            }
        };

        // Pairwise scan limited by thresholds
        let mut pair_count = 0usize;
        let max_pairs_per_block = self.thresholds.max_pairs_per_block;
        for i in 0..tokens.len() {
            for j in (i + 1)..tokens.len() {
                if opportunities.len() >= self.thresholds.max_opportunities_per_block {
                    break;
                }
                if pair_count >= max_pairs_per_block {
                    break;
                }
                pair_count += 1;

                let token_a = tokens[i];
                let token_b = tokens[j];

                // Amount_in = 1 whole token A in its native decimals (fallback to 18)
                let dec_a = self.infer_token_decimals_from_valid_pools(&valid_pools, token_a).await.unwrap_or(18);
                let amount_in_a = U256::exp10(dec_a as usize);

                // Find best path A->B and B->A at this block
                let path_a_to_b = match infra
                    .path_finder
                    .find_optimal_path(token_a, token_b, amount_in_a, Some(block))
                    .await
                {
                    Ok(Some(p)) => p,
                    _ => continue,
                };

                let path_b_to_a = match infra
                    .path_finder
                    .find_optimal_path(token_b, token_a, path_a_to_b.expected_output, Some(block))
                    .await
                {
                    Ok(Some(p)) => p,
                    _ => continue,
                };

                let profit_wei = path_b_to_a
                    .expected_output
                    .saturating_sub(path_a_to_b.initial_amount_in_wei);
                if profit_wei <= U256::zero() { *rejection_stats.entry("non_profitable_cycle".to_string()).or_insert(0)+=1; continue; }

                // Convert profit to USD using historical price and token decimals
                let block_ts = infra
                    .blockchain_manager
                    .get_block(block.into())
                    .await
                    .ok()
                    .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
                    .unwrap_or(0);

                let profit_usd = match infra
                    .price_oracle
                    .get_token_price_usd_at(&chain_name, token_a, Some(block), Some(block_ts))
                    .await
                {
                    Ok(price) if price > 0.0 => {
                        let maybe_decimals = self.infer_token_decimals_from_valid_pools(&valid_pools, token_a).await;
                        if let Some(d) = maybe_decimals {
                            let normalized = crate::types::normalize_units(profit_wei, d);
                            normalized * price
                        } else {
                            continue;
                        }
                    }
                    _ => { continue; }
                };
                if profit_usd <= self.thresholds.min_profit_usd { *rejection_stats.entry("below_min_profit".to_string()).or_insert(0)+=1; continue; }

                // Confidence and validation
                let mut enhanced_a = path_a_to_b.clone();
                let mut enhanced_b = path_b_to_a.clone();
                enhanced_a
                    .calculate_confidence_score(&infra.pool_manager, &chain_name, Some(block))
                    .await;
                enhanced_b
                    .calculate_confidence_score(&infra.pool_manager, &chain_name, Some(block))
                    .await;
                enhanced_a.calculate_mev_resistance();
                enhanced_b.calculate_mev_resistance();
                if enhanced_a.confidence_score < self.thresholds.min_confidence_score
                    || enhanced_b.confidence_score < self.thresholds.min_confidence_score
                {
                    continue;
                }

                let validation_a = infra
                    .path_finder
                    .validate_path(&enhanced_a, Some(block))
                    .await
                    .map_err(|e| BacktestError::PathValidation(format!("validate_path A->B failed: {}", e)))?;
                let validation_b = infra
                    .path_finder
                    .validate_path(&enhanced_b, Some(block))
                    .await
                    .map_err(|e| BacktestError::PathValidation(format!("validate_path B->A failed: {}", e)))?;
                if !validation_a.is_valid || !validation_b.is_valid {
                    continue;
                }

                // Build combined route legs
                let mut combined_legs: Vec<crate::types::ArbitrageRouteLeg> = enhanced_a
                    .pool_path
                    .iter()
                    .zip(enhanced_a.token_path.windows(2))
                    .enumerate()
                    .map(|(idx, (pool, tokens))| crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossDEX".to_string(),
                        protocol_type: enhanced_a
                            .protocol_path
                            .get(idx)
                            .cloned()
                            .unwrap_or(ProtocolType::UniswapV2),
                        pool_address: *pool,
                        token_in: tokens[0],
                        token_out: tokens[1],
                        amount_in: enhanced_a.legs.get(idx).map(|l| l.amount_in).unwrap_or(enhanced_a.initial_amount_in_wei),
                        min_amount_out: enhanced_a.legs.get(idx).map(|l| l.amount_out).unwrap_or(enhanced_a.min_output),
                        fee_bps: enhanced_a.fee_path.get(idx).copied().unwrap_or(3000),
                        expected_out: Some(enhanced_a.legs.get(idx).map(|l| l.amount_out).unwrap_or(enhanced_a.expected_output)),
                    })
                    .collect();
                let mut legs_b: Vec<crate::types::ArbitrageRouteLeg> = enhanced_b
                    .pool_path
                    .iter()
                    .zip(enhanced_b.token_path.windows(2))
                    .enumerate()
                    .map(|(idx, (pool, tokens))| crate::types::ArbitrageRouteLeg {
                        dex_name: "CrossDEX".to_string(),
                        protocol_type: enhanced_b
                            .protocol_path
                            .get(idx)
                            .cloned()
                            .unwrap_or(ProtocolType::UniswapV2),
                        pool_address: *pool,
                        token_in: tokens[0],
                        token_out: tokens[1],
                        amount_in: enhanced_b.legs.get(idx).map(|l| l.amount_in).unwrap_or(enhanced_b.initial_amount_in_wei),
                        min_amount_out: enhanced_b.legs.get(idx).map(|l| l.amount_out).unwrap_or(enhanced_b.min_output),
                        fee_bps: enhanced_b.fee_path.get(idx).copied().unwrap_or(3000),
                        expected_out: Some(enhanced_b.legs.get(idx).map(|l| l.amount_out).unwrap_or(enhanced_b.expected_output)),
                    })
                    .collect();
                combined_legs.append(&mut legs_b);

                let route = crate::types::ArbitrageRoute {
                    legs: combined_legs,
                    initial_amount_in_wei: enhanced_a.initial_amount_in_wei,
                    amount_out_wei: enhanced_b.expected_output,
                    profit_usd,
                    estimated_gas_cost_wei: U256::from(enhanced_a.gas_estimate + enhanced_b.gas_estimate),
                    price_impact_bps: enhanced_a.price_impact_bps.saturating_add(enhanced_b.price_impact_bps),
                    route_description: format!("{}↔{}", token_a, token_b),
                    risk_score: 1.0 - ((enhanced_a.confidence_score + enhanced_b.confidence_score) / 2.0),
                    chain_id,
                    created_at_ns: (block as u128 * 1_000_000_000u128) as u64,
                    mev_risk_score: 1.0 - ((enhanced_a.mev_resistance_score + enhanced_b.mev_resistance_score) / 2.0),
                    flashloan_amount_wei: None,
                    flashloan_fee_wei: None,
                    token_in: token_a,
                    token_out: token_a,
                    amount_in: enhanced_a.initial_amount_in_wei,
                    amount_out: enhanced_b.expected_output,
                };

                opportunities.push(ArbitrageOpportunity {
                    route,
                    amount_in: enhanced_a.initial_amount_in_wei,
                    block_number: block,
                    block_timestamp: 0,
                    profit_usd,
                    profit_token_amount: profit_wei,
                    profit_token: token_a,
                    estimated_gas_cost_wei: U256::from(enhanced_a.gas_estimate + enhanced_b.gas_estimate),
                    risk_score: 1.0 - ((enhanced_a.confidence_score + enhanced_b.confidence_score) / 2.0),
                    mev_risk_score: 1.0 - ((enhanced_a.mev_resistance_score + enhanced_b.mev_resistance_score) / 2.0),
                    simulation_result: None,
                    metadata: Some(serde_json::json!({
                        "strategy": "cross_dex",
                        "chain_name": chain_name,
                        "block": block,
                    })),
                    token_in: token_a,
                    token_out: token_a,
                    chain_id,
                });
            }
        }

        Ok(DiscoveryOutcome { opportunities: opportunities.into_vec(), rejection_stats })
    }

    fn name(&self) -> &str {
        "cross_dex"
    }
}

/// Hot token arbitrage discovery strategy
pub struct HotTokenStrategy {
    thresholds: BacktestThresholds,
    hot_tokens: Vec<Address>,
}

impl HotTokenStrategy {
    pub fn new(thresholds: BacktestThresholds, hot_tokens: Vec<Address>) -> Self {
        Self { thresholds, hot_tokens }
    }

    /// Filter pools to only include those created at or before the current block
    async fn get_block_valid_pools(
        &self,
        pool_manager: &Arc<dyn PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        current_block: u64,
    ) -> Result<Vec<crate::types::PoolInfo>, BacktestError> {
        let all_pools = pool_manager
            .get_all_pools(chain_name, None)
            .await
            .map_err(|e| BacktestError::PoolDiscovery(format!("Failed to get all pools: {}", e)))?;
        let total_count = all_pools.len();

        // Filter pools by creation block to prevent lookahead bias
        let valid_metadata: Vec<Arc<crate::pool_manager::PoolStaticMetadata>> = all_pools
            .into_iter()
            .filter(|pool| {
                // Only include pools that were created at or before the current block
                pool.creation_block <= current_block
            })
            .collect();

        debug!(
            "HotTokenStrategy filtered pools for block {}: {} total pools, {} valid (filtered {})",
            current_block,
            total_count,
            valid_metadata.len(),
            total_count.saturating_sub(valid_metadata.len())
        );

        // Convert metadata to full PoolInfo objects
        let mut valid_pools = Vec::new();
        for metadata in valid_metadata {
            match pool_manager.get_pool_info(chain_name, metadata.pool_address, Some(current_block)).await {
                Ok(pool_info) => valid_pools.push(pool_info),
                Err(e) => {
                    debug!("Failed to get pool info for {}: {}, skipping", metadata.pool_address, e);
                }
            }
        }

        Ok(valid_pools)
    }

    /// Infer token decimals from block-valid pool metadata only
    async fn infer_token_decimals_from_valid_pools(
        &self,
        valid_pools: &[crate::types::PoolInfo],
        token: Address,
    ) -> Option<u8> {
        for pool in valid_pools {
            if pool.token0 == token {
                return Some(pool.token0_decimals);
            }
            if pool.token1 == token {
                return Some(pool.token1_decimals);
            }
        }
        None
    }
}

#[async_trait]
impl OpportunityDiscoveryStrategy for HotTokenStrategy {
    async fn discover(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryOutcome, BacktestError> {
        // For backward compatibility, create empty pool states map
        let empty_pool_states = std::collections::HashMap::new();
        self.discover_with_pool_states(infra, block, swaps, &empty_pool_states, 0).await
    }

    async fn discover_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        _swap_index: usize,
    ) -> Result<DiscoveryOutcome, BacktestError> {
        let mut opportunities = BoundedOpportunityList::new(self.thresholds.max_opportunities_per_block);
        let rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        // Merge configured hot tokens with tokens observed in this block's swaps
        let mut candidate_tokens: std::collections::HashSet<Address> = self.hot_tokens.iter().copied().collect();
        for sw in swaps {
            candidate_tokens.insert(sw.token_in);
            candidate_tokens.insert(sw.token_out);
        }

        for token in candidate_tokens {
            if opportunities.len() >= self.thresholds.max_opportunities_per_block {
                break;
            }
            if let Ok(hot_opps) = self.search_hot_token_opportunities_with_pool_states(infra, block, token, current_block_pool_states).await {
                opportunities.extend(hot_opps);
            }
        }

        Ok(DiscoveryOutcome { opportunities: opportunities.into_vec(), rejection_stats })
    }
    
    fn name(&self) -> &str {
        "hot_token"
    }
}

impl HotTokenStrategy {
    async fn search_hot_token_opportunities(
        &self,
       infra: &Arc<ChainInfra>,
        block: u64,
        hot_token: Address,
    ) -> Result<Vec<ArbitrageOpportunity>, BacktestError> {
        let chain_name = infra.blockchain_manager.get_chain_name().to_string();
        let chain_id = infra.blockchain_manager.get_chain_id();

        // Ensure token exists in current graph snapshot
        let path_finder_ref = infra
            .path_finder
            .as_any()
            .downcast_ref::<AStarPathFinder>()
            .ok_or_else(|| BacktestError::SwapLoad("PathFinder is not AStarPathFinder".into()))?;
        let graph_snapshot = path_finder_ref.get_graph_snapshot().await;
        let token_in_graph = graph_snapshot
            .graph
            .node_indices()
            .any(|idx| graph_snapshot.graph[idx] == hot_token);
        if !token_in_graph {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        // Compute scan amounts from USD notionals at this block for the hot token
        let block_ts = infra
            .blockchain_manager
            .get_block(block.into())
            .await
            .ok()
            .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
            .unwrap_or(0);

        let usd_notionals: Vec<f64> = self
            .thresholds
            .scan_usd_amounts
            .clone()
            .unwrap_or_else(|| DEFAULT_SCAN_USD_NOTIONALS.to_vec());

        // Get block-valid pools for filtering
        let valid_pools = match self.get_block_valid_pools(&infra.pool_manager, &chain_name, block).await {
            Ok(pools) => pools,
            Err(e) => {
                debug!("Failed to get valid pools for HotToken: {}", e);
                return Ok(Vec::new());
            }
        };

        // Resolve decimals from block-valid pools
        let decimals = match self.infer_token_decimals_from_valid_pools(&valid_pools, hot_token).await {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        let price = match infra
            .price_oracle
            .get_token_price_usd_at(&chain_name, hot_token, Some(block), Some(block_ts))
            .await
        {
            Ok(p) if p > 0.0 => p,
            _ => return Ok(Vec::new()),
        };

        let mut scan_amounts: Vec<U256> = Vec::new();
        for usd in usd_notionals {
            if usd <= 0.0 { continue; }
            let normalized = usd / price;
            let units = normalized * (10u128.pow(decimals as u32) as f64);
            if !units.is_finite() || units <= 0.0 { continue; }
            scan_amounts.push(U256::from(units as u128));
        }

        for amount in scan_amounts {
            let paths = match infra
                .path_finder
                .find_circular_arbitrage_paths(hot_token, amount, Some(block))
                .await
            {
                Ok(p) => p,
                Err(_) => continue,
            };

            for mut path in paths {
                path.calculate_confidence_score(&infra.pool_manager, &chain_name, Some(block)).await;
                path.calculate_mev_resistance();
                let validation = infra
                    .path_finder
                    .validate_path(&path, Some(block))
                    .await
                    .map_err(|e| BacktestError::PathValidation(format!("validate_path failed: {}", e)))?;
                if !validation.is_valid {
                    continue;
                }

                if path.confidence_score < self.thresholds.min_confidence_score
                    || path.mev_resistance_score < self.thresholds.min_mev_resistance
                    || path.profit_estimate_usd <= self.thresholds.min_profit_usd
                {
                    continue;
                }

                let route = crate::types::ArbitrageRoute::from(&path);
                let token_in = path.token_path[0];
                let metadata = serde_json::json!({
                    "strategy": "hot_token",
                    "chain_name": chain_name,
                });
                out.push(ArbitrageOpportunity {
                    route,
                    amount_in: path.initial_amount_in_wei,
                    block_number: block,
                    block_timestamp: 0,
                    profit_usd: path.profit_estimate_usd,
                    profit_token_amount: path
                        .expected_output
                        .saturating_sub(path.initial_amount_in_wei),
                    profit_token: token_in,
                    estimated_gas_cost_wei: U256::from(path.gas_estimate),
                    risk_score: (1.0 - path.confidence_score).min(1.0).max(0.0),
                    mev_risk_score: (1.0 - path.mev_resistance_score).min(1.0).max(0.0),
                    simulation_result: None,
                    metadata: Some(metadata),
                    token_in,
                    token_out: token_in,
                    chain_id,
                });
                if out.len() >= self.thresholds.max_opportunities_per_block {
                    break;
                }
            }
        }

        Ok(out)
    }

    async fn search_hot_token_opportunities_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        hot_token: Address,
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
    ) -> Result<Vec<ArbitrageOpportunity>, BacktestError> {
        let chain_name = infra.blockchain_manager.get_chain_name().to_string();
        let chain_id = infra.blockchain_manager.get_chain_id();

        // Ensure token exists in current graph snapshot
        let path_finder_ref = infra
            .path_finder
            .as_any()
            .downcast_ref::<AStarPathFinder>()
            .ok_or_else(|| BacktestError::SwapLoad("PathFinder is not AStarPathFinder".into()))?;
        let graph_snapshot = path_finder_ref.get_graph_snapshot().await;
        let token_in_graph = graph_snapshot
            .graph
            .node_indices()
            .any(|idx| graph_snapshot.graph[idx] == hot_token);
        if !token_in_graph {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        // Compute scan amounts from USD notionals at this block for the hot token
        let block_ts = infra
            .blockchain_manager
            .get_block(block.into())
            .await
            .ok()
            .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
            .unwrap_or(0);

        let usd_notionals: Vec<f64> = self
            .thresholds
            .scan_usd_amounts
            .clone()
            .unwrap_or_else(|| DEFAULT_SCAN_USD_NOTIONALS.to_vec());

        // Use current block pool states if available, otherwise get block-valid pools
        let valid_pools = if current_block_pool_states.is_empty() {
            match self.get_block_valid_pools(&infra.pool_manager, &chain_name, block).await {
                Ok(pools) => pools,
                Err(e) => {
                    debug!("Failed to get valid pools for HotToken: {}", e);
                    return Ok(Vec::new());
                }
            }
        } else {
            // Convert current block pool states to Vec for compatibility
            current_block_pool_states.values().cloned().collect()
        };

        // Resolve decimals from valid pools
        let decimals = match self.infer_token_decimals_from_valid_pools(&valid_pools, hot_token).await {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        let price = match infra
            .price_oracle
            .get_token_price_usd_at(&chain_name, hot_token, Some(block), Some(block_ts))
            .await
        {
            Ok(p) if p > 0.0 => p,
            _ => return Ok(Vec::new()),
        };

        let mut scan_amounts: Vec<U256> = Vec::new();
        for usd in usd_notionals {
            if usd <= 0.0 { continue; }
            let normalized = usd / price;
            let units = normalized * (10u128.pow(decimals as u32) as f64);
            if !units.is_finite() || units <= 0.0 { continue; }
            scan_amounts.push(U256::from(units as u128));
        }

        for amount in scan_amounts {
            let paths = match infra
                .path_finder
                .find_circular_arbitrage_paths(hot_token, amount, Some(block))
                .await
            {
                Ok(p) => p,
                Err(_) => continue,
            };

            for mut path in paths {
                path.calculate_confidence_score(&infra.pool_manager, &chain_name, Some(block)).await;
                path.calculate_mev_resistance();
                let validation = infra
                    .path_finder
                    .validate_path(&path, Some(block))
                    .await
                    .map_err(|e| BacktestError::PathValidation(format!("validate_path failed: {}", e)))?;
                if !validation.is_valid {
                    continue;
                }

                if path.confidence_score < self.thresholds.min_confidence_score
                    || path.mev_resistance_score < self.thresholds.min_mev_resistance
                    || path.profit_estimate_usd <= self.thresholds.min_profit_usd
                {
                    continue;
                }

                let route = crate::types::ArbitrageRoute::from(&path);
                let token_in = path.token_path[0];
                let metadata = serde_json::json!({
                    "strategy": "hot_token",
                    "chain_name": chain_name,
                });
                out.push(ArbitrageOpportunity {
                    route,
                    amount_in: path.initial_amount_in_wei,
                    block_number: block,
                    block_timestamp: 0,
                    profit_usd: path.profit_estimate_usd,
                    profit_token_amount: path
                        .expected_output
                        .saturating_sub(path.initial_amount_in_wei),
                    profit_token: token_in,
                    estimated_gas_cost_wei: U256::from(path.gas_estimate),
                    risk_score: 1.0 - path.confidence_score,
                    mev_risk_score: 1.0 - path.mev_resistance_score,
                    simulation_result: None,
                    metadata: Some(metadata),
                    token_in,
                    token_out: token_in,
                    chain_id,
                });
                if out.len() >= self.thresholds.max_opportunities_per_block {
                    break;
                }
            }
        }

        Ok(out)
    }
}

/// Reactive strategy for responding to DEX swaps
pub struct ReactiveSwapStrategy {
    thresholds: BacktestThresholds,
}

impl ReactiveSwapStrategy {
    pub fn new(thresholds: BacktestThresholds) -> Self {
        Self { thresholds }
    }

    /// Filter pools to only include those created at or before the current block
    async fn get_block_valid_pools(
        &self,
        pool_manager: &Arc<dyn PoolManagerTrait + Send + Sync>,
        chain_name: &str,
        current_block: u64,
    ) -> Result<Vec<crate::types::PoolInfo>, BacktestError> {
        let all_pools = pool_manager
            .get_all_pools(chain_name, None)
            .await
            .map_err(|e| BacktestError::PoolDiscovery(format!("Failed to get all pools: {}", e)))?;

        // Filter pools by creation block to prevent lookahead bias
        let valid_metadata: Vec<Arc<crate::pool_manager::PoolStaticMetadata>> = all_pools
            .into_iter()
            .filter(|pool| {
                // Only include pools that were created at or before the current block
                pool.creation_block <= current_block
            })
            .collect();

        debug!(
            "ReactiveSwapStrategy filtered pools for block {}: {} total pools, {} valid for this block",
            current_block,
            pool_manager.get_all_pools(chain_name, None).await.unwrap_or_default().len(),
            valid_metadata.len()
        );

        // Convert metadata to full PoolInfo objects
        let mut valid_pools = Vec::new();
        for metadata in valid_metadata {
            match pool_manager.get_pool_info(chain_name, metadata.pool_address, Some(current_block)).await {
                Ok(pool_info) => valid_pools.push(pool_info),
                Err(e) => {
                    debug!("Failed to get pool info for {}: {}, skipping", metadata.pool_address, e);
                }
            }
        }

        Ok(valid_pools)
    }

    /// Analyze the price impact of a swap to determine if it creates arbitrage opportunities
    async fn analyze_swap_impact(
        &self,
        swap: &SwapEvent,
        infra: &Arc<ChainInfra>,
        block: u64,
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
    ) -> Result<Option<f64>, BacktestError> {
        // Use intra-block pre-swap state if available; otherwise, load state from previous block
        let chain_name = infra.blockchain_manager.get_chain_name();
        let pool_state = if let Some(state) = current_block_pool_states.get(&swap.pool_address) {
            state.clone()
        } else {
            match infra
                .pool_manager
                .get_pool_info(&chain_name, swap.pool_address, Some(block.saturating_sub(1)))
                .await
            {
                Ok(pool_info) => pool_info,
                Err(e) => {
                    debug!(
                        "Failed to get pre-swap pool info for {} at block {}: {}",
                        swap.pool_address,
                        block.saturating_sub(1),
                        e
                    );
                    // As a last resort, attempt first-swap heuristic with correct decimals later
                    return self.analyze_first_swap_impact(swap, infra, block).await;
                }
            }
        };

        // Compare actual output vs. expected output from pool math using normalized units to avoid overflow
        let expected_out = match crate::dex_math::get_amount_out(&pool_state, swap.token_in, swap.amount_in) {
            Ok(v) => v,
            Err(e) => {
                debug!("Failed to compute expected_out for impact: {}", e);
                return Ok(None);
            }
        };
        if expected_out.is_zero() {
            return Ok(None);
        }
        // Normalize amounts using pool decimals
        let out_dec: u8 = if swap.token_in == pool_state.token0 {
            pool_state.token1_decimals
        } else {
            pool_state.token0_decimals
        };
        let expected_norm = crate::types::normalize_units(expected_out, out_dec);
        let actual_norm = crate::types::normalize_units(swap.amount_out, out_dec);
        if expected_norm == 0.0 { return Ok(None); }
        let impact = ((actual_norm - expected_norm).abs() / expected_norm) * 100.0;

        // Only consider significant price impacts (0.5% threshold)
        if impact > 0.5 {
            debug!(
                "Significant price impact detected: {:.2}% for swap in pool {} (block {})",
                impact, swap.pool_address, block
            );
            Ok(Some(impact))
        } else {
            debug!(
                "Minor price impact: {:.4}% for swap in pool {} (block {})",
                impact, swap.pool_address, block
            );
            Ok(None)
        }
    }

    /// Analyze the impact of what appears to be the first swap in a pool
    async fn analyze_first_swap_impact(
        &self,
        swap: &SwapEvent,
        infra: &Arc<ChainInfra>,
        block: u64,
    ) -> Result<Option<f64>, BacktestError> {
        // For the first swap, use actual pool decimals if obtainable; otherwise fallback conservatively
        let chain_name = infra.blockchain_manager.get_chain_name();
        let maybe_pool_info_prev = infra
            .pool_manager
            .get_pool_info(&chain_name, swap.pool_address, Some(block.saturating_sub(1)))
            .await
            .ok();
        let (dec_in, dec_out) = if let Some(pool) = maybe_pool_info_prev {
            let dec_in = if swap.token_in == pool.token0 { pool.token0_decimals } else { pool.token1_decimals };
            let dec_out = if swap.token_out == pool.token0 { pool.token0_decimals } else { pool.token1_decimals };
            (dec_in, dec_out)
        } else {
            (18u8, 18u8)
        };

        let amount_in_f64 = crate::types::normalize_units(swap.amount_in, dec_in);
        let amount_out_f64 = crate::types::normalize_units(swap.amount_out, dec_out);

        let significant_threshold = 10_000.0;

        if amount_in_f64 > significant_threshold || amount_out_f64 > significant_threshold {
            debug!(
                "First swap detected with significant amounts: in={:.2}, out={:.2} for pool {} (block {})",
                amount_in_f64, amount_out_f64, swap.pool_address, block
            );

            let estimated_impact = 5.0; // 5% estimated impact for first swaps

            debug!(
                "Estimated {:.2}% impact for first swap in pool {} (block {})",
                estimated_impact, swap.pool_address, block
            );

            Ok(Some(estimated_impact))
        } else {
            debug!(
                "First swap with minor amounts: in={:.2}, out={:.2} for pool {} (block {}), skipping",
                amount_in_f64, amount_out_f64, swap.pool_address, block
            );
            Ok(None)
        }
    }

    /// Find arbitrage paths triggered by a significant price impact
    async fn find_impact_arbitrage_paths(
        &self,
        swap: &SwapEvent,
        infra: &Arc<ChainInfra>,
        block: u64,
    ) -> Result<Vec<ArbitrageOpportunity>, BacktestError> {
        let mut opportunities = Vec::new();

        // Use the shared path finder from infrastructure instead of creating a new one
        let path_finder = infra.path_finder.clone();

        // Find circular arbitrage paths starting from the output token
        // This looks for paths that go from token_out back to token_in (reversing the swap)
        let paths = path_finder.find_circular_arbitrage_paths(
            swap.token_out,
            swap.amount_out,
            Some(block)
        ).await
        .map_err(|e| BacktestError::Infrastructure(format!("Path finding failed: {}", e)))?;

        debug!("Found {} potential arbitrage paths for impacted swap", paths.len());

        for path in paths {
            // Check if this path ends with the input token (reversing the swap)
            if path.token_path.last() == Some(&swap.token_in) {
                // Calculate potential profit for this path
                match path_finder.calculate_path_output_amount(&path, swap.amount_out, Some(block)).await {
                    Ok(expected_output) => {
                        // Calculate profit in USD using price oracle
                        let profit_wei = expected_output.saturating_sub(swap.amount_out);

                        let chain_name = infra.blockchain_manager.get_chain_name();
                        // Use pool decimals for the *profit token* (swap.token_in here)
                        let pool_info = match infra.pool_manager.get_pool_info(&chain_name, swap.pool_address, Some(block)).await {
                            Ok(pi) => pi,
                            Err(e) => {
                                debug!("Pool info unavailable for decimals: {}", e);
                                continue;
                            }
                        };
                        let decimals = if swap.token_in == pool_info.token0 { pool_info.token0_decimals } else { pool_info.token1_decimals };
                        let profit_tokens = crate::types::normalize_units(profit_wei, decimals);
                        let block_ts = infra
                            .blockchain_manager
                            .get_block(block.into())
                            .await
                            .ok()
                            .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
                            .unwrap_or(0);
                        let profit_usd = match infra.price_oracle.get_token_price_usd_at(&chain_name, swap.token_in, Some(block), Some(block_ts)).await {
                            Ok(price) => profit_tokens * price,
                            Err(e) => {
                                debug!("Failed to get token price for profit calculation: {}", e);
                                continue;
                            }
                        };

                        if profit_usd > self.thresholds.min_profit_usd {
                            debug!(
                                "Found profitable arbitrage path: profit={:.6} USD, legs={}",
                                profit_usd, path.legs.len()
                            );

                            // Calculate price impact
                            let price_impact = match path_finder.calculate_path_price_impact(&path, swap.amount_out, Some(block)).await {
                                Ok(impact_bps) => impact_bps,
                                Err(_) => 0,
                            };

                            // Convert PathLegs to ArbitrageRouteLegs with per-leg amounts
                            let arbitrage_legs: Vec<crate::types::ArbitrageRouteLeg> = path.legs.iter().map(|leg| {
                                crate::types::ArbitrageRouteLeg {
                                    dex_name: format!("{:?}", leg.protocol_type),
                                    protocol_type: leg.protocol_type.clone(),
                                    pool_address: leg.pool_address,
                                    token_in: leg.from_token,
                                    token_out: leg.to_token,
                                    amount_in: leg.amount_in,
                                    min_amount_out: leg.amount_out,
                                    fee_bps: leg.fee,
                                    expected_out: Some(leg.amount_out),
                                }
                            }).collect();

                            // Create opportunity from this path
                            let opportunity = ArbitrageOpportunity {
                                route: crate::types::ArbitrageRoute {
                                    legs: arbitrage_legs,
                                    initial_amount_in_wei: swap.amount_out,
                                    amount_out_wei: expected_output,
                                    profit_usd,
                                    estimated_gas_cost_wei: U256::from(150_000u64 * 30_000_000_000u64), // Rough gas estimate
                                    price_impact_bps: 50, // Default 50 bps impact
                                    route_description: format!("Reactive arbitrage triggered by swap in pool {}", swap.pool_address),
                                    risk_score: (1.0 - path.confidence_score).min(1.0).max(0.0),
                                    chain_id: infra.blockchain_manager.get_chain_id(),
                                    created_at_ns: block_ts.saturating_mul(1_000_000_000),
                                    mev_risk_score: (1.0 - path.mev_resistance_score).min(1.0).max(0.0),
                                    flashloan_amount_wei: None,
                                    flashloan_fee_wei: None,
                                    token_in: swap.token_out,
                                    token_out: swap.token_in,
                                    amount_in: swap.amount_out,
                                    amount_out: expected_output,
                                },
                                block_number: block,
                                block_timestamp: block_ts,
                                profit_usd,
                                profit_token_amount: profit_wei,
                                profit_token: swap.token_in,
                                estimated_gas_cost_wei: U256::from(150_000u64 * 30_000_000_000u64),
                                risk_score: (1.0 - path.confidence_score).min(1.0).max(0.0),
                                mev_risk_score: (1.0 - path.mev_resistance_score).min(1.0).max(0.0),
                                simulation_result: None,
                                token_in: swap.token_out,
                                token_out: swap.token_in,
                                amount_in: swap.amount_out,
                                chain_id: infra.blockchain_manager.get_chain_id(),
                                metadata: Some(serde_json::json!({
                                    "trigger_swap": {
                                        "tx_hash": swap.tx_hash,
                                        "pool_address": swap.pool_address,
                                        "token_in": swap.token_in,
                                        "token_out": swap.token_out,
                                        "amount_in": swap.amount_in,
                                        "price_impact": price_impact
                                    },
                                    "path_analysis": {
                                        "path_length": path.legs.len(),
                                        "expected_output": expected_output,
                                        "profit_wei": profit_wei
                                    }
                                })),
                            };

                            opportunities.push(opportunity);
                        } else {
                            debug!(
                                "Path not profitable: profit={:.6} USD (threshold: {:.6})",
                                profit_usd, self.thresholds.min_profit_usd
                            );
                        }
                    }
                    Err(e) => {
                        debug!("Failed to calculate path output: {}", e);
                    }
                }
            }
        }

        Ok(opportunities)
    }
}

#[async_trait]
impl OpportunityDiscoveryStrategy for ReactiveSwapStrategy {
    async fn discover(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryOutcome, BacktestError> {
        // For backward compatibility, create empty pool states map
        let empty_pool_states = std::collections::HashMap::new();
        self.discover_with_pool_states(infra, block, swaps, &empty_pool_states, 0).await
    }

    async fn discover_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        _current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        _swap_index: usize,
    ) -> Result<DiscoveryOutcome, BacktestError> {
        let mut opportunities = BoundedOpportunityList::new(self.thresholds.max_opportunities_per_block);
        let mut rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        debug!("ReactiveSwapStrategy analyzing {} swaps for block {}", swaps.len(), block);

        // First, ensure all swap tokens are in the path finder graph
        let mut unique_tokens = std::collections::HashSet::new();
        let mut unique_pools = std::collections::HashSet::new();

        for swap in swaps {
            if swap.block_number != block {
                continue;
            }
            unique_tokens.insert(swap.token_in);
            unique_tokens.insert(swap.token_out);
            unique_pools.insert(swap.pool_address);
        }

        // Use the shared path finder from infrastructure; graph was already built in process_block
        let _path_finder = infra.path_finder.clone();


        // Group swaps by transaction for analysis
        let mut swaps_by_tx: HashMap<H256, Vec<&SwapEvent>> = HashMap::new();
        for swap in swaps {
            if swap.block_number != block {
                continue;
            }
            swaps_by_tx.entry(swap.tx_hash).or_default().push(swap);
        }

        debug!("Analyzing {} transactions with swaps", swaps_by_tx.len());

        for (tx_hash, tx_swaps) in swaps_by_tx {
            debug!("Analyzing transaction {} with {} swaps", tx_hash, tx_swaps.len());

            // Analyze each swap for price impact and arbitrage opportunities
            for swap in &tx_swaps {
                // First analyze the price impact of this swap
                match self.analyze_swap_impact(swap, infra, block, _current_block_pool_states).await {
                    Ok(Some(impact)) => {
                        debug!(
                            "Swap {} has significant price impact: {:.2}%. Looking for arbitrage paths...",
                            swap.tx_hash, impact
                        );

                        // Find arbitrage opportunities triggered by this price impact
                        match self.find_impact_arbitrage_paths(swap, infra, block).await {
                            Ok(impact_opportunities) => {
                                if !impact_opportunities.is_empty() {
                                    debug!(
                                        "Found {} arbitrage opportunities triggered by swap {}",
                                        impact_opportunities.len(), swap.tx_hash
                                    );
                                    opportunities.extend(impact_opportunities);
                                    *rejection_stats.entry("impact_triggered_opportunities".to_string()).or_insert(0) += 1;
                                } else {
                                    *rejection_stats.entry("no_opportunities_for_impact".to_string()).or_insert(0) += 1;
                                }
                            }
                            Err(e) => {
                                warn!("Failed to find impact arbitrage paths for swap {}: {}", swap.tx_hash, e);
                                *rejection_stats.entry("impact_path_finding_failed".to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("Swap {} has minor price impact, skipping arbitrage analysis", swap.tx_hash);
                        *rejection_stats.entry("minor_price_impact".to_string()).or_insert(0) += 1;
                    }
                    Err(e) => {
                        warn!("Failed to analyze price impact for swap {}: {}", swap.tx_hash, e);
                        *rejection_stats.entry("price_impact_analysis_failed".to_string()).or_insert(0) += 1;
                    }
                }
            }

            // Also run the traditional DEX opportunity discovery for comparison
            let _dex_swaps: Vec<DexSwapEvent> = tx_swaps.iter().map(|sw| {
                DexSwapEvent {
                    chain_name: sw.chain_name.clone(),
                    protocol: sw.protocol.clone(),
                    transaction_hash: sw.tx_hash,
                    block_number: sw.block_number,
                    log_index: sw.log_index,
                    pool_address: sw.pool_address,
                    data: sw.data.clone(),
                    unix_ts: Some(sw.unix_ts),
                    token0: sw.token0,
                    token1: sw.token1,
                    price_t0_in_t1: None,
                    price_t1_in_t0: None,
                    reserve0: None,
                    reserve1: None,
                    transaction: None,
                    transaction_metadata: None,
                    token_in: Some(sw.token_in),
                    token_out: Some(sw.token_out),
                    amount_in: Some(sw.amount_in),
                    amount_out: Some(sw.amount_out),
                    pool_id: None,
                    buyer: None,
                    sold_id: None,
                    tokens_sold: None,
                    bought_id: None,
                    tokens_bought: None,
                    sqrt_price_x96: None,
                    liquidity: None,
                    tick: None,
                    token0_decimals: None,
                    token1_decimals: None,
                    fee_tier: None,
                    protocol_type: Some(ProtocolType::from(&sw.protocol)),
                }
            }).collect();

            // Find arbitrage opportunities using PathFinder for targeted discovery
            // Use the PathFinder directly instead of ArbitrageEngine for more efficient discovery
            if let Some(first_swap) = tx_swaps.first() {
                // Use PathFinder to find circular arbitrage paths starting from the swapped token
                // Determine a sensible amount_in based on token_out decimals (1 whole token)
                let dec_out = {
                    let chain_name = infra.blockchain_manager.get_chain_name();
                    // Try current block state first, else previous block
                    let pool_res_now = infra.pool_manager.get_pool_info(&chain_name, first_swap.pool_address, Some(block)).await;
                    let pool_dec = match pool_res_now {
                        Ok(pool) => if first_swap.token_out == pool.token0 { pool.token0_decimals } else { pool.token1_decimals },
                        Err(_) => {
                            match infra.pool_manager.get_pool_info(&chain_name, first_swap.pool_address, Some(block.saturating_sub(1))).await {
                                Ok(pool) => if first_swap.token_out == pool.token0 { pool.token0_decimals } else { pool.token1_decimals },
                                Err(_) => 18,
                            }
                        }
                    };
                    pool_dec
                };
                let one_unit_out = U256::exp10(dec_out as usize);
                match infra.path_finder.find_circular_arbitrage_paths(
                    first_swap.token_out,
                    one_unit_out,
                    Some(block)
                ).await {
                    Ok(paths) => {
                        debug!("Found {} circular arbitrage paths for tx {}", paths.len(), tx_hash);

                        // Convert paths to opportunities using the existing AlphaEngine approach
                        for path in &paths {
                            // Calculate potential profit and create opportunity
                            if let Ok(amount_out) = infra.path_finder.calculate_path_output_amount(&path, one_unit_out, Some(block)).await {
                                let profit_amount = amount_out.saturating_sub(one_unit_out);

                                // Convert profit to USD for threshold check
                                let block_ts = infra
                                    .blockchain_manager
                                    .get_block(block.into())
                                    .await
                                    .ok()
                                    .and_then(|opt| opt.map(|b| b.timestamp.as_u64()))
                                    .unwrap_or(0);
                                let price_res = infra.price_oracle.get_token_price_usd_at(&first_swap.chain_name, first_swap.token_out, Some(block), Some(block_ts)).await;
                                if let Ok(price_out) = price_res {
                                    // determine decimals for token_out
                                    let dec_out_for_norm = dec_out;
                                    let profit_tokens = crate::types::normalize_units(profit_amount, dec_out_for_norm);
                                    let profit_usd: f64 = profit_tokens * price_out;
                                    if profit_usd > self.thresholds.min_profit_usd {
                                        // Create opportunity using existing logic
                                        let opportunity = crate::types::ArbitrageOpportunity {
                                            route: crate::types::ArbitrageRoute {
                                                legs: path.legs.iter().map(|leg| {
                                                    crate::types::ArbitrageRouteLeg {
                                                        dex_name: format!("{:?}", leg.protocol_type),
                                                        protocol_type: leg.protocol_type.clone(),
                                                        pool_address: leg.pool_address,
                                                        token_in: leg.from_token,
                                                        token_out: leg.to_token,
                                                        amount_in: leg.amount_in,
                                                        min_amount_out: leg.amount_out,
                                                        fee_bps: leg.fee,
                                                        expected_out: Some(leg.amount_out),
                                                    }
                                                }).collect(),
                                                initial_amount_in_wei: one_unit_out,
                                                amount_out_wei: amount_out,
                                                profit_usd,
                                                estimated_gas_cost_wei: U256::from(300_000_000_000_000u64), // 300k gas * 1 gwei
                                                price_impact_bps: 50, // Default 50 bps impact
                                                route_description: format!("reactive_arbitrage_{}", first_swap.tx_hash),
                                                risk_score: (1.0f64 - 0.8f64).max(0.0f64).min(1.0f64),
                                                chain_id: infra.blockchain_manager.get_chain_id(),
                                                created_at_ns: block_ts.saturating_mul(1_000_000_000),
                                                mev_risk_score: (1.0f64 - 0.8f64).max(0.0f64).min(1.0f64),
                                                flashloan_amount_wei: None,
                                                flashloan_fee_wei: None,
                                                token_in: first_swap.token_in,
                                                token_out: first_swap.token_out,
                                                amount_in: one_unit_out,
                                                amount_out,
                                            },
                                            block_number: block,
                                            block_timestamp: first_swap.unix_ts as u64,
                                            profit_usd,
                                            profit_token_amount: profit_amount,
                                            profit_token: first_swap.token_out,
                                            estimated_gas_cost_wei: U256::from(300_000_000_000_000u64), // 300k gas * 1 gwei
                                            risk_score: (1.0f64 - 0.8f64).max(0.0f64).min(1.0f64),
                                            mev_risk_score: (1.0f64 - 0.8f64).max(0.0f64).min(1.0f64),
                                            simulation_result: None,
                                            metadata: Some(serde_json::json!({
                                                "strategy": "reactive_swap",
                                                "tx_hash": tx_hash,
                                                "path_id": format!("path_{}", first_swap.tx_hash),
                                                "confidence_score": 0.8,
                                                "execution_complexity": path.legs.len()
                                            })),
                                            token_in: first_swap.token_in,
                                            token_out: first_swap.token_out,
                                            amount_in: one_unit_out,
                                            chain_id: infra.blockchain_manager.get_chain_id(),
                                        };

                                        opportunities.push(opportunity);
                                        *rejection_stats.entry("path_finder_opportunities_found".to_string()).or_insert(0) += 1;
                                    } else {
                                        *rejection_stats.entry("path_finder_opportunity_below_threshold".to_string()).or_insert(0) += 1;
                                    }
                                }
                            }
                        }

                        if paths.is_empty() {
                            *rejection_stats.entry("no_path_finder_opportunities".to_string()).or_insert(0) += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to find circular arbitrage paths for tx {}: {}", tx_hash, e);
                        *rejection_stats.entry("path_finder_failed".to_string()).or_insert(0) += 1;
                    }
                }
            }
        }

        let final_opportunities = opportunities.into_vec();
        debug!("ReactiveSwapStrategy found {} total opportunities for block {}", final_opportunities.len(), block);

        Ok(DiscoveryOutcome {
            opportunities: final_opportunities,
            rejection_stats
        })
    }

    fn name(&self) -> &str {
        "reactive_swap"
    }
}

/// Discovery engine that orchestrates multiple strategies
pub struct DiscoveryEngine {
    strategies: Vec<Box<dyn OpportunityDiscoveryStrategy>>,
}

impl DiscoveryEngine {
    pub fn new(strategies: Vec<Box<dyn OpportunityDiscoveryStrategy>>) -> Self {
        Self { strategies }
    }

    pub async fn discover_all(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<(Vec<ArbitrageOpportunity>, std::collections::HashMap<String, u64>), BacktestError> {
        let mut all_opportunities = BoundedOpportunityList::new(DEFAULT_MAX_OPPORTUNITIES_PER_BLOCK);
        let mut rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        // Run strategies in parallel
        let futures: Vec<_> = self.strategies
            .iter()
            .map(|strategy| strategy.discover(infra, block, swaps))
            .collect();

        let results = futures::future::join_all(futures).await;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(outcome) => {
                    debug!(
                        "Strategy '{}' found {} opportunities",
                        self.strategies[i].name(),
                        outcome.opportunities.len()
                    );
                    all_opportunities.extend(outcome.opportunities);
                    for (k, v) in outcome.rejection_stats.into_iter() {
                        *rejection_stats.entry(k).or_insert(0) += v;
                    }
                }
                Err(e) => {
                    warn!(
                        "Strategy '{}' failed: {}",
                        self.strategies[i].name(),
                        e
                    );
                }
            }
        }

        Ok((all_opportunities.into_vec(), rejection_stats))
    }

    /// Discover opportunities with intra-block pool state tracking
    pub async fn discover_all_with_pool_states(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
        current_block_pool_states: &std::collections::HashMap<Address, crate::types::PoolInfo>,
        swap_index: usize,
    ) -> Result<(Vec<ArbitrageOpportunity>, std::collections::HashMap<String, u64>), BacktestError> {
        let mut all_opportunities = BoundedOpportunityList::new(DEFAULT_MAX_OPPORTUNITIES_PER_BLOCK);
        let mut rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        // Run strategies in parallel with current block pool states
        let futures: Vec<_> = self.strategies
            .iter()
            .map(|strategy| strategy.discover_with_pool_states(infra, block, swaps, current_block_pool_states, swap_index))
            .collect();

        let results = futures::future::join_all(futures).await;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(outcome) => {
                    debug!(
                        "Strategy '{}' found {} opportunities after swap {}",
                        self.strategies[i].name(),
                        outcome.opportunities.len(),
                        swap_index
                    );
                    all_opportunities.extend(outcome.opportunities);
                    for (k, v) in outcome.rejection_stats.into_iter() {
                        *rejection_stats.entry(k).or_insert(0) += v;
                    }
                }
                Err(e) => {
                    warn!(
                        "Strategy '{}' failed after swap {}: {}",
                        self.strategies[i].name(),
                        swap_index,
                        e
                    );
                }
            }
        }

        Ok((all_opportunities.into_vec(), rejection_stats))
    }
}

/// Simulated competing bot with configurable skill and latency.
#[derive(Debug, Clone)]
struct CompetingBot {
    skill_level:             f64,
    latency_ms:              u64,
    specialization:          BotSpecialization,
    gas_bidding_aggressive:  f64,
}

/// Bot specialization types for realistic competition simulation.
#[derive(Debug, Clone)]
enum BotSpecialization {
    General,
    CrossDex,
    FlashLoan,
    HighFreq,
    MEVHunter,
}

/// Simulates realistic bot competition for arbitrage opportunities.
pub struct CompetitionSimulator {
    bots: Vec<CompetingBot>,
    rng: AsyncRng,
}

impl CompetitionSimulator {
    pub fn new(cfg: &BacktestConfig, seed: u64) -> Self {
        let mut temp_rng = ChaCha20Rng::seed_from_u64(seed);
        let mut bots = Vec::new();

        let bot_count = cfg.competition_bot_count.unwrap_or(DEFAULT_COMPETITION_BOT_COUNT);
        let (skill_min, skill_max) = cfg.competition_skill_range.unwrap_or((0.3, 0.9));
        let (latency_min, latency_max) = cfg.competition_latency_range_ms.unwrap_or((50, 500));

        for _ in 0..bot_count {
            let skill = temp_rng.gen_range(skill_min..skill_max);
            let latency = temp_rng.gen_range(latency_min..latency_max);
            let specialization = match temp_rng.gen_range(0..5) {
                0 => BotSpecialization::General,
                1 => BotSpecialization::CrossDex,
                2 => BotSpecialization::FlashLoan,
                3 => BotSpecialization::HighFreq,
                _ => BotSpecialization::MEVHunter,
            };
            let gas_bidding = temp_rng.gen_range(0.8..1.5);

            bots.push(CompetingBot {
                skill_level: skill,
                latency_ms: latency,
                specialization,
                gas_bidding_aggressive: gas_bidding,
            });
        }

        Self {
            bots,
            rng: AsyncRng::new(seed),
        }
    }

    pub async fn simulate(&self, opp: &ArbitrageOpportunity) -> CompetitionResult {
        // Use async-safe RNG
        
        let our_skill = 0.85;
        let our_latency = 25;
        
        let mut best_bot_score = 0.0;
        let mut best_bot = None;
        
        for bot in &self.bots {
            let specialization_bonus = match bot.specialization {
                BotSpecialization::General => 0.0,
                BotSpecialization::CrossDex => {
                    if opp.route.legs.len() > 2 { 0.15 } else { 0.0 }
                },
                BotSpecialization::FlashLoan => {
                    if opp.route.flashloan_amount_wei.is_some() { 0.2 } else { 0.0 }
                },
                BotSpecialization::HighFreq => {
                    if opp.profit_usd > 100.0 { 0.1 } else { 0.0 }
                },
                BotSpecialization::MEVHunter => {
                    if opp.mev_risk_score > 0.5 { 0.25 } else { 0.0 }
                },
            };
            
            let gas_bidding_multiplier = if opp.route.estimated_gas_cost_wei > U256::from(50_000_000_000u64) {
                bot.gas_bidding_aggressive
            } else {
                1.0
            };
            
            let effective_skill = (bot.skill_level + specialization_bonus) * gas_bidding_multiplier;
            let latency_penalty = (bot.latency_ms as f64) * 0.001;
            let bot_score = effective_skill - latency_penalty;
            
            if bot_score > best_bot_score {
                best_bot_score = bot_score;
                best_bot = Some(bot);
            }
        }
        
        let best_bot = match best_bot {
            Some(bot) => bot,
            None => {
                // No competing bots, we win by default
                return CompetitionResult {
                    opportunity_id: opp.id(),
                    we_win: true,
                    winning_bot_skill: 0.0,
                    latency_margin_ms: 0,
                };
            }
        };
        let skill_advantage = our_skill - best_bot.skill_level;
        let latency_advantage = (best_bot.latency_ms as i64).saturating_sub(our_latency as i64);
        
        let specialization_match = match best_bot.specialization {
            BotSpecialization::General => 0.0,
            BotSpecialization::CrossDex => {
                if opp.route.legs.len() > 2 { -0.1 } else { 0.0 }
            },
            BotSpecialization::FlashLoan => {
                if opp.route.flashloan_amount_wei.is_some() { -0.15 } else { 0.0 }
            },
            BotSpecialization::HighFreq => {
                if opp.profit_usd > 100.0 { -0.08 } else { 0.0 }
            },
            BotSpecialization::MEVHunter => {
                if opp.mev_risk_score > 0.5 { -0.12 } else { 0.0 }
            },
        };
        
        let gas_bidding_advantage = if opp.route.estimated_gas_cost_wei > U256::from(50_000_000_000u64) {
            (1.0 - best_bot.gas_bidding_aggressive) * 0.1
        } else {
            0.0
        };
        
        let win_probability = 0.5 + (skill_advantage * 0.3) + (latency_advantage as f64 * 0.001) + specialization_match + gas_bidding_advantage;
        let we_win = self.rng.gen_range(0.0..1.0).await < win_probability.max(0.1).min(0.9);

        debug!(
            "Simulating competition: opportunity_id={}, best_bot_skill={}, specialization={:?}, gas_bidding={}, our_skill={}, skill_advantage={}, latency_advantage={}, specialization_match={}, gas_bidding_advantage={}, win_probability={}, we_win={}",
            opp.id(), best_bot.skill_level, best_bot.specialization, best_bot.gas_bidding_aggressive, our_skill, skill_advantage, latency_advantage, specialization_match, gas_bidding_advantage, win_probability, we_win
        );

        CompetitionResult {
            opportunity_id: opp.id(),
            we_win,
            winning_bot_skill: if we_win { our_skill } else { best_bot.skill_level },
            latency_margin_ms: latency_advantage,
        }
    }
}

/// Simulates MEV attacks on arbitrage opportunities.
pub struct MEVAttackSimulator {
    rng: AsyncRng,
    prob_sandwich: f64,
    prob_frontrun: f64,
    avg_profit_loss_usd: f64,
}

impl MEVAttackSimulator {
    pub fn new(cfg: &BacktestConfig, seed: u64) -> Self {
        Self {
            rng: AsyncRng::new(seed),
            prob_sandwich: cfg.mev_sandwich_success_rate.unwrap_or(0.7),
            prob_frontrun: cfg.mev_frontrun_success_rate.unwrap_or(0.5),
            avg_profit_loss_usd: cfg.mev_avg_profit_loss_usd.unwrap_or(50.0),
        }
    }

    pub async fn simulate(&self, opp: &ArbitrageOpportunity) -> MEVAttackResult {
        // Use async-safe RNG
        
        // Determine attack type and success probability
        let attack_type_roll = self.rng.gen_range(0.0..1.0).await;
        let attack_type = if attack_type_roll < 0.6 {
            "sandwich"
        } else {
            "frontrun"
        };

        let success_rate = match attack_type {
            "sandwich" => self.prob_sandwich,
            "frontrun" => self.prob_frontrun,
            _ => 0.3,
        };

        let success = self.rng.gen_range(0.0..1.0).await < success_rate;
        let profit_lost = if success { self.avg_profit_loss_usd } else { 0.0 };

        debug!(
            "Simulating MEV attack: opportunity_id={}, attack_type={}, success_rate={}, success={}, profit_lost_usd={}",
            opp.id(), attack_type, success_rate, success, profit_lost
        );

        MEVAttackResult {
            opportunity_id: opp.id(),
            attack_type: attack_type.to_string(),
            success,
            profit_lost_usd: profit_lost,
        }
    }
}

/// Per‑chain infrastructure components for backtesting.
pub struct ChainInfra {
    pub blockchain_manager:   Arc<dyn BlockchainManager + Send + Sync>,
    pub pool_manager:         Arc<dyn PoolManagerTrait + Send + Sync>,
    pub pool_state_oracle:    Arc<PoolStateOracle>,
    pub transaction_optimizer:Arc<TransactionOptimizerV2>,
    pub gas_oracle:           Arc<dyn GasOracleProvider + Send + Sync>,
    pub price_oracle:         Arc<dyn PriceOracle + Send + Sync>,
    /// Strongly-typed handle to the historical price provider wrapper used across the system
    /// so we can update block context consistently for all dependent components.
    pub price_oracle_wrapper: Option<Arc<HistoricalPriceProviderWrapper>>,
    pub historical_price_provider: Option<Arc<HistoricalPriceProvider>>,
    pub path_finder:          Arc<dyn PathFinder + Send + Sync>,
    pub risk_manager:         Arc<RwLock<Option<Arc<dyn RiskManager + Send + Sync>>>>,
    pub predictive_engine:    Option<Arc<dyn crate::predict::PredictiveEngine + Send + Sync>>,
    pub quote_source:         Arc<dyn AggregatorQuoteSource + Send + Sync>,
    pub arbitrage_engine:     Arc<ArbitrageEngine>,
    pub simulation_engine:    Arc<SimulationEngine>,
}

impl ChainInfra {
    /// Create a new ChainInfra instance with providers updated for the specified block.
    /// This replaces the set_block_context pattern by creating new immutable provider instances.
    pub async fn for_block(
        &self,
        chain_name: String,
        block: u64,
        swap_loader: Arc<HistoricalSwapLoader>,
        chain_config: Arc<ChainConfig>,
        gas_oracle_config: GasOracleConfig,
        strict_backtest: bool,
    ) -> Result<Arc<Self>> {

        // Create new gas oracle for this block
        let gas_oracle = Arc::new(HistoricalGasProvider::new(
            chain_name.clone(),
            block,
            swap_loader.clone(),
            gas_oracle_config,
            chain_config.clone(),
            strict_backtest,
        ));

        // Update block context on the shared price oracle wrapper to keep all components consistent
        if let Some(wrapper) = &self.price_oracle_wrapper {
            wrapper
                .set_block_context(block)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to update price oracle block context: {}", e))?;
        }

        // Create new infrastructure with updated providers
        let new_infra = Arc::new(Self {
            blockchain_manager: self.blockchain_manager.clone(),
            pool_manager: self.pool_manager.clone(),
            pool_state_oracle: self.pool_state_oracle.clone(),
            transaction_optimizer: self.transaction_optimizer.clone(),
            gas_oracle,
            price_oracle: self.price_oracle.clone(),
            price_oracle_wrapper: self.price_oracle_wrapper.clone(),
            historical_price_provider: self.historical_price_provider.clone(),
            path_finder: self.path_finder.clone(),
            risk_manager: self.risk_manager.clone(),
            predictive_engine: self.predictive_engine.clone(),
            quote_source: self.quote_source.clone(),
            arbitrage_engine: self.arbitrage_engine.clone(),
            simulation_engine: self.simulation_engine.clone(),
        });

        Ok(new_infra)
    }
}

/// Quote source implementation for backtesting.
pub struct BacktestQuoteSource {
    pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    // Current block number for block-aware pool discovery in backtesting
    current_block: SyncRwLock<Option<u64>>,
}

pub struct HistoricalPriceProviderWrapper {
    provider: Arc<RwLock<Arc<HistoricalPriceProvider>>>,
}

impl std::fmt::Debug for BacktestQuoteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BacktestQuoteSource").finish()
    }
}

impl std::fmt::Debug for HistoricalPriceProviderWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalPriceProviderWrapper").finish()
    }
}

impl HistoricalPriceProviderWrapper {
    /// Set the block context for the underlying historical price provider
    pub async fn set_block_context(&self, block_number: u64) -> Result<(), anyhow::Error> {
        let current_arc = self.provider.read().await.clone();
        let new_provider = current_arc.set_block_context(block_number)?;
        *self.provider.write().await = Arc::new(new_provider);
        debug!("Updated block context for provider to block {}", block_number);
        
        Ok(())
    }
    
    /// Get a block-specific provider for the given block number
    async fn get_provider_for_block(&self, block_number: Option<u64>) -> Arc<HistoricalPriceProvider> {
        let current_arc = self.provider.read().await.clone();
        match block_number {
            Some(bn) if bn != current_arc.block_number() => {
                match current_arc.set_block_context(bn) {
                    Ok(new_provider) => {
                        let new_arc = Arc::new(new_provider);
                        *self.provider.write().await = new_arc.clone();
                        new_arc
                    }
                    Err(_) => current_arc,
                }
            }
            _ => current_arc,
        }
    }


}

#[async_trait]
impl PriceOracle for HistoricalPriceProviderWrapper {
    async fn get_token_price_usd(
        &self,
        chain_name: &str,
        token: Address,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.get_token_price_usd(chain_name, token).await
    }

    async fn get_token_price_usd_at(
        &self,
        chain_name: &str,
        token: Address,
        block_number: Option<u64>,
        unix_ts: Option<u64>,
    ) -> Result<f64, PriceError> {
        let provider = self.get_provider_for_block(block_number).await;
        // Use the timestamp-aware method if timestamp is provided, otherwise fall back to basic method
        if let Some(ts) = unix_ts {
            provider.get_token_price_usd_at(chain_name, token, block_number, Some(ts)).await
        } else {
            provider.get_token_price_usd(chain_name, token).await
        }
    }

    async fn get_pair_price_ratio(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.get_pair_price_ratio(chain_name, token_a, token_b).await
    }

    async fn get_historical_prices_batch(
        &self,
        chain_name: &str,
        tokens: &[Address],
        block_number: u64,
    ) -> Result<HashMap<Address, f64>, PriceError> {
        let provider = self.get_provider_for_block(Some(block_number)).await;
        provider.get_historical_prices_batch(chain_name, tokens, block_number).await
    }

    async fn get_token_volatility(
        &self,
        chain_name: &str,
        token: Address,
        timeframe_seconds: u64,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.get_token_volatility(chain_name, token, timeframe_seconds).await
    }

    async fn calculate_liquidity_usd(
        &self,
        pool_info: &PoolInfo,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.calculate_liquidity_usd(pool_info).await
    }

    

    async fn get_pair_price_at_block(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        block_number: u64,
    ) -> Result<f64, PriceError> {
        let provider = self.get_provider_for_block(Some(block_number)).await;
        provider.get_pair_price(chain_name, token_a, token_b).await
    }

    async fn get_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, PriceError> {
        let price_a = self.get_token_price_usd(chain_name, token_a).await?;
        let price_b = self.get_token_price_usd(chain_name, token_b).await?;
        if price_b == 0.0 {
            return Err(PriceError::Calculation("Token B price is zero".to_string()));
        }
        Ok(price_a / price_b)
    }

    async fn convert_token_to_usd(
        &self,
        chain_name: &str,
        amount: ethers::types::U256,
        token: Address,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.convert_token_to_usd(chain_name, amount, token).await
    }

    async fn convert_native_to_usd(
        &self,
        chain_name: &str,
        amount_native: ethers::types::U256,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.convert_native_to_usd(chain_name, amount_native).await
    }

    async fn estimate_token_conversion(
        &self,
        chain_name: &str,
        amount_in: ethers::types::U256,
        token_in: Address,
        token_out: Address,
    ) -> Result<ethers::types::U256, PriceError> {
        let provider = self.provider.read().await.clone();
        provider.estimate_token_conversion(chain_name, amount_in, token_in, token_out).await
    }

    async fn estimate_swap_value_usd(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Result<f64, PriceError> {
        let provider = self.provider.read().await;
        // Delegate to provider which knows actual decimals; provider may be block-aware
        provider.estimate_swap_value_usd(chain_name, token_in, token_out, amount_in).await
    }
}

impl BacktestQuoteSource {
    pub fn new(
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    ) -> Self {
        Self {
            pool_manager,
            price_oracle,
            gas_oracle,
            current_block: SyncRwLock::new(None),
        }
    }

    /// Create a new BacktestQuoteSource with a specific block context for backtesting
    pub fn new_with_block(
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
        current_block: u64,
    ) -> Self {
        Self {
            pool_manager,
            price_oracle,
            gas_oracle,
            current_block: SyncRwLock::new(Some(current_block)),
        }
    }

    /// Set the current block number for block-aware pool discovery in backtesting
    pub fn set_current_block(&self, block_number: u64) {
        *self.current_block.write() = Some(block_number);
    }

    /// Clear the current block number (for live mode)
    pub fn clear_current_block(&self) {
        *self.current_block.write() = None;
    }
}

#[async_trait]
impl AggregatorQuoteSource for BacktestQuoteSource {
    async fn get_rate_estimate(
        &self,
        chain_id:   u64,
        token_in:   Address,
        token_out:  Address,
        amount_in:  U256,
    ) -> Result<RateEstimate, DexError> {
            let chain_name = self.pool_manager.chain_name_by_id(chain_id)
        .map_err(|e| DexError::Other(e.to_string()))?;
        
        // CRITICAL: Use block-aware pool discovery in backtesting to prevent future pool leakage
        // Extract current block before await to avoid Send issues
        let current_block_opt = *self.current_block.read();
        let pools = if let Some(current_block) = current_block_opt {
            // In backtesting mode with current block set, only consider pools that existed at that block
            self.pool_manager.get_pools_for_pair_at_block(&chain_name, token_in, token_out, current_block).await
                .map_err(|e| DexError::RouteNotFound(e.to_string()))?
        } else {
            // In live mode, use all available pools
            self.pool_manager.get_pools_for_pair(&chain_name, token_in, token_out).await
                .map_err(|e| DexError::RouteNotFound(e.to_string()))?
        };

        if pools.is_empty() {
            return Err(DexError::RouteNotFound("No pools found for token pair".to_string()));
        }

        let mut best_rate = None;
        let mut best_amount_out = U256::zero();
        let mut best_price_impact = 0u64;

        for pool_address in pools {
            let pool_info = self.pool_manager.get_pool_info(&chain_name, pool_address, current_block_opt).await
                .map_err(|e| DexError::PoolInfoError(e.to_string()))?;
            
            let amount_out = dex_math::get_amount_out(&pool_info, token_in, amount_in)
                .map_err(|e| DexError::Math(e))?;

            let price_impact = self.calculate_price_impact(&pool_info, token_in, amount_in, amount_out).await?; // bps

            if amount_out > best_amount_out {
                best_amount_out = amount_out;
                best_price_impact = price_impact;
                
                // Enhanced price validation using price oracle
                let price_validation_result = self.validate_rate_with_price_oracle(
                    &chain_name, token_in, token_out, amount_in, amount_out, &pool_info
                ).await;
                
                if let Err(price_error) = price_validation_result {
                    debug!("Price validation failed for pool {}: {}", pool_address, price_error);
                    continue;
                }
                
                // Use gas oracle to estimate gas for this specific protocol
                let gas_estimate = self.gas_oracle.estimate_protocol_gas(&pool_info.protocol_type);
                
                best_rate = Some(RateEstimate {
                    amount_out,
                    fee_bps: if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(3000) },
                    price_impact_bps: price_impact as u32, // already in bps
                    gas_estimate: U256::from(gas_estimate),
                    pool_address: pool_info.address,
                });
            }
        }

        debug!(
            "Rate estimate: chain_id={}, token_in={}, token_out={}, amount_in={}, best_amount_out={}, best_price_impact={}, best_rate={:?}",
            chain_id, token_in, token_out, amount_in, best_amount_out, best_price_impact, best_rate
        );

        best_rate.ok_or_else(|| DexError::RouteNotFound("No valid rate found".to_string()))
    }

    async fn get_rate_estimate_from_quote(
        &self,
        chain_id:  u64,
        token_in:  Address,
        token_out: Address,
        amount_in: U256,
        backtest: bool,
    ) -> Result<RateEstimate, DexError> {
        // Use backtest flag to adjust rate estimation strategy for historical accuracy
        if backtest {
            // In backtest mode, apply historical accuracy adjustments
            let adjusted_amount_in = if amount_in > U256::from(1000u64).saturating_mul(U256::exp10(18)) {
                // For large amounts, apply historical slippage simulation
                amount_in.saturating_mul(U256::from(95)).checked_div(U256::from(100)).unwrap_or(amount_in)
            } else {
                amount_in
            };
            
            let rate_estimate = self.get_rate_estimate(chain_id, token_in, token_out, adjusted_amount_in).await?;
            
            // Apply backtest-specific adjustments to gas estimates
            let adjusted_gas_estimate = rate_estimate
                .gas_estimate
                .saturating_mul(U256::from(110))
                .div(U256::from(100));
            
            Ok(RateEstimate {
                amount_out: rate_estimate.amount_out,
                fee_bps: rate_estimate.fee_bps,
                price_impact_bps: rate_estimate.price_impact_bps,
                gas_estimate: adjusted_gas_estimate,
                pool_address: rate_estimate.pool_address,
            })
        } else {
            self.get_rate_estimate(chain_id, token_in, token_out, amount_in).await
        }
    }

    fn name(&self) -> &'static str { "backtest_quote_source" }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl BacktestQuoteSource {
    async fn calculate_price_impact(
        &self,
        pool_info: &crate::types::PoolInfo,
        token_in: Address,
        amount_in: U256,
        amount_out: U256,
    ) -> Result<u64, DexError> {
        let token0_decimals = pool_info.token0_decimals;
        let token1_decimals = pool_info.token1_decimals;

        let (reserve_in, reserve_out, decimals_in, decimals_out) = if token_in == pool_info.token0 {
            (
                pool_info.v2_reserve0.unwrap_or(U256::zero()),
                pool_info.v2_reserve1.unwrap_or(U256::zero()),
                token0_decimals,
                token1_decimals,
            )
        } else {
            (
                pool_info.v2_reserve1.unwrap_or(U256::zero()),
                pool_info.v2_reserve0.unwrap_or(U256::zero()),
                token1_decimals,
                token0_decimals,
            )
        };

        if reserve_in == U256::zero() || reserve_out == U256::zero() {
            return Ok(0);
        }

        let normalized_reserve_in = crate::types::normalize_units(reserve_in, decimals_in);
        let normalized_reserve_out = crate::types::normalize_units(reserve_out, decimals_out);
        let normalized_amount_in = crate::types::normalize_units(amount_in, decimals_in);
        let normalized_amount_out = crate::types::normalize_units(amount_out, decimals_out);

        if normalized_reserve_in == 0.0 || normalized_reserve_out == 0.0 {
            return Ok(0);
        }

        let fee_bps = pool_info.fee.unwrap_or(3000) as f64;
        let fee_multiplier = (10_000f64 - fee_bps) / 10_000f64;
        let amount_in_with_fee = normalized_amount_in * fee_multiplier;
        let numerator = amount_in_with_fee * normalized_reserve_out;
        let denominator = normalized_reserve_in * 1.0 + amount_in_with_fee;
        let expected_out = numerator / denominator;
        let price_impact = if expected_out > 0.0 {
            ((expected_out - normalized_amount_out) * 10000.0 / expected_out).abs()
        } else {
            0.0
        };

        Ok(price_impact.round() as u64)
    }

    async fn validate_rate_with_price_oracle(
        &self,
        chain_name: &str,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        amount_out: U256,
        pool_info: &crate::types::PoolInfo,
    ) -> Result<(), DexError> {
        // Derive current block for backtest pricing alignment
        let current_block_opt = *self.current_block.read();
        let block_ts = None; // No timestamp available here; wrapper will use block context

        // Get block-aware market prices for both tokens
        let token_in_price = self.price_oracle.get_token_price_usd_at(chain_name, token_in, current_block_opt, block_ts).await
            .map_err(|e| DexError::Other(format!("Failed to get token_in price: {}", e)))?;
        
        let token_out_price = self.price_oracle.get_token_price_usd_at(chain_name, token_out, current_block_opt, block_ts).await
            .map_err(|e| DexError::Other(format!("Failed to get token_out price: {}", e)))?;

        if token_in_price <= 0.0 || token_out_price <= 0.0 {
            return Err(DexError::Other("Invalid token prices for validation".to_string()));
        }

        // Calculate expected USD values
        let token_in_decimals = if token_in == pool_info.token0 {
            pool_info.token0_decimals
        } else {
            pool_info.token1_decimals
        };
        
        let token_out_decimals = if token_out == pool_info.token0 {
            pool_info.token0_decimals
        } else {
            pool_info.token1_decimals
        };

        let amount_in_normalized = crate::types::normalize_units(amount_in, token_in_decimals);
        let amount_out_normalized = crate::types::normalize_units(amount_out, token_out_decimals);

        let amount_in_usd = amount_in_normalized * token_in_price;
        let amount_out_usd = amount_out_normalized * token_out_price;

        // Calculate the effective exchange rate
        let effective_rate = if amount_in_usd > 0.0 {
            amount_out_usd / amount_in_usd
        } else {
            return Err(DexError::Other("Invalid input amount for rate validation".to_string()));
        };

        // Get the market exchange rate from price oracle
        let market_rate = if token_in_price > 0.0 {
            token_out_price / token_in_price
        } else {
            return Err(DexError::Other("Invalid market rate calculation".to_string()));
        };

        // Validate that the effective rate is within reasonable bounds of the market rate
        let rate_deviation = (effective_rate - market_rate).abs() / market_rate;
        let max_allowed_deviation = 0.05; // 5% maximum deviation

        if rate_deviation > max_allowed_deviation {
            return Err(DexError::Other(format!(
                "Rate deviation too high: effective={:.6}, market={:.6}, deviation={:.2}%",
                effective_rate, market_rate, rate_deviation * 100.0
            )));
        }

        // Additional validation: reject extreme deviations (acts as coarse price-impact guard)
        let price_impact_threshold = 0.20; // 20% maximum deviation
        if rate_deviation > price_impact_threshold {
            return Err(DexError::Other(format!(
                "Effective rate deviation too high: {:.4}% exceeds threshold of {:.4}%",
                rate_deviation * 100.0, price_impact_threshold * 100.0
            )));
        }

        Ok(())
    }
}

/// Predictive engine implementation for backtesting.
#[derive(Debug)]
pub struct BacktestPredictiveEngine {
    price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    rng: AsyncRng,
}

impl BacktestPredictiveEngine {
    pub fn new(
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
        seed: u64,
    ) -> Self {
        Self { price_oracle, gas_oracle, rng: AsyncRng::new(seed) }
    }
}

#[async_trait]
impl crate::predict::PredictiveEngine for BacktestPredictiveEngine {
    async fn predict<'a>(
        &'a self,
        ctx: &'a TransactionContext,
        _current_backtest_block_timestamp: Option<u64>,
    ) -> Result<Option<MEVPrediction>, crate::errors::PredictiveEngineError> {
        let gas_price = ctx.gas_price_history.last().copied().unwrap_or(20.0);
        let mempool_congestion = ctx.mempool_context.congestion_score;
        let market_volatility = ctx.market_conditions.overall_volatility;

        // Use price_oracle to get token volatility for the main token in the transaction context
        let token = ctx.token_market_data.keys().next().copied().unwrap_or(Address::zero());
        let chain_name = ctx.related_pools.get(0).map(|p| p.pool_info.chain_name.as_str()).unwrap_or("ethereum");
        let token_volatility = match self.price_oracle.get_token_volatility(chain_name, token, 3600).await {
            Ok(v) => v,
            Err(_) => 0.0,
        };

        // Use gas_oracle to get recent gas price (fallback for trend)
        let gas_trend = match self.gas_oracle.get_gas_price(chain_name, None).await {
            Ok(gp) => gp.effective_price().as_u64() as f64,
            Err(_) => 0.0,
        };

        let adjusted_market_volatility = (market_volatility + token_volatility + gas_trend) / 3.0;
        let mev_risk_score = self.calculate_mev_risk_score(gas_price, mempool_congestion, adjusted_market_volatility);

        if mev_risk_score > 0.7 {
            let prediction = MEVPrediction {
                prediction_id: H256(self.random_h256().await),
                transaction_hash: if ctx.tx.hash != H256::zero() { ctx.tx.hash } else { H256(self.random_h256().await) },
                predicted_profit_usd: -50.0,
                confidence_score: 0.85,
                risk_score: mev_risk_score,
                mev_type: crate::types::MEVType::Sandwich,
                estimated_success_rate: 0.7,
                features: crate::types::PredictionFeatures::default(),
                competition_level: crate::types::CompetitionLevel::High,
            };
            Ok(Some(prediction))
        } else if mev_risk_score > 0.4 {
            let prediction = MEVPrediction {
                prediction_id: H256(self.random_h256().await),
                transaction_hash: if ctx.tx.hash != H256::zero() { ctx.tx.hash } else { H256(self.random_h256().await) },
                predicted_profit_usd: -25.0,
                confidence_score: 0.65,
                risk_score: mev_risk_score,
                mev_type: crate::types::MEVType::Frontrun,
                estimated_success_rate: 0.5,
                features: crate::types::PredictionFeatures::default(),
                competition_level: crate::types::CompetitionLevel::Medium,
            };
            Ok(Some(prediction))
        } else {
            Ok(None)
        }
    }

    async fn update_model<'a>(
        &'a self,
        res: &'a ExecutionResult,
    ) -> Result<(), crate::errors::PredictiveEngineError> {
        let profit = res.actual_profit_usd.unwrap_or(0.0);
        debug!("Predictive engine: updating model with execution result: success={}, profit={}", res.success, profit);
        Ok(())
    }

    async fn get_performance_metrics<'a>(
        &'a self,
    ) -> Result<ModelPerformance, crate::errors::PredictiveEngineError> {
        debug!("Predictive engine: get performance metrics called");
        Ok(ModelPerformance {
            accuracy: 0.0,
            precision: 0.0,
            recall: 0.0,
            f1_score: 0.0,
            total_predictions: 0,
            successful_predictions: 0,
            true_positives: 0,
            false_positives: 0,
            false_negatives: 0,
            cumulative_actual_profit_usd: 0.0,
            cumulative_predicted_profit_usd: 0.0,
        })
    }
}

impl BacktestPredictiveEngine {
    fn calculate_mev_risk_score(&self, gas_price: f64, mempool_congestion: f64, market_volatility: f64) -> f64 {
        let gas_risk = (gas_price / 100.0).min(1.0);
        let congestion_risk = mempool_congestion.min(1.0);
        let volatility_risk = market_volatility.min(1.0);
        let combined_risk = (gas_risk * 0.4 + congestion_risk * 0.4 + volatility_risk * 0.2).min(1.0);
        debug!(
            "MEV risk calculation: gas_price={}, mempool_congestion={}, market_volatility={}, gas_risk={}, congestion_risk={}, volatility_risk={}, combined_risk={}",
            gas_price, mempool_congestion, market_volatility, gas_risk, congestion_risk, volatility_risk, combined_risk
        );
        combined_risk
    }

    async fn random_h256(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for b in &mut bytes {
            *b = self.rng.gen_range(0u8..=255u8).await;
        }
        bytes
    }
}

/// Helper struct for simulation results
#[derive(Debug, Clone)]
struct SimulationResults {
    competition_results: Vec<CompetitionResult>,
    mev_attacks: Vec<MEVAttackResult>,
}

/// Main backtesting orchestrator.
pub struct Backtester {
    pub cfg:                  Arc<BacktestConfig>,
    pub global_cfg:           Arc<Config>,
    pub chains:               Vec<BacktestChainConfig>,
    competition:              Arc<CompetitionSimulator>,
    mev_sim:                  Arc<MEVAttackSimulator>,
    swap_loader:              Arc<HistoricalSwapLoader>,
    metrics:                  Arc<ArbitrageMetrics>,
    discovery_engine:         Arc<DiscoveryEngine>,
    thresholds:               BacktestThresholds,
}

impl Backtester {
    pub async fn new(cfg: Arc<Config>) -> Result<Self> {
        let backtest_cfg = cfg.backtest.as_ref()
            .ok_or_else(|| anyhow::anyhow!("backtest config missing"))?;

        // Initialize global rate limiter manager
        use crate::rate_limiter::initialize_global_rate_limiter_manager;
        use crate::config::ChainSettings;
        let rate_limiter_settings = Arc::new(ChainSettings {
            mode: cfg.mode,
            global_rps_limit: cfg.chain_config.rate_limiter_settings.global_rps_limit,
            default_chain_rps_limit: cfg.chain_config.rate_limiter_settings.default_chain_rps_limit,
            default_max_concurrent_requests: 50,
            rate_limit_burst_size: 5,
            rate_limit_timeout_secs: 30,
            rate_limit_max_retries: 3,
            rate_limit_initial_backoff_ms: 1000,
            rate_limit_backoff_multiplier: 2.0,
            rate_limit_max_backoff_ms: 10000,
            rate_limit_jitter_factor: 0.1,
            batch_size: 10,
            min_batch_delay_ms: 100,
            max_blocks_per_query: Some(1000),
            log_fetch_concurrency: Some(5),
        });
        initialize_global_rate_limiter_manager(rate_limiter_settings);
        info!("Global rate limiter manager initialized for backtest");

        // Get shared database pool instead of creating a new one
        let db_pool = crate::database::get_shared_database_pool(&cfg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get shared database pool: {}", e))?;

        use std::sync::atomic::{AtomicUsize, Ordering};
        static BACKTEST_SWAP_LOADER_COUNT: AtomicUsize = AtomicUsize::new(0);
        let count = BACKTEST_SWAP_LOADER_COUNT.fetch_add(1, Ordering::Relaxed);
        warn!("CREATING BACKTEST HistoricalSwapLoader #{} - this should only happen once per backtest run", count + 1);

        let swap_loader = Arc::new(HistoricalSwapLoader::new(
            db_pool,
            Arc::new(cfg.chain_config.clone()),
        ).await?);
        
        // Determinism: random_seed must be provided
        let seed = backtest_cfg
            .random_seed
            .ok_or_else(|| anyhow::anyhow!("backtest.random_seed must be set for deterministic runs"))?;
        let competition = Arc::new(CompetitionSimulator::new(
            backtest_cfg,
            seed,
        ));
        let mev_sim = Arc::new(MEVAttackSimulator::new(
            backtest_cfg,
            seed,
        ));

        let chains: Vec<BacktestChainConfig> = backtest_cfg.chains.iter()
            .filter_map(|(name, chain_cfg)| {
                if chain_cfg.enabled {
                    info!("Adding enabled chain to Backtester: {}", name);
                    Some(BacktestChainConfig {
                        chain_name: name.clone(),
                        start_block: chain_cfg.backtest_start_block,
                        end_block: chain_cfg.backtest_end_block,
                        result_output_file: std::path::PathBuf::from(&chain_cfg.backtest_result_output_file),
                    })
                } else {
                    None
                }
            })
            .collect();
        
        info!("Backtester created with {} chains", chains.len());

        // Initialize thresholds
        let mut thresholds = BacktestThresholds::default();
        thresholds.min_profit_usd = backtest_cfg.min_profit_usd.unwrap_or(MIN_PROFIT_THRESHOLD_USD);
        thresholds.min_confidence_score = backtest_cfg.confidence_threshold.unwrap_or(MIN_CONFIDENCE_SCORE);
        thresholds.min_mev_resistance = backtest_cfg.mev_resistance_threshold.unwrap_or(MIN_MEV_RESISTANCE);
        thresholds.max_opportunities_per_block = backtest_cfg
            .max_opportunities_per_block
            .unwrap_or(DEFAULT_MAX_OPPORTUNITIES_PER_BLOCK);
        thresholds.max_tokens_per_scan = backtest_cfg
            .max_tokens_per_block_scan
            .unwrap_or(DEFAULT_MAX_TOKENS_PER_SCAN);
        thresholds.scan_usd_amounts = backtest_cfg.scan_usd_amounts.clone();
        
        // Initialize discovery strategies
        let mut strategies: Vec<Box<dyn OpportunityDiscoveryStrategy>> = Vec::new();
        
        if backtest_cfg.proactive_scanning_enabled.unwrap_or(true) {
            strategies.push(Box::new(ProactiveScanStrategy::new(thresholds.clone())));
        }
        
        strategies.push(Box::new(CrossDexStrategy::new(thresholds.clone())));
        strategies.push(Box::new(ReactiveSwapStrategy::new(thresholds.clone())));
        
        // Add hot token strategy if configured
        if backtest_cfg.hot_tokens_search_enabled.unwrap_or(false) {
            let hot_tokens = Vec::new(); // Will be populated per chain
            strategies.push(Box::new(HotTokenStrategy::new(thresholds.clone(), hot_tokens)));
        }
        
        let discovery_engine = Arc::new(DiscoveryEngine::new(strategies));
        
        debug!(
            "Backtester initialized: chains={}, competition_bots={}, mev_simulator_seed={}, strategies={}",
            chains.len(), competition.bots.len(), backtest_cfg.random_seed.unwrap_or(0), discovery_engine.strategies.len()
        );

        Ok(Self {
            cfg: Arc::new(backtest_cfg.clone()),
            global_cfg: cfg,
            chains,
            competition,
            mev_sim,
            swap_loader,
            metrics: Arc::new(crate::types::ArbitrageMetrics::new()),
            discovery_engine,
            thresholds,
        })
    }

    fn fresh_seed() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_nanos() as u64
    }
    
    /// Save checkpoint for resuming backtest
    async fn save_checkpoint(&self, checkpoint: &BacktestCheckpoint) -> Result<(), BacktestError> {
        let checkpoint_path = format!(".backtest_checkpoint_{}.json", checkpoint.chain);
        let json = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| BacktestError::Serialization(format!("Failed to serialize checkpoint: {}", e)))?;
        
        tokio::fs::write(&checkpoint_path, json)
            .await
            .map_err(|e| BacktestError::IO(format!("Failed to write checkpoint: {}", e)))?;
        
        info!("Checkpoint saved for chain {} at block {}", checkpoint.chain, checkpoint.last_processed_block);
        Ok(())
    }
    
    /// Load checkpoint if it exists
    async fn load_checkpoint(&self, chain: &str) -> Result<Option<BacktestCheckpoint>, BacktestError> {
        let checkpoint_path = format!(".backtest_checkpoint_{}.json", chain);
        
        if !tokio::fs::try_exists(&checkpoint_path).await.unwrap_or(false) {
            return Ok(None);
        }
        
        let json = tokio::fs::read_to_string(&checkpoint_path)
            .await
            .map_err(|e| BacktestError::IO(format!("Failed to read checkpoint: {}", e)))?;
        
        let checkpoint: BacktestCheckpoint = serde_json::from_str(&json)
            .map_err(|e| BacktestError::Serialization(format!("Failed to deserialize checkpoint: {}", e)))?;
        
        // Check if checkpoint is stale (older than 24 hours)
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if current_time - checkpoint.timestamp > 86400 {
            warn!("Checkpoint for chain {} is older than 24 hours, ignoring", chain);
            return Ok(None);
        }
        
        Ok(Some(checkpoint))
    }

    pub async fn run(&self) -> Result<Vec<String>> {
        info!("Backtester::run() called with {} chains to process", self.chains.len());
        let mut summaries = Vec::new();
        for chain_cfg in &self.chains {
            info!("Processing chain: {}", chain_cfg.chain_name);
            let res = self.run_chain(chain_cfg).await?;
            summaries.push(res);
        }
        info!("Backtester::run() completed successfully");
        Ok(summaries)
    }

    // ─────────────────────────────────────────────────────────────────────
    // single‑chain back‑test
    // ─────────────────────────────────────────────────────────────────────

    async fn run_chain(&self, cfg: &BacktestChainConfig) -> Result<String> {
        cfg.validate()?;
        info!(
            chain       = %cfg.chain_name,
            start_block = cfg.start_block,
            end_block   = cfg.end_block,
            "starting backtest chain",
        );

        let checkpoint = self.load_checkpoint(&cfg.chain_name).await?;
        let start_block = if let Some(cp) = &checkpoint {
            info!("Resuming from checkpoint at block {}", cp.last_processed_block);
            (cp.last_processed_block + 1).min(cfg.end_block)
        } else {
            cfg.start_block
        };

        if start_block > cfg.end_block {
            info!("Backtest for {} already complete.", cfg.chain_name);
            return Ok(format!("Backtest already completed for {}", cfg.chain_name));
        }

        let mut result_stream = ResultStream::new(cfg.result_output_file.clone()).await?;
        if let Some(cp) = checkpoint {
            for result in cp.partial_results {
                result_stream.push(result).await?;
            }
        }

        // ------------------- FIX: ONE-TIME INFRASTRUCTURE SETUP -------------------
        // Initialize the base infrastructure *ONCE* before the loop starts.
        // This creates the connection pool and loads the pool registry a single time.
        let infra = self.prepare_infra(&cfg.chain_name, start_block).await?;
        info!("Base infrastructure prepared. Starting block processing loop...");
        // -------------------------------------------------------------------------

        let mut results = Vec::new();
        let total_blocks = cfg.end_block - start_block + 1;
        let mut processed_blocks = 0;

        for block in start_block..=cfg.end_block {
            let block_start = Instant::now();

            // --- FIX: CREATE NEW INFRASTRUCTURE FOR EACH BLOCK ---
            // Create block-specific infrastructure with new immutable providers
            let gas_oracle_config = self.global_cfg
                .module_config
                .as_ref()
                .unwrap()
                .gas_oracle_config
                .clone();
            let chain_config = Arc::new(self.global_cfg.chain_config.clone());

            let block_infra = infra.for_block(
                cfg.chain_name.clone(),
                block,
                self.swap_loader.clone(),
                chain_config,
                gas_oracle_config,
                self.global_cfg.strict_backtest,
            ).await?;

            let swaps = self
                .swap_loader
                .load_swaps_for_block_chunk(&cfg.chain_name, block as i64, block as i64)
                .await
                .map_err(|e| BacktestError::SwapLoad(e.to_string()))?;

            debug!("Block {}: Loaded {} swaps.", block, swaps.len());

            let br = self.process_block(&block_infra, block, &swaps).await?;

            result_stream.push(br.clone()).await?;
            processed_blocks += 1;

            if (processed_blocks % 10 == 0) || block == cfg.end_block {
                let checkpoint = BacktestCheckpoint::new(cfg.chain_name.clone(), block, vec![]);
                self.save_checkpoint(&checkpoint).await?;
            }

            info!(
                block       = block,
                swaps       = swaps.len(),
                opps        = br.opportunities_found,
                trades      = br.trades_executed.len(),
                time_ms     = block_start.elapsed().as_millis(),
                progress    = format!("{:.1}%", (processed_blocks as f64 / total_blocks as f64) * 100.0),
                "block done",
            );
            if br.opportunities_found == 0 && !swaps.is_empty() {
                 warn!(
                    "CRITICAL: Block {} processed {} swaps but found 0 opportunities! Check graph building, pool state loading, and threshold filters.",
                    block, swaps.len()
                );
            }
            results.push(br);
        }

        result_stream.finalize().await?;
        self.write_results(&results, cfg).await
    }

    // ─────────────────────────────────────────────────────────────────────
    // infrastructure
    // ─────────────────────────────────────────────────────────────────────

    async fn prepare_infra(&self, chain: &str, start_block: u64) -> Result<Arc<ChainInfra>> {
        let chain_cfg = self
            .global_cfg
            .chain_config
            .chains
            .get(chain)
            .ok_or_else(|| anyhow::anyhow!("missing chain cfg"))?;

        chain_cfg.validate()
            .map_err(|e| anyhow::anyhow!("Invalid chain configuration for {}: {}", chain, e))?;

        if chain_cfg.backtest_archival_rpc_url.is_none() {
            info!("No backtest archival RPC URL configured for {}, using default RPC", chain);
        }

        let mut bc_mgr = BlockchainManagerImpl::new(&self.global_cfg, chain, ExecutionMode::Backtest, None).await?;
        bc_mgr.set_swap_loader(self.swap_loader.clone());

        // Create base gas oracle and set it on blockchain manager for ArbitrageEngine
        let base_gas_oracle = Arc::new(
            HistoricalGasProvider::new(
                chain.to_string(),
                start_block, // Use the actual start block instead of 0
                self.swap_loader.clone(),
                self.global_cfg
                    .module_config
                    .as_ref()
                    .unwrap()
                    .gas_oracle_config
                    .clone(),
                Arc::new(self.global_cfg.chain_config.clone()),
                self.global_cfg.strict_backtest,
            )
        ) as Arc<dyn GasOracleProvider + Send + Sync>;
        bc_mgr.set_gas_oracle(base_gas_oracle.clone());

        let bc_mgr = Arc::new(bc_mgr);

        // base_gas_oracle already created above

        let base_historical_price_provider = Arc::new(HistoricalPriceProvider::new(
            chain.to_string(),
            0, // Base block number, will be replaced per block
            None,
            self.swap_loader.clone(),
            Arc::new(self.global_cfg.chain_config.clone()),
            self.global_cfg.strict_backtest,
        ).map_err(|e| BacktestError::InfraInit(format!("Failed to create historical price provider: {}", e)))?);
        
        let base_wrapper_arc = Arc::new(HistoricalPriceProviderWrapper {
            provider: Arc::new(RwLock::new(base_historical_price_provider.clone())),
        });
        let base_price_oracle = base_wrapper_arc.clone() as Arc<dyn PriceOracle + Send + Sync>;

        let rps_limit = chain_cfg.rps_limit.unwrap_or(
            self.global_cfg.chain_config.rate_limiter_settings.default_chain_rps_limit
        );
        let max_concurrent = chain_cfg.max_concurrent_requests.unwrap_or(16);
        info!(
            chain = %chain,
            rps_limit = rps_limit,
            max_concurrent = max_concurrent,
            "Configured chain-specific rate limiting"
        );

        // Create minimal base infrastructure - all components will be recreated per-block
        let pool_manager = Arc::new(crate::pool_manager::PoolManager::new_without_state_oracle(
            self.global_cfg.clone(),
        ).await?) as Arc<dyn PoolManagerTrait + Send + Sync>;

        let pool_state_oracle = Arc::new(PoolStateOracle::new(
            self.global_cfg.clone(),
            {
                let mut map = HashMap::new();
                map.insert(chain.to_string(), bc_mgr.clone() as Arc<dyn BlockchainManager>);
                map
            },
            Some(self.swap_loader.clone()),
            pool_manager.clone(),
        )?);

        pool_manager.set_state_oracle(pool_state_oracle.clone()).await?;

        let quote_source = Arc::new(BacktestQuoteSource::new(
            pool_manager.clone(),
            base_price_oracle.clone(),
            base_gas_oracle.clone(),
        ));

        let router_tokens = chain_cfg.router_tokens.clone().unwrap_or_default();
        let path_finder = Arc::new(AStarPathFinder::new(
            self.global_cfg
                .module_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Module config not found"))?
                .path_finder_settings
                .clone(),
            pool_manager.clone(),
            bc_mgr.clone(),
            self.global_cfg.clone(),
            Arc::new(GasOracle::new(base_gas_oracle.clone())),
            base_price_oracle.clone(),
            bc_mgr.get_chain_id(),
            true,
            router_tokens,
        ).map_err(|e| anyhow::anyhow!("Failed to create AStarPathFinder: {}", e))?) as Arc<dyn PathFinder + Send + Sync>;

        let risk_mgr_impl = Arc::new(RiskManagerImpl::new(
            Arc::new(
                self.global_cfg
                    .module_config
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Module config not found"))?
                    .risk_settings
                    .clone(),
            ),
            bc_mgr.clone(),
            pool_manager.clone(),
            base_gas_oracle.clone(),
            base_price_oracle.clone(),
        ));
        let risk_mgr = Arc::new(RwLock::new(Some(risk_mgr_impl as Arc<dyn RiskManager + Send + Sync>)));

        // Default arbitrage executor address - should be configured per chain in production
        let arbitrage_executor_address = Address::from_str("0x1234567890123456789012345678901234567890")
            .unwrap_or_else(|_| Address::zero());

        let base_config = Arc::new(
            self.global_cfg
                .module_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Module config not found"))?
                .transaction_optimizer_settings
                .clone(),
        );
        
        let enhanced_config = crate::transaction_optimizer::EnhancedOptimizerConfig::default();
        
        let tx_opt = 
            crate::transaction_optimizer::TransactionOptimizerV2::new(
                base_config,
                enhanced_config,
                bc_mgr.clone(),
                base_gas_oracle.clone(),
                base_price_oracle.clone(),
                None,
                arbitrage_executor_address,
            )
            .await?;

        let predictive_engine: Option<Arc<dyn crate::predict::PredictiveEngine + Send + Sync>> = Some(Arc::new(
            BacktestPredictiveEngine::new(
                base_price_oracle.clone(),
                base_gas_oracle.clone(),
                self.cfg.random_seed.unwrap_or_else(Self::fresh_seed),
            )
        ));

        let arb_engine = Arc::new(ArbitrageEngine {
            config: Arc::new(
                self.global_cfg
                    .module_config
                                    .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Module config not found"))?
                .arbitrage_settings
                    .clone(),
            ),
            full_config: self.global_cfg.clone(),
            price_oracle: base_price_oracle.clone(),
            risk_manager: risk_mgr.clone(),
            blockchain_manager: bc_mgr.clone(),
            quote_source: quote_source.clone(),
            global_metrics: self.metrics.clone(),
            alpha_engine: None, // Not available in backtest mode
            swap_loader: Some(self.swap_loader.clone()),
        });

        if let Some(factories) = &chain_cfg.factories {
            info!(
                chain = %chain,
                factory_count = factories.len(),
                "Chain has {} factory configurations for pool discovery",
                factories.len()
            );
        } else {
            info!(
                chain = %chain,
                "No factory configurations found, pool discovery may be limited"
            );
        }

        debug!(
            "Base infrastructure prepared: chain={}, ready for block-specific oracle creation",
            chain
        );

        // Create simulation engine for intra-block state updates
        let simulation_engine = Arc::new(SimulationEngine::new(
            pool_manager.clone(),
            bc_mgr.clone(),
            base_price_oracle.clone(),
            base_gas_oracle.clone(),
            self.global_cfg.clone(),
        ));

        Ok(Arc::new(ChainInfra {
            blockchain_manager: bc_mgr,
            pool_manager,
            pool_state_oracle,
            transaction_optimizer: tx_opt,
            gas_oracle: base_gas_oracle,
            price_oracle: base_price_oracle,
            price_oracle_wrapper: Some(base_wrapper_arc),
            path_finder,
            risk_manager: risk_mgr,
            predictive_engine,
            historical_price_provider: Some(base_historical_price_provider),
            quote_source,
            arbitrage_engine: arb_engine,
            simulation_engine,
        }))
    }



    // ─────────────────────────────────────────────────────────────────────
    // per‑block processing
    // ─────────────────────────────────────────────────────────────────────

    async fn process_block(
        &self,
        infra: &Arc<ChainInfra>,
        block: u64,
        swaps: &[SwapEvent],
    ) -> Result<BlockResult, BacktestError> {
        let overall_start = Instant::now();
        let scan_start = Instant::now();

        // Set block context for pool manager to enable backtest mode pool loading
        if let Err(e) = infra.pool_manager.set_block_context(block).await {
            warn!("Failed to set block context on pool manager: {}", e);
        }

        // Reload the pool registry for the current block
        if let Err(e) = infra.pool_manager.reload_registry_for_backtest(block).await {
            warn!("Failed to reload pool registry for block {}: {}", block, e);
        }

        // Set block context for quote source to enable block-aware pool discovery
        if let Some(quote_source) = infra.quote_source.as_any().downcast_ref::<BacktestQuoteSource>() {
            quote_source.set_current_block(block);
        }

        // Price oracle wrapper block context is updated once in ChainInfra::for_block

        // Set block context for pool state oracle to clear caches
        if let Err(e) = infra.pool_state_oracle.set_block_context(block).await {
            warn!("Failed to set block context on pool state oracle: {}", e);
        }

        // Build graph once per block. The pool registry has already been constrained to the block
        // via set_block_context + reload_registry_for_backtest above, so we can load pool infos directly.
        let chain_name = infra.blockchain_manager.get_chain_name().to_string();
        let block_valid_pools = match infra.pool_manager.get_all_pools(&chain_name, None).await {
            Ok(pools) => {
                let mut valid_pools = Vec::with_capacity(pools.len());
                for metadata in pools.into_iter() {
                    match infra.pool_manager.get_pool_info(&chain_name, metadata.pool_address, Some(block)).await {
                        Ok(pool_info) => valid_pools.push(pool_info),
                        Err(e) => debug!("Failed to get pool info for {}: {}, skipping", metadata.pool_address, e),
                    }
                }
                valid_pools
            }
            Err(e) => {
                debug!("Failed to get pools for graph building: {}", e);
                Vec::new()
            }
        };

        // Collect all unique tokens from swaps in the current block
        let all_tokens_in_block = swaps.iter().flat_map(|s| vec![s.token_in, s.token_out]).collect::<std::collections::HashSet<_>>();
        info!(
            "Found {} unique tokens in {} swaps for block {}",
            all_tokens_in_block.len(),
            swaps.len(),
            block
        );

        // Build graph once for the entire block processing, ensuring all tokens from swaps are included
        let path_finder_ref = infra
            .path_finder
            .as_any()
            .downcast_ref::<AStarPathFinder>()
            .ok_or_else(|| BacktestError::SwapLoad("PathFinder is not AStarPathFinder".into()))?;

        path_finder_ref.build_graph_with_pools_and_tokens(&block_valid_pools, all_tokens_in_block.into_iter().collect(), block).await
            .map_err(|e| BacktestError::Infrastructure(format!("Failed to build block graph: {}", e)))?;

        debug!("Built graph for block {} with {} valid pools", block, block_valid_pools.len());

        // Sort swaps by log_index to ensure correct sequential processing
        let mut sorted_swaps = swaps.to_vec();
        sorted_swaps.sort_by(|a, b| a.log_index.cmp(&b.log_index));

        // Initialize current block pool states with baseline states from end of previous block
        let mut current_block_pool_states: std::collections::HashMap<Address, crate::types::PoolInfo> = std::collections::HashMap::new();
        let mut discovered_opportunities = Vec::new();
        let mut rejection_stats: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        // Process swaps sequentially to simulate intra-block state changes
        for (swap_index, swap) in sorted_swaps.iter().enumerate() {
            // Update pool state for the swap's pool if it exists in our tracking
            if let Some(pool_state) = current_block_pool_states.get_mut(&swap.pool_address) {
                // Use simulation engine to calculate new pool state after this swap
                match infra.simulation_engine.calculate_new_pool_state(
                    pool_state,
                    swap.token_in,
                    swap.token_out,
                    swap.amount_in,
                    swap.amount_out,
                ) {
                    Ok(new_pool_state) => {
                        *pool_state = new_pool_state;
                        debug!(
                            "Updated pool {} state after swap {} (index {}): reserves0={:?}, reserves1={:?}",
                            swap.pool_address,
                            swap.tx_hash,
                            swap_index,
                            pool_state.v2_reserve0,
                            pool_state.v2_reserve1
                        );
                    }
                    Err(e) => {
                        debug!("Failed to update pool state for {} after swap {}: {}", swap.pool_address, swap.tx_hash, e);
                        continue;
                    }
                }
            } else {
                // First time seeing this pool in this block - load its state from previous block
                if let Ok(pool_info) = infra.pool_manager.get_pool_info(&infra.blockchain_manager.get_chain_name(), swap.pool_address, Some(block.saturating_sub(1))).await {
                    current_block_pool_states.insert(swap.pool_address, pool_info);
                    debug!("Loaded initial pool state for {} from block {}", swap.pool_address, block.saturating_sub(1));
                }
            }

            // Run discovery strategies with current block pool states
            let (mut block_discovered, block_rejection_stats) = self.discovery_engine.discover_all_with_pool_states(
                infra,
                block,
                &sorted_swaps,
                &current_block_pool_states,
                swap_index,
            ).await?;

            // Enforce thresholds before simulation to bound queue size
            block_discovered.retain(|opp| {
                opp.profit_usd > self.thresholds.min_profit_usd
                    && opp.risk_score <= (1.0 - self.thresholds.min_confidence_score)
                    && opp.mev_risk_score <= (1.0 - self.thresholds.min_mev_resistance)
            });

            // Store length before extend moves the vector
            let block_discovered_len = block_discovered.len();
            discovered_opportunities.extend(block_discovered);

            // Merge rejection stats
            for (k, v) in block_rejection_stats.into_iter() {
                *rejection_stats.entry(k).or_insert(0) += v;
            }

            debug!(
                "After swap {} (tx: {}), discovered {} opportunities, total so far: {}",
                swap_index,
                swap.tx_hash,
                block_discovered_len,
                discovered_opportunities.len()
            );
        }

        let scan_time_ms = scan_start.elapsed().as_millis() as u64;

        // Log discovered opportunities at a high level
        for opp in &discovered_opportunities {
            info!(
                "\u{1F4CA} Opportunity found: legs={}, profit_usd={:.6}, block={}, chain_id={}",
                opp.route.legs.len(),
                opp.profit_usd,
                block,
                opp.route.chain_id
            );
        }

        // Simulate competition and MEV
        let sim = self
            .simulate_opportunities(&discovered_opportunities, infra, block)
            .await?;

        // Execute trades for winners
        let trades = self
            .execute_winning_trades(
                &discovered_opportunities,
                &sim.competition_results,
                &sim.mev_attacks,
                infra,
                block,
            )
            .await?;

        let execution_time_ms = overall_start.elapsed().as_millis() as u64;
        Ok(self.build_block_result(
            block,
            execution_time_ms,
            discovered_opportunities,
            trades,
            sim,
            scan_time_ms,
            rejection_stats,
        ))
    }
    
    /// Simulate competition and MEV attacks for opportunities
    async fn simulate_opportunities(
        &self,
        opportunities: &[ArbitrageOpportunity],
        infra: &Arc<ChainInfra>,
        block: u64,
    ) -> Result<SimulationResults, BacktestError> {
        let mut competition_results = Vec::new();
        let mut mev_attacks = Vec::new();
        
        // Use risk manager and gas oracle contextually at this block to refine simulation odds
        let _gas_price_info = infra
            .gas_oracle
            .get_gas_price(&infra.blockchain_manager.get_chain_name(), Some(block))
            .await
            .ok();
        // In a future enhancement, we will incorporate gas price into MEV simulation parameters
        
        for opp in opportunities {
            // Simulate competition
            let comp_result = self.competition.simulate(opp).await;
            competition_results.push(comp_result.clone());
            
            // Only simulate MEV if we win competition
            if comp_result.we_win && self.cfg.mev_attack_probability.unwrap_or(DEFAULT_MEV_ATTACK_PROBABILITY) > 0.0 {
                // Adjust MEV simulation seed with block context and gas price to create deterministic variability
                let mev_result = self.mev_sim.simulate(opp).await;
                mev_attacks.push(mev_result);
            }
        }
        
        Ok(SimulationResults {
            competition_results,
            mev_attacks,
        })
    }
    
    /// Execute trades for opportunities that won competition and survived MEV
    async fn execute_winning_trades(
        &self,
        opportunities: &[ArbitrageOpportunity],
        competition_results: &[CompetitionResult],
        mev_attacks: &[MEVAttackResult],
        infra: &Arc<ChainInfra>,
        block: u64,
    ) -> Result<Vec<TradeDetail>, BacktestError> {
        let mut trades = Vec::new();
        
        for (i, opp) in opportunities.iter().enumerate() {
            if i >= competition_results.len() {
                break;
            }
            
            let comp_result = &competition_results[i];
            if !comp_result.we_win {
                continue;
            }
            
            // Check if MEV attack succeeded
            let mev_succeeded = mev_attacks
                .iter()
                .any(|mev| mev.opportunity_id == opp.id() && mev.success);
            
            if !mev_succeeded {
                // Execute trade simulation
                if let Ok(trade) = self.execute_trade_sim_with_mev(
                    opp,
                    infra,
                    block,
                    opp.profit_usd,
                    None, // MEV prediction would go here if available
                ).await {
                    trades.push(trade);
                }
            }
        }
        
        Ok(trades)
    }
    
    /// Build the final block result
    fn build_block_result(
        &self,
        block_number: u64,
        execution_time_ms: u64,
        opportunities: Vec<ArbitrageOpportunity>,
        trades: Vec<TradeDetail>,
        simulation_results: SimulationResults,
        scan_time_ms: u64,
        rejection_stats: std::collections::HashMap<String, u64>,
    ) -> BlockResult {
        // Count opportunities by strategy label in metadata for accurate categorization
        let mut proactive_count = 0usize;
        let mut reactive_count = 0usize;
        let mut hot_token_count = 0usize;
        let mut cross_dex_count = 0usize;
        for opp in &opportunities {
            if let Some(meta) = &opp.metadata {
                if let Some(strategy) = meta.get("strategy").and_then(|v| v.as_str()) {
                    match strategy {
                        "proactive_scan" => proactive_count = proactive_count.saturating_add(1),
                        "reactive_swap" => reactive_count = reactive_count.saturating_add(1),
                        "hot_token" => hot_token_count = hot_token_count.saturating_add(1),
                        "cross_dex" => cross_dex_count = cross_dex_count.saturating_add(1),
                        _ => proactive_count = proactive_count.saturating_add(1),
                    }
                    continue;
                }
            }
            // Fallback categorization if metadata is missing
            proactive_count = proactive_count.saturating_add(1);
        }
        
        BlockResult {
            block_number,
            execution_time_ms,
            opportunities_found: opportunities.len(),
            proactive_opportunities_found: proactive_count,
            reactive_opportunities_found: reactive_count,
            hot_token_opportunities_found: hot_token_count,
            cross_dex_opportunities_found: cross_dex_count,
            trades_executed: trades,
            errors: Vec::new(),
            mev_attacks: simulation_results.mev_attacks,
            competition_results: simulation_results.competition_results,
            scan_metrics: ScanMetrics {
                proactive_opportunities: proactive_count,
                reactive_opportunities: reactive_count,
                hot_token_opportunities: hot_token_count,
                cross_dex_opportunities: cross_dex_count,
                total_scan_time_ms: scan_time_ms,
            },
            rejection_stats,
        }
    }
    

    async fn execute_trade_sim_with_mev(
        &self,
        opp:           &ArbitrageOpportunity,
        infra:         &Arc<ChainInfra>,
        block:         u64,
        profit_after:  f64,
        mev_prediction: Option<MEVPrediction>,
    ) -> Result<TradeDetail, ArbitrageError> {
        // Use transaction optimizer for accurate gas simulation
        let (gas_used, gas_cost_usd) = infra.transaction_optimizer
            .simulate_trade_submission(&opp.route, block)
            .await
            .map_err(|e| ArbitrageError::Submission(format!("Gas simulation failed: {}", e)))?;
        
        let net_profit = profit_after - gas_cost_usd;
        let success = net_profit > 0.0;

        info!(
            "\u{1F4B0} Profit tokens (approx USD before gas): {:.6}",
            profit_after
        );
        info!(
            "\u{26FD} Gas cost (USD): {:.6}",
            gas_cost_usd
        );
        info!(
            "\u{2705} Net profit (USD): {:.6} (success={})",
            net_profit,
            success
        );

        Ok(TradeDetail {
            opportunity: opp.clone(),
            success,
            profit_usd: net_profit,
            gas_used,
            tx_hash: None,
            failure_reason: if success {
                None
            } else {
                Some(format!("Insufficient profit after gas costs (${:.2})", gas_cost_usd))
            },
            mev_prediction,
        })
    }



    async fn write_results(
        &self,
        blocks: &[BlockResult],
        cfg:    &BacktestChainConfig,
    ) -> Result<String> {
        let summary = serde_json::to_string_pretty(&blocks)
            .map_err(|e| anyhow::anyhow!("failed to serialize results: {}", e))?;

        fs::write(&cfg.result_output_file, &summary).await
            .map_err(|e| anyhow::anyhow!("failed to write results: {}", e))?;

        let total_opportunities: usize = blocks.iter().map(|b| b.opportunities_found).sum();
        let total_trades: usize = blocks.iter().map(|b| b.trades_executed.len()).sum();
        let total_profit: f64 = blocks.iter()
            .flat_map(|b| &b.trades_executed)
            .filter(|t| t.success)
            .map(|t| t.profit_usd)
            .sum();

        debug!(
            "Results written: chain_name={}, total_blocks={}, total_opportunities={}, total_trades={}, total_profit={}",
            cfg.chain_name, blocks.len(), total_opportunities, total_trades, total_profit
        );

        let summary_msg = format!(
            "Backtest completed for {}: {} blocks, {} opportunities, {} trades, ${:.2} profit",
            cfg.chain_name,
            blocks.len(),
            total_opportunities,
            total_trades,
            total_profit,
        );

        Ok(summary_msg)
    }
}