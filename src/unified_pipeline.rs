use std::sync::Arc;
use tokio::sync::{broadcast, RwLock, Mutex};
use tracing::{info, warn, error, debug};
use std::collections::HashMap;

use crate::{
    alpha::AlphaEngine,
    mev_engine::MEVEngine,
    mempool::MEVOpportunity,
    errors::BotError,
    types::ArbitrageOpportunity,
};

/// Unified pipeline that orchestrates opportunity discovery and execution
/// This bridges the gap between Alpha engine discovery and MEV engine execution
pub struct UnifiedOpportunityPipeline {
    /// Alpha engine for opportunity discovery
    alpha_engine: Arc<AlphaEngine>,
    
    /// MEV engine for opportunity execution
    mev_engine: Arc<MEVEngine>,
    
    /// Channel for broadcasting opportunities from Alpha to MEV
    opportunity_broadcaster: broadcast::Sender<MEVOpportunity>,
    
    /// Channel for MEV engine to receive opportunities
    opportunity_receiver: Arc<Mutex<broadcast::Receiver<MEVOpportunity>>>,
    
    /// Opportunity deduplication cache
    dedup_cache: Arc<RwLock<HashMap<String, std::time::Instant>>>,
    
    /// Pipeline metrics
    metrics: Arc<PipelineMetrics>,
    
    /// Configuration
    config: PipelineConfig,
    
    /// Running state
    is_running: Arc<RwLock<bool>>,
}

#[derive(Clone)]
pub struct PipelineConfig {
    /// Maximum opportunities per block
    pub max_opportunities_per_block: usize,
    
    /// Opportunity expiry time in seconds
    pub opportunity_expiry_secs: u64,
    
    /// Minimum profit threshold to forward
    pub min_profit_usd: f64,
    
    /// Enable parallel discovery
    pub enable_parallel_discovery: bool,
    
    /// Discovery timeout in milliseconds
    pub discovery_timeout_ms: u64,
    
    /// Enable opportunity prioritization
    pub enable_prioritization: bool,
    
    /// Maximum concurrent executions
    pub max_concurrent_executions: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_opportunities_per_block: 100,
            opportunity_expiry_secs: 10,
            min_profit_usd: 5.0,
            enable_parallel_discovery: true,
            discovery_timeout_ms: 5000,
            enable_prioritization: true,
            max_concurrent_executions: 10,
        }
    }
}

struct PipelineMetrics {
    opportunities_discovered: std::sync::atomic::AtomicU64,
    opportunities_forwarded: std::sync::atomic::AtomicU64,
    opportunities_executed: std::sync::atomic::AtomicU64,
    opportunities_expired: std::sync::atomic::AtomicU64,
    opportunities_deduplicated: std::sync::atomic::AtomicU64,
    total_profit_usd: Arc<RwLock<f64>>,
    average_discovery_time_ms: Arc<RwLock<f64>>,
    average_execution_time_ms: Arc<RwLock<f64>>,
}

impl UnifiedOpportunityPipeline {
    /// Create a new unified pipeline
    pub async fn new(
        alpha_engine: Arc<AlphaEngine>,
        mev_engine: Arc<MEVEngine>,
        config: PipelineConfig,
    ) -> Result<Self, BotError> {
        // Create opportunity broadcast channel
        let (tx, rx) = broadcast::channel::<MEVOpportunity>(1024);

        Ok(Self {
            alpha_engine,
            mev_engine,
            opportunity_broadcaster: tx,
            opportunity_receiver: Arc::new(Mutex::new(rx)),
            dedup_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(PipelineMetrics {
                opportunities_discovered: std::sync::atomic::AtomicU64::new(0),
                opportunities_forwarded: std::sync::atomic::AtomicU64::new(0),
                opportunities_executed: std::sync::atomic::AtomicU64::new(0),
                opportunities_expired: std::sync::atomic::AtomicU64::new(0),
                opportunities_deduplicated: std::sync::atomic::AtomicU64::new(0),
                total_profit_usd: Arc::new(RwLock::new(0.0)),
                average_discovery_time_ms: Arc::new(RwLock::new(0.0)),
                average_execution_time_ms: Arc::new(RwLock::new(0.0)),
            }),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Create a new unified pipeline that uses an externally provided opportunity channel
    pub async fn new_with_channel(
        alpha_engine: Arc<AlphaEngine>,
        mev_engine: Arc<MEVEngine>,
        config: PipelineConfig,
        channel: Option<(broadcast::Sender<MEVOpportunity>, Arc<Mutex<broadcast::Receiver<MEVOpportunity>>>)>,
    ) -> Result<Self, BotError> {
        let (opportunity_broadcaster, opportunity_receiver) = match channel {
            Some((tx, rx)) => (tx, rx),
            None => {
                let (tx, rx) = broadcast::channel::<MEVOpportunity>(1024);
                (tx, Arc::new(Mutex::new(rx)))
            }
        };

        Ok(Self {
            alpha_engine,
            mev_engine,
            opportunity_broadcaster,
            opportunity_receiver,
            dedup_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(PipelineMetrics {
                opportunities_discovered: std::sync::atomic::AtomicU64::new(0),
                opportunities_forwarded: std::sync::atomic::AtomicU64::new(0),
                opportunities_executed: std::sync::atomic::AtomicU64::new(0),
                opportunities_expired: std::sync::atomic::AtomicU64::new(0),
                opportunities_deduplicated: std::sync::atomic::AtomicU64::new(0),
                total_profit_usd: Arc::new(RwLock::new(0.0)),
                average_discovery_time_ms: Arc::new(RwLock::new(0.0)),
                average_execution_time_ms: Arc::new(RwLock::new(0.0)),
            }),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the unified pipeline
    pub async fn start(self: Arc<Self>) -> Result<(), BotError> {
        let mut running = self.is_running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        info!("Starting Unified Opportunity Pipeline");

        // Start Alpha engine
        self.alpha_engine.start().await?;

        // Connect MEV engine to opportunity stream
        self.connect_mev_engine().await?;

        // Start MEV engine
        self.mev_engine.clone().start().await
            .map_err(|e| BotError::Configuration(e.to_string()))?;

        // Start opportunity forwarder task
        let pipeline = self.clone();
        tokio::spawn(async move {
            pipeline.opportunity_forwarder_loop().await;
        });

        // Start cleanup task
        let pipeline = self.clone();
        tokio::spawn(async move {
            pipeline.cleanup_loop().await;
        });

        info!("Unified Opportunity Pipeline started successfully");
        Ok(())
    }

    /// Stop the unified pipeline
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
        drop(running);

        info!("Stopping Unified Opportunity Pipeline");

        // Stop Alpha engine
        self.alpha_engine.stop().await;

        // Stop MEV engine
        self.mev_engine.stop().await;

        info!("Unified Opportunity Pipeline stopped");
    }

    /// Connect MEV engine to opportunity stream
    async fn connect_mev_engine(&self) -> Result<(), BotError> {
        // The MEV engine should already be initialized with a receiver
        // This is where we'd reconnect if needed
        Ok(())
    }

    /// Main loop for forwarding opportunities from Alpha to MEV engine
    async fn opportunity_forwarder_loop(&self) {
        // Reduce frequency from 100ms to 1 second to prevent excessive graph rebuilding
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        
        while *self.is_running.read().await {
            interval.tick().await;
            
            // Get the latest block number
            if let Ok(block_number) = self.get_current_block_number().await {
                // Run discovery cycle
                if let Err(e) = self.discover_and_forward_opportunities(block_number).await {
                    error!("Failed to discover opportunities: {}", e);
                }
            }
        }
    }

    /// Discover opportunities and forward to MEV engine
    async fn discover_and_forward_opportunities(
        &self,
        block_number: u64,
    ) -> Result<(), BotError> {
        let start_time = std::time::Instant::now();
        
        // Run Alpha engine discovery
        self.alpha_engine.run_discovery_cycle(block_number).await?;
        
        // Get discovered opportunities
        let opportunities = self.alpha_engine
            .get_arbitrage_opportunities(block_number)
            .await?;
        
        self.metrics.opportunities_discovered
            .fetch_add(opportunities.len() as u64, std::sync::atomic::Ordering::Relaxed);
        
        // Convert and filter opportunities
        let mev_opportunities = self.convert_and_filter_opportunities(opportunities.clone()).await?;
        
        // Prioritize if enabled
        let prioritized = if self.config.enable_prioritization {
            self.prioritize_opportunities(mev_opportunities).await
        } else {
            mev_opportunities
        };
        
        // Forward to MEV engine
        let mut forwarded = 0;
        for opportunity in prioritized.into_iter().take(self.config.max_opportunities_per_block) {
            if self.should_forward_opportunity(&opportunity).await {
                if let Err(e) = self.opportunity_broadcaster.send(opportunity) {
                    warn!("Failed to broadcast opportunity: {}", e);
                } else {
                    forwarded += 1;
                }
            }
        }
        
        self.metrics.opportunities_forwarded
            .fetch_add(forwarded, std::sync::atomic::Ordering::Relaxed);
        
        // Update metrics
        let discovery_time = start_time.elapsed().as_millis() as f64;
        let mut avg_time = self.metrics.average_discovery_time_ms.write().await;
        *avg_time = (*avg_time * 0.9) + (discovery_time * 0.1);
        
        debug!(
            "Discovery cycle for block {} completed: {} opportunities found, {} forwarded",
            block_number,
            opportunities.len(),
            forwarded
        );
        
        Ok(())
    }

    /// Convert ArbitrageOpportunity to MEVOpportunity and filter
    async fn convert_and_filter_opportunities(
        &self,
        opportunities: Vec<ArbitrageOpportunity>,
    ) -> Result<Vec<MEVOpportunity>, BotError> {
        let mut mev_opportunities = Vec::new();
        
        for opp in opportunities {
            // Check profit threshold
            if opp.profit_usd < self.config.min_profit_usd {
                continue;
            }
            
            // Convert to MEVOpportunity
            let mev_opp = MEVOpportunity::Arbitrage(opp.route.clone());
            
            // Check deduplication
            if !self.is_duplicate(&mev_opp).await {
                mev_opportunities.push(mev_opp);
            }
        }
        
        Ok(mev_opportunities)
    }

    /// Check if opportunity is duplicate
    async fn is_duplicate(&self, opportunity: &MEVOpportunity) -> bool {
        let key = self.get_opportunity_key(opportunity);
        let mut cache = self.dedup_cache.write().await;
        
        // Check if exists and not expired
        if let Some(timestamp) = cache.get(&key) {
            if timestamp.elapsed().as_secs() < self.config.opportunity_expiry_secs {
                self.metrics.opportunities_deduplicated
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        
        // Add to cache
        cache.insert(key, std::time::Instant::now());
        false
    }

    /// Generate unique key for opportunity
    fn get_opportunity_key(&self, opportunity: &MEVOpportunity) -> String {
        match opportunity {
            MEVOpportunity::Arbitrage(arb) => {
                format!("arb_{}_{}_{}_{}", 
                    arb.chain_id, arb.token_in, arb.token_out, arb.amount_in)
            }
            MEVOpportunity::Sandwich(sandwich) => {
                format!("sandwich_{}_{}_{}", 
                    sandwich.target_tx_hash, sandwich.pool_address, sandwich.amount_in)
            }
            MEVOpportunity::Liquidation(liq) => {
                format!("liq_{}_{}", liq.borrower, liq.debt_token)
            }
            MEVOpportunity::JITLiquidity(jit) => {
                format!("jit_{}_{}_{}", jit.pool_address, jit.tick_lower, jit.tick_upper)
            }
            MEVOpportunity::FlashLoan(flash) => {
                format!("flash_{}_{}_{}", 
                    flash.lending_protocol, flash.amount_to_flash, flash.estimated_profit_usd)
            }
            MEVOpportunity::OracleUpdate(oracle) => {
                format!("oracle_{}_{}_{}", 
                    oracle.oracle_address, oracle.token_address, oracle.new_price)
            }
        }
    }

    /// Prioritize opportunities by profit and execution likelihood
    async fn prioritize_opportunities(
        &self,
        mut opportunities: Vec<MEVOpportunity>,
    ) -> Vec<MEVOpportunity> {
        opportunities.sort_by(|a, b| {
            let profit_a = self.get_opportunity_profit(a);
            let profit_b = self.get_opportunity_profit(b);
            profit_b.partial_cmp(&profit_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        opportunities
    }

    /// Get profit estimate for opportunity
    fn get_opportunity_profit(&self, opportunity: &MEVOpportunity) -> f64 {
        match opportunity {
            MEVOpportunity::Arbitrage(arb) => arb.profit_usd,
            MEVOpportunity::Sandwich(sandwich) => sandwich.estimated_profit_usd,
            MEVOpportunity::Liquidation(liq) => liq.estimated_profit_usd,
            MEVOpportunity::JITLiquidity(jit) => jit.estimated_profit_usd,
            MEVOpportunity::FlashLoan(flash) => flash.estimated_profit_usd,
            MEVOpportunity::OracleUpdate(oracle) => oracle.estimated_profit_usd,
        }
    }

    /// Check if opportunity should be forwarded
    async fn should_forward_opportunity(&self, opportunity: &MEVOpportunity) -> bool {
        // Check profit threshold
        let profit = self.get_opportunity_profit(opportunity);
        if profit < self.config.min_profit_usd {
            return false;
        }
        
        // Check if MEV engine can handle this type
        // This would be checked against registered strategies
        true
    }

    /// Cleanup expired entries from dedup cache
    async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        
        while *self.is_running.read().await {
            interval.tick().await;
            
            let mut cache = self.dedup_cache.write().await;
            
            cache.retain(|_, timestamp| {
                timestamp.elapsed().as_secs() < self.config.opportunity_expiry_secs * 2
            });
            
            debug!("Cleaned up dedup cache, {} entries remaining", cache.len());
        }
    }

    /// Get current block number
    async fn get_current_block_number(&self) -> Result<u64, BotError> {
        // Query any attached chain infra through the alpha engine
        // We use the first available chain
        self.alpha_engine.get_head_block_number().await
    }

    /// Get pipeline metrics
    pub async fn get_metrics(&self) -> PipelineMetricsSnapshot {
        PipelineMetricsSnapshot {
            opportunities_discovered: self.metrics.opportunities_discovered
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_forwarded: self.metrics.opportunities_forwarded
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_executed: self.metrics.opportunities_executed
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_expired: self.metrics.opportunities_expired
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_deduplicated: self.metrics.opportunities_deduplicated
                .load(std::sync::atomic::Ordering::Relaxed),
            total_profit_usd: *self.metrics.total_profit_usd.read().await,
            average_discovery_time_ms: *self.metrics.average_discovery_time_ms.read().await,
            average_execution_time_ms: *self.metrics.average_execution_time_ms.read().await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineMetricsSnapshot {
    pub opportunities_discovered: u64,
    pub opportunities_forwarded: u64,
    pub opportunities_executed: u64,
    pub opportunities_expired: u64,
    pub opportunities_deduplicated: u64,
    pub total_profit_usd: f64,
    pub average_discovery_time_ms: f64,
    pub average_execution_time_ms: f64,
}