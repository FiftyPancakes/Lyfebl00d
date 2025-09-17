use async_trait::async_trait;
use ethers::types::{Address, U256};

use std::sync::Arc;


use crate::{
    blockchain::BlockchainManager,
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    path::PathFinder,
    pool_manager::PoolManagerTrait,
    pool_states::PoolStateOracle,
    price_oracle::PriceOracle,
    types::ArbitrageRoute,
    shared_setup::ChainInfra,
};

pub mod cyclic_scanner;
pub mod statistical_scanner;
pub mod cross_chain_scanner;
pub mod market_making_scanner;
pub mod event_driven_discovery;
pub mod unified_discovery;

// Re-export scanners
pub use cyclic_scanner::CyclicArbitrageScanner;
pub use statistical_scanner::StatisticalArbitrageScanner;
pub use cross_chain_scanner::CrossChainArbitrageScanner;
pub use market_making_scanner::MarketMakingScanner;
pub use event_driven_discovery::EventDrivenDiscoveryService;
pub use unified_discovery::UnifiedArbitrageDiscovery;

/// Enhanced configuration for discovery scanners
#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub min_profit_usd: f64,
    pub max_gas_cost_usd: f64,
    pub min_liquidity_usd: f64,
    pub max_price_impact_bps: u32,
    pub scan_interval_ms: u64,
    pub batch_size: usize,
    pub max_hops: usize,
    pub timeout_ms: u64,
    pub enable_statistical_filtering: bool,
    pub min_confidence_score: f64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            min_profit_usd: 10.0,
            max_gas_cost_usd: 50.0,
            min_liquidity_usd: 10000.0,
            max_price_impact_bps: 500, // 5%
            scan_interval_ms: 1000,
            batch_size: 100,
            max_hops: 5,
            timeout_ms: 30000, // 30 seconds
            enable_statistical_filtering: true,
            min_confidence_score: 0.7,
        }
    }
}

/// Simplified trait for all discovery scanners - no complex sender management
#[async_trait]
pub trait DiscoveryScanner: Send + Sync + 'static {
    /// Human-readable scanner identifier
    fn name(&self) -> &'static str;

    /// Initialize the scanner with configuration
    async fn initialize(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError>;

    /// Shutdown the scanner
    async fn shutdown(&self) -> Result<(), ArbitrageError>;

    /// Check if the scanner is running
    async fn is_running(&self) -> bool;

    /// Get scanner statistics
    async fn get_stats(&self) -> ScannerStats;

    /// Main scanning method - returns opportunities directly
    async fn scan(&self, block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError>;

    /// Update scanner configuration at runtime
    async fn update_config(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        // Default implementation validates inputs that materially affect scanning logic
        if !(0.0..=1.0).contains(&config.min_confidence_score) {
            return Err(ArbitrageError::Config(format!(
                "Invalid min_confidence_score: {}",
                config.min_confidence_score
            )));
        }
        if config.max_price_impact_bps > 10_000 {
            return Err(ArbitrageError::Config(format!(
                "max_price_impact_bps exceeds 10000: {}",
                config.max_price_impact_bps
            )));
        }
        if config.max_hops == 0 || config.batch_size == 0 {
            return Err(ArbitrageError::Config("max_hops and batch_size must be non-zero".to_string()));
        }
        Ok(())
    }

    /// Get current configuration
    async fn get_config(&self) -> DiscoveryConfig;
}

/// Statistics for a discovery scanner
#[derive(Debug, Clone, Default)]
pub struct ScannerStats {
    pub opportunities_found: u64,
    pub opportunities_sent: u64,
    pub last_scan_time: Option<u64>,
    pub errors: u64,
    pub avg_scan_duration_ms: f64,
}

/// Common scanner context shared across all scanners with enhanced error handling
#[derive(Clone)]
pub struct ScannerContext {
    pub chain_infra: Arc<ChainInfra>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub path_finder: Arc<dyn PathFinder + Send + Sync>,
    pub pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    pub pool_state_oracle: Arc<PoolStateOracle>,
    pub blockchain_manager: Arc<dyn BlockchainManager + Send + Sync>,
}

impl ScannerContext {
    pub fn new(chain_infra: Arc<ChainInfra>) -> Self {
        Self {
            price_oracle: chain_infra.price_oracle.clone(),
            path_finder: chain_infra.path_finder.clone(),
            pool_manager: chain_infra.pool_manager.clone(),
            pool_state_oracle: chain_infra.pool_state_oracle.clone(),
            blockchain_manager: chain_infra.blockchain_manager.clone(),
            chain_infra,
        }
    }

    /// Get chain name safely
    pub fn get_chain_name(&self) -> String {
        self.blockchain_manager.get_chain_name().to_string()
    }

    /// Get test amount for a token (used for simulation/profit calculation)
    pub fn get_test_amount_for_token(&self, _token: Address) -> U256 {
        // Return 1 ETH equivalent in wei as a standard test amount
        U256::from(1000000000000000000u128)
    }

    /// Get current block number safely
    pub async fn get_current_block_number(&self) -> Result<u64, ArbitrageError> {
        self.blockchain_manager.get_current_block_number().await
            .map_err(|e| ArbitrageError::BlockchainQueryFailed(format!("Failed to get current block: {}", e)))
    }
}

/// Helper trait for converting arbitrage opportunities to MEV opportunities
pub trait OpportunityConverter {
    fn to_mev_opportunity(&self, route: ArbitrageRoute, block_number: u64) -> MEVOpportunity;
}

impl OpportunityConverter for ArbitrageRoute {
    fn to_mev_opportunity(&self, route: ArbitrageRoute, block_number: u64) -> MEVOpportunity {
        MEVOpportunity::Arbitrage(route)
    }
}

/// Scanner manager for coordinating multiple discovery scanners
pub struct ScannerManager {
    scanners: Vec<Arc<dyn DiscoveryScanner>>,
    config: DiscoveryConfig,
}

impl ScannerManager {
    pub fn new(scanners: Vec<Arc<dyn DiscoveryScanner>>, config: DiscoveryConfig) -> Self {
        Self { scanners, config }
    }

    /// Initialize all scanners
    pub async fn initialize_all(&self) -> Result<(), ArbitrageError> {
        for scanner in &self.scanners {
            scanner.initialize(self.config.clone()).await?;
        }
        Ok(())
    }

    /// Shutdown all scanners
    pub async fn shutdown_all(&self) -> Result<(), ArbitrageError> {
        for scanner in &self.scanners {
            if let Err(e) = scanner.shutdown().await {
                tracing::warn!("Error shutting down scanner {}: {}", scanner.name(), e);
            }
        }
        Ok(())
    }

    /// Run all scanners in parallel for a given block
    pub async fn scan_all(&self, block_number: Option<u64>) -> Vec<MEVOpportunity> {
        let mut all_opportunities = Vec::new();
        let scan_futures: Vec<_> = self.scanners.iter().map(|scanner| {
            let scanner = scanner.clone();
            async move {
                match scanner.scan(block_number).await {
                    Ok(opportunities) => opportunities,
                    Err(e) => {
                        tracing::error!("Scanner {} failed: {}", scanner.name(), e);
                        Vec::new()
                    }
                }
            }
        }).collect();

        let results = futures::future::join_all(scan_futures).await;
        for opportunities in results {
            all_opportunities.extend(opportunities);
        }

        all_opportunities
    }

    /// Get aggregated statistics from all scanners
    pub async fn get_aggregated_stats(&self) -> ScannerStats {
        let mut aggregated = ScannerStats::default();
        
        for scanner in &self.scanners {
            let stats = scanner.get_stats().await;
            aggregated.opportunities_found += stats.opportunities_found;
            aggregated.opportunities_sent += stats.opportunities_sent;
            aggregated.errors += stats.errors;
            if let Some(last_scan) = stats.last_scan_time {
                aggregated.last_scan_time = Some(aggregated.last_scan_time.unwrap_or(0).max(last_scan));
            }
            aggregated.avg_scan_duration_ms = (aggregated.avg_scan_duration_ms + stats.avg_scan_duration_ms) / 2.0;
        }
        
        aggregated
    }
}

/// Base scanner implementation with common functionality
pub struct BaseScanner {
    running: Arc<tokio::sync::RwLock<bool>>,
    stats: Arc<tokio::sync::RwLock<ScannerStats>>,
    config: Arc<tokio::sync::RwLock<DiscoveryConfig>>,
}

impl BaseScanner {
    pub fn new() -> Self {
        Self {
            running: Arc::new(tokio::sync::RwLock::new(false)),
            stats: Arc::new(tokio::sync::RwLock::new(ScannerStats::default())),
            config: Arc::new(tokio::sync::RwLock::new(DiscoveryConfig::default())),
        }
    }

    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    pub async fn start(&self) {
        *self.running.write().await = true;
    }

    pub async fn stop(&self) {
        *self.running.write().await = false;
    }

    pub async fn get_stats(&self) -> ScannerStats {
        self.stats.read().await.clone()
    }

    /// Thread-safe stats update with closure
    pub async fn update_stats<F>(&self, updater: F) 
    where 
        F: FnOnce(&mut ScannerStats) + Send,
    {
        let mut stats = self.stats.write().await;
        updater(&mut stats);
    }

    pub async fn get_config(&self) -> DiscoveryConfig {
        self.config.read().await.clone()
    }

    pub async fn set_config(&self, config: DiscoveryConfig) {
        *self.config.write().await = config;
    }

    /// Record successful scan with opportunities found
    pub async fn record_scan_result(&self, opportunities_count: usize, scan_duration_ms: f64) {
        let mut stats = self.stats.write().await;
        stats.opportunities_found = stats.opportunities_found.saturating_add(opportunities_count as u64);
        stats.avg_scan_duration_ms = if stats.avg_scan_duration_ms == 0.0 {
            scan_duration_ms
        } else {
            (stats.avg_scan_duration_ms + scan_duration_ms) / 2.0
        };
        stats.last_scan_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
    }

    /// Record error in scanning
    pub async fn record_error(&self) {
        let mut stats = self.stats.write().await;
        stats.errors = stats.errors.saturating_add(1);
    }

    /// Mark opportunities as sent (for compatibility)
    pub async fn record_opportunities_sent(&self, count: usize) {
        let mut stats = self.stats.write().await;
        stats.opportunities_sent = stats.opportunities_sent.saturating_add(count as u64);
    }
}