use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tracing::{info, warn, error};

use crate::{
    errors::ArbitrageError,
    strategies::discovery::{
        DiscoveryConfig, ScannerContext, DiscoveryScanner,
        UnifiedArbitrageDiscovery,
    },
    types::{ChainEvent, MarketRegime},
    alpha::{ScannerType, RegimeScannerConfig},
};

impl crate::types::MarketRegime {
    /// Get the recommended scanner configuration for this market regime
    pub fn scanner_config(&self) -> RegimeScannerConfig {
        match self {
            crate::types::MarketRegime::HighVolatility => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.6,
            },
            crate::types::MarketRegime::LowVolatility => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking, ScannerType::CrossChain],
                priority_order: vec![ScannerType::Statistical, ScannerType::CrossChain, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 1.0,
            },
            crate::types::MarketRegime::Trending => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Statistical, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.8,
            },
            crate::types::MarketRegime::Sideways => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Statistical, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.9,
            },
            crate::types::MarketRegime::Ranging => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::Statistical, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Statistical, ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.8,
            },
            crate::types::MarketRegime::Volatile => RegimeScannerConfig {
                active_scanners: vec![ScannerType::Cyclic, ScannerType::MarketMaking],
                priority_order: vec![ScannerType::Cyclic, ScannerType::MarketMaking],
                resource_allocation: 0.7,
            },
        }
    }
}

/// Dynamic scanner manager that adapts scanner configuration based on market regime
pub struct DynamicScannerManager {
    context: ScannerContext,
    config: Arc<RwLock<DiscoveryConfig>>,
    running: Arc<RwLock<bool>>,
    event_broadcaster: broadcast::Sender<ChainEvent>,
    event_receiver: broadcast::Receiver<ChainEvent>,
    current_regime: Arc<RwLock<MarketRegime>>,
    active_scanners: Arc<RwLock<HashMap<ScannerType, Arc<dyn DiscoveryScanner + Send + Sync>>>>,
    unified_discovery: Option<Arc<UnifiedArbitrageDiscovery>>,
    statistical_pair_stats: Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
}

impl DynamicScannerManager {
    pub fn new(
        context: ScannerContext,
        event_broadcaster: broadcast::Sender<ChainEvent>,
        statistical_pair_stats: Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
    ) -> Self {
        let event_broadcaster_clone = event_broadcaster.clone();
        Self {
            context,
            config: Arc::new(RwLock::new(DiscoveryConfig::default())),
            running: Arc::new(RwLock::new(false)),
            event_broadcaster: event_broadcaster_clone,
            event_receiver: event_broadcaster.subscribe(),
            current_regime: Arc::new(RwLock::new(MarketRegime::Sideways)),
            active_scanners: Arc::new(RwLock::new(HashMap::new())),
            unified_discovery: None,
            statistical_pair_stats,
        }
    }

    /// Start the dynamic scanner manager
    pub async fn start(&self) -> Result<(), ArbitrageError> {
        info!("Starting DynamicScannerManager");
        {
            let mut running_guard = self.running.write().await;
            *running_guard = true;
        }

        // Initialize unified discovery service with correct event broadcaster
        let unified_discovery = Arc::new(UnifiedArbitrageDiscovery::new(
            self.context.clone(),
            self.event_broadcaster.clone(),
            self.statistical_pair_stats.clone(),
        ));

        if let Err(e) = unified_discovery.start().await {
            error!("Failed to start unified discovery service: {}", e);
            return Err(e);
        }

        // Store reference for later use (requires interior mutability or redesign for production)
        {
            let unified_discovery_ptr = Arc::clone(&unified_discovery);
            let this = self as *const Self as *mut Self;
            // SAFETY: This is safe here because we have exclusive access during startup
            unsafe {
                (*this).unified_discovery = Some(unified_discovery_ptr);
            }
        }

        // Start the regime monitoring loop
        let context = self.context.clone();
        let config = self.config.clone();
        let running = self.running.clone();
        let current_regime = self.current_regime.clone();
        let active_scanners = self.active_scanners.clone();
        let mut event_receiver = self.event_receiver.resubscribe();
        let statistical_pair_stats = self.statistical_pair_stats.clone();

        tokio::spawn(async move {
            Self::regime_monitoring_loop(
                context,
                config,
                running,
                &mut event_receiver,
                current_regime,
                active_scanners,
                statistical_pair_stats,
            ).await;
        });

        info!("DynamicScannerManager started successfully");
        Ok(())
    }

    /// Stop the dynamic scanner manager
    pub async fn stop(&self) -> Result<(), ArbitrageError> {
        info!("Stopping DynamicScannerManager");
        *self.running.write().await = false;

        // Stop all active scanners
        let scanners = self.active_scanners.read().await;
        for (scanner_type, scanner) in scanners.iter() {
            if let Err(e) = scanner.shutdown().await {
                error!("Failed to stop scanner {:?}: {}", scanner_type, e);
            }
        }

        Ok(())
    }

    /// Main regime monitoring loop
    async fn regime_monitoring_loop(
        context: ScannerContext,
        config: Arc<RwLock<DiscoveryConfig>>,
        running: Arc<RwLock<bool>>,
        event_receiver: &mut broadcast::Receiver<ChainEvent>,
        current_regime: Arc<RwLock<MarketRegime>>,
        active_scanners: Arc<RwLock<HashMap<ScannerType, Arc<dyn DiscoveryScanner + Send + Sync>>>>,
        statistical_pair_stats: Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
    ) {
        info!("Regime monitoring loop started");

        while *running.read().await {
            match event_receiver.recv().await {
                Ok(event) => {
                    match event {
                        ChainEvent::NewMarketRegime(new_regime) => {
                            if let Err(e) = Self::handle_regime_change(
                                &context,
                                &config,
                                &current_regime,
                                &active_scanners,
                                &statistical_pair_stats,
                                new_regime,
                            ).await {
                                error!("Failed to handle regime change: {}", e);
                            }
                        }
                        ChainEvent::EvmNewHead { .. } => {
                            // Periodic scanner health check
                            if let Err(e) = Self::health_check_scanners(&active_scanners).await {
                                warn!("Scanner health check failed: {}", e);
                            }
                        }
                        _ => {} // Ignore other events
                    }
                }
                Err(e) => {
                    warn!("Error receiving event in scanner manager: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }

        info!("Regime monitoring loop stopped");
    }

    /// Handle market regime change
    async fn handle_regime_change(
        context: &ScannerContext,
        config: &Arc<RwLock<DiscoveryConfig>>,
        current_regime: &Arc<RwLock<MarketRegime>>,
        active_scanners: &Arc<RwLock<HashMap<ScannerType, Arc<dyn DiscoveryScanner + Send + Sync>>>>,
        statistical_pair_stats: &Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
        new_regime: MarketRegime,
    ) -> Result<(), ArbitrageError> {
        let old_regime = current_regime.read().await.clone();

        // Clone new_regime to avoid ownership issues
        let new_regime_clone = new_regime.clone();

        if old_regime == new_regime_clone {
            return Ok(()); // No change
        }
        info!("Market regime changed from {:?} to {:?}", old_regime, new_regime_clone);

        // Update current regime
        *current_regime.write().await = new_regime_clone.clone();

        // Get new scanner configuration
        let scanner_config = new_regime_clone.scanner_config();

        // Reconfigure scanners for the new regime
        Self::reconfigure_scanners_for_regime(
            context,
            config,
            active_scanners,
            statistical_pair_stats,
            &scanner_config,
            new_regime_clone.clone(),
        ).await?;

        info!(
            "Scanner reconfiguration completed for regime {:?}: {} active scanners",
            new_regime_clone,
            scanner_config.active_scanners.len()
        );

        Ok(())
    }

    /// Reconfigure scanners for the new market regime
    async fn reconfigure_scanners_for_regime(
        context: &ScannerContext,
        config: &Arc<RwLock<DiscoveryConfig>>,
        active_scanners: &Arc<RwLock<HashMap<ScannerType, Arc<dyn DiscoveryScanner + Send + Sync>>>>,
        statistical_pair_stats: &Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
        scanner_config: &RegimeScannerConfig,
        _regime: MarketRegime,
    ) -> Result<(), ArbitrageError> {
        let mut scanners = active_scanners.write().await;
        let current_config = config.read().await;

        // Determine which scanners to stop
        let active_types: Vec<ScannerType> = scanners.keys().cloned().collect();
        let desired_types = &scanner_config.active_scanners;

        // Stop scanners that are no longer needed
        for scanner_type in active_types {
            if !desired_types.contains(&scanner_type) {
                if let Some(scanner) = scanners.get(&scanner_type) {
                    if let Err(e) = scanner.shutdown().await {
                        error!("Failed to stop scanner {:?}: {}", scanner_type, e);
                    }
                }
                scanners.remove(&scanner_type);
                info!("Stopped scanner: {:?}", scanner_type);
            }
        }

        // Start or update scanners that should be active
        for scanner_type in desired_types {
            if !scanners.contains_key(scanner_type) {
                // Create and start new scanner
                let scanner = Self::create_scanner(
                    context,
                    &current_config,
                    statistical_pair_stats,
                    *scanner_type,
                ).await?;

                if let Err(e) = scanner.initialize(current_config.clone()).await {
                    error!("Failed to initialize scanner {:?}: {}", scanner_type, e);
                    continue;
                }

                scanners.insert(*scanner_type, scanner);
                info!("Started scanner: {:?}", scanner_type);
            }
        }

        // Update configuration for all active scanners
        let regime_adjusted_config = Self::adjust_config_for_regime(&current_config, scanner_config);
        for scanner in scanners.values() {
            if let Err(e) = scanner.update_config(regime_adjusted_config.clone()).await {
                warn!("Failed to update scanner config: {}", e);
            }
        }

        Ok(())
    }

    /// Create a scanner instance for the given type
    async fn create_scanner(
        context: &ScannerContext,
        config: &DiscoveryConfig,
        statistical_pair_stats: &Arc<RwLock<HashMap<(ethers::types::Address, ethers::types::Address), crate::types::TokenPairStats>>>,
        scanner_type: ScannerType,
    ) -> Result<Arc<dyn DiscoveryScanner + Send + Sync>, ArbitrageError> {
        match scanner_type {
            ScannerType::Cyclic => {
                let scanner = Arc::new(crate::strategies::discovery::CyclicArbitrageScanner::new(
                    context.clone(),
                    (config.min_profit_usd as u64 * 1_000_000_000_000_000_000).into(),
                    config.min_profit_usd,
                ));
                Ok(scanner)
            }
            ScannerType::Statistical => {
                let scanner = Arc::new(crate::strategies::discovery::StatisticalArbitrageScanner::new(
                    context.clone(),
                    (config.min_profit_usd as u64 * 1_000_000_000_000_000_000).into(),
                    config.min_profit_usd,
                    2.0, // Z-score threshold
                    statistical_pair_stats.clone(),
                ));
                Ok(scanner)
            }
            ScannerType::MarketMaking => {
                let scanner = Arc::new(crate::strategies::discovery::MarketMakingScanner::new(
                    context.clone(),
                    (config.min_profit_usd as u64 * 1_000_000_000_000_000_000).into(),
                    config.min_profit_usd,
                ));
                Ok(scanner)
            }
            ScannerType::CrossChain => {
                // Cross-chain scanner requires additional setup
                // For now, return an error indicating it's not available
                Err(ArbitrageError::Other("Cross-chain scanner not yet implemented in dynamic manager".to_string()))
            }
        }
    }

    /// Adjust configuration based on regime scanner config
    fn adjust_config_for_regime(
        base_config: &DiscoveryConfig,
        scanner_config: &RegimeScannerConfig,
    ) -> DiscoveryConfig {
        let mut adjusted = base_config.clone();

        // Adjust scan interval based on resource allocation
        // Higher resource allocation = more frequent scanning
        adjusted.scan_interval_ms = (adjusted.scan_interval_ms as f64 * (2.0 - scanner_config.resource_allocation)) as u64;

        // Adjust batch size based on priority
        // Higher priority scanners get larger batches
        adjusted.batch_size = (adjusted.batch_size as f64 * scanner_config.resource_allocation) as usize;
        adjusted.batch_size = adjusted.batch_size.max(10); // Minimum batch size

        adjusted
    }

    /// Health check for active scanners
    async fn health_check_scanners(
        active_scanners: &Arc<RwLock<HashMap<ScannerType, Arc<dyn DiscoveryScanner + Send + Sync>>>>,
    ) -> Result<(), ArbitrageError> {
        let scanners = active_scanners.read().await;

        for (scanner_type, scanner) in scanners.iter() {
            if !scanner.is_running().await {
                warn!("Scanner {:?} is not running, attempting restart", scanner_type);
                // In a real implementation, you'd attempt to restart the scanner
            }

            let stats = scanner.get_stats().await;
            if stats.errors > 10 {
                warn!("Scanner {:?} has high error count: {}", scanner_type, stats.errors);
            }
        }

        Ok(())
    }

    /// Get current scanner status
    pub async fn get_scanner_status(&self) -> HashMap<ScannerType, bool> {
        let scanners = self.active_scanners.read().await;
        let mut status = HashMap::new();

        for (scanner_type, scanner) in scanners.iter() {
            status.insert(*scanner_type, scanner.is_running().await);
        }

        status
    }

    /// Get current market regime
    pub async fn get_current_regime(&self) -> MarketRegime {
        self.current_regime.read().await.clone()
    }

    /// Manually trigger regime change (for testing)
    pub async fn force_regime_change(&self, new_regime: MarketRegime) -> Result<(), ArbitrageError> {
        let current_regime = self.current_regime.clone();
        let active_scanners = self.active_scanners.clone();
        let statistical_pair_stats = self.statistical_pair_stats.clone();
        let config = self.config.clone();

        Self::handle_regime_change(
            &self.context,
            &config,
            &current_regime,
            &active_scanners,
            &statistical_pair_stats,
            new_regime,
        ).await
    }

    /// Get scanner performance metrics
    pub async fn get_scanner_metrics(&self) -> HashMap<ScannerType, crate::strategies::discovery::ScannerStats> {
        let scanners = self.active_scanners.read().await;
        let mut metrics = HashMap::new();

        for (scanner_type, scanner) in scanners.iter() {
            metrics.insert(*scanner_type, scanner.get_stats().await);
        }

        metrics
    }
}
