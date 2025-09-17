use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use ethers::types::{Address, H256, Block};
use std::sync::atomic::{AtomicUsize, Ordering};
use futures;

use crate::{
    config::Config,
    errors::{BotError, ArbitrageError},
    types::{
        ArbitrageOpportunity
    },
    shared_setup::ChainInfra as SharedChainInfra,
    cross::CrossChainPathfinder,
    strategies::discovery::{
        CyclicArbitrageScanner, StatisticalArbitrageScanner,
        CrossChainArbitrageScanner, MarketMakingScanner,
        DiscoveryConfig, ScannerContext, DiscoveryScanner,
        UnifiedArbitrageDiscovery, EventDrivenDiscoveryService,
    },
    mempool::MEVOpportunity,
    alpha::{
        circuit_breaker::CircuitBreaker,
        types::{ LiquidationCandidate, YieldStrategy, OptionsPosition},
        scanner_manager::DynamicScannerManager,
        MarketRegime, AlphaEngineConfig, AlphaPerformanceMetrics, ScannerPerformanceMetrics
    },
    types::TokenPairStats
};

/// The Alpha Engine - strategic core of the trading system
/// 
/// This engine orchestrates multiple discovery scanners to identify trading opportunities
/// across different strategies and market conditions. It operates with strict separation
/// of concerns: the alpha engine finds opportunities, the MEV engine executes them.
#[derive(Clone)]
pub struct AlphaEngine {
    alpha_config: AlphaEngineConfig,
    chain_infras: Arc<RwLock<HashMap<String, SharedChainInfra>>>,
    event_broadcaster: broadcast::Sender<crate::types::ChainEvent>,
    metrics: Arc<crate::types::ArbitrageMetrics>,
    running: Arc<RwLock<bool>>,
    circuit_breaker: Arc<CircuitBreaker>,
    performance_metrics: Arc<RwLock<AlphaPerformanceMetrics>>,
    statistical_pair_stats: Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
    execution_semaphore: Arc<Semaphore>,
    active_scanners: Arc<AtomicUsize>,
    current_market_regime: Arc<RwLock<MarketRegime>>,
    cross_chain_pathfinder: Option<Arc<CrossChainPathfinder>>,
    liquidation_candidates: Arc<RwLock<Vec<LiquidationCandidate>>>,
    yield_strategies: Arc<RwLock<Vec<YieldStrategy>>>,
    options_positions: Arc<RwLock<Vec<OptionsPosition>>>,
    market_regime_history: Arc<RwLock<std::collections::VecDeque<(MarketRegime, Instant)>>>,
    last_scan_timestamps: Arc<RwLock<HashMap<String, Instant>>>,
    opportunity_cache: Arc<RwLock<HashMap<String, ArbitrageOpportunity>>>,
    discovery_scanners: Arc<RwLock<HashMap<String, Arc<dyn DiscoveryScanner>>>>,
    // New event-driven discovery services
    unified_discovery: Arc<RwLock<Option<Arc<UnifiedArbitrageDiscovery>>>>,
    dynamic_scanner_manager: Arc<RwLock<Option<Arc<DynamicScannerManager>>>>,
}

impl AlphaEngine {
    /// Create a new Alpha Engine instance
    pub fn new(
        config: Arc<Config>,
        chain_infras: HashMap<String, SharedChainInfra>,
        metrics: Arc<crate::types::ArbitrageMetrics>,
        event_broadcaster: broadcast::Sender<crate::types::ChainEvent>,
    ) -> Result<Self, BotError> {
        debug!("Creating a new AlphaEngine instance with provided configuration.");
        let alpha_config = Self::extract_alpha_config(&config)?;
        
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            alpha_config.circuit_breaker_threshold,
            Duration::from_secs(alpha_config.circuit_breaker_timeout_seconds),
        ));

        let performance_metrics = Arc::new(RwLock::new(AlphaPerformanceMetrics::new()));
        let statistical_pair_stats = Arc::new(RwLock::new(HashMap::new()));
        let execution_semaphore = Arc::new(Semaphore::new(alpha_config.max_concurrent_ops));
        let active_scanners = Arc::new(AtomicUsize::new(0));
        let current_market_regime = Arc::new(RwLock::new(MarketRegime::Sideways));
        let liquidation_candidates = Arc::new(RwLock::new(Vec::new()));
        let yield_strategies = Arc::new(RwLock::new(Vec::new()));
        let options_positions = Arc::new(RwLock::new(Vec::new()));
        let market_regime_history = Arc::new(RwLock::new(std::collections::VecDeque::new()));
        let last_scan_timestamps = Arc::new(RwLock::new(HashMap::new()));
        let opportunity_cache = Arc::new(RwLock::new(HashMap::new()));
        let discovery_scanners = Arc::new(RwLock::new(HashMap::new()));

        // Initialize new services (will be properly initialized in start() method)
        let unified_discovery = Arc::new(RwLock::new(None));
        let dynamic_scanner_manager = Arc::new(RwLock::new(None));

        debug!("AlphaEngine instance created successfully.");
        Ok(Self {
            alpha_config,
            chain_infras: Arc::new(RwLock::new(chain_infras)),
            event_broadcaster: event_broadcaster.clone(),
            metrics: metrics.clone(),
            running: Arc::new(RwLock::new(false)),
            circuit_breaker,
            performance_metrics,
            statistical_pair_stats,
            execution_semaphore,
            active_scanners,
            current_market_regime,
            cross_chain_pathfinder: None,
            liquidation_candidates,
            yield_strategies,
            options_positions,
            market_regime_history,
            last_scan_timestamps,
            opportunity_cache,
            discovery_scanners,
            unified_discovery,
            dynamic_scanner_manager,
        })
    }

    /// Extract alpha engine configuration from the main config
    fn extract_alpha_config(config: &Config) -> Result<AlphaEngineConfig, BotError> {
        debug!("Extracting alpha engine configuration from the main config.");
        let alpha_settings = config.module_config.as_ref()
            .and_then(|m| m.alpha_engine_settings.as_ref())
            .ok_or_else(|| BotError::Config("Missing alpha engine settings".to_string()))?;
            
        debug!("Alpha engine configuration extracted successfully.");
        Ok(AlphaEngineConfig {
            min_profit_threshold_usd: alpha_settings.min_profit_threshold_usd,
            max_concurrent_ops: alpha_settings.max_concurrent_ops,
            circuit_breaker_threshold: alpha_settings.circuit_breaker_threshold,
            circuit_breaker_timeout_seconds: alpha_settings.circuit_breaker_timeout_seconds,
            min_scan_amount_wei: alpha_settings.min_scan_amount_wei,
            min_pool_liquidity_usd: alpha_settings.min_pool_liquidity_usd,
            statistical_arb_z_score_threshold: alpha_settings.statistical_arb_z_score_threshold,
            market_regime_volatility_threshold: 0.25, // Default
            max_position_size_usd: 100000.0, // Default
            risk_free_rate: 0.02, // Default 2%
            enable_cross_chain: true,
            enable_options_arbitrage: false,
            enable_yield_farming: true,
            enable_liquidations: true,
        })
    }

    /// Start the Alpha Engine and all discovery scanners
    pub async fn start(&self) -> Result<(), BotError> {
        debug!("Attempting to start the AlphaEngine.");
        let mut running = self.running.write().await;
        if *running {
            debug!("AlphaEngine is already running.");
            return Ok(());
        }
        *running = true;
        drop(running);

        info!("Starting AlphaEngine with event-driven discovery");

        // Try to initialize event-driven discovery services first
        match self.initialize_event_driven_services().await {
            Ok(()) => {
                info!("Successfully initialized event-driven discovery - O(N²) problem solved!");
                // Start observability monitoring
                self.start_observability_monitor().await;
            }
            Err(e) => {
                warn!("Event-driven discovery initialization failed: {}, falling back to traditional scanners", e);
                // Fallback to traditional scanners
                self.initialize_discovery_scanners().await?;
                self.start_discovery_scanners().await?;
            }
        }

        info!("AlphaEngine discovery services started successfully");
        Ok(())
    }

    /// Stop the Alpha Engine and all discovery scanners
    pub async fn stop(&self) {
        debug!("Stopping the AlphaEngine.");
        let mut running = self.running.write().await;
        *running = false;
        drop(running);

        info!("Stopping AlphaEngine discovery services");

        // Stop new event-driven services
        if let Some(ref unified_discovery) = *self.unified_discovery.read().await {
            if let Err(e) = unified_discovery.stop().await {
                error!("Failed to stop unified discovery service: {}", e);
            }
        }

        if let Some(ref dynamic_manager) = *self.dynamic_scanner_manager.read().await {
            if let Err(e) = dynamic_manager.stop().await {
                error!("Failed to stop dynamic scanner manager: {}", e);
            }
        }

        // Stop traditional scanners as fallback
        let scanners = self.discovery_scanners.read().await;
        for (name, scanner) in scanners.iter() {
            if let Err(e) = scanner.shutdown().await {
                error!("Failed to stop scanner {}: {}", name, e);
            } else {
                debug!("Stopped scanner: {}", name);
                // Decrement active scanner count when successfully stopped
                self.active_scanners.fetch_sub(1, Ordering::Relaxed);
            }
        }

        // Reset active scanner count to ensure accuracy
        self.active_scanners.store(0, Ordering::Relaxed);

        info!("AlphaEngine stopped");
    }

    /// Initialize all discovery scanners based on configuration
    async fn initialize_discovery_scanners(&self) -> Result<(), BotError> {
        debug!("Initializing traditional discovery scanners (fallback mode).");
        let chain_infras = self.chain_infras.read().await;
        let mut scanners = self.discovery_scanners.write().await;

        // Check if event-driven discovery is already active
        let has_event_driven = scanners.keys().any(|k| k.contains("event_driven"));
        
        for (chain_name, chain_infra) in chain_infras.iter() {
            debug!("Setting up ScannerContext for chain: {}", chain_name);
            let context = ScannerContext::new(Arc::new(chain_infra.clone()));

            // Skip cyclic scanner if event-driven is active (it handles cyclic discovery)
            if !has_event_driven {
                // Create cyclic arbitrage scanner
                debug!("Creating cyclic arbitrage scanner for chain: {}", chain_name);
                let cyclic_scanner = Arc::new(CyclicArbitrageScanner::new(
                    context.clone(),
                    self.alpha_config.min_scan_amount_wei,
                    self.alpha_config.min_profit_threshold_usd,
                ));
                scanners.insert(format!("{}_cyclic", chain_name), cyclic_scanner as Arc<dyn DiscoveryScanner + Send + Sync>);
            }

            // Create statistical arbitrage scanner (still useful alongside event-driven)
            debug!("Creating statistical arbitrage scanner for chain: {}", chain_name);
            let statistical_scanner = Arc::new(StatisticalArbitrageScanner::new(
                context.clone(),
                self.alpha_config.min_scan_amount_wei,
                self.alpha_config.min_profit_threshold_usd,
                self.alpha_config.statistical_arb_z_score_threshold,
                self.statistical_pair_stats.clone(),
            ));

            // Create market making scanner (still useful alongside event-driven)
            debug!("Creating market making scanner for chain: {}", chain_name);
            let market_making_scanner = Arc::new(MarketMakingScanner::new(
                context.clone(),
                self.alpha_config.min_scan_amount_wei,
                self.alpha_config.min_profit_threshold_usd,
            ));

            // Add scanners to the collection
            scanners.insert(format!("{}_statistical", chain_name), statistical_scanner as Arc<dyn DiscoveryScanner + Send + Sync>);
            scanners.insert(format!("{}_market_making", chain_name), market_making_scanner as Arc<dyn DiscoveryScanner + Send + Sync>);

            // Add cross-chain scanner if enabled and pathfinder is available
            if self.alpha_config.enable_cross_chain && self.cross_chain_pathfinder.is_some() {
                if let Some(ref cross_chain_pathfinder) = self.cross_chain_pathfinder {
                    debug!("Creating cross-chain arbitrage scanner for chain: {}", chain_name);
                    let cross_chain_scanner = Arc::new(CrossChainArbitrageScanner::new(
                        context.clone(),
                        self.alpha_config.min_scan_amount_wei,
                        self.alpha_config.min_profit_threshold_usd,
                        cross_chain_pathfinder.clone(),
                        self.opportunity_cache.clone(),
                    ));
                    scanners.insert(format!("{}_cross_chain", chain_name), cross_chain_scanner as Arc<dyn DiscoveryScanner + Send + Sync>);
                }
            }

            info!("Initialized discovery scanners for chain: {}", chain_name);
        }

        debug!("All discovery scanners initialized successfully.");
        Ok(())
    }

    /// Start all initialized discovery scanners
    async fn start_discovery_scanners(&self) -> Result<(), BotError> {
        debug!("Starting all initialized discovery scanners.");
        let scanners = self.discovery_scanners.read().await;
        
        for (name, scanner) in scanners.iter() {
            debug!("Initializing scanner: {}", name);
            let config = self.create_discovery_config().await;
            if let Err(e) = scanner.initialize(config).await {
                error!("Failed to start scanner {}: {}", name, e);
                self.circuit_breaker.record_failure().await;
            } else {
                info!("Started discovery scanner: {}", name);
                self.active_scanners.fetch_add(1, Ordering::Relaxed);
            }
        }
        debug!("All discovery scanners started successfully.");
        Ok(())
    }

    /// Initialize event-driven discovery services
    async fn initialize_event_driven_services(&self) -> Result<(), BotError> {
        debug!("Initializing event-driven discovery services");

        let chain_infras = self.chain_infras.read().await;
        let mut event_driven_services_initialized = false;

        // Initialize event-driven discovery for each chain
        for (chain_name, chain_infra) in chain_infras.iter() {
            debug!("Setting up event-driven discovery for chain: {}", chain_name);
            let context = ScannerContext::new(Arc::new(chain_infra.clone()));

            // Create event-driven discovery service
            let event_discovery = Arc::new(EventDrivenDiscoveryService::new(
                context.clone(),
                self.event_broadcaster.clone(),
            ));

            // Start the event-driven discovery service
            if let Err(e) = event_discovery.start().await {
                warn!("Failed to start event-driven discovery for chain {}: {}", chain_name, e);
                continue;
            }

            // Store in discovery_scanners as a special scanner
            {
                let mut scanners = self.discovery_scanners.write().await;
                scanners.insert(
                    format!("{}_event_driven", chain_name),
                    event_discovery as Arc<dyn DiscoveryScanner + Send + Sync>
                );
            }

            info!("Event-driven discovery service started for chain: {}", chain_name);
            event_driven_services_initialized = true;
        }

        if event_driven_services_initialized {
            info!("Event-driven discovery services initialized successfully");
            Ok(())
        } else {
            Err(BotError::Infrastructure("Failed to initialize any event-driven discovery services".to_string()))
        }
    }

    /// Create discovery configuration based on current market regime
    async fn create_discovery_config(&self) -> DiscoveryConfig {
        debug!("Creating discovery configuration based on current market regime.");
        let current_regime = *self.current_market_regime.read().await;
        
        let config = match current_regime {
            MarketRegime::HighVolatility => DiscoveryConfig {
                min_profit_usd: self.alpha_config.min_profit_threshold_usd * 1.5,
                max_gas_cost_usd: 100.0,
                min_liquidity_usd: self.alpha_config.min_pool_liquidity_usd * 2.0,
                max_price_impact_bps: 300, // 3% - tighter in volatile markets
                scan_interval_ms: 500, // Faster scanning
                batch_size: 50, // Smaller batches for responsiveness
                ..Default::default()
            },
            MarketRegime::LowVolatility => DiscoveryConfig {
                min_profit_usd: self.alpha_config.min_profit_threshold_usd * 0.8,
                max_gas_cost_usd: 30.0,
                min_liquidity_usd: self.alpha_config.min_pool_liquidity_usd * 0.5,
                max_price_impact_bps: 800, // 8% - more lenient in stable markets
                scan_interval_ms: 2000, // Slower scanning
                batch_size: 200, // Larger batches for efficiency
                ..Default::default()
            },
            MarketRegime::Crisis => DiscoveryConfig {
                min_profit_usd: self.alpha_config.min_profit_threshold_usd * 3.0,
                max_gas_cost_usd: 200.0,
                min_liquidity_usd: self.alpha_config.min_pool_liquidity_usd * 5.0,
                max_price_impact_bps: 100, // 1% - very conservative
                scan_interval_ms: 200, // Very fast scanning
                batch_size: 20, // Small batches for safety
                ..Default::default()
            },
            _ => DiscoveryConfig::default(),
        };

        debug!("Discovery configuration created: {:?}", config);
        config
    }

    /// Run a discovery cycle for the given block number
    /// This method now works with both event-driven and traditional approaches
    pub async fn run_discovery_cycle(&self, block_number: u64) -> Result<(), BotError> {
        debug!("Running discovery cycle for block number: {}", block_number);
        let running = self.running.read().await;
        if !*running {
            debug!("AlphaEngine is not running. Exiting discovery cycle.");
            return Ok(());
        }
        drop(running);

        // Check circuit breaker
        if self.circuit_breaker.is_tripped().await {
            let stats = self.circuit_breaker.get_stats().await;
            debug!(
                "Circuit breaker is tripped (backoff: {}x, recovery in ~{}s), skipping block {}",
                stats.current_backoff_multiplier,
                stats.effective_timeout_seconds(),
                block_number
            );
            return Ok(());
        }

        // Track scan timing
        let scan_start = std::time::Instant::now();

        // Check if we've scanned too recently
        {
            let last_scan = self.last_scan_timestamps.read().await;
            if let Some(last_time) = last_scan.get("global") {
                if last_time.elapsed() < Duration::from_millis(100) {
                    debug!("Throttling discovery scan - too recent");
                    return Ok(());
                }
            }
        }

        let mut all_opportunities = Vec::new();
        let mut successful_scans = 0;
        let mut failed_scans = 0;
        let mut total_opportunities = 0;

        // Try to get opportunities from event-driven services first
        let unified_discovery_lock = self.unified_discovery.read().await;
        if let Some(ref unified_discovery) = *unified_discovery_lock {
            debug!("Using event-driven discovery for block {}", block_number);
            let discovered_paths = unified_discovery.get_discovered_paths().await;

            for (path_key, discovered_path) in discovered_paths {
                if discovered_path.timestamp.elapsed() < std::time::Duration::from_secs(300) {
                    // Convert discovered path to MEV opportunity
                    match unified_discovery.path_to_mev_opportunity(&discovered_path).await {
                        Ok(opportunity) => {
                            all_opportunities.push(opportunity);
                            total_opportunities += 1;
                            successful_scans += 1;
                            self.metrics.opportunities_found
                                .with_label_values(&["ethereum", "event_driven"])
                                .inc();
                        }
                        Err(e) => {
                            warn!("Failed to convert discovered path {} to opportunity: {}", path_key, e);
                            failed_scans += 1;
                        }
                    }
                }
            }

            if !all_opportunities.is_empty() {
                info!("Event-driven discovery found {} opportunities for block {}", total_opportunities, block_number);
            }
        } else {
            // Fallback to traditional scanner approach
            debug!("Using traditional scanners for block {}", block_number);
            let scanners = self.discovery_scanners.read().await;
            let mut scan_tasks = Vec::new();
            let scan_timeout = Duration::from_secs(30);

            for (name, scanner) in scanners.iter() {
                if scanner.is_running().await {
                    debug!("Scanner {} is running. Preparing to scan block {}", name, block_number);
                    let scanner_clone = scanner.clone();
                    let name_clone = name.clone();
                    let opportunity_cache = self.opportunity_cache.clone();
                    let metrics = self.metrics.clone();

                    let task = tokio::spawn(async move {
                        // Add timeout to scanner operations
                        match tokio::time::timeout(
                            scan_timeout,
                            scanner_clone.scan(Some(block_number))
                        ).await {
                            Ok(Ok(opportunities)) => {
                                debug!("Scanner {} found {} opportunities for block {}",
                                    name_clone, opportunities.len(), block_number);

                                // Record metrics for found opportunities
                                if !opportunities.is_empty() {
                                    metrics.opportunities_found
                                        .with_label_values(&["ethereum", &name_clone])
                                        .inc_by(opportunities.len() as u64);
                                }

                                // Cache valid opportunities
                                let mut cached_count = 0;
                                let mut cache = opportunity_cache.write().await;
                                for opp in &opportunities {
                                    if let MEVOpportunity::Arbitrage(arb) = opp {
                                        let cache_key = format!(
                                            "{}_{}_{}_{}",
                                            block_number,
                                            arb.chain_id,
                                            arb.token_in,
                                            arb.token_out
                                        );
                                        cache.insert(cache_key, ArbitrageOpportunity {
                                            route: arb.clone(),
                                            block_number,
                                            block_timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            profit_usd: arb.profit_usd,
                                            profit_token_amount: arb.amount_out_wei.saturating_sub(arb.amount_in),
                                            profit_token: arb.token_out,
                                            estimated_gas_cost_wei: arb.estimated_gas_cost_wei,
                                            risk_score: arb.risk_score,
                                            mev_risk_score: arb.mev_risk_score,
                                            simulation_result: None,
                                            metadata: None,
                                            token_in: arb.token_in,
                                            token_out: arb.token_out,
                                            amount_in: arb.amount_in,
                                            chain_id: arb.chain_id,
                                        });
                                        cached_count += 1;
                                    }
                                }
                                if cached_count > 0 {
                                    debug!("Cached {} arbitrage opportunities from {}", cached_count, name_clone);
                                }

                                (name_clone, Ok(opportunities))
                            }
                            Ok(Err(e)) => {
                                warn!("Scanner {} failed for block {}: {}",
                                    name_clone, block_number, e);
                                (name_clone, Err(e))
                            }
                            Err(_) => {
                                warn!("Scanner {} timed out for block {} after {:?}",
                                    &name_clone, block_number, scan_timeout);
                                let scanner_name = name_clone.clone();
                                (name_clone, Err(ArbitrageError::Other(format!(
                                    "Scanner {} timed out after {:?}", scanner_name, scan_timeout
                                ))))
                            }
                        }
                    });
                    scan_tasks.push(task);
                }
            }

            // Wait for all scan tasks to complete
            let results = futures::future::join_all(scan_tasks).await;

            for task_result in results {
                match task_result {
                    Ok((scanner_name, Ok(opportunities))) => {
                        let count = opportunities.len();
                        debug!("Scanner {} completed with {} opportunities", scanner_name, count);
                        total_opportunities += count;
                        successful_scans += 1;
                        all_opportunities.extend(opportunities);
                        self.update_scanner_performance(&scanner_name, true, count).await;
                        self.circuit_breaker.record_success().await;
                    }
                    Ok((scanner_name, Err(e))) => {
                        warn!("Scanner {} failed: {:?}. This will increment circuit breaker failure count.", scanner_name, e);
                        failed_scans += 1;
                        self.update_scanner_performance(&scanner_name, false, 0).await;

                        // Get circuit breaker stats before recording failure
                        let stats_before = self.circuit_breaker.get_stats().await;
                        warn!(
                            "Circuit breaker before failure: consecutive_failures={}, is_tripped={}",
                            stats_before.consecutive_failures, stats_before.is_tripped
                        );

                        self.circuit_breaker.record_failure().await;

                        let stats_after = self.circuit_breaker.get_stats().await;
                        warn!(
                            "Circuit breaker after failure: consecutive_failures={}, is_tripped={}. Recovery in ~{}s",
                            stats_after.consecutive_failures, stats_after.is_tripped, stats_after.effective_timeout_seconds()
                        );
                    }
                    Err(e) => {
                        error!("Scanner task panicked: {:?}. This will increment circuit breaker failure count.", e);
                        failed_scans += 1;

                        // Get circuit breaker stats before recording failure
                        let stats_before = self.circuit_breaker.get_stats().await;
                        error!(
                            "Circuit breaker before panic failure: consecutive_failures={}, is_tripped={}",
                            stats_before.consecutive_failures, stats_before.is_tripped
                        );

                        self.circuit_breaker.record_failure().await;

                        let stats_after = self.circuit_breaker.get_stats().await;
                        error!(
                            "Circuit breaker after panic failure: consecutive_failures={}, is_tripped={}. Recovery in ~{}s",
                            stats_after.consecutive_failures, stats_after.is_tripped, stats_after.effective_timeout_seconds()
                        );
                    }
                }
            }
        }

        // Update scan timestamp
        {
            let mut timestamps = self.last_scan_timestamps.write().await;
            timestamps.insert("global".to_string(), std::time::Instant::now());
        }

        // Process and deduplicate opportunities
        let deduplicated = self.deduplicate_opportunities(all_opportunities).await;
        let dedup_count = deduplicated.len();

        // Store deduplicated opportunities in cache for retrieval
        if dedup_count > 0 {
            let mut cache = self.opportunity_cache.write().await;
            for opp in deduplicated {
                if let MEVOpportunity::Arbitrage(arb) = opp {
                    // Include token addresses and amount to avoid cache key collisions
                    let cache_key = format!("dedup_{}_{}_{}_{}_{}",
                        block_number, arb.chain_id, arb.token_in, arb.token_out, arb.amount_in);
                    cache.insert(cache_key, ArbitrageOpportunity {
                        route: arb.clone(),
                        block_number,
                        block_timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        profit_usd: arb.profit_usd,
                        profit_token_amount: arb.amount_out_wei.saturating_sub(arb.amount_in),
                        profit_token: arb.token_out,
                        estimated_gas_cost_wei: arb.estimated_gas_cost_wei,
                        risk_score: arb.risk_score,
                        mev_risk_score: arb.mev_risk_score,
                        simulation_result: None,
                        metadata: None,
                        token_in: arb.token_in,
                        token_out: arb.token_out,
                        amount_in: arb.amount_in,
                        chain_id: arb.chain_id,
                    });
                }
            }
        }

        // Update overall performance metrics with actual opportunities found
        {
            let mut metrics = self.performance_metrics.write().await;
            metrics.total_opportunities_found = metrics.total_opportunities_found.saturating_add(total_opportunities as u64);
            metrics.active_scanners = self.active_scanners.load(Ordering::Relaxed);
        }

        let scan_duration = scan_start.elapsed();
        info!(
            "Discovery cycle for block {} completed in {:.2}s: {} opportunities ({} after dedup) from {} scanners ({} successful, {} failed)",
            block_number, scan_duration.as_secs_f64(), total_opportunities, dedup_count,
            successful_scans + failed_scans, successful_scans, failed_scans
        );

        // Clean up expired cache entries
        if block_number % 100 == 0 {
            self.cleanup_expired_data().await;
        }

        Ok(())
    }

    /// Deduplicate opportunities based on their characteristics
    async fn deduplicate_opportunities(&self, opportunities: Vec<MEVOpportunity>) -> Vec<MEVOpportunity> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut deduplicated = Vec::new();
        let original_count = opportunities.len();
        
        for opp in opportunities {
            let key = match &opp {
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
            };
            
            if seen.insert(key) {
                deduplicated.push(opp);
            }
        }
        
        if deduplicated.len() < original_count {
            debug!("Deduplicated {} opportunities to {}", 
                original_count, deduplicated.len());
        }
        
        deduplicated
    }
    
    /// Get arbitrage opportunities from discovery scanners for a specific block
    pub async fn get_arbitrage_opportunities(
        &self,
        block_number: u64,
    ) -> Result<Vec<crate::types::ArbitrageOpportunity>, crate::errors::BotError> {
        debug!("Getting arbitrage opportunities from AlphaEngine for block {}", block_number);
        
        // First check cache
        let mut cached_opportunities = Vec::new();
        {
            let cache = self.opportunity_cache.read().await;
            for (key, opp) in cache.iter() {
                if key.starts_with(&format!("{}_", block_number)) {
                    cached_opportunities.push(opp.clone());
                }
            }
        }
        
        if !cached_opportunities.is_empty() {
            debug!("Returning {} cached opportunities for block {}", 
                cached_opportunities.len(), block_number);
            return Ok(cached_opportunities);
        }
        
        let scanners = self.discovery_scanners.read().await;
        let mut all_opportunities = Vec::new();
        
        for (name, scanner) in scanners.iter() {
            if scanner.is_running().await {
                debug!("Scanner {} is running. Getting opportunities for block {}", name, block_number);
                
                match scanner.scan(Some(block_number)).await {
                    Ok(mev_opportunities) => {
                        debug!("Scanner {} found {} MEV opportunities", name, mev_opportunities.len());
                        
                        // Convert MEVOpportunity to ArbitrageOpportunity
                        for mev_opp in mev_opportunities {
                            if let crate::mempool::MEVOpportunity::Arbitrage(route) = mev_opp {
                                let opportunity = crate::types::ArbitrageOpportunity {
                                    route: route.clone(),
                                    block_number,
                                    block_timestamp: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    profit_usd: route.profit_usd,
                                    profit_token_amount: route.amount_out_wei.saturating_sub(route.initial_amount_in_wei),
                                    profit_token: route.token_in,
                                    estimated_gas_cost_wei: route.estimated_gas_cost_wei,
                                    risk_score: route.risk_score,
                                    mev_risk_score: route.mev_risk_score,
                                    simulation_result: None,
                                    metadata: None,
                                    token_in: route.token_in,
                                    token_out: route.token_out,
                                    amount_in: route.amount_in,
                                    chain_id: route.chain_id,
                                };
                                all_opportunities.push(opportunity);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Scanner {} failed for block {}: {}", name, block_number, e);
                        continue;
                    }
                }
            }
        }
        
        debug!("AlphaEngine found {} arbitrage opportunities for block {}", all_opportunities.len(), block_number);
        Ok(all_opportunities)
    }

    /// Handle new block events
    pub async fn on_new_block(&self, block: Block<H256>) {
        debug!("Handling new block event.");
        if let Some(block_number) = block.number {
            let block_num = block_number.as_u64();
            debug!("New block number: {}", block_num);
            
            // Broadcast block event to all listeners
            let chain_event = crate::types::ChainEvent::NewBlock {
                block_number: block_num,
                timestamp: block.timestamp.as_u64(),
            };
            if let Err(e) = self.event_broadcaster.send(chain_event) {
                warn!("Failed to broadcast new block event: {}", e);
            }
            
            // Run discovery cycle
            if let Err(e) = self.run_discovery_cycle(block_num).await {
                error!("Failed to run discovery cycle for block {}: {}. This will increment circuit breaker failure count.", block_num, e);

                // Get circuit breaker stats before recording failure
                let stats_before = self.circuit_breaker.get_stats().await;
                error!(
                    "Circuit breaker before discovery cycle failure: consecutive_failures={}, is_tripped={}",
                    stats_before.consecutive_failures, stats_before.is_tripped
                );

                self.circuit_breaker.record_failure().await;

                let stats_after = self.circuit_breaker.get_stats().await;
                error!(
                    "Circuit breaker after discovery cycle failure: consecutive_failures={}, is_tripped={}. Recovery in ~{}s",
                    stats_after.consecutive_failures, stats_after.is_tripped, stats_after.effective_timeout_seconds()
                );
            }

            // Update statistical data and market regime
            self.update_market_data(block_num).await;
            
            // Cleanup expired data
            self.cleanup_expired_data().await;
        }
    }

    /// Update market data and regime analysis
    async fn update_market_data(&self, block_number: u64) {
        debug!("Updating market data and regime analysis for block number: {}", block_number);
        // Analyze market regime
        let new_regime = self.analyze_market_regime().await;
        let mut current_regime = self.current_market_regime.write().await;
        
        if *current_regime != new_regime {
            info!("Market regime changed from {:?} to {:?} at block {}", 
                  *current_regime, new_regime, block_number);
            
            // Record regime change in history
            let mut history = self.market_regime_history.write().await;
            history.push_back((new_regime, Instant::now()));
            if history.len() > 1000 {
                history.pop_front();
            }
            
            *current_regime = new_regime;
        }
        
        // Note: Removed incorrect increment of total_opportunities_found
        // This should only be incremented when actual opportunities are found in run_discovery_cycle
    }

    /// Analyze current market regime based on statistical data
    async fn analyze_market_regime(&self) -> MarketRegime {
        debug!("Analyzing current market regime based on statistical data.");
        let statistical_pair_stats = self.statistical_pair_stats.read().await;
        
        if statistical_pair_stats.is_empty() {
            debug!("No statistical data available. Defaulting to Sideways market regime.");
            return MarketRegime::Sideways;
        }
        
        let mut total_volatility = 0.0;
        let mut total_momentum = 0.0;
        let mut pair_count = 0;
        
        for stats in statistical_pair_stats.values() {
            if stats.has_sufficient_data() {
                total_volatility += stats.volatility;
                total_momentum += stats.price_momentum;
                pair_count += 1;
            }
        }
        
        if pair_count == 0 {
            debug!("No pairs with sufficient data. Defaulting to Sideways market regime.");
            return MarketRegime::Sideways;
        }
        
        let avg_volatility = total_volatility / pair_count as f64;
        let avg_momentum = total_momentum / pair_count as f64;
        
        // Classify regime based on volatility and momentum
        let regime = match (avg_volatility, avg_momentum.abs()) {
            (v, _) if v > 0.8 => MarketRegime::Crisis,
            (v, _) if v > 0.5 => MarketRegime::HighVolatility,
            (v, _) if v < 0.1 => MarketRegime::LowVolatility,
            (_, m) if m > 0.3 => MarketRegime::Trending,
            (v, m) if v > 0.25 && m > 0.1 => MarketRegime::Bull,
            (v, m) if v > 0.25 && m < -0.1 => MarketRegime::Bear,
            _ => MarketRegime::Sideways,
        };

        debug!("Market regime analysis complete. Determined regime: {:?}", regime);
        regime
    }

    /// Update performance metrics for a specific scanner
    async fn update_scanner_performance(&self, scanner_name: &str, success: bool, opportunity_count: usize) {
        debug!("Updating performance metrics for scanner: {}", scanner_name);
        let mut metrics = self.performance_metrics.write().await;
        
        let scanner_metrics = metrics.scanner_performance
            .entry(scanner_name.to_string())
            .or_insert_with(ScannerPerformanceMetrics::new);
        
        if success {
            scanner_metrics.opportunities_found = scanner_metrics.opportunities_found.saturating_add(opportunity_count as u64);
            debug!("Scanner {} found {} opportunities. Total opportunities found: {}", scanner_name, opportunity_count, scanner_metrics.opportunities_found);
        } else {
            scanner_metrics.error_count = scanner_metrics.error_count.saturating_add(1);
            debug!("Scanner {} encountered an error. Total errors: {}", scanner_name, scanner_metrics.error_count);
        }
    }

    /// Update statistical pair statistics
    pub async fn update_statistical_pair_stats(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        price: f64,
        volume_usd: f64,
        block_number: u64,
    ) {
        debug!("Updating statistical pair stats for tokens: {:?} and {:?} at block {}", token_a, token_b, block_number);
        let mut stats_map = self.statistical_pair_stats.write().await;
        let key = (token_a, token_b);
        let stats = stats_map.entry(key).or_insert_with(TokenPairStats::new);
        stats.add_price_observation(price, block_number);
        stats.add_volume_observation(volume_usd, block_number);
        debug!("Updated price and volume observations for token pair: {:?}", key);

        // Record per-chain update timestamps to enable chain-aware throttling and monitoring
        {
            let mut timestamps = self.last_scan_timestamps.write().await;
            timestamps.insert(chain_name.to_string(), Instant::now());
        }
    }

    /// Cleanup expired data to prevent memory leaks
    async fn cleanup_expired_data(&self) {
        debug!("Cleaning up expired data to prevent memory leaks.");
        let now = Instant::now();
        let expiry_duration = Duration::from_secs(3600); // 1 hour

        // Clean opportunity cache
        {
            let mut cache = self.opportunity_cache.write().await;
            let system_now = std::time::SystemTime::now();
            cache.retain(|_, opportunity| {
                let opportunity_time = std::time::UNIX_EPOCH + Duration::from_secs(opportunity.block_timestamp);
                if let Ok(opportunity_elapsed) = system_now.duration_since(opportunity_time) {
                    opportunity_elapsed < expiry_duration
                } else {
                    false
                }
            });
            debug!("Cleaned opportunity cache. Remaining entries: {}", cache.len());
        }

        // Clean scan timestamps
        {
            let mut timestamps = self.last_scan_timestamps.write().await;
            timestamps.retain(|_, timestamp| {
                now.duration_since(*timestamp) < expiry_duration
            });
            debug!("Cleaned scan timestamps. Remaining entries: {}", timestamps.len());
        }

        // Clean old statistical data
        {
            let mut stats_map = self.statistical_pair_stats.write().await;
            stats_map.retain(|_, stats| {
                now.duration_since(stats.last_update) < Duration::from_secs(86400) // 24 hours
            });
            debug!("Cleaned old statistical data. Remaining entries: {}", stats_map.len());
        }
    }

    /// Get current performance metrics
    pub async fn get_performance_metrics(&self) -> AlphaPerformanceMetrics {
        debug!("Fetching current performance metrics.");
        self.performance_metrics.read().await.clone()
    }

    /// Get circuit breaker status
    pub async fn get_circuit_breaker_status(&self) -> (bool, u64) {
        debug!("Fetching circuit breaker status.");
        let stats = self.circuit_breaker.get_stats().await;
        debug!("Circuit breaker status: is_tripped = {}, total_trips = {}", stats.is_tripped, stats.total_trips);
        (stats.is_tripped, stats.total_trips)
    }

    /// Get current market regime
    pub async fn get_current_market_regime(&self) -> MarketRegime {
        debug!("Fetching current market regime.");
        *self.current_market_regime.read().await
    }

    /// Get active scanner count
    pub fn get_active_scanner_count(&self) -> usize {
        debug!("Fetching active scanner count.");
        self.active_scanners.load(Ordering::Relaxed)
    }

    /// Force reset the circuit breaker (administrative override)
    pub async fn force_reset_circuit_breaker(&self) {
        debug!("Forcing reset of the circuit breaker.");
        self.circuit_breaker.force_reset().await;
        warn!("Alpha Engine circuit breaker manually reset");
    }

    /// Get detailed scanner statistics
    pub async fn get_scanner_statistics(&self) -> HashMap<String, ScannerPerformanceMetrics> {
        debug!("Fetching detailed scanner statistics.");
        let metrics = self.performance_metrics.read().await;
        metrics.scanner_performance.clone()
    }

    /// Public accessor to fetch a current block number from any configured chain infra
    pub async fn get_head_block_number(&self) -> Result<u64, BotError> {
        let chain_infras = self.chain_infras.read().await;
        if let Some((_, infra)) = chain_infras.iter().next() {
            return infra
                .blockchain_manager
                .get_block_number()
                .await
                .map_err(BotError::from);
        }
        Err(BotError::Blockchain(crate::errors::BlockchainError::NotAvailable(
            "No chain infrastructure available".to_string(),
        )))
    }

    /// Update liquidation candidates (called by external liquidation monitor)
    pub async fn update_liquidation_candidates(&self, candidates: Vec<LiquidationCandidate>) {
        debug!("Updating liquidation candidates.");
        let mut liquidation_candidates = self.liquidation_candidates.write().await;
        *liquidation_candidates = candidates;
        debug!("Updated {} liquidation candidates", liquidation_candidates.len());
    }

    /// Update yield strategies (called by external yield monitor)
    pub async fn update_yield_strategies(&self, strategies: Vec<YieldStrategy>) {
        debug!("Updating yield strategies.");
        let mut yield_strategies = self.yield_strategies.write().await;
        *yield_strategies = strategies;
        debug!("Updated {} yield strategies", yield_strategies.len());
    }

    /// Update options positions (called by external options monitor)
    pub async fn update_options_positions(&self, positions: Vec<OptionsPosition>) {
        debug!("Updating options positions.");
        let mut options_positions = self.options_positions.write().await;
        *options_positions = positions;
        debug!("Updated {} options positions", options_positions.len());
    }

    /// Get current liquidation candidates
    pub async fn get_liquidation_candidates(&self) -> Vec<LiquidationCandidate> {
        debug!("Fetching current liquidation candidates.");
        self.liquidation_candidates.read().await.clone()
    }

    /// Get current yield strategies
    pub async fn get_yield_strategies(&self) -> Vec<YieldStrategy> {
        debug!("Fetching current yield strategies.");
        self.yield_strategies.read().await.clone()
    }

    /// Get current options positions
    pub async fn get_options_positions(&self) -> Vec<OptionsPosition> {
        debug!("Fetching current options positions.");
        self.options_positions.read().await.clone()
    }

    /// Check if the alpha engine is healthy and operational
    pub async fn is_healthy(&self) -> bool {
        debug!("Checking if the AlphaEngine is healthy and operational.");
        let running = *self.running.read().await;
        let circuit_breaker_healthy = self.circuit_breaker.is_healthy().await;
        let active_scanners = self.get_active_scanner_count() > 0;
        
        let is_healthy = running && circuit_breaker_healthy && active_scanners;
        debug!("AlphaEngine health check result: {}", is_healthy);
        is_healthy
    }

    /// Start comprehensive observability monitoring
    async fn start_observability_monitor(&self) {
        let metrics = self.metrics.clone();
        let discovery_scanners = self.discovery_scanners.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            let mut last_opportunities_found = 0u64;
            let mut last_opportunities_executed = 0u64;
            let mut last_trades_confirmed = 0u64;
            let mut last_trades_failed = 0u64;
            let mut last_report_time = std::time::Instant::now();

            loop {
                interval.tick().await;

                // Get current metric values with proper error handling
                let current_opportunities_found = metrics.opportunities_found
                    .with_label_values(&["ethereum", "event_driven"])
                    .get();

                // Aggregate opportunities from all discovery scanners
                let mut total_opportunities_found = current_opportunities_found;
                {
                    let scanners = discovery_scanners.read().await;
                    for (_scanner_name, scanner) in scanners.iter() {
                        let stats = scanner.get_stats().await;
                        total_opportunities_found = total_opportunities_found.saturating_add(stats.opportunities_found);
                    }
                }
                let current_opportunities_executed = metrics.opportunities_executed
                    .with_label_values(&["ethereum"])
                    .get();
                let current_trades_confirmed = metrics.trades_confirmed.get();
                let current_trades_failed = metrics.trades_failed
                    .with_label_values(&[""])
                    .get();

                let elapsed_minutes = last_report_time.elapsed().as_secs_f64() / 60.0;

                // Calculate rates per minute
                let opportunities_per_minute = ((total_opportunities_found.saturating_sub(last_opportunities_found)) as f64) / elapsed_minutes;
                let executions_per_minute = ((current_opportunities_executed.saturating_sub(last_opportunities_executed)) as f64) / elapsed_minutes;
                let trades_confirmed_per_minute = ((current_trades_confirmed.saturating_sub(last_trades_confirmed)) as f64) / elapsed_minutes;
                let trades_failed_per_minute = ((current_trades_failed.saturating_sub(last_trades_failed)) as f64) / elapsed_minutes;

                // Calculate success rates
                let total_trades = current_trades_confirmed + current_trades_failed;
                let success_rate = if total_trades > 0 {
                    (current_trades_confirmed as f64 / total_trades as f64) * 100.0
                } else {
                    0.0
                };

                let execution_rate = if total_opportunities_found > 0 {
                    (current_opportunities_executed as f64 / total_opportunities_found as f64) * 100.0
                } else {
                    0.0
                };

                // Get system health metrics
                let active_tasks = metrics.active_tasks
                    .with_label_values(&["alpha_engine"])
                    .get();

                info!(target: "observability",
                      opportunities_found = total_opportunities_found,
                      opportunities_executed = current_opportunities_executed,
                      opportunities_per_minute = %format!("{:.2}", opportunities_per_minute),
                      executions_per_minute = %format!("{:.2}", executions_per_minute),
                      execution_rate = %format!("{:.1}%", execution_rate),
                      trades_confirmed = current_trades_confirmed,
                      trades_failed = current_trades_failed,
                      trades_per_minute = %format!("{:.2}", trades_confirmed_per_minute),
                      failed_trades_per_minute = %format!("{:.2}", trades_failed_per_minute),
                      success_rate = %format!("{:.1}%", success_rate),
                      active_discovery_tasks = active_tasks,
                      "AlphaEngine discovery pipeline observability");

                // Update last values for next iteration
                last_opportunities_found = total_opportunities_found;
                last_opportunities_executed = current_opportunities_executed;
                last_trades_confirmed = current_trades_confirmed;
                last_trades_failed = current_trades_failed;
                last_report_time = std::time::Instant::now();
            }
        });
    }

    /// Register external discovery scanners with the AlphaEngine
    ///
    /// This method allows external components (like IntegrationCoordinator) to register
    /// scanners that were created outside of the AlphaEngine's internal initialization.
    /// These scanners will be added to the existing scanner collection and managed alongside
    /// internally created scanners.
    ///
    /// # Arguments
    /// * `scanners` - A HashMap of scanner name to scanner instance
    ///
    /// # Returns
    /// A Result indicating success or failure of the registration process
    pub async fn register_scanners(
        &self,
        scanners: HashMap<String, Arc<dyn DiscoveryScanner>>
    ) -> Result<(), BotError> {
        debug!("Registering {} external discovery scanners", scanners.len());

        let mut existing_scanners = self.discovery_scanners.write().await;

        let scanner_count = scanners.len();
        for (name, scanner) in scanners {
            if existing_scanners.contains_key(&name) {
                warn!("Scanner '{}' already exists, overwriting", name);
            }
            existing_scanners.insert(name.clone(), scanner);
            debug!("Registered external scanner: {}", name);
        }

        info!("Successfully registered {} external discovery scanners", scanner_count);
        Ok(())
    }
}