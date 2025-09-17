use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::fmt;

use dashmap::DashMap;
use tokio::sync::{broadcast, oneshot, watch, RwLock, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use ethers::types::U256;

use crate::{
    blockchain::BlockchainManager,
    config::Config,
    execution::TransactionOptimizerTrait,
    gas_oracle::GasOracleProvider,
    mempool::MEVOpportunity,
    pool_manager::PoolManagerTrait,
    price_oracle::PriceOracle,
    quote_source::AggregatorQuoteSource,
    risk::RiskManager,
    strategies::{MEVStrategy, MEVStrategyConfig, StrategyContext},
    config::MEVEngineSettings,
};

pub mod types;
mod handlers;
mod monitor;
mod contracts;

pub use types::*;

// Configuration constants
const MAX_CONCURRENT_PLAYS: usize = 100;
const MAX_PLAY_AGE_SECONDS: u64 = 300; // 5 minutes
const CLEANUP_INTERVAL_SECONDS: u64 = 60;
const MONITOR_INTERVAL_SECONDS: u64 = 5;
const MAX_RETRIES_PER_PLAY: u32 = 3;
const STRATEGY_HEALTH_CHECK_INTERVAL_SECONDS: u64 = 30;
const EMERGENCY_PAUSE_THRESHOLD: u32 = 10; // Consecutive failures before pause

#[derive(Debug, Clone)]
pub struct MEVEngineConfig {
    pub max_concurrent_executions: usize,
    pub opportunity_timeout_ms: u64,
    pub status_check_interval_secs: u64,
    pub cleanup_interval_secs: u64,
    pub max_play_age_secs: u64,
    pub max_retries: u32,
    pub enable_circuit_breaker: bool,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout_secs: u64,
    pub emergency_pause_threshold: u32,
}

impl Default for MEVEngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_executions: MAX_CONCURRENT_PLAYS,
            opportunity_timeout_ms: 30_000,
            status_check_interval_secs: MONITOR_INTERVAL_SECONDS,
            cleanup_interval_secs: CLEANUP_INTERVAL_SECONDS,
            max_play_age_secs: MAX_PLAY_AGE_SECONDS,
            max_retries: MAX_RETRIES_PER_PLAY,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_secs: 300,
            emergency_pause_threshold: EMERGENCY_PAUSE_THRESHOLD,
        }
    }
}

// Strategy registry for O(1) lookups
struct StrategyRegistry {
    strategies_by_type: HashMap<String, Arc<dyn MEVStrategy>>,
    all_strategies: Vec<Arc<dyn MEVStrategy>>,
    health_tracker: Arc<RwLock<HashMap<String, StrategyHealth>>>,
}

impl StrategyRegistry {
    fn new() -> Self {
        Self {
            strategies_by_type: HashMap::new(),
            all_strategies: Vec::new(),
            health_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&mut self, strategy: Arc<dyn MEVStrategy>) {
        let opportunity_type = strategy.name().to_string();
        self.strategies_by_type.insert(opportunity_type.clone(), strategy.clone());
        self.all_strategies.push(strategy);
        
        // Initialize health tracking synchronously to avoid race conditions
        let health = StrategyHealth::new(opportunity_type.clone());
        let mut tracker = self.health_tracker.write().await;
        tracker.insert(opportunity_type, health);
    }

    fn get_strategy_for(&self, opportunity: &MEVOpportunity) -> Option<Arc<dyn MEVStrategy>> {
        // Use O(1) lookup by opportunity type discriminant
        let opportunity_type = match opportunity {
            MEVOpportunity::Sandwich(_) => "Sandwich",
            MEVOpportunity::Liquidation(_) => "Liquidation",
            MEVOpportunity::JITLiquidity(_) => "JITLiquidity",
            MEVOpportunity::Arbitrage(_) => "Arbitrage",
            MEVOpportunity::OracleUpdate(_) => "OracleUpdate",
            MEVOpportunity::FlashLoan(_) => "FlashLoan",
        }.to_string();

        self.strategies_by_type.get(&opportunity_type).cloned()
    }

    async fn record_success(&self, strategy_name: &str) {
        let mut tracker = self.health_tracker.write().await;
        if let Some(health) = tracker.get_mut(strategy_name) {
            health.record_success();
        }
    }

    async fn record_failure(&self, strategy_name: &str) -> bool {
        let mut tracker = self.health_tracker.write().await;
        if let Some(health) = tracker.get_mut(strategy_name) {
            health.record_failure()
        } else {
            false
        }
    }

    async fn is_strategy_healthy(&self, strategy_name: &str) -> bool {
        let tracker = self.health_tracker.read().await;
        tracker.get(strategy_name)
            .map(|h| h.is_healthy())
            .unwrap_or(false)
    }

    async fn get_all_health_statuses(&self) -> HashMap<String, StrategyHealthStatus> {
        let tracker = self.health_tracker.read().await;
        tracker.iter()
            .map(|(name, health)| (name.clone(), health.get_status()))
            .collect()
    }
}

#[derive(Debug, Clone)]
struct StrategyHealth {
    name: String,
    consecutive_failures: u32,
    last_success: Option<Instant>,
    last_failure: Option<Instant>,
    total_successes: u64,
    total_failures: u64,
    is_paused: bool,
    pause_until: Option<Instant>,
}

impl StrategyHealth {
    fn new(name: String) -> Self {
        Self {
            name,
            consecutive_failures: 0,
            last_success: None,
            last_failure: None,
            total_successes: 0,
            total_failures: 0,
            is_paused: false,
            pause_until: None,
        }
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.last_success = Some(Instant::now());
        self.total_successes += 1;
        self.is_paused = false;
        self.pause_until = None;
    }

    fn record_failure(&mut self) -> bool {
        self.consecutive_failures += 1;
        self.last_failure = Some(Instant::now());
        self.total_failures += 1;
        
        // Check if we should pause
        if self.consecutive_failures >= EMERGENCY_PAUSE_THRESHOLD {
            self.is_paused = true;
            self.pause_until = Some(Instant::now() + Duration::from_secs(300));
            return true; // Strategy should be paused
        }
        false
    }

    fn is_healthy(&self) -> bool {
        if self.is_paused {
            if let Some(pause_until) = self.pause_until {
                if Instant::now() < pause_until {
                    return false;
                }
            }
        }
        true
    }

    fn get_status(&self) -> StrategyHealthStatus {
        StrategyHealthStatus {
            is_healthy: self.is_healthy(),
            consecutive_failures: self.consecutive_failures,
            total_successes: self.total_successes,
            total_failures: self.total_failures,
            success_rate: if self.total_successes + self.total_failures > 0 {
                (self.total_successes as f64) / ((self.total_successes + self.total_failures) as f64)
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyHealthStatus {
    pub is_healthy: bool,
    pub consecutive_failures: u32,
    pub total_successes: u64,
    pub total_failures: u64,
    pub success_rate: f64,
}

pub struct MEVEngine {
    config: Arc<MEVEngineConfig>,
    strategy_registry: Arc<RwLock<StrategyRegistry>>,
    strategy_context: Arc<StrategyContext>,
    opportunity_receiver: Arc<Mutex<broadcast::Receiver<MEVOpportunity>>>,
    active_plays: Arc<DashMap<Uuid, SafeActiveMEVPlay>>,
    execution_semaphore: Arc<Semaphore>,
    listener_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    monitor_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    cleanup_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    health_monitor_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    shutdown_signal: Arc<watch::Sender<bool>>,
    metrics: Arc<RwLock<MEVPlayMetrics>>,
    is_paused: Arc<RwLock<bool>>,
    task_pool: Arc<Semaphore>,
}

impl fmt::Debug for MEVEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MEVEngine")
            .field("config", &self.config)
            .field("active_plays_count", &self.active_plays.len())
            .finish()
    }
}

impl MEVEngine {
    pub async fn new(
        global_config: Arc<Config>,
        blockchain_manager: Arc<dyn BlockchainManager>,
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        risk_manager: Arc<dyn RiskManager + Send + Sync>,
        transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync>,
        quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
        opportunity_receiver: broadcast::Receiver<MEVOpportunity>,
    ) -> Result<Self, MEVEngineError> {
        Self::new_with_gas_oracle(
            global_config,
            blockchain_manager,
            pool_manager,
            price_oracle,
            risk_manager,
            transaction_optimizer,
            quote_source,
            opportunity_receiver,
            None, // Auto-create gas oracle based on execution mode
        ).await
    }

    pub async fn new_with_gas_oracle(
        global_config: Arc<Config>,
        blockchain_manager: Arc<dyn BlockchainManager>,
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        risk_manager: Arc<dyn RiskManager + Send + Sync>,
        transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync>,
        quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
        opportunity_receiver: broadcast::Receiver<MEVOpportunity>,
        gas_oracle: Option<Arc<dyn crate::gas_oracle::GasOracleProvider + Send + Sync>>,
    ) -> Result<Self, MEVEngineError> {
        let mev_settings = global_config.module_config.as_ref()
            .and_then(|m| m.mev_engine_settings.clone())
            .ok_or_else(|| MEVEngineError::ConfigError("MEV engine config missing".into()))?;

        // Use provided gas oracle or create one based on execution mode
        let gas_oracle_provider = if let Some(provider) = gas_oracle {
            provider
        } else {
            use crate::config::ExecutionMode;
            use crate::gas_oracle::GasOracleMode;

            let mode = match global_config.mode {
                ExecutionMode::Backtest => GasOracleMode::Historical,
                ExecutionMode::Live => GasOracleMode::Live,
                ExecutionMode::PaperTrading => GasOracleMode::Live, // Paper trading uses live data
            };

            match mode {
                GasOracleMode::Live => {
                    crate::gas_oracle::create_gas_oracle_provider(
                        global_config.clone(),
                        blockchain_manager.clone(),
                    ).await
                },
                GasOracleMode::Historical => {
                    // For historical mode in MEVEngine context, we need to handle the case where
                    // the historical loader is not yet available. We'll create a live provider
                    // as fallback and rely on the infrastructure to provide the right context later.
                    warn!("MEVEngine: Creating live gas oracle for historical mode - historical context will be set per operation");
                    crate::gas_oracle::create_gas_oracle_provider(
                        global_config.clone(),
                        blockchain_manager.clone(),
                    ).await
                }
            }
            .map_err(|e| MEVEngineError::ConfigError(format!("Failed to create gas oracle: {}", e)))?
        };

        // Create simulation engine
        let simulation_engine = Arc::new(crate::strategies::simulation::SimulationEngine::new(
            pool_manager.clone(),
            blockchain_manager.clone(),
            price_oracle.clone(),
            gas_oracle_provider.clone(),
            global_config.clone(),
        ));
        
        // Create execution builder
        let execution_builder = Arc::new(crate::execution::ExecutionBuilder::new(
            pool_manager.clone(),
        ));
        
        // Create analytics service
        let analytics = Arc::new(crate::strategies::analytics::SimpleAnalytics::new(
            price_oracle.clone(),
        )) as Arc<dyn crate::strategies::AnalyticsTrait + Send + Sync>;

        // Create strategy context
        let strategy_context = Arc::new(StrategyContext {
            blockchain_manager: blockchain_manager.clone(),
            pool_manager: pool_manager.clone(),
            price_oracle: price_oracle.clone(),
            risk_manager: risk_manager.clone(),
            transaction_optimizer: transaction_optimizer.clone(),
            simulation_engine,
            execution_builder,
            gas_oracle: tokio::sync::RwLock::new(gas_oracle_provider.clone()),
            analytics,
            quote_source,
        });

        // Initialize strategies
        let strategies = Self::initialize_strategies(
            mev_settings.clone(),
            global_config.clone(),
            blockchain_manager.clone(),
            pool_manager.clone(),
            price_oracle.clone(),
            gas_oracle_provider.clone(),
        ).await?;

        // Build strategy registry
        let mut registry = StrategyRegistry::new();
        for strategy in strategies {
            registry.register(strategy).await;
        }

        let config = MEVEngineConfig {
            max_concurrent_executions: mev_settings.max_active_mev_plays,
            opportunity_timeout_ms: (mev_settings.mev_simulation_timeout_secs * 1000) as u64,
            status_check_interval_secs: mev_settings.status_check_interval_secs,
            cleanup_interval_secs: CLEANUP_INTERVAL_SECONDS,
            max_play_age_secs: MAX_PLAY_AGE_SECONDS,
            max_retries: MAX_RETRIES_PER_PLAY,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_secs: 300,
            emergency_pause_threshold: EMERGENCY_PAUSE_THRESHOLD,
        };

        let (shutdown_signal, _) = watch::channel(false);

        Ok(Self {
            config: Arc::new(config),
            strategy_registry: Arc::new(RwLock::new(registry)),
            strategy_context,
            opportunity_receiver: Arc::new(Mutex::new(opportunity_receiver)),
            active_plays: Arc::new(DashMap::new()),
            execution_semaphore: Arc::new(Semaphore::new(mev_settings.max_active_mev_plays)),
            listener_task_handle: Arc::new(Mutex::new(None)),
            monitor_task_handle: Arc::new(Mutex::new(None)),
            cleanup_task_handle: Arc::new(Mutex::new(None)),
            health_monitor_handle: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            shutdown_signal: Arc::new(shutdown_signal),
            metrics: Arc::new(RwLock::new(MEVPlayMetrics::default())),
            is_paused: Arc::new(RwLock::new(false)),
            task_pool: Arc::new(Semaphore::new(MAX_CONCURRENT_PLAYS * 2)), // Allow 2x plays for task spawning
        })
    }

    /// Update the gas oracle provider (useful for backtesting where historical context becomes available later)
    pub async fn update_gas_oracle(&self, oracle: Arc<dyn crate::gas_oracle::GasOracleProvider + Send + Sync>) {
        *self.strategy_context.gas_oracle.write().await = oracle;
    }

    async fn initialize_strategies(
        mev_cfg: MEVEngineSettings,
        global_config: Arc<Config>,
        blockchain_manager: Arc<dyn BlockchainManager>,
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    ) -> Result<Vec<Arc<dyn MEVStrategy>>, MEVEngineError> {
        let mut strategies: Vec<Arc<dyn MEVStrategy>> = Vec::new();

        // Initialize path finder for arbitrage strategies
        let path_finder = crate::path::create_path_finder(
            global_config.clone(),
            blockchain_manager.clone(),
            pool_manager.clone(),
            gas_oracle.clone(),
            price_oracle.clone(),
        ).await
        .map_err(|e| MEVEngineError::ConfigError(format!("Failed to create path finder: {}", e)))?;

        // Initialize Arbitrage Strategy
        if mev_cfg.enabled {
            let strategy_config = MEVStrategyConfig {
                min_profit_usd: mev_cfg.min_arbitrage_profit_usd.unwrap_or(10.0),
                ignore_risk: false,
                volatility_discount: Some(0.95), // 5% discount for volatility
                gas_escalation_factor: Some(100), // 10% escalation factor (within valid range 1-1000)
            };

            let arbitrage_config = crate::strategies::arbitrage::ArbitrageStrategyConfig::default();
            
            let strategy = Arc::new(crate::strategies::arbitrage::ArbitrageStrategy::new(
                arbitrage_config,
                strategy_config,
                path_finder.clone(),
                pool_manager.clone(),
                price_oracle.clone(),
            ));
            
            strategies.push(strategy as Arc<dyn MEVStrategy>);
        }

        // Initialize Sandwich Strategy  
        if mev_cfg.enabled {
            let sandwich_config = crate::strategies::sandwich::SandwichStrategyConfig {
                min_profit_usd: mev_cfg.min_sandwich_profit_usd,
                max_sandwich_hops: 3,
                frontrun_gas_offset_gwei: mev_cfg.sandwich_frontrun_gas_price_gwei_offset,
                backrun_gas_offset_gwei: mev_cfg.sandwich_backrun_gas_price_gwei_offset,
                fixed_frontrun_native_amount_wei: mev_cfg.sandwich_fixed_frontrun_native_amount_wei,
                max_tx_gas_limit: U256::from(mev_cfg.max_tx_gas_limit_mev),
                use_flashbots: mev_cfg.use_flashbots,
                ignore_risk: mev_cfg.ignore_risk_mev,
                gas_escalation_factor: mev_cfg.gas_escalation_factor.unwrap_or(100) as u128,
            };
            
            let strategy = Arc::new(crate::strategies::sandwich::SandwichStrategy::new(
                sandwich_config,
            ));
            
            strategies.push(strategy as Arc<dyn MEVStrategy>);
        }

        // Initialize Liquidation Strategy
        if mev_cfg.enabled {
            let strategy_config = MEVStrategyConfig {
                min_profit_usd: mev_cfg.min_liquidation_profit_usd,
                ignore_risk: false,
                volatility_discount: Some(0.98), // Lower discount for liquidations
                gas_escalation_factor: Some(100), // Moderate gas escalation (within valid range 1-1000)
            };

            let strategy = Arc::new(crate::strategies::liquidation::LiquidationStrategy::new(
                strategy_config,
            ).map_err(|e| {
                error!("Failed to create liquidation strategy: {}", e);
                e
            })?);
            
            strategies.push(strategy as Arc<dyn MEVStrategy>);
        }

        // Initialize JIT Liquidity Strategy
        if mev_cfg.enabled {
            let strategy_config = MEVStrategyConfig {
                min_profit_usd: mev_cfg.min_jit_profit_usd,
                ignore_risk: false,
                volatility_discount: Some(0.95), // Moderate discount for JIT
                gas_escalation_factor: Some(100), // Standard gas escalation (within valid range 1-1000)
            };

            let strategy = Arc::new(crate::strategies::jit_liquidity::JITLiquidityStrategy::new(
                strategy_config,
            ));
            
            strategies.push(strategy as Arc<dyn MEVStrategy>);
        }

        if strategies.is_empty() {
            return Err(MEVEngineError::ConfigError("No strategies enabled".into()));
        }

        info!("Initialized {} MEV strategies", strategies.len());
        Ok(strategies)
    }

    pub async fn start(self: Arc<Self>) -> Result<(), MEVEngineError> {
        info!("Starting MEV Engine");

        // Start opportunity listener
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        {
            let mut tx = self.shutdown_tx.lock().await;
            *tx = Some(shutdown_tx);
        }

        let engine = self.clone();
        let listener_handle = tokio::spawn(async move {
            MEVEngine::opportunity_listener_loop(engine, shutdown_rx).await;
        });
        {
            let mut handle = self.listener_task_handle.lock().await;
            *handle = Some(listener_handle);
        }

        // Start monitor task
        let engine = self.clone();
        let shutdown_signal = self.shutdown_signal.subscribe();
        let monitor_handle = tokio::spawn(async move {
            engine.active_play_monitor_loop(shutdown_signal).await;
        });
        {
            let mut handle = self.monitor_task_handle.lock().await;
            *handle = Some(monitor_handle);
        }

        // Start cleanup task
        let engine = self.clone();
        let shutdown_signal = self.shutdown_signal.subscribe();
        let cleanup_handle = tokio::spawn(async move {
            MEVEngine::cleanup_loop(engine, shutdown_signal).await;
        });
        {
            let mut handle = self.cleanup_task_handle.lock().await;
            *handle = Some(cleanup_handle);
        }

        // Start health monitor
        let engine = self.clone();
        let shutdown_signal = self.shutdown_signal.subscribe();
        let health_handle = tokio::spawn(async move {
            engine.health_monitor_loop(shutdown_signal).await;
        });
        {
            let mut handle = self.health_monitor_handle.lock().await;
            *handle = Some(health_handle);
        }

        info!("MEV Engine started successfully");
        Ok(())
    }

    pub async fn stop(&self) {
        info!("Stopping MEV Engine");

        // Signal shutdown
        let _ = self.shutdown_signal.send(true);

        // Send shutdown signal to listener
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }

        // Wait for tasks to complete
        if let Some(handle) = self.listener_task_handle.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.monitor_task_handle.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.cleanup_task_handle.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.health_monitor_handle.lock().await.take() {
            let _ = handle.await;
        }

        info!("MEV Engine stopped");
    }

    async fn opportunity_listener_loop(engine: Arc<MEVEngine>, mut shutdown_rx: oneshot::Receiver<()>) {
        info!("Starting opportunity listener loop");
        
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Opportunity listener received shutdown signal");
                    break;
                }
                opportunity = async {
                    let mut receiver = engine.opportunity_receiver.lock().await;
                    receiver.recv().await
                } => {
                    match opportunity {
                        Ok(opp) => {
                            // Check if paused
                            if *engine.is_paused.read().await {
                                warn!("MEV Engine is paused, ignoring opportunity");
                                continue;
                            }

                            // Try to acquire task permit before spawning
                            let engine_clone = Arc::clone(&engine);
                            let play_id = Uuid::new_v4();
                            
                            tokio::spawn(async move {
                                let task_pool = engine_clone.task_pool.clone();
                                let Ok(permit) = task_pool.try_acquire() else {
                                    warn!("Task pool exhausted, dropping opportunity");
                                    return;
                                };
                                // Hold permit until task completes
                                if let Err(e) = engine_clone.handle_opportunity_with_shared_state(
                                    play_id,
                                    opp,
                                ).await {
                                    error!("Failed to handle opportunity: {}", e);
                                }
                                drop(permit);
                            });
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Opportunity receiver lagged by {} messages", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            error!("Opportunity channel closed");
                            break;
                        }
                    }
                }
            }
        }
        
        info!("Opportunity listener loop stopped");
    }

    async fn cleanup_loop(engine: Arc<MEVEngine>, mut shutdown_signal: watch::Receiver<bool>) {
        let mut interval = interval(Duration::from_secs(engine.config.cleanup_interval_secs));
        
        loop {
            tokio::select! {
                _ = shutdown_signal.changed() => {
                    if *shutdown_signal.borrow() {
                        info!("Cleanup loop received shutdown signal");
                        break;
                    }
                }
                _ = interval.tick() => {
                    engine.cleanup_stale_plays().await;
                }
            }
        }
    }

    async fn cleanup_stale_plays(&self) {
        let mut removed = 0;
        let max_age = Duration::from_secs(self.config.max_play_age_secs);
        
        // Collect plays to remove first to avoid async issues
        let mut plays_to_remove = Vec::new();
        for entry in self.active_plays.iter() {
            let play_id = entry.key().clone();
            let play_wrapper = entry.value().clone();
            
            let should_remove = play_wrapper.read(|play| {
                play.is_terminal() && play.age() > max_age
            }).await;
            
            if should_remove {
                plays_to_remove.push(play_id);
            }
        }
        
        // Remove collected plays
        for play_id in plays_to_remove {
            self.active_plays.remove(&play_id);
            removed += 1;
        }
        
        if removed > 0 {
            debug!("Cleaned up {} stale plays", removed);
        }
    }

    async fn health_monitor_loop(self: Arc<Self>, mut shutdown_signal: watch::Receiver<bool>) {
        let mut interval = interval(Duration::from_secs(STRATEGY_HEALTH_CHECK_INTERVAL_SECONDS));
        
        loop {
            tokio::select! {
                _ = shutdown_signal.changed() => {
                    if *shutdown_signal.borrow() {
                        info!("Health monitor received shutdown signal");
                        break;
                    }
                }
                _ = interval.tick() => {
                    self.check_system_health().await;
                }
            }
        }
    }

    async fn check_system_health(&self) {
        let registry = self.strategy_registry.read().await;
        let health_statuses = registry.get_all_health_statuses().await;
        let unhealthy_count = health_statuses.values()
            .filter(|status| !status.is_healthy)
            .count();
        
        if unhealthy_count > 0 {
            warn!("Found {} unhealthy strategies", unhealthy_count);
        }
        
        // Check overall system health
        let metrics = self.metrics.read().await;
        if metrics.total_plays > 100 && metrics.success_rate() < 0.1 {
            error!("System success rate below 10%, consider pausing");
            *self.is_paused.write().await = true;
        }
    }

    pub async fn pause(&self) {
        info!("Pausing MEV Engine");
        *self.is_paused.write().await = true;
    }

    pub async fn resume(&self) {
        info!("Resuming MEV Engine");
        *self.is_paused.write().await = false;
    }

    pub async fn get_metrics(&self) -> MEVPlayMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn get_active_plays(&self) -> Vec<ActiveMEVPlay> {
        let mut plays = Vec::new();
        for entry in self.active_plays.iter() {
            let play = entry.value().read(|p| p.clone()).await;
            plays.push(play);
        }
        plays
    }

    pub async fn get_strategy_health(&self) -> HashMap<String, StrategyHealthStatus> {
        let registry = self.strategy_registry.read().await;
        registry.get_all_health_statuses().await
    }

    /// Register external strategies with the MEVEngine
    ///
    /// This method allows external components (like IntegrationCoordinator) to register
    /// strategies that were created outside of the MEVEngine's internal initialization.
    /// These strategies will be added to the strategy registry and used for opportunity execution.
    ///
    /// # Arguments
    /// * `strategies` - A vector of strategy instances to register
    ///
    /// # Returns
    /// A Result indicating success or failure of the registration process
    pub async fn register_strategies(
        &self,
        strategies: Vec<Arc<dyn MEVStrategy>>
    ) -> Result<(), MEVEngineError> {
        let strategy_count = strategies.len();
        debug!("Registering {} external strategies", strategy_count);

        let mut registry = self.strategy_registry.write().await;

        for strategy in strategies {
            let strategy_name = strategy.name().to_string();
            registry.register(strategy).await;
            debug!("Registered strategy: {}", strategy_name);
        }

        info!("Successfully registered {} external strategies", strategy_count);
        Ok(())
    }
}