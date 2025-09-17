use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{broadcast, RwLock, Mutex};
use tracing::{info, warn};
use ethers::types::U256;

use crate::{
    alpha::{AlphaEngine, AlphaEngineConfig},
    mev_engine::MEVEngine,
    unified_pipeline::{UnifiedOpportunityPipeline, PipelineConfig},
    path::{
        enhanced_heuristics::EnhancedHeuristics,
        circular_arbitrage::CircularArbitrageDetector,
        finder::AStarPathFinder,
    },
    strategies::{
        MEVStrategy, MEVStrategyConfig,
        arbitrage::ArbitrageStrategy,
        sandwich::SandwichStrategy,
        liquidation::LiquidationStrategy,
        jit_liquidity::JITLiquidityStrategy,
        discovery::{
            DiscoveryScanner,
            cyclic_scanner::CyclicArbitrageScanner,
            statistical_scanner::StatisticalArbitrageScanner,
            market_making_scanner::MarketMakingScanner,
        },
    },
    config::Config,
    shared_setup::ChainInfra,
    errors::BotError,
    types::ChainEvent,
    mempool::MEVOpportunity,
};

/// Master coordinator that integrates all components
pub struct IntegrationCoordinator {
    /// Global configuration
    config: Arc<Config>,
    
    /// Chain infrastructures
    chain_infras: HashMap<String, Arc<ChainInfra>>,
    
    /// Enhanced path finder with improved heuristics
    enhanced_path_finders: HashMap<String, Arc<EnhancedPathFinder>>,
    
    /// Circular arbitrage detectors per chain
    circular_detectors: HashMap<String, Arc<CircularArbitrageDetector>>,
    
    /// Alpha engine for discovery
    alpha_engine: Option<Arc<AlphaEngine>>,
    
    /// MEV engine for execution
    mev_engine: Option<Arc<MEVEngine>>,
    
    /// Unified pipeline
    unified_pipeline: Option<Arc<UnifiedOpportunityPipeline>>,
    
    /// All registered strategies
    strategies: Vec<Arc<dyn MEVStrategy>>,
    
    /// All registered scanners
    scanners: HashMap<String, Arc<dyn DiscoveryScanner>>,
    
    /// Event broadcaster
    event_broadcaster: broadcast::Sender<ChainEvent>,
    
    /// Opportunity channel
    opportunity_channel: (broadcast::Sender<MEVOpportunity>, Arc<Mutex<broadcast::Receiver<MEVOpportunity>>>),
    
    /// Metrics
    metrics: Arc<CoordinatorMetrics>,
    
    /// Running state
    is_running: Arc<RwLock<bool>>,

    /// Cached alpha engine configuration to align downstream components
    alpha_engine_config: Option<AlphaEngineConfig>,
}

/// Enhanced path finder that combines A* with advanced heuristics
pub struct EnhancedPathFinder {
    base_finder: Arc<AStarPathFinder>,
    heuristics: Arc<EnhancedHeuristics>,
    circular_detector: Arc<CircularArbitrageDetector>,
}

struct CoordinatorMetrics {
    total_strategies: std::sync::atomic::AtomicUsize,
    total_scanners: std::sync::atomic::AtomicUsize,
    active_chains: std::sync::atomic::AtomicUsize,
    opportunities_found: std::sync::atomic::AtomicU64,
    opportunities_executed: std::sync::atomic::AtomicU64,
    total_profit_usd: Arc<RwLock<f64>>,
}

impl IntegrationCoordinator {
    /// Create a new integration coordinator
    pub async fn new(config: Arc<Config>) -> Result<Self, BotError> {
        let (event_tx, _) = broadcast::channel::<ChainEvent>(1024);
        let (opp_tx, opp_rx) = broadcast::channel::<MEVOpportunity>(1024);
        
        Ok(Self {
            config,
            chain_infras: HashMap::new(),
            enhanced_path_finders: HashMap::new(),
            circular_detectors: HashMap::new(),
            alpha_engine: None,
            mev_engine: None,
            unified_pipeline: None,
            strategies: Vec::new(),
            scanners: HashMap::new(),
            event_broadcaster: event_tx,
            opportunity_channel: (opp_tx, Arc::new(Mutex::new(opp_rx))),
            metrics: Arc::new(CoordinatorMetrics {
                total_strategies: std::sync::atomic::AtomicUsize::new(0),
                total_scanners: std::sync::atomic::AtomicUsize::new(0),
                active_chains: std::sync::atomic::AtomicUsize::new(0),
                opportunities_found: std::sync::atomic::AtomicU64::new(0),
                opportunities_executed: std::sync::atomic::AtomicU64::new(0),
                total_profit_usd: Arc::new(RwLock::new(0.0)),
            }),
            is_running: Arc::new(RwLock::new(false)),
            alpha_engine_config: None,
        })
    }

    /// Initialize all components
    pub async fn initialize(&mut self) -> Result<(), BotError> {
        info!("Initializing Integration Coordinator");
        
        // Initialize chain infrastructures
        self.initialize_chains().await?;
        
        // Initialize enhanced path finders
        self.initialize_enhanced_pathfinders().await?;
        
        // Initialize all strategies
        self.initialize_all_strategies().await?;
        
        // Initialize all scanners
        self.initialize_all_scanners().await?;
        
        // Initialize Alpha engine
        self.initialize_alpha_engine().await?;
        
        // Initialize MEV engine
        self.initialize_mev_engine().await?;
        
        // Initialize unified pipeline
        self.initialize_unified_pipeline().await?;
        
        info!("Integration Coordinator initialized successfully");
        Ok(())
    }

    /// Initialize chain infrastructures
    async fn initialize_chains(&mut self) -> Result<(), BotError> {
        // Load chain configurations and create infrastructure for each configured chain
        // This is a placeholder implementation - in practice, this would load from config
        // and create ChainInfra instances using shared_setup::setup_chain_infra

        // For now, we'll assume chain_infras are populated elsewhere or this is a test setup
        // In a real implementation, this would iterate over configured chains and call
        // setup_chain_infra for each one

        if self.chain_infras.is_empty() {
            warn!("No chain infrastructures were initialized - MEVEngine and other components may fail to initialize");
        }

        self.metrics.active_chains.store(
            self.chain_infras.len(),
            std::sync::atomic::Ordering::Relaxed
        );
        Ok(())
    }

    /// Initialize enhanced path finders with improved heuristics
    async fn initialize_enhanced_pathfinders(&mut self) -> Result<(), BotError> {
        for (chain_name, chain_infra) in &self.chain_infras {
            // Create enhanced heuristics
            let heuristics = Arc::new(EnhancedHeuristics::new());
            
            // Create circular arbitrage detector with optimized parameters
            let circular_detector = Arc::new(CircularArbitrageDetector::new(
                3,  // min cycle length (triangular)
                7,  // max cycle length
                50, // min profit bps
            ));
            
            // Get base path finder
            let base_finder = chain_infra.path_finder.as_any()
                .downcast_ref::<AStarPathFinder>()
                .ok_or_else(|| BotError::Configuration(format!(
                    "PathFinder for chain '{}' is not of the expected AStarPathFinder type. This is a core system assumption and indicates a configuration error.",
                    chain_name
                )))?;

            let enhanced = Arc::new(EnhancedPathFinder {
                base_finder: Arc::new(base_finder.clone()),
                heuristics: heuristics.clone(),
                circular_detector: circular_detector.clone(),
            });

            self.enhanced_path_finders.insert(chain_name.clone(), enhanced);
            self.circular_detectors.insert(chain_name.clone(), circular_detector);
        }
        
        info!("Initialized {} enhanced path finders", self.enhanced_path_finders.len());
        Ok(())
    }

    /// Initialize all MEV strategies
    async fn initialize_all_strategies(&mut self) -> Result<(), BotError> {
        // Arbitrage Strategy
        let arb_config = crate::strategies::arbitrage::ArbitrageStrategyConfig {
            min_profit_usd: 10.0,
            max_profit_usd: 100_000.0,
            slippage_tolerance_bps: 100,
            max_gas_price_gwei: 500,
            enable_flashloans: true,
            max_position_size_usd: 1_000_000.0,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_seconds: 300,
            volatility_discount: 0.95,
            gas_escalation_factor: 1.1,
        };
        
        let base_config = MEVStrategyConfig {
            min_profit_usd: 10.0,
            ignore_risk: false,
            volatility_discount: Some(0.95),
            gas_escalation_factor: Some(110),
        };
        
        // Create strategies for each chain
        for (chain_name, chain_infra) in &self.chain_infras {
            if let Some(enhanced_finder) = self.enhanced_path_finders.get(chain_name) {
                let arb_strategy = Arc::new(ArbitrageStrategy::new(
                    arb_config.clone(),
                    base_config.clone(),
                    enhanced_finder.base_finder.clone(),
                    chain_infra.pool_manager.clone(),
                    chain_infra.price_oracle.clone(),
                ));
                self.strategies.push(arb_strategy);
            }
        }
        
        // Sandwich Strategy
        let sandwich_config = crate::strategies::sandwich::SandwichStrategyConfig {
            min_profit_usd: 20.0,
            max_sandwich_hops: 3,
            frontrun_gas_offset_gwei: 2,
            backrun_gas_offset_gwei: 1,
            fixed_frontrun_native_amount_wei: U256::from(10).pow(U256::from(17)), // 0.1 ETH
            max_tx_gas_limit: U256::from(500_000),
            use_flashbots: true,
            ignore_risk: false,
            gas_escalation_factor: 120,
        };
        
        self.strategies.push(Arc::new(SandwichStrategy::new(sandwich_config)));
        
        // Liquidation Strategy
        let liquidation_config = MEVStrategyConfig {
            min_profit_usd: 50.0,
            ignore_risk: false,
            volatility_discount: Some(0.98),
            gas_escalation_factor: Some(105),
        };
        
        match LiquidationStrategy::new(liquidation_config) {
            Ok(strategy) => self.strategies.push(Arc::new(strategy)),
            Err(e) => warn!("Failed to create liquidation strategy: {}", e),
        }
        
        // JIT Liquidity Strategy
        let jit_config = MEVStrategyConfig {
            min_profit_usd: 30.0,
            ignore_risk: false,
            volatility_discount: Some(0.95),
            gas_escalation_factor: Some(110),
        };
        
        self.strategies.push(Arc::new(JITLiquidityStrategy::new(jit_config)));
        
        self.metrics.total_strategies.store(
            self.strategies.len(),
            std::sync::atomic::Ordering::Relaxed
        );
        
        info!("Initialized {} MEV strategies", self.strategies.len());
        Ok(())
    }

    /// Initialize all discovery scanners
    async fn initialize_all_scanners(&mut self) -> Result<(), BotError> {
        for (chain_name, chain_infra) in &self.chain_infras {
            let context = crate::strategies::discovery::ScannerContext::new(chain_infra.clone());
            
            // Cyclic Arbitrage Scanner
            let cyclic_scanner = Arc::new(CyclicArbitrageScanner::new(
                context.clone(),
                U256::from(10).pow(U256::from(17)), // 0.1 ETH min
                5.0, // $5 min profit
            ));
            self.scanners.insert(
                format!("{}_cyclic", chain_name),
                cyclic_scanner as Arc<dyn DiscoveryScanner>
            );
            
            // Statistical Arbitrage Scanner
            let statistical_scanner = Arc::new(StatisticalArbitrageScanner::new(
                context.clone(),
                U256::from(10).pow(U256::from(17)),
                10.0,
                2.5, // z-score threshold
                Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            ));
            self.scanners.insert(
                format!("{}_statistical", chain_name),
                statistical_scanner as Arc<dyn DiscoveryScanner>
            );
            
            // Market Making Scanner
            let market_scanner = Arc::new(MarketMakingScanner::new(
                context.clone(),
                U256::from(10).pow(U256::from(17)),
                8.0,
            ));
            self.scanners.insert(
                format!("{}_market", chain_name),
                market_scanner as Arc<dyn DiscoveryScanner>
            );
            
            // Cross-chain Scanner (if applicable)
            // Would need cross-chain pathfinder instance
        }
        
        self.metrics.total_scanners.store(
            self.scanners.len(),
            std::sync::atomic::Ordering::Relaxed
        );
        
        info!("Initialized {} discovery scanners", self.scanners.len());
        Ok(())
    }

    /// Initialize Alpha engine with all scanners
    async fn initialize_alpha_engine(&mut self) -> Result<(), BotError> {
        // Extract from global config to align with system settings
        let alpha_config = AlphaEngineConfig::from_config(&self.config)
            .map_err(|e| BotError::Config(e.to_string()))?;
        
        // Create shared chain infras
        let mut shared_infras = HashMap::new();
        for (chain_name, infra) in &self.chain_infras {
            shared_infras.insert(chain_name.clone(), crate::shared_setup::ChainInfra {
                chain_name: chain_name.clone(),
                price_oracle: infra.price_oracle.clone(),
                path_finder: infra.path_finder.clone(),
                pool_manager: infra.pool_manager.clone(),
                blockchain_manager: infra.blockchain_manager.clone(),
                risk_manager: infra.risk_manager.clone(),
                transaction_optimizer: infra.transaction_optimizer.clone(),
                gas_oracle: infra.gas_oracle.clone(),
                pool_state_oracle: infra.pool_state_oracle.clone(),
                token_registry: infra.token_registry.clone(),
                arbitrage_engine: infra.arbitrage_engine.clone(),
                quote_source: infra.quote_source.clone(),
                global_metrics: infra.global_metrics.clone(),
                mempool_monitor: infra.mempool_monitor.clone(),
            });
        }
        
        let alpha_engine = AlphaEngine::new(
            self.config.clone(),
            shared_infras,
            Arc::new(crate::types::ArbitrageMetrics::new()),
            self.event_broadcaster.clone(),
        )?;

        let alpha_engine_arc = Arc::new(alpha_engine);

        // Register the scanners created in initialize_all_scanners
        if !self.scanners.is_empty() {
            alpha_engine_arc.register_scanners(self.scanners.clone()).await?;
            info!("Registered {} discovery scanners with AlphaEngine", self.scanners.len());
        } else {
            warn!("No scanners available to register with AlphaEngine - this may cause discovery failures");
        }

        self.alpha_engine = Some(alpha_engine_arc);
        self.alpha_engine_config = Some(alpha_config);

        info!("Initialized Alpha Engine");
        Ok(())
    }

    /// Initialize MEV engine with all strategies
    async fn initialize_mev_engine(&mut self) -> Result<(), BotError> {
        // Get the first available chain infrastructure for MEVEngine initialization
        let first_chain_infra = self.chain_infras.values().next()
            .ok_or_else(|| BotError::Configuration("No chain infrastructure available for MEVEngine initialization".to_string()))?;

        // Create the MEVEngine with the first available chain's infrastructure
        let mev_engine = Arc::new(
            crate::mev_engine::MEVEngine::new(
                self.config.clone(),
                first_chain_infra.blockchain_manager.clone(),
                first_chain_infra.pool_manager.clone(),
                first_chain_infra.price_oracle.clone(),
                first_chain_infra.risk_manager.clone(),
                first_chain_infra.transaction_optimizer.clone(),
                first_chain_infra.quote_source.clone(),
                self.opportunity_channel.0.subscribe(),
            ).await?
        );

        // Register the strategies created in initialize_all_strategies
        if !self.strategies.is_empty() {
            mev_engine.register_strategies(self.strategies.clone()).await?;
            info!("Registered {} strategies with MEVEngine", self.strategies.len());
        } else {
            warn!("No strategies available to register with MEVEngine - this may cause execution failures");
        }

        self.mev_engine = Some(mev_engine);

        info!("Initialized MEV Engine with {} strategies", self.strategies.len());
        Ok(())
    }

    /// Initialize unified pipeline
    async fn initialize_unified_pipeline(&mut self) -> Result<(), BotError> {
        if let (Some(alpha), Some(mev)) = (&self.alpha_engine, &self.mev_engine) {
            let pipeline_config = PipelineConfig {
                max_opportunities_per_block: 100,
                opportunity_expiry_secs: 10,
                min_profit_usd: 5.0,
                enable_parallel_discovery: true,
                discovery_timeout_ms: 5000,
                enable_prioritization: true,
                max_concurrent_executions: 20,
            };
            
            let pipeline = UnifiedOpportunityPipeline::new(
                alpha.clone(),
                mev.clone(),
                pipeline_config,
            ).await?;
            
            self.unified_pipeline = Some(Arc::new(pipeline));
            
            info!("Initialized Unified Pipeline");
        }
        
        Ok(())
    }

    /// Start all components
    pub async fn start(&self) -> Result<(), BotError> {
        let mut running = self.is_running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);
        
        info!("Starting Integration Coordinator");
        
        // Start unified pipeline (which starts Alpha and MEV engines)
        if let Some(pipeline) = &self.unified_pipeline {
            pipeline.clone().start().await?;
        }
        
        // Start monitoring tasks
        self.start_monitoring_tasks().await;
        
        info!("Integration Coordinator started successfully");
        Ok(())
    }

    /// Stop all components
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
        drop(running);
        
        info!("Stopping Integration Coordinator");
        
        // Stop unified pipeline
        if let Some(pipeline) = &self.unified_pipeline {
            pipeline.stop().await;
        }
        
        info!("Integration Coordinator stopped");
    }

    /// Start monitoring tasks
    async fn start_monitoring_tasks(&self) {
        // Would start various monitoring tasks
    }

    /// Get coordinator metrics
    pub async fn get_metrics(&self) -> CoordinatorMetricsSnapshot {
        CoordinatorMetricsSnapshot {
            total_strategies: self.metrics.total_strategies
                .load(std::sync::atomic::Ordering::Relaxed),
            total_scanners: self.metrics.total_scanners
                .load(std::sync::atomic::Ordering::Relaxed),
            active_chains: self.metrics.active_chains
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_found: self.metrics.opportunities_found
                .load(std::sync::atomic::Ordering::Relaxed),
            opportunities_executed: self.metrics.opportunities_executed
                .load(std::sync::atomic::Ordering::Relaxed),
            total_profit_usd: *self.metrics.total_profit_usd.read().await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoordinatorMetricsSnapshot {
    pub total_strategies: usize,
    pub total_scanners: usize,
    pub active_chains: usize,
    pub opportunities_found: u64,
    pub opportunities_executed: u64,
    pub total_profit_usd: f64,
}