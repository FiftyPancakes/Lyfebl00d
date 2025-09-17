// src/shared_setup.rs

//! # Shared Infrastructure Setup
//!
//! This module provides a centralized, production-grade function for initializing the
//! complete infrastructure stack for a single blockchain. It is designed to be called
//! by both the live trading entry point (`main.rs`) and the backtesting engine
//! (`backtester.rs`) to ensure a consistent and correctly configured environment.
//!
//! The core function, `setup_chain_infra`, orchestrates the instantiation and wiring
//! of all essential components, respecting the complex dependency graph between them.

use crate::{
    blockchain::{BlockchainManager, BlockchainManagerImpl},
    config::{Config, ExecutionMode, PerChainConfig},
    gas_oracle::{GasOracle, GasOracleProvider},
    path::{AStarPathFinder, PathFinder},
    pool_manager::{PoolManager, PoolManagerTrait},
    pool_states::PoolStateOracle,
    price_oracle::PriceOracle as PriceOracleTrait,
    quote_source::AggregatorQuoteSource,
    risk::{RiskManager, RiskManagerImpl},
    swap_loader::HistoricalSwapLoader,
    token_registry::{CoinGeckoTokenRegistry, TokenRegistry},
    transaction_optimizer::{
        TransactionOptimizerV2, TransactionOptimizerTrait,
        EnhancedOptimizerConfig, MonitoringConfig, SecurityConfig, PerformanceConfig,
        BundleCheckMethod
    },
    types::{
        ArbitrageEngine, ArbitrageMetrics,
    },
    mempool::{MempoolMonitor, BlocknativeMempoolMonitor, CompositeMempoolMonitor},
    mempool_stats::MempoolStatsProvider,
};
use sqlx::PgPool;
use eyre::{Result, WrapErr};
use std::sync::Arc;
use std::time::Instant;
use std::str::FromStr;
use tokio::sync::RwLock;
use tracing::{info, debug};

use std::collections::{HashMap, HashSet};
use std::time::Duration;
use ethers::types::U256;

/// A container for all fully initialized, chain-specific infrastructure components.
///
/// This struct acts as a bundle of all the services required to operate on a single
/// blockchain. It is the return type of the main setup function and provides a clean
/// way to pass the entire infrastructure stack to higher-level modules like the
/// `AlphaEngine` or a dedicated backtest runner.
#[derive(Clone)]
pub struct ChainInfra {
    pub chain_name: String,
    pub blockchain_manager: Arc<dyn BlockchainManager + Send + Sync>,
    pub pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    pub path_finder: Arc<dyn PathFinder + Send + Sync>,
    pub risk_manager: Arc<dyn RiskManager + Send + Sync>,
    pub transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync>,
    pub arbitrage_engine: Arc<ArbitrageEngine>,
    pub pool_state_oracle: Arc<PoolStateOracle>,
    pub price_oracle: Arc<dyn PriceOracleTrait>,
    pub gas_oracle: Arc<dyn GasOracleProvider>,
    pub token_registry: Arc<dyn TokenRegistry + Send + Sync>,
    pub quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
    pub global_metrics: Arc<ArbitrageMetrics>,
    pub mempool_monitor: Arc<dyn MempoolMonitor + Send + Sync>,
}

/// Orchestrates the complete setup of all infrastructure components for a single chain.
///
/// This function is the single source of truth for component initialization. It takes
/// chain-specific and global configurations and wires together the complex dependency
/// graph of managers, engines, and services.
///
/// # Arguments
/// * `chain_name` - The canonical name of the chain being set up (e.g., "ethereum").
/// * `per_chain_config` - The configuration specific to this chain.
/// * `global_config` - The `Arc` wrapped global configuration containing all settings.
/// * `quote_sources` - A shared `Arc` to the global aggregator quote source.
/// * `swap_loader` - Required in backtest mode: Arc to the historical swap loader.
/// * `global_metrics` - A shared `Arc` to the global metrics collector.
/// * `db_pool` - A `Pool` to the PostgreSQL database for the token registry.
///
/// # Returns
/// A `Result` containing the fully instantiated `ChainInfra` struct, or an `eyre::Report`
/// detailing any configuration or initialization failure.
pub async fn setup_chain_infra(
    chain_name: &str,
    per_chain_config: &PerChainConfig,
    global_config: &Arc<Config>,
    quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
    swap_loader: Option<Arc<HistoricalSwapLoader>>,
    global_metrics: Arc<ArbitrageMetrics>,
    db_pool: Arc<PgPool>,
    price_oracle: Arc<dyn PriceOracleTrait>,
    gas_oracle: Arc<dyn GasOracleProvider>,
) -> Result<ChainInfra> {
    let start_time = Instant::now();
    info!(
        chain = %chain_name,
        "Starting infrastructure setup for chain: {}", chain_name
    );

    let mode = global_config.mode;
    
    // Validate configuration based on execution mode
    debug!("Validating configuration for chain: {}", chain_name);
    if mode == ExecutionMode::Live {
        if let Err(e) = per_chain_config.validate_for_live() {
            return Err(eyre::eyre!("Invalid configuration for chain '{}'", chain_name)).wrap_err(e);
        }
    } else {
        if let Err(e) = per_chain_config.validate() {
            return Err(eyre::eyre!("Invalid configuration for chain '{}'", chain_name)).wrap_err(e);
        }
    }
    if mode == ExecutionMode::Backtest && swap_loader.is_none() {
        return Err(eyre::eyre!(
            "HistoricalSwapLoader is required for backtest mode on chain '{}'",
            chain_name
        ));
    }

    let module_config = global_config.module_config.as_ref()
        .ok_or_else(|| eyre::eyre!("`module_config` section is missing from the global configuration"))?;

    // --- 1. Blockchain Manager ---
    debug!("Initializing BlockchainManager for chain: {}", chain_name);
    let mut blockchain_manager_impl = BlockchainManagerImpl::new(&global_config.clone(), chain_name, mode, Some(price_oracle.clone())).await?;
    
    // Set the gas oracle on the blockchain manager
    blockchain_manager_impl.set_gas_oracle(gas_oracle.clone());
    
    // Set the swap loader for backtest mode
    if let Some(loader) = &swap_loader {
        blockchain_manager_impl.set_swap_loader(loader.clone());
    }
    
    let blockchain_manager: Arc<dyn BlockchainManager> = Arc::new(blockchain_manager_impl);
    info!("BlockchainManager initialized.");

    // --- 2. Token Registry ---
    debug!("Initializing TokenRegistry for chain: {}", chain_name);
    let token_registry: Arc<dyn TokenRegistry + Send + Sync> =
        Arc::new(CoinGeckoTokenRegistry::new(db_pool.clone(), chain_name.to_string()));
    info!("TokenRegistry initialized.");

    // --- 3. Pool Manager (without PoolStateOracle initially) ---
    debug!("Initializing PoolManager for chain: {}", chain_name);
    let pool_manager: Arc<dyn PoolManagerTrait> = Arc::new(PoolManager::new_without_state_oracle(
        global_config.clone(),
    ).await?);
    info!("PoolManager initialized.");

    // --- 4. Pool State Oracle (with real PoolManager) ---
    debug!("Initializing PoolStateOracle with real PoolManager for chain: {}", chain_name);
    let pool_state_oracle = Arc::new(PoolStateOracle::new(
        global_config.clone(),
        {
            let mut map = HashMap::new();
            map.insert(chain_name.to_string(), blockchain_manager.clone() as Arc<dyn BlockchainManager>);
            map
        },
        swap_loader.clone(),
        pool_manager.clone(),
    )?);

    // Start periodic pool state refresh to ensure data freshness
    pool_state_oracle.start_periodic_refresh(300, 600); // Refresh every 5 minutes, max age 10 minutes
    info!("PoolStateOracle initialized with periodic refresh enabled.");

    // --- 5. Set PoolStateOracle in PoolManager ---
    debug!("Setting PoolStateOracle in PoolManager for chain: {}", chain_name);
    pool_manager.set_state_oracle(pool_state_oracle.clone()).await?;
    info!("PoolStateOracle set in PoolManager.");

    // --- 5. Path Finder ---
    debug!("Initializing PathFinder for chain: {}", chain_name);
    let router_tokens = per_chain_config.router_tokens.clone().unwrap_or_default();
    let path_finder: Arc<dyn PathFinder + Send + Sync> = Arc::new(AStarPathFinder::new(
        module_config.path_finder_settings.clone(),
        pool_manager.clone(),
        blockchain_manager.clone(),
        global_config.clone(),
        Arc::new(GasOracle::new(gas_oracle.clone())),
        price_oracle.clone(),
        blockchain_manager.get_chain_id(),
        mode == ExecutionMode::Backtest,
        router_tokens,
    ).map_err(|e| eyre::eyre!("Failed to create AStarPathFinder: {}", e))?);
    info!("PathFinder initialized.");

    // --- 6. Risk Manager ---
    debug!("Initializing RiskManager for chain: {}", chain_name);
    let risk_manager: Arc<dyn RiskManager> = Arc::new(RiskManagerImpl::new(
        Arc::new(module_config.risk_settings.clone()),
        blockchain_manager.clone(),
        pool_manager.clone(),
        gas_oracle.clone(),
        price_oracle.clone(),
    ));
    info!("RiskManager initialized.");

    // --- 7. Mempool Monitor and Transaction Optimizer ---
    debug!("Initializing MempoolMonitor and TransactionOptimizer for chain: {}", chain_name);
    
    let mempool_config = module_config.mempool_config.as_ref()
        .cloned()
        .unwrap_or_default();
    
    let mempool_monitor: Arc<dyn MempoolMonitor + Send + Sync> = {
        let blocknative_monitor = BlocknativeMempoolMonitor::new(
            Arc::new(mempool_config.clone()),
            "blocknative_api_key".to_string(), // TODO: Get from config
        ).map_err(|e| eyre::eyre!("Failed to create BlocknativeMempoolMonitor: {}", e))?;
        
        let composite_monitor = CompositeMempoolMonitor::new(
            vec![Arc::new(blocknative_monitor)],
            mempool_config.max_pending_txs,
        ).map_err(|e| eyre::eyre!("Failed to create CompositeMempoolMonitor: {}", e))?;
        
        Arc::new(composite_monitor)
    };
    
    let mempool_stats_provider: Arc<dyn MempoolStatsProvider + Send + Sync> = 
        Arc::new(crate::mempool_stats::LiveMempoolStatsProvider::new(mempool_monitor.clone()));
    
    // Default arbitrage executor address - should be configured per chain in production
    let arbitrage_executor_address = ethers::types::Address::from_str("0x1234567890123456789012345678901234567890")
        .unwrap_or_else(|_| ethers::types::Address::zero());
    
    // Create EnhancedOptimizerConfig with production defaults
    let enhanced_config = EnhancedOptimizerConfig {
        base_config: Arc::new(module_config.transaction_optimizer_settings.clone()),
        monitoring: MonitoringConfig::default(),
        security: SecurityConfig {
            max_transaction_value: U256::from_dec_str("1000000000000000000000").unwrap(), // 1000 ETH
            max_gas_limit: U256::from(10_000_000u64),
            blacklisted_addresses: HashSet::new(),
            whitelisted_addresses: HashSet::new(),
            require_whitelist: false,
            max_daily_transactions: 1000,
            max_daily_value: U256::from_dec_str("10000000000000000000000").unwrap(), // 10000 ETH
        },
        performance: PerformanceConfig {
            simulation_cache_size: 1000,
            simulation_cache_ttl: Duration::from_secs(300),
            max_concurrent_retries: 3,
            retry_jitter_factor: 0.1,
            bundle_status_check_method: BundleCheckMethod::Hybrid,
        },
        circuit_breaker_threshold: 5,
        circuit_breaker_timeout: Duration::from_secs(300),
    };
    
    let transaction_optimizer: Arc<dyn TransactionOptimizerTrait> = 
        TransactionOptimizerV2::new(
            Arc::new(module_config.transaction_optimizer_settings.clone()),
            enhanced_config,
            blockchain_manager.clone(),
            gas_oracle.clone(),
            price_oracle.clone(),
            Some(mempool_stats_provider.clone()),
            arbitrage_executor_address,
        )
        .await? as Arc<dyn TransactionOptimizerTrait>;
    info!("TransactionOptimizer initialized.");

    // --- 8. Arbitrage Engine ---
    debug!("Initializing ArbitrageEngine for chain: {}", chain_name);
    let arbitrage_engine = Arc::new(ArbitrageEngine {
        config: Arc::new(module_config.arbitrage_settings.clone()),
        full_config: global_config.clone(),
        price_oracle: price_oracle.clone(),
        risk_manager: Arc::new(RwLock::new(Some(risk_manager.clone()))),
        blockchain_manager: blockchain_manager.clone(),
        quote_source: quote_source.clone(),
        global_metrics: global_metrics.clone(),
        alpha_engine: None, // Will be set later in main.rs
        swap_loader: swap_loader.clone(),
    });
    info!("ArbitrageEngine initialized.");

    let infra = ChainInfra {
        chain_name: chain_name.to_string(),
        blockchain_manager,
        pool_manager,
        path_finder,
        risk_manager,
        transaction_optimizer,
        arbitrage_engine,
        pool_state_oracle,
        price_oracle,
        gas_oracle,
        token_registry,
        quote_source,
        global_metrics,
        mempool_monitor,
    };

    info!(
        chain = %chain_name,
        duration_ms = start_time.elapsed().as_millis(),
        "Completed infrastructure setup successfully."
    );

    Ok(infra)
}