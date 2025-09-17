//! Application entry‑point – orchestrates **live trading** or **historical back‑testing**
//! depending on `execution_mode` defined in `config/global.toml`.
//!
//! 1. Load configuration → initialise tracing
//! 2. Spin‑up either **live** infrastructure (multi‑chain, WS/HTTP, mempool listeners)
//!    or the deterministic **back‑test** harness.
//! 3. Wire a broadcast bus so every `ChainListener` feeds the `AlphaEngine`.
//! 4. Clean, two‑phase, graceful shutdown driven by Ctrl‑C or panic‑propagation.

// Compile-time mode separation - ensure only one execution mode is enabled
#[cfg(all(feature = "live", feature = "backtest"))]
compile_error!("Enable only one of: `live` or `backtest`.");

#[cfg(feature = "live")]
pub type PriceProvider = rust::price_oracle::live_price_provider::LivePriceProvider;

#[cfg(feature = "backtest")]
pub type PriceProvider = rust::price_oracle::historical_price_provider::HistoricalPriceProvider;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use rust::{
    config::{Config, ExecutionMode},
    errors::{BotError, BacktestError},
    types::ChainEvent,
    alpha::engine::AlphaEngine,
    mev_engine::MEVEngine,
    mempool::{DefaultMempoolMonitor, MEVOpportunity, MempoolMonitor, PotentialSandwichOpportunity},
    shared_setup::{setup_chain_infra, ChainInfra as SharedChainInfra},
    listen::{ChainListener, UnifiedListener},
    types::ArbitrageMetrics,
    backtest::{Backtester},
    quote_source::{AggregatorQuoteSource, OneInchQuoteSource, MultiAggregatorQuoteSource},
    price_oracle::live_price_provider::LivePriceProvider,
    gas_oracle::LiveGasProvider,
    providers::ProductionProviderFactory,
    database,
};

use rust::blockchain::BlockchainManagerImpl;
use rust::rate_limiter::initialize_global_rate_limiter_manager;
use rust::config::ChainSettings;

#[tokio::main]
async fn main() -> Result<(), BotError> {
    // Configure logging to suppress ethers_providers spam while keeping useful logs
    let filter = EnvFilter::from_default_env()
        .add_directive("ethers_providers=warn".parse().unwrap()) // Suppress ethers_providers debug spam
        .add_directive("ethers=warn".parse().unwrap()) // Also suppress ethers core debug logs
        .add_directive("tokio_postgres=warn".parse().unwrap()) // Suppress postgres connection logs
        .add_directive("rust=info".parse().unwrap()) // Keep our application logs at info level
        .add_directive("mev_bot=info".parse().unwrap()) // Keep main bot logs
        .add_directive("blockchain=info".parse().unwrap()) // Keep blockchain connection logs
        .add_directive("chain_listener=info".parse().unwrap()); // Keep listener logs

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting MEV Bot with MEVEngine and AlphaEngine as central orchestrators");

    let config = Arc::new(Config::load_from_directory("config").await?);
    info!("Configuration loaded successfully");

    // Initialize global rate limiter manager early before any blockchain operations
    let rate_limiter_settings = ChainSettings {
        mode: config.mode,
        global_rps_limit: config.chain_config.rate_limiter_settings.global_rps_limit,
        default_chain_rps_limit: config.chain_config.rate_limiter_settings.default_chain_rps_limit,
        default_max_concurrent_requests: 16, // Use default value
        rate_limit_burst_size: 10, // Use default value
        rate_limit_timeout_secs: 30, // Use default value
        rate_limit_max_retries: 3, // Use default value
        rate_limit_initial_backoff_ms: 100, // Use default value
        rate_limit_backoff_multiplier: 2.0, // Use default value
        rate_limit_max_backoff_ms: 5000, // Use default value
        rate_limit_jitter_factor: 0.1, // Use default value
        batch_size: 50, // Use default value
        min_batch_delay_ms: 10, // Use default value
        max_blocks_per_query: None,
        log_fetch_concurrency: None,
    };
    initialize_global_rate_limiter_manager(Arc::new(rate_limiter_settings));
    info!("Global rate limiter manager initialized successfully");

    // Secrets are now loaded automatically by Config::load_from_directory
    // Environment variables are set there for backward compatibility

    match config.mode {
        ExecutionMode::Live => {
            info!("Starting in LIVE mode");
            run_live_trading(config).await?;
        }
        ExecutionMode::Backtest => {
            info!("Starting in BACKTEST mode");
            run_backtest(config).await?;
        }
        ExecutionMode::PaperTrading => {
            info!("Starting in PAPER TRADING mode");
            run_live_trading(config).await?;
        }
    }

    Ok(())
}

async fn run_live_trading(cfg: Arc<Config>) -> Result<(), BotError> {
    // Increase buffer size to handle high event volume on busy chains
    let (event_tx, event_rx) = broadcast::channel::<ChainEvent>(32768);

    // Monitor global event channel utilization and throughput
    let event_tx_monitor = event_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut event_count = 0u64;
        let mut last_report = std::time::Instant::now();

        loop {
            interval.tick().await;
            let subscriber_count = event_tx_monitor.receiver_count();
            let elapsed = last_report.elapsed();
            let events_per_sec = if elapsed.as_secs() > 0 {
                event_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };

            info!(target: "throughput_monitor",
                  global_event_channel_subscribers = subscriber_count,
                  channel_capacity = 32768,
                  events_per_sec = %format!("{:.1}", events_per_sec),
                  "Global event channel throughput");

            // Reset counters
            event_count = 0;
            last_report = std::time::Instant::now();
        }
    });

    let mut chain_infras: HashMap<String, SharedChainInfra> = HashMap::new();
    for (chain_name, chain_config) in &cfg.chain_config.chains {
        // Skip chains that are not enabled for live trading
        if !chain_config.live_enabled {
            info!("Skipping chain {} as it's not enabled for live trading", chain_name);
            continue;
        }
        
        let db_pool = database::get_shared_sqlx_pool(&cfg).await.map_err(|e| BotError::Database(rust::errors::StateError::Config(format!("Failed to get shared sqlx pool: {}", e))))?;
        
        // Load 1inch API key from config secrets or environment
        let one_inch_key = cfg.get_api_key("1inch").unwrap_or_else(|| std::env::var("ONEINCH_API_KEY").unwrap_or_default());

        // Create a proper quote source implementation
        let quote_source = Arc::new(MultiAggregatorQuoteSource::new(vec![
            Arc::new(OneInchQuoteSource::new(one_inch_key, 10)) as Arc<dyn AggregatorQuoteSource>
        ]));
        
        // Share one blockchain manager per chain to avoid duplicate connections
        let bm = Arc::new(BlockchainManagerImpl::new(&cfg, chain_name, cfg.mode, None).await?);
        
        let infra = setup_chain_infra(
            chain_name,
            chain_config,
            &cfg,
            quote_source.clone(),
            None,
            Arc::new(ArbitrageMetrics::new()),
            db_pool,
            Arc::new(LivePriceProvider::new(
                cfg.clone(),
                bm.clone(),
                quote_source.clone(),
            ).await?),
            Arc::new(LiveGasProvider::new(
                chain_name.clone(),
                chain_config.chain_id,
                cfg.clone(),
                cfg.module_config.as_ref().ok_or_else(|| BotError::Config("Gas oracle config not found".to_string()))?.gas_oracle_config.clone(),
                bm.clone(),
            )),
        ).await?;
        chain_infras.insert(chain_name.clone(), infra);
        info!("Infrastructure set up for chain: {}", chain_name);
    }

    let arbitrage_metrics = Arc::new(ArbitrageMetrics::new());

    // Initialize AlphaEngine for all modes - provides advanced discovery strategies
    let alpha_engine = {
        let engine = Arc::new(AlphaEngine::new(
            cfg.clone(),
            chain_infras.clone(),
            arbitrage_metrics.clone(),
            event_tx.clone(),
        )?);

        engine.clone().start().await.map_err(|e| BotError::Infrastructure(format!("AlphaEngine start error: {}", e)))?;
        info!("AlphaEngine initialized and started successfully for {} mode", match cfg.mode {
            ExecutionMode::Live => "LIVE",
            ExecutionMode::Backtest => "BACKTEST",
            ExecutionMode::PaperTrading => "PAPER TRADING",
        });
        Some(engine)
    };

    // Replace mpsc with broadcast for MEVOpportunity so each engine can subscribe
    let (opportunity_tx, _) = broadcast::channel::<MEVOpportunity>(1024);
    let mut mev_engines: HashMap<String, Arc<MEVEngine>> = HashMap::new();
    for (chain_name, infra) in &chain_infras {
        let engine_rx = opportunity_tx.subscribe();
        let mev_engine = Arc::new(MEVEngine::new(
            cfg.clone(),
            infra.blockchain_manager.clone(),
            infra.pool_manager.clone(),
            infra.price_oracle.clone(),
            infra.risk_manager.clone(),
            infra.transaction_optimizer.clone(),
            infra.quote_source.clone(),
            engine_rx,
        ).await.map_err(|e| BotError::Infrastructure(format!("MEVEngine error for chain {}: {}", chain_name, e)))?);
        mev_engine.clone().start().await.map_err(|e| BotError::Infrastructure(format!("MEVEngine start error for chain {}: {}", chain_name, e)))?;
        mev_engines.insert(chain_name.clone(), mev_engine);
        info!("MEVEngine initialized and started successfully for chain: {}", chain_name);
    }

    let mut unified_listener = UnifiedListener::new();
    let mut mempool_handles = Vec::new();

    for (chain_name, infra) in &chain_infras {
        // Start mempool monitoring for both Live and Paper Trading modes
        // Paper trading will process transactions read-only for opportunity discovery
            let mempool_monitor_cfg = cfg.module_config.as_ref()
                .and_then(|mc| mc.mempool_config.clone())
                .unwrap_or_default();
            let mempool_monitor = Arc::new(DefaultMempoolMonitor::new(
                Arc::new(mempool_monitor_cfg),
                infra.blockchain_manager.clone(),
            ).map_err(|e| BotError::Infrastructure(format!("Mempool monitor error: {}", e)))?);
            MempoolMonitor::start(&*mempool_monitor).await.map_err(|e| BotError::Infrastructure(format!("Mempool start error: {}", e)))?;

        let mut mempool_rx = MempoolMonitor::subscribe_to_raw_transactions(&*mempool_monitor);
        let opportunity_tx_clone = opportunity_tx.clone();
        let chain_name_clone = chain_name.clone();
        let pool_manager = infra.pool_manager.clone();
        let price_oracle = infra.price_oracle.clone();
        let risk_manager = infra.risk_manager.clone();

        // Create bounded channel for mempool transaction processing
        let (tx_sender, tx_receiver) = tokio::sync::mpsc::channel::<ethers::types::Transaction>(1000); // Buffer up to 1000 transactions

        // Spawn transaction receiver that feeds the channel
        let receiver_handle = tokio::spawn(async move {
            while let Ok(tx) = mempool_rx.recv().await {
                // Send transaction to worker pool, drop if channel is full (natural backpressure)
                if let Err(e) = tx_sender.send(tx).await {
                    warn!("Failed to send transaction to processing channel (backpressure): {}", e);
                }
            }
        });
        mempool_handles.push(receiver_handle);

        // Create shared receiver for all workers
        let tx_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));

        // Spawn fixed pool of worker tasks (10 workers)
        for worker_id in 0..10 {
            let opportunity_tx_worker = opportunity_tx_clone.clone();
            let chain_name_worker = chain_name_clone.clone();
            let pool_manager_worker = pool_manager.clone();
            let price_oracle_worker = price_oracle.clone();
            let risk_manager_worker = risk_manager.clone();
            let tx_receiver_worker = tx_receiver.clone();

            let worker_handle = tokio::spawn(async move {
                info!("Started mempool processing worker {} for chain {}", worker_id, chain_name_worker);

                while let Some(tx) = tx_receiver_worker.lock().await.recv().await {
                    match parse_tx_for_mev_opp(tx, &chain_name_worker, pool_manager_worker.clone(), price_oracle_worker.clone(), risk_manager_worker.clone()).await {
                        Ok(Some(opp)) => {
                            if let Err(e) = opportunity_tx_worker.send(opp) {
                                error!("Failed to send mempool opportunity to MEVEngine: {}", e);
                            }
                        }
                        Ok(None) => {} // No opportunity found, continue
                        Err(e) => {
                            error!("Failed to parse mempool tx for MEV opportunity: {}", e);
                        }
                    }
                }

                info!("Mempool processing worker {} for chain {} finished", worker_id, chain_name_worker);
            });
            mempool_handles.push(worker_handle);
        }

        // Log mempool monitor mode
        if cfg.mode == ExecutionMode::PaperTrading {
            info!("Mempool monitor started in read-only mode for chain {} (Paper Trading)", chain_name);
        } else {
            info!("Mempool monitor started for chain {} (Live Trading)", chain_name);
        }

        let cancellation_token = CancellationToken::new();
        
        // Bridge per-chain events to the global broadcast bus
        let (chain_event_tx, mut chain_event_rx) = mpsc::channel::<ChainEvent>(1024);
        let bcast = event_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = chain_event_rx.recv().await {
                let _ = bcast.send(ev);
            }
        });
        
        let chain_config = cfg.chain_config.chains.get(chain_name)
            .ok_or_else(|| BotError::Config(format!("Chain {} not found in configuration", chain_name)))?
            .clone();

        let listener = ChainListener::new(
            chain_config,
            chain_event_tx,
            Arc::new(ProductionProviderFactory::new()),
            cancellation_token,
        )?;
        unified_listener.add_chain_listener(chain_name.clone(), listener);
    }

    unified_listener.start_all().await?;

    tokio::select! {
        _ = signal::ctrl_c() => info!("SIGINT – shutting down"),
    }

    // Graceful shutdown in reverse order
    for (_, mev_engine) in mev_engines {
        mev_engine.stop().await;
    }
    if let Some(alpha_engine) = alpha_engine {
        alpha_engine.stop().await;
    }
    unified_listener.shutdown().await;

    Ok(())
}

async fn run_backtest(cfg: Arc<Config>) -> Result<(), BotError> {
    info!("Starting backtest run...");
    
    let backtest_config = cfg.backtest.as_ref()
        .ok_or_else(|| BacktestError::Config("Backtest configuration not found".to_string()))?;
    
    info!("Backtest config loaded, checking chains...");
    
    let backtest_cfgs: Vec<rust::config::BacktestChainConfig> = cfg.chain_config.chains.iter()
        .filter_map(|(chain_name, _chain_cfg)| {
            info!("Checking chain: {}", chain_name);
            backtest_config.chains.get(chain_name).and_then(|config| {
                if config.enabled {
                    info!("Found ENABLED config for chain: {}", chain_name);
                    Some(rust::config::BacktestChainConfig::from(config.clone()))
                } else {
                    info!("Chain {} is disabled in backtest config", chain_name);
                    None
                }
            })
        })
        .collect();

    info!("Found {} backtest configurations", backtest_cfgs.len());

    if backtest_cfgs.is_empty() {
        return Err(BotError::Backtest(BacktestError::Config("No backtest configurations found".to_string())));
    }

    let mut handles = Vec::new();
    for _chain_cfg in backtest_cfgs {
        let cfg = cfg.clone();
        let handle = tokio::spawn(async move {
            info!("Spawned backtest task starting...");
            match Backtester::new(cfg.clone()).await {
                Ok(backtester) => {
                    info!("Backtester created successfully");
                    match backtester.run().await {
                        Ok(summary) => {
                            info!("Backtest completed successfully");
                            info!("{:?}", summary);
                            Ok::<(), BotError>(())
                        }
                        Err(e) => {
                            error!("Backtester::run() failed: {:?}", e);
                            Err(BotError::Backtest(BacktestError::DataLoad(e.to_string())))
                        }
                    }
                }
                Err(e) => {
                    error!("Backtester::new() failed: {:?}", e);
                    Err(BotError::Backtest(BacktestError::InfraInit(e.to_string())))
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => info!("Backtest task completed successfully"),
            Ok(Err(e)) => error!("Backtest task returned error: {:?}", e),
            Err(e) => error!("Backtest task panicked: {:?}", e),
        }
    }

    Ok(())
}

async fn parse_tx_for_mev_opp(
    tx: ethers::types::Transaction,
    chain_name: &str,
    pool_manager: Arc<dyn rust::pool_manager::PoolManagerTrait + Send + Sync>,
    price_oracle: Arc<dyn rust::price_oracle::PriceOracle + Send + Sync>,
    risk_manager: Arc<dyn rust::risk::RiskManager + Send + Sync>,
) -> Result<Option<MEVOpportunity>, BotError> {
    match rust::mempool::parse_swap_params(&tx) {
        Ok(decoded) => {
            let pools = pool_manager
                .get_pools_for_pair(chain_name, decoded.token_in, decoded.token_out)
                .await
                .map_err(|e| BotError::Infrastructure(format!("PoolManager error: {}", e)))?;

            // No viable pool -> no opportunity
            if pools.is_empty() {
                return Ok(None);
            }

            // Select the best pool based on liquidity and calculate realistic profit estimate
            let mut best_pool = None;
            let mut best_liquidity = 0u128;

            for pool_addr in pools {
                let pool_info = match pool_manager.get_pool_info(chain_name, pool_addr, None).await {
                    Ok(info) => info,
                    Err(e) => {
                        warn!("Failed to get pool info for {}: {}", pool_addr, e);
                        continue;
                    }
                };

                // Calculate realistic profit estimate using dex_math instead of heuristic
                let estimated_output = match rust::dex_math::get_amount_out(&pool_info, decoded.token_in, decoded.amount_in) {
                    Ok(output) => output,
                    Err(e) => {
                        warn!("Failed to calculate amount out for pool {}: {}", pool_addr, e);
                        continue;
                    }
                };

                // Convert amounts to USD for profit calculation
                let amount_in_usd = price_oracle
                    .convert_token_to_usd(chain_name, decoded.amount_in, decoded.token_in)
                    .await
                    .unwrap_or(0.0);

                let amount_out_usd = price_oracle
                    .convert_token_to_usd(chain_name, estimated_output, decoded.token_out)
                    .await
                    .unwrap_or(0.0);

                // Calculate slippage-based profit estimate (more realistic than fixed percentage)
                let slippage_profit_usd = (amount_out_usd - amount_in_usd).max(0.0);

                // Use V2 reserve sum or V3 liquidity as proxy for pool depth
                let pool_liquidity = match pool_info.protocol_type {
                    rust::types::ProtocolType::UniswapV2 => {
                        // For V2, use sum of reserves as liquidity proxy
                        pool_info.v2_reserve0.unwrap_or_default().as_u128()
                            .saturating_add(pool_info.v2_reserve1.unwrap_or_default().as_u128())
                    }
                    rust::types::ProtocolType::UniswapV3 => {
                        // For V3, use liquidity value directly
                        pool_info.v3_liquidity.unwrap_or_default() as u128
                    }
                    _ => 0
                };

                // Select pool with highest liquidity and sufficient profit potential
                if pool_liquidity > best_liquidity && slippage_profit_usd > 0.01 { // Minimum $0.01 profit threshold
                    best_liquidity = pool_liquidity;
                    best_pool = Some((pool_addr, pool_info, slippage_profit_usd));
                }
            }

            let Some((pool_addr, pool_info, estimated_profit_usd)) = best_pool else {
                return Ok(None);
            };

            let sandwich_opportunity = MEVOpportunity::Sandwich(PotentialSandwichOpportunity {
                target_tx_hash: tx.hash,
                victim_tx: tx.clone(),
                swap_params: Some(decoded.clone()),
                block_number: 0,
                estimated_profit_usd,
                gas_price: tx.gas_price.unwrap_or_default(), // OK if EIP-1559 fields absent here
                pool_address: pool_addr,
                token_in: decoded.token_in,
                token_out: decoded.token_out,
                amount_in: decoded.amount_in,
                amount_out_min: decoded.amount_out_min,
                router_address: tx.to.unwrap_or_default(),
                protocol_type: pool_info.protocol_type, // Real protocol from pool metadata
            });

            // Use a strategy-agnostic risk check
            match risk_manager.assess_trade(decoded.token_in, decoded.token_out, decoded.amount_in, None).await {
                Ok(r) if r.is_safe => Ok(Some(sandwich_opportunity)),
                Ok(_) => Ok(None),
                Err(e) => {
                    error!("Risk evaluation failed: {}", e);
                    Ok(None)
                }
            }
        }
        Err(e) => {
            error!("Failed to parse swap params: {}", e);
            Ok(None)
        }
    }
}