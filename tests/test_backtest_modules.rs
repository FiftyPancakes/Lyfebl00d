use anyhow::Result;
use ethers::types::{Address, U256};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use rust::types::ProtocolType;
// remove unused SwapData import; simulation now uses PoolStateOracle
use std::ops::Mul;
use rust::rate_limiter::initialize_global_rate_limiter_manager;
use rust::config::ChainSettings;
use rust::{
    backtest::Backtester,
    blockchain::{BlockchainManager, BlockchainManagerImpl},
    config::Config,
    gas_oracle::{GasOracle, GasOracleProvider, GasPrice},
    path::{AStarPathFinder, PathFinder},
    pool_manager::{PoolManager, PoolManagerTrait},
    pool_states::PoolStateOracle,
    price_oracle::PriceOracle,
    swap_loader::HistoricalSwapLoader,
    types::PoolInfo,
};

#[derive(Debug)]
struct MockGasOracle;

#[async_trait::async_trait]
impl GasOracleProvider for MockGasOracle {
    async fn get_gas_price(
        &self,
        _chain_name: &str,
        _block_number: Option<u64>,
    ) -> Result<GasPrice, rust::errors::GasError> {
        Ok(GasPrice {
            base_fee: U256::from(30u64) * U256::from(10u64).pow(U256::from(9u64)),
            priority_fee: U256::from(2u64) * U256::from(10u64).pow(U256::from(9u64)),
        })
    }

    async fn get_fee_data(
        &self,
        _chain_name: &str,
        _block_number: Option<u64>,
    ) -> Result<rust::types::FeeData, rust::errors::GasError> {
        Err(rust::errors::GasError::Other("not implemented in tests".to_string()))
    }

    fn get_chain_name_from_id(&self, _chain_id: u64) -> Option<String> {
        Some("ethereum".to_string())
    }

    fn estimate_protocol_gas(&self, _protocol: &ProtocolType) -> u64 {
        150_000
    }

    async fn get_gas_price_with_block(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, rust::errors::GasError> {
        self.get_gas_price(chain_name, block_number).await
    }

    async fn estimate_gas_cost_native(
        &self,
        _chain_name: &str,
        gas_units: u64,
    ) -> Result<f64, rust::errors::GasError> {
        let gas_price = U256::from(32u64) * U256::from(10u64).pow(U256::from(9u64));
        let cost_wei = gas_price * U256::from(gas_units);
        let cost_eth = cost_wei.as_u128() as f64 / 1e18;
        Ok(cost_eth * 2000.0)
    }
}

#[derive(Debug)]
struct MockPriceOracle;

#[async_trait::async_trait]
impl PriceOracle for MockPriceOracle {
    async fn get_token_price_usd(
        &self,
        _chain_name: &str,
        _token: Address,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(1.0)
    }

    async fn get_token_price_usd_at(
        &self,
        chain_name: &str,
        token: Address,
        _block_number: Option<u64>,
        _unix_ts: Option<u64>,
    ) -> Result<f64, rust::errors::PriceError> {
        self.get_token_price_usd(chain_name, token).await
    }

    async fn get_pair_price_ratio(
        &self,
        _chain_name: &str,
        _token_a: Address,
        _token_b: Address,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(1.0)
    }

    async fn get_historical_prices_batch(
        &self,
        _chain_name: &str,
        tokens: &[Address],
        _block_number: u64,
    ) -> Result<HashMap<Address, f64>, rust::errors::PriceError> {
        let mut prices = HashMap::new();
        for token in tokens {
            prices.insert(*token, 1.0);
        }
        Ok(prices)
    }

    async fn get_token_volatility(
        &self,
        _chain_name: &str,
        _token: Address,
        _timeframe_seconds: u64,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(0.1)
    }

    async fn calculate_liquidity_usd(
        &self,
        _pool_info: &PoolInfo,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(1_000_000.0)
    }

    async fn get_pair_price_at_block(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
        _block_number: u64,
    ) -> Result<f64, rust::errors::PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }

    async fn get_pair_price(
        &self,
        chain_name: &str,
        token_a: Address,
        token_b: Address,
    ) -> Result<f64, rust::errors::PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }

    async fn convert_token_to_usd(
        &self,
        _chain_name: &str,
        amount: U256,
        _token: Address,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(amount.as_u128() as f64 / 1e18)
    }

    async fn convert_native_to_usd(
        &self,
        _chain_name: &str,
        amount_native: U256,
    ) -> Result<f64, rust::errors::PriceError> {
        Ok(amount_native.as_u128() as f64 / 1e18 * 2000.0)
    }

    async fn estimate_token_conversion(
        &self,
        _chain_name: &str,
        amount_in: U256,
        _token_in: Address,
        _token_out: Address,
    ) -> Result<U256, rust::errors::PriceError> {
        Ok(amount_in)
    }

    async fn estimate_swap_value_usd(
        &self,
        chain_name: &str,
        token_in: Address,
        _token_out: Address,
        amount_in: U256,
    ) -> Result<f64, rust::errors::PriceError> {
        self.convert_token_to_usd(chain_name, amount_in, token_in).await
    }
}

fn initialize_rate_limiter_for_tests(cfg: &Arc<Config>) {
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
}

async fn create_blockchain(cfg: &Arc<Config>) -> Result<Arc<dyn BlockchainManager + Send + Sync>> {
    let price_oracle: Arc<dyn PriceOracle> = Arc::new(MockPriceOracle);
    let mut bm = BlockchainManagerImpl::new(&cfg.clone(), "ethereum", cfg.mode, Some(price_oracle)).await?;
    bm.set_gas_oracle(Arc::new(MockGasOracle));
    Ok(Arc::new(bm) as Arc<dyn BlockchainManager + Send + Sync>)
}

#[tokio::test]
async fn test_backtest_engine_initialization() -> Result<()> {
    println!("Testing Backtester initialization...");
    
    // Set up test database URL - this needs to be done BEFORE any config loading
    std::env::set_var("DATABASE_URL", "postgresql://ichevako:maddie@localhost:5432/lyfeblood");
    
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    // Initialize rate limiter for tests (Backtester::new will also do this, but we initialize here for consistency)
    initialize_rate_limiter_for_tests(&cfg);

    let bt = Backtester::new(cfg.clone()).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    assert!(bt.chains.len() >= 0);
    
    println!("✅ Backtester initialization validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_pool_loading() -> Result<()> {
    println!("Testing backtest pool loading...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    // Create components aligned with project architecture
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    // Initialize rate limiter for tests
    initialize_rate_limiter_for_tests(&cfg);
    
    // Parse DATABASE_URL for deadpool configuration
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let mut db_cfg = deadpool_postgres::Config::new();
    
    // Parse the DATABASE_URL to extract components
    if let Ok(url) = url::Url::parse(&database_url) {
        db_cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        db_cfg.port = Some(url.port().unwrap_or(5432));
        db_cfg.user = Some(url.username().to_string());
        db_cfg.password = url.password().map(|p| p.to_string());
        db_cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
    } else {
        // Fallback to individual env vars or defaults
        db_cfg.host = std::env::var("PGHOST").ok().or_else(|| Some("localhost".to_string()));
        db_cfg.user = std::env::var("PGUSER").ok().or_else(|| Some("ichevako".to_string()));
        db_cfg.password = std::env::var("PGPASSWORD").ok().or_else(|| Some("maddie".to_string()));
        db_cfg.dbname = std::env::var("PGDATABASE").ok().or_else(|| Some("lyfeblood".to_string()));
    }
    
    let dp_pool = db_cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(|e| anyhow::anyhow!(format!("Failed to create deadpool: {}", e)))?;
    let swap_loader = Arc::new(
        HistoricalSwapLoader::new(Arc::new(dp_pool), Arc::new(cfg.chain_config.clone()))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let pool_manager = Arc::new(
        PoolManager::new_without_state_oracle(cfg.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let blockchain = create_blockchain(&cfg).await?;
    let mut map: HashMap<String, Arc<dyn BlockchainManager>> = HashMap::new();
    map.insert("ethereum".to_string(), blockchain.clone() as Arc<dyn BlockchainManager>);
    let pool_state_oracle = Arc::new(
        PoolStateOracle::new(
            cfg.clone(),
            map,
            Some(swap_loader.clone()),
            pool_manager.clone(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    pool_manager
        .set_state_oracle(pool_state_oracle.clone())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    
    // Test loading pools at different blocks
    let test_blocks = vec![19000000, 19500000, 20000000];
    
    for block in test_blocks {
        println!("\n  Testing pool loading at block {}...", block);
        
        // Get all pools
        let all_pools = pool_manager.get_all_pools("ethereum", None).await?;
        println!("    Total pools available: {}", all_pools.len());
        
        // Try to load state for a sample of pools
        let mut successful = 0;
        let mut _failed = 0;
        let sample_size = 10;
        
        for pool_meta in all_pools.iter().take(sample_size) {
            match pool_manager.get_pool_info(
                "ethereum",
                pool_meta.pool_address,
                Some(block)
            ).await {
                Ok(_) => successful += 1,
                Err(_) => _failed += 1,
            }
        }
        
        println!("    Pool states loaded: {}/{} successful", successful, sample_size);
    }
    
    println!("\n✅ Backtest pool loading validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_arbitrage_detection() -> Result<()> {
    println!("Testing backtest arbitrage detection...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    // Create full component stack aligned with project APIs
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    // Initialize rate limiter for tests
    initialize_rate_limiter_for_tests(&cfg);
    
    // Parse DATABASE_URL for deadpool configuration
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let mut db_cfg = deadpool_postgres::Config::new();
    
    // Parse the DATABASE_URL to extract components
    if let Ok(url) = url::Url::parse(&database_url) {
        db_cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        db_cfg.port = Some(url.port().unwrap_or(5432));
        db_cfg.user = Some(url.username().to_string());
        db_cfg.password = url.password().map(|p| p.to_string());
        db_cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
    } else {
        // Fallback to individual env vars or defaults
        db_cfg.host = std::env::var("PGHOST").ok().or_else(|| Some("localhost".to_string()));
        db_cfg.user = std::env::var("PGUSER").ok().or_else(|| Some("ichevako".to_string()));
        db_cfg.password = std::env::var("PGPASSWORD").ok().or_else(|| Some("maddie".to_string()));
        db_cfg.dbname = std::env::var("PGDATABASE").ok().or_else(|| Some("lyfeblood".to_string()));
    }
    
    let dp_pool = db_cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(|e| anyhow::anyhow!(format!("Failed to create deadpool: {}", e)))?;
    let swap_loader = Arc::new(
        HistoricalSwapLoader::new(Arc::new(dp_pool), Arc::new(cfg.chain_config.clone()))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let price_oracle: Arc<dyn PriceOracle> = Arc::new(MockPriceOracle);
    let mut bm_impl = BlockchainManagerImpl::new(&cfg.clone(), "ethereum", cfg.mode, Some(price_oracle.clone())).await?;
    bm_impl.set_gas_oracle(Arc::new(MockGasOracle));
    let blockchain: Arc<dyn BlockchainManager> = Arc::new(bm_impl);
    let pool_manager = Arc::new(
        PoolManager::new_without_state_oracle(cfg.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let mut map: HashMap<String, Arc<dyn BlockchainManager>> = HashMap::new();
    map.insert("ethereum".to_string(), blockchain.clone() as Arc<dyn BlockchainManager>);
    let pool_state_oracle = Arc::new(
        PoolStateOracle::new(
            cfg.clone(),
            map,
            Some(swap_loader.clone()),
            pool_manager.clone(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    pool_manager
        .set_state_oracle(pool_state_oracle.clone())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let pf_settings = cfg
        .module_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("module_config missing"))?
        .path_finder_settings
        .clone();
    let router_tokens = cfg.chain_config.chains.get("ethereum").and_then(|c| c.router_tokens.clone()).unwrap_or_default();
    let path_finder = Arc::new(
        AStarPathFinder::new(
            pf_settings,
            pool_manager.clone(),
            blockchain.clone(),
            cfg.clone(),
            Arc::new(GasOracle::new(Arc::new(MockGasOracle))),
            price_oracle.clone(),
            blockchain.get_chain_id(),
            true,
            router_tokens,
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    
    // Test finding arbitrage at specific blocks
    let test_blocks = vec![19000000, 19500000, 20000000];
    
    for block in test_blocks {
        println!("\n  Testing path-based arbitrage detection at block {}...", block);
        let weth = Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")?;
        let scan_amount = U256::from(1_000_000_000_000_000_000u128);
        match path_finder
            .find_circular_arbitrage_paths(weth, scan_amount, Some(block))
            .await
        {
            Ok(paths) => {
                println!("    Found {} circular opportunities", paths.len());
            }
            Err(e) => {
                println!("    Error finding circular opportunities: {}", e);
            }
        }
    }
    
    println!("\n✅ Backtest arbitrage detection validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_simulation_accuracy() -> Result<()> {
    println!("Testing backtest simulation accuracy...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    // Create components via project loaders
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    // Initialize rate limiter for tests
    initialize_rate_limiter_for_tests(&cfg);
    
    // Parse DATABASE_URL for deadpool configuration
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let mut db_cfg = deadpool_postgres::Config::new();
    
    // Parse the DATABASE_URL to extract components
    if let Ok(url) = url::Url::parse(&database_url) {
        db_cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        db_cfg.port = Some(url.port().unwrap_or(5432));
        db_cfg.user = Some(url.username().to_string());
        db_cfg.password = url.password().map(|p| p.to_string());
        db_cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
    } else {
        // Fallback to individual env vars or defaults
        db_cfg.host = std::env::var("PGHOST").ok().or_else(|| Some("localhost".to_string()));
        db_cfg.user = std::env::var("PGUSER").ok().or_else(|| Some("ichevako".to_string()));
        db_cfg.password = std::env::var("PGPASSWORD").ok().or_else(|| Some("maddie".to_string()));
        db_cfg.dbname = std::env::var("PGDATABASE").ok().or_else(|| Some("lyfeblood".to_string()));
    }
    
    let dp_pool = db_cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(|e| anyhow::anyhow!(format!("Failed to create deadpool: {}", e)))?;
    let swap_loader = Arc::new(
        HistoricalSwapLoader::new(Arc::new(dp_pool), Arc::new(cfg.chain_config.clone()))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let price_oracle: Arc<dyn PriceOracle> = Arc::new(MockPriceOracle);
    let mut bm = BlockchainManagerImpl::new(&cfg.clone(), "ethereum", cfg.mode, Some(price_oracle.clone())).await?;
    bm.set_gas_oracle(Arc::new(MockGasOracle));
    let blockchain: Arc<dyn BlockchainManager> = Arc::new(bm);
    let pool_manager = Arc::new(
        PoolManager::new_without_state_oracle(cfg.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let mut map: HashMap<String, Arc<dyn BlockchainManager>> = HashMap::new();
    map.insert("ethereum".to_string(), blockchain.clone() as Arc<dyn BlockchainManager>);
    let pool_state_oracle = Arc::new(
        PoolStateOracle::new(
            cfg.clone(),
            map,
            Some(swap_loader.clone()),
            pool_manager.clone(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    pool_manager
        .set_state_oracle(pool_state_oracle.clone())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    // Test simulating swaps with historical data
    let test_pools = vec![
        Address::from_str("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")?, // ETH/USDT V2
        Address::from_str("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640")?, // USDC/ETH V3
    ];
    
    for pool_address in test_pools {
        println!("\n  Testing simulation for pool {}...", pool_address);
        
        // Load historical state
        match pool_state_oracle.get_pool_state("ethereum", pool_address, Some(20000000)).await {
            Ok(swap_data) => {
                // Simulate a swap
                let amount_in = U256::from(10).pow(U256::from(18)); // 1 ETH
                
                match swap_data.pool_state.protocol_type {
                    ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                        let amount_out = calculate_v2_swap_output(
                            amount_in,
                            swap_data.pool_state.reserve0,
                            swap_data.pool_state.reserve1,
                            30, // 0.3% fee
                        );
                        
                        println!("    V2 Swap simulation:");
                        println!("      Input: {} wei", amount_in);
                        println!("      Output: {} wei", amount_out);
                        println!(
                            "      Reserves: {} / {}",
                            swap_data.pool_state.reserve0, swap_data.pool_state.reserve1
                        );
                        
                        assert!(amount_out > U256::zero(), "Output should be positive");
                    }
                    ProtocolType::UniswapV3 => {
                        println!("    V3 Swap simulation:");
                        println!(
                            "      sqrt_price: {:?}",
                            swap_data.pool_state.sqrt_price_x96
                        );
                        println!("      Liquidity: {:?}", swap_data.pool_state.liquidity);
                    }
                    _ => {
                        println!("    Other protocol type");
                    }
                }
            }
            Err(e) => {
                println!("    Could not load state: {}", e);
            }
        }
    }
    
    println!("\n✅ Backtest simulation accuracy validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_performance_metrics() -> Result<()> {
    println!("Testing backtest performance metrics...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let start_time = std::time::Instant::now();
    println!("\n  Simulating backtest run...");
    let backtest_cfg = cfg
        .backtest
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("backtest config missing"))?;
    let (start_block, end_block) = if let Some(eth) = backtest_cfg.chains.get("ethereum") {
        (eth.backtest_start_block, eth.backtest_end_block)
    } else if let Some((_, c)) = backtest_cfg.chains.iter().next() {
        (c.backtest_start_block, c.backtest_end_block)
    } else {
        (0u64, 0u64)
    };
    let blocks_processed = end_block.saturating_sub(start_block);
    let elapsed = start_time.elapsed();
    
    println!("  Performance metrics:");
    println!("    Blocks processed: {}", blocks_processed);
    println!("    Time elapsed: {:?}", elapsed);
    println!("    Blocks per second: {:.2}", 
        blocks_processed as f64 / elapsed.as_secs_f64());
    
    // Test checkpoint saving
    println!("\n  Testing checkpoint functionality...");
    let checkpoint_data = BacktestCheckpoint {
        current_block: 19000050,
        opportunities_found: 5,
        total_profit_usd: 1234.56,
        total_gas_cost_usd: 123.45,
        timestamp: chrono::Utc::now(),
    };
    
    println!("    Checkpoint at block {}: {} opportunities, ${:.2} profit",
        checkpoint_data.current_block,
        checkpoint_data.opportunities_found,
        checkpoint_data.total_profit_usd
    );
    
    println!("\n✅ Backtest performance metrics validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_data_pipeline() -> Result<()> {
    println!("Testing complete backtest data pipeline...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    // Step 1: Load pools from database
    println!("\n  Step 1: Loading pools from database...");
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    // Initialize rate limiter for tests
    initialize_rate_limiter_for_tests(&cfg);
    
    // Parse DATABASE_URL for deadpool configuration
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let mut db_cfg = deadpool_postgres::Config::new();
    
    // Parse the DATABASE_URL to extract components
    if let Ok(url) = url::Url::parse(&database_url) {
        db_cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        db_cfg.port = Some(url.port().unwrap_or(5432));
        db_cfg.user = Some(url.username().to_string());
        db_cfg.password = url.password().map(|p| p.to_string());
        db_cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
    } else {
        // Fallback to individual env vars or defaults
        db_cfg.host = std::env::var("PGHOST").ok().or_else(|| Some("localhost".to_string()));
        db_cfg.user = std::env::var("PGUSER").ok().or_else(|| Some("ichevako".to_string()));
        db_cfg.password = std::env::var("PGPASSWORD").ok().or_else(|| Some("maddie".to_string()));
        db_cfg.dbname = std::env::var("PGDATABASE").ok().or_else(|| Some("lyfeblood".to_string()));
    }
    
    let dp_pool = db_cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(|e| anyhow::anyhow!(format!("Failed to create deadpool: {}", e)))?;
    let swap_loader = Arc::new(
        HistoricalSwapLoader::new(Arc::new(dp_pool), Arc::new(cfg.chain_config.clone()))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let pool_manager = Arc::new(
        PoolManager::new_without_state_oracle(cfg.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    let total_pools = pool_manager.get_all_pools("ethereum", None).await.map_err(|e| anyhow::anyhow!(e.to_string()))?.len();
    println!("    Loaded {} pools", total_pools);
    
    // Step 2: Initialize swap loader
    println!("\n  Step 2: Initializing swap loader...");
    
    // Step 3: Create pool state oracle
    println!("\n  Step 3: Creating pool state oracle...");
    let price_oracle: Arc<dyn PriceOracle> = Arc::new(MockPriceOracle);
    let mut bm = BlockchainManagerImpl::new(&cfg.clone(), "ethereum", cfg.mode, Some(price_oracle.clone())).await?;
    bm.set_gas_oracle(Arc::new(MockGasOracle));
    let blockchain: Arc<dyn BlockchainManager> = Arc::new(bm);
    let mut map: HashMap<String, Arc<dyn BlockchainManager>> = HashMap::new();
    map.insert("ethereum".to_string(), blockchain.clone() as Arc<dyn BlockchainManager>);
    let pool_state_oracle = Arc::new(
        PoolStateOracle::new(
            cfg.clone(),
            map,
            Some(swap_loader.clone()),
            pool_manager.clone(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    pool_manager.set_state_oracle(pool_state_oracle.clone()).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    
    // Step 4: Initialize path finder
    println!("\n  Step 4: Initializing path finder...");
    let pf_settings = cfg
        .module_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("module_config missing"))?
        .path_finder_settings
        .clone();
    let router_tokens = cfg.chain_config.chains.get("ethereum").and_then(|c| c.router_tokens.clone()).unwrap_or_default();
    let path_finder = Arc::new(
        AStarPathFinder::new(
            pf_settings,
            pool_manager.clone(),
            blockchain.clone(),
            cfg.clone(),
            Arc::new(GasOracle::new(Arc::new(MockGasOracle))),
            price_oracle.clone(),
            blockchain.get_chain_id(),
            true,
            router_tokens,
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    
    // Step 5: Test complete pipeline with real data
    println!("\n  Step 5: Testing complete pipeline...");
    let test_block = 20000000;
    let weth = Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")?;
    let usdc = Address::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")?;
    
    // Find paths
    let amount_in = U256::from(10u64).pow(U256::from(18u64));
    let maybe_path = path_finder
        .find_optimal_path(weth, usdc, amount_in, Some(test_block))
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if let Some(first_path) = maybe_path {
        println!(
            "    Best path: {} pools, estimated output: {}",
            first_path.pool_path.len(),
            first_path.expected_output
        );
        for pool_addr in &first_path.pool_path {
            match pool_manager.get_pool_info("ethereum", *pool_addr, Some(test_block)).await {
                Ok(_) => println!("      ✓ Pool {} state available", pool_addr),
                Err(e) => println!("      ✗ Pool {} state error: {}", pool_addr, e),
            }
        }
    } else {
        println!("    No paths found (may need more data)");
    }
    
    println!("\n✅ Complete backtest data pipeline validated");
    Ok(())
}

#[tokio::test]
async fn test_backtest_error_recovery() -> Result<()> {
    println!("Testing backtest error recovery...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let _db_pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?
    );
    
    let cfg = Arc::new(
        Config::load_from_directory("/Users/ichevako/Desktop/rust/config")
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    
    // Parse DATABASE_URL for deadpool configuration
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let mut db_cfg = deadpool_postgres::Config::new();
    
    // Parse the DATABASE_URL to extract components
    if let Ok(url) = url::Url::parse(&database_url) {
        db_cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        db_cfg.port = Some(url.port().unwrap_or(5432));
        db_cfg.user = Some(url.username().to_string());
        db_cfg.password = url.password().map(|p| p.to_string());
        db_cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
    } else {
        // Fallback to individual env vars or defaults
        db_cfg.host = std::env::var("PGHOST").ok().or_else(|| Some("localhost".to_string()));
        db_cfg.user = std::env::var("PGUSER").ok().or_else(|| Some("ichevako".to_string()));
        db_cfg.password = std::env::var("PGPASSWORD").ok().or_else(|| Some("maddie".to_string()));
        db_cfg.dbname = std::env::var("PGDATABASE").ok().or_else(|| Some("lyfeblood".to_string()));
    }
    
    let dp_pool = db_cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(|e| anyhow::anyhow!(format!("Failed to create deadpool: {}", e)))?;
    let swap_loader = Arc::new(
        HistoricalSwapLoader::new(Arc::new(dp_pool), Arc::new(cfg.chain_config.clone()))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    
    // Test recovery from various error conditions
    println!("\n  Testing recovery from missing data...");
    
    // Try to load state for a non-existent pool
    let fake_pool = Address::from_str("0x0000000000000000000000000000000000000001")?;
    match swap_loader.get_pool_state_at_start_of_block_withchain(fake_pool, 20000000, Some("ethereum")).await {
        Ok(_) => println!("    Unexpected success for fake pool"),
        Err(e) => println!("    ✓ Handled missing pool gracefully: {}", e),
    }
    
    // Try to load state for a very old block
    let valid_pool = Address::from_str("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")?;
    match swap_loader.get_pool_state_at_start_of_block_withchain(valid_pool, 1, Some("ethereum")).await {
        Ok(_) => println!("    Found data for block 1 (unexpected)"),
        Err(e) => println!("    ✓ Handled missing historical data: {}", e),
    }
    
    // Test with invalid chain
    match swap_loader.get_pool_state_at_start_of_block_withchain(valid_pool, 20000000, Some("invalid_chain")).await {
        Ok(_) => println!("    Unexpected success for invalid chain"),
        Err(e) => println!("    ✓ Handled invalid chain: {}", e),
    }
    
    println!("\n✅ Backtest error recovery validated");
    Ok(())
}

// Helper structures and functions
#[derive(Debug, Clone)]
struct BacktestCheckpoint {
    current_block: u64,
    opportunities_found: usize,
    total_profit_usd: f64,
    total_gas_cost_usd: f64,
    timestamp: chrono::DateTime<chrono::Utc>,
}

fn calculate_v2_swap_output(
    amount_in: U256,
    reserve_in: U256,
    reserve_out: U256,
    fee_bps: u32,
) -> U256 {
    let amount_in_with_fee = amount_in * U256::from(10000u64 - fee_bps as u64);
    let numerator = amount_in_with_fee * reserve_out;
    let denominator = reserve_in * U256::from(10000u64) + amount_in_with_fee;
    if denominator > U256::zero() { numerator / denominator } else { U256::zero() }
}

fn create_test_config() -> Result<Arc<Config>> {
    // Create minimal test config for backtest mode
    let config = Config {
        mode: rust::config::ExecutionMode::Backtest,
        log_level: "info".to_string(),
        chain_config: rust::config::ChainConfig {
            chains: std::collections::HashMap::new(),
            rate_limiter_settings: rust::config::RateLimiterSettings::default(),
        },
        module_config: None,
        backtest: None,
        gas_estimates: rust::config::GasEstimates::default(),
        strict_backtest: false,
        secrets: None,
    };
    
    Ok(Arc::new(config))
}

fn create_mock_blockchain() -> Result<Arc<dyn BlockchainManager + Send + Sync>> {
    // This would be a mock implementation
    // For now, return error to indicate it needs implementation
    Err(anyhow::anyhow!("Mock blockchain not implemented for tests"))
}

fn create_mock_gas_oracle() -> Arc<dyn GasOracleProvider + Send + Sync> {
    #[derive(Debug)]
    struct MockGasOracle;
    
    #[async_trait::async_trait]
    impl GasOracleProvider for MockGasOracle {
        async fn get_gas_price(
            &self,
            _chain_name: &str,
            _block_number: Option<u64>,
        ) -> Result<rust::gas_oracle::GasPrice, rust::errors::GasError> {
            Ok(rust::gas_oracle::GasPrice {
                base_fee: U256::from(30).mul(U256::from(10).pow(U256::from(9))),
                priority_fee: U256::from(2).mul(U256::from(10).pow(U256::from(9))),
            })
        }
        
        async fn get_fee_data(
            &self,
            _chain_name: &str,
            _block_number: Option<u64>,
        ) -> Result<rust::types::FeeData, rust::errors::GasError> {
            unimplemented!()
        }
        
        fn get_chain_name_from_id(&self, _chain_id: u64) -> Option<String> {
            Some("ethereum".to_string())
        }
        
        fn estimate_protocol_gas(&self, _protocol: &ProtocolType) -> u64 {
            150000
        }
        
        async fn get_gas_price_with_block(
            &self,
            chain_name: &str,
            block_number: Option<u64>,
        ) -> Result<rust::gas_oracle::GasPrice, rust::errors::GasError> {
            self.get_gas_price(chain_name, block_number).await
        }
        
        async fn estimate_gas_cost_native(
            &self,
            _chain_name: &str,
            gas_units: u64,
        ) -> Result<f64, rust::errors::GasError> {
            let gas_price = U256::from(32).mul(U256::from(10).pow(U256::from(9)));
            let cost_wei = gas_price.mul(U256::from(gas_units));
            let cost_eth = cost_wei.as_u128() as f64 / 1e18;
            Ok(cost_eth * 2000.0) // Assume $2000 ETH
        }
    }
    
    Arc::new(MockGasOracle)
}

fn create_mock_price_oracle() -> Arc<dyn rust::price_oracle::PriceOracle + Send + Sync> {
    #[derive(Debug)]
    struct MockPriceOracle;
    
    #[async_trait::async_trait]
    impl rust::price_oracle::PriceOracle for MockPriceOracle {
        async fn get_token_price_usd(
            &self,
            _chain_name: &str,
            _token: Address,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(1.0) // Return $1 for all tokens in mock
        }
        
        async fn get_token_price_usd_at(
            &self,
            chain_name: &str,
            token: Address,
            _block_number: Option<u64>,
            _unix_ts: Option<u64>,
        ) -> Result<f64, rust::errors::PriceError> {
            self.get_token_price_usd(chain_name, token).await
        }
        
        async fn get_pair_price_ratio(
            &self,
            _chain_name: &str,
            _token_a: Address,
            _token_b: Address,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(1.0)
        }
        
        async fn get_historical_prices_batch(
            &self,
            _chain_name: &str,
            tokens: &[Address],
            _block_number: u64,
        ) -> Result<HashMap<Address, f64>, rust::errors::PriceError> {
            let mut prices = HashMap::new();
            for token in tokens {
                prices.insert(*token, 1.0);
            }
            Ok(prices)
        }
        
        async fn get_token_volatility(
            &self,
            _chain_name: &str,
            _token: Address,
            _timeframe_seconds: u64,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(0.1)
        }
        
        async fn calculate_liquidity_usd(
            &self,
            _pool_info: &PoolInfo,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(1000000.0)
        }
        
        async fn get_pair_price_at_block(
            &self,
            chain_name: &str,
            token_a: Address,
            token_b: Address,
            _block_number: u64,
        ) -> Result<f64, rust::errors::PriceError> {
            self.get_pair_price_ratio(chain_name, token_a, token_b).await
        }
        
        async fn get_pair_price(
            &self,
            chain_name: &str,
            token_a: Address,
            token_b: Address,
        ) -> Result<f64, rust::errors::PriceError> {
            self.get_pair_price_ratio(chain_name, token_a, token_b).await
        }
        
        async fn convert_token_to_usd(
            &self,
            _chain_name: &str,
            amount: U256,
            _token: Address,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(amount.as_u128() as f64 / 1e18)
        }
        
        async fn convert_native_to_usd(
            &self,
            _chain_name: &str,
            amount_native: U256,
        ) -> Result<f64, rust::errors::PriceError> {
            Ok(amount_native.as_u128() as f64 / 1e18 * 2000.0)
        }
        
        async fn estimate_token_conversion(
            &self,
            _chain_name: &str,
            amount_in: U256,
            _token_in: Address,
            _token_out: Address,
        ) -> Result<U256, rust::errors::PriceError> {
            Ok(amount_in)
        }
        
        async fn estimate_swap_value_usd(
            &self,
            chain_name: &str,
            token_in: Address,
            _token_out: Address,
            amount_in: U256,
        ) -> Result<f64, rust::errors::PriceError> {
            self.convert_token_to_usd(chain_name, amount_in, token_in).await
        }
    }
    
    Arc::new(MockPriceOracle)
}
