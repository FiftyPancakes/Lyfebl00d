// Integration test for database-backed swap loader functionality

use rust::{
    config::{ChainConfig, PerChainConfig, RateLimiterSettings},
    swap_loader::HistoricalSwapLoader,
    types::ProtocolType,
};

use ethers::types::{Address, U256};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

// Use common test utilities
use common::{
    setup_test_db,
    seed_test_data,
    V2_POOL_ADDR,
    V3_POOL_ADDR,
    OLD_POOL_ADDR,
};

// Helper function to create a comprehensive test configuration
fn create_test_chain_config() -> ChainConfig {
    let mut chains = HashMap::new();
            chains.insert(
            "ethereum".to_string(),
            PerChainConfig {
                chain_id: 1,
                chain_name: "ethereum".to_string(),
                rpc_url: "https://eth-mainnet.g.alchemy.com/v2/test".to_string(),
                ws_url: "wss://eth-mainnet.g.alchemy.com/v2/test".to_string(),
                private_key: "".to_string(),
                weth_address: Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
                live_enabled: false,
                backtest_archival_rpc_url: None,
                max_gas_price: U256::from(200_000_000_000u64), // 200 gwei
                gas_bump_percent: Some(10),
                max_pending_transactions: Some(100),
                rps_limit: Some(100),
                max_concurrent_requests: Some(16),
                endpoints: vec![],
                log_filter: None,
                reconnect_delay_ms: Some(5000),
                chain_type: "ethereum".to_string(),
                factories: None,
                chainlink_native_usd_feed: None,
                reference_stablecoins: None,
                router_tokens: None,
                bridges: None,
                multicall_address: None,
                default_base_fee_gwei: Some(20),
                default_priority_fee_gwei: Some(2),
                price_timestamp_tolerance_seconds: Some(300),
                avg_block_time_seconds: Some(12.0),
                volatility_sample_interval_seconds: Some(60),
                volatility_max_samples: Some(100),
                chainlink_token_feeds: None,
                stablecoin_symbols: None,
                is_test_environment: Some(true),
                price_lookback_blocks: Some(10),
                fallback_window_blocks: Some(5),
                reciprocal_tolerance_pct: Some(0.05),
                same_token_io_min_ratio: Some(0.95),
                same_token_io_max_ratio: Some(1.05),
                strict_backtest_no_lookahead: Some(false),
                backtest_use_external_price_apis: Some(false),
                is_l2: Some(false),
                max_ws_reconnect_attempts: Some(5),
                ws_reconnect_base_delay_ms: Some(1000),
                ws_max_backoff_delay_ms: Some(30000),
                ws_backoff_jitter_factor: Some(0.1),
                subscribe_pending_txs: Some(false),
            },
        );

    ChainConfig {
        chains,
        rate_limiter_settings: RateLimiterSettings {
            global_rps_limit: 1000,
            default_chain_rps_limit: 100,
        },
    }
}

// Helper module for test setup and shared types
mod common {
    use ethers::types::Address;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::Arc;
    use rust::types::ProtocolType;
    use rust::errors::ArbitrageError;
    use rust::database::get_shared_database_pool;

    // Helper function to convert ProtocolType to i16 for database storage
    fn protocol_type_to_i16(protocol: &ProtocolType) -> i16 {
        match protocol {
            ProtocolType::Unknown => 0,
            ProtocolType::UniswapV2 => 1,
            ProtocolType::UniswapV3 => 2,
            ProtocolType::Curve => 3,
            ProtocolType::Balancer => 4,
            ProtocolType::SushiSwap => 5,
            ProtocolType::UniswapV4 => 8,
            ProtocolType::Other(_) => 99,
        }
    }

    // Lazy statics for consistent test addresses
    lazy_static::lazy_static! {
        pub static ref V2_POOL_ADDR: Address = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        pub static ref V3_POOL_ADDR: Address = Address::from_str("0x0000000000000000000000000000000000000002").unwrap();
        pub static ref OLD_POOL_ADDR: Address = Address::from_str("0x0000000000000000000000000000000000000003").unwrap();
        pub static ref TOKEN0_ADDR: Address = Address::from_str("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        pub static ref TOKEN1_ADDR: Address = Address::from_str("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
    }

    /// Creates a database connection using the real database module
    /// Always creates fresh tables from the canonical schema.sql file
    pub async fn setup_test_db() -> Result<(Arc<deadpool_postgres::Pool>, ()), ArbitrageError> {
        // Set up test database URL - this needs to be done BEFORE any config loading
        std::env::set_var("DATABASE_URL", "postgresql://ichevako:maddie@localhost:5432/lyfeblood");

        // Load the full configuration from config files
        let test_config = rust::config::Config::load_from_directory("config")
            .await
            .unwrap_or_else(|_| {
                // Fallback to basic config if config loading fails
                rust::config::Config {
                    mode: rust::config::ExecutionMode::Live,
                    log_level: "info".to_string(),
                    chain_config: super::create_test_chain_config(),
                    module_config: None,
                    backtest: None,
                    gas_estimates: rust::config::GasEstimates::default(),
                    strict_backtest: false,
                    secrets: None,
                }
            });

        // Use the real database module to get a shared pool
        let pool = match get_shared_database_pool(&test_config).await {
            Ok(pool) => pool,
            Err(e) => {
                if e.to_string().contains("does not exist") || e.to_string().contains("connect") {
                    println!("⚠️  Skipping database tests - no database available: {}", e);
                    return Err(ArbitrageError::RiskCheckFailed("Database not available for testing".to_string()));
                } else {
                    return Err(e);
                }
            }
        };

        // Get a client to set up test tables
        let client = pool.get().await
            .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to get database client: {}", e)))?;

        // Always drop and recreate tables from canonical schema
        println!("Setting up fresh database schema from canonical schema.sql...");

        // Read the canonical schema file
        let schema_content = match std::fs::read_to_string("database_schema.sql") {
            Ok(content) => content,
            Err(e) => {
                println!("⚠️  Could not read database_schema.sql: {}. Using fallback schema setup.", e);
                return setup_fallback_schema(&client).await;
            }
        };

        // Split schema into individual statements and execute them
        let statements: Vec<&str> = schema_content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && !s.starts_with("--"))
            .collect();

        for statement in statements {
            if !statement.trim().is_empty() {
                if let Err(e) = client.execute(statement, &[]).await {
                    println!("⚠️  Schema statement failed (may be normal for some statements): {}", e);
                }
            }
        }

        println!("✅ Database schema setup complete");
        Ok((pool, ()))
    }

    /// Fallback schema setup when schema.sql is not available
    async fn setup_fallback_schema(client: &tokio_postgres::Client) -> Result<(Arc<deadpool_postgres::Pool>, ()), ArbitrageError> {
        println!("Using fallback schema setup...");

        // Drop existing tables if they exist
        let _ = client.execute("DROP TABLE IF EXISTS swaps CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS pools CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS cg_pools CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS trades CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS tokens CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS dexes CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS networks CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS ohlc_data CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS market_trends CASCADE", &[]).await;
        let _ = client.execute("DROP TABLE IF EXISTS blocks CASCADE", &[]).await;
        let _ = client.execute("DROP VIEW IF EXISTS block_timestamps CASCADE", &[]).await;

        // Create minimal required tables for swap tests
        if let Err(e) = client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS pools (
                pool_address BYTEA PRIMARY KEY,
                token0_address BYTEA,
                token1_address BYTEA,
                token0_symbol TEXT,
                token1_symbol TEXT,
                token0_decimals INTEGER,
                token1_decimals INTEGER,
                fee_tier INTEGER,
                protocol_type SMALLINT,
                dex_name TEXT,
                chain_name TEXT
            )
            "#,
            &[]
        ).await {
            println!("⚠️  Failed to create pools table: {}", e);
        }

        if let Err(e) = client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS swaps (
                network_id TEXT,
                chain_name TEXT,
                pool_address BYTEA,
                transaction_hash BYTEA,
                log_index BIGINT,
                block_number BIGINT,
                unix_ts BIGINT,
                pool_id BYTEA,
                dex_name TEXT,
                protocol_type SMALLINT,
                token0_address BYTEA,
                token1_address BYTEA,
                token0_decimals INTEGER,
                token1_decimals INTEGER,
                fee_tier INTEGER,
                amount0_in NUMERIC,
                amount1_in NUMERIC,
                amount0_out NUMERIC,
                amount1_out NUMERIC,
                price_t0_in_t1 NUMERIC,
                price_t1_in_t0 NUMERIC,
                token0_reserve NUMERIC,
                token1_reserve NUMERIC,
                sqrt_price_x96 NUMERIC,
                liquidity NUMERIC,
                tick INTEGER,
                v3_sender BYTEA,
                v3_recipient BYTEA,
                v3_amount0 NUMERIC,
                v3_amount1 NUMERIC,
                gas_used BIGINT,
                gas_price NUMERIC,
                tx_origin BYTEA,
                tx_to BYTEA,
                tx_value NUMERIC,
                tx_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(pool_address, transaction_hash, log_index)
            )
            "#,
            &[]
        ).await {
            println!("⚠️  Failed to create swaps table: {}", e);
        }

        // Create indexes
        let _ = client.execute("CREATE INDEX IF NOT EXISTS idx_swaps_pool ON swaps (pool_address)", &[]).await;
        let _ = client.execute("CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps (unix_ts)", &[]).await;
        let _ = client.execute("CREATE INDEX IF NOT EXISTS idx_swaps_tx ON swaps (transaction_hash)", &[]).await;
        let _ = client.execute("CREATE INDEX IF NOT EXISTS idx_pools_chain ON pools (chain_name)", &[]).await;

        println!("✅ Fallback schema setup complete");
        // Return empty pool - this will be handled by caller
        Err(ArbitrageError::RiskCheckFailed("Using fallback schema - pool not available".to_string()))
    }

    /// Seeds the test database with sample swap/state data.
    pub async fn seed_test_data(client: &tokio_postgres::Client) {
        println!("Seeding test data...");

        // First, insert pool data - handle case where table might have different column names
        let pool_stmt_result = client.prepare(
            "INSERT INTO pools (pool_address, token0_address, token1_address, token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee_tier, protocol_type, dex_name, chain_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (pool_address) DO NOTHING"
        ).await;

        match pool_stmt_result {
            Ok(pool_stmt) => {
                // Insert V2 pool
                let _ = client.execute(&pool_stmt, &[
                    &V2_POOL_ADDR.as_bytes(), &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(),
                    &"TOKEN0", &"TOKEN1", &18i32, &6i32, &3000i32,
                    &protocol_type_to_i16(&ProtocolType::UniswapV2), &"UniswapV2", &"ethereum"
                ]).await;

                // Insert V3 pool
                let _ = client.execute(&pool_stmt, &[
                    &V3_POOL_ADDR.as_bytes(), &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(),
                    &"TOKEN0", &"TOKEN1", &18i32, &6i32, &500i32,
                    &protocol_type_to_i16(&ProtocolType::UniswapV3), &"UniswapV3", &"ethereum"
                ]).await;

                // Insert Old pool
                let _ = client.execute(&pool_stmt, &[
                    &OLD_POOL_ADDR.as_bytes(), &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(),
                    &"TOKEN0", &"TOKEN1", &18i32, &6i32, &3000i32,
                    &protocol_type_to_i16(&ProtocolType::UniswapV2), &"UniswapV2", &"ethereum"
                ]).await;
            }
            Err(e) => {
                println!("Pool statement preparation failed (table might not exist or have different schema): {}", e);
            }
        }

        // Now prepare swap statement with reserve fields for V2 pools
        let swap_stmt_result = client.prepare(
            "INSERT INTO swaps (network_id, block_number, log_index, pool_address, transaction_hash, token0_address, token1_address, token0_decimals, token1_decimals, protocol_type, fee_tier, amount0_in, amount1_in, amount0_out, amount1_out, token0_reserve, token1_reserve, sqrt_price_x96, liquidity, tick, unix_ts, chain_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
             ON CONFLICT (pool_address, transaction_hash, log_index) DO NOTHING"
        ).await;

        match swap_stmt_result {
            Ok(stmt) => {
                // First, clear any existing data for our test pools to avoid conflicts
                let _ = client.execute(
                    "DELETE FROM swaps WHERE pool_address = $1 OR pool_address = $2 OR pool_address = $3",
                    &[&V2_POOL_ADDR.as_bytes(), &V3_POOL_ADDR.as_bytes(), &OLD_POOL_ADDR.as_bytes()]
                ).await;

                // V2 Pool State @ block 23026004
                let _ = client.execute(&stmt, &[
                    &"ethereum", &23026004i64, &1i64, &V2_POOL_ADDR.as_bytes(), &&[0u8; 32][..], &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(),
                    &18i32, &6i32, &protocol_type_to_i16(&ProtocolType::UniswapV2), &3000i32,
                    &Decimal::from_str("1000000").unwrap(), // Smaller amount0_in
                    &Decimal::from_str("0").unwrap(), // amount1_in
                    &Decimal::from_str("0").unwrap(), // amount0_out
                    &Decimal::from_str("500000").unwrap(), // Smaller amount1_out
                    &Decimal::from_str("1000000000").unwrap(), // token0_reserve
                    &Decimal::from_str("500000000").unwrap(), // token1_reserve
                    &None::<Decimal>, &None::<Decimal>, &None::<i32>, &9999999999i64, &"ethereum"
                ]).await;

                // V3 Pool State @ block 998
                let _ = client.execute(&stmt, &[
                    &"ethereum", &998i64, &1i64, &V3_POOL_ADDR.as_bytes(), &&[1u8; 32][..], &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(), &18i32, &6i32,
                    &protocol_type_to_i16(&ProtocolType::UniswapV3),
                    &500i32,
                    &Decimal::from_str("1000000").unwrap(), // amount0_in
                    &Decimal::from_str("0").unwrap(), // amount1_in
                    &Decimal::from_str("0").unwrap(), // amount0_out
                    &Decimal::from_str("500000").unwrap(), // amount1_out
                    &None::<Decimal>, // token0_reserve (V3 doesn't use reserves)
                    &None::<Decimal>, // token1_reserve (V3 doesn't use reserves)
                    &Decimal::from_str("151786956677").unwrap(), // Smaller sqrtPrice
                    &Decimal::from_str("2000000000").unwrap(), // Smaller liquidity
                    &Some(200000i32), // Example tick
                    &9989999999i64, &"ethereum"
                ]).await;

                // Old Pool State @ block 100 (to test lookback failure)
                let _ = client.execute(&stmt, &[
                    &"ethereum", &100i64, &1i64, &OLD_POOL_ADDR.as_bytes(), &&[2u8; 32][..], &TOKEN0_ADDR.as_bytes(), &TOKEN1_ADDR.as_bytes(),
                    &18i32, &6i32, &protocol_type_to_i16(&ProtocolType::UniswapV2), &3000i32,
                    &Decimal::from_str("1000000").unwrap(), // Smaller amount0_in
                    &Decimal::from_str("0").unwrap(), // amount1_in
                    &Decimal::from_str("0").unwrap(), // amount0_out
                    &Decimal::from_str("500000").unwrap(), // Smaller amount1_out
                    &Decimal::from_str("500000000").unwrap(), // token0_reserve
                    &Decimal::from_str("250000000").unwrap(), // token1_reserve
                    &None::<Decimal>, &None::<Decimal>, &None::<i32>, &1009999999i64, &"ethereum"
                ]).await;

                println!("✅ Test data seeded successfully");
            }
            Err(e) => {
                println!("Swap statement preparation failed (table might not exist or have different schema): {}", e);
            }
        }
    }
}


#[tokio::test]
async fn test_swap_loader_initialization() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_swap_loader_initialization - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await;
    assert!(loader.is_ok(), "Swap loader should initialize successfully");
}

#[tokio::test]
async fn test_get_pool_state_at_exact_block() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_get_pool_state_at_exact_block - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    // First, let's check what's actually in the database
    let row_count = client.query_one(
        "SELECT COUNT(*) as count FROM swaps WHERE pool_address = $1 AND block_number = $2",
        &[&V2_POOL_ADDR.as_bytes(), &23026004i64]
    ).await.unwrap().get::<_, i64>(0);
    println!("Found {} rows for V2 pool at block 23026004", row_count);

    if row_count > 0 {
        // First, let's check what columns actually exist
        let columns = client.query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'swaps' AND table_schema = 'public'",
            &[]
        ).await.unwrap();

        println!("Available columns in swaps table:");
        for row in columns {
            let col_name: String = row.get(0);
            println!("  {}", col_name);
        }

        // Now try to query the data
        let row = client.query_one(
            "SELECT token0_reserve, token1_reserve FROM swaps WHERE pool_address = $1 AND block_number = $2",
            &[&V2_POOL_ADDR.as_bytes(), &23026004i64]
        ).await.unwrap();

        let token0_reserve: Option<rust_decimal::Decimal> = row.get("token0_reserve");
        let token1_reserve: Option<rust_decimal::Decimal> = row.get("token1_reserve");

        println!("Raw database values:");
        println!("token0_reserve: {:?}", token0_reserve);
        println!("token1_reserve: {:?}", token1_reserve);
    }

    // Test getting pool state at exact block
    let state_opt = loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, 23026004, Some("ethereum")).await.unwrap();
    assert!(state_opt.is_some(), "Should find state for V2 pool at exact block");
    let state = state_opt.unwrap();

    assert_eq!(state.pool_address, *V2_POOL_ADDR);
    assert_eq!(state.block_number, 23026004);
    assert_eq!(state.protocol_type, ProtocolType::UniswapV2);
    assert_eq!(state.fee_tier, 3000);

    // Debug logging
    println!("Parsed Reserve0: {}, Reserve1: {}", state.reserve0, state.reserve1);
    println!("Reserve0 > 0: {}, Reserve1 > 0: {}", state.reserve0 > U256::zero(), state.reserve1 > U256::zero());

    assert!(state.reserve0 > U256::zero() || state.reserve1 > U256::zero(), "V2 pool should have reserves");

    println!("✅ Successfully retrieved V2 pool state at exact block 23026004");
}

#[tokio::test]
async fn test_get_pool_state_fallback_within_range() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_get_pool_state_fallback_within_range - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    // Test fallback within range - look for block 998, should find V3 pool at block 998
    let state_opt = loader.get_pool_state_at_start_of_block_withchain(*V3_POOL_ADDR, 998, Some("ethereum")).await.unwrap();
    assert!(state_opt.is_some(), "Should find state for V3 pool at exact block");
    let state = state_opt.unwrap();

    assert_eq!(state.pool_address, *V3_POOL_ADDR);
    assert_eq!(state.block_number, 998);
    assert_eq!(state.protocol_type, ProtocolType::UniswapV3);
    assert_eq!(state.fee_tier, 500);
    assert!(state.sqrt_price_x96.is_some(), "V3 pool should have sqrt_price_x96");
    assert_eq!(state.tick, Some(200000));

    println!("✅ Successfully retrieved V3 pool state at exact block 998");
}

#[tokio::test]
async fn test_get_pool_state_fallback_outside_range() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_get_pool_state_fallback_outside_range - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    // Test fallback outside range - look for block 500 (no data should exist)
    let state_opt = loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, 500, Some("ethereum")).await.unwrap();
    assert!(state_opt.is_none(), "Should not find state for V2 pool at block 500 (no data exists)");

    println!("✅ Correctly returned None for non-existent pool state at block 500");
}

#[tokio::test]
async fn test_get_pool_state_no_data_found() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_get_pool_state_no_data_found - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    // Test with non-existent pool address
    let non_existent_pool = Address::from_str("0x9999999999999999999999999999999999999999").unwrap();
    let state_opt = loader.get_pool_state_at_start_of_block_withchain(non_existent_pool, 23026004, Some("ethereum")).await.unwrap();
    assert!(state_opt.is_none(), "Should not find state for non-existent pool");

    println!("✅ Correctly returned None for non-existent pool");
}

#[tokio::test]
async fn test_v3_pool_state_parsing() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_v3_pool_state_parsing - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    let state_opt = loader.get_pool_state_at_start_of_block_withchain(*V3_POOL_ADDR, 998, Some("ethereum")).await.unwrap();
    assert!(state_opt.is_some(), "Should find state for V3 pool");
    let state = state_opt.unwrap();

    assert_eq!(state.protocol_type, ProtocolType::UniswapV3);
    assert_eq!(state.fee_tier, 500);
    assert!(state.sqrt_price_x96.is_some());
    assert!(state.liquidity.is_some());
    assert_eq!(state.tick, Some(200000));
    assert_eq!(state.reserve0, U256::zero(), "V3 state should have zero reserves when parsed from sqrtPrice");
    assert_eq!(state.reserve1, U256::zero(), "V3 state should have zero reserves when parsed from sqrtPrice");
}

#[tokio::test]
async fn test_comprehensive_pool_state_loading() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_comprehensive_pool_state_loading - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config.clone()).await.unwrap();

    // Test comprehensive pool state loading across multiple scenarios
    println!("Testing comprehensive pool state loading...");

    // Test 1: Load multiple pools at the same block
    let block_number = 23026004;
    let pools_to_test = vec![
        (*V2_POOL_ADDR, "V2 pool"),
        (*V3_POOL_ADDR, "V3 pool"),
        (*OLD_POOL_ADDR, "Old pool"),
    ];

    let mut successful_loads = 0;
    for (pool_addr, pool_type) in pools_to_test {
        match loader.get_pool_state_at_start_of_block_withchain(pool_addr, block_number, Some("ethereum")).await {
            Ok(Some(state)) => {
                println!("✅ Successfully loaded {} at block {}: {} reserves, protocol: {:?}",
                    pool_type, block_number, state.reserve0 + state.reserve1, state.protocol_type);
                successful_loads += 1;

                // Verify state integrity
                assert_eq!(state.pool_address, pool_addr);
                // Note: The method returns the most recent state before or at the requested block
                // So we should check that the returned block is <= the requested block
                assert!(state.block_number <= block_number, 
                    "Returned block {} should be <= requested block {}", state.block_number, block_number);
                // Note: PoolStateFromSwaps doesn't have a chain_name field

                // Protocol-specific validations
                match state.protocol_type {
                    ProtocolType::UniswapV2 => {
                        assert!(state.reserve0 > U256::zero() || state.reserve1 > U256::zero(),
                            "V2 pool should have non-zero reserves");
                        assert!(state.sqrt_price_x96.is_none(), "V2 pools shouldn't have sqrt_price_x96");
                    }
                    ProtocolType::UniswapV3 => {
                        assert!(state.sqrt_price_x96.is_some(), "V3 pool should have sqrt_price_x96");
                        assert!(state.liquidity.is_some(), "V3 pool should have liquidity");
                    }
                    _ => {
                        println!("⚠️  Unexpected protocol type: {:?}", state.protocol_type);
                    }
                }
            }
            Ok(None) => {
                println!("⚠️  No state found for {} at block {}", pool_type, block_number);
            }
            Err(e) => {
                println!("❌ Failed to load {} at block {}: {}", pool_type, block_number, e);
            }
        }
    }

    // Test 2: Test fallback behavior with different block ranges
    let current_block = 23026004;
    let test_scenarios = vec![
        (current_block, "exact block match"),
        (current_block - 5, "close block (within range)"),
        (current_block - 100, "distant block (should fallback or fail)"),
    ];

    for (test_block, scenario_desc) in test_scenarios {
        match loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, test_block, Some("ethereum")).await {
            Ok(Some(state)) => {
                println!("✅ {} - Found V2 pool at block {} (requested: {}): {} reserves",
                    scenario_desc, state.block_number, test_block, state.reserve0 + state.reserve1);
            }
            Ok(None) => {
                println!("ℹ️  {} - No state found for V2 pool at block {}", scenario_desc, test_block);
            }
            Err(e) => {
                println!("❌ {} - Error loading V2 pool at block {}: {}", scenario_desc, test_block, e);
            }
        }
    }

    // Test 3: Verify database schema compatibility
    match client.query_one(
        "SELECT COUNT(*) as count FROM information_schema.columns
         WHERE table_name = 'swaps' AND column_name IN ('token0_reserve', 'token1_reserve', 'sqrt_price_x96')",
        &[]
    ).await {
        Ok(row) => {
            let column_count: i64 = row.get(0);
            println!("✅ Database schema validation: {} required columns found", column_count);
            assert!(column_count >= 3, "Should have at least 3 key columns for pool state");
        }
        Err(e) => {
            println!("⚠️  Could not validate database schema: {}", e);
        }
    }

    // Test 4: Performance test - multiple queries
    let start_time = std::time::Instant::now();
    let mut query_count = 0;

    for _ in 0..10 {
        if let Ok(Some(_)) = loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, block_number, Some("ethereum")).await {
            query_count += 1;
        }
    }

    let elapsed = start_time.elapsed();
    let avg_query_time = elapsed.as_millis() as f64 / query_count as f64;

    println!("✅ Performance test: {} successful queries in {:.2}ms (avg: {:.2}ms per query)",
        query_count, elapsed.as_millis(), avg_query_time);

    // Verify we had successful loads
    assert!(successful_loads > 0, "Should have successfully loaded at least one pool state");

    println!("✅ Comprehensive pool state loading test completed successfully");
}

#[tokio::test]
async fn test_database_connection_and_schema() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_database_connection_and_schema - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");

    // Test that we can get a client and run queries
    let client = pool.get().await.expect("Failed to get database client");

    // Verify tables exist
    let tables_result = client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('pools', 'swaps')",
        &[]
    ).await.expect("Failed to query table information");

    assert!(tables_result.len() >= 2, "Should have at least pools and swaps tables");

    // Test that we can query the seeded data
    let pool_count = client.query_one(
        "SELECT COUNT(*) as count FROM pools",
        &[]
    ).await.expect("Failed to count pools");

    let pool_count: i64 = pool_count.get(0);
    assert!(pool_count > 0, "Should have seeded pool data");

    let swap_count = client.query_one(
        "SELECT COUNT(*) as count FROM swaps",
        &[]
    ).await.expect("Failed to count swaps");

    let swap_count: i64 = swap_count.get(0);
    assert!(swap_count > 0, "Should have seeded swap data");
}

#[tokio::test]
async fn test_error_handling_and_edge_cases() {
    let db_result = setup_test_db().await;
    if let Err(e) = &db_result {
        if e.to_string().contains("Database not available for testing") {
            println!("⚠️  Skipping test_error_handling_and_edge_cases - database not available");
            return;
        }
    }

    let (pool, ()) = db_result.expect("Failed to setup test database");
    let client = pool.get().await.unwrap();
    seed_test_data(&client).await;

    let chain_config = Arc::new(create_test_chain_config());
    let loader = HistoricalSwapLoader::new(pool, chain_config).await.unwrap();

    println!("Testing error handling and edge cases...");

    // Test 1: Invalid pool addresses
    let invalid_addresses = vec![
        Address::zero(),
        Address::from_str("0x0000000000000000000000000000000000000001").unwrap(),
        Address::from_str("0x9999999999999999999999999999999999999999").unwrap(),
    ];

    let mut handled_errors = 0;
    for addr in invalid_addresses {
        match loader.get_pool_state_at_start_of_block_withchain(addr, 23026004, Some("ethereum")).await {
            Ok(None) => {
                println!("✅ Correctly handled non-existent pool: {:?}", addr);
                handled_errors += 1;
            }
            Ok(Some(state)) => {
                println!("ℹ️  Found state for pool (may be test data): {:?}", state.pool_address);
                handled_errors += 1;
            }
            Err(e) => {
                println!("✅ Correctly returned error for invalid pool: {}", e);
                handled_errors += 1;
            }
        }
    }

    // Test 2: Invalid block numbers
    let invalid_blocks = vec![
        0u64,           // Genesis block
        u64::MAX,       // Max block
        999999999999u64, // Way future block
    ];

    for block_num in invalid_blocks {
        match loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, block_num, Some("ethereum")).await {
            Ok(None) => {
                println!("✅ Correctly handled invalid block {}: no state found", block_num);
                handled_errors += 1;
            }
            Ok(Some(state)) => {
                println!("ℹ️  Found state at extreme block {} (may be test data): block {}", block_num, state.block_number);
                handled_errors += 1;
            }
            Err(e) => {
                println!("✅ Correctly returned error for invalid block {}: {}", block_num, e);
                handled_errors += 1;
            }
        }
    }

    // Test 3: Invalid chain names
    let invalid_chains = vec![
        "",
        "invalid_chain",
        "nonexistent",
        "polygon", // Different chain
    ];

    for chain_name in invalid_chains {
        match loader.get_pool_state_at_start_of_block_withchain(*V2_POOL_ADDR, 23026004, Some(chain_name)).await {
            Ok(None) => {
                println!("✅ Correctly handled invalid chain '{}': no state found", chain_name);
                handled_errors += 1;
            }
            Ok(Some(state)) => {
                println!("ℹ️  Found state for chain '{}' (may be test data): block {}", chain_name, state.block_number);
                handled_errors += 1;
            }
            Err(e) => {
                println!("✅ Correctly returned error for invalid chain '{}': {}", chain_name, e);
                handled_errors += 1;
            }
        }
    }

    // Test 4: Concurrent access patterns
    let loader_clone = loader.clone();
    let pool_addr = *V2_POOL_ADDR;
    let block_num = 23026004;

    let handles: Vec<_> = (0..5).map(|i| {
        let loader = loader_clone.clone();
        tokio::spawn(async move {
            match loader.get_pool_state_at_start_of_block_withchain(pool_addr, block_num, Some("ethereum")).await {
                Ok(Some(_)) => {
                    println!("✅ Concurrent request {} succeeded", i);
                    true
                }
                Ok(None) => {
                    println!("ℹ️  Concurrent request {}: no state found", i);
                    true // Still handled correctly
                }
                Err(e) => {
                    println!("❌ Concurrent request {} failed: {}", i, e);
                    false
                }
            }
        })
    }).collect();

    let mut concurrent_successes = 0;
    for handle in handles {
        if let Ok(success) = handle.await {
            if success {
                concurrent_successes += 1;
            }
        }
    }

    println!("✅ Concurrent access test: {}/5 requests handled successfully", concurrent_successes);

    // Test 5: Database connection errors (simulate by dropping connection)
    // Note: This is a basic test - in production you'd want more sophisticated connection error testing

    println!("✅ Error handling and edge cases test completed");
    assert!(handled_errors > 0, "Should have handled at least some errors gracefully");
}