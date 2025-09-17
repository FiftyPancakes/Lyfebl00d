//! discover_pools.rs - Comprehensive DEX Pool Discovery Engine
//!
//! This script builds the foundational dataset for the backtesting engine by creating a
//! master list of all liquidity pools on a given blockchain.
//!
//! ## Workflow:
//! 1.  **Configuration:** Reads a list of DEX factory contracts for a specified chain
//!     from `config/discovery_config.json`.
//! 2.  **Resumability:** Connects to the database and checks a `discovery_progress` table
//!     to find the last block scanned for each factory, ensuring it can resume if interrupted.
//! 3.  **Scanning:** Iterates through the blockchain's history in large, efficient chunks,
//!     from the last scanned block to the current chain head.
//! 4.  **Log Fetching:** For each chunk, it uses `eth_getLogs` to find all `PairCreated`
//!     (Uniswap V2-style) and `PoolCreated` (Uniswap V3-style) events.
//! 5.  **Decoding:** Decodes the raw event logs to extract critical pool information:
//!     `pool_address`, `token0`, `token1`, `fee` (for V3), and `creation_block`.
//! 6.  **Database Insertion:** Inserts the discovered pools into a `pools` table in
//!     batches, using `ON CONFLICT DO NOTHING` for idempotency. After each successful
//!     batch, it updates the `discovery_progress` table.
//!
//! ## Usage:
//! `cargo run --release --bin discover_pools -- --chain <CHAIN_NAME>`
//! Example: `cargo run --release --bin discover_pools -- --chain polygon`

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// --- Configuration Structs ---

#[derive(Debug, Deserialize)]
struct DiscoveryConfig {
    #[serde(flatten)]
    chains: HashMap<String, ChainDiscoveryConfig>,
}

#[derive(Debug, Deserialize)]
struct ChainDiscoveryConfig {
    rpc_url: String,
    factories: Vec<Factory>,
}

#[derive(Debug, Deserialize, Clone)]
struct Factory {
    address: Address,
    protocol_type: String,
    creation_block: u64,
}

// --- Database Struct ---

#[derive(Debug)]
struct DiscoveredPool {
    pool_address: Address,
    token0: Address,
    token1: Address,
    fee: Option<u32>,
    protocol_type: String, // e.g., 'UniswapV2', 'UniswapV3', 'SushiSwap', etc.
    dex_name: String,      // e.g., 'Uniswap', 'SushiSwap', 'QuickSwap', etc.
    chain_name: String,
    creation_block: u64,
}

// --- CLI Definition ---

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The name of the chain to discover pools for (e.g., "ethereum", "polygon").
    #[arg(long)]
    chain: String,
}

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let filter = EnvFilter::from_default_env().add_directive("discover_pools=info".parse()?);
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    info!("Starting pool discovery for chain: {}", args.chain);

    // Load configuration
    let config = load_config().await?;
    let chain_config = config
        .chains
        .get(&args.chain)
        .ok_or_else(|| anyhow!("Configuration for chain '{}' not found", args.chain))?;

    // Connect to database and provider
    let mut db_client = connect_db().await?;
    let provider = Arc::new(
        Provider::<Http>::try_from(&chain_config.rpc_url)
            .context("Failed to create provider from RPC URL")?,
    );

    // Ensure database tables are set up
    setup_database(&db_client).await?;

    // Check current pool count
    let initial_count = get_pool_count(&db_client, &args.chain).await?;
    info!("Initial pool count for chain {}: {}", args.chain, initial_count);

    // Scan each factory defined in the config
    for factory in &chain_config.factories {
        info!(
            "Scanning factory {} ({}) on chain {}",
            factory.address, factory.protocol_type, args.chain
        );
        if let Err(e) = scan_factory(
            provider.clone(),
            &mut db_client,
            factory,
            &args.chain,
        )
        .await
        {
            error!(
                "Failed to scan factory {}: {:?}",
                factory.address, e
            );
        }
    }

    // Check final pool count
    let final_count = get_pool_count(&db_client, &args.chain).await?;
    info!(
        "Pool discovery for chain '{}' complete. Final pool count: {} (added: {})",
        args.chain, final_count, final_count - initial_count
    );
    Ok(())
}

/// Scans a single factory contract for pool creation events from a saved starting point.
async fn scan_factory(
    provider: Arc<Provider<Http>>,
    db_client: &mut Client,
    factory: &Factory,
    chain_name: &str,
) -> Result<()> {
    let (event_topic, decoder): (Option<H256>, Option<fn(&Log) -> Result<DiscoveredPool>>)
        = match factory.protocol_type.as_str() {
        "UniswapV2" | "SushiSwap" | "QuickSwap" | "Pangolin" | "PancakeSwapV2" => (
            Some(H256::from_str("0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9")?),
            Some(decode_v2_creation_log),
        ),
        "UniswapV3" => (
            Some(H256::from_str("0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118")?),
            Some(decode_v3_creation_log),
        ),
        unsupported => {
            warn!("Skipping unsupported protocol_type: {} (factory: {})", unsupported, factory.address);
            (None, None)
        }
    };

    if event_topic.is_none() || decoder.is_none() {
        return Ok(()); // Skip unsupported
    }
    let event_topic = event_topic.unwrap();
    let decoder = decoder.unwrap();

    let last_scanned = get_last_scanned_block(db_client, factory.address, chain_name)
        .await?
        .unwrap_or(factory.creation_block);

    let mut current_block = last_scanned + 1;
    let latest_block = provider.get_block_number().await?.as_u64();
    const CHUNK_SIZE: u64 = 2000;

    info!(
        "Starting scan from block {} to {} for factory {} ({})",
        current_block, latest_block, factory.address, factory.protocol_type
    );

    while current_block <= latest_block {
        let to_block = std::cmp::min(current_block + CHUNK_SIZE - 1, latest_block);
        let filter = Filter::new()
            .address(factory.address)
            .topic0(event_topic)
            .from_block(current_block)
            .to_block(to_block);

        let logs = provider.get_logs(&filter).await?;
        let mut discovered_pools = Vec::new();
        let mut decode_errors = 0;

        for log in logs {
            match decoder(&log) {
                Ok(mut pool) => {
                    // Always set protocol_type and dex_name based on the factory config, not the event signature
                    pool.protocol_type = factory.protocol_type.clone();
                    pool.dex_name = protocol_type_to_dex_name(&factory.protocol_type);
                    pool.chain_name = chain_name.to_string();
                    discovered_pools.push(pool);
                }
                Err(e) => {
                    decode_errors += 1;
                    warn!("Failed to decode pool creation log: {}", e);
                }
            }
        }

        info!(
            "Successfully decoded {} pools, {} decode errors for factory {}",
            discovered_pools.len(), decode_errors, factory.address
        );

        if !discovered_pools.is_empty() {
            if let Err(e) = insert_pools_batch(db_client, &discovered_pools).await {
                error!(
                    "Failed to insert pools batch for factory {}: {}",
                    factory.address, e
                );
            }
        }

        update_last_scanned_block(db_client, factory.address, chain_name, to_block).await?;
        info!("Progress: Scanned up to block {} for factory {}", to_block, factory.address);
        current_block = to_block + 1;
    }
    Ok(())
}

/// Decodes a Uniswap V2 `PairCreated` event log.
fn decode_v2_creation_log(log: &Log) -> Result<DiscoveredPool> {
    if log.topics.len() != 3 || log.data.len() < 32 {
        return Err(anyhow!("Invalid V2 PairCreated log format"));
    }
    Ok(DiscoveredPool {
        token0: Address::from(log.topics[1]),
        token1: Address::from(log.topics[2]),
        pool_address: Address::from_slice(&log.data[12..32]),
        fee: Some(30), // V2 standard fee is 0.3% = 30 bps
        creation_block: log.block_number.context("Missing block number in log")?.as_u64(),
        protocol_type: String::new(), // Will be filled in by caller
        dex_name: String::new(),     // Will be filled in by caller
        chain_name: String::new(),    // Will be filled in by caller
    })
}

/// Decodes a Uniswap V3 `PoolCreated` event log.
fn decode_v3_creation_log(log: &Log) -> Result<DiscoveredPool> {
    if log.topics.len() != 4 || log.data.len() < 64 {
        return Err(anyhow!("Invalid V3 PoolCreated log format"));
    }

    let tick_spacing = U256::from_big_endian(&log.data[0..32]);
    let pool_address = Address::from_slice(&log.data[44..64]);

    if pool_address == Address::zero() {
        return Err(anyhow!("Invalid pool address in V3 PoolCreated event"));
    }

    let fee_topic = log.topics[3];
    let fee = U256::from_big_endian(fee_topic.as_bytes()).as_u32();

    let expected_tick_spacing = match fee {
        100 => 1,
        500 => 10,
        3000 => 60,
        10000 => 200,
        _ => 0,
    };
    if expected_tick_spacing != 0 && tick_spacing.as_u32() != expected_tick_spacing {
        return Err(anyhow!("Tick spacing {} does not match expected for fee tier {}", tick_spacing, fee));
    }

    Ok(DiscoveredPool {
        token0: Address::from(log.topics[1]),
        token1: Address::from(log.topics[2]),
        pool_address,
        fee: Some(fee),
        creation_block: log.block_number.context("Missing block number in log")?.as_u64(),
        protocol_type: String::new(),
        dex_name: String::new(),
        chain_name: String::new(),
    })
}

// --- Database Functions ---

async fn connect_db() -> Result<Client> {
    let conn_str = format!(
        "host={} port={} dbname={} user={} password={}",
        "localhost", 5432, "lyfeblood", "ichevako", "maddie"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("Failed to connect to PostgreSQL database")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });
    info!("Successfully connected to PostgreSQL");
    Ok(client)
}

async fn setup_database(client: &Client) -> Result<()> {
    // First, check if the pools table exists
    let table_exists = client
        .query_opt(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'pools'",
            &[],
        )
        .await?;

    if table_exists.is_none() {
        // Table doesn't exist, create it from scratch
        info!("Creating pools table from scratch");
        client
            .batch_execute(
                "CREATE TABLE pools (\n
                    pool_address BYTEA NOT NULL,\n
                    chain_name TEXT NOT NULL,\n
                    dex_name TEXT NOT NULL,\n
                    token0 BYTEA NOT NULL,\n
                    token1 BYTEA NOT NULL,\n
                    protocol_type TEXT NOT NULL,\n
                    fee INTEGER,\n
                    creation_block BIGINT NOT NULL,\n
                    PRIMARY KEY (pool_address, chain_name)\n
                );\n\
                CREATE INDEX idx_pools_tokens ON pools (token0, token1);\n
                CREATE INDEX idx_pools_chain ON pools (chain_name);\n
                CREATE INDEX idx_pools_dex_name ON pools (dex_name);\n
                CREATE INDEX idx_pools_protocol_type ON pools (protocol_type);\n
                "
            )
            .await
            .context("Failed to create pools table")?;
    } else {
        // Table exists, add the dex_name column if it doesn't exist
        info!("Adding dex_name column to existing pools table");
        client
            .execute("ALTER TABLE pools ADD COLUMN IF NOT EXISTS dex_name TEXT", &[])
            .await
            .context("Failed to add dex_name column")?;
        
        // Create indexes if they don't exist
        client
            .batch_execute(
                "CREATE INDEX IF NOT EXISTS idx_pools_tokens ON pools (token0, token1);\n
                CREATE INDEX IF NOT EXISTS idx_pools_chain ON pools (chain_name);\n
                CREATE INDEX IF NOT EXISTS idx_pools_dex_name ON pools (dex_name);\n
                CREATE INDEX IF NOT EXISTS idx_pools_protocol_type ON pools (protocol_type);\n
                "
            )
            .await
            .context("Failed to create indexes")?;
    }

    // Create discovery_progress table
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS discovery_progress (\n
                factory_address BYTEA NOT NULL,\n
                chain_name TEXT NOT NULL,\n
                last_scanned_block BIGINT NOT NULL,\n
                PRIMARY KEY (factory_address, chain_name)\n
            );\n
            CREATE INDEX IF NOT EXISTS idx_discovery_progress_factory ON discovery_progress (factory_address);\n
            CREATE INDEX IF NOT EXISTS idx_discovery_progress_chain ON discovery_progress (chain_name);\n
            "
        )
        .await
        .context("Failed to create discovery_progress table")?;

    info!("Database schema and indexes verified successfully");
    Ok(())
}

async fn get_last_scanned_block(
    client: &Client,
    factory: Address,
    chain_name: &str,
) -> Result<Option<u64>> {
    let row = client
        .query_opt(
            "SELECT last_scanned_block FROM discovery_progress WHERE factory_address = $1 AND chain_name = $2",
            &[&factory.as_bytes(), &chain_name],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0) as u64))
}

async fn get_pool_count(client: &Client, chain_name: &str) -> Result<u64> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM pools WHERE chain_name = $1",
            &[&chain_name],
        )
        .await?;
    Ok(row.get::<_, i64>(0) as u64)
}

async fn update_last_scanned_block(
    client: &Client,
    factory: Address,
    chain_name: &str,
    block_number: u64,
) -> Result<()> {
    client
        .execute(
            "INSERT INTO discovery_progress (factory_address, chain_name, last_scanned_block)
             VALUES ($1, $2, $3)
             ON CONFLICT (factory_address, chain_name)
             DO UPDATE SET last_scanned_block = EXCLUDED.last_scanned_block",
            &[&factory.as_bytes(), &chain_name, &(block_number as i64)],
        )
        .await?;
    Ok(())
}

async fn insert_pools_batch(client: &mut Client, pools: &[DiscoveredPool]) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }
    
    info!("Attempting to insert {} pools into database", pools.len());
    
    let transaction = client.transaction().await?;
    let statement = transaction
        .prepare(
            "INSERT INTO pools (pool_address, chain_name, dex_name, token0, token1, protocol_type, fee, creation_block)\n
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)\n
             ON CONFLICT (pool_address, chain_name) DO NOTHING",
        )
        .await?;
    
    let mut inserted_count = 0;
    let mut error_count = 0;
    
    for pool in pools {
        match transaction
            .execute(
                &statement,
                &[
                    &pool.pool_address.as_bytes(),
                    &pool.chain_name,
                    &pool.dex_name,
                    &pool.token0.as_bytes(),
                    &pool.token1.as_bytes(),
                    &pool.protocol_type,
                    &pool.fee.map(|f| f as i32),
                    &(pool.creation_block as i64),
                ],
            )
            .await
        {
            Ok(_) => {
                inserted_count += 1;
            }
            Err(e) => {
                error_count += 1;
                error!(
                    "Failed to insert pool {} ({}): {}",
                    pool.pool_address, pool.protocol_type, e
                );
            }
        }
    }
    
    match transaction.commit().await {
        Ok(_) => {
            info!(
                "Successfully inserted {} pools, {} errors, {} total processed",
                inserted_count, error_count, pools.len()
            );
            Ok(())
        }
        Err(e) => {
            error!("Failed to commit transaction: {}", e);
            Err(anyhow!("Database transaction failed: {}", e))
        }
    }
}

fn protocol_type_to_dex_name(protocol_type: &str) -> String {
    protocol_type.to_string()
}

// --- Configuration Loading ---

async fn load_config() -> Result<DiscoveryConfig> {
    let config_path = "config/discovery_config.json";
    let content = fs::read_to_string(config_path)
        .context(format!("Failed to read config file at {}", config_path))?;
    serde_json::from_str(&content).context("Failed to parse discovery_config.json")
}