use serde::Deserialize;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};

use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn, error, debug};
use tracing_subscriber;
use std::sync::Arc;
use std::fs;
use tokio_postgres::{Client, NoTls};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use ethers::prelude::*;

type RateLimiter = Mutex<VecDeque<Instant>>;

static API_KEY: &str = "CG-3GBPr8Kx99Y5VfcC5DVTsxkG";
const MAX_RETRIES: usize = 5;
const BASE_DELAY: f64 = 1.0;
const MAX_REQUESTS_PER_MINUTE: usize = usize::MAX;
const DEFAULT_SORT: &str = "h6_trending";

// Database config
const PG_HOST: &str = "localhost";
const PG_PORT: u16 = 5432;
const PG_USER: &str = "ichevako";
const PG_DB: &str = "lyfeblood";
const PG_PASS: &str = "maddie";
const TABLE_NAME: &str = "cg_pools";

// Pre-filter for reserves > $100
const MIN_RESERVE_USD: f64 = 100.0;

mod flexible_f64 {
    use serde::{self, Deserialize, Deserializer};
    use std::str::FromStr;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrFloat {
        Float(f64),
        String(String),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<StringOrFloat>::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(StringOrFloat::Float(f)) => Ok(Some(f)),
            Some(StringOrFloat::String(s)) => match f64::from_str(&s) {
                Ok(f) => Ok(Some(f)),
                Err(_) => Ok(None),
            },
        }
    }
}

// Flattened pool structure for database
#[derive(Debug, Clone)]
struct FlattenedPool {
    // Pool identifiers
    pool_id: String,
    pool_address: String,
    pool_name: Option<String>,
    chain_name: String,      // Changed from network_id for database storage
    dex_id: String,
    dex_name: String,        // Changed from dex_protocol
    protocol_type: i16,
    source: String, // "megafilter" or "new_pools"
    
    // Token addresses - Use consistent naming
    token0_address: Option<String>,  // Changed from from_token_address
    token1_address: Option<String>,  // Changed from to_token_address
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    token0_name: Option<String>,
    token1_name: Option<String>,
    token0_decimals: Option<i32>,
    token1_decimals: Option<i32>,
    
    // Prices
    token0_price_usd: Option<Decimal>,
    token1_price_usd: Option<Decimal>,
    token0_price_native: Option<Decimal>,
    token1_price_native: Option<Decimal>,
    
    // Pool metrics
    fdv_usd: Option<Decimal>,
    market_cap_usd: Option<Decimal>,
    reserve_in_usd: Option<Decimal>,
    
    // Price changes
    price_change_5m: Option<Decimal>,
    price_change_1h: Option<Decimal>,
    price_change_6h: Option<Decimal>,
    price_change_24h: Option<Decimal>,
    
    // Volume
    volume_usd_5m: Option<Decimal>,
    volume_usd_1h: Option<Decimal>,
    volume_usd_6h: Option<Decimal>,
    volume_usd_24h: Option<Decimal>,
    volume_usd_7d: Option<Decimal>,    // Added missing field
    
    // Transactions
    tx_24h_buys: Option<i64>,
    tx_24h_sells: Option<i64>,
    tx_24h_buyers: Option<i64>,
    tx_24h_sellers: Option<i64>,
    
    // Pool state
    fee_tier: Option<i32>,              // Added missing field
    is_active: Option<bool>,            // Added missing field
    
    // Timestamps
    pool_created_at: Option<i64>,
    first_seen: i64,
    last_updated: i64,
    

}

#[derive(Deserialize, Debug, Clone)]
pub struct MegafilterResponse {
    pub data: Vec<RawPool>,
    pub included: Option<Vec<IncludedItem>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct IncludedItem {
    pub id: String,
    pub r#type: String,
    pub attributes: serde_json::Value,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RawPool {
    pub id: Option<String>,
    pub r#type: Option<String>,
    pub attributes: Option<PoolAttributes>,
    pub relationships: Option<PoolRelationships>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PoolAttributes {
    pub address: Option<String>,
    #[serde(with = "flexible_f64")]
    pub fdv_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub market_cap_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub reserve_in_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub base_token_price_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub base_token_price_native_currency: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub quote_token_price_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub quote_token_price_native_currency: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub base_token_price_quote_token: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub quote_token_price_base_token: Option<f64>,
    pub pool_created_at: Option<String>,
    pub name: Option<String>,
    pub price_change_percentage: Option<PriceChangePercent>,
    pub transactions: Option<Transactions>,
    pub volume_usd: Option<VolumeUsd>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PriceChangePercent {
    #[serde(with = "flexible_f64")]
    pub m5: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub m15: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub m30: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h1: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h6: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h24: Option<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Transactions {
    pub m5: Option<TxInterval>,
    pub m15: Option<TxInterval>,
    pub m30: Option<TxInterval>,
    pub h1: Option<TxInterval>,
    pub h6: Option<TxInterval>,
    pub h24: Option<TxInterval>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TxInterval {
    pub buys: Option<i64>,
    pub sells: Option<i64>,
    pub buyers: Option<i64>,
    pub sellers: Option<i64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct VolumeUsd {
    #[serde(with = "flexible_f64")]
    pub m5: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub m15: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub m30: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h1: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h6: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub h24: Option<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PoolRelationships {
    pub base_token: Option<RelationshipDataWrap>,
    pub quote_token: Option<RelationshipDataWrap>,
    pub dex: Option<RelationshipDataWrap>,
    pub network: Option<RelationshipDataWrap>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RelationshipDataWrap {
    pub data: Option<RelationshipData>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RelationshipData {
    pub id: Option<String>,
    pub r#type: Option<String>,
}

// Database functions
async fn connect_db() -> Result<Client, Box<dyn std::error::Error>> {
    let mut attempts = 0;
    let max_attempts = 5;

    while attempts < max_attempts {
        let conn_str = format!(
            "host={} port={} dbname={} user={} password={}",
            PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
        );

        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("Connection error: {}", e);
                    }
                });
                info!("[DB] Connected to PostgreSQL");
                return Ok(client);
            }
            Err(e) => {
                attempts += 1;
                error!("[DB] Connection attempt {} failed: {}", attempts, e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    Err("[DB] Could not connect after multiple attempts".into())
}

async fn create_pools_table(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Don't drop the table - use CREATE IF NOT EXISTS instead
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            pool_id TEXT PRIMARY KEY,
            pool_address TEXT NOT NULL,
            pool_name TEXT,
            chain_name TEXT NOT NULL,
            dex_id TEXT NOT NULL,
            dex_name TEXT NOT NULL,
            protocol_type SMALLINT NOT NULL,
            source TEXT NOT NULL,
            
            token0_address TEXT,
            token1_address TEXT,
            token0_symbol TEXT,
            token1_symbol TEXT,
            token0_name TEXT,
            token1_name TEXT,
            token0_decimals INTEGER,
            token1_decimals INTEGER,
            
            token0_price_usd NUMERIC,
            token1_price_usd NUMERIC,
            token0_price_native NUMERIC,
            token1_price_native NUMERIC,
            
            fdv_usd NUMERIC,
            market_cap_usd NUMERIC,
            reserve_in_usd NUMERIC,
            
            price_change_5m NUMERIC,
            price_change_1h NUMERIC,
            price_change_6h NUMERIC,
            price_change_24h NUMERIC,
            
            volume_usd_5m NUMERIC,
            volume_usd_1h NUMERIC,
            volume_usd_6h NUMERIC,
            volume_usd_24h NUMERIC,
            volume_usd_7d NUMERIC,
            
            tx_24h_buys BIGINT,
            tx_24h_sells BIGINT,
            tx_24h_buyers BIGINT,
            tx_24h_sellers BIGINT,
            
            fee_tier INTEGER,
            is_active BOOLEAN,
            
            pool_created_at BIGINT,
            first_seen BIGINT NOT NULL,
            last_updated BIGINT NOT NULL
        )",
        TABLE_NAME
    );

    client.execute(&create_sql, &[]).await?;
    info!("[DB] Table {} created or already exists", TABLE_NAME);
    
    // Create indexes for common queries (only if they don't exist)
    let indexes = vec![
        format!("CREATE INDEX IF NOT EXISTS idx_{}_chain ON {} (chain_name)", TABLE_NAME, TABLE_NAME),
        format!("CREATE INDEX IF NOT EXISTS idx_{}_dex ON {} (dex_id)", TABLE_NAME, TABLE_NAME),
        format!("CREATE INDEX IF NOT EXISTS idx_{}_protocol ON {} (protocol_type)", TABLE_NAME, TABLE_NAME),
        format!("CREATE INDEX IF NOT EXISTS idx_{}_reserve ON {} (reserve_in_usd)", TABLE_NAME, TABLE_NAME),
        format!("CREATE INDEX IF NOT EXISTS idx_{}_source ON {} (source)", TABLE_NAME, TABLE_NAME),
    ];
    
    for index_sql in indexes {
        if let Err(e) = client.execute(&index_sql, &[]).await {
            warn!("[DB] Index creation warning (may already exist): {}", e);
        }
    }
    
    Ok(())
}

async fn verify_database_setup(client: &Client, chain: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Check if cg_pools table exists and has data
    let table_exists = client.query_one(
        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1)",
        &[&TABLE_NAME]
    ).await?.get::<_, bool>(0);
    
    if !table_exists {
        return Err(format!("cg_pools table does not exist. Run pool fetcher first.").into());
    }
    
    // Check column names
    let columns = client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
        &[&TABLE_NAME]
    ).await?;
    
    let column_names: Vec<String> = columns.iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    
    // Verify required columns exist
    let required = vec!["token0_address", "token1_address", "pool_address", "chain_name"];
    for col in required {
        if !column_names.contains(&col.to_string()) {
            return Err(format!("Missing required column {} in cg_pools", col).into());
        }
    }
    
    // Check if we have pools for this chain
    let pool_count: i64 = client.query_one(
        "SELECT COUNT(*) FROM cg_pools WHERE chain_name = $1",
        &[&chain]
    ).await?.get(0);
    
    if pool_count == 0 {
        return Err(format!("No pools found in cg_pools for chain {}. Run pool fetcher for this chain first.", chain).into());
    }
    
    info!("[DB] Database verification passed: {} pools found for {}", pool_count, chain);
    Ok(())
}

async fn insert_pools_batch(
    client: &Client,
    pools: &[FlattenedPool],
) -> Result<(), Box<dyn std::error::Error>> {
    if pools.is_empty() {
        warn!("[DB] No pools to insert - empty batch");
        return Ok(());
    }

    info!("[DB] Starting batch insert of {} pools", pools.len());
    
    // First, let's verify the table exists
    let check_table = client.query_one(
        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1)",
        &[&TABLE_NAME]
    ).await?;
    let table_exists: bool = check_table.get(0);
    
    if !table_exists {
        error!("[DB] Table {} does not exist!", TABLE_NAME);
        return Err(format!("Table {} does not exist", TABLE_NAME).into());
    }
    
    info!("[DB] Table {} confirmed to exist", TABLE_NAME);

            let sql = format!(
        "INSERT INTO {} (pool_id, pool_address, pool_name, chain_name, dex_id, dex_name, protocol_type, source, token0_address, token1_address, token0_symbol, token1_symbol, token0_name, token1_name, token0_decimals, token1_decimals, token0_price_usd, token1_price_usd, token0_price_native, token1_price_native, fdv_usd, market_cap_usd, reserve_in_usd, price_change_5m, price_change_1h, price_change_6h, price_change_24h, volume_usd_5m, volume_usd_1h, volume_usd_6h, volume_usd_24h, volume_usd_7d, tx_24h_buys, tx_24h_sells, tx_24h_buyers, tx_24h_sellers, fee_tier, is_active, pool_created_at, first_seen, last_updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41) ON CONFLICT (pool_id) DO UPDATE SET last_updated = EXCLUDED.last_updated",
        TABLE_NAME
    );
    
    info!("[DB] Generated SQL: {}", sql);
    
    let statement = client.prepare(&sql).await?;

    let mut success_count = 0;
    let mut error_count = 0;
    
    for (idx, pool) in pools.iter().enumerate() {
        match client.execute(&statement, &[
            &pool.pool_id,
            &pool.pool_address,
            &pool.pool_name,
            &pool.chain_name,
            &pool.dex_id,
            &pool.dex_name,
            &pool.protocol_type,
            &pool.source,
            &pool.token0_address,
            &pool.token1_address,
            &pool.token0_symbol,
            &pool.token1_symbol,
            &pool.token0_name,
            &pool.token1_name,
            &pool.token0_decimals,
            &pool.token1_decimals,
            &pool.token0_price_usd,
            &pool.token1_price_usd,
            &pool.token0_price_native,
            &pool.token1_price_native,
            &pool.fdv_usd,
            &pool.market_cap_usd,
            &pool.reserve_in_usd,
            &pool.price_change_5m,
            &pool.price_change_1h,
            &pool.price_change_6h,
            &pool.price_change_24h,
            &pool.volume_usd_5m,
            &pool.volume_usd_1h,
            &pool.volume_usd_6h,
            &pool.volume_usd_24h,
            &pool.volume_usd_7d,
            &pool.tx_24h_buys,
            &pool.tx_24h_sells,
            &pool.tx_24h_buyers,
            &pool.tx_24h_sellers,
            &pool.fee_tier,
            &pool.is_active,
            &pool.pool_created_at,
            &pool.first_seen,
            &pool.last_updated,
        ]).await {
            Ok(rows_affected) => {
                success_count += 1;
                if idx < 3 {  // Log first few for debugging
                    debug!("[DB] Inserted pool {}: {} (rows affected: {})", idx + 1, pool.pool_id, rows_affected);
                }
            }
            Err(e) => {
                error_count += 1;
                error!("[DB] Failed to insert pool {}: {} - Error: {}", idx + 1, pool.pool_id, e);
                if error_count > 5 {
                    return Err(format!("Too many insert errors: {}", e).into());
                }
            }
        }
    }
    
    info!("[DB] Batch insert completed: {} successful, {} failed out of {} total", 
          success_count, error_count, pools.len());
    
    // Verify data was actually written
    let count_result = client.query_one(
        &format!("SELECT COUNT(*) FROM {}", TABLE_NAME),
        &[]
    ).await?;
    let total_count: i64 = count_result.get(0);
    info!("[DB] Total rows in {} table after insert: {}", TABLE_NAME, total_count);
    
    if success_count > 0 && total_count == 0 {
        error!("[DB] WARNING: Insert reported success but table is empty!");
    }
    
    Ok(())
}

async fn check_rate_limit(limiter: &Arc<RateLimiter>) {
    let mut rl = limiter.lock().await;
    let now = Instant::now();

    while !rl.is_empty() && now.duration_since(*rl.front().unwrap()) >= Duration::from_secs(60) {
        rl.pop_front();
    }

    if rl.len() >= MAX_REQUESTS_PER_MINUTE {
        let earliest = *rl.front().unwrap();
        let wait_time = Duration::from_secs(60) - now.duration_since(earliest);
        warn!("[RateLimit] Reached {} reqs/60s, sleeping {:.2}s", MAX_REQUESTS_PER_MINUTE, wait_time.as_secs_f32());
        sleep(wait_time).await;
        while !rl.is_empty() && now.duration_since(*rl.front().unwrap()) >= Duration::from_secs(60) {
            rl.pop_front();
        }
    }

    rl.push_back(Instant::now());
}

fn extract_network_and_id(composite_id: &str) -> (String, String) {
    if let Some(index) = composite_id.find('_') {
        let network = composite_id[0..index].to_string();
        let entity_id = composite_id[index+1..].to_string();
        return (network, entity_id);
    }
    (String::new(), composite_id.to_string())
}

fn normalize_network_id(network_id: &str) -> String {
    match network_id {
        "eth" => "ethereum".to_string(),
        "avax" => "avalanche".to_string(),
        "base" => "base".to_string(),
        "bnb" => "bsc".to_string(),
        "arb" => "arbitrum".to_string(),
        "matic" => "polygon".to_string(),
        "polygon_pos" => "polygon".to_string(),
        "sol" => "solana".to_string(),
        _ => network_id.to_string(),
    }
}

fn extract_token_info(
    included: &Option<Vec<IncludedItem>>,
    token_id: Option<&String>,
) -> (Option<String>, Option<String>, Option<i32>) {
    if let (Some(included), Some(token_id)) = (included, token_id) {
        for item in included {
            if &item.id == token_id && item.r#type == "token" {
                let symbol = item.attributes.get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let name = item.attributes.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let decimals = item.attributes.get("decimals")
                    .and_then(|v| {
                        if v.is_u64() {
                            v.as_u64().and_then(|d| i32::try_from(d).ok())
                        } else if v.is_string() {
                            v.as_str().and_then(|s| s.parse::<i32>().ok())
                        } else {
                            None
                        }
                    });
                return (symbol, name, decimals);
            }
        }
    }
    (None, None, None)
}

fn transform_to_flattened_pools(
    response: &MegafilterResponse,
    dex_version_map: &HashMap<String, String>,
    source: &str,
) -> Vec<FlattenedPool> {
    let mut out = Vec::new();
    let now_unix = Utc::now().timestamp();
    
    for pool in &response.data {
        let pool_id = match &pool.id { Some(id) => id.clone(), None => continue };
        let attributes = match &pool.attributes { Some(attrs) => attrs, None => continue };
        let relationships = match &pool.relationships { Some(rels) => rels, None => continue };
        
        // Apply reserve filter
        if let Some(reserve) = attributes.reserve_in_usd {
            if reserve < MIN_RESERVE_USD {
                continue;
            }
        } else {
            continue; // Skip if no reserve data
        }
        
        let chain_name = relationships.network
            .as_ref()
            .and_then(|wrap| wrap.data.as_ref())
            .and_then(|data| data.id.as_ref())
            .map(|id| normalize_network_id(id))
            .unwrap_or_else(|| {
                let (network, _) = extract_network_and_id(&pool_id);
                normalize_network_id(&network)
            });
            
        let dex_id = relationships.dex
            .as_ref()
            .and_then(|wrap| wrap.data.as_ref())
            .and_then(|data| data.id.as_ref())
            .map(|id| id.to_string())
            .unwrap_or_default();
            
        let base_token_id = relationships.base_token
            .as_ref()
            .and_then(|wrap| wrap.data.as_ref())
            .and_then(|data| data.id.as_ref());
        let quote_token_id = relationships.quote_token
            .as_ref()
            .and_then(|wrap| wrap.data.as_ref())
            .and_then(|data| data.id.as_ref());
            
        let (_, token0_address) = base_token_id
            .map(|id| extract_network_and_id(id))
            .unwrap_or((String::new(), String::new()));
        let (_, token1_address) = quote_token_id
            .map(|id| extract_network_and_id(id))
            .unwrap_or((String::new(), String::new()));
            
        let (token0_symbol, token0_name, token0_decimals) = extract_token_info(&response.included, base_token_id);
        let (token1_symbol, token1_name, token1_decimals) = extract_token_info(&response.included, quote_token_id);
        
        let protocol_type_str = resolve_protocol_type(&dex_id, pool.r#type.as_deref(), dex_version_map);
        let protocol_type_id = get_protocol_type_id(&protocol_type_str);
        
        let price_change = &attributes.price_change_percentage;
        let volume = &attributes.volume_usd;
        let tx = &attributes.transactions;
        
        out.push(FlattenedPool {
            pool_id: pool_id.clone(),
            pool_address: attributes.address.clone().unwrap_or_default(),
            pool_name: attributes.name.clone(),
            chain_name,
            dex_id: dex_id.clone(),
            dex_name: dex_id.clone(),
            protocol_type: protocol_type_id,
            source: source.to_string(),
            
            token0_address: if token0_address.is_empty() { None } else { Some(token0_address) },
            token1_address: if token1_address.is_empty() { None } else { Some(token1_address) },
            token0_symbol,
            token1_symbol,
            token0_name,
            token1_name,
            token0_decimals,
            token1_decimals,
            
            token0_price_usd: attributes.base_token_price_usd.and_then(|v| Decimal::from_f64(v)),
            token1_price_usd: attributes.quote_token_price_usd.and_then(|v| Decimal::from_f64(v)),
            token0_price_native: attributes.base_token_price_native_currency.and_then(|v| Decimal::from_f64(v)),
            token1_price_native: attributes.quote_token_price_native_currency.and_then(|v| Decimal::from_f64(v)),
            
            fdv_usd: attributes.fdv_usd.and_then(|v| Decimal::from_f64(v)),
            market_cap_usd: attributes.market_cap_usd.and_then(|v| Decimal::from_f64(v)),
            reserve_in_usd: attributes.reserve_in_usd.and_then(|v| Decimal::from_f64(v)),
            
            price_change_5m: price_change.as_ref().and_then(|p| p.m5).and_then(|v| Decimal::from_f64(v)),
            price_change_1h: price_change.as_ref().and_then(|p| p.h1).and_then(|v| Decimal::from_f64(v)),
            price_change_6h: price_change.as_ref().and_then(|p| p.h6).and_then(|v| Decimal::from_f64(v)),
            price_change_24h: price_change.as_ref().and_then(|p| p.h24).and_then(|v| Decimal::from_f64(v)),
            
            volume_usd_5m: volume.as_ref().and_then(|v| v.m5).and_then(|v| Decimal::from_f64(v)),
            volume_usd_1h: volume.as_ref().and_then(|v| v.h1).and_then(|v| Decimal::from_f64(v)),
            volume_usd_6h: volume.as_ref().and_then(|v| v.h6).and_then(|v| Decimal::from_f64(v)),
            volume_usd_24h: volume.as_ref().and_then(|v| v.h24).and_then(|v| Decimal::from_f64(v)),
            volume_usd_7d: None, // Not available in the current API response
            
            tx_24h_buys: tx.as_ref().and_then(|t| t.h24.as_ref()).and_then(|t| t.buys),
            tx_24h_sells: tx.as_ref().and_then(|t| t.h24.as_ref()).and_then(|t| t.sells),
            tx_24h_buyers: tx.as_ref().and_then(|t| t.h24.as_ref()).and_then(|t| t.buyers),
            tx_24h_sellers: tx.as_ref().and_then(|t| t.h24.as_ref()).and_then(|t| t.sellers),
            
            fee_tier: extract_fee_tier_from_pool(protocol_type_id),
            is_active: Some(true), // Assume pools from API are active
            
            pool_created_at: attributes.pool_created_at.as_ref().and_then(|s| iso8601_to_unix(s)),
            first_seen: now_unix,
            last_updated: now_unix,
        });
    }
    
    out
}

async fn fetch_pools_megafilter(
    limiter: &Arc<RateLimiter>,
    page: usize,
    networks: Option<Vec<&str>>,
    sort: Option<&str>,
    min_reserve_usd: Option<f64>,
) -> Result<MegafilterResponse, Box<dyn Error + Send + Sync>> {
    let mut delay = BASE_DELAY;
    let client = reqwest::Client::new();

    let mut params = vec![
        ("include", "base_token,quote_token,dex,network".to_string()),
        ("page", page.to_string()),
        ("sort", sort.unwrap_or(DEFAULT_SORT).to_string()),
        ("tx_count_duration", "5m".to_string()),
        ("buys_duration", "24h".to_string()),
        ("sells_duration", "24h".to_string()),
        ("checks", "no_honeypot,good_gt_score".to_string()),
    ];

    // Add network filter if specified
    if let Some(network_list) = networks {
        if !network_list.is_empty() {
            let networks_str = network_list.join(",");
            params.push(("networks", networks_str));
        }
    }

    if let Some(min_reserve) = min_reserve_usd {
        params.push(("reserve_in_usd_min", min_reserve.to_string()));
    }

    let mut attempt = 0;
    while attempt < MAX_RETRIES {
        check_rate_limit(limiter).await;

        let mut headers = HeaderMap::new();
        headers.insert("x-cg-pro-api-key", HeaderValue::from_static(API_KEY));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let url = "https://pro-api.coingecko.com/api/v3/onchain/pools/megafilter";
        let resp = client
            .get(url)
            .headers(headers.clone())
            .query(&params)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().await?;

                if status.is_success() {
                    let response: MegafilterResponse = serde_json::from_str(&text)?;
                    tokio::time::sleep(Duration::from_millis(
                        100 + rand::random::<u64>() % 200,
                    )).await;
                    return Ok(response);
                } else if status.as_u16() == 429 {
                    warn!(
                        "[429] Megafilter page {}: Attempt {}/{} backoff {:.1}s",
                        page,
                        attempt + 1,
                        MAX_RETRIES,
                        delay
                    );
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else if [500, 502, 503, 504].contains(&status.as_u16()) {
                    warn!(
                        "[HTTP {}] Megafilter page {}: attempt {}/{}, delay={:.1}s",
                        status,
                        page,
                        attempt + 1,
                        MAX_RETRIES,
                        delay
                    );
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else if status.as_u16() == 404 {
                    warn!(
                        "[HTTP 404] Megafilter request failed - check parameters: {}", 
                        text
                    );
                    return Err("Megafilter endpoint returned 404 - invalid parameters".into());
                } else {
                    warn!(
                        "[HTTP {}] Megafilter page {}: {}",
                        status, page, text
                    );
                    return Err(format!("HTTP error {}", status).into());
                }
            }
            Err(e) => {
                error!(
                    "[FetchError] Megafilter page {} => {}",
                    page, e
                );
                sleep(Duration::from_secs_f64(delay)).await;
                delay *= 2.0;
            }
        }
        attempt += 1;
    }
    Err("Max retries exceeded".into())
}

async fn fetch_pools_new_pools(
    limiter: &Arc<RateLimiter>,
    page: usize,
    networks: Option<Vec<&str>>,
    min_reserve_usd: Option<f64>,
) -> Result<MegafilterResponse, Box<dyn Error + Send + Sync>> {
    let mut delay = BASE_DELAY;
    let client = reqwest::Client::new();

    // Build params - include networks in query params, not in URL path
    let mut params = vec![
        ("include", "base_token,quote_token,dex,network".to_string()),
        ("page", page.to_string()),
    ];

    // Add network filter if specified
    if let Some(network_list) = networks {
        if !network_list.is_empty() {
            let networks_str = network_list.join(",");
            params.push(("networks", networks_str));
        }
    }

    if let Some(min_reserve) = min_reserve_usd {
        params.push(("reserve_in_usd_min", min_reserve.to_string()));
    }

    let mut attempt = 0;
    while attempt < MAX_RETRIES {
        check_rate_limit(limiter).await;
        let mut headers = HeaderMap::new();
        headers.insert("x-cg-pro-api-key", HeaderValue::from_static(API_KEY));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        // Use the correct URL without network in path
        let url = "https://pro-api.coingecko.com/api/v3/onchain/networks/new_pools";

        let resp = client
            .get(url)
            .headers(headers.clone())
            .query(&params)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().await?;
                if status.is_success() {
                    let response: MegafilterResponse = serde_json::from_str(&text)?;
                    tokio::time::sleep(Duration::from_millis(100 + rand::random::<u64>() % 200)).await;
                    return Ok(response);
                } else if status.as_u16() == 429 {
                    warn!(
                        "[429] new_pools page {}: Attempt {}/{} backoff {:.1}s",
                        page,
                        attempt + 1,
                        MAX_RETRIES,
                        delay
                    );
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else if [500, 502, 503, 504].contains(&status.as_u16()) {
                    warn!(
                        "[HTTP {}] new_pools page {}: attempt {}/{}, delay={:.1}s",
                        status,
                        page,
                        attempt + 1,
                        MAX_RETRIES,
                        delay
                    );
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else {
                    warn!("[HTTP {}] new_pools page {}: {}", status, page, text);
                    return Err(format!("HTTP error {}", status).into());
                }
            }
            Err(e) => {
                error!("[FetchError] new_pools page {} => {}", page, e);
                sleep(Duration::from_secs_f64(delay)).await;
                delay *= 2.0;
            }
        }
        attempt += 1;
    }
    Err("Max retries exceeded".into())
}

fn iso8601_to_unix(ts: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(ts).ok().map(|dt| dt.timestamp())
}

fn extract_fee_tier_from_pool(protocol_type_id: i16) -> Option<i32> {
    // FALLBACK ONLY: Default fee tiers by protocol in basis points
    // V3 pools MUST have their fees fetched dynamically using the fee() function
    // This function should only be used as a last resort fallback
    match protocol_type_id {
        1 => Some(30),     // UniswapV2 - 0.30%
        2 => None,         // UniswapV3 - MUST be fetched on-chain - no default!
        3 => Some(4),      // Curve - 0.04%
        4 => Some(30),     // Balancer - 0.30% (can vary)
        5 => Some(30),     // SushiSwap - 0.30%
        6 => Some(25),     // PancakeSwap - 0.25%
        7 => None,         // PancakeSwapV3 - MUST be fetched on-chain - no default!
        8 => None,         // UniswapV4 - MUST be fetched on-chain - no default!
        9 => Some(30),     // Bancor - 0.30%
        10 => Some(30),    // TraderJoe - 0.30%
        _ => Some(30),     // Other - Default to 0.30%
    }
}

// Protocol type mapping for database storage (consistent with swaps.rs)
fn get_protocol_type_id(protocol: &str) -> i16 {
    match protocol {
        "UniswapV2" => 1,
        "UniswapV3" => 2,
        "Curve" => 3,
        "Balancer" => 4,
        "SushiSwap" => 5,
        "PancakeSwap" => 6,
        "PancakeSwapV3" => 7,
        "UniswapV4" => 8,
        "Bancor" => 9,
        "TraderJoe" => 10,
        _ => 99, // Other
    }
}

fn resolve_protocol_type(
    dex_id: &str,
    pool_type: Option<&str>,
    dex_version_map: &HashMap<String, String>,
) -> String {
    let lower_dex_id = dex_id.to_lowercase();
    let lower_pool_type = pool_type.map(|s| s.to_lowercase()).unwrap_or_default();

    // First check pool type
    match lower_pool_type.as_str() {
        "v2" | "uniswapv2" | "uniswap_v2" => return "UniswapV2".to_string(),
        "v3" | "uniswapv3" | "uniswap_v3" => return "UniswapV3".to_string(),
        "v4" | "uniswapv4" | "uniswap_v4" => return "UniswapV4".to_string(),
        "curve" => return "Curve".to_string(),
        "balancer" | "balancer_v2" => return "Balancer".to_string(),
        "sushiswap" => return "SushiSwap".to_string(),
        "pancakeswap" | "pancakeswap_v2" | "pancakeswap_v3" => return "PancakeSwap".to_string(),
        "traderjoe" | "trader_joe_v2" => return "TraderJoe".to_string(),
        _ => {}
    }

    // Check version map from new config format
    if let Some(version) = dex_version_map.get(dex_id) {
        match version.to_lowercase().as_str() {
            "v2" => {
                // Map common v2 DEXes to their protocols
                match lower_dex_id.as_str() {
                    s if s.contains("uniswap") => "UniswapV2".to_string(),
                    s if s.contains("sushiswap") || s.contains("sushi") => "SushiSwap".to_string(),
                    s if s.contains("pancakeswap") || s.contains("pancake") => "PancakeSwap".to_string(),
                    s if s.contains("traderjoe") || s.contains("trader_joe") => "TraderJoe".to_string(),
                    s if s.contains("balancer") => "Balancer".to_string(),
                    _ => format!("Other({}-v2)", dex_id),
                }
            },
            "v3" => {
                // Map common v3 DEXes to their protocols
                match lower_dex_id.as_str() {
                    s if s.contains("uniswap") => "UniswapV3".to_string(),
                    s if s.contains("pancakeswap") || s.contains("pancake") => "PancakeSwapV3".to_string(),
                    _ => format!("Other({}-v3)", dex_id),
                }
            },
            "v4" => {
                match lower_dex_id.as_str() {
                    s if s.contains("uniswap") => "UniswapV4".to_string(),
                    _ => format!("Other({}-v4)", dex_id),
                }
            },
            "v1" => {
                match lower_dex_id.as_str() {
                    s if s.contains("curve") => "Curve".to_string(),
                    _ => format!("Other({}-v1)", dex_id),
                }
            },
            _ => format!("Other({}-{})", dex_id, version),
        }
    } else {
        // Fallback to pattern matching if not found in config
        if lower_dex_id.contains("uniswap") {
            if lower_dex_id.contains("v4") {
                return "UniswapV4".to_string();
            } else if lower_dex_id.contains("v3") {
                return "UniswapV3".to_string();
            } else if lower_dex_id.contains("v2") || lower_dex_id.contains("v1") {
                return "UniswapV2".to_string();
            }
        }

        // Match common DEX patterns
        match lower_dex_id.as_str() {
            s if s.contains("curve") => "Curve".to_string(),
            s if s.contains("balancer") => "Balancer".to_string(),
            s if s.contains("sushiswap") || s.contains("sushi") => "SushiSwap".to_string(),
            s if s.contains("pancakeswap") || s.contains("pancake") => "PancakeSwap".to_string(),
            s if s.contains("traderjoe") || s.contains("trader_joe") => "TraderJoe".to_string(),
            s if s.contains("aerodrome") => "Aerodrome".to_string(),
            s if s.contains("raydium") => "Raydium".to_string(),
            s if s.contains("orca") => "Orca".to_string(),
            s if s.contains("quickswap") => "QuickSwap".to_string(),
            s if s.contains("camelot") => "Camelot".to_string(),
            s if s.contains("kyberswap") => "KyberSwap".to_string(),
            s if s.contains("velodrome") => "Velodrome".to_string(),
            s if s.contains("solidly") => "Solidly".to_string(),
            s if s.contains("maverick") => "Maverick".to_string(),
            _ => format!("Other({})", dex_id),
        }
    }
}

async fn run_monitor(
    dex_version_map: Arc<HashMap<String, String>>,
    db_client: Arc<Client>,
) {
    info!("[Monitor] Starting run_monitor() with megafilter and new_pools API...");
    
    // Create/recreate table on each run to ensure clean data
    if let Err(e) = create_pools_table(&db_client).await {
        error!("[Monitor] Failed to create/recreate pools table: {}", e);
        return;
    }
    info!("[Monitor] Successfully created/recreated pools table");
    
    let limiter = Arc::new(Mutex::new(VecDeque::new()));
    let mut all_pools = Vec::new();
    let mut total_written = 0;
    const BATCH_SIZE: usize = 100; // Write every 100 pools
    
    let networks = vec!["eth", "arbitrum", "base", "polygon_pos", "optimism", "bsc", "avax"];

    let num_megafilter_pages: usize = 500; // Increased from 250
    info!("[Monitor] Will fetch {} megafilter pages per run", num_megafilter_pages);

    // Fetch megafilter pages
    let _megafilter_available = true;
    for page in 1..=num_megafilter_pages {
        if !_megafilter_available {
            info!("[Monitor] Skipping megafilter pages due to previous API unavailability");
            break;
        }
        
        info!("[Monitor] Fetching megafilter page {}", page);
        match fetch_pools_megafilter(
            &limiter,
            page,
            Some(networks.clone()),
            Some("h6_trending"),
            Some(MIN_RESERVE_USD)
        ).await {
            Ok(response) => {
                let transformed = transform_to_flattened_pools(&response, &*dex_version_map, "megafilter");
                if transformed.is_empty() {
                    info!("[Monitor] No pools found on page {}, stopping pagination", page);
                    break;
                }
                info!("[Monitor] Fetched {} pools on page {} (after filtering)", transformed.len(), page);
                all_pools.extend(transformed);
                
                // Write to database every BATCH_SIZE pools
                if all_pools.len() >= BATCH_SIZE {
                    info!("[Monitor] Reached {} pools, writing batch to database", all_pools.len());
                    
                    // Deduplicate current batch
                    let mut seen = std::collections::HashSet::new();
                    let mut batch_to_write = Vec::new();
                    for pool in all_pools.drain(..) {
                        if seen.insert(pool.pool_id.clone()) {
                            batch_to_write.push(pool);
                        }
                    }
                    
                    if !batch_to_write.is_empty() {
                        match insert_pools_batch(&db_client, &batch_to_write).await {
                            Ok(_) => {
                                total_written += batch_to_write.len();
                                info!("[Monitor] Successfully wrote batch of {} pools (total written: {})", 
                                      batch_to_write.len(), total_written);
                            }
                            Err(e) => error!("[Monitor] Error inserting batch: {}", e),
                        }
                    }
                }
                
                sleep(Duration::from_millis(500)).await;
            },
            Err(e) => {
                error!("[Monitor] Error fetching megafilter page {}: {}", page, e);
                if e.to_string().contains("404") {
                    warn!("[Monitor] Megafilter API returned 404 - invalid parameters. Skipping remaining pages and using new_pools only");
                    break;
                }
                break;
            }
        }
    }

    info!("[Monitor] Finished megafilter, {} pools pending write", all_pools.len());

    // Fetch new pools with pagination
    let num_new_pools_pages: usize = 200; // Increased from 100
    info!("[Monitor] Will fetch up to {} new_pools pages", num_new_pools_pages);
    
    for page in 1..=num_new_pools_pages {
        info!("[Monitor] Fetching new_pools page {}", page);
        match fetch_pools_new_pools(&limiter, page, Some(networks.clone()), Some(MIN_RESERVE_USD)).await {
            Ok(response) => {
                let transformed = transform_to_flattened_pools(&response, &*dex_version_map, "new_pools");
                if transformed.is_empty() {
                    info!("[Monitor] No pools found on new_pools page {}, stopping pagination", page);
                    break;
                }
                info!("[Monitor] Fetched {} pools from new_pools page {} (after filtering)", transformed.len(), page);
                all_pools.extend(transformed);
                
                // Write to database every BATCH_SIZE pools
                if all_pools.len() >= BATCH_SIZE {
                    info!("[Monitor] Reached {} pools, writing batch to database", all_pools.len());
                    
                    // Deduplicate current batch
                    let mut seen = std::collections::HashSet::new();
                    let mut batch_to_write = Vec::new();
                    for pool in all_pools.drain(..) {
                        if seen.insert(pool.pool_id.clone()) {
                            batch_to_write.push(pool);
                        }
                    }
                    
                    if !batch_to_write.is_empty() {
                        match insert_pools_batch(&db_client, &batch_to_write).await {
                            Ok(_) => {
                                total_written += batch_to_write.len();
                                info!("[Monitor] Successfully wrote batch of {} pools (total written: {})", 
                                      batch_to_write.len(), total_written);
                            }
                            Err(e) => error!("[Monitor] Error inserting batch: {}", e),
                        }
                    }
                }
                
                sleep(Duration::from_millis(500)).await;
            },
            Err(e) => {
                error!("[Monitor] Error fetching new_pools page {}: {}", page, e);
                break;
            }
        }
    }

    // Write any remaining pools
    if !all_pools.is_empty() {
        info!("[Monitor] Writing final batch of {} pools", all_pools.len());
        
        // Deduplicate final batch
        let mut seen = std::collections::HashSet::new();
        let mut final_batch = Vec::new();
        for pool in all_pools {
            if seen.insert(pool.pool_id.clone()) {
                final_batch.push(pool);
            }
        }
        
        if !final_batch.is_empty() {
            match insert_pools_batch(&db_client, &final_batch).await {
                Ok(_) => {
                    total_written += final_batch.len();
                    info!("[Monitor] Successfully wrote final batch of {} pools (total written: {})", 
                          final_batch.len(), total_written);
                }
                Err(e) => error!("[Monitor] Error inserting final batch: {}", e),
            }
        }
    }

    info!("[Monitor] Run complete. Total pools written to database: {}", total_written);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    
    // Load dex config
    let dex_config: serde_json::Value = {
        let data = fs::read_to_string("config/dex_config.json")
            .expect("Failed to read dex_config.json");
        serde_json::from_str(&data).expect("Failed to parse dex_config.json")
    };
    
    // Extract dex version map from the new config format
    let dex_version_map: HashMap<String, String> = {
        let mut map = HashMap::new();
        if let Some(dexes) = dex_config.get("dexes") {
            if let Some(dexes_obj) = dexes.as_object() {
                for (dex_id, dex_config) in dexes_obj {
                    if let Some(version) = dex_config.get("version") {
                        if let Some(version_str) = version.as_str() {
                            map.insert(dex_id.clone(), version_str.to_string());
                        }
                    }
                }
            }
        }
        map
    };
    
    // Connect to database
    let db_client = match connect_db().await {
        Ok(client) => client,
        Err(e) => {
            error!("[Monitor] Failed to connect to database: {}", e);
            return;
        }
    };
    let db_client_arc = Arc::new(db_client);
    
    info!("[Main] Starting single pool fetch run");
    
    // Run once
    run_monitor(Arc::new(dex_version_map), db_client_arc.clone()).await;
    
    info!("[Main] Pool fetch complete, exiting");
}