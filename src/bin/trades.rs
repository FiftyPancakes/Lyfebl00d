use serde::Deserialize;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use std::collections::VecDeque;
use std::error::Error;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn, error};
use tracing_subscriber;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

type RateLimiter = Mutex<VecDeque<Instant>>;

// API Configuration
static API_KEY: &str = "CG-3GBPr8Kx99Y5VfcC5DVTsxkG";
static API_URL_TEMPLATE: &str = "https://pro-api.coingecko.com/api/v3/onchain/networks/{network}/pools/{pool_address}/trades";
const MAX_RETRIES: usize = 3;
const BASE_DELAY: f64 = 2.0;
const MAX_REQUESTS_PER_MINUTE: usize = 500;

// Database Configuration
const PG_HOST: &str = "localhost";
const PG_PORT: u16 = 5432;
const PG_USER: &str = "ichevako";
const PG_DB: &str = "lyfeblood";
const PG_PASS: &str = "maddie";
const TRADES_TABLE_NAME: &str = "onchain_pool_trades";

// Batching Configuration - similar to pools.rs
const BATCH_SIZE: usize = 50; // Process 50 pools at a time
const WRITE_BATCH_SIZE: usize = 100; // Write every 100 trade records

// Flexible deserialization modules
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

mod flexible_i64 {
    use serde::{self, Deserialize, Deserializer};
    use std::str::FromStr;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberVariant {
        Int(i64),
        Float(f64),
        String(String),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<NumberVariant>::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(NumberVariant::Int(i)) => Ok(Some(i)),
            Some(NumberVariant::Float(f)) => Ok(Some(f as i64)),
            Some(NumberVariant::String(s)) => match i64::from_str(&s) {
                Ok(i) => Ok(Some(i)),
                Err(_) => Ok(None),
            },
        }
    }
}

// Database Models - Updated to match pools.rs column names
#[derive(Debug, Clone)]
struct PoolData {
    pool_address: String,
    network_id: String,
    pool_id: String,
}

#[derive(Debug, Clone)]
struct TradeRecord {
    chain_name: String,
    pool_id: String,
    pool_address: String,
    block_number: Option<i64>,
    tx_hash: String,
    tx_from_address: Option<String>,
    from_token_amount: Option<Decimal>,
    to_token_amount: Option<Decimal>,
    price_from_in_currency_token: Option<Decimal>,
    price_to_in_currency_token: Option<Decimal>,
    price_from_in_usd: Option<Decimal>,
    price_to_in_usd: Option<Decimal>,
    block_timestamp: Option<i64>,
    kind: Option<String>,
    volume_in_usd: Option<Decimal>,
    from_token_address: Option<String>,
    to_token_address: Option<String>,
    first_seen: i64,
}

// API Response Models
#[derive(Deserialize, Debug, Clone)]
pub struct TradesResponse {
    pub data: Vec<TradeData>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TradeData {
    pub id: Option<String>,
    pub r#type: Option<String>,
    pub attributes: Option<TradeAttributes>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TradeAttributes {
    #[serde(with = "flexible_i64")]
    pub block_number: Option<i64>,
    pub tx_hash: Option<String>,
    pub tx_from_address: Option<String>,
    #[serde(with = "flexible_f64")]
    pub from_token_amount: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub to_token_amount: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_from_in_currency_token: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_to_in_currency_token: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_from_in_usd: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_to_in_usd: Option<f64>,
    pub block_timestamp: Option<String>,
    pub kind: Option<String>,
    #[serde(with = "flexible_f64")]
    pub volume_in_usd: Option<f64>,
    pub from_token_address: Option<String>,
    pub to_token_address: Option<String>,
}

// Database Connection with Retry
async fn connect_db() -> Result<Client, Box<dyn Error>> {
    let mut attempts = 0;
    let max_attempts = 3;
    let mut delay = 1.0;

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
                if attempts < max_attempts {
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                }
            }
        }
    }

    Err(format!("[DB] Could not connect after {} attempts", max_attempts).into())
}

// Fetch pool data from existing cg_pools table
async fn fetch_pool_data_from_db(client: &Client) -> Result<Vec<PoolData>, Box<dyn Error>> {
    let sql_query = "
        SELECT pool_address, chain_name, pool_id 
        FROM cg_pools 
        WHERE pool_address IS NOT NULL 
        AND chain_name IS NOT NULL 
        AND pool_id IS NOT NULL
        ORDER BY pool_id
        LIMIT 2000
    ";
    
    let rows = client.query(sql_query, &[]).await?;
    let mut pools = Vec::new();
    
    for row in rows {
        pools.push(PoolData {
            pool_address: row.get("pool_address"),
            network_id: row.get("chain_name"), // Map chain_name to network_id for API calls
            pool_id: row.get("pool_id"),
        });
    }
    
    info!("[DB] Fetched {} pools from database", pools.len());
    Ok(pools)
}





// Rate limiting
async fn check_rate_limit(limiter: &Arc<RateLimiter>) {
    let mut rl = limiter.lock().await;
    let now = Instant::now();

    // Remove timestamps older than 60 seconds
    while !rl.is_empty() && now.duration_since(*rl.front().unwrap()) >= Duration::from_secs(60) {
        rl.pop_front();
    }

    // Check if we've hit the rate limit
    if rl.len() >= MAX_REQUESTS_PER_MINUTE {
        let earliest = *rl.front().unwrap();
        let wait_time = Duration::from_secs(60) - now.duration_since(earliest);
        warn!("[RateLimit] Reached {} reqs/60s, sleeping {:.2}s", 
              MAX_REQUESTS_PER_MINUTE, wait_time.as_secs_f32());
        drop(rl); // Release the lock before sleeping
        sleep(wait_time).await;
        
        // Reacquire lock and clear old timestamps
        let mut rl = limiter.lock().await;
        let now = Instant::now();
        while !rl.is_empty() && now.duration_since(*rl.front().unwrap()) >= Duration::from_secs(60) {
            rl.pop_front();
        }
        rl.push_back(Instant::now());
    } else {
        rl.push_back(Instant::now());
    }
}

// Fetch trades for a single pool with retry logic - Updated URL construction
async fn fetch_pool_trades(
    limiter: &Arc<RateLimiter>,
    network_id: &str,
    pool_address: &str,
) -> Result<Option<TradesResponse>, Box<dyn Error + Send + Sync>> {
    let client = reqwest::Client::new();
    // Updated URL construction to match the API documentation
    let url = API_URL_TEMPLATE
        .replace("{network}", network_id)
        .replace("{pool_address}", pool_address);
    
    let mut delay = BASE_DELAY;
    
    for attempt in 1..=MAX_RETRIES {
        check_rate_limit(limiter).await;
        
        let mut headers = HeaderMap::new();
        headers.insert("x-cg-pro-api-key", HeaderValue::from_static(API_KEY));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        
        let params = [
            ("trade_volume_in_usd_greater_than", "0"),
            ("token", "base")
        ];
        
        match client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                
                if status.is_success() {
                    let text = response.text().await?;
                    match serde_json::from_str::<TradesResponse>(&text) {
                        Ok(data) => {
                            info!("[API] Successfully fetched trades for {}/{}", network_id, pool_address);
                            return Ok(Some(data));
                        }
                        Err(e) => {
                            warn!("[API] JSON parse error for {}/{}: {}", network_id, pool_address, e);
                            return Ok(None);
                        }
                    }
                } else if status.as_u16() == 404 {
                    warn!("[404] Pool not found: {}/{}", network_id, pool_address);
                    return Ok(None);
                } else if status.as_u16() == 429 {
                    warn!("[429] Rate-limited fetching '{}/{}' (attempt {}/{}). Sleeping {:.1}s...",
                          network_id, pool_address, attempt, MAX_RETRIES, delay);
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else {
                    let error_text = response.text().await.unwrap_or_default();
                    warn!("[ERROR {}] Unable to fetch '{}/{}': {}", 
                          status, network_id, pool_address, error_text);
                    return Ok(None);
                }
            }
            Err(e) => {
                error!("[EXCEPTION] Failed fetching '{}/{}': {}", network_id, pool_address, e);
                if attempt == MAX_RETRIES {
                    return Ok(None);
                }
                sleep(Duration::from_secs_f64(delay)).await;
                delay *= 2.0;
            }
        }
    }
    
    warn!("Failed to fetch '{}/{}' after {} attempts", network_id, pool_address, MAX_RETRIES);
    Ok(None)
}

// NEW: Batch fetch trades for multiple pools - similar to pools.rs batch processing
async fn fetch_trades_batch(
    limiter: &Arc<RateLimiter>,
    pools: &[PoolData],
) -> Vec<(PoolData, Option<TradesResponse>)> {
    let mut results = Vec::new();
    
    info!("[Batch] Processing batch of {} pools", pools.len());
    
    for (idx, pool) in pools.iter().enumerate() {
        info!("[Batch] Processing pool {}/{}: {}/{}", 
              idx + 1, pools.len(), pool.network_id, pool.pool_address);
        
        let trades_data = fetch_pool_trades(
            limiter,
            &pool.network_id,
            &pool.pool_address,
        ).await.unwrap_or(None);
        
        results.push((pool.clone(), trades_data));
        
        // Small delay between requests to be nice to the API
        sleep(Duration::from_millis(200)).await;
    }
    
    info!("[Batch] Completed batch of {} pools", pools.len());
    results
}



// Transform API response to database records
async fn transform_trades_data(
    client: &Client,
    pool_trades: &[(PoolData, Option<TradesResponse>)],
) -> Result<Vec<TradeRecord>, Box<dyn Error>> {
    let mut transformed_data = Vec::new();
    let now_unix = Utc::now().timestamp();
    
    // Prepare statement to check for existing trades
    let check_trade_stmt = client.prepare(&format!(
        "SELECT 1 FROM {} WHERE pool_address = $1 AND tx_hash = $2",
        TRADES_TABLE_NAME
    )).await?;
    
    for (pool_data, trades_response) in pool_trades {
        if let Some(response) = trades_response {
            info!("[Transform] Processing {} trades for pool {}", 
                  response.data.len(), pool_data.pool_address);
            
            for trade_data in &response.data {
                if let Some(attributes) = &trade_data.attributes {
                    // Extract required fields
                    let tx_hash = attributes.tx_hash.clone().unwrap_or_default();
                    if tx_hash.is_empty() {
                        continue; // Skip trades without tx_hash
                    }
                    
                    // Check if trade already exists in database
                    let exists = client
                        .query_opt(&check_trade_stmt, &[&pool_data.pool_address, &tx_hash])
                        .await?;
                    
                    if exists.is_some() {
                        continue; // Skip existing trades
                    }
                    
                    // Parse timestamp
                    let block_timestamp = attributes.block_timestamp
                        .as_ref()
                        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                        .map(|dt| dt.timestamp());
                    
                    let trade_record = TradeRecord {
                        chain_name: pool_data.network_id.clone(),
                        pool_id: pool_data.pool_id.clone(),
                        pool_address: pool_data.pool_address.clone(),
                        block_number: attributes.block_number,
                        tx_hash,
                        tx_from_address: attributes.tx_from_address.clone(),
                        from_token_amount: attributes.from_token_amount.and_then(Decimal::from_f64),
                        to_token_amount: attributes.to_token_amount.and_then(Decimal::from_f64),
                        price_from_in_currency_token: attributes.price_from_in_currency_token.and_then(Decimal::from_f64),
                        price_to_in_currency_token: attributes.price_to_in_currency_token.and_then(Decimal::from_f64),
                        price_from_in_usd: attributes.price_from_in_usd.and_then(Decimal::from_f64),
                        price_to_in_usd: attributes.price_to_in_usd.and_then(Decimal::from_f64),
                        block_timestamp,
                        kind: attributes.kind.clone(),
                        volume_in_usd: attributes.volume_in_usd.and_then(Decimal::from_f64),
                        from_token_address: attributes.from_token_address.clone(),
                        to_token_address: attributes.to_token_address.clone(),
                        first_seen: now_unix,
                    };
                    
                    transformed_data.push(trade_record);
                }
            }
        } else {
            warn!("[Transform] No trade data for pool {}/{}", 
                  pool_data.network_id, pool_data.pool_id);
        }
    }
    
    info!("[Transform] Transformed {} trade records", transformed_data.len());
    Ok(transformed_data)
}

// Create trades table if it doesn't exist
async fn create_trades_table_if_not_exists(client: &Client) -> Result<(), Box<dyn Error>> {
    // Check if trades table exists
    let table_exists = client
        .query_opt(
            "SELECT 1 FROM information_schema.tables WHERE table_name = $1",
            &[&TRADES_TABLE_NAME],
        )
        .await?;

    if table_exists.is_some() {
        info!("[DB] Table '{}' already exists", TRADES_TABLE_NAME);
        return Ok(());
    }

    info!("[DB] Creating table '{}'...", TRADES_TABLE_NAME);
    
    let create_sql = format!(
        "CREATE TABLE {} (
            chain_name TEXT NOT NULL,
            pool_id TEXT NOT NULL,
            pool_address TEXT NOT NULL,
            block_number BIGINT,
            tx_hash TEXT NOT NULL,
            tx_from_address TEXT,
            from_token_amount NUMERIC,
            to_token_amount NUMERIC,
            price_from_in_currency_token NUMERIC,
            price_to_in_currency_token NUMERIC,
            price_from_in_usd NUMERIC,
            price_to_in_usd NUMERIC,
            block_timestamp BIGINT,
            kind TEXT,
            volume_in_usd NUMERIC,
            from_token_address TEXT,
            to_token_address TEXT,
            first_seen BIGINT NOT NULL,
            PRIMARY KEY (pool_address, tx_hash)
        )",
        TRADES_TABLE_NAME
    );
    
    client.execute(&create_sql, &[]).await?;
    
    // Create indexes
    let indexes = vec![
        format!("CREATE INDEX idx_{}_network ON {} (chain_name)", TRADES_TABLE_NAME, TRADES_TABLE_NAME),
        format!("CREATE INDEX idx_{}_pool ON {} (pool_id)", TRADES_TABLE_NAME, TRADES_TABLE_NAME),
        format!("CREATE INDEX idx_{}_timestamp ON {} (block_timestamp)", TRADES_TABLE_NAME, TRADES_TABLE_NAME),
        format!("CREATE INDEX idx_{}_kind ON {} (kind)", TRADES_TABLE_NAME, TRADES_TABLE_NAME),
    ];
    
    for index_sql in indexes {
        client.execute(&index_sql, &[]).await?;
    }
    
    info!("[DB] Created new table '{}' with indexes", TRADES_TABLE_NAME);
    Ok(())
}

// NEW: Batch insert trades - similar to pools.rs insert_pools_batch
async fn insert_trades_batch(
    client: &Client,
    trades: &[TradeRecord],
) -> Result<(), Box<dyn Error>> {
    if trades.is_empty() {
        warn!("[DB] No trades to insert - empty batch");
        return Ok(());
    }

    info!("[DB] Starting batch insert of {} trades", trades.len());
    
    let insert_sql = format!(
        "INSERT INTO {} (
            chain_name, pool_id, pool_address,
            block_number, tx_hash, tx_from_address, from_token_amount, to_token_amount,
            price_from_in_currency_token, price_to_in_currency_token,
            price_from_in_usd, price_to_in_usd, block_timestamp, kind,
            volume_in_usd, from_token_address, to_token_address, first_seen
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
            $11, $12, $13, $14, $15, $16, $17, $18
        )
        ON CONFLICT (pool_address, tx_hash) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            tx_from_address = EXCLUDED.tx_from_address,
            from_token_amount = EXCLUDED.from_token_amount,
            to_token_amount = EXCLUDED.to_token_amount,
            price_from_in_currency_token = EXCLUDED.price_from_in_currency_token,
            price_to_in_currency_token = EXCLUDED.price_to_in_currency_token,
            price_from_in_usd = EXCLUDED.price_from_in_usd,
            price_to_in_usd = EXCLUDED.price_to_in_usd,
            block_timestamp = EXCLUDED.block_timestamp,
            kind = EXCLUDED.kind,
            volume_in_usd = EXCLUDED.volume_in_usd,
            from_token_address = EXCLUDED.from_token_address,
            to_token_address = EXCLUDED.to_token_address",
        TRADES_TABLE_NAME
    );
    
    let statement = client.prepare(&insert_sql).await?;
    
    let mut inserted_count = 0;
    let mut error_count = 0;
    
    for (idx, trade) in trades.iter().enumerate() {
        match client.execute(&statement, &[
            &trade.chain_name,
            &trade.pool_id,
            &trade.pool_address,
            &trade.block_number,
            &trade.tx_hash,
            &trade.tx_from_address,
            &trade.from_token_amount,
            &trade.to_token_amount,
            &trade.price_from_in_currency_token,
            &trade.price_to_in_currency_token,
            &trade.price_from_in_usd,
            &trade.price_to_in_usd,
            &trade.block_timestamp,
            &trade.kind,
            &trade.volume_in_usd,
            &trade.from_token_address,
            &trade.to_token_address,
            &trade.first_seen,
        ]).await {
            Ok(rows_affected) => {
                inserted_count += 1;
                if idx < 3 {  // Log first few for debugging
                    info!("[DB] Inserted trade {}: {} (rows affected: {})", idx + 1, trade.tx_hash, rows_affected);
                }
            }
            Err(e) => {
                error_count += 1;
                error!("[DB] Failed to insert trade {}: {} - Error: {}", idx + 1, trade.tx_hash, e);
                if error_count > 10 {
                    return Err(format!("Too many insert errors: {}", e).into());
                }
            }
        }
    }
    
    info!("[DB] Batch insert completed: {} successful, {} failed out of {} total", 
          inserted_count, error_count, trades.len());
    
    // Verify data was actually written
    let count_result = client.query_one(
        &format!("SELECT COUNT(*) FROM {}", TRADES_TABLE_NAME),
        &[]
    ).await?;
    let total_count: i64 = count_result.get(0);
    info!("[DB] Total rows in {} table after insert: {}", TRADES_TABLE_NAME, total_count);
    
    Ok(())
}

// NEW: Main batched processing function - similar to pools.rs run_monitor
async fn run_batched_trade_collection(client: Arc<Client>) -> Result<(), Box<dyn Error>> {
    info!("[Main] Starting batched trade collection...");
    
    // 1) Create trades table if it doesn't exist
    create_trades_table_if_not_exists(&client).await?;
    
    // 2) Fetch pool data from database
    let pools = fetch_pool_data_from_db(&client).await?;
    if pools.is_empty() {
        warn!("[Main] No pool data found in database.");
        return Ok(());
    }
    
    // 3) Setup rate limiter
    let limiter = Arc::new(Mutex::new(VecDeque::new()));
    
    // 4) Process pools in batches
    let mut all_trades = Vec::new();
    let mut total_written = 0;
    
    info!("[Main] Processing {} pools in batches of {}", pools.len(), BATCH_SIZE);
    
    for (batch_num, pool_batch) in pools.chunks(BATCH_SIZE).enumerate() {
        info!("[Main] Processing batch {} ({} pools)", batch_num + 1, pool_batch.len());
        
        // Fetch trades for this batch of pools
        let batch_trades = fetch_trades_batch(&limiter, pool_batch).await;
        
        // Transform to database records
        let transformed_trades = transform_trades_data(&client, &batch_trades).await?;
        
        info!("[Main] Batch {} generated {} trade records", batch_num + 1, transformed_trades.len());
        
        // Add to accumulator
        all_trades.extend(transformed_trades);
        
        // Write to database when we have enough records
        if all_trades.len() >= WRITE_BATCH_SIZE {
            info!("[Main] Reached {} trades, writing batch to database", all_trades.len());
            
            // Deduplicate current batch by (pool_address, tx_hash)
            let mut seen = std::collections::HashSet::new();
            let mut batch_to_write = Vec::new();
            for trade in all_trades.drain(..) {
                let key = (trade.pool_address.clone(), trade.tx_hash.clone());
                if seen.insert(key) {
                    batch_to_write.push(trade);
                }
            }
            
            if !batch_to_write.is_empty() {
                insert_trades_batch(&client, &batch_to_write).await?;
                total_written += batch_to_write.len();
                info!("[Main] Successfully wrote batch of {} trades (total written: {})", 
                      batch_to_write.len(), total_written);
            }
        }
        
        // Small delay between batches
        sleep(Duration::from_millis(500)).await;
    }
    
    // Write any remaining trades
    if !all_trades.is_empty() {
        info!("[Main] Writing final batch of {} trades", all_trades.len());
        
        // Deduplicate final batch
        let mut seen = std::collections::HashSet::new();
        let mut final_batch = Vec::new();
        for trade in all_trades {
            let key = (trade.pool_address.clone(), trade.tx_hash.clone());
            if seen.insert(key) {
                final_batch.push(trade);
            }
        }
        
        if !final_batch.is_empty() {
            insert_trades_batch(&client, &final_batch).await?;
            total_written += final_batch.len();
            info!("[Main] Successfully wrote final batch of {} trades (total written: {})", 
                  final_batch.len(), total_written);
        }
    }
    
    info!("[Main] Batched trade collection complete. Total trades written: {}", total_written);
    Ok(())
}

// Main execution function
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    
    info!("[Start] Fetching and storing on-chain pool trade data with batching.");
    
    // Connect to database
    let client = connect_db().await?;
    let client_arc = Arc::new(client);
    
    // Run batched collection
    run_batched_trade_collection(client_arc).await?;
    
    info!("[End] Finished processing on-chain pool trade data.");
    Ok(())
}