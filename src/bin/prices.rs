use serde::Deserialize;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::time::{Duration, Instant};
use chrono::{Utc};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn, error};
use tracing_subscriber;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

type RateLimiter = Mutex<VecDeque<Instant>>;

static API_KEY: &str = "CG-3GBPr8Kx99Y5VfcC5DVTsxkG";
const MAX_RETRIES: usize = 5;
const BASE_DELAY: f64 = 1.0;
const MAX_REQUESTS_PER_MINUTE: usize = 500;
// const MIN_RESERVE_USD: f64 = 100.0; // Removed as unused

const PG_HOST: &str = "localhost";
const PG_PORT: u16 = 5432;
const PG_USER: &str = "ichevako";
const PG_DB: &str = "lyfeblood";
const PG_PASS: &str = "maddie";

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

// Token struct is never constructed, so remove it

#[derive(Debug, Clone)]
struct OhlcData {
    token_id: String,
    timestamp: i64,
    timeframe: String,
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    volume: Option<Decimal>,
    market_cap: Option<Decimal>,
    first_seen: i64,
}

#[derive(Debug, Clone)]
struct MarketTrend {
    trend_id: String,
    token_id: String,
    trend_type: String,
    rank: Option<i32>,
    score: Option<Decimal>,
    price_btc: Option<Decimal>,
    volume_24h: Option<Decimal>,
    market_cap: Option<Decimal>,
    search_count: Option<i64>,
    timestamp: i64,
}

#[derive(Debug, Clone)]
struct Network {
    chain_name: String,  // Changed from network_id to chain_name for database storage
    name: String,
    chain_identifier: Option<i64>,
    native_currency_id: Option<String>,
    native_currency_symbol: Option<String>,
    rpc_url: Option<String>,
    block_explorer_url: Option<String>,
    is_testnet: bool,
    first_seen: i64,
    last_updated: i64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CoinListResponse {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub platforms: Option<HashMap<String, Option<String>>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CoinsMarketsResponse {
    pub id: String,
    pub symbol: String,
    pub name: String,
    #[serde(with = "flexible_f64")]
    pub current_price: Option<f64>,
    #[serde(with = "flexible_i64")]
    pub market_cap: Option<i64>,
    #[serde(with = "flexible_f64")]
    pub market_cap_rank: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub fully_diluted_valuation: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub total_volume: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub high_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub low_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_change_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_change_percentage_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub price_change_percentage_7d_in_currency: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub market_cap_change_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub market_cap_change_percentage_24h: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub circulating_supply: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub total_supply: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub max_supply: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub ath: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub ath_change_percentage: Option<f64>,
    pub ath_date: Option<String>,
    #[serde(with = "flexible_f64")]
    pub atl: Option<f64>,
    #[serde(with = "flexible_f64")]
    pub atl_change_percentage: Option<f64>,
    pub atl_date: Option<String>,
    pub roi: Option<serde_json::Value>,
    pub last_updated: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MarketChartResponse {
    pub prices: Vec<[f64; 2]>,
    pub market_caps: Vec<[f64; 2]>,
    pub total_volumes: Vec<[f64; 2]>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingResponse {
    pub coins: Vec<TrendingCoin>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingCoin {
    pub item: TrendingCoinItem,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingCoinItem {
    pub id: String,
    pub coin_id: Option<i64>,
    pub name: String,
    pub symbol: String,
    pub market_cap_rank: Option<i64>,
    pub thumb: String,
    pub small: String,
    pub large: String,
    #[serde(default)]
    pub slug: Option<String>,
    pub price_btc: f64,
    pub score: i64,
    pub data: Option<TrendingCoinExtra>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingCoinExtra {
    pub market_cap: String,
    pub total_volume: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingNft {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub thumb: String,
    pub nft_contract_id: i64,
    pub native_currency_symbol: String,
    pub floor_price_in_native_currency: f64,
    pub floor_price_24h_percentage_change: f64,
    pub data: TrendingNftData,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingNftData {
    pub floor_price: String,
    pub floor_price_in_usd_24h_percentage_change: String,
    pub h24_volume: String,
    pub h24_average_sale_price: String,
    pub sparkline: String,
    pub content: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingCategory {
    pub id: i64,
    pub name: String,
    pub market_cap_1h_change: f64,
    pub slug: String,
    pub coins_count: i64,
    pub data: TrendingCategoryData,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrendingCategoryData {
    pub market_cap: f64,
    pub market_cap_btc: f64,
    pub total_volume: f64,
    pub total_volume_btc: f64,
    pub market_cap_change_percentage_24h: HashMap<String, f64>,
    pub sparkline: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TopGainersLosersResponse {
    pub top_gainers: Vec<GainerLoserCoin>,
    pub top_losers: Vec<GainerLoserCoin>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GainerLoserCoin {
    pub id: String,
    pub symbol: String,
    pub name: String,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(
        with = "flexible_f64",
        alias = "usd",
        alias = "price",
        alias = "current_price"
    )]
    pub current_price: Option<f64>,
    #[serde(
        with = "flexible_f64",
        alias = "usd_24h_change",
        alias = "price_change_percentage_24h"
    )]
    pub price_change_percentage_24h: Option<f64>,
    #[serde(
        with = "flexible_f64",
        alias = "usd_24h_vol",
        alias = "total_volume"
    )]
    pub usd_24h_vol: Option<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AssetPlatformsResponse {
    pub id: String,
    pub chain_identifier: Option<i64>,
    pub name: String,
    pub shortname: String,
    pub native_currency_id: Option<String>,
}

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

async fn create_all_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let drop_queries = vec![
        "DROP TABLE IF EXISTS market_trends CASCADE",
        "DROP TABLE IF EXISTS ohlc_data CASCADE", 
        "DROP TABLE IF EXISTS tokens CASCADE",
        "DROP TABLE IF EXISTS networks CASCADE",
    ];

    for query in drop_queries {
        client.execute(query, &[]).await?;
    }

    let create_networks = "
        CREATE TABLE networks (
            chain_name TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            chain_identifier BIGINT,
            native_currency_id TEXT,
            native_currency_symbol TEXT,
            rpc_url TEXT,
            block_explorer_url TEXT,
            is_testnet BOOLEAN DEFAULT FALSE,
            first_seen BIGINT NOT NULL,
            last_updated BIGINT NOT NULL
        )";

    let create_tokens = "
        CREATE TABLE tokens (
            token_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            contract_address TEXT,
            network_id TEXT,
            decimals INTEGER,
            current_price_usd NUMERIC,
            market_cap_usd NUMERIC,
            total_volume_24h NUMERIC,
            price_change_24h NUMERIC,
            price_change_7d NUMERIC,
            ath_usd NUMERIC,
            ath_date BIGINT,
            atl_usd NUMERIC,
            atl_date BIGINT,
            circulating_supply NUMERIC,
            total_supply NUMERIC,
            max_supply NUMERIC,
            fdv_usd NUMERIC,
            first_seen BIGINT NOT NULL,
            last_updated BIGINT NOT NULL
        )";

    let create_ohlc = "
        CREATE TABLE ohlc_data (
            token_id TEXT NOT NULL,
            timestamp BIGINT NOT NULL,
            timeframe TEXT NOT NULL,
            open_price NUMERIC NOT NULL,
            high_price NUMERIC NOT NULL,
            low_price NUMERIC NOT NULL,
            close_price NUMERIC NOT NULL,
            volume NUMERIC,
            market_cap NUMERIC,
            first_seen BIGINT NOT NULL,
            PRIMARY KEY (token_id, timestamp, timeframe)
        )";

    let create_trends = "
        CREATE TABLE market_trends (
            trend_id TEXT PRIMARY KEY,
            token_id TEXT NOT NULL,
            trend_type TEXT NOT NULL,
            rank INTEGER,
            score NUMERIC,
            price_btc NUMERIC,
            volume_24h NUMERIC,
            market_cap NUMERIC,
            search_count BIGINT,
            timestamp BIGINT NOT NULL
        )";

    let tables = vec![
        ("networks", create_networks),
        ("tokens", create_tokens),
        ("ohlc_data", create_ohlc),
        ("market_trends", create_trends),
    ];

    for (name, query) in tables {
        client.execute(query, &[]).await?;
        info!("[DB] Created table: {}", name);
    }

    let indexes = vec![
        "CREATE INDEX idx_tokens_symbol ON tokens (symbol)",
        "CREATE INDEX idx_tokens_network ON tokens (network_id)",
        "CREATE INDEX idx_tokens_price ON tokens (current_price_usd)",
        "CREATE INDEX idx_ohlc_token_time ON ohlc_data (token_id, timestamp)",
        "CREATE INDEX idx_ohlc_timeframe ON ohlc_data (timeframe)",
        "CREATE INDEX idx_trends_type ON market_trends (trend_type)",
        "CREATE INDEX idx_trends_timestamp ON market_trends (timestamp)",
        "CREATE INDEX idx_trends_rank ON market_trends (rank)",
    ];

    for index_sql in indexes {
        client.execute(index_sql, &[]).await?;
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

async fn fetch_with_retry<T: for<'de> Deserialize<'de>>(
    limiter: &Arc<RateLimiter>,
    url: &str,
    params: &[(&str, String)],
    endpoint_name: &str,
) -> Result<T, Box<dyn Error + Send + Sync>> {
    let mut delay = BASE_DELAY;
    let client = reqwest::Client::new();

    for attempt in 0..MAX_RETRIES {
        check_rate_limit(limiter).await;

        let mut headers = HeaderMap::new();
        headers.insert("x-cg-pro-api-key", HeaderValue::from_static(API_KEY));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let resp = client
            .get(url)
            .headers(headers)
            .query(params)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().await?;

                if status.is_success() {
                    let response: T = serde_json::from_str(&text)?;
                    tokio::time::sleep(Duration::from_millis(100 + rand::random::<u64>() % 200)).await;
                    return Ok(response);
                } else if status.as_u16() == 429 {
                    warn!("[429] {}: Attempt {}/{} backoff {:.1}s", endpoint_name, attempt + 1, MAX_RETRIES, delay);
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else if [500, 502, 503, 504].contains(&status.as_u16()) {
                    warn!("[HTTP {}] {}: attempt {}/{}, delay={:.1}s", status, endpoint_name, attempt + 1, MAX_RETRIES, delay);
                    sleep(Duration::from_secs_f64(delay)).await;
                    delay *= 2.0;
                } else {
                    warn!("[HTTP {}] {}: {}", status, endpoint_name, text);
                    return Err(format!("HTTP error {} for {}", status, endpoint_name).into());
                }
            }
            Err(e) => {
                error!("[FetchError] {} => {}", endpoint_name, e);
                sleep(Duration::from_secs_f64(delay)).await;
                delay *= 2.0;
            }
        }
    }
    Err(format!("Max retries exceeded for {}", endpoint_name).into())
}

async fn fetch_asset_platforms(limiter: &Arc<RateLimiter>) -> Result<Vec<AssetPlatformsResponse>, Box<dyn Error + Send + Sync>> {
    let url = "https://pro-api.coingecko.com/api/v3/asset_platforms";
    let params = vec![];
    fetch_with_retry(limiter, url, &params, "asset_platforms").await
}

// fetch_coins_list is never used, so remove it
/*
async fn fetch_coins_list(limiter: &Arc<RateLimiter>) -> Result<Vec<CoinListResponse>, Box<dyn Error + Send + Sync>> {
    let url = "https://pro-api.coingecko.com/api/v3/coins/list";
    let params = vec![("include_platform", "true".to_string())];
    fetch_with_retry(limiter, url, &params, "coins_list").await
}
*/

async fn fetch_coins_markets(limiter: &Arc<RateLimiter>, page: usize, per_page: usize) -> Result<Vec<CoinsMarketsResponse>, Box<dyn Error + Send + Sync>> {
    let url = "https://pro-api.coingecko.com/api/v3/coins/markets";
    let params = vec![
        ("vs_currency", "usd".to_string()),
        ("order", "market_cap_desc".to_string()),
        ("per_page", per_page.to_string()),
        ("page", page.to_string()),
        ("sparkline", "false".to_string()),
        ("price_change_percentage", "24h,7d".to_string()),
        ("locale", "en".to_string()),
    ];
    fetch_with_retry(limiter, url, &params, "coins_markets").await
}

async fn fetch_coin_ohlc(
    limiter: &Arc<RateLimiter>,
    coin_id: &str,
    days: u32,
) -> Result<Vec<[f64; 5]>, Box<dyn Error + Send + Sync>> {
    let url = format!(
        "https://pro-api.coingecko.com/api/v3/coins/{}/ohlc",
        coin_id
    );
    let params = vec![
        ("vs_currency", "usd".to_string()),
        ("days", days.to_string()),
    ];

    // The endpoint returns a raw JSON array of arrays.
    let data: Vec<[f64; 5]> = fetch_with_retry(
        limiter,
        &url,
        &params,
        &format!("ohlc_{}", coin_id),
    )
    .await?;
    Ok(data)
}

async fn fetch_coin_market_chart(limiter: &Arc<RateLimiter>, coin_id: &str, days: u32) -> Result<MarketChartResponse, Box<dyn Error + Send + Sync>> {
    let url = format!("https://pro-api.coingecko.com/api/v3/coins/{}/market_chart", coin_id);
    let params = vec![
        ("vs_currency", "usd".to_string()),
        ("days", days.to_string()),
        ("interval", if days <= 1 { "hourly" } else { "daily" }.to_string()),
    ];
    fetch_with_retry(limiter, &url, &params, &format!("market_chart_{}", coin_id)).await
}

async fn fetch_trending(limiter: &Arc<RateLimiter>) -> Result<TrendingResponse, Box<dyn Error + Send + Sync>> {
    let url = "https://pro-api.coingecko.com/api/v3/search/trending?show_max=coin";
    let params = vec![];
    fetch_with_retry(limiter, url, &params, "trending").await
}

async fn fetch_top_gainers_losers(limiter: &Arc<RateLimiter>) -> Result<TopGainersLosersResponse, Box<dyn Error + Send + Sync>> {
    let url = "https://pro-api.coingecko.com/api/v3/coins/top_gainers_losers";
    let params = vec![("vs_currency", "usd".to_string())];
    fetch_with_retry(limiter, url, &params, "top_gainers_losers").await
}

async fn insert_networks_batch(client: &Client, networks: &[Network]) -> Result<(), Box<dyn std::error::Error>> {
    if networks.is_empty() {
        return Ok(());
    }

    let statement = client.prepare(
        "INSERT INTO networks (
            chain_name, name, chain_identifier, native_currency_id, native_currency_symbol,
            rpc_url, block_explorer_url, is_testnet, first_seen, last_updated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (chain_name) DO UPDATE SET
            name = EXCLUDED.name,
            chain_identifier = EXCLUDED.chain_identifier,
            native_currency_id = EXCLUDED.native_currency_id,
            native_currency_symbol = EXCLUDED.native_currency_symbol,
            last_updated = EXCLUDED.last_updated"
    ).await?;

    for network in networks {
        client.execute(&statement, &[
            &network.chain_name,
            &network.name,
            &network.chain_identifier,
            &network.native_currency_id,
            &network.native_currency_symbol,
            &network.rpc_url,
            &network.block_explorer_url,
            &network.is_testnet,
            &network.first_seen,
            &network.last_updated,
        ]).await?;
    }

    info!("[DB] Inserted {} networks", networks.len());
    Ok(())
}

async fn insert_ohlc_batch(client: &Client, ohlc_data: &[OhlcData]) -> Result<(), Box<dyn std::error::Error>> {
    if ohlc_data.is_empty() {
        return Ok(());
    }

    let statement = client.prepare(
        "INSERT INTO ohlc_data (
            token_id, timestamp, timeframe, open_price, high_price, low_price, close_price,
            volume, market_cap, first_seen
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (token_id, timestamp, timeframe) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            market_cap = EXCLUDED.market_cap"
    ).await?;

    for ohlc in ohlc_data {
        client.execute(&statement, &[
            &ohlc.token_id,
            &ohlc.timestamp,
            &ohlc.timeframe,
            &ohlc.open_price,
            &ohlc.high_price,
            &ohlc.low_price,
            &ohlc.close_price,
            &ohlc.volume,
            &ohlc.market_cap,
            &ohlc.first_seen,
        ]).await?;
    }

    info!("[DB] Inserted {} OHLC records", ohlc_data.len());
    Ok(())
}

async fn insert_trends_batch(client: &Client, trends: &[MarketTrend]) -> Result<(), Box<dyn std::error::Error>> {
    if trends.is_empty() {
        return Ok(());
    }

    let statement = client.prepare(
        "INSERT INTO market_trends (
            trend_id, token_id, trend_type, rank, score, price_btc, volume_24h,
            market_cap, search_count, timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (trend_id) DO UPDATE SET
            rank = EXCLUDED.rank,
            score = EXCLUDED.score,
            price_btc = EXCLUDED.price_btc,
            volume_24h = EXCLUDED.volume_24h,
            market_cap = EXCLUDED.market_cap,
            search_count = EXCLUDED.search_count,
            timestamp = EXCLUDED.timestamp"
    ).await?;

    for trend in trends {
        client.execute(&statement, &[
            &trend.trend_id,
            &trend.token_id,
            &trend.trend_type,
            &trend.rank,
            &trend.score,
            &trend.price_btc,
            &trend.volume_24h,
            &trend.market_cap,
            &trend.search_count,
            &trend.timestamp,
        ]).await?;
    }

    info!("[DB] Inserted {} trend records", trends.len());
    Ok(())
}

// normalize_network_id is never used, so remove it
/*
fn normalize_network_id(network_id: &str) -> String {
    match network_id.to_lowercase().as_str() {
        "ethereum" | "eth" | "mainnet" => "ethereum".to_string(),
        "arbitrum" | "arbitrum-one" | "arb" => "arbitrum".to_string(),
        "avalanche" | "avax" | "avalanche-c-chain" => "avalanche".to_string(),
        "base" => "base".to_string(),
        "binance-smart-chain" | "bsc" | "bnb" => "binance-smart-chain".to_string(),
        "polygon" | "polygon-pos" | "matic" => "polygon-pos".to_string(),
        "solana" | "sol" => "solana".to_string(),
        "optimism" => "optimism".to_string(),
        _ => network_id.to_string(),
    }
}
*/

// iso8601_to_unix is never used, so remove it
/*
fn iso8601_to_unix(ts: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(ts).ok().map(|dt| dt.timestamp())
}
*/

fn transform_asset_platforms_to_networks(platforms: &[AssetPlatformsResponse]) -> Vec<Network> {
    let now_unix = Utc::now().timestamp();
    let mut networks = Vec::new();

    for platform in platforms {
        networks.push(Network {
            chain_name: platform.id.clone(), // API uses "id" as network_id, we store as chain_name
            name: platform.name.clone(),
            chain_identifier: platform.chain_identifier,
            native_currency_id: platform.native_currency_id.clone(),
            native_currency_symbol: None,
            rpc_url: None,
            block_explorer_url: None,
            is_testnet: false,
            first_seen: now_unix,
            last_updated: now_unix,
        });
    }

    networks.push(Network {
        chain_name: "mainnet".to_string(), // API uses "id" as network_id, we store as chain_name
        name: "Mainnet (Non-Platform Tokens)".to_string(),
        chain_identifier: None,
        native_currency_id: None,
        native_currency_symbol: None,
        rpc_url: None,
        block_explorer_url: None,
        is_testnet: false,
        first_seen: now_unix,
        last_updated: now_unix,
    });

    networks
}

// transform_coins_list_to_tokens is never used, so remove it
/*
fn transform_coins_list_to_tokens(coins: &[CoinListResponse]) -> Vec<Token> {
    let now_unix = Utc::now().timestamp();
    let mut tokens = Vec::new();

    for coin in coins {
        if let Some(platforms) = &coin.platforms {
            for (network_id, contract_address) in platforms {
                let network_normalized = normalize_network_id(network_id);
                if let Some(addr) = contract_address {
                    if !addr.is_empty() {
                        tokens.push(Token {
                            token_id: format!("{}_{}", coin.id, network_normalized),
                            symbol: coin.symbol.clone(),
                            name: coin.name.clone(),
                            contract_address: Some(addr.clone()),
                            network_id: network_normalized,
                            decimals: None,
                            current_price_usd: None,
                            market_cap_usd: None,
                            total_volume_24h: None,
                            price_change_24h: None,
                            price_change_7d: None,
                            ath_usd: None,
                            ath_date: None,
                            atl_usd: None,
                            atl_date: None,
                            circulating_supply: None,
                            total_supply: None,
                            max_supply: None,
                            fdv_usd: None,
                            first_seen: now_unix,
                            last_updated: now_unix,
                        });
                    }
                }
            }
        }
        
        tokens.push(Token {
            token_id: coin.id.clone(),
            symbol: coin.symbol.clone(),
            name: coin.name.clone(),
            contract_address: None,
            network_id: "mainnet".to_string(),
            decimals: None,
            current_price_usd: None,
            market_cap_usd: None,
            total_volume_24h: None,
            price_change_24h: None,
            price_change_7d: None,
            ath_usd: None,
            ath_date: None,
            atl_usd: None,
            atl_date: None,
            circulating_supply: None,
            total_supply: None,
            max_supply: None,
            fdv_usd: None,
            first_seen: now_unix,
            last_updated: now_unix,
        });
    }

    tokens
}
*/

// transform_coins_markets_to_tokens is never used, so remove it
/*
fn transform_coins_markets_to_tokens(coins: &[CoinsMarketsResponse]) -> Vec<Token> {
    let now_unix = Utc::now().timestamp();
    let mut tokens = Vec::new();

    for coin in coins {
        tokens.push(Token {
            token_id: coin.id.clone(),
            symbol: coin.symbol.clone(),
            name: coin.name.clone(),
            contract_address: None,
            network_id: "mainnet".to_string(),
            decimals: None,
            current_price_usd: coin.current_price.and_then(|p| Decimal::from_f64(p)),
            market_cap_usd: coin.market_cap.and_then(|mc| Decimal::from_i64(mc)),
            total_volume_24h: coin.total_volume.and_then(|v| Decimal::from_f64(v)),
            price_change_24h: coin.price_change_percentage_24h.and_then(|p| Decimal::from_f64(p)),
            price_change_7d: coin.price_change_percentage_7d_in_currency.and_then(|p| Decimal::from_f64(p)),
            ath_usd: coin.ath.and_then(|a| Decimal::from_f64(a)),
            ath_date: coin.ath_date.as_ref().and_then(|d| iso8601_to_unix(d)),
            atl_usd: coin.atl.and_then(|a| Decimal::from_f64(a)),
            atl_date: coin.atl_date.as_ref().and_then(|d| iso8601_to_unix(d)),
            circulating_supply: coin.circulating_supply.and_then(|s| Decimal::from_f64(s)),
            total_supply: coin.total_supply.and_then(|s| Decimal::from_f64(s)),
            max_supply: coin.max_supply.and_then(|s| Decimal::from_f64(s)),
            fdv_usd: coin.fully_diluted_valuation.and_then(|f| Decimal::from_f64(f)),
            first_seen: now_unix,
            last_updated: now_unix,
        });
    }

    tokens
}
*/

fn transform_ohlc_to_data(coin_id: &str, ohlc_data: &[[f64; 5]], timeframe: &str) -> Vec<OhlcData> {
    let now_unix = Utc::now().timestamp();
    let mut ohlc_records = Vec::new();

    for data_point in ohlc_data {
        if data_point.len() >= 5 {
            let timestamp = (data_point[0] / 1000.0) as i64;
            if let (Some(open), Some(high), Some(low), Some(close)) = (
                Decimal::from_f64(data_point[1]),
                Decimal::from_f64(data_point[2]),
                Decimal::from_f64(data_point[3]),
                Decimal::from_f64(data_point[4]),
            ) {
                ohlc_records.push(OhlcData {
                    token_id: coin_id.to_string(),
                    timestamp,
                    timeframe: timeframe.to_string(),
                    open_price: open,
                    high_price: high,
                    low_price: low,
                    close_price: close,
                    volume: None,
                    market_cap: None,
                    first_seen: now_unix,
                });
            }
        }
    }

    ohlc_records
}

fn transform_market_chart_to_ohlc(coin_id: &str, chart_data: &MarketChartResponse, timeframe: &str) -> Vec<OhlcData> {
    let now_unix = Utc::now().timestamp();
    let mut ohlc_records = Vec::new();

    for (i, price_point) in chart_data.prices.iter().enumerate() {
        if price_point.len() >= 2 {
            let timestamp = (price_point[0] / 1000.0) as i64;
            if let Some(price) = Decimal::from_f64(price_point[1]) {
                let volume = chart_data.total_volumes.get(i)
                    .and_then(|v| if v.len() >= 2 { Decimal::from_f64(v[1]) } else { None });
                let market_cap = chart_data.market_caps.get(i)
                    .and_then(|mc| if mc.len() >= 2 { Decimal::from_f64(mc[1]) } else { None });

                ohlc_records.push(OhlcData {
                    token_id: coin_id.to_string(),
                    timestamp,
                    timeframe: timeframe.to_string(),
                    open_price: price,
                    high_price: price,
                    low_price: price,
                    close_price: price,
                    volume,
                    market_cap,
                    first_seen: now_unix,
                });
            }
        }
    }

    ohlc_records
}

fn transform_trending_to_market_trends(trending: &TrendingResponse) -> Vec<MarketTrend> {
    let now_unix = Utc::now().timestamp();
    let mut trends = Vec::new();

    for (rank, coin) in trending.coins.iter().enumerate() {
        let trend_id = format!("trending_{}_{}", coin.item.id, now_unix);
        trends.push(MarketTrend {
            trend_id,
            token_id: coin.item.id.clone(),
            trend_type: "trending".to_string(),
            rank: Some((rank + 1) as i32),
            score: Decimal::from_f64(coin.item.score as f64),
            price_btc: Some(Decimal::from_f64(coin.item.price_btc).unwrap_or_default()),
            volume_24h: coin.item
                .data
                .as_ref()
                .and_then(|d| d.total_volume.parse::<f64>().ok())
                .and_then(|v| Decimal::from_f64(v)),
            market_cap: coin
                .item
                .data
                .as_ref()
                .and_then(|d| d.market_cap.parse::<f64>().ok())
                .and_then(|v| Decimal::from_f64(v)),
            search_count: None,
            timestamp: now_unix,
        });
    }

    trends
}

fn transform_gainers_losers_to_trends(
    gainers_losers: &TopGainersLosersResponse,
) -> Vec<MarketTrend> {
    let now_unix = Utc::now().timestamp();
    let mut trends = Vec::new();

    for (rank, coin) in gainers_losers.top_gainers.iter().enumerate() {
        let trend_id = format!("gainer_{}_{}", coin.id, now_unix);
        trends.push(MarketTrend {
            trend_id,
            token_id: coin.id.clone(),
            trend_type: "gainer".to_string(),
            rank: Some((rank + 1) as i32),
            score: coin.price_change_percentage_24h
                .and_then(|v| Decimal::from_f64(v)),
            price_btc: None,
            volume_24h: coin
                .usd_24h_vol
                .and_then(|v| Decimal::from_f64(v)),
            market_cap: None,
            search_count: None,
            timestamp: now_unix,
        });
    }

    for (rank, coin) in gainers_losers.top_losers.iter().enumerate() {
        let trend_id = format!("loser_{}_{}", coin.id, now_unix);
        trends.push(MarketTrend {
            trend_id,
            token_id: coin.id.clone(),
            trend_type: "loser".to_string(),
            rank: Some((rank + 1) as i32),
            score: coin.price_change_percentage_24h
                .and_then(|v| Decimal::from_f64(v)),
            price_btc: None,
            volume_24h: coin
                .usd_24h_vol
                .and_then(|v| Decimal::from_f64(v)),
            market_cap: None,
            search_count: None,
            timestamp: now_unix,
        });
    }

    trends
}

async fn run_data_collection(db_client: Arc<Client>) {
    info!("[DataCollection] Starting comprehensive DeFi data collection");

    if let Err(e) = create_all_tables(&db_client).await {
        error!("[DataCollection] Failed to create tables: {}", e);
        return;
    }

    let limiter = Arc::new(Mutex::new(VecDeque::new()));

    info!("[DataCollection] Step 1: Fetching asset platforms");
    match fetch_asset_platforms(&limiter).await {
        Ok(platforms) => {
            let networks_data = transform_asset_platforms_to_networks(&platforms);
            if let Err(e) = insert_networks_batch(&db_client, &networks_data).await {
                error!("[DataCollection] Failed to insert networks: {}", e);
            }
        }
        Err(e) => error!("[DataCollection] Failed to fetch asset platforms: {}", e),
    }

    info!("[DataCollection] Step 2: Fetching coins markets data");
    let mut market_coin_ids: Vec<String> = Vec::new();
    for page in 1..=50 {
        match fetch_coins_markets(&limiter, page, 250).await {
            Ok(markets) => {
                if markets.is_empty() {
                    break;
                }
                for coin in &markets {
                    market_coin_ids.push(coin.id.clone());
                }
            }
            Err(e) => {
                error!("[DataCollection] Failed to fetch markets page {}: {}", page, e);
                break;
            }
        }
    }

    info!("[DataCollection] Step 3: Fetching trending data");
    match fetch_trending(&limiter).await {
        Ok(trending) => {
            let trend_data = transform_trending_to_market_trends(&trending);
            if let Err(e) = insert_trends_batch(&db_client, &trend_data).await {
                error!("[DataCollection] Failed to insert trending data: {}", e);
            }
        }
        Err(e) => error!("[DataCollection] Failed to fetch trending: {}", e),
    }

    info!("[DataCollection] Step 4: Fetching top gainers/losers");
    match fetch_top_gainers_losers(&limiter).await {
        Ok(gainers_losers) => {
            let trend_data = transform_gainers_losers_to_trends(&gainers_losers);
            if let Err(e) = insert_trends_batch(&db_client, &trend_data).await {
                error!("[DataCollection] Failed to insert gainers/losers data: {}", e);
            }
        }
        Err(e) => error!("[DataCollection] Failed to fetch gainers/losers: {}", e),
    }

    info!("[DataCollection] Step 5: Fetching OHLC data for market tokens ({}) tokens", market_coin_ids.len());

    for token_id in market_coin_ids {
        match fetch_coin_ohlc(&limiter, &token_id, 30).await {
            Ok(ohlc_data) => {
                let ohlc_records = transform_ohlc_to_data(&token_id, &ohlc_data, "daily");
                if let Err(e) = insert_ohlc_batch(&db_client, &ohlc_records).await {
                    error!("[DataCollection] Failed to insert OHLC for {}: {}", token_id, e);
                }
            }
            Err(e) => error!("[DataCollection] Failed to fetch OHLC for {}: {}", token_id, e),
        }

        match fetch_coin_market_chart(&limiter, &token_id, 90).await {
            Ok(chart_data) => {
                let chart_records = transform_market_chart_to_ohlc(&token_id, &chart_data, "hourly");
                if let Err(e) = insert_ohlc_batch(&db_client, &chart_records).await {
                    error!("[DataCollection] Failed to insert chart data for {}: {}", token_id, e);
                }
            }
            Err(e) => error!("[DataCollection] Failed to fetch chart for {}: {}", token_id, e),
        }
    }

    info!("[DataCollection] Data collection complete");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db_client = match connect_db().await {
        Ok(client) => client,
        Err(e) => {
            error!("[Main] Failed to connect to database: {}", e);
            return;
        }
    };

    let db_client_arc = Arc::new(db_client);

    info!("[Main] Starting DeFi data fetcher - single run");
    run_data_collection(db_client_arc.clone()).await;
    info!("[Main] Data collection complete, exiting");
}