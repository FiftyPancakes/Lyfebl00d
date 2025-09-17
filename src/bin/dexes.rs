// main.rs

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{info, warn, error, debug};
use tracing_subscriber;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

// API constants
static BASE_URL: &str = "https://pro-api.coingecko.com/api/v3";
static API_KEY: &str = "CG-3GBPr8Kx99Y5VfcC5DVTsxkG";

// Database config
const PG_HOST: &str = "localhost";
const PG_PORT: u16 = 5432;
const PG_USER: &str = "ichevako";
const PG_DB: &str = "lyfeblood";
const PG_PASS: &str = "maddie";
const TABLE_NAME: &str = "dexes";

#[derive(Debug, Clone, Deserialize)]
struct RawDex {
    id: Option<String>,
    #[serde(rename = "type")]
    r#type: Option<String>,
    attributes: Option<DexAttributes>,
    #[serde(default)]
    network_id: Option<String>, // this will be injected
}

#[derive(Debug, Clone, Deserialize)]
struct DexAttributes {
    name: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
struct DexTransformed {
    dex_id: String,
    chain_name: String, // Renamed from network_id
    dex_name: String,
    dex_type: String,  // Renamed from 'type' for consistency
}

// ---- Database Functions ----

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
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    Err("[DB] Could not connect after multiple attempts".into())
}

async fn create_dexes_table(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", TABLE_NAME);
    let create_sql = format!(
        "CREATE TABLE {} (
            dex_id TEXT NOT NULL,
            chain_name TEXT NOT NULL,
            dex_name TEXT NOT NULL,
            dex_type TEXT NOT NULL,
            PRIMARY KEY (dex_id, chain_name)
        )",
        TABLE_NAME
    );

    client.execute(&drop_sql, &[]).await?;
    client.execute(&create_sql, &[]).await?;
    info!("[DB] Table {} created", TABLE_NAME);
    
    // Create indexes for common queries
    let indexes = vec![
        format!("CREATE INDEX idx_{}_chain ON {} (chain_name)", TABLE_NAME, TABLE_NAME),
        format!("CREATE INDEX idx_{}_name ON {} (dex_name)", TABLE_NAME, TABLE_NAME),
    ];
    
    for index_sql in indexes {
        client.execute(&index_sql, &[]).await?;
    }
    
    Ok(())
}

async fn insert_dexes_batch(
    client: &Client,
    dexes: &[DexTransformed],
) -> Result<(), Box<dyn std::error::Error>> {
    if dexes.is_empty() {
        return Ok(());
    }

    let statement = client.prepare(&format!(
        "INSERT INTO {} (dex_id, chain_name, dex_name, dex_type) 
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (dex_id, chain_name) DO UPDATE SET
            dex_name = EXCLUDED.dex_name,
            dex_type = EXCLUDED.dex_type",
        TABLE_NAME
    )).await?;

    for dex in dexes {
        client.execute(&statement, &[
            &dex.dex_id,
            &dex.chain_name,
            &dex.dex_name,
            &dex.dex_type,
        ]).await?;
    }
    
    info!("[DB] Inserted {} DEX records into {}", dexes.len(), TABLE_NAME);
    Ok(())
}

// ---- Fetchers ----

async fn fetch_network_dexes(session: &reqwest::Client, network_id: &str) -> Vec<RawDex> {
    let url = format!("{}/onchain/networks/{}/dexes", BASE_URL, network_id);
    debug!("[DEBUG API] Fetching URL: {}", &url);
    let retries = 3;
    for attempt in 0..retries {
        let resp_result = session
            .get(&url)
            .header("accept", "application/json")
            .header("x-cg-pro-api-key", API_KEY)
            .send()
            .await;
        match resp_result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                debug!("[DEBUG API] Response Status for {}: {}", network_id, status);
                debug!("[DEBUG API] Response Text for {}: {}", network_id, &body[..body.len().min(200)]);
                if status == 200 {
                    let value: Value = serde_json::from_str(&body).unwrap_or(json!({}));
                    if let Some(data) = value.get("data") {
                        match serde_json::from_value::<Vec<RawDex>>(data.clone()) {
                            Ok(mut dexes) => {
                                // inject network_id for later grouping/transform
                                for d in &mut dexes {
                                    d.network_id = Some(network_id.to_string());
                                }
                                info!("[OK] Network {}: {} DEXes", network_id, dexes.len());
                                return dexes;
                            }
                            Err(e) => {
                                warn!("Malformed JSON structure for {}: {}", network_id, e);
                                if attempt + 1 == retries {
                                    return vec![];
                                }
                                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                                continue;
                            }
                        }
                    } else {
                        warn!("[Data Structure] Unexpected format for {}. Attempt {}/{}", network_id, attempt+1, retries);
                        if attempt + 1 == retries {
                            return vec![];
                        }
                        tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                    }
                } else if status == 404 {
                    info!("[Skip] Network {}: No DEX data available", network_id);
                    return vec![];
                } else if status == 429 {
                    let delay = 60 * (attempt + 1);
                    warn!("[Rate Limit] Network {}: Waiting {}s... Attempt {}/{}", network_id, delay, attempt+1, retries);
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                } else {
                    warn!("[HTTP {}] Network {}: {}. Attempt {}/{}", status, network_id, &body[..body.len().min(200)], attempt+1, retries);
                    if status.is_server_error() && attempt + 1 < retries {
                        tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                    } else {
                        return vec![];
                    }
                }
            }
            Err(e) => {
                error!("[ClientError] Network {}: {}. Attempt {}/{}", network_id, e, attempt+1, retries);
                if attempt + 1 == retries {
                    return vec![];
                }
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
            }
        }
    }
    vec![]
}

async fn fetch_all_dexes(network_ids: &[&str]) -> Vec<RawDex> {
    debug!("[DEBUG] Starting fetch_all_dexes");
    let mut all_dexes = Vec::new();
    let client = reqwest::Client::new();
    let semaphore = Arc::new(Semaphore::new(10));

    let mut handles = Vec::new();

    for &network_id in network_ids {
        let session_clone = client.clone();
        let sem_clone = semaphore.clone();
        let permit = sem_clone.acquire_owned().await.unwrap();
        let network_id_string = network_id.to_string();
        let handle = tokio::spawn(async move {
            let _permit = permit;
            fetch_network_dexes(&session_clone, &network_id_string).await
        });
        handles.push(handle);
    }

    for h in handles {
        match h.await {
            Ok(res) => all_dexes.extend(res),
            Err(_join_err) => {} // error already logged
        }
    }

    debug!("[DEBUG] Finished fetch_all_dexes. Total DEXes: {}", all_dexes.len());
    all_dexes
}

// ---- Transform ----

fn transform_dex_data(raw_data: Vec<RawDex>) -> Vec<DexTransformed> {
    let mut transformed = Vec::new();

    for item in raw_data {
        let dex_id = match item.id { Some(x) => x, None => continue };
        let network_id = match item.network_id { Some(x) => x, None => continue };
        let dex_name = match &item.attributes { Some(a) => a.name.clone(), None => None };
        let dex_type = item.r#type.clone();

        if let (Some(dex_name), Some(dex_type)) = (dex_name, dex_type) {
            transformed.push(DexTransformed {
                dex_id,
                chain_name: network_id, // rename network_id to chain_name for DB
                dex_name,
                dex_type,
            });
        }
    }
    transformed
}

// ---- Main work ----

async fn monitor() {
    info!("Fetching DEXes from CoinGecko Pro...");

    let network_ids = [
        "eth", "arbitrum", "avax", "base", "optimism",
        "polygon_pos", "ronin", "bsc", "solana"
    ];

    let raw = fetch_all_dexes(&network_ids).await;
    if raw.is_empty() {
        warn!("No DEX data returned from API. Exiting.");
        return;
    }

    let transformed = transform_dex_data(raw);

    if transformed.is_empty() {
        warn!("No valid DEX data after transformation. Exiting.");
        return;
    }

    // Connect to database
    let db_client = match connect_db().await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to connect to database: {}", e);
            return;
        }
    };

    // Create table (drops if exists)
    if let Err(e) = create_dexes_table(&db_client).await {
        error!("Failed to create table: {}", e);
        return;
    }

    // Insert data
    if let Err(e) = insert_dexes_batch(&db_client, &transformed).await {
        error!("Failed to insert data: {}", e);
        return;
    }

    info!("Successfully wrote {} DEXes to database", transformed.len());
}

// ---- Entrypoint ----

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("Starting DEX fetch/transform script.");
    monitor().await;
}
