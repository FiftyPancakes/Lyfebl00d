use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::str::FromStr;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize};
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn, debug};
use ethers::{
    abi::Abi,
    contract::Contract,
    providers::{Provider, Http},
    types::Address,
};

// Configuration constants
const API_ENDPOINT: &str = "https://pro-api.coingecko.com/api/v3/coins/list?include_platform=true&status=active";
const API_KEY: &str = "CG-3GBPr8Kx99Y5VfcC5DVTsxkG";

const PG_HOST: &str = "localhost";
const PG_PORT: u16 = 5432;
const PG_USER: &str = "ichevako";
const PG_DB: &str = "lyfeblood";
const PG_PASS: &str = "maddie";

const TABLE_NAME: &str = "coins";
const MAX_ADDRESSES_PER_API_CALL: usize = 50;
const MAX_CONCURRENT_REQUESTS: usize = 10;

// ERC20 ABI for decimals function
const ERC20_DECIMALS_ABI: &str = r#"[
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]"#;

// Network mappings
lazy_static::lazy_static! {
    static ref NETWORK_MAPPING: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        // Map internal names to CoinGecko platform names
        m.insert("eth", "ethereum");
        m.insert("arbitrum", "arbitrum-one");
        m.insert("avax", "avalanche");
        m.insert("base", "base");
        m.insert("bsc", "binance-smart-chain");
        m.insert("polygon", "polygon-pos");
        m.insert("solana", "solana");
        m
    };
    
    static ref REVERSE_NETWORK_MAPPING: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        // Map CoinGecko platform names to internal names
        m.insert("ethereum", "eth");
        m.insert("arbitrum-one", "arbitrum");
        m.insert("avalanche", "avax");
        m.insert("base", "base");
        m.insert("binance-smart-chain", "bsc");
        m.insert("polygon-pos", "polygon");
        m.insert("solana", "solana");
        m
    };
    
    static ref HARDCODED_NETWORKS: Vec<&'static str> = {
        vec!["eth", "arbitrum", "avax", "base", "bsc", "polygon", "solana"]
    };
}

#[derive(Debug, Clone)]
struct CoinRow {
    coin_id: String,
    symbol: String,
    name: String,
    chain_name: String,
    token_address: String,
    current_price: Decimal,
    decimals: Option<i32>,
}

#[derive(Debug, Clone)]
struct AddressMetadata {
    coin_id: String,
    symbol: String,
    name: String,
}

// Chain config structures (simplified)
#[derive(Debug, Clone, Deserialize)]
struct Config {
    chains: HashMap<String, PerChainConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct PerChainConfig {
    chain_name: String,
    chain_aliases: Vec<String>,
    rpc_url: String,
}

// Database manager
struct DataManager {
    client: Client,
}

impl DataManager {
    async fn new() -> Result<Self> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            PG_HOST, PG_PORT, PG_USER, PG_PASS, PG_DB
        );
        
        let retries = 5;
        let mut attempt = 0;
        
        let (client, connection) = loop {
            attempt += 1;
            match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok(result) => break result,
                Err(e) => {
                    error!("Connection attempt {} failed: {}", attempt, e);
                    if attempt >= retries {
                        return Err(anyhow::anyhow!("Failed to connect after {} attempts", retries));
                    }
                    sleep(Duration::from_secs(5)).await;
                }
            }
        };
        
        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });
        
        info!("PostgreSQL connection established");
        Ok(DataManager { client })
    }
    
    async fn ensure_table_exists(&self) -> Result<()> {
        info!("Checking if table {} exists", TABLE_NAME);
        
        let query = "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1)";
        let row = self.client.query_one(query, &[&TABLE_NAME]).await?;
        let exists: bool = row.get(0);
        
        if !exists {
            info!("Table {} does not exist, creating", TABLE_NAME);
            let create_query = format!(
                r#"
                CREATE TABLE {} (
                    coin_id VARCHAR(255) NOT NULL,
                    symbol VARCHAR(255),
                    name VARCHAR(255),
                    chain_name VARCHAR(255),
                    token_address VARCHAR(255),
                    current_price NUMERIC,
                    decimals INTEGER,
                    PRIMARY KEY (coin_id, chain_name, token_address)
                )
                "#,
                TABLE_NAME
            );
            self.client.execute(&create_query, &[]).await?;
            info!("Table {} created", TABLE_NAME);
        } else {
            info!("Table {} exists", TABLE_NAME);
            // Add decimals column if it doesn't exist
            let add_column_query = format!(
                r#"
                ALTER TABLE {} 
                ADD COLUMN IF NOT EXISTS decimals INTEGER
                "#,
                TABLE_NAME
            );
            self.client.execute(&add_column_query, &[]).await?;
            info!("Ensured decimals column exists in table {}", TABLE_NAME);
        }
        
        Ok(())
    }
    
    async fn store_data(&self, data: Vec<CoinRow>) -> Result<()> {
        if data.is_empty() {
            info!("No data to store");
            return Ok(());
        }
        
        let statement = format!(
            r#"
            INSERT INTO {} 
                (coin_id, symbol, name, chain_name, token_address, current_price, decimals)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (coin_id, chain_name, token_address)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                current_price = EXCLUDED.current_price,
                decimals = EXCLUDED.decimals
            "#,
            TABLE_NAME
        );
        
        let statement = self.client.prepare(&statement).await?;
        
        for coin_row in &data {
            self.client.execute(
                &statement,
                &[
                    &coin_row.coin_id,
                    &coin_row.symbol,
                    &coin_row.name,
                    &coin_row.chain_name,
                    &coin_row.token_address,
                    &coin_row.current_price,
                    &coin_row.decimals,
                ],
            ).await?;
        }
        
        info!("Successfully merged {} records into {}", data.len(), TABLE_NAME);
        Ok(())
    }
}

// JSON processing
fn process_json(value: &mut Value, parent_coin_id: Option<String>) -> () {
    match value {
        Value::Object(map) => {
            let mut current_parent_coin_id = parent_coin_id;
            
            // Rename 'id' to 'coin_id'
            if let Some(id_value) = map.remove("id") {
                if let Value::String(coin_id_str) = &id_value {
                    // Handle platforms renaming while we have the coin_id
                    if let Some(platforms_value) = map.remove("platforms") {
                        let new_key = format!("{}_network_address", coin_id_str);
                        map.insert(new_key, platforms_value);
                    }
                }
                
                map.insert("coin_id".to_string(), id_value);
            } else if let Some(coin_id) = current_parent_coin_id.as_ref() {
                // Handle platforms renaming when we didn't just process an id
                if let Some(platforms_value) = map.remove("platforms") {
                    let new_key = format!("{}_network_address", coin_id);
                    map.insert(new_key, platforms_value);
                }
            }
            
            // Update current_parent_coin_id if we found a coin_id
            if let Some(Value::String(coin_id_str)) = map.get("coin_id") {
                current_parent_coin_id = Some(coin_id_str.clone());
            }
            
            // Recursively process all values
            for (_, value) in map.iter_mut() {
                process_json(value, current_parent_coin_id.clone());
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                process_json(item, parent_coin_id.clone());
            }
        }
        _ => {}
    }
}

// HTTP client setup
fn create_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("Failed to create HTTP client")
}

// Load chain config from config/chains.json
async fn load_chain_config() -> Result<Config> {
    let config_path = "config/chains.json";
    let config_content = tokio::fs::read_to_string(config_path)
        .await
        .context("Failed to read config/chains.json")?;
    let config: Config = serde_json::from_str(&config_content)
        .context("Failed to parse config/chains.json")?;
    Ok(config)
}

// Normalize network ID to match our internal naming
fn normalize_network_id(network_id: &str) -> String {
    match network_id.to_lowercase().as_str() {
        "ethereum" | "eth" | "mainnet" => "eth".to_string(),
        "arbitrum" | "arbitrum-one" | "arb" => "arbitrum".to_string(),
        "avalanche" | "avax" | "avalanche-c-chain" => "avax".to_string(),
        "base" => "base".to_string(),
        "binance-smart-chain" | "bsc" | "bnb" => "bsc".to_string(),
        "polygon" | "polygon-pos" | "matic" => "polygon".to_string(),
        "solana" | "sol" => "solana".to_string(),
        _ => network_id.to_string(),
    }
}

// Find chain config by network_id (matches against chain_name and chain_aliases)
fn find_chain_config<'a>(config: &'a Config, network_id: &str) -> Option<&'a PerChainConfig> {
    let normalized_network = normalize_network_id(network_id);
    
    // First try exact match on chain name
    for (_, chain) in &config.chains {
        if normalize_network_id(&chain.chain_name) == normalized_network {
            return Some(chain);
        }
        // Then check aliases
        for alias in &chain.chain_aliases {
            if normalize_network_id(alias) == normalized_network {
                return Some(chain);
            }
        }
    }
    None
}

// Fetch decimals from blockchain
async fn fetch_decimals_onchain(
    rpc_url: &str,
    token_address: &str,
) -> Result<Option<i32>> {
    // Parse address
    let address = match Address::from_str(token_address) {
        Ok(addr) => addr,
        Err(e) => {
            debug!("Invalid address format {}: {}", token_address, e);
            return Ok(None);
        }
    };
    
    // Create provider
    let provider = match Provider::<Http>::try_from(rpc_url) {
        Ok(p) => Arc::new(p),
        Err(e) => {
            warn!("Failed to create provider for {}: {}", rpc_url, e);
            return Ok(None);
        }
    };
    
    // Parse ABI
    let abi: Abi = serde_json::from_str(ERC20_DECIMALS_ABI)?;
    
    // Create contract instance
    let contract = Contract::new(address, abi, provider);
    
    // Call decimals function
    match contract.method::<_, u8>("decimals", ())?.call().await {
        Ok(decimals) => {
            debug!("Token {} has {} decimals", token_address, decimals);
            Ok(Some(decimals as i32))
        }
        Err(e) => {
            debug!("Failed to fetch decimals for {}: {}", token_address, e);
            // Default to 18 decimals if the call fails
            Ok(Some(18))
        }
    }
}

// Fetch decimals for a batch of addresses
async fn fetch_decimals_batch(
    config: &Config,
    network_id: &str,
    addresses: &[String],
) -> Result<HashMap<String, Option<i32>>> {
    let mut decimals_map = HashMap::new();
    
    // Find the chain config for this network
    let chain_config = match find_chain_config(config, network_id) {
        Some(config) => config,
        None => {
            warn!("No chain config found for network: {}", network_id);
            // Return all addresses with None
            for address in addresses {
                decimals_map.insert(address.to_lowercase(), None);
            }
            return Ok(decimals_map);
        }
    };
    
    info!("Using RPC URL {} for network {}", chain_config.rpc_url, network_id);
    
    // Fetch decimals for each address
    for address in addresses {
        let decimals = fetch_decimals_onchain(&chain_config.rpc_url, address).await?;
        decimals_map.insert(address.to_lowercase(), decimals);
        
        // Small delay to avoid rate limiting
        sleep(Duration::from_millis(100)).await;
    }
    
    Ok(decimals_map)
}

// Fetch prices for a batch of addresses
async fn fetch_prices_batch(
    client: &reqwest::Client,
    network_id: &str,
    addresses: &[String],
) -> Result<HashMap<String, f64>> {
    let coingecko_network = match NETWORK_MAPPING.get(network_id) {
        Some(network) => network,
        None => {
            warn!("No mapping found for network: {}", network_id);
            return Ok(HashMap::new());
        }
    };
    
    let address_string = addresses
        .iter()
        .map(|addr| addr.trim().to_lowercase())
        .collect::<Vec<_>>()
        .join("%2C");
    
    let url = format!(
        "https://pro-api.coingecko.com/api/v3/simple/token_price/{}?contract_addresses={}&vs_currencies=usd",
        coingecko_network, address_string
    );
    
    let response = client
        .get(&url)
        .header("accept", "application/json")
        .header("x-cg-pro-api-key", API_KEY)
        .send()
        .await?;
    
    if response.status().is_success() {
        let price_data: HashMap<String, HashMap<String, f64>> = response.json().await?;
        let mut prices = HashMap::new();
        
        for address in addresses {
            let clean_address = address.trim().to_lowercase();
            for (key, value_map) in &price_data {
                if key.to_lowercase() == clean_address {
                    if let Some(usd_price) = value_map.get("usd") {
                        prices.insert(clean_address.clone(), *usd_price);
                        break;
                    }
                }
            }
            
            if !prices.contains_key(&clean_address) {
                prices.insert(clean_address, 0.0);
            }
        }
        
        Ok(prices)
    } else {
        error!("Error response for {}: {}", network_id, response.text().await?);
        Ok(addresses.iter().map(|addr| (addr.to_lowercase(), 0.0)).collect())
    }
}

// Fetch all prices and decimals with concurrency control
async fn fetch_all_data(
    config: Config,
    network_addresses: HashMap<String, Vec<String>>,
    address_metadata: HashMap<String, AddressMetadata>,
) -> Result<Vec<CoinRow>> {
    let client = Arc::new(create_http_client()?);
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut tasks = vec![];

    // We'll need to keep a mapping from network_id to chain_name for later renaming
    let mut network_id_to_chain_name: HashMap<String, String> = HashMap::new();
    for (_chain_name, chain) in &config.chains {
        let normalized = normalize_network_id(&chain.chain_name);
        network_id_to_chain_name.insert(normalized, chain.chain_name.clone());
        // Also insert aliases
        for alias in &chain.chain_aliases {
            let normalized_alias = normalize_network_id(alias);
            network_id_to_chain_name.insert(normalized_alias, chain.chain_name.clone());
        }
    }

    for (network_id, addresses) in network_addresses {
        for chunk in addresses.chunks(MAX_ADDRESSES_PER_API_CALL) {
            let client = Arc::clone(&client);
            let semaphore = Arc::clone(&semaphore);
            let network_id = network_id.clone();
            let batch = chunk.to_vec();
            let metadata = address_metadata.clone();
            let config_clone = config.clone();
            
            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                
                // Add delay between requests
                sleep(Duration::from_millis(600)).await;
                
                // Fetch prices from API and decimals from blockchain
                let (prices_result, decimals_result) = tokio::join!(
                    fetch_prices_batch(&client, &network_id, &batch),
                    fetch_decimals_batch(&config_clone, &network_id, &batch)
                );
                
                let prices = prices_result?;
                let decimals = decimals_result?;
                
                Ok::<_, anyhow::Error>((network_id, batch, prices, decimals, metadata))
            });
            
            tasks.push(task);
        }
    }
    
    info!("Created {} tasks for data fetching", tasks.len());
    
    let mut rows_to_insert = vec![];
    
    for task in tasks {
        match task.await? {
            Ok((network_id, batch, prices, decimals, metadata)) => {
                // Convert network_id to chain_name if possible
                let chain_name = {
                    // Use the mapping, fallback to network_id if not found
                    network_id_to_chain_name
                        .get(&normalize_network_id(&network_id))
                        .cloned()
                        .unwrap_or_else(|| network_id.clone())
                };
                for address in batch {
                    let metadata_key = format!("{}:{}", network_id, address.to_lowercase());
                    if let Some(coin_metadata) = metadata.get(&metadata_key) {
                        let raw_price = prices.get(&address.to_lowercase()).copied().unwrap_or(0.0);
                        let token_decimals = decimals.get(&address.to_lowercase()).copied().flatten();
                        
                        rows_to_insert.push(CoinRow {
                            coin_id: coin_metadata.coin_id.clone(),
                            symbol: coin_metadata.symbol.clone(),
                            name: coin_metadata.name.clone(),
                            chain_name: chain_name.clone(), // Use chain_name here
                            token_address: address.to_lowercase(),
                            current_price: Decimal::from_f64_retain(raw_price).unwrap_or(Decimal::ZERO),
                            decimals: token_decimals,
                        });
                    }
                }
            }
            Err(e) => error!("Task failed: {}", e),
        }
    }
    
    Ok(rows_to_insert)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    // Load chain config
    info!("Loading chain configuration...");
    let config = load_chain_config().await?;
    
    // Create database manager
    let data_manager = DataManager::new().await?;
    data_manager.ensure_table_exists().await?;
    
    // Fetch coin metadata from CoinGecko
    info!("Fetching data from CoinGecko...");
    let client = create_http_client()?;
    let response = client
        .get(API_ENDPOINT)
        .header("accept", "application/json")
        .header("x-cg-pro-api-key", API_KEY)
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "API error: {} - {}",
            response.status(),
            response.text().await?
        ));
    }
    
    let mut data: Value = response.json().await?;
    info!("Received data from CoinGecko");
    
    // Process JSON to rename keys
    process_json(&mut data, None);
    info!("Data processed");
    
    // Build network addresses for price and decimals lookup
    let mut network_addresses: HashMap<String, Vec<String>> = HashMap::new();
    let mut address_metadata: HashMap<String, AddressMetadata> = HashMap::new();
    
    if let Value::Array(coins) = data {
        for coin in coins {
            if let Value::Object(coin_obj) = coin {
                let coin_id = coin_obj.get("coin_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                
                let symbol = coin_obj.get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                
                let name = coin_obj.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                
                let platforms_key = format!("{}_network_address", coin_id);
                
                if let Some(Value::Object(platforms)) = coin_obj.get(&platforms_key) {
                    for (network_name, address_value) in platforms {
                        if let Some(address) = address_value.as_str() {
                            // Debug: Log the network name from CoinGecko
                            debug!("CoinGecko network name: {}", network_name);
                            
                            let internal_network = REVERSE_NETWORK_MAPPING
                                .get(network_name.as_str())
                                .copied()
                                .or_else(|| {
                                    NETWORK_MAPPING.iter()
                                        .find(|(_, v)| **v == network_name)
                                        .map(|(k, _)| *k)
                                });
                            
                            // Debug: Log the mapping result
                            debug!("Mapped {} to internal network: {:?}", network_name, internal_network);
                            
                            if let Some(internal_network) = internal_network {
                                if HARDCODED_NETWORKS.contains(&internal_network) && !address.is_empty() {
                                    let clean_address = address.trim().to_lowercase();
                                    
                                    network_addresses
                                        .entry(internal_network.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(clean_address.clone());
                                    
                                    let metadata_key = format!("{}:{}", internal_network, clean_address);
                                    address_metadata.insert(
                                        metadata_key,
                                        AddressMetadata {
                                            coin_id: coin_id.clone(),
                                            symbol: symbol.clone(),
                                            name: name.clone(),
                                        },
                                    );
                                }
                            } else {
                                debug!("Network {} not found in mapping, skipping", network_name);
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Debug: Log the network distribution
    info!("Network distribution:");
    for (network, addresses) in &network_addresses {
        info!("  {}: {} addresses", network, addresses.len());
    }
    
    // Fetch prices and decimals asynchronously
    info!("Starting async data fetching (prices from API, decimals from blockchain)...");
    let rows_to_insert = fetch_all_data(config, network_addresses, address_metadata).await?;
    info!("Fetched data for {} tokens", rows_to_insert.len());
    
    // Insert data into PostgreSQL
    info!("Inserting data into PostgreSQL...");
    data_manager.store_data(rows_to_insert).await?;
    
    info!("Process completed successfully");
    Ok(())
}