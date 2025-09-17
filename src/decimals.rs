//! Centralized, multi-chain token decimals management.
//!

use crate::blockchain::BlockchainManager;
use ahash::AHashMap;
use ethers::types::Address;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, trace, warn};

// --- Configuration-driven Hardcoded Decimals ---

#[derive(Debug, Deserialize)]
struct FullConfig {
    chains: HashMap<String, ChainConfig>,
}

#[derive(Debug, Deserialize)]
struct ChainConfig {
    chain_id: u64,
    weth_address: Option<String>,
    reference_stablecoins: Option<Vec<String>>,
}

/// A global map of universally known token addresses to their decimal places.
/// This acts as a helper to build the chain-specific hardcoded maps from config.
static KNOWN_TOKEN_DECIMALS: Lazy<HashMap<Address, u8>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // WETH and variants are always 18
    m.insert(Address::from_str("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").unwrap(), 18); // WETH (Ethereum)
    m.insert(Address::from_str("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1").unwrap(), 18); // WETH (Arbitrum)
    m.insert(Address::from_str("0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7").unwrap(), 18); // WAVAX (Avalanche)
    m.insert(Address::from_str("0x4200000000000000000000000000000000000006").unwrap(), 18); // WETH (Base, Optimism)
    m.insert(Address::from_str("0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270").unwrap(), 18); // WMATIC (Polygon)
    m.insert(Address::from_str("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c").unwrap(), 18); // WBNB (BSC) - Note: Not WETH but serves same purpose

    // Common Stables
    m.insert(Address::from_str("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap(), 6);  // USDC (Ethereum)
    m.insert(Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831").unwrap(), 6);  // USDC (Arbitrum)
    m.insert(Address::from_str("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913").unwrap(), 6);  // USDC (Base)
    m.insert(Address::from_str("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E").unwrap(), 6);  // USDC (Avalanche)
    m.insert(Address::from_str("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174").unwrap(), 6);  // USDC (Polygon)
    m.insert(Address::from_str("0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d").unwrap(), 18); // USDC (BSC - Note: 18 decimals)

    m.insert(Address::from_str("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap(), 6);  // USDT (Ethereum)
    m.insert(Address::from_str("0xc2132D05D31c914a87C6611C10748AEb04B58e8F").unwrap(), 6);  // USDT (Polygon)

    m.insert(Address::from_str("0x6b175474e89094c44da98b954eedeac495271d0f").unwrap(), 18); // DAI (Ethereum)
    m.insert(Address::from_str("0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063").unwrap(), 18); // DAI (Polygon)

    // WBTC
    m.insert(Address::from_str("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599").unwrap(), 8); // WBTC (Ethereum)
    m
});

/// A multi-chain map of hardcoded decimals, built from configuration at startup.
/// The outer key is the `chain_id`, and the inner map is `token_address -> decimals`.
static MULTI_CHAIN_HARDCODED_DECIMALS: Lazy<HashMap<u64, HashMap<Address, u8>>> =
    Lazy::new(|| {
        // In a real application, this would be loaded from a file.
        // For this self-contained example, we embed the JSON.
        let config_json = include_str!("/Users/ichevako/Desktop/rust/config/chains.json");

        let config: FullConfig = serde_json::from_str(config_json)
            .expect("Failed to parse embedded chain configuration for decimals module. This is a critical startup failure.");

        let mut multi_chain_map = HashMap::new();

        for (_, chain_config) in config.chains {
            let mut chain_decimals = HashMap::new();

            // Add WETH/native wrapped token for the chain
            if let Some(weth_str) = chain_config.weth_address {
                if let Ok(weth_addr) = Address::from_str(&weth_str) {
                    if let Some(&decimals) = KNOWN_TOKEN_DECIMALS.get(&weth_addr) {
                        chain_decimals.insert(weth_addr, decimals);
                    } else {
                        // Default to 18 for unknown wrapped native tokens
                        chain_decimals.insert(weth_addr, 18);
                    }
                }
            }

            // Add reference stablecoins for the chain
            if let Some(stables) = chain_config.reference_stablecoins {
                for stable_str in stables {
                    if let Ok(stable_addr) = Address::from_str(&stable_str) {
                        if let Some(&decimals) = KNOWN_TOKEN_DECIMALS.get(&stable_addr) {
                            chain_decimals.insert(stable_addr, decimals);
                        }
                    }
                }
            }

            if !chain_decimals.is_empty() {
                multi_chain_map.insert(chain_config.chain_id, chain_decimals);
            }
        }
        multi_chain_map
    });

/// Retrieves hardcoded decimals for a token on a specific chain.
pub fn get_hardcoded_decimals(chain_id: u64, token: Address) -> Option<u8> {
    MULTI_CHAIN_HARDCODED_DECIMALS
        .get(&chain_id)
        .and_then(|chain_map| chain_map.get(&token).copied())
}

/// A thread-safe, multi-chain token decimals cache with a cascading fallback mechanism.
#[derive(Default)]
pub struct DecimalsCache {
    // (chain_id, token_address) -> decimals
    cache: Arc<RwLock<AHashMap<(u64, Address), u8>>>,
}

impl DecimalsCache {
    /// Creates a new, empty `DecimalsCache`.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(AHashMap::with_capacity(2048))),
        }
    }

    /// The core private helper for the decimals cascade logic.
    /// It is generic over blockchain and database providers to remain decoupled and testable.
    async fn decimals_cascade<B, D>(
        &self,
        chain_id: u64,
        token: Address,
        blockchain: Option<&B>,
        database: Option<&D>,
        is_backtest_mode: bool,
    ) -> Result<u8, String>
    where
        B: BlockchainManager + ?Sized,
        D: DatabaseProvider + ?Sized,
    {
        // 1. Check memory cache (fastest)
        if let Some(&decimals) = self.cache.read().await.get(&(chain_id, token)) {
            trace!(target: "decimals", %chain_id, ?token, %decimals, "Found in memory cache");
            return Ok(decimals);
        }

        // 2. Try database (persistent source)
        if let Some(db) = database {
            match db.get_token_decimals(chain_id, token).await {
                Ok(Some(decimals)) => {
                    debug!(target: "decimals", %chain_id, ?token, %decimals, "Found in database");
                    self.cache.write().await.insert((chain_id, token), decimals);
                    return Ok(decimals);
                }
                Ok(None) => trace!(target: "decimals", %chain_id, ?token, "Not found in database"),
                Err(e) => warn!(target: "decimals", %chain_id, ?token, error = %e, "Database query failed"),
            }
        }

        // 3. Check configuration-driven hardcoded values
        if let Some(decimals) = get_hardcoded_decimals(chain_id, token) {
            debug!(target: "decimals", %chain_id, ?token, %decimals, "Found in hardcoded config");
            self.cache.write().await.insert((chain_id, token), decimals);
            if let Some(db) = database {
                if let Err(e) = db.store_token_decimals(chain_id, token, decimals).await {
                    warn!(target: "decimals", %chain_id, ?token, error = %e, "Failed to persist hardcoded decimal to DB");
                }
            }
            return Ok(decimals);
        }

        // 4. Blockchain query (last resort, only in live mode)
        if is_backtest_mode {
            return Err(format!("Backtest mode: Decimals for token {} on chain {} are not in cache, DB, or config. Blockchain lookup is disabled.", token, chain_id));
        }

        let bc = blockchain.ok_or_else(|| "No blockchain provider available for RPC lookup".to_string())?;
        match bc.get_token_decimals(token, None).await {
            Ok(decimals) => {
                debug!(target: "decimals", %chain_id, ?token, %decimals, "Retrieved from blockchain RPC");
                self.cache.write().await.insert((chain_id, token), decimals);
                if let Some(db) = database {
                    if let Err(e) = db.store_token_decimals(chain_id, token, decimals).await {
                        warn!(target: "decimals", %chain_id, ?token, error = %e, "Failed to persist RPC-fetched decimal to DB");
                    }
                }
                Ok(decimals)
            }
            Err(e) => {
                error!(target: "decimals", %chain_id, ?token, error = %e, "Blockchain RPC query failed");
                Err(e.to_string())
            }
        }
    }

    /// Get token decimals with cascading fallback, providing all possible data sources.
    pub async fn get_with_providers<B, D>(
        &self,
        chain_id: u64,
        token: Address,
        blockchain: Option<&B>,
        database: Option<&D>,
        is_backtest_mode: bool,
    ) -> Result<u8, String>
    where
        B: BlockchainManager + ?Sized,
        D: DatabaseProvider + ?Sized,
    {
        self.decimals_cascade(chain_id, token, blockchain, database, is_backtest_mode).await
    }

    /// A convenience method for `get_with_providers` that accepts trait objects.
    pub async fn get_with_providers_dyn(
        &self,
        chain_id: u64,
        token: Address,
        blockchain: Option<&dyn BlockchainManager>,
        database: Option<&dyn DatabaseProvider>,
        is_backtest_mode: bool,
    ) -> Result<u8, String> {
        self.decimals_cascade(chain_id, token, blockchain, database, is_backtest_mode).await
    }

    /// A simple get method for live mode when only a blockchain provider is available.
    pub async fn get(&self, chain_id: u64, token: Address, blockchain: &dyn BlockchainManager) -> Result<u8, String> {
        self.get_with_providers_dyn(chain_id, token, Some(blockchain), None, false).await
    }

    /// Manually inserts a token decimal value into the cache. Useful for seeding the cache during initialization.
    pub async fn insert(&self, chain_id: u64, token: Address, decimals: u8) {
        self.cache.write().await.insert((chain_id, token), decimals);
        debug!(target: "decimals", %chain_id, ?token, %decimals, "Manually inserted into cache");
    }

    /// Clears the entire in-memory cache.
    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }

    /// Returns the number of entries currently in the cache.
    pub async fn size(&self) -> usize {
        self.cache.read().await.len()
    }
}

/// A trait defining the necessary database operations for storing and retrieving token decimals.
/// This allows the `DecimalsCache` to be independent of the specific database implementation.
#[async_trait::async_trait]
pub trait DatabaseProvider: Send + Sync {
    /// Retrieves the decimals for a given token on a specific chain from the database.
    async fn get_token_decimals(&self, chain_id: u64, token: Address) -> Result<Option<u8>, String>;

    /// Stores the decimals for a given token on a specific chain into the database.
    async fn store_token_decimals(&self, chain_id: u64, token: Address, decimals: u8) -> Result<(), String>;
}

/// A no-op database implementation for use when a persistent database is not available or needed (e.g., in tests).
pub struct EmptyDatabase;

#[async_trait::async_trait]
impl DatabaseProvider for EmptyDatabase {
    async fn get_token_decimals(&self, _chain_id: u64, _token: Address) -> Result<Option<u8>, String> {
        Ok(None)
    }

    async fn store_token_decimals(&self, _chain_id: u64, _token: Address, _decimals: u8) -> Result<(), String> {
        Ok(())
    }
}