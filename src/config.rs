// src/config.rs

//! # Modular Configuration System
//!
//! This module provides a robust configuration system that loads settings from a
//! directory of specialized JSON files. This modular approach makes configuration
//! easier to manage, version, and understand. The main `Config` struct acts as
//! the single source of truth for all system parameters.

use ethers::types::{Address, U256};
use eyre::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, path::Path};
use rust_decimal::Decimal;
use crate::types::ProtocolType;
use crate::secrets::{SecretData, DatabaseSecrets};

//================================================================================================//
//                                       Top-Level Config                                         //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub mode: ExecutionMode,
    pub log_level: String,
    pub chain_config: ChainConfig,
    pub module_config: Option<ModuleConfig>, // Encapsulate module settings
    pub backtest: Option<BacktestConfig>,
    pub gas_estimates: GasEstimates,
    /// When true, backtests fail closed: no data fallbacks, no nearby-window lookups,
    /// and strict block equality is enforced across providers.
    #[serde(default)]
    pub strict_backtest: bool,
    /// Centralized secrets loaded from encrypted vault
    #[serde(skip)]
    pub secrets: Option<Secrets>,
}

#[derive(Debug, Clone)]
pub struct Secrets {
    pub private_keys: HashMap<String, String>, // chain_name -> private_key
    pub api_keys: HashMap<String, String>, // service_name -> api_key
    pub database: DatabaseSecrets,
}

impl From<SecretData> for Secrets {
    fn from(data: SecretData) -> Self {
        Self {
            private_keys: data.private_keys,
            api_keys: data.api_keys,
            database: data.database,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceOracleConfig {
    pub native_asset_address: Address,
    pub stablecoin_address: Address,
    pub native_stable_pool: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasEstimates {
    pub uniswap_v2: u64,
    pub uniswap_v3: u64,
    pub uniswap_v4: u64,
    pub sushiswap: u64,
    pub pancakeswap: u64,
    pub curve: u64,
    pub balancer: u64,
    pub other: u64,
}

impl Default for GasEstimates {
    fn default() -> Self {
        Self {
            uniswap_v2: 120_000,
            uniswap_v3: 150_000,
            uniswap_v4: 180_000,
            sushiswap: 120_000,
            pancakeswap: 120_000,
            curve: 200_000,
            balancer: 250_000,
            other: 150_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleConfig {
    pub arbitrage_settings: ArbitrageSettings,
    pub path_finder_settings: PathFinderSettings,
    pub risk_settings: RiskSettings,
    pub transaction_optimizer_settings: TransactionOptimizerSettings,
    pub gas_oracle_config: GasOracleConfig,
    pub blockchain_settings: BlockchainSettings,
    pub data_science_settings: Option<DataScienceSettings>,
    pub alpha_engine_settings: Option<AlphaEngineSettings>,
    pub mev_engine_settings: Option<MEVEngineSettings>,
    pub chatbot_operator_settings: Option<ChatbotOperatorSettings>,
    pub mempool_config: Option<MempoolConfig>,
    pub cross_chain_settings: Option<CrossChainSettings>,
    pub liquidation_config: Option<LiquidationConfig>,
    pub jit_liquidity_config: Option<JITLiquidityConfig>,
}


impl Config {
    pub async fn load_from_directory<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let main_config: MainConfig = Self::load_file(dir.join("main.json")).await?;
        let chain_config: ChainConfig = Self::load_file(dir.join("chains.json")).await?;

        // Skip secret loading in test environments
        let is_test_env = chain_config.chains.values().any(|c| c.is_test_environment.unwrap_or(false));
        
        let secrets = if !is_test_env {
            Self::load_secrets_from_vault().await
        } else {
            None
        };

        Ok(Self {
            mode: main_config.mode,
            log_level: main_config.log_level.unwrap_or_else(|| "info".to_string()),
            chain_config,
            module_config: Self::load_optional_file(dir.join("modules.json")).await?,
            backtest: Self::load_optional_file(dir.join("backtest.json")).await?,
            gas_estimates: Self::load_optional_file(dir.join("gas_estimates.json")).await?.unwrap_or_default(),
            strict_backtest: std::env::var("STRICT_BACKTEST").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false),
            secrets,
        })
    }

    async fn load_file<T: for<'de> Deserialize<'de>>(path: impl AsRef<Path>) -> Result<T> {
        let content = tokio::fs::read_to_string(path.as_ref()).await.with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
        serde_json::from_str(&content).with_context(|| format!("Failed to parse config from JSON: {}", path.as_ref().display()))
    }

    async fn load_optional_file<T: for<'de> Deserialize<'de>>(path: impl AsRef<Path>) -> Result<Option<T>> {
        if !path.as_ref().exists() { return Ok(None); }
        let content = tokio::fs::read_to_string(path.as_ref()).await.with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
        match serde_json::from_str::<T>(&content) {
            Ok(config) => Ok(Some(config)),
            Err(e) => {
                eprintln!("Detailed JSON parse error for {}: {}", path.as_ref().display(), e);
                eprintln!("Error at line: {}, column: {}", e.line(), e.column());
                Err(eyre::eyre!("Failed to parse config from JSON: {} - Error: {}", path.as_ref().display(), e))
            }
        }
    }

    /// Load configuration from a single JSON file
    /// This is a simplified loader for unified config files
    pub async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = tokio::fs::read_to_string(path.as_ref()).await
            .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
        let mut cfg: Self = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config from JSON: {}", path.as_ref().display()))?;
        if let Ok(val) = std::env::var("STRICT_BACKTEST") {
            if val == "1" || val.eq_ignore_ascii_case("true") {
                cfg.strict_backtest = true;
            }
        }
        Ok(cfg)
    }

    pub fn get_chain_config(&self, name: &str) -> Result<&PerChainConfig> {
        self.chain_config.chains.get(name).ok_or_else(|| eyre::eyre!("Chain config not found: {}", name))
    }
    
    pub fn get_chain_name(&self, id: u64) -> Option<&str> {
        self.chain_config.chains.values().find(|c| c.chain_id == id).map(|c| c.chain_name.as_str())
    }
    
    pub fn get_factories_for_chain(&self, chain_name: &str) -> Vec<FactoryConfig> {
        if let Ok(chain_config) = self.get_chain_config(chain_name) {
            chain_config.factories.clone().unwrap_or_default()
        } else {
            Vec::new()
        }
    }



    /// Get pool state cache size from module config, with sensible default
    pub fn get_pool_state_cache_size(&self) -> u64 {
        self.module_config
            .as_ref()
            .and_then(|mc| mc.blockchain_settings.pool_state_cache_size)
            .unwrap_or(10000)
    }

    /// Get pool state cache TTL in seconds from module config, with sensible default
    pub fn get_pool_state_cache_ttl_seconds(&self) -> u64 {
        self.module_config
            .as_ref()
            .and_then(|mc| mc.blockchain_settings.pool_state_cache_ttl_seconds)
            .unwrap_or(300) // 5 minutes default
    }

    /// Load secrets from encrypted vault if MASTER_PASSWORD is provided
    async fn load_secrets_from_vault() -> Option<Secrets> {
        if let Ok(master_password) = std::env::var("MASTER_PASSWORD") {
            let environment = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "production".to_string());
            match crate::secrets::load_secrets(&environment, &master_password).await {
                Ok(secrets_mgr) => {
                    // Extract secrets we need for the application
                    if let Ok(api_keys) = secrets_mgr.get_api_key("1inch").await {
                        // Set environment variable for backward compatibility during transition
                        if let Some(key) = api_keys {
                            std::env::set_var("ONEINCH_API_KEY", key);
                        }
                    }

                    // Extract database credentials and set DATABASE_URL
                    if let Ok(db_secrets) = secrets_mgr.get_database_credentials().await {
                        if let Some(pg) = db_secrets.postgres.as_ref() {
                            let db_url = format!(
                                "postgresql://{}:{}@{}:{}/{}",
                                pg.username, pg.password, pg.host, pg.port, pg.database
                            );
                            std::env::set_var("DATABASE_URL", db_url);
                        }
                    }

                    // Return the secrets for direct access from Config
                    if let Ok(secrets_data) = secrets_mgr.get_all_secrets().await {
                        Some(Secrets::from(secrets_data))
                    } else {
                        None
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to load secrets vault: {}. Continuing without vault.", e);
                    None
                }
            }
        } else {
            // Only warn about missing MASTER_PASSWORD if not in paper trading mode
            // or if explicitly requested via environment variable
            let should_warn = std::env::var("WARN_MISSING_MASTER_PASSWORD")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

            if should_warn {
                tracing::warn!("MASTER_PASSWORD not set; skipping vault load. Using existing environment variables.");
            } else {
                tracing::debug!("MASTER_PASSWORD not set; skipping vault load. Using existing environment variables.");
            }
            None
        }
    }

    /// Get database URL from config secrets or environment variable
    pub fn database_url(&self) -> Option<String> {
        // First try to get from secrets
        if let Some(secrets) = &self.secrets {
            if let Some(pg) = secrets.database.postgres.as_ref() {
                return Some(format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    pg.username, pg.password, pg.host, pg.port, pg.database
                ));
            }
        }
        // Fallback to environment variable
        std::env::var("DATABASE_URL").ok()
    }

    /// Get API key from config secrets or environment variable
    pub fn get_api_key(&self, service_name: &str) -> Option<String> {
        // First try to get from secrets
        if let Some(secrets) = &self.secrets {
            if let Some(key) = secrets.api_keys.get(service_name) {
                return Some(key.clone());
            }
        }
        // Fallback to environment variable
        std::env::var(&format!("{}_API_KEY", service_name.to_uppercase())).ok()
    }

    /// Get private key for a specific chain from config secrets or environment variable
    pub fn get_private_key(&self, chain_name: &str) -> Option<String> {
        // First try to get from secrets
        if let Some(secrets) = &self.secrets {
            if let Some(key) = secrets.private_keys.get(chain_name) {
                return Some(key.clone());
            }
        }
        // Fallback to environment variable
        std::env::var(&format!("{}_PRIVATE_KEY", chain_name.to_uppercase())).ok()
    }
}

//================================================================================================//
//                                       Sub-Configurations                                       //
//================================================================================================//

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionMode { Live, Backtest, PaperTrading }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainConfig {
    pub mode: ExecutionMode,
    pub database_url: Option<String>,
    pub log_level: Option<String>,
    pub max_connections: Option<u32>,
    pub connection_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub chains: HashMap<String, PerChainConfig>,
    pub rate_limiter_settings: RateLimiterSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerChainConfig {
    pub chain_id: u64,
    pub chain_name: String,
    pub rpc_url: String,
    pub ws_url: String,
    #[serde(default)]
    pub private_key: String,
    pub weth_address: Address,
    pub live_enabled: bool,
    pub backtest_archival_rpc_url: Option<String>,
    pub max_gas_price: U256,
    pub gas_bump_percent: Option<u64>,
    pub max_pending_transactions: Option<usize>,
    pub rps_limit: Option<u32>,
    pub max_concurrent_requests: Option<u32>,
    pub endpoints: Vec<EndpointConfig>,
    pub log_filter: Option<LogFilterConfig>,
    pub reconnect_delay_ms: Option<u64>,
    pub chain_type: String,
    pub factories: Option<Vec<FactoryConfig>>,
    pub chainlink_native_usd_feed: Option<Address>, // Added for price oracle
    pub avg_block_time_seconds: Option<f64>,
    pub is_l2: Option<bool>,
    pub reference_stablecoins: Option<Vec<Address>>, // Added for price oracle
    pub router_tokens: Option<Vec<Address>>, // Added for router tokens
    pub bridges: Option<HashMap<String, Address>>,
    pub multicall_address: Option<Address>, // Added for batch token operations
    pub default_base_fee_gwei: Option<u64>,
    pub default_priority_fee_gwei: Option<u64>,
    pub price_timestamp_tolerance_seconds: Option<u64>,
    pub volatility_sample_interval_seconds: Option<u64>,
    pub volatility_max_samples: Option<u64>,
    pub chainlink_token_feeds: Option<HashMap<Address, Address>>, // Token -> Feed mapping
    pub stablecoin_symbols: Option<HashMap<String, Address>>, // Symbol -> Address mapping
    pub is_test_environment: Option<bool>, // Test environment flag
    // Price loader knobs (optional, with safe defaults)
    pub price_lookback_blocks: Option<u64>,
    pub fallback_window_blocks: Option<u64>,
    pub reciprocal_tolerance_pct: Option<f64>,
    pub same_token_io_min_ratio: Option<f64>,
    pub same_token_io_max_ratio: Option<f64>,
    pub strict_backtest_no_lookahead: Option<bool>,
    pub backtest_use_external_price_apis: Option<bool>, // Control external API calls during backtesting
    pub major_intermediaries: Option<Vec<Address>>, // Major liquid tokens for price routing
    pub use_token_prices_table: Option<bool>, // Whether token_prices table exists and should be used

    // Connection and backoff configuration for WebSocket listeners
    pub max_ws_reconnect_attempts: Option<u32>,
    pub ws_reconnect_base_delay_ms: Option<u64>,
    pub ws_max_backoff_delay_ms: Option<u64>,
    pub ws_backoff_jitter_factor: Option<f64>,
    pub subscribe_pending_txs: Option<bool>,


}

impl PerChainConfig {
    pub fn validate(&self) -> Result<(), eyre::Report> {
        if self.rpc_url.is_empty() {
            return Err(eyre::eyre!("RPC URL is missing for chain {}", self.chain_name));
        }
        Ok(())
    }

    pub fn validate_for_live(&self) -> Result<(), eyre::Report> {
        if self.private_key.is_empty() {
            return Err(eyre::eyre!("Private key is missing for chain {}", self.chain_name));
        }
        if self.rpc_url.is_empty() {
            return Err(eyre::eyre!("RPC URL is missing for chain {}", self.chain_name));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactoryConfig {
    pub address: Address,
    pub protocol_type: ProtocolType,
    pub creation_event_topic: String,
    pub pair_created_topic: Option<String>, // For older factories
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub url: String,
    pub ws: bool,
    pub priority: Option<u32>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFilterConfig {
    // This struct should match the filter config used in event_processor.rs/listen.rs
    // For example:
    pub address: Option<Vec<Address>>,
    pub topics: Option<Vec<Option<Vec<String>>>>,
}

impl From<LogFilterConfig> for ethers::types::Filter {
    fn from(config: LogFilterConfig) -> Self {
        let mut filter = ethers::types::Filter::new();
        
        if let Some(addresses) = config.address {
            filter = filter.address(addresses);
        }
        
        if let Some(topics) = config.topics {
            for (i, topic_set) in topics.into_iter().enumerate() {
                if let Some(topic_strings) = topic_set {
                    let topic_hashes: Vec<ethers::types::H256> = topic_strings
                        .iter()
                        .filter_map(|s| {
                            if s.starts_with("0x") && s.len() == 66 {
                                s.parse().ok()
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    if !topic_hashes.is_empty() {
                        match i {
                            0 => filter = filter.topic0(topic_hashes),
                            1 => filter = filter.topic1(topic_hashes),
                            2 => filter = filter.topic2(topic_hashes),
                            3 => filter = filter.topic3(topic_hashes),
                            _ => {} // Ignore topics beyond index 3
                        }
                    }
                }
            }
        }
        
        filter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterSettings {
    pub global_rps_limit: u32,
    pub default_chain_rps_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageSettings {
    pub min_profit_usd: f64,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_value_ratio: Decimal,
    pub trade_concurrency: usize,
    pub max_opportunities_per_event: usize,
    pub execution_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathFinderSettings {
    pub max_hops: usize,
    pub max_routes_to_consider: usize,
    pub min_liquidity_usd: f64,
    pub backtest_min_liquidity_multiplier: f64,
    pub parallel_worker_count: usize,
    pub max_slippage_bps: u32,
    pub max_processed_nodes: usize,
    pub min_profit_usd: f64,
    pub adaptive_beam_width: bool,
    pub high_connectivity_threshold: usize,
    pub beam_width: usize,
    pub beam_width_high_connectivity: usize,
    pub max_degree_per_token: usize,
    pub heartbeat_interval_ms: u64,
    pub max_search_time_seconds: u64,
    pub max_search_time_seconds_backtest: u64,
    pub stagnation_threshold_live: usize,
    pub stagnation_threshold_backtest: usize,
    pub graph_rebuild_block_interval: u64,
    /// Maximum search nodes allowed in a single run (frontier + explored). Acts as a soft ceiling; use with safety upper bounds in code.
    pub max_search_nodes_ceiling: usize,
    /// Maximum frontier size before pruning
    pub max_frontier_size: usize,
    /// Patience window without improvement before early termination in live mode (seconds)
    pub patience_seconds_live: u64,
    /// Patience window without improvement before early termination in backtest mode (seconds)
    pub patience_seconds_backtest: u64,
    /// Maximum token graph nodes allowed
    pub max_graph_nodes: usize,
    /// Maximum token graph edges allowed
    pub max_graph_edges: usize,
    /// Max attempts to wait for a concurrent graph build to finish
    pub graph_build_wait_max_attempts: u32,
    /// Interval between attempts while waiting for concurrent graph build (milliseconds)
    pub graph_build_wait_interval_ms: u64,
    /// Heuristic penalty to apply when price ratio is unavailable
    pub unknown_price_ratio_penalty: f64,
    /// Allow an on-chain decimals() fallback in strict backtest if pool-based inference fails
    pub decimals_strict_backtest_allow_onchain_fallback: bool,
}

impl Default for PathFinderSettings {
    fn default() -> Self {
        Self {
            max_hops: 5,
            max_routes_to_consider: 100,
            min_liquidity_usd: 1000.0,
            backtest_min_liquidity_multiplier: 0.5,
            parallel_worker_count: 8,
            max_slippage_bps: 500, // 5%
            max_processed_nodes: 10000,
            min_profit_usd: 1.0,
            adaptive_beam_width: true,
            high_connectivity_threshold: 50,
            beam_width: 10,
            beam_width_high_connectivity: 20,
            max_degree_per_token: 100,
            heartbeat_interval_ms: 1000,
            max_search_time_seconds: 5,
            max_search_time_seconds_backtest: 30,
            stagnation_threshold_live: 1000,
            stagnation_threshold_backtest: 5000,
            graph_rebuild_block_interval: 100,
            max_search_nodes_ceiling: 100_000,
            max_frontier_size: 20_000,
            patience_seconds_live: 10,
            patience_seconds_backtest: 30,
            max_graph_nodes: 100_000,
            max_graph_edges: 500_000,
            graph_build_wait_max_attempts: 100,
            graph_build_wait_interval_ms: 100,
            unknown_price_ratio_penalty: 10.0,
            decimals_strict_backtest_allow_onchain_fallback: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskSettings {
    /// Maximum allowed total risk score (0.0 to 1.0)
    pub max_risk_score: f64, // 0.0 to 1.0
    /// Maximum allowed exposure per trade in USD
    pub max_exposure_usd: f64,
    /// Maximum allowed slippage in basis points (bps)
    pub max_slippage_bps: u32,
    /// Maximum allowed annualized volatility
    pub max_volatility: f64, // Annualized volatility
    /// Minimum required liquidity in USD
    pub min_liquidity_usd: f64, // Minimum liquidity required
    /// Weights for each risk component
    pub risk_score_weights: RiskScoreWeights,
    /// Minimum gas price (in gwei) for risk normalization
    pub min_gas_gwei: f64,
    /// Maximum gas price (in gwei) for risk normalization
    pub max_gas_gwei: f64,
    /// Whether to disable sandwich attack opportunities
    pub disable_sandwich: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskScoreWeights {
    pub volatility: f64,
    pub liquidity: f64,
    pub slippage: f64,
    pub mev: f64,
    pub gas: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOptimizerSettings {
    pub use_flashbots: bool,
    pub flashbots_endpoint: String,
    pub flashbots_signing_key: String,
    pub max_retry_attempts: u32,
    pub confirmation_timeout_secs: u64,
    pub mev_gas_multiplier_bps: u128,
    pub flashbots_block_offset: u64,
    pub private_relays: Vec<PrivateRelayConfig>,
    pub max_concurrent_submissions: usize,
}

impl Default for TransactionOptimizerSettings {
    fn default() -> Self {
        Self {
            use_flashbots: true,
            flashbots_endpoint: "https://relay.flashbots.net".to_string(),
            flashbots_signing_key: "".to_string(),
            max_retry_attempts: 3,
            confirmation_timeout_secs: 300,
            mev_gas_multiplier_bps: 120,
            flashbots_block_offset: 1,
            private_relays: Vec::new(),
            max_concurrent_submissions: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateRelayConfig {
    pub name: String,
    pub endpoint: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasOracleConfig {
    pub cache_ttl_secs: u64,
    pub backtest_default_priority_fee_gwei: u64,
    pub protocol_gas_estimates: HashMap<String, u64>,
    pub default_gas_estimate: u64,
    pub nearby_window_blocks: Option<i64>,
    pub block_distance_tolerance: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainSettings {
    pub nonce_grace_blocks: u64,
    pub tx_receipt_cache_ttl_secs: u64,
    pub tx_receipt_cache_max_entries: usize,
    pub ws_reconnect_interval_secs: u64,
    pub max_ws_reconnect_attempts: u32,
    pub pending_tx_poll_interval_secs: u64,
    pub block_poll_interval_secs: u64,
    pub max_block_reorg_depth: u64,
    pub block_timestamp_tolerance_secs: u64,
    pub pool_state_cache_size: Option<u64>,
    pub pool_state_cache_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl StateDbConfig {
    pub fn connection_string(&self) -> String {
        format!("postgresql://{}:{}@{}:{}/{}", self.user, self.password, self.host, self.port, self.dbname)
    }
}

/// Custom deserializer for Vec<U256> from string array
fn deserialize_scan_amounts<'de, D>(deserializer: D) -> Result<Option<Vec<U256>>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    
    let opt_vec = Option::<Vec<String>>::deserialize(deserializer)?;
    match opt_vec {
        Some(strings) => {
            let mut amounts = Vec::new();
            for s in strings {
                let amount = U256::from_dec_str(&s)
                    .map_err(|e| Error::custom(format!("Failed to parse scan amount '{}': {}", s, e)))?;
                amounts.push(amount);
            }
            Ok(Some(amounts))
        }
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub enabled: bool,
    pub chains: HashMap<String, BacktestChainConfig>,
    pub max_parallel_blocks: usize,
    pub database: Option<StateDbConfig>,
    pub random_seed: Option<u64>,
    pub rps_limit: f64,
    pub concurrent_limit: usize,
    pub burst_allowance: usize,
    pub rate_limit_initial_backoff_ms: u64,
    pub rate_limit_max_backoff_ms: u64,
    pub rate_limit_backoff_multiplier: f64,
    pub rate_limit_jitter_factor: f64,
    pub rate_limit_max_retries: usize,
    pub rate_limit_timeout_secs: u64,
    pub batch_size: usize,
    pub min_batch_delay_ms: u64,
    pub rate_limiter_settings: RateLimiterSettings,
    
    // Competition simulation settings
    pub competition_bot_count: Option<usize>,
    pub competition_skill_range: Option<(f64, f64)>,
    pub competition_latency_range_ms: Option<(u64, u64)>,
    
    // MEV attack simulation settings
    pub mev_attack_probability: Option<f64>,
    pub mev_sandwich_success_rate: Option<f64>,
    pub mev_frontrun_success_rate: Option<f64>,
    pub mev_avg_profit_loss_usd: Option<f64>,
    
    // Hot token opportunities settings
    pub hot_tokens_search_enabled: Option<bool>,
    pub hot_tokens_search_interval_blocks: Option<u64>,
    pub hot_tokens: Option<Vec<Address>>,
    
    // Proactive scanning settings
    pub proactive_scanning_enabled: Option<bool>,
    pub max_tokens_per_block_scan: Option<usize>,
    pub max_opportunities_per_block: Option<usize>,
    #[serde(deserialize_with = "deserialize_scan_amounts")]
    pub scan_amounts: Option<Vec<U256>>,
    /// Optional USD notionals to use when sizing scans. If set, these take precedence over `scan_amounts`.
    /// Each value is an absolute USD notional (e.g. 100.0 for one hundred USD) that will be converted
    /// to token units at the block's historical price and token decimals.
    pub scan_usd_amounts: Option<Vec<f64>>,
    
    // Threshold settings for opportunities  
    pub min_profit_usd: Option<f64>,
    pub confidence_threshold: Option<f64>,
    /// Minimum MEV resistance score required for an opportunity to be considered
    pub mev_resistance_threshold: Option<f64>,
    pub hot_token_search_enabled: Option<bool>,

    // Competition tuning
    /// Gas bidding trigger threshold in Gwei; when the estimated effective gas price exceeds this,
    /// competitors are modeled as more aggressive. Defaults to 50.0 if not set.
    pub gas_bidding_trigger_gwei: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestChainConfig {
    pub enabled: bool,
    pub backtest_start_block: u64,
    pub backtest_end_block: u64,
    pub backtest_archival_rpc_url: String,
    pub backtest_result_output_file: String,
    /// Configuration for historical USD pricing via reference pools
    #[serde(default)]
    pub price_oracle: Option<PriceOracleConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaEngineSettings {
    pub enabled: bool,
    pub run_interval_secs: u64,
    pub opportunity_channel_size: usize,
    pub circuit_breaker_threshold: u64,
    pub circuit_breaker_timeout_seconds: u64,
    pub max_concurrent_ops: usize,
    pub min_profit_threshold_usd: f64,
    pub max_risk_score: f64,
    pub min_pool_liquidity_usd: f64,
    pub market_regime_interval_seconds: u64,
    pub high_volatility_threshold: f64,
    pub trending_threshold: f64,
    // Additional settings moved from hardcoded values
    pub min_scan_amount_wei: U256,
    pub statistical_arb_z_score_threshold: f64,
    pub mev_detection_window_blocks: u64,
    pub priority_score_decay_rate: f64,
    pub gas_spike_threshold_multiplier: f64,
    pub execution_latency_target_ms: u64,
    pub market_impact_warning_bps: u32,
    pub parallel_validation_batch_size: usize,
    pub performance_sample_size: usize,
    pub hot_path_cache_ttl_secs: u64,
    pub token_metadata_cache_ttl_secs: u64,
    pub volatility_cache_ttl_secs: u64,
    pub volatility_cache_capacity: usize,
    pub bridge_route_cache_ttl_secs: u64,
    pub bridge_route_cache_capacity: usize,
    pub cross_chain_price_divergence_threshold: f64,
    pub tick_data_capacity: usize,
    pub regime_history_capacity: usize,
    pub opportunity_expiration_secs: u64,
}

impl Default for AlphaEngineSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            run_interval_secs: 30,
            opportunity_channel_size: 100,
            circuit_breaker_threshold: 1000,
            circuit_breaker_timeout_seconds: 300,
            max_concurrent_ops: 10,
            min_profit_threshold_usd: 1.0,
            max_risk_score: 0.8,
            min_pool_liquidity_usd: 0.01,
            market_regime_interval_seconds: 3600,
            high_volatility_threshold: 0.5,
            trending_threshold: 0.3,
            min_scan_amount_wei: U256::from(10).pow(U256::from(18)), // 1 ETH
            statistical_arb_z_score_threshold: 2.5,
            mev_detection_window_blocks: 20,
            priority_score_decay_rate: 0.95,
            gas_spike_threshold_multiplier: 2.0,
            execution_latency_target_ms: 100,
            market_impact_warning_bps: 100,
            parallel_validation_batch_size: 100,
            performance_sample_size: 1000,
            hot_path_cache_ttl_secs: 300,
            token_metadata_cache_ttl_secs: 3600,
            volatility_cache_ttl_secs: 60,
            volatility_cache_capacity: 10000,
            bridge_route_cache_ttl_secs: 600,
            bridge_route_cache_capacity: 5000,
            cross_chain_price_divergence_threshold: 0.005,
            tick_data_capacity: 1000,
            regime_history_capacity: 100,
            opportunity_expiration_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEVEngineSettings {
    pub enabled: bool,
    pub max_active_mev_plays: usize,
    pub max_tx_gas_limit_mev: u64,
    pub min_sandwich_profit_usd: f64,
    pub min_liquidation_profit_usd: f64,
    pub min_jit_profit_usd: f64,
    pub min_flashloan_profit_usd: f64,
    pub min_arbitrage_profit_usd: Option<f64>,
    pub sandwich_frontrun_gas_price_gwei_offset: u64,
    pub sandwich_backrun_gas_price_gwei_offset: u64,
    pub sandwich_fixed_frontrun_native_amount_wei: U256,
    pub use_flashbots_for_mev: bool,
    pub ignore_risk_mev: bool,
    pub gas_escalation_factor: Option<u32>,
    pub jit_volatility_discount: Option<f64>,
    pub max_retries_mev_submission: usize,
    pub mev_play_expiry_secs: u64,
    pub bundle_monitor_expiry_secs: u64,
    pub mev_simulation_timeout_secs: u64,
    pub use_flashbots: bool,
    pub jit_helper_contract_address: Option<Address>,
    pub jit_gas_price_gwei_offset: Option<u64>,
    pub status_check_interval_secs: u64,
}

impl Default for MEVEngineSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_active_mev_plays: 10,
            max_tx_gas_limit_mev: 500_000,
            min_sandwich_profit_usd: 50.0,
            min_liquidation_profit_usd: 100.0,
            min_jit_profit_usd: 25.0,
            min_flashloan_profit_usd: 10.0,
            min_arbitrage_profit_usd: Some(10.0),
            sandwich_frontrun_gas_price_gwei_offset: 5,
            sandwich_backrun_gas_price_gwei_offset: 3,
            sandwich_fixed_frontrun_native_amount_wei: U256::from(10).pow(U256::from(18)), // 1 ETH
            use_flashbots_for_mev: true,
            ignore_risk_mev: false,
            gas_escalation_factor: Some(3),
            jit_volatility_discount: Some(0.8),
            max_retries_mev_submission: 2,
            mev_play_expiry_secs: 120,
            bundle_monitor_expiry_secs: 60,
            mev_simulation_timeout_secs: 15,
            use_flashbots: true,
            jit_helper_contract_address: None,
            jit_gas_price_gwei_offset: None,
            status_check_interval_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataScienceSettings {
    pub enabled: bool,
    pub analysis_interval_secs: u64,
    pub regime_lookback_secs: u64,
    pub pool_state_cache_ttl_seconds: Option<u64>,
    pub pool_state_cache_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolConfig {
    /// Maximum number of pending transactions to hold in the processing channel.
    pub max_pending_txs: usize,
    /// Number of parallel workers to spawn for processing transactions.
    pub parallel_workers: usize,
    /// Whether to subscribe to pending transactions on this chain.
    #[serde(default)]
    pub subscribe_pending_txs: bool,
    /// Optional: The number of transaction hashes to batch together for fetching.
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Optional: The timeout in milliseconds to wait before processing a batch, even if it's not full.
    #[serde(default)]
    pub batch_timeout_ms: Option<u64>,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            max_pending_txs: 10_000,
            parallel_workers: num_cpus::get().saturating_mul(2),
            subscribe_pending_txs: true,
            batch_size: Some(50),
            batch_timeout_ms: Some(100),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatbotOperatorSettings {
    pub enabled: bool,
    pub llm_api_key: String,
    pub llm_api_endpoint: String,
    pub llm_api_timeout_secs: u64,
    pub llm_model: String,
    pub run_interval_secs: u64,
    pub human_approval_mode: bool,
    pub enable_auto_rollback: bool,
    pub tuning_objective: String,
    pub tuning_constraints: Vec<String>,
    pub tunable_parameters: Vec<String>,
    pub min_profit_usd: f64,
    pub max_recent_trades: usize,
}

//================================================================================================//
//                                CROSS-CHAIN SETTINGS                                           //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossChainSettings {
    pub bridge_transaction_timeout_secs: u64,
    pub cross_chain_enabled: bool,
    pub min_profit_usd: f64,
    pub min_synergy_score: f64,
    pub max_bridge_fee_usd: f64,
    pub bridge_slippage_bps: u32,
    pub max_bridging_time_secs: u64,
    pub bridge_health_threshold: f64,
    pub min_confirmations: u64,
    pub max_size_per_bridge_usd: f64,
    pub timeout_penalty_usd: f64,
    pub per_bridge_fee_bps: Option<std::collections::HashMap<String, u32>>,
}

// Configuration for the liquidation simulation strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationConfig {
    pub liquidation_bonus_bps: Option<u32>,
    pub flash_loan_fee_bps: Option<u32>,
}

// Configuration for the JIT liquidity simulation strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JITLiquidityConfig {
    pub expected_interaction_fraction: Option<f64>,
    pub fee_multiplier: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainSettings {
    pub mode: ExecutionMode,
    pub global_rps_limit: u32,
    pub default_chain_rps_limit: u32,
    pub default_max_concurrent_requests: u32,
    pub rate_limit_burst_size: u32,
    pub rate_limit_timeout_secs: u64,
    pub rate_limit_max_retries: u32,
    pub rate_limit_initial_backoff_ms: u64,
    pub rate_limit_backoff_multiplier: f64,
    pub rate_limit_max_backoff_ms: u64,
    pub rate_limit_jitter_factor: f64,
    pub batch_size: usize,
    pub min_batch_delay_ms: u64,
    // RPC fetching specific settings
    pub max_blocks_per_query: Option<u64>,
    pub log_fetch_concurrency: Option<usize>,
    // Add more fields as needed for per-chain rate limiting and batching
}

impl Default for ChainSettings {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Live,
            global_rps_limit: 1000,
            default_chain_rps_limit: 100,
            default_max_concurrent_requests: 16,
            rate_limit_burst_size: 10,
            rate_limit_timeout_secs: 30,
            rate_limit_max_retries: 3,
            rate_limit_initial_backoff_ms: 100,
            rate_limit_backoff_multiplier: 2.0,
            rate_limit_max_backoff_ms: 5000,
            rate_limit_jitter_factor: 0.1,
            batch_size: 50,
            min_batch_delay_ms: 10,
            max_blocks_per_query: None, // Will use DEFAULT_MAX_BLOCKS_PER_QUERY
            log_fetch_concurrency: None, // Will use DEFAULT_LOG_FETCH_CONCURRENCY
        }
    }
}

impl Default for RateLimiterSettings {
    fn default() -> Self {
        Self {
            global_rps_limit: 1000,
            default_chain_rps_limit: 1000,
        }
    }
}

impl From<&PerChainConfig> for ChainSettings {
    fn from(config: &PerChainConfig) -> Self {
        Self {
            mode: ExecutionMode::Live, // Default to live mode
            global_rps_limit: 1000, // Default global limit
            default_chain_rps_limit: config.rps_limit.unwrap_or(100),
            default_max_concurrent_requests: config.max_concurrent_requests.unwrap_or(16),
            rate_limit_burst_size: 10,
            rate_limit_timeout_secs: 30,
            rate_limit_max_retries: 3,
            rate_limit_initial_backoff_ms: 100,
            rate_limit_backoff_multiplier: 2.0,
            rate_limit_max_backoff_ms: 5000,
            rate_limit_jitter_factor: 0.1,
            batch_size: 50,
            min_batch_delay_ms: 10,
            max_blocks_per_query: None, // Use default from RpcFetcher
            log_fetch_concurrency: None, // Use default from RpcFetcher
        }
    }
}
