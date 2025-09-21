use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use std::str::FromStr;
use futures_util::future::join_all;
use tokio::sync::Semaphore;
use serde_json;
use rand;
use ethers::{
    abi::{Tokenize, Detokenize, Token, ParamType},
    types::{Address, U256, H256, Block, Eip1559TransactionRequest},
    contract::{Contract, ContractCall, ContractFactory},
    core::utils::Anvil,
    middleware::{SignerMiddleware, Middleware},
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
};
use tokio::time::timeout;
use tracing::{info, debug, warn, error};
use eyre::{Result, WrapErr};
use std::fs;
use rust::types::SwapEvent;
use hex;
use ethers::utils::AnvilInstance;
use tokio::sync::RwLock;

use rust::{
    config::{Config, ChainConfig, PerChainConfig, ModuleConfig, ArbitrageSettings, PathFinderSettings,
             TransactionOptimizerSettings, RiskSettings, GasOracleConfig, BlockchainSettings,
             CrossChainSettings, StateDbConfig, RateLimiterSettings, FactoryConfig, MempoolConfig,
             ExecutionMode, ChainSettings, RiskScoreWeights},
    pool_manager::{PoolManager, PoolManagerTrait},
    types::{ArbitrageEngine, ArbitrageOpportunity, TransactionContext, MEVPrediction, ExecutionResult, ModelPerformance, ArbitrageEngineTrait, ArbitrageMetrics, DexProtocol, ProtocolType, PoolStateFromSwaps},
    path::{AStarPathFinder, PathFinder},
    risk::{RiskManagerImpl},
    blockchain::{BlockchainManagerImpl, BlockchainManager},
    quote_source::{MultiAggregatorQuoteSource, AggregatorQuoteSource},
    transaction_optimizer::{TransactionOptimizer, TransactionOptimizerTrait},
    state::{StateManager, StateConfig},
    mempool::{DefaultMempoolMonitor},
    gas_oracle::{GasOracle},
    price_oracle::{PriceOracle},
    cross::{CrossChainPathfinder},
    predict::PredictiveEngine,
    errors::{PredictiveEngineError, CrossChainError},
    synergy_scorer::SynergyScorer,
    pool_states::PoolStateOracle,
};

pub mod mocks;
pub mod real_harness;

// Shared test harness for multiple tests
// NOTE: This static is currently unused and may cause resource leaks
// TODO: Remove if not needed, or implement proper cleanup
static SHARED_HARNESS: std::sync::OnceLock<Arc<TestHarness>> = std::sync::OnceLock::new();
const ANVIL_BLOCK_TIME_MS: u64 = 200;

#[derive(Debug, Clone)]
pub enum TokenType { 
    Standard, 
    FeeOnTransfer, 
    Reverting 
}

#[derive(Debug, Clone)]
pub enum UarActionType {
    Swap,
    WrapNative,
    UnwrapNative,
    TransferProfit,
}

#[derive(Debug, Clone)]
pub struct UarAction {
    pub action_type: UarActionType,
    pub target: Address,
    pub token_in: Address,
    pub amount_in: U256,
    pub call_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DeployedToken {
    pub contract: ContractWrapper,
    pub address: Address,
    pub token_type: TokenType,
    pub decimals: u8,
    pub symbol: String,
}

#[derive(Debug, Clone)]
pub struct DeployedPair {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
}

#[derive(Debug, Clone)]
pub struct MockTransaction {
    pub hash: H256,
    pub priority_fee: U256,
    pub max_fee: U256,
}

#[derive(Debug)]
pub struct BlockBuilder {
    pub pending_txs: Arc<RwLock<Vec<MockTransaction>>>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            pending_txs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_tx(&self, tx: MockTransaction) {
        let mut pending = self.pending_txs.write().await;
        pending.push(tx);
    }

    pub async fn build_block(&self, provider: &Provider<Http>) -> Result<Block<H256>> {
        let mut pending = self.pending_txs.write().await;
        if pending.is_empty() {
            // Mine an empty block if no transactions
            provider.request::<_, U256>("anvil_mine", [1, 1]).await?;
        } else {
            // Sort by priority fee
            pending.sort_by_key(|tx| std::cmp::Reverse(tx.priority_fee));

            // Create and send transactions
            let accounts: Vec<Address> = provider.get_accounts().await?;
            let mut futures = Vec::new();
            for (i, tx) in pending.iter().enumerate() {
                let from = accounts[i % accounts.len()];
                let to = accounts[(i + 1) % accounts.len()];
                let request = Eip1559TransactionRequest::new()
                    .from(from)
                    .to(to)
                    .value(U256::from(100))
                    .max_priority_fee_per_gas(tx.priority_fee)
                    .max_fee_per_gas(tx.max_fee);
                
                futures.push(provider.send_transaction(request, None));
            }
            join_all(futures).await;

            // Mine block with sorted transactions
            provider.request::<_, U256>("anvil_mine", [pending.len(), 1]).await?;
            pending.clear();
        }

        let block_number = provider.get_block_number().await?;
        Ok(provider.get_block(block_number).await?.unwrap())
    }
}

pub struct TestHarness {
    pub anvil: Option<AnvilInstance>,
    pub chain_id: u64,
    pub rpc_url: String,
    pub ws_url: String,
    pub chain_name: String,
    pub weth_address: Address,
    pub deployer_client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    pub deployer_address: Address,
    pub factory_contract: ContractWrapper,
    pub router_contract: ContractWrapper,
    pub uar_contract: ContractWrapper,
    pub mock_aave_pool: ContractWrapper,
    pub mock_balancer_vault: ContractWrapper,
    pub mock_weth: ContractWrapper,
    pub tokens: Vec<DeployedToken>,
    pub pairs: Vec<DeployedPair>,
    pub config: Arc<Config>,
    pub blockchain_manager: Arc<BlockchainManagerImpl>,
    pub pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    pub path_finder: Arc<AStarPathFinder>,
    pub risk_manager: Arc<RiskManagerImpl>,
    pub arbitrage_engine: Arc<ArbitrageEngine>,
    pub cross_chain_pathfinder: Arc<CrossChainPathfinder>,
    pub predictive_engine: Arc<dyn PredictiveEngine + Send + Sync>,
    pub metrics: Arc<ArbitrageMetrics>,
    pub quote_source: Arc<dyn AggregatorQuoteSource + Send + Sync>,
    pub transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync>,
    pub gas_oracle: Arc<GasOracle>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub state_manager: Arc<StateManager>,
    pub mempool_monitor: Arc<DefaultMempoolMonitor>,
    pub test_mode: TestMode,
    pub block_builder: Arc<BlockBuilder>,
}

impl std::fmt::Debug for TestHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestHarness")
            .field("chain_id", &self.chain_id)
            .field("rpc_url", &self.rpc_url)
            .field("ws_url", &self.ws_url)
            .field("chain_name", &self.chain_name)
            .field("weth_address", &self.weth_address)
            .field("deployer_address", &self.deployer_address)
            .field("factory_contract", &self.factory_contract.address())
            .field("router_contract", &self.router_contract.address())
            .field("uar_contract", &self.uar_contract.address())
            .field("mock_aave_pool", &self.mock_aave_pool.address())
            .field("mock_balancer_vault", &self.mock_balancer_vault.address())
            .field("mock_weth", &self.mock_weth.address())
            .field("tokens", &self.tokens.len())
            .field("pairs", &self.pairs.len())
            .field("test_mode", &self.test_mode)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct TestMode {
    pub enable_chaos_testing: bool,
    pub enable_mock_providers: bool,
    pub enable_database: bool,
    pub timeout_seconds: u64,
    pub max_concurrent_operations: usize,
}

impl Default for TestMode {
    fn default() -> Self {
        Self {
            enable_chaos_testing: false,
            enable_mock_providers: false, // Use mock providers for integration tests with Anvil
            enable_database: false, // Disable database by default to avoid connection issues
            timeout_seconds: 20, // Increased timeout for contract deployments
            max_concurrent_operations: 10,
        }
    }
}

impl TestMode {
    pub fn fast() -> Self {
        Self {
            enable_chaos_testing: false,
            enable_mock_providers: true,
            enable_database: false,
            timeout_seconds: 5, // Very fast timeout for quick tests
            max_concurrent_operations: 20, // Higher concurrency
        }
    }
}

impl TestHarness {
    pub async fn new() -> Result<Self> {
        Self::with_config(TestMode::default()).await
    }
    
    pub async fn shared() -> Result<Arc<TestHarness>> {
        if let Some(harness) = SHARED_HARNESS.get() {
            Ok(harness.clone())
        } else {
            let harness = Arc::new(Self::new().await?);
            SHARED_HARNESS.set(harness.clone()).unwrap();
            Ok(harness)
        }
    }
    
    pub async fn with_config(test_mode: TestMode) -> Result<Self> {
        let timeout_seconds = test_mode.timeout_seconds;
        let test_mode_clone = test_mode.clone();

        debug!("Starting test harness creation with timeout: {} seconds (effective: {} seconds)",
               timeout_seconds, timeout_seconds * 5);
        debug!("Environment check - PRIVATE_KEY set: {}", std::env::var("PRIVATE_KEY").is_ok());
        debug!("Environment check - DATABASE_URL set: {}", std::env::var("DATABASE_URL").is_ok());

        let harness_result = tokio::time::timeout(
            Duration::from_secs(timeout_seconds * 5), // Increase timeout further for slower systems
            Self::create_harness(test_mode_clone)
        ).await;

        match harness_result {
            Ok(Ok(harness)) => {
                debug!("Test harness created successfully");
                Ok(harness)
            },
            Ok(Err(e)) => {
                error!("Test harness creation failed with error: {:?}", e);
                Err(e)
            },
            Err(_) => {
                error!("Test harness creation timed out after {} seconds", timeout_seconds * 5);
                Err(eyre::eyre!("Test harness creation timed out after {} seconds", timeout_seconds * 5))
            },
        }
    }

    async fn create_harness(test_mode: TestMode) -> Result<TestHarness> {
        debug!("Initializing test harness creation");
        Self::initialize_rate_limiter();
        debug!("Rate limiter initialized");

        let (anvil_instance, rpc_url, ws_url, chain_id, private_key_str) = if test_mode.enable_mock_providers {
            debug!("Using existing Anvil instance for mock providers");
            let rpc_url = "http://127.0.0.1:8545".to_string();
            let ws_url = "ws://127.0.0.1:8545".to_string();
            let chain_id = 31337u64;
            let private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
            (None, rpc_url, ws_url, chain_id, private_key.to_string())
        } else {
            debug!("Starting new Anvil instance...");
            let anvil_process = Anvil::new()
                .arg("--base-fee")
                .arg("10000000000") // 10 gwei
                .arg("--block-time")
                .arg(ANVIL_BLOCK_TIME_MS.to_string())
                .spawn();
            debug!("Anvil spawned successfully");
            tokio::time::sleep(Duration::from_millis(1000)).await; // Shorter wait
            let rpc_url = anvil_process.endpoint();
            let ws_url = anvil_process.ws_endpoint();
            let chain_id = anvil_process.chain_id();
            let private_key = format!("0x{}", hex::encode(anvil_process.keys()[0].to_bytes()));

            (Some(anvil_process), rpc_url, ws_url, chain_id, private_key)
        };
        let chain_name = "anvil".to_string();

        let deployer_wallet = private_key_str
            .parse::<LocalWallet>()?
            .with_chain_id(chain_id);
        
        let deployer_address = deployer_wallet.address();
        let provider = Provider::<Http>::try_from(rpc_url.clone())?
            .interval(Duration::from_millis(100));
        let deployer_client = Arc::new(SignerMiddleware::new(provider.clone(), deployer_wallet));
        debug!("Deployer client created");

        let block_builder = Arc::new(BlockBuilder::new());
        debug!("Block builder created");

        // Setup configuration for test environment
        debug!("Loading configuration for test environment...");
        let mut config = Config::load_from_directory("config").await?;
        debug!("Configuration loaded successfully");
        config.chain_config.chains.insert(
            chain_name.clone(),
            PerChainConfig {
                chain_id,
                chain_name: chain_name.clone(),
                rpc_url: rpc_url.clone(),
                ws_url: ws_url.clone(),
                private_key: private_key_str.to_string(),
                weth_address: Address::zero(),
                live_enabled: true,
                backtest_archival_rpc_url: None,
                max_gas_price: U256::from(100_000_000_000u64),
                gas_bump_percent: Some(10),
                max_pending_transactions: Some(100),
                rps_limit: Some(100),
                max_concurrent_requests: Some(50),
                endpoints: Vec::new(),
                log_filter: None,
                reconnect_delay_ms: Some(1000),
                chain_type: "Evm".to_string(),
                factories: Some(vec![FactoryConfig {
                    address: Address::zero(),
                    protocol_type: ProtocolType::UniswapV2,
                    // PairCreated(address,address,address,uint256)
                    creation_event_topic: "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".to_string(),
                    pair_created_topic: Some("0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".to_string()),
                }]),
                chainlink_native_usd_feed: None,
                reference_stablecoins: Some({
                    let mut stablecoins: Vec<Address> = Vec::new();
                    stablecoins.push(Address::zero()); // Add mock WETH as reference
                    stablecoins
                }),
                router_tokens: Some({
                    let mut router_tokens: Vec<Address> = Vec::new();
                    // Add UAR contract addresses for testing
                    router_tokens.push(Address::zero());
                    router_tokens.push(Address::zero()); // Add mock AAVE pool
                    router_tokens.push(Address::zero()); // Add mock Balancer vault
                    router_tokens.push(Address::zero()); // Add mock WETH
                    router_tokens
                }),
                bridges: None,
                multicall_address: None,
                default_base_fee_gwei: Some(20),
                default_priority_fee_gwei: Some(1),
                price_timestamp_tolerance_seconds: Some(300),
                avg_block_time_seconds: Some(12.0),
                volatility_sample_interval_seconds: Some(3600),
                volatility_max_samples: Some(1000),
                chainlink_token_feeds: None,
                stablecoin_symbols: None,
                is_test_environment: Some(true),
                price_lookback_blocks: None,
                fallback_window_blocks: None,
                reciprocal_tolerance_pct: None,
                same_token_io_min_ratio: Some(0.95),
                same_token_io_max_ratio: Some(1.05),
                            strict_backtest_no_lookahead: Some(true),
                backtest_use_external_price_apis: Some(false),
                is_l2: Some(false),
                max_ws_reconnect_attempts: Some(5),
                ws_reconnect_base_delay_ms: Some(1000),
                ws_max_backoff_delay_ms: Some(30000),
                ws_backoff_jitter_factor: Some(0.1),
                subscribe_pending_txs: Some(false),
                major_intermediaries: None,
                use_token_prices_table: Some(false),
            },
        );
        config.backtest = None;
        config.gas_estimates = rust::config::GasEstimates {
            uniswap_v2: 90_000,
            uniswap_v3: 150_000,
            uniswap_v4: 180_000,
            sushiswap: 95_000,
            pancakeswap: 95_000,
            curve: 200_000,
            balancer: 250_000,
            other: 150_000,
        };
        config.module_config = Some(ModuleConfig {
            arbitrage_settings: ArbitrageSettings { 
                min_profit_usd: -1000.0, // More lenient for testing
                min_value_ratio: rust_decimal::Decimal::new(1, 3), // More lenient ratio
                trade_concurrency: 1,
                max_opportunities_per_event: 10,
                execution_timeout_secs: 60,
            },
            path_finder_settings: PathFinderSettings { 
                max_hops: 3, // Reduced from 4 to limit search space
                max_routes_to_consider: 5, // Reduced from 10
                min_liquidity_usd: 0.001, // More lenient for testing
                backtest_min_liquidity_multiplier: 1.0,
                parallel_worker_count: 4,
                max_slippage_bps: 500, // More lenient slippage
                max_processed_nodes: 500, // Reduced from 1000
                min_profit_usd: -1000.0, // More lenient for testing
                adaptive_beam_width: true,
                high_connectivity_threshold: 50,
                beam_width: 5, // Reduced from 10
                beam_width_high_connectivity: 10, // Reduced from 20
                max_degree_per_token: 10, // Reduced from 100
                heartbeat_interval_ms: 1000,
                max_search_time_seconds: 10, // Increased for test stability
                stagnation_threshold_live: 1,
                stagnation_threshold_backtest: 1,
                graph_rebuild_block_interval: 100,
                max_search_time_seconds_backtest: 20,
                // Newly added fields with safe testing defaults
                max_search_nodes_ceiling: 10_000,
                max_frontier_size: 2_000,
                patience_seconds_live: 10,
                patience_seconds_backtest: 15,
                max_graph_nodes: 50_000,
                max_graph_edges: 200_000,
                graph_build_wait_max_attempts: 50,
                graph_build_wait_interval_ms: 200,
                unknown_price_ratio_penalty: 10.0,
                decimals_strict_backtest_allow_onchain_fallback: true,
            },
            transaction_optimizer_settings: TransactionOptimizerSettings { 
                use_flashbots: false,
                flashbots_endpoint: "".to_string(),
                flashbots_signing_key: "".to_string(),
                max_retry_attempts: 3,
                confirmation_timeout_secs: 60,
                mev_gas_multiplier_bps: 1100,
                flashbots_block_offset: 1,
                private_relays: Vec::new(),
                max_concurrent_submissions: 5,
            },
            risk_settings: RiskSettings { 
                max_risk_score: 0.95, // More lenient for testing
                max_exposure_usd: 100000.0, // Higher exposure limit
                max_slippage_bps: 500, // More lenient slippage
                max_volatility: 0.5, // More lenient volatility
                min_liquidity_usd: 0.001, // More lenient liquidity
                risk_score_weights: RiskScoreWeights {
                    volatility: 0.3,
                    liquidity: 0.2,
                    slippage: 0.2,
                    mev: 0.2,
                    gas: 0.1,
                },
                min_gas_gwei: 1.0,
                max_gas_gwei: 1000.0,
                disable_sandwich: false,
            },
            gas_oracle_config: GasOracleConfig {
                cache_ttl_secs: 60,
                backtest_default_priority_fee_gwei: 1,
                protocol_gas_estimates: HashMap::new(),
                default_gas_estimate: 15000,
                nearby_window_blocks: Some(10),
                block_distance_tolerance: Some(5),
            },
            blockchain_settings: BlockchainSettings {
                nonce_grace_blocks: 5,
                tx_receipt_cache_ttl_secs: 300,
                tx_receipt_cache_max_entries: 1000,
                ws_reconnect_interval_secs: 5,
                max_ws_reconnect_attempts: 10,
                pending_tx_poll_interval_secs: 2,
                block_poll_interval_secs: 1,
                max_block_reorg_depth: 10,
                block_timestamp_tolerance_secs: 30,
                pool_state_cache_size: Some(1000),
                pool_state_cache_ttl_seconds: Some(60),
            },
            data_science_settings: None,
            alpha_engine_settings: None,
            mev_engine_settings: None,
            chatbot_operator_settings: None,
            mempool_config: Some(MempoolConfig {
                max_pending_txs: 1000,
                parallel_workers: 4,
                subscribe_pending_txs: true,
                batch_size: Some(50),
                batch_timeout_ms: Some(1000),
            }),
            cross_chain_settings: Some(CrossChainSettings {
                bridge_transaction_timeout_secs: 180,
                cross_chain_enabled: false,
                min_profit_usd: 50.0,
                min_synergy_score: 0.7,
                max_bridge_fee_usd: 100.0,
                bridge_slippage_bps: 100,
                max_bridging_time_secs: 300,
                bridge_health_threshold: 0.8,
                // Newly required fields
                min_confirmations: 1,
                max_size_per_bridge_usd: 10_000.0,
                timeout_penalty_usd: 0.0,
                per_bridge_fee_bps: None,
            }),
            liquidation_config: None,
            jit_liquidity_config: None,
        });

        // Test connection with retry
        debug!("Testing Anvil connection with up to 10 retries...");
        let mut retries = 0;
        while retries < 10 {
            debug!("Attempting to connect to Anvil (attempt {})", retries + 1);
            match deployer_client.get_block_number().await {
                Ok(block_num) => {
                    debug!("Successfully connected to Anvil on attempt {} (block: {})", retries + 1, block_num);
                    break;
                }
                Err(e) => {
                    if retries < 9 {
                        warn!("Failed to connect to Anvil (attempt {}): {}. Retrying...", retries + 1, e);
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        retries += 1;
                    } else {
                        error!("Failed to connect to Anvil after {} attempts: {}", retries + 1, e);
                        return Err(eyre::eyre!("Failed to connect to Anvil after {} attempts: {}", retries + 1, e));
                    }
                }
            }
        }
        info!("Using deployer account: {} (first Anvil account)", deployer_address);
        let balance = deployer_client.get_balance(deployer_address, None).await?;
        info!("Deployer balance: {} ETH", ethers::utils::format_ether(balance));

        // Deploy contracts without internal timeout
        debug!("Starting contract deployment...");
        let (factory_contract, router_contract, uar_contract, mock_aave_pool, mock_balancer_vault, mock_weth, tokens, pairs) =
            Self::deploy_contracts(&deployer_client, &test_mode).await?;
        
        let weth_address = tokens[0].address;
        let config = Self::create_config(
            &chain_name,
            chain_id,
            rpc_url.clone(),
            ws_url.clone(),
            &private_key_str,
            weth_address,
            factory_contract.address(),
            &tokens,
            uar_contract.address(),
            mock_aave_pool.address(),
            mock_balancer_vault.address(),
            mock_weth.address(),
            &test_mode,
        )?;
        
        // Initialize components without internal timeout
        let (blockchain_manager, pool_manager, path_finder, risk_manager, arbitrage_engine, 
            cross_chain_pathfinder, predictive_engine, metrics, quote_source, 
            transaction_optimizer, gas_oracle, price_oracle, state_manager, mempool_monitor) = 
            Self::initialize_components(
                &config,
                &deployer_client,
                &test_mode,
                &tokens,
                &pairs,
                &chain_name,
            ).await?;
        
        // Use MockPoolManager for all pool info queries in mock mode
        let pool_manager: Arc<dyn PoolManagerTrait + Send + Sync> = if test_mode.enable_mock_providers {
            let mut mock_pm = mocks::MockPoolManager::new();
            mock_pm.add_test_pools(pairs.as_slice(), tokens.as_slice());
            Arc::new(mock_pm)
        } else {
            pool_manager
        };
        
        Ok(TestHarness {
            anvil: anvil_instance,
            chain_id,
            rpc_url,
            ws_url,
            chain_name,
            weth_address,
            deployer_client,
            deployer_address,
            factory_contract,
            router_contract,
            uar_contract,
            mock_aave_pool,
            mock_balancer_vault,
            mock_weth,
            tokens,
            pairs,
            config,
            blockchain_manager,
            pool_manager,
            path_finder,
            risk_manager,
            arbitrage_engine,
            cross_chain_pathfinder,
            predictive_engine,
            metrics,
            quote_source,
            transaction_optimizer,
            gas_oracle,
            price_oracle,
            state_manager,
            mempool_monitor,
            test_mode,
            block_builder,
        })
    }

    fn initialize_rate_limiter() {
        use rust::rate_limiter::initialize_global_rate_limiter_manager;
        let rate_limiter_settings = Arc::new(ChainSettings {
            mode: ExecutionMode::Live,
            // Much more aggressive limits for local test environment
            global_rps_limit: 5000,
            default_chain_rps_limit: 1000,
            default_max_concurrent_requests: 200,
            rate_limit_burst_size: 20,
            rate_limit_timeout_secs: 30,
            rate_limit_max_retries: 3,
            // Much shorter backoff times for tests
            rate_limit_initial_backoff_ms: 50,
            rate_limit_backoff_multiplier: 1.5,
            rate_limit_max_backoff_ms: 500,
            rate_limit_jitter_factor: 0.1,
            batch_size: 50,
            min_batch_delay_ms: 10,
            max_blocks_per_query: Some(1000),
            log_fetch_concurrency: Some(20),
        });
        initialize_global_rate_limiter_manager(rate_limiter_settings);
        info!("Rate limiter initialized for test environment");
    }

    async fn deploy_contracts(
        deployer_client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        test_mode: &TestMode,
    ) -> Result<(
        ContractWrapper,
        ContractWrapper,
        ContractWrapper,
        ContractWrapper,
        ContractWrapper,
        ContractWrapper,
        Vec<DeployedToken>,
        Vec<DeployedPair>
    )> {
        if test_mode.enable_mock_providers {
            debug!("Using mock contracts for testing");

            // Create UniswapV2 ABI for mock contracts
            let uniswap_v2_abi: ethers::abi::Abi = serde_json::from_str(r#"[
                {
                    "inputs": [],
                    "name": "getReserves",
                    "outputs": [
                        {"name": "reserve0", "type": "uint112"},
                        {"name": "reserve1", "type": "uint112"},
                        {"name": "blockTimestampLast", "type": "uint32"}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "token0",
                    "outputs": [{"name": "", "type": "address"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "token1",
                    "outputs": [{"name": "", "type": "address"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "factory",
                    "outputs": [{"name": "", "type": "address"}],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]"#).expect("Failed to parse UniswapV2 ABI");

            let mock_factory = ContractWrapper::Manual(Contract::new(
                Address::from_str("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f")?, // UniswapV2Factory mainnet
                uniswap_v2_abi.clone(),
                deployer_client.clone()
            ));
            let mock_router = ContractWrapper::Manual(Contract::new(
                Address::from_str("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")?, // UniswapV2Router02 mainnet
                uniswap_v2_abi.clone(),
                deployer_client.clone()
            ));
            let mock_uar = ContractWrapper::Manual(Contract::new(
                Address::from_str("0x1234567890123456789012345678901234567890")?,
                ethers::abi::Abi::default(),
                deployer_client.clone()
            ));
            let mock_aave = ContractWrapper::Manual(Contract::new(
                Address::from_str("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9")?, // Aave Pool mainnet
                ethers::abi::Abi::default(),
                deployer_client.clone()
            ));
            let mock_balancer = ContractWrapper::Manual(Contract::new(
                Address::from_str("0xBA12222222228d8Ba445958a75a0704d566BF2C8")?, // Balancer Vault mainnet
                ethers::abi::Abi::default(),
                deployer_client.clone()
            ));
            let mock_weth = ContractWrapper::Manual(Contract::new(
                Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")?, // WETH mainnet
                ethers::abi::Abi::default(),
                deployer_client.clone()
            ));

            let tokens = Self::deploy_test_tokens(deployer_client, &test_mode).await?;

            // Create mock pairs for the mock tokens
            let mut pairs = Vec::new();
            for i in 0..tokens.len() {
                for j in (i + 1)..tokens.len() {
                    let pair_address = Address::from_str(&format!("0x{:040x}", 0x1000 + i * 10 + j))?;
                    pairs.push(DeployedPair {
                        address: pair_address,
                        token0: tokens[i].address,
                        token1: tokens[j].address,
                    });
                }
            }

            return Ok((
                mock_factory,
                mock_router,
                mock_uar,
                mock_aave,
                mock_balancer,
                mock_weth,
                tokens,
                pairs
            ));
        }

        // Deploy WETH first (needed for router)
        let mock_weth_wrapper = Self::deploy_contract(
            "MockERC20",
            ("Wrapped Ether".to_string(), "WETH".to_string(), 18u8),
            deployer_client
        ).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Deploy factory with deployer as fee setter
        let factory_contract_wrapper = Self::deploy_contract(
            "MockUniswapV2Factory",
            (deployer_client.address(),),
            deployer_client
        ).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Deploy router with factory and WETH addresses
        let router_contract_wrapper = Self::deploy_contract(
            "MockUniswapV2Router02",
            (factory_contract_wrapper.address(), mock_weth_wrapper.address()),
            deployer_client
        ).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Deploy mock AAVE pool
        let mock_aave_pool_wrapper = Self::deploy_contract(
            "MockERC20",
            ("Mock AAVE Pool".to_string(), "MAVE".to_string(), 18u8),
            deployer_client
        ).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Deploy mock Balancer vault
        let mock_balancer_vault_wrapper = Self::deploy_contract(
            "MockERC20",
            ("Mock Balancer Vault".to_string(), "MBAL".to_string(), 18u8),
            deployer_client
        ).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Deploy UAR contract with correct constructor arguments
        let uar_contract_wrapper = Self::deploy_contract(
            "UniversalArbitrageExecutor", 
            (factory_contract_wrapper.address(), router_contract_wrapper.address(), mock_weth_wrapper.address()), 
            deployer_client
        ).await?;
        
        // Deploy test tokens
        let tokens = Self::deploy_test_tokens(deployer_client, &test_mode).await?;
        
        // Create pairs using the deployed tokens
        let pairs = create_pairs(
            &tokens,
            &factory_contract_wrapper,
            &router_contract_wrapper,
            deployer_client
        ).await?;
        
        Ok((
            factory_contract_wrapper,
            router_contract_wrapper,
            uar_contract_wrapper,
            mock_aave_pool_wrapper,
            mock_balancer_vault_wrapper,
            mock_weth_wrapper,
            tokens,
            pairs
        ))
    }

    async fn deploy_test_tokens(
        deployer_client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        test_mode: &TestMode,
    ) -> Result<Vec<DeployedToken>> {
        if test_mode.enable_mock_providers {
            debug!("Using mock tokens for testing");
            let mock_tokens = vec![
                DeployedToken {
                    symbol: "WETH".to_string(),
                    address: Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")?, // WETH mainnet address
                    token_type: TokenType::Standard,
                    decimals: 18,
                    contract: ContractWrapper::Manual(Contract::new(
                        Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")?,
                        ethers::abi::Abi::default(),
                        deployer_client.clone()
                    )),
                },
                DeployedToken {
                    symbol: "USDC".to_string(),
                    address: Address::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")?, // USDC mainnet address
                    token_type: TokenType::Standard,
                    decimals: 6,
                    contract: ContractWrapper::Manual(Contract::new(
                        Address::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")?,
                        ethers::abi::Abi::default(),
                        deployer_client.clone()
                    )),
                },
                DeployedToken {
                    symbol: "TOKA".to_string(),
                    address: Address::from_str("0x1234567890123456789012345678901234567890")?,
                    token_type: TokenType::Standard,
                    decimals: 18,
                    contract: ContractWrapper::Manual(Contract::new(
                        Address::from_str("0x1234567890123456789012345678901234567890")?,
                        ethers::abi::Abi::default(),
                        deployer_client.clone()
                    )),
                },
            ];
            return Ok(mock_tokens);
        }

        let mut tokens = Vec::new();
        let deployer_address = deployer_client.address();
        let token_configs = vec![
            ("Standard Token A", "TOKA", TokenType::Standard),
            ("Standard Token B", "TOKB", TokenType::Standard),
            ("Standard Token C", "TOKC", TokenType::Standard),
            ("Fee Token", "FEE", TokenType::FeeOnTransfer),
        ];
        
        info!("Deploying {} test tokens", token_configs.len());
        
        // Phase 1: Deploy all tokens in parallel
        let deployment_futures = token_configs.iter().map(|(name, symbol, token_type)| {
            let deployer_client_clone = deployer_client.clone();
            let name = name.to_string();
            let symbol = symbol.to_string();
            let token_type = token_type.clone();
            async move {
                debug!("Deploying token: {}", symbol);
                let token_wrapper = Self::deploy_contract(
                    "MockERC20",
                    (name.clone(), symbol.clone(), 18u8),
                    &deployer_client_clone
                ).await?;
                debug!("Deployed token: name={}, symbol={}, address={:?}", name, symbol, token_wrapper.address());
                Ok::<(String, String, TokenType, ContractWrapper), eyre::Report>((name, symbol, token_type, token_wrapper))
            }
        });

        // Deploy tokens sequentially to avoid overloading Anvil
        let mut deployment_results = Vec::new();
        for future in deployment_futures {
            deployment_results.push(future.await);
            // Add a small delay between deployments to avoid overloading Anvil
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let mut deployed_tokens = Vec::new();

        for result in deployment_results {
            let (name, symbol, token_type, token_wrapper) = result?;
            deployed_tokens.push((name, symbol, token_type, token_wrapper));
        }

        // Phase 2: Mint tokens to deployer (sequential for simplicity)
        let mint_amount = U256::from(100_000u64) * U256::exp10(18); // 100k tokens
        info!("Minting tokens to deployer");

        for (_name, symbol, token_type, token_wrapper) in deployed_tokens {
            debug!("Minting {} {} tokens to deployer", mint_amount / U256::exp10(18), symbol);

            let mint_call = token_wrapper.method::<_, bool>("mint", (deployer_address, mint_amount))?
                .gas(300_000u64)  // Higher gas limit for minting
                .gas_price(DEFAULT_GAS_PRICE);
            let tx = mint_call.send().await?;
            let receipt = tx.await?.ok_or_else(|| eyre::eyre!("Mint transaction failed for {}", symbol))?;
            if receipt.status != Some(1.into()) {
                return Err(eyre::eyre!("Mint transaction reverted for token {}", symbol));
            }

            debug!("Minted {} {} tokens to deployer", mint_amount / U256::exp10(18), symbol);

            tokens.push(DeployedToken {
                contract: token_wrapper.clone(),
                address: token_wrapper.address(),
                token_type,
                decimals: 18,
                symbol: symbol.to_string(),
            });
        }
        
        // Verify balances
        for token in &tokens {
            let balance = token.contract.method::<_, U256>("balanceOf", deployer_address)?
                .call()
                .await?;
            debug!("Token {} balance: {}", token.symbol, balance / U256::exp10(18));
            if balance == U256::zero() {
                return Err(eyre::eyre!("Token {} has zero balance after minting", token.symbol));
            }
        }
        
        info!("Successfully deployed and minted {} tokens", tokens.len());
        Ok(tokens)
    }

    async fn deploy_contract(
        contract_name: &str,
        args: impl Tokenize,
        client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    ) -> Result<ContractWrapper> {
        info!("Deploying contract: {}", contract_name);

        // Test connectivity to Anvil
        match client.get_chainid().await {
            Ok(chain_id) => info!("Connected to Anvil on chain {}", chain_id),
            Err(e) => {
                error!("Cannot connect to Anvil: {:?}", e);
                return Err(eyre::eyre!("Anvil connection failed: {:?}", e));
            }
        }

        // Test if Anvil is responsive by getting latest block
        match client.get_block_number().await {
            Ok(block_num) => info!("Anvil is responsive, current block: {}", block_num),
            Err(e) => {
                error!("Anvil is not responsive: {:?}", e);
                return Err(eyre::eyre!("Anvil is not responsive: {:?}", e));
            }
        }
        let abi_path = match contract_name {
            "UniversalArbitrageExecutor" => "out/uar.sol/UniversalArbitrageExecutor.json".to_string(),
            "MockERC20" => "out/MockERC20.sol/MockERC20.json".to_string(),
            "MockUniswapV2Factory" => "out/MockUniswapV2Factory.sol/MockUniswapV2Factory.json".to_string(),
            "MockUniswapV2Router02" => "out/MockUniswapV2Router02.sol/MockUniswapV2Router02.json".to_string(),
            "MockAavePool" => "out/deploy_uar_simple.sol/MockAavePool.json".to_string(),
            "MockBalancerVault" => "out/deploy_uar_simple.sol/MockBalancerVault.json".to_string(),
            _ => format!("out/{}.sol/{}.json", contract_name, contract_name),
        };
        
        let abi_content = fs::read_to_string(&abi_path)
            .wrap_err_with(|| format!("Failed to read ABI file: {}", abi_path))?;
        let artifact: serde_json::Value = serde_json::from_str(&abi_content)?;
        let abi_value = artifact["abi"].as_array()
            .ok_or_else(|| eyre::eyre!("ABI not found in artifact"))?;
        let abi: ethers::abi::Abi = serde_json::from_value(serde_json::Value::Array(abi_value.clone()))?;
        let bytecode = artifact["bytecode"]["object"].as_str()
            .ok_or_else(|| eyre::eyre!("Bytecode not found in artifact"))?;
            
        info!("Creating contract factory for {}", contract_name);
        let factory = ContractFactory::new(abi.clone(), bytecode.parse()?, client.clone());
        
        info!("Creating deployer for {}", contract_name);
        let deployer = factory.deploy(args)?;

        info!("Sending deployment transaction for {}", contract_name);

        // Deploy the contract
        let deployed_contract = deployer.send()
            .await
            .wrap_err_with(|| format!("Failed to deploy contract {}", contract_name))?;

        info!("{} deployed successfully at: {:?}", contract_name, deployed_contract.address());

        // Recreate the contract instance with the ABI to ensure proper method resolution
        let contract = Contract::new(deployed_contract.address(), abi, client.clone());

        Ok(ContractWrapper::Manual(contract))
    }

    fn create_config(
        chain_name: &str,
        chain_id: u64,
        rpc_url: String,
        ws_url: String,
        private_key: &str,
        weth_address: Address,
        factory_address: Address,
        tokens: &[DeployedToken],
        uar_address: Address,
        mock_aave_pool_address: Address,
        mock_balancer_vault_address: Address,
        mock_weth_address: Address,
        test_mode: &TestMode,
    ) -> Result<Arc<Config>> {
        let chain_name = chain_name.to_string();
        // Disable database for tests to avoid connection issues
        if test_mode.enable_database {
            // Use a mock database URL that won't actually connect
            std::env::set_var("DATABASE_URL", "postgresql://test:test@localhost:5432/test_db");
        } else {
            // Ensure database is completely disabled for tests
            std::env::remove_var("DATABASE_URL");
            // Also make sure no other code can set it
            std::env::set_var("REQUIRE_POOL_REGISTRY", "false");
        }
        let mut config = Config {
            mode: ExecutionMode::Live,
            log_level: "info".to_string(),
            secrets: None,
            chain_config: ChainConfig {
                chains: HashMap::new(),
                rate_limiter_settings: RateLimiterSettings {
                    global_rps_limit: 1000,
                    default_chain_rps_limit: 100,
                },
            },
            // Ensure database is completely disabled for test mode
            strict_backtest: false,
            module_config: Some(ModuleConfig {
                arbitrage_settings: ArbitrageSettings { 
                    min_profit_usd: -1000.0, // More lenient for testing
                    min_value_ratio: rust_decimal::Decimal::new(1, 3), // More lenient ratio
                    trade_concurrency: 1,
                    max_opportunities_per_event: 10,
                    execution_timeout_secs: 60,
                },
                path_finder_settings: PathFinderSettings { 
                    max_hops: 3, // Reduced from 4 to limit search space
                    max_routes_to_consider: 5, // Reduced from 10
                    min_liquidity_usd: 0.001, // More lenient for testing
                    backtest_min_liquidity_multiplier: 1.0,
                    parallel_worker_count: 4,
                    max_slippage_bps: 500, // More lenient slippage
                    max_processed_nodes: 500, // Reduced from 1000
                    min_profit_usd: -1000.0, // More lenient for testing
                    adaptive_beam_width: true,
                    high_connectivity_threshold: 50,
                    beam_width: 5, // Reduced from 10
                    beam_width_high_connectivity: 10, // Reduced from 20
                    max_degree_per_token: 10, // Reduced from 100
                    heartbeat_interval_ms: 1000,
                    max_search_time_seconds: 10, // Increased for test stability
                    stagnation_threshold_live: 1,
                    stagnation_threshold_backtest: 1,
                    graph_rebuild_block_interval: 100,
                    max_search_time_seconds_backtest: 20, // Increased for test stability
                    // Newly added fields with safe testing defaults
                    max_search_nodes_ceiling: 10_000,
                    max_frontier_size: 2_000,
                    patience_seconds_live: 10, // Increased patience for live operations
                    patience_seconds_backtest: 15, // Increased patience for backtest operations
                    max_graph_nodes: 50_000,
                    max_graph_edges: 200_000,
                    graph_build_wait_max_attempts: 50, // Increased attempts for graph building
                    graph_build_wait_interval_ms: 200, // Increased interval between attempts
                    unknown_price_ratio_penalty: 10.0,
                    decimals_strict_backtest_allow_onchain_fallback: true,
                },
                transaction_optimizer_settings: TransactionOptimizerSettings { 
                    use_flashbots: false,
                    flashbots_endpoint: "".to_string(),
                    flashbots_signing_key: "".to_string(),
                    max_retry_attempts: 3,
                    confirmation_timeout_secs: 60,
                    mev_gas_multiplier_bps: 1100,
                    flashbots_block_offset: 1,
                    private_relays: Vec::new(),
                    max_concurrent_submissions: 5,
                },
                risk_settings: RiskSettings { 
                    max_risk_score: 0.95, // More lenient for testing
                    max_exposure_usd: 100000.0, // Higher exposure limit
                    max_slippage_bps: 500, // More lenient slippage
                    max_volatility: 0.5, // More lenient volatility
                    min_liquidity_usd: 0.001, // More lenient liquidity
                    risk_score_weights: RiskScoreWeights {
                        volatility: 0.3,
                        liquidity: 0.2,
                        slippage: 0.2,
                        mev: 0.2,
                        gas: 0.1,
                    },
                    min_gas_gwei: 1.0,
                    max_gas_gwei: 1000.0,
                    disable_sandwich: false,
                },
                gas_oracle_config: GasOracleConfig {
                    cache_ttl_secs: 60,
                    backtest_default_priority_fee_gwei: 1,
                    protocol_gas_estimates: HashMap::new(),
                    default_gas_estimate: 15000,
                    nearby_window_blocks: Some(10),
                    block_distance_tolerance: Some(5),
                },
                blockchain_settings: BlockchainSettings {
                    nonce_grace_blocks: 5,
                    tx_receipt_cache_ttl_secs: 300,
                    tx_receipt_cache_max_entries: 1000,
                    ws_reconnect_interval_secs: 5,
                    max_ws_reconnect_attempts: 10,
                    pending_tx_poll_interval_secs: 2,
                    block_poll_interval_secs: 1,
                    max_block_reorg_depth: 10,
                    block_timestamp_tolerance_secs: 30,
                    pool_state_cache_size: Some(1000),
                    pool_state_cache_ttl_seconds: Some(60),
                },
                data_science_settings: None,
                alpha_engine_settings: None,
                mev_engine_settings: None,
                chatbot_operator_settings: None,
                mempool_config: Some(MempoolConfig {
                    max_pending_txs: 1000,
                    parallel_workers: 4,
                    subscribe_pending_txs: true,
                    batch_size: Some(50),
                    batch_timeout_ms: Some(1000),
                }),
                cross_chain_settings: Some(CrossChainSettings {
                    bridge_transaction_timeout_secs: 180,
                    cross_chain_enabled: false,
                    min_profit_usd: 50.0,
                    min_synergy_score: 0.7,
                    max_bridge_fee_usd: 100.0,
                    bridge_slippage_bps: 100,
                    max_bridging_time_secs: 300,
                    bridge_health_threshold: 0.8,
                    // Newly required fields
                    min_confirmations: 1,
                    max_size_per_bridge_usd: 10_000.0,
                    timeout_penalty_usd: 0.0,
                    per_bridge_fee_bps: None,
                }),
                liquidation_config: None,
                jit_liquidity_config: None,
            }),
            backtest: None,
            gas_estimates: rust::config::GasEstimates {
                uniswap_v2: 90_000,
                uniswap_v3: 150_000,
                uniswap_v4: 180_000,
                sushiswap: 95_000,
                pancakeswap: 95_000,
                curve: 200_000,
                balancer: 250_000,
                other: 150_000,
            },
        };
        let per_chain_config = PerChainConfig {
            chain_id,
            chain_name: chain_name.clone(),
            rpc_url: rpc_url.clone(),
            ws_url: ws_url.clone(),
            private_key: private_key.to_string(),
            weth_address,
            live_enabled: true,
            backtest_archival_rpc_url: None,
            max_gas_price: U256::from(100_000_000_000u64),
            gas_bump_percent: Some(10),
            max_pending_transactions: Some(100),
            rps_limit: Some(100),
            max_concurrent_requests: Some(50),
            endpoints: Vec::new(),
            log_filter: None,
            reconnect_delay_ms: Some(1000),
            chain_type: "Evm".to_string(),
            factories: Some(vec![FactoryConfig {
                address: factory_address,
                protocol_type: ProtocolType::UniswapV2,
                // PairCreated(address,address,address,uint256)
                creation_event_topic: "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".to_string(),
                pair_created_topic: Some("0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".to_string()),
            }]),
            chainlink_native_usd_feed: None,
            reference_stablecoins: Some({
                let mut stablecoins: Vec<Address> = tokens.iter().map(|t| t.address).collect();
                stablecoins.push(mock_weth_address); // Add mock WETH as reference
                stablecoins
            }),
            router_tokens: Some({
                let mut router_tokens: Vec<Address> = tokens.iter().map(|t| t.address).collect();
                // Add UAR contract addresses for testing
                router_tokens.push(uar_address);
                router_tokens.push(mock_aave_pool_address);
                router_tokens.push(mock_balancer_vault_address);
                router_tokens.push(mock_weth_address);
                router_tokens
            }),
            bridges: None,
            multicall_address: None,
            default_base_fee_gwei: Some(20),
            default_priority_fee_gwei: Some(1),
            price_timestamp_tolerance_seconds: Some(300),
            avg_block_time_seconds: Some(12.0),
            volatility_sample_interval_seconds: Some(3600),
            volatility_max_samples: Some(1000),
            chainlink_token_feeds: None,
            stablecoin_symbols: None,
            is_test_environment: Some(true),
            price_lookback_blocks: None,
            fallback_window_blocks: None,
            reciprocal_tolerance_pct: None,
            same_token_io_min_ratio: Some(0.95),
            same_token_io_max_ratio: Some(1.05),
                            strict_backtest_no_lookahead: Some(true),
                backtest_use_external_price_apis: Some(false),
                is_l2: Some(false),
                max_ws_reconnect_attempts: Some(5),
                ws_reconnect_base_delay_ms: Some(1000),
                ws_max_backoff_delay_ms: Some(30000),
                ws_backoff_jitter_factor: Some(0.1),
                subscribe_pending_txs: Some(false),
                major_intermediaries: None,
                use_token_prices_table: None,
            };
        config.chain_config.chains.insert(chain_name, per_chain_config);
        Ok(Arc::new(config))
    }

    async fn initialize_components(
        config: &Arc<Config>,
        deployer_client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        test_mode: &TestMode,
        tokens: &[DeployedToken],
        pairs: &[DeployedPair],
        chain_name: &str,
    ) -> Result<(
        Arc<BlockchainManagerImpl>,
        Arc<dyn PoolManagerTrait + Send + Sync>,
        Arc<AStarPathFinder>,
        Arc<RiskManagerImpl>,
        Arc<ArbitrageEngine>,
        Arc<CrossChainPathfinder>,
        Arc<dyn PredictiveEngine + Send + Sync>,
        Arc<ArbitrageMetrics>,
        Arc<dyn AggregatorQuoteSource + Send + Sync>,
        Arc<dyn TransactionOptimizerTrait + Send + Sync>,
        Arc<GasOracle>,
        Arc<dyn PriceOracle + Send + Sync>,
        Arc<StateManager>,
        Arc<DefaultMempoolMonitor>,
    )> {
        debug!("Initializing QuoteSource");
        let quote_source = Arc::new(MultiAggregatorQuoteSource::new(Vec::new()));

        debug!("Initializing PriceOracle");
        let price_oracle: Arc<dyn PriceOracle + Send + Sync> = if test_mode.enable_mock_providers {
            let mock_oracle = mocks::MockPriceOracle::new(2000.0); // Default ETH price
            // Set up prices for test tokens
            // Set WETH price (first token)
            if let Some(weth_token) = tokens.first() {
                mock_oracle.set_price(weth_token.address, 2000.0); // WETH = $2000
            }
            // Set prices for other tokens
            for token in &tokens[1..] {
                match token.token_type {
                    TokenType::Standard => mock_oracle.set_price(token.address, 1.0),
                    TokenType::FeeOnTransfer => mock_oracle.set_price(token.address, 0.5),
                    TokenType::Reverting => mock_oracle.set_price(token.address, 0.1),
                }
            }
            Arc::new(mock_oracle)
        } else {
            // Create a temporary BlockchainManagerImpl for the LivePriceProvider
            let temp_blockchain_manager = Arc::new(BlockchainManagerImpl::new(
                config,
                chain_name,
                ExecutionMode::Live,
                None,
            ).await?);
            Arc::new(rust::price_oracle::live_price_provider::LivePriceProvider::new(
                config.clone(),
                temp_blockchain_manager,
                quote_source.clone(),
            ).await?)
        };

        debug!("Initializing BlockchainManager");
        debug!("Execution mode: Live");
        debug!("PRIVATE_KEY env var set: {}", std::env::var("PRIVATE_KEY").is_ok());
        if let Ok(pk) = std::env::var("PRIVATE_KEY") {
            debug!("PRIVATE_KEY length: {}", pk.len());
            debug!("PRIVATE_KEY starts with 0x: {}", pk.starts_with("0x"));
        }

        let blockchain_manager_result = BlockchainManagerImpl::new(
            config,
            chain_name,
            ExecutionMode::Live,
            Some(price_oracle.clone()),
        ).await;

        let blockchain_manager = match blockchain_manager_result {
            Ok(bm) => {
                debug!("BlockchainManager initialized successfully");
                Arc::new(bm)
            }
            Err(e) => {
                error!("BlockchainManager initialization failed: {:?}", e);
                error!("Config details: chain_name={}, has_private_key={}",
                       chain_name,
                       std::env::var("PRIVATE_KEY").is_ok());
                return Err(e.into());
            }
        };

        debug!("Initializing PoolManager");
        let mut pool_manager = PoolManager::new_for_testing(config.clone())?;
        // Register all deployed pairs with the pool manager BEFORE creating path finder
        println!("POOL_REGISTRATION: Registering {} pairs with pool manager for chain '{}'", pairs.len(), chain_name);
        debug!("Registering {} pairs with pool manager for chain '{}'", pairs.len(), chain_name);
        for (i, pair) in pairs.iter().enumerate() {
            let token0 = tokens.iter().find(|t| t.address == pair.token0).unwrap();
            let token1 = tokens.iter().find(|t| t.address == pair.token1).unwrap();
            debug!("Registering pair {}: token0={:?}, token1={:?}, pair_address={:?}", i, pair.token0, pair.token1, pair.address);
            pool_manager.register_test_pool(
                pair.address,
                pair.token0,
                pair.token1,
                token0.decimals,
                token1.decimals,
                Some(30), // 0.3% fee for UniswapV2 (30 bps)
                ProtocolType::UniswapV2,
                "UniswapV2".to_string(),
                chain_name.to_string(),
                1, // creation block
            ).await;
        }
        
        // Debug: Print registered pools
        let all_pools = pool_manager.get_all_pools(chain_name, None).await?;
        debug!("Registered {} pools with pool manager for chain '{}'", all_pools.len(), chain_name);
        if all_pools.is_empty() {
            warn!("No pools found after registration - this will cause pathfinder graph to be empty!");
        }
        for pool in &all_pools {
            debug!("Registered pool: {:?} (token0: {:?}, token1: {:?})",
                   pool.pool_address, pool.token0, pool.token1);
        }

        // Set mock reserve data for test pools to enable path finding
        for pool in &all_pools {
            // Set reasonable reserve amounts for testing (1000 tokens each)
            let reserve0 = rust_decimal::Decimal::from(1000);
            let reserve1 = rust_decimal::Decimal::from(1000);
            if let Err(e) = pool_manager.set_test_pool_reserves(pool.pool_address, reserve0, reserve1).await {
                warn!("Failed to set test pool reserves for {:?}: {:?}", pool.pool_address, e);
            }
        }

        let pool_manager: Arc<dyn PoolManagerTrait + Send + Sync> = if test_mode.enable_mock_providers {
            let mut mock_pm = mocks::MockPoolManager::new();
            mock_pm.add_test_pools(pairs, tokens);
            Arc::new(mock_pm)
        } else {
            Arc::new(pool_manager)
        };

        // Now create PoolStateOracle
        debug!("Creating PoolStateOracle");
        let mut blockchain_managers = HashMap::new();
        blockchain_managers.insert(chain_name.to_string(), blockchain_manager.clone() as Arc<dyn rust::blockchain::BlockchainManager>);

        // Create PoolStateOracle
        let pool_state_oracle = Arc::new(PoolStateOracle::new(
            config.clone(),
            blockchain_managers,
            None,
            pool_manager.clone(),
        )?);

        // CRITICAL: Set the state oracle on the pool manager
        debug!("Setting PoolStateOracle on PoolManager");
        pool_manager.set_state_oracle(pool_state_oracle.clone()).await?;

        // For mock providers, set mock pool state data
        if test_mode.enable_mock_providers {
            debug!("Setting mock pool state data for test pools");
            for pool_meta in &all_pools {
                let mock_state = rust::pool_states::PoolStateData {
                    pool_state: PoolStateFromSwaps {
                        pool_address: pool_meta.pool_address,
                        token0: pool_meta.token0,
                        token1: pool_meta.token1,
                        reserve0: ethers::types::U256::from(1000000000000000000000u128), // 1000 tokens with 18 decimals
                        reserve1: ethers::types::U256::from(1000000000000000000000u128), // 1000 tokens with 18 decimals
                        sqrt_price_x96: None,
                        liquidity: None,
                        tick: None,
                        v3_tick_data: None,
                        v3_tick_current: None,
                        fee_tier: pool_meta.fee.unwrap_or(30),
                        protocol: match pool_meta.protocol_type {
                            ProtocolType::UniswapV2 => DexProtocol::UniswapV2,
                            ProtocolType::UniswapV3 => DexProtocol::UniswapV3,
                            _ => DexProtocol::UniswapV2,
                        },
                        dex_protocol: match pool_meta.protocol_type {
                            ProtocolType::UniswapV2 => DexProtocol::UniswapV2,
                            ProtocolType::UniswapV3 => DexProtocol::UniswapV3,
                            _ => DexProtocol::UniswapV2,
                        },
                        protocol_type: pool_meta.protocol_type.clone(),
                        block_number: 0,
                        timestamp: 0,
                        token0_decimals: pool_meta.token0_decimals as u32,
                        token1_decimals: pool_meta.token1_decimals as u32,
                        price0_cumulative_last: None,
                        price1_cumulative_last: None,
                        last_reserve_update_timestamp: Some(0),
                    },
                    block_number: 0,
                    timestamp: 0,
                    source: rust::pool_states::PoolStateSource::HistoricalDb,
                };

                pool_state_oracle.set_mock_pool_state("anvil", pool_meta.pool_address, None, mock_state).await?;
            }
        }

        debug!("Initializing GasOracle");
        let live_gas_provider = Arc::new(rust::gas_oracle::live_gas_provider::LiveGasProvider::new(
            chain_name.to_string(),
            blockchain_manager.get_chain_id(),
            config.clone(),
            config.module_config.as_ref().unwrap().gas_oracle_config.clone(),
            blockchain_manager.clone(),
        ));
        let gas_oracle = Arc::new(GasOracle::new(live_gas_provider.clone()));

        debug!("Initializing PathFinder");
        let router_tokens = config.chain_config.chains.get(chain_name).and_then(|c| c.router_tokens.clone()).unwrap_or_default();
        let path_finder = AStarPathFinder::new(
            config.module_config.as_ref().unwrap().path_finder_settings.clone(),
            pool_manager.clone(),
            blockchain_manager.clone(),
            config.clone(),
            gas_oracle.clone(),
            price_oracle.clone(),
            blockchain_manager.get_chain_id(),
            false,
            router_tokens,
        )?;
        
        // Initialize the pathfinder graph AFTER pools are registered
        debug!("Building pathfinder graph with registered pools");
        path_finder.preload_data().await?;
        let path_finder = Arc::new(path_finder);

        // After pathfinder graph build
        let snapshot = path_finder.get_graph_snapshot().await;
        println!("=== PATHFINDER GRAPH DEBUG ===");
        println!("Graph nodes: {}", snapshot.node_count());
        println!("Graph edges: {}", snapshot.edge_count());
        for node in snapshot.graph.node_indices() {
            println!("Node: {:?}", snapshot.graph[node]);
        }
        for edge in snapshot.graph.edge_indices() {
            let (a, b) = snapshot.graph.edge_endpoints(edge).unwrap();
            println!(
                "Edge: {:?} <-> {:?}, pool: {:?}",
                snapshot.graph[a], snapshot.graph[b], snapshot.graph[edge].pool_address
            );
        }

        debug!("Initializing RiskManager");
        let module_cfg_risk = config.module_config.as_ref().unwrap();
        let risk_manager = Arc::new(RiskManagerImpl::new(
            Arc::new(module_cfg_risk.risk_settings.clone()),
            blockchain_manager.clone(),
            pool_manager.clone(),
            live_gas_provider.clone(),
            price_oracle.clone(),
        ));

        debug!("Initializing TransactionOptimizer");
        let transaction_optimizer: Arc<dyn TransactionOptimizerTrait + Send + Sync> =
            if test_mode.enable_mock_providers {
                let mock = Arc::new(mocks::MockTransactionOptimizer::new());
                mock.set_wallet_address(deployer_client.address());
                mock
            } else {
                TransactionOptimizer::new(
                    Arc::new(module_cfg_risk.transaction_optimizer_settings.clone()),
                    rust::transaction_optimizer::EnhancedOptimizerConfig::default(),
                    blockchain_manager.clone(),
                    live_gas_provider.clone(),
                    price_oracle.clone(),
                    None,
                    deployer_client.address(),
                ).await?
            };

        debug!("Creating metrics instance");
        let metrics = Arc::new(rust::types::ArbitrageMetrics::new());

        debug!("Initializing StateManager");
        let state_config = StateConfig {
            db: if test_mode.enable_database {
                Some(StateDbConfig {
                    host: "localhost".to_string(),
                    port: 5432,
                    user: "ichevako".to_string(),
                    password: "maddie".to_string(),
                    dbname: "lyfeblood".to_string(),
                })
            } else {
                None
            },
            persist_trades: true,
        };
        let state_manager = Arc::new(StateManager::new(state_config).await?);

        debug!("Initializing MempoolMonitor");
        let mempool_config = Arc::new(MempoolConfig {
            max_pending_txs: 1000,
            parallel_workers: 4,
            subscribe_pending_txs: true,
            batch_size: Some(50),
            batch_timeout_ms: Some(1000),
        });
        let mempool_monitor = Arc::new(DefaultMempoolMonitor::new(
            mempool_config,
            blockchain_manager.clone(),
        )?);

        debug!("Initializing ArbitrageEngine");
        let arbitrage_engine = Arc::new(ArbitrageEngine {
            config: Arc::new(config.module_config.as_ref().unwrap().arbitrage_settings.clone()),
            full_config: config.clone(),
            price_oracle: price_oracle.clone(),
            risk_manager: Arc::new(tokio::sync::RwLock::new(Some(risk_manager.clone()))),
            blockchain_manager: blockchain_manager.clone(),
            quote_source: quote_source.clone(),
            global_metrics: metrics.clone(),
            alpha_engine: None,
            swap_loader: None,
        });

        debug!("Initializing CrossChainPathfinder");
        let cross_chain_pathfinder = Arc::new(CrossChainPathfinder::new(
            Arc::new(HashMap::new()),
            Arc::new(config.module_config.as_ref().unwrap().cross_chain_settings.clone().expect("Cross chain settings must be available")),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            price_oracle.clone(),
            live_gas_provider.clone(),
            Arc::new(MockSynergyScorer),
            Arc::new(HashMap::new()),
        ));

        debug!("Initializing PredictiveEngine");
        let predictive_engine: Arc<dyn PredictiveEngine + Send + Sync> = Arc::new(MockPredictiveEngine);

        info!("All system components initialized successfully");

        Ok((
            blockchain_manager,
            pool_manager,
            path_finder,
            risk_manager,
            arbitrage_engine,
            cross_chain_pathfinder,
            predictive_engine,
            metrics,
            quote_source,
            transaction_optimizer,
            gas_oracle,
            price_oracle,
            state_manager,
            mempool_monitor,
        ))
    }

    pub fn token(&self, index: usize) -> Option<&DeployedToken> {
        self.tokens.get(index)
    }
    
        pub fn pair(&self, index: usize) -> Option<&DeployedPair> {
        self.pairs.get(index)
    }

    pub fn get_pair_contract(&self, pair_address: Address) -> Result<Contract<SignerMiddleware<Provider<Http>, LocalWallet>>> {
        // Create a contract instance for the pair address using the deployer client
        let contract_bytes = include_bytes!("../../out/MockUniswapV2Pair.sol/MockUniswapV2Pair.json");
        let artifact: serde_json::Value = serde_json::from_slice(contract_bytes)?;
        let abi = artifact["abi"].clone();
        let abi: ethers::abi::Abi = serde_json::from_value(abi)?;
        let contract = Contract::new(
            pair_address,
            abi,
            self.deployer_client.clone(),
        );
        Ok(contract)
    }

    pub fn get_token_contract(&self, token_address: Address) -> Result<Contract<SignerMiddleware<Provider<Http>, LocalWallet>>> {
        // Create a contract instance for the token address using the deployer client
        let contract_bytes = include_bytes!("../../out/MockERC20.sol/MockERC20.json");
        let artifact: serde_json::Value = serde_json::from_slice(contract_bytes)?;
        let abi = artifact["abi"].clone();
        let abi: ethers::abi::Abi = serde_json::from_value(abi)?;
        let contract = Contract::new(
            token_address,
            abi,
            self.deployer_client.clone(),
        );
        Ok(contract)
    }

    pub fn weth(&self) -> Address {
        self.weth_address
    }
    
    pub fn deployer(&self) -> Address {
        self.deployer_address
    }
    
    pub async fn create_and_fund_pair(
        &self,
        token0_index: usize,
        token1_index: usize,
        amount0: U256,
        amount1: U256,
    ) -> Result<Address> {
        let token0 = &self.tokens[token0_index];
        let token1 = &self.tokens[token1_index];

        let token0_symbol = token0.symbol.clone();
        let token1_symbol = token1.symbol.clone();
        info!("Adding additional liquidity to pair between {} ({}) and {} ({})", token0_symbol, token0.address, token1_symbol, token1.address);

        // Find the already created pair
        let pair = self.pairs.iter().find(|p| {
            (p.token0 == token0.address && p.token1 == token1.address) ||
            (p.token0 == token1.address && p.token1 == token0.address)
        }).ok_or_else(|| eyre::eyre!("Pair not found for tokens: {} {}", token0.address, token1.address))?;

        // Approve tokens for the router
        token0.contract.method::<_, bool>("approve", (self.router_contract.address(), amount0))?.send().await?.await?;
        token1.contract.method::<_, bool>("approve", (self.router_contract.address(), amount1))?.send().await?.await?;

        let deadline = U256::from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
        let _receipt = self.router_contract
            .method::<_, (U256, U256, U256)>(
                "addLiquidity",
                (
                    token0.address,
                    token1.address,
                    amount0,
                    amount1,
                    U256::one(),
                    U256::one(),
                    self.deployer_address,
                    deadline,
                ),
            )?
            .send()
            .await?
            .await?
            .ok_or_else(|| eyre::eyre!("addLiquidity transaction failed to confirm"))?;

        info!("Added liquidity to pair at address: {} for {} <-> {}", pair.address, token0_symbol, token1_symbol);
        Ok(pair.address)
    }
    
    /// Safely get pool info, detecting pool type first to avoid calling wrong methods
    pub async fn safe_get_pool_info(&self, pool_address: Address) -> Result<rust::types::PoolInfo> {
        // First, try to determine pool type from our known pairs
        let known_pair = self.pairs.iter().find(|p| p.address == pool_address);
        
        // All test pools are UniswapV2 by default
        let pool_type = if known_pair.is_some() {
            ProtocolType::UniswapV2
        } else {
            // Try to detect pool type by checking bytecode patterns
            match self.blockchain_manager.get_pool_type(pool_address).await {
                Ok(pt) => pt,
                Err(e) => {
                    debug!("Failed to detect pool type for {:?}, defaulting to UniswapV2: {}", pool_address, e);
                    ProtocolType::UniswapV2
                }
            }
        };

        match pool_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                // Get V2-specific info
                let pair_contract = self.get_pair_contract(pool_address)?;
                let (reserve0, reserve1, _) = pair_contract
                    .method::<_, (U256, U256, u32)>("getReserves", ())?
                    .call()
                    .await?;

                // Get token addresses
                let token0: Address = pair_contract.method("token0", ())?.call().await?;
                let token1: Address = pair_contract.method("token1", ())?.call().await?;

                Ok(rust::types::PoolInfo {
                    address: pool_address,
                    protocol_type: pool_type,
                    token0,
                    token1,
                    token0_decimals: 18, // All test tokens use 18 decimals
                    token1_decimals: 18,
                    chain_name: self.chain_name.clone(),
                    fee: Some(30), // 0.3% for V2
                    last_updated: 0,
                    v2_reserve0: Some(reserve0),
                    v2_reserve1: Some(reserve1),
                    v2_block_timestamp_last: None,
                    v3_sqrt_price_x96: None,
                    v3_liquidity: None,
                    v3_ticks: None,
                    v3_tick_data: None,
                    v3_tick_current: None,
                    reserves0: reserve0,
                    reserves1: reserve1,
                    liquidity_usd: 0.0,
                    pool_address,
                    fee_bps: 30,
                    creation_block_number: None,
                })
            }
            ProtocolType::UniswapV3 => {
                // Handle V3 pools (not used in tests but included for completeness)
                warn!("UniswapV3 pool detected in test harness at {:?} - this is unexpected", pool_address);
                Ok(rust::types::PoolInfo {
                    address: pool_address,
                    protocol_type: ProtocolType::UniswapV3,
                    token0: Address::zero(),
                    token1: Address::zero(),
                    token0_decimals: 18,
                    token1_decimals: 18,
                    chain_name: self.chain_name.clone(),
                    fee: Some(3000), // 0.3% for V3
                    last_updated: 0,
                    v2_reserve0: None,
                    v2_reserve1: None,
                    v2_block_timestamp_last: None,
                    v3_sqrt_price_x96: None,
                    v3_liquidity: None,
                    v3_ticks: None,
                    v3_tick_data: None,
                    v3_tick_current: None,
                    reserves0: U256::zero(),
                    reserves1: U256::zero(),
                    liquidity_usd: 0.0,
                    pool_address,
                    fee_bps: 3000,
                    creation_block_number: None,
                })
            }
            _ => Err(eyre::eyre!("Unsupported pool type: {:?}", pool_type))
        }
    }

    pub async fn create_imbalanced_pool_via_swap(
        &self,
        pair_index: usize,
        swap_amount: U256,
    ) -> Result<u64> {
        let pair = &self.pairs[pair_index];
        let token0 = self.tokens.iter().find(|t| t.address == pair.token0).ok_or_else(|| eyre::eyre!("Token0 not found for pair"))?;
        
        info!("Creating imbalance in pool {}: swapping {} tokens of {}", pair_index, swap_amount / U256::exp10(18), token0.symbol);
        
        // Check initial reserves
        let pair_contract = self.get_pair_contract(pair.address)?;
        let (initial_r0, initial_r1, _) = pair_contract
            .method::<_, (U256, U256, u32)>("getReserves", ())?
            .call()
            .await?;
        info!("Initial reserves: r0={}, r1={}", initial_r0, initial_r1);
        
        // Approve tokens
        let router_address = self.router_contract.address();
        let approve_params = (router_address, swap_amount);
        let method_call = token0.contract.method::<_, bool>(
            "approve",
            approve_params
        )?;
        let approve_tx = method_call.send().await?;
        let approve_receipt = approve_tx.await?;
        if let Some(receipt) = approve_receipt {
            info!("Approval transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
        }
        
        // Execute swap
        let deadline = U256::from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
        let amount_out_min = U256::zero();
        let router_ref = &self.router_contract;
        let path = vec![pair.token0, pair.token1];
        
        let swap_params = (
            swap_amount,
            amount_out_min,
            path,
            self.deployer_address,
            deadline,
        );
        let method_call = router_ref.method::<_, Vec<U256>>(
            "swapExactTokensForTokens",
            swap_params
        )?;
        let swap_tx = method_call.send().await?;
        
        // Wait for the transaction to be confirmed and get the receipt
        let swap_receipt = swap_tx.await?
            .ok_or_else(|| eyre::eyre!("Swap transaction for imbalance did not return a receipt"))?;

        info!("Swap transaction: hash={:?}, status={:?}", swap_receipt.transaction_hash, swap_receipt.status);
        
        // Ensure the transaction was successful
        if swap_receipt.status != Some(1.into()) {
            return Err(eyre::eyre!("Imbalancing swap transaction failed (reverted)"));
        }

        // Extract and return the block number
        let confirmation_block = swap_receipt.block_number
            .ok_or_else(|| eyre::eyre!("Swap receipt did not contain a block number"))?
            .as_u64();
            
        info!("Imbalance swap confirmed in block {}", confirmation_block);
        
        // Check final reserves
        let (final_r0, final_r1, _) = pair_contract
            .method::<_, (U256, U256, u32)>("getReserves", ())?
            .call()
            .await?;
        info!("Final reserves: r0={}, r1={}", final_r0, final_r1);
        
        if final_r0 == initial_r0 && final_r1 == initial_r1 {
            warn!("Reserves unchanged after swap - imbalance not created!");
            return Err(eyre::eyre!("Swap did not change pool reserves"));
        }
        
        info!("Successfully created imbalance: r0 changed by {}, r1 changed by {}", 
              final_r0.saturating_sub(initial_r0), final_r1.saturating_sub(initial_r1));
        
        Ok(confirmation_block)
    }
    
    pub async fn find_arbitrage_opportunities(&self, block_number: u64) -> Result<Vec<ArbitrageOpportunity>> {
        // --- NEW LOGIC: Create realistic, synthetic swap events to trigger the engine ---
        let mut swap_events = Vec::new();

        // Create a swap event for each pair to ensure all markets are checked.
        for pair in &self.pairs {
            // Simulate a small swap to trigger analysis of this pool.
            // The amount doesn't need to be huge, just non-zero.
            let amount_in = U256::from(1) * U256::exp10(17); // 0.1 tokens

            let event = rust::types::DexSwapEvent {
                chain_name: self.chain_name.clone(),
                protocol: rust::types::DexProtocol::UniswapV2,
                transaction_hash: H256::random(),
                block_number,
                pool_address: pair.address,
                token_in: Some(pair.token0),
                token_out: Some(pair.token1),
                amount_in: Some(amount_in),
                log_index: U256::zero(),
                data: rust::types::SwapData::UniswapV2 {
                    sender: self.deployer_address,
                    to: self.deployer_address,
                    amount0_in: if rand::random() { amount_in } else { U256::zero() },
                    amount1_in: if rand::random() { U256::zero() } else { amount_in },
                    amount0_out: U256::zero(),
                    amount1_out: U256::zero(),
                },
                unix_ts: Some(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs()),
                token0: Some(pair.token0),
                token1: Some(pair.token1),
                price_t0_in_t1: None,
                price_t1_in_t0: None,
                reserve0: None,
                reserve1: None,
                transaction: None,
                transaction_metadata: None,
                amount_out: None,
                pool_id: None,
                buyer: None,
                sold_id: None,
                tokens_sold: None,
                bought_id: None,
                tokens_bought: None,
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                token0_decimals: Some(18),
                token1_decimals: Some(18),
                fee_tier: None,
                protocol_type: Some(ProtocolType::UniswapV2),
            };
            swap_events.push(event);
        }
        
        info!("Created {} synthetic swap events to trigger arbitrage detection", swap_events.len());

        let opportunities = self.arbitrage_engine.find_opportunities(&swap_events).await
            .map_err(|e| eyre::eyre!("Failed to find opportunities: {:?}", e))?;
        
        info!("Found {} arbitrage opportunities", opportunities.len());
        for (i, opp) in opportunities.iter().enumerate() {
            info!("Opportunity {}: profit=${:.2}, route={:?}", i, opp.profit_usd, opp.route);
        }
        
        Ok(opportunities)
    }
    
    pub async fn simulate_trade(&self, route: &rust::types::ArbitrageRoute, block_number: u64) -> Result<(U256, f64)> {
        self.transaction_optimizer.simulate_trade_submission(route, block_number).await
            .map_err(|e| eyre::eyre!("Failed to simulate trade: {:?}", e))
    }
    
    pub async fn get_pool_info(&self, pool_address: Address) -> Result<rust::types::PoolInfo> {
        // Note: get_pool_state is not available on BlockchainManager, use pool_manager instead
        self.pool_manager.get_pool_info(&self.chain_name, pool_address, None).await
            .map_err(|e| eyre::eyre!("Failed to get pool info: {:?}", e))
    }
    
    pub async fn get_token_price(&self, token: Address) -> Result<f64> {
        self.price_oracle.get_token_price_usd(&self.chain_name, token).await
            .map_err(|e| eyre::eyre!("Failed to get token price: {:?}", e))
    }
    
    pub async fn get_gas_price(&self) -> Result<rust::gas_oracle::GasPrice> {
        self.gas_oracle.get_gas_price(&self.chain_name, None).await
            .map_err(|e| eyre::eyre!("Failed to get gas price: {:?}", e))
    }

    /// Helper to create a transaction with proper gas settings
    pub fn with_gas<M, T>(call: ContractCall<M, T>) -> ContractCall<M, T>
    where
        M: Middleware,
        T: Tokenize + Detokenize,
    {
        call.gas(1_000_000u64)
            .legacy()
            .gas_price(20_000_000_000u64)
    }
    
    /// Execute a contract method with default gas settings
    pub async fn execute_with_gas<T: Tokenize + Clone, R: Detokenize>(
        &self,
        contract: &Contract<SignerMiddleware<Provider<Http>, LocalWallet>>,
        method_name: &str,
        args: T,
    ) -> Result<R> {
        let receipt = contract
            .method::<_, R>(method_name, args.clone())?
            .legacy()
            .gas(1_000_000u64)
            .gas_price(20_000_000_000u64)
            .send()
            .await?
            .await?;
        
        // Extract the return value from the receipt
        match receipt {
            Some(_) => Ok(contract.method::<_, R>(method_name, args)?.call().await?),
            None => Err(eyre::eyre!("Transaction failed"))
        }
    }
    
    pub async fn wait_with_timeout<F, T>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        timeout(Duration::from_secs(self.test_mode.timeout_seconds), future).await
            .map_err(|_| eyre::eyre!("Operation timed out"))?
    }
    
    pub async fn run_with_chaos_testing<F, T>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if self.test_mode.enable_chaos_testing {
            info!("Running with chaos testing enabled");
            // Add random delays and failures for chaos testing
            tokio::time::sleep(Duration::from_millis(rand::random::<u64>() % 100)).await;
        }
        self.wait_with_timeout(future).await
    }
    
    pub async fn run_with_concurrency_limit<F, T>(&self, futures: Vec<F>) -> Result<Vec<T>>
    where
        F: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(self.test_mode.max_concurrent_operations));
        let futures_with_semaphore: Vec<_> = futures.into_iter().map(|future| {
            let semaphore = semaphore.clone();
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                future.await
            }
        }).collect();
        
        join_all(futures_with_semaphore).await.into_iter().collect()
    }

    // UAR Contract Helper Methods
    pub async fn add_trusted_target_to_uar(&self, target: Address) -> Result<()> {
        self.uar_contract.method::<_, ()>("addTrustedTarget", target)?
            .legacy()
            .gas(100_000u64)  // Add gas limit
            .send()
            .await?
            .await?;
        Ok(())
    }

    pub async fn remove_trusted_target_from_uar(&self, target: Address) -> Result<()> {
        self.uar_contract.method::<_, ()>("removeTrustedTarget", target)?
            .legacy()
            .gas(100_000u64)  // Add gas limit
            .send()
            .await?
            .await?;
        Ok(())
    }

    pub async fn is_trusted_target_in_uar(&self, target: Address) -> Result<bool> {
        let result = self.uar_contract.method::<_, bool>("isTrustedTarget", target)?
            .call()
            .await?;
        Ok(result)
    }

    pub async fn get_uar_owner(&self) -> Result<Address> {
        let result = self.uar_contract.method::<_, Address>("owner", ())?
            .call()
            .await?;
        Ok(result)
    }

    pub async fn execute_uar_actions(&self, actions: Vec<UarAction>, profit_token: Address) -> Result<()> {
        // Convert actions to the format expected by the contract
        let contract_actions: Vec<(u8, Address, Address, U256, Vec<u8>)> = actions.into_iter()
            .map(|action| {
                let action_type = match action.action_type {
                    UarActionType::Swap => 0u8,
                    UarActionType::WrapNative => 1u8,
                    UarActionType::UnwrapNative => 2u8,
                    UarActionType::TransferProfit => 3u8,
                };
                (action_type, action.target, action.token_in, action.amount_in, action.call_data)
            })
            .collect();

        self.uar_contract.method::<_, ()>("execute", (contract_actions, profit_token))?
            .legacy()
            .gas(500_000u64)  // Add gas limit for complex operation
            .send()
            .await?
            .await?;
        Ok(())
    }

    pub async fn test_uar_flash_loan(&self, asset: Address, amount: U256, actions: Vec<UarAction>, profit_token: Address) -> Result<()> {
        // Test Aave flash loan
        let params = ethers::abi::encode(&[
            ethers::abi::Token::Array(actions.into_iter().map(|action| {
                let action_type = match action.action_type {
                    UarActionType::Swap => 0u8,
                    UarActionType::WrapNative => 1u8,
                    UarActionType::UnwrapNative => 2u8,
                    UarActionType::TransferProfit => 3u8,
                };
                ethers::abi::Token::Tuple(vec![
                    ethers::abi::Token::Uint(ethers::types::U256::from(action_type)),
                    ethers::abi::Token::Address(action.target),
                    ethers::abi::Token::Address(action.token_in),
                    ethers::abi::Token::Uint(action.amount_in),
                    ethers::abi::Token::Bytes(action.call_data),
                ])
            }).collect()),
            ethers::abi::Token::Address(profit_token),
        ]);

        self.mock_aave_pool.method::<_, ()>("flashLoanSimple", (
            self.uar_contract.address(),
            asset,
            amount,
            params,
            0u16, // referral code
        ))?
            .send()
            .await?
            .await?;
        Ok(())
    }

    /// Create actual arbitrage opportunities by creating pool imbalances
    pub async fn create_arbitrage_opportunity(&self) -> Result<()> {
        info!("Creating arbitrage opportunity with imbalanced pools...");
        
        // First, identify which pools form a proper triangle
        // We need pools that connect: WETH -> STD -> FEE -> WETH
        let mut weth_std_pool = None;
        let mut std_fee_pool = None;
        let mut fee_weth_pool = None;
        
        // Find the correct pools
        for (i, pair) in self.pairs.iter().enumerate() {
            let token0_idx = self.tokens.iter().position(|t| t.address == pair.token0);
            let token1_idx = self.tokens.iter().position(|t| t.address == pair.token1);
            
            if let (Some(idx0), Some(idx1)) = (token0_idx, token1_idx) {
                // WETH (0) <-> STD (1)
                if (idx0 == 0 && idx1 == 1) || (idx0 == 1 && idx1 == 0) {
                    weth_std_pool = Some(i);
                    info!("Found WETH-STD pool at index {}", i);
                }
                // STD (1) <-> FEE (2)
                else if (idx0 == 1 && idx1 == 2) || (idx0 == 2 && idx1 == 1) {
                    std_fee_pool = Some(i);
                    info!("Found STD-FEE pool at index {}", i);
                }
                // FEE (2) <-> WETH (0)
                else if (idx0 == 2 && idx1 == 0) || (idx0 == 0 && idx1 == 2) {
                    fee_weth_pool = Some(i);
                    info!("Found FEE-WETH pool at index {}", i);
                }
            }
        }
        
        // If we don't have all three pools for a triangle, create a simpler arbitrage
        if weth_std_pool.is_none() || std_fee_pool.is_none() || fee_weth_pool.is_none() {
            info!("Complete triangle not found, creating simple two-pool arbitrage");
            
            // Just create extreme imbalance in first two pools
            if self.pairs.len() >= 2 {
                // Make first pool extremely imbalanced - swap 95% of one token
                let huge_swap = U256::from(95) * U256::exp10(18);
                let _ = self.create_imbalanced_pool_via_swap(0, huge_swap).await?;
                
                // Make second pool imbalanced in opposite direction - swap 90% of the other token
                let reverse_swap = U256::from(90) * U256::exp10(18);
                let _ = self.create_imbalanced_pool_via_swap(1, reverse_swap).await?;
            }
            return Ok(());
        }
        
        // We have a triangle! Create arbitrage opportunity with much larger imbalances
        info!("Creating triangular arbitrage opportunity with extreme imbalances");
        
        // Step 1: Make STD extremely cheap in WETH-STD pool (swap 99% of WETH for STD)
        if let Some(pool_idx) = weth_std_pool {
            let swap_amount = U256::from(99) * U256::exp10(18); // 99 WETH (99% of total)
            info!("Swapping {} WETH for STD to make STD extremely cheap", swap_amount / U256::exp10(18));
            match self.create_imbalanced_pool_via_swap(pool_idx, swap_amount).await {
                Ok(block) => {
                    info!("✅ Successfully created WETH-STD imbalance in block {}", block);
                }
                Err(e) => {
                    warn!("❌ Failed to create WETH-STD imbalance: {}", e);
                    return Err(e);
                }
            }
        }
        
        // Step 2: Make FEE extremely cheap in STD-FEE pool (swap 98% of STD for FEE)
        if let Some(_pool_idx) = std_fee_pool {
            // First approve STD tokens
            let std_token = &self.tokens[1];
            let swap_amount = U256::from(98) * U256::exp10(18); // 98 STD (98% of total)
            
            info!("Approving {} STD tokens for router", swap_amount / U256::exp10(18));
            let router_address = self.router_contract.address();
            let token0_contract = &std_token.contract;
            let method_call = token0_contract.method::<_, bool>(
                "approve",
                (router_address, swap_amount)
            )?;
            let approve_tx = method_call.send().await?;
            let approve_receipt = approve_tx.await?;
            if let Some(receipt) = approve_receipt {
                info!("Approval transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
            }
            
            info!("Swapping {} STD for FEE to make FEE extremely cheap", swap_amount / U256::exp10(18));
            
            let deadline = U256::from(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
            
            let method_call = self.router_contract.method::<_, Vec<U256>>(
                "swapExactTokensForTokens",
                (
                    swap_amount,
                    U256::zero(), // Accept any output
                    vec![self.tokens[1].address, self.tokens[2].address],
                    self.deployer_address,
                    deadline,
                )
            )?;
            let swap_tx = method_call.send().await?;
            
            let swap_receipt = swap_tx.await?;
            if let Some(receipt) = swap_receipt {
                info!("STD-FEE swap transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
                if let Some(status) = receipt.status {
                    if status.as_u64() == 0 {
                        warn!("❌ STD-FEE swap transaction failed");
                        return Err(eyre::eyre!("STD-FEE swap transaction failed"));
                    }
                }
            }
            info!("✅ Successfully created STD-FEE imbalance");
        }
        
        // Step 3: Make WETH extremely expensive in FEE-WETH pool (swap 97% of FEE for WETH)
        if let Some(_pool_idx) = fee_weth_pool {
            // First approve FEE tokens
            let fee_token = &self.tokens[2];
            let swap_amount = U256::from(97) * U256::exp10(18); // 97 FEE (97% of total)
            
            info!("Approving {} FEE tokens for router", swap_amount / U256::exp10(18));
            let router_address = self.router_contract.address();
            let method_call = fee_token.contract.method::<_, bool>(
                "approve",
                (router_address, swap_amount)
            )?;
            let approve_tx = method_call.send().await?;
            let approve_receipt = approve_tx.await?;
            if let Some(receipt) = approve_receipt {
                info!("Approval transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
            }
            
            info!("Swapping {} FEE for WETH to make WETH extremely expensive", swap_amount / U256::exp10(18));
            
            let deadline = U256::from(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
            
            let method_call = self.router_contract.method::<_, Vec<U256>>(
                "swapExactTokensForTokens",
                (
                    swap_amount,
                    U256::zero(), // Accept any output
                    vec![self.tokens[2].address, self.tokens[0].address],
                    self.deployer_address,
                    deadline,
                )
            )?;
            let swap_tx = method_call.send().await?;
            
            let swap_receipt = swap_tx.await?;
            if let Some(receipt) = swap_receipt {
                info!("FEE-WETH swap transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
                if let Some(status) = receipt.status {
                    if status.as_u64() == 0 {
                        warn!("❌ FEE-WETH swap transaction failed");
                        return Err(eyre::eyre!("FEE-WETH swap transaction failed"));
                    }
                }
            }
            info!("✅ Successfully created FEE-WETH imbalance");
        }
        
        info!("Extreme arbitrage opportunity created with 99%/98%/97% imbalances!");
        
        // Debug: Print final pool reserves to verify imbalances
        info!("=== FINAL POOL RESERVES DEBUG ===");
        for (i, pair) in self.pairs.iter().enumerate() {
            match self.get_pair_contract(pair.address) {
                Ok(pair_contract) => {
                    match pair_contract.method::<_, (U256, U256, u32)>("getReserves", ())?.call().await {
                        Ok((r0, r1, _)) => {
                            let token0 = self.tokens.iter().find(|t| t.address == pair.token0);
                            let token1 = self.tokens.iter().find(|t| t.address == pair.token1);
                            let unknown = "UNKNOWN".to_string();
                            let token0_symbol = token0.map(|t| &t.symbol).unwrap_or(&unknown);
                            let token1_symbol = token1.map(|t| &t.symbol).unwrap_or(&unknown);
                            info!("Pool {} ({}-{}): r0={} {}, r1={} {}", i, token0_symbol, token1_symbol, r0, token0_symbol, r1, token1_symbol);
                        }
                        Err(e) => {
                            warn!("Failed to get reserves for pair {}: {:?}", i, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create pair contract for {}: {:?}", i, e);
                }
            }
        }
        Ok(())
    }

    /// Helper function to directly set pool reserves (for testing)
    async fn set_pool_reserves(
        &self,
        pair: &DeployedPair,
        reserve0: U256,
        reserve1: U256,
    ) -> Result<()> {
        // This would require a custom test contract that allows setting reserves
        // For now, we'll use multiple swaps to achieve desired ratios
        info!("Setting reserves for pair {:?}: {} / {}", pair.address, reserve0, reserve1);
        
        // Get current reserves
        let pair_contract = self.get_pair_contract(pair.address)?;
        let (current_r0, current_r1, _) = pair_contract
            .method::<_, (U256, U256, u32)>("getReserves", ())?
            .call()
            .await?;
        
        info!("Current reserves: {} / {}", current_r0, current_r1);
        
        // Calculate how much to swap to achieve target reserves
        let target_ratio = reserve1.as_u128() as f64 / reserve0.as_u128() as f64;
        let current_ratio = current_r1.as_u128() as f64 / current_r0.as_u128() as f64;
        
        if (target_ratio - current_ratio).abs() > 0.01 {
            // Need to swap to adjust ratio
            let swap_amount = if target_ratio > current_ratio {
                // Need more of token1, swap token0 for token1
                U256::from((current_r0.as_u128() as f64 * 0.3) as u128)
            } else {
                // Need more of token0, swap token1 for token0
                U256::from((current_r1.as_u128() as f64 * 0.3) as u128)
            };
            
            info!("Swapping {} to adjust ratio", swap_amount);
            self.swap_tokens_for_arbitrage(0, swap_amount).await?;
        }
        
        Ok(())
    }

    /// Swap tokens to create arbitrage opportunities
    async fn swap_tokens_for_arbitrage(&self, pair_index: usize, amount: U256) -> Result<()> {
        if pair_index >= self.pairs.len() {
            return Err(eyre::eyre!("Invalid pair index"));
        }
        
        let pair = &self.pairs[pair_index];
        let router = &self.router_contract;
        
        // Approve tokens for the router
        let token0 = self.tokens.iter().find(|t| t.address == pair.token0)
            .ok_or_else(|| eyre::eyre!("Token0 not found"))?;
        let router_address = router.address();
        let method_call = token0.contract.method::<_, bool>("approve", (router_address, amount))?
            .gas(50_000u64);  // Add gas limit
        let approve_tx = method_call.send().await?;
        let approve_receipt = approve_tx.await?;
        if let Some(receipt) = approve_receipt {
            info!("Approval transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
        }
        
        // Execute swap using the router
        let deadline = U256::from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
        let amount_out_min = U256::zero();
        
        let method_call = router.method::<_, Vec<U256>>(
            "swapExactTokensForTokens",
            (
                amount,
                amount_out_min,
                vec![pair.token0, pair.token1],
                self.deployer_address,
                deadline,
            )
        )?
        .gas(500_000u64);  // Add gas limit for swap operation
        let swap_tx = method_call.send().await?;
        
        let swap_receipt = swap_tx.await?;
        if let Some(receipt) = swap_receipt {
            info!("Swap transaction: hash={:?}, status={:?}", receipt.transaction_hash, receipt.status);
            if let Some(status) = receipt.status {
                if status.as_u64() == 0 {
                    return Err(eyre::eyre!("Swap transaction failed"));
                }
            }
        }
        
        Ok(())
    }



    /// Test pathfinder can find basic paths
    pub async fn test_pathfinder_basic_path(&self) -> Result<()> {
        // First test: Can it find ANY path between two tokens?
        let token_a = self.tokens[0].address; // WETH
        let token_b = self.tokens[1].address; // STD
        let amount = U256::from(1) * U256::exp10(18);
        
        // There's a direct pool between these tokens
        info!("Testing direct path from {:?} to {:?}", token_a, token_b);
        
        let paths = self.path_finder.find_optimal_path(
            token_a,
            token_b,
            amount,
            None
        ).await?;
        
        info!("Found {} paths", if paths.is_some() { 1 } else { 0 });
        assert!(paths.is_some(), "Should find at least one direct path");
        
        // Test circular path
        info!("Testing circular arbitrage from {:?}", token_a);
        
        let circular_paths = self.path_finder.find_circular_arbitrage_paths(
            token_a,
            amount,
            None
        ).await?;
        
        info!("Found {} circular paths", circular_paths.len());
        
        // Even if not profitable, should find the path structure
        Ok(())
    }

    /// Debug circular path finding
    pub async fn test_circular_path_finding(&self) -> Result<()> {
        // Test 1: Can it find any circular path?
        let start_token = self.tokens[0].address; // WETH
        let amount = U256::from(1) * U256::exp10(18);
        
        info!("Testing circular path from {:?}", start_token);
        
        // Enable debug logging
        std::env::set_var("RUST_LOG", "path_finder=debug");
        env_logger::init();
        
        let paths = self.path_finder.find_circular_arbitrage_paths(
            start_token,
            amount,
            None
        ).await?;
        
        info!("Found {} circular paths", paths.len());
        
        // Even without profit, it should find the path structure
        // Expected paths for WETH:
        // 1. WETH -> STD -> WETH (2 hops)
        // 2. WETH -> FEE -> WETH (2 hops)
        // 3. WETH -> REV -> WETH (2 hops)
        // 4. WETH -> STD -> FEE -> WETH (3 hops)
        // etc.
        
        assert!(paths.len() >= 3, "Should find at least 3 circular paths");
        
        for (i, path) in paths.iter().enumerate() {
            info!("Path {}: {} hops, profit: {}", i, path.legs.len(), path.profit_estimate_usd);
            for (j, leg) in path.legs.iter().enumerate() {
                info!("  Step {}: {:?} -> {:?}", j, leg.from_token, leg.to_token);
            }
        }
        
        Ok(())
    }

    /// Create extreme pool imbalances for arbitrage testing
    pub async fn create_extreme_imbalances(&self) -> Result<()> {
        info!("Creating extreme pool imbalances for arbitrage testing...");
        
        // Find pools that form a triangle
        let mut triangle_pools = Vec::new();
        
        // Try to find WETH-STD pool
        for (i, pair) in self.pairs.iter().enumerate() {
            if (pair.token0 == self.tokens[0].address && pair.token1 == self.tokens[1].address) ||
               (pair.token0 == self.tokens[1].address && pair.token1 == self.tokens[0].address) {
                triangle_pools.push(i);
                break;
            }
        }
        
        if triangle_pools.is_empty() {
            warn!("No suitable pools found for extreme imbalances");
            return Ok(());
        }
        
        // Create EXTREME imbalance by swapping 98% of one token
        let pool_idx = triangle_pools[0];
        let extreme_swap = U256::from(98) * U256::exp10(18); // 98 tokens (98% of total)
        
        info!("Creating extreme imbalance in pool {} with {} token swap (98% of total)", pool_idx, extreme_swap / U256::exp10(18));
        
        // Approve tokens
        let token = &self.tokens[0]; // WETH
        token.contract.method::<_, bool>(
            "approve",
            (self.router_contract.address(), extreme_swap)
        )?.send().await?.await?;
        
        // Execute swap
        let deadline = U256::from(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() + 300);
        
        self.router_contract.method::<_, Vec<U256>>(
            "swapExactTokensForTokens",
            (
                extreme_swap,
                U256::zero(), // Accept any output
                vec![self.pairs[pool_idx].token0, self.pairs[pool_idx].token1],
                self.deployer_address,
                deadline,
            )
        )?.send().await?.await?;
        
        info!("Extreme imbalance created with 98% swap!");
        Ok(())
    }

    /// Helper to calculate cycle profit correctly
    pub fn calculate_cycle_profit(pool_reserves: &[(U256, U256)], path: Vec<usize>) -> f64 {
        let mut current_amount = 1.0; // Start with 1 unit
        
        for &pool_idx in &path {
            if pool_idx >= pool_reserves.len() {
                warn!("Invalid pool index {} in path", pool_idx);
                return 0.0;
            }
            
            let (reserve_in, reserve_out) = pool_reserves[pool_idx];
            
            // UniswapV2 formula with 0.3% fee
            let amount_in_with_fee = current_amount * 0.997;
            let numerator = amount_in_with_fee * reserve_out.as_u128() as f64;
            let denominator = reserve_in.as_u128() as f64 + amount_in_with_fee;
            
            if denominator == 0.0 {
                warn!("Zero denominator in pool {}", pool_idx);
                return 0.0;
            }
            
            current_amount = numerator / denominator;
            info!("Pool {}: rate = {:.4}", pool_idx, current_amount);
        }
        
        info!("Total rate product: {:.4}", current_amount);
        info!("Calculated cycle profit: {:.2}%", (current_amount - 1.0) * 100.0);
        
        current_amount
    }

    /// Get all pool reserves for manual calculation
    pub async fn get_all_pool_reserves(&self) -> Result<Vec<(U256, U256)>> {
        let mut reserves = Vec::new();
        
        for pair in &self.pairs {
            match self.get_pair_contract(pair.address) {
                Ok(pair_contract) => {
                    match pair_contract.method::<_, (U256, U256, u32)>("getReserves", ())?.call().await {
                        Ok((r0, r1, _)) => {
                            reserves.push((r0, r1));
                            info!("Pool {:?}: reserves {} / {}", pair.address, r0, r1);
                        }
                        Err(e) => {
                            warn!("Failed to get reserves for pair {:?}: {:?}", pair.address, e);
                            reserves.push((U256::zero(), U256::zero()));
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create pair contract for {:?}: {:?}", pair.address, e);
                    reserves.push((U256::zero(), U256::zero()));
                }
            }
        }
        
        Ok(reserves)
    }

    pub async fn execute_transaction<T: Tokenize + Clone, R: Detokenize>(
        &self,
        contract: &ContractWrapper,
        method_name: &str,
        args: T,
        gas_limit: Option<u64>,
    ) -> Result<R> {
        let gas = gas_limit.unwrap_or(DEFAULT_GAS_LIMIT);
        let call = contract.method::<_, R>(method_name, args.clone())?
            .gas(gas)
            .gas_price(DEFAULT_GAS_PRICE);
        let tx = call.send().await?;
        let receipt = tx.await?.ok_or_else(|| eyre::eyre!("Transaction failed: no receipt"))?;
        if receipt.status != Some(1.into()) {
            return Err(eyre::eyre!("Transaction reverted: method={}", method_name));
        }
        let result = contract.method::<_, R>(method_name, args)?
            .call()
            .await?;
        Ok(result)
    }

    pub async fn advance_blocks(&self, num_blocks: u64) -> Result<()> {
        for _ in 0..num_blocks {
            self.block_builder.build_block(&self.deployer_client.provider()).await?;
        }
        Ok(())
    }

    /// Get router address for a specific protocol type
    pub fn get_router_address(&self, _protocol_type: ProtocolType) -> Address {
        self.router_contract.address()
    }

    /// Get WETH address
    pub fn get_weth_address(&self) -> Address {
        self.weth_address
    }

    /// Get token by symbol
    pub fn get_token_by_symbol(&self, symbol: &str) -> Option<Address> {
        self.tokens.iter().find(|token| token.symbol == symbol).map(|token| token.address)
    }

    /// Get token balance for an address
    pub fn get_token_balance(&self, _token_address: Address, _owner: Address) -> Result<U256> {
        // Return a mock balance for testing
        Ok(U256::from(1000000000000000000u128)) // 1 ETH equivalent
    }

    /// Ensure token balance by minting if necessary
    pub fn ensure_token_balance(&self, _token_address: Address, _owner: Address, _amount: U256) -> Result<()> {
        // In test environment, assume balance is always sufficient
        Ok(())
    }

    /// Convert wei amount to USD
    pub fn convert_wei_to_usd(&self, _wei_amount: U256) -> Result<f64> {
        // Mock conversion: assume 1 ETH = $2000
        let eth_amount = _wei_amount.as_u128() as f64 / 1e18;
        Ok(eth_amount * 2000.0)
    }

    /// Decode swap events from transaction logs using ABI configuration
    pub fn decode_swap_events(&self, logs: &[ethers::types::Log]) -> Result<Vec<SwapEvent>> {
        let mut events = Vec::new();

        // Load ABI configuration
        let abi_config: serde_json::Value = serde_json::from_str(include_str!("../../config/abi_parse.json"))?;
        let dexes = abi_config["dexes"].as_object()
            .ok_or_else(|| eyre::eyre!("Invalid ABI config: missing dexes"))?;

        for log in logs {
            if log.topics.is_empty() {
                continue;
            }

            let topic0 = format!("0x{}", hex::encode(log.topics[0].as_bytes()));

            // Find matching DEX configuration
            for (dex_name, dex_config) in dexes {
                let topic0_map = dex_config["topic0"].as_object();
                let event_signatures = dex_config["event_signatures"].as_object();
                let field_map = dex_config["swap_event_field_map"].as_object();

                if let (Some(topics), Some(signatures), Some(fields)) = (topic0_map, event_signatures, field_map) {
                    // Check if this log matches any event signature
                    for (event_name, signature) in signatures {
                        if let Some(expected_topic) = topics.get(event_name) {
                            if let (Some(expected_topic_str), Some(signature_str)) =
                                (expected_topic.as_str(), signature.as_str()) {

                                if topic0 == expected_topic_str {
                                    // This is a matching event, decode it
                                    if let Ok(decoded_event) = self.decode_swap_event(
                                        log,
                                        dex_name,
                                        event_name,
                                        signature_str,
                                        fields
                                    ) {
                                        events.push(decoded_event);
                                    }
                                    break; // Found matching event, no need to check others
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    /// Decode a single swap event
    fn decode_swap_event(
        &self,
        log: &ethers::types::Log,
        dex_name: &str,
        _event_name: &str,
        signature: &str,
        _field_map: &serde_json::Map<String, serde_json::Value>
    ) -> Result<SwapEvent> {

        // Parse event signature to get parameter types
        let param_types = self.parse_event_signature(signature)?;

        // Decode the log data
        let decoded = ethers::abi::decode(&param_types, &log.data)?;

        // Extract indexed parameters from topics (skip topic[0] which is event signature)
        let mut indexed_params = Vec::new();
        for i in 1..log.topics.len() {
            indexed_params.push(log.topics[i]);
        }

        // Map decoded data to SwapEvent fields using field_map
        let mut event_data = rust::types::SwapData::UniswapV2 {
            sender: Address::zero(),
            to: Address::zero(),
            amount0_in: U256::zero(),
            amount1_in: U256::zero(),
            amount0_out: U256::zero(),
            amount1_out: U256::zero(),
        };

        // Map fields using the event signature and field mapping
        // The decoded array contains non-indexed parameters in order
        // The indexed parameters are in the topics

        // For UniswapV2 Swap event: Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
        // indexed[0] = sender (topic 1)
        // param[0] = amount0In
        // param[1] = amount1In
        // param[2] = amount0Out
        // param[3] = amount1Out
        // indexed[1] = to (topic 2)

        // Extract indexed parameters first
        if indexed_params.len() >= 2 {
            // indexed[0] = sender
            let topic_bytes = indexed_params[0].as_bytes();
            let address_bytes = &topic_bytes[12..32];
            let address = Address::from_slice(address_bytes);
            let token = Token::Address(address);
            self.map_token_to_swap_data(&mut event_data, "sender", &token);

            // indexed[1] = to/recipient
            let topic_bytes = indexed_params[1].as_bytes();
            let address_bytes = &topic_bytes[12..32];
            let address = Address::from_slice(address_bytes);
            let token = Token::Address(address);
            self.map_token_to_swap_data(&mut event_data, "to", &token);
        }

        // Extract non-indexed parameters
        if decoded.len() >= 4 {
            // Map the decoded parameters to the correct field names
            let param_mappings = [
                ("amount0In", 0),
                ("amount1In", 1),
                ("amount0Out", 2),
                ("amount1Out", 3),
            ];

            for (field_name, param_index) in param_mappings {
                if let Some(token) = decoded.get(param_index) {
                    self.map_token_to_swap_data(&mut event_data, field_name, token);
                }
            }
        }

        Ok(SwapEvent {
            chain_name: "test".to_string(),
            tx_hash: log.transaction_hash.unwrap_or_default(),
            block_number: log.block_number.unwrap_or_default().as_u64(),
            log_index: U256::from(log.log_index.unwrap_or_default()),
            unix_ts: 0, // Default timestamp
            token_in: Address::zero(), // Will be set based on swap direction
            token_out: Address::zero(), // Will be set based on swap direction
            amount_in: U256::zero(),
            amount_out: U256::zero(),
            sender: Address::zero(),
            recipient: Address::zero(),
            gas_used: None,
            effective_gas_price: None,
            protocol: match dex_name.as_ref() {
                "UniswapV2" | "SushiSwap" => rust::types::DexProtocol::UniswapV2,
                "UniswapV3" => rust::types::DexProtocol::UniswapV3,
                "Curve" => rust::types::DexProtocol::Curve,
                "Balancer" => rust::types::DexProtocol::Balancer,
                _ => rust::types::DexProtocol::Other(0),
            },
            data: event_data,
            pool_address: log.address,
            token0: None,
            token1: None,
            price_t0_in_t1: None,
            price_t1_in_t0: None,
            reserve0: None,
            reserve1: None,
            transaction: None,
            transaction_metadata: None,
            pool_id: None,
            buyer: None,
            sold_id: None,
            tokens_sold: None,
            bought_id: None,
            tokens_bought: None,
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            token0_decimals: Some(18),
            token1_decimals: Some(18),
            fee_tier: None,
            protocol_type: Some(match dex_name.as_ref() {
                "UniswapV2" | "SushiSwap" => ProtocolType::UniswapV2,
                "UniswapV3" => ProtocolType::UniswapV3,
                "Curve" => ProtocolType::Curve,
                "Balancer" => ProtocolType::Balancer,
                _ => ProtocolType::Other("unknown".to_string()),
            }),
            v3_amount0: None,
            v3_amount1: None,
            v3_tick_data: None,
            price_from_in_usd: None,
            price_to_in_usd: None,
            token0_price_usd: None,
            token1_price_usd: None,
            pool_liquidity_usd: None,
        })
    }

    /// Parse event signature to extract parameter types
    fn parse_event_signature(&self, signature: &str) -> Result<Vec<ParamType>> {
        // Simple parser for event signatures like:
        // "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"

        let mut param_types = Vec::new();

        // Extract parameters from signature
        if let Some(params_start) = signature.find('(') {
            if let Some(params_end) = signature.find(')') {
                let params_str = &signature[params_start + 1..params_end];

                for param in params_str.split(',') {
                    let parts: Vec<&str> = param.trim().split_whitespace().collect();
                    if parts.len() >= 2 {
                        let param_type = match parts[0] {
                            "address" => ParamType::Address,
                            "uint" | "uint256" => ParamType::Uint(256),
                            "uint112" => ParamType::Uint(112),
                            "uint32" => ParamType::Uint(32),
                            "uint24" => ParamType::Uint(24),
                            "uint16" => ParamType::Uint(16),
                            "uint8" => ParamType::Uint(8),
                            "int256" => ParamType::Int(256),
                            "int24" => ParamType::Int(24),
                            "int128" => ParamType::Int(128),
                            "bool" => ParamType::Bool,
                            "bytes32" => ParamType::FixedBytes(32),
                            "bytes" => ParamType::Bytes,
                            _ => continue, // Skip unknown types
                        };
                        param_types.push(param_type);
                    }
                }
            }
        }

        Ok(param_types)
    }

    /// Map decoded token to SwapData fields
    fn map_token_to_swap_data(&self, event_data: &mut rust::types::SwapData, field_key: &str, token: &Token) {
        match (field_key, token) {
            ("sender", Token::Address(addr)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut sender, .. } = event_data {
                    *sender = *addr;
                }
            }
            ("to" | "recipient", Token::Address(addr)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut to, .. } = event_data {
                    *to = *addr;
                }
            }
            ("amount0In", Token::Uint(value)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut amount0_in, .. } = event_data {
                    *amount0_in = *value;
                }
            }
            ("amount1In", Token::Uint(value)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut amount1_in, .. } = event_data {
                    *amount1_in = *value;
                }
            }
            ("amount0Out", Token::Uint(value)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut amount0_out, .. } = event_data {
                    *amount0_out = *value;
                }
            }
            ("amount1Out", Token::Uint(value)) => {
                if let rust::types::SwapData::UniswapV2 { ref mut amount1_out, .. } = event_data {
                    *amount1_out = *value;
                }
            }
            _ => {} // Ignore unmapped fields
        }
    }

    /// Format token amount with appropriate decimals
    pub fn format_token_amount(&self, _token_address: Address, _amount: U256) -> Result<String> {
        // Mock formatting
        Ok(format!("{} tokens", _amount))
    }

    /// Get current pool reserves
    pub fn get_current_pool_reserves(&self, _pool_address: Address) -> Result<(U256, U256)> {
        // Return mock reserves
        Ok((U256::from(1000000000000000000000u128), U256::from(2000000000000u128))) // 1000 ETH, 2M USDC
    }

    /// Convert token amount to USD
    pub fn convert_token_to_usd(&self, _token_address: Address, _amount: U256) -> Result<f64> {
        // Mock conversion: assume token = $1
        let token_amount = _amount.as_u128() as f64 / 1e18;
        Ok(token_amount * 1.0)
    }
}

#[derive(Debug, Clone)]
struct MockPredictiveEngine;

#[derive(Debug)]
struct MockSynergyScorer;

#[async_trait::async_trait]
impl SynergyScorer for MockSynergyScorer {
    async fn score(
        &self,
        source_chain: &str,
        dest_chain: &str,
        token: Address,
    ) -> Result<f64, CrossChainError> {
        let source_score: f64 = if source_chain == "ethereum" { 0.7 } else { 0.5 };
        let dest_score: f64 = if dest_chain == "polygon" { 0.6 } else { 0.4 };
        let token_score: f64 = if token == Address::zero() { 0.2 } else { 0.8 };
        let synergy: f64 = (source_score + dest_score + token_score) / 3.0;
        Ok(synergy.max(0.0).min(1.0))
    }
}

#[async_trait::async_trait]
impl PredictiveEngine for MockPredictiveEngine {
    async fn predict<'a>(
        &'a self,
        tx_context: &'a TransactionContext,
        _block_number: Option<u64>,
    ) -> Result<Option<MEVPrediction>, PredictiveEngineError> {
        use ethers::types::H256;
        use rust::types::{PredictionFeatures, CompetitionLevel, MEVType};
        let profit_estimate = if tx_context.market_conditions.gas_price_gwei > 100.0 {
            0.0
        } else {
            100.0
        };
        let mev_type = if tx_context.tx.to == Some(Address::zero()) {
            MEVType::NoMEV
        } else {
            MEVType::Backrun
        };
        let prediction = MEVPrediction {
            prediction_id: H256::zero(),
            transaction_hash: tx_context.tx.hash,
            predicted_profit_usd: profit_estimate,
            confidence_score: 0.85,
            risk_score: 0.5,
            mev_type,
            estimated_success_rate: 0.9,
            features: PredictionFeatures::default(),
            competition_level: CompetitionLevel::Medium,
        };
        if profit_estimate > 0.0 {
            Ok(Some(prediction))
        } else {
            Ok(None)
        }
    }

    async fn update_model<'a>(
        &'a self,
        result: &'a ExecutionResult,
    ) -> Result<(), PredictiveEngineError> {
        if result.success {
            Ok(())
        } else {
            Err(PredictiveEngineError::ModelUpdate("Execution failed".to_string()))
        }
    }

    async fn get_performance_metrics<'a>(&'a self) -> Result<ModelPerformance, PredictiveEngineError> {
        Ok(ModelPerformance {
            accuracy: 0.92,
            precision: 0.89,
            recall: 0.91,
            f1_score: 0.90,
            total_predictions: 1000,
            successful_predictions: 920,
            true_positives: 800,
            false_positives: 50,
            false_negatives: 30,
            cumulative_actual_profit_usd: 100000.0,
            cumulative_predicted_profit_usd: 95000.0,
        })
    }
}

// Generic contract wrapper to handle both generated and manual contract types
#[derive(Clone, Debug)]
pub enum ContractWrapper {
    Manual(Contract<SignerMiddleware<Provider<Http>, LocalWallet>>),
}

impl ContractWrapper {
    pub fn address(&self) -> Address {
        match self {
            ContractWrapper::Manual(contract) => contract.address(),
        }
    }

    pub fn method<T: Tokenize, R: Detokenize>(&self, method_name: &str, args: T) -> Result<ContractCall<SignerMiddleware<Provider<Http>, LocalWallet>, R>> {
        match self {
            ContractWrapper::Manual(contract) => Ok(contract.method(method_name, args)?),
        }
    }
} 

// Constants for gas configuration
const DEFAULT_GAS_LIMIT: u64 = 3_000_000;
const DEPLOYMENT_GAS_LIMIT: u64 = 8_000_000;
const APPROVAL_GAS_LIMIT: u64 = 100_000;
const SWAP_GAS_LIMIT: u64 = 500_000;
const DEFAULT_GAS_PRICE: u64 = 20_000_000_000; // 20 gwei

pub async fn create_pairs(
    tokens: &[DeployedToken],
    factory_contract: &ContractWrapper,
    router_contract: &ContractWrapper,
    deployer_client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
) -> Result<Vec<DeployedPair>> {
    let mut pairs = Vec::new();
    let deployer_address = deployer_client.address();
    let approve_amount = U256::MAX;

    info!("Starting pair creation for {} tokens", tokens.len());

    // First, approve router for all tokens
    for token in tokens {
        debug!("Approving router for token {}", token.symbol);

        // Check current allowance first
        let current_allowance = token.contract.method::<_, U256>("allowance", (deployer_address, router_contract.address()))?
            .call()
            .await?;
        debug!("Current allowance for {}: {}", token.symbol, current_allowance);

        if current_allowance < approve_amount {
            let approve_call = token.contract.method::<_, bool>(
                "approve",
                (router_contract.address(), approve_amount)
            )?
                .gas(APPROVAL_GAS_LIMIT)
                .gas_price(DEFAULT_GAS_PRICE);
            let tx = approve_call.send().await?;
            let receipt = tx.await?.ok_or_else(|| eyre::eyre!("Approval transaction failed"))?;
            if receipt.status != Some(1.into()) {
                return Err(eyre::eyre!("Approval transaction reverted for token {}", token.symbol));
            }
            debug!("Approved router for token {}", token.symbol);
        } else {
            debug!("Router already approved for token {}", token.symbol);
        }
    }

    info!("Approved router for all tokens");

    // Check factory contract state
    let factory_fee_to = factory_contract.method::<_, Address>("feeTo", ())?
        .call()
        .await?;
    debug!("Factory feeTo: {:?}", factory_fee_to);

    let factory_fee_to_setter = factory_contract.method::<_, Address>("feeToSetter", ())?
        .call()
        .await?;
    debug!("Factory feeToSetter: {:?}", factory_fee_to_setter);

    // Create pairs for all token combinations
    for i in 0..tokens.len() {
        for j in (i + 1)..tokens.len() {
            let token0 = &tokens[i];
            let token1 = &tokens[j];

            debug!("Creating pair for {}/{}", token0.symbol, token1.symbol);
            debug!("Token0 address: {:?}", token0.address);
            debug!("Token1 address: {:?}", token1.address);

            // Check if pair already exists
            let existing_pair = factory_contract.method::<_, Address>(
                "getPair",
                (token0.address, token1.address)
            )?
                .call()
                .await?;

            if existing_pair != Address::zero() {
                debug!("Pair already exists for {}/{} at {:?}", token0.symbol, token1.symbol, existing_pair);
                pairs.push(DeployedPair {
                    address: existing_pair,
                    token0: token0.address,
                    token1: token1.address,
                });
                continue;
            }

            // Check token balances before creating pair
            let token0_balance = token0.contract.method::<_, U256>("balanceOf", deployer_address)?
                .call()
                .await?;
            let token1_balance = token1.contract.method::<_, U256>("balanceOf", deployer_address)?
                .call()
                .await?;
            debug!("Token {} balance: {}", token0.symbol, token0_balance);
            debug!("Token {} balance: {}", token1.symbol, token1_balance);

            if token0_balance == U256::zero() || token1_balance == U256::zero() {
                return Err(eyre::eyre!("Insufficient token balance for pair creation: {}={}, {}={}",
                    token0.symbol, token0_balance, token1.symbol, token1_balance));
            }

            // Create the pair with more detailed error handling
            debug!("Calling createPair on factory contract...");
            let create_pair_call = factory_contract.method::<_, Address>(
                "createPair",
                (token0.address, token1.address)
            )?
                .gas(1_000_000u64)  // Higher gas limit for pair creation
                .gas_price(DEFAULT_GAS_PRICE);

            let tx = create_pair_call.send().await?;
            let receipt = tx.await?.ok_or_else(|| eyre::eyre!("Create pair transaction failed"))?;

            if receipt.status != Some(1.into()) {
                // Try to get more detailed error information
                let tx_hash = receipt.transaction_hash;
                debug!("Create pair transaction failed with hash: {:?}", tx_hash);

                // Try to get the transaction details
                let tx_details = deployer_client.get_transaction(tx_hash).await?;
                if let Some(tx) = tx_details {
                    debug!("Transaction details: {:?}", tx);
                }

                return Err(eyre::eyre!("Create pair transaction reverted for {}/{}", token0.symbol, token1.symbol));
            }

            // Get the created pair address
            let pair_address = factory_contract.method::<_, Address>(
                "getPair",
                (token0.address, token1.address)
            )?
                .call()
                .await?;

            if pair_address == Address::zero() {
                return Err(eyre::eyre!("Pair address is zero after creation for {}/{}", token0.symbol, token1.symbol));
            }

            debug!("Created pair for {}/{} at {:?}", token0.symbol, token1.symbol, pair_address);

            // Add initial liquidity to the pair
            let amount = U256::from(100) * U256::exp10(18); // 100 tokens each (smaller amount)
            let deadline = U256::from(u64::MAX);

            debug!("Adding liquidity to pair {}/{}", token0.symbol, token1.symbol);
            debug!("Amount0: {}, Amount1: {}", amount, amount);

            // First check if we have enough balance
            let token0_balance_for_liquidity = token0.contract.method::<_, U256>("balanceOf", deployer_address)?
                .call()
                .await?;
            let token1_balance_for_liquidity = token1.contract.method::<_, U256>("balanceOf", deployer_address)?
                .call()
                .await?;

            debug!("Balance for liquidity - {}: {}, {}: {}", token0.symbol, token0_balance_for_liquidity, token1.symbol, token1_balance_for_liquidity);

            if token0_balance_for_liquidity < amount || token1_balance_for_liquidity < amount {
                debug!("Insufficient balance for liquidity, skipping liquidity addition");
                info!("Created pair for {}/{} at {:?} (without liquidity)", token0.symbol, token1.symbol, pair_address);
                pairs.push(DeployedPair {
                    address: pair_address,
                    token0: token0.address,
                    token1: token1.address,
                });
                continue;
            }

            // Try to add liquidity with better error handling
            let add_liquidity_call = router_contract.method::<_, (U256, U256, U256)>(
                "addLiquidity",
                (
                    token0.address,
                    token1.address,
                    amount,
                    amount,
                    U256::one(),
                    U256::one(),
                    deployer_address,
                    deadline,
                ),
            )?
                .gas(1_000_000u64)  // Higher gas limit for liquidity addition
                .gas_price(DEFAULT_GAS_PRICE);

            let tx = add_liquidity_call.send().await?;
            let receipt = tx.await?.ok_or_else(|| eyre::eyre!("Add liquidity transaction failed"))?;

            if receipt.status != Some(1.into()) {
                debug!("Add liquidity transaction reverted, but pair was created successfully");
                info!("Created pair for {}/{} at {:?} (liquidity addition failed)", token0.symbol, token1.symbol, pair_address);
                pairs.push(DeployedPair {
                    address: pair_address,
                    token0: token0.address,
                    token1: token1.address,
                });
                continue;
            }

            info!("Created and funded pair for {}/{} at {:?}", token0.symbol, token1.symbol, pair_address);
            pairs.push(DeployedPair {
                address: pair_address,
                token0: token0.address,
                token1: token1.address,
            });
        }
    }

    Ok(pairs)
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        if self.anvil.take().is_some() {
            info!("Shutting down Anvil instance...");
            info!("Anvil instance shut down.");
        }
    }
}

/// Helper to configure a real RPC environment for testing
async fn configure_real_rpc_env(chain_name: &str) -> Result<(String, String, u64)> {
    // Implement the logic to configure a real RPC environment for testing
    // This might involve setting up a local blockchain instance, configuring a provider, etc.
    // For simplicity, we'll use a placeholder implementation
    let rpc_url = format!("http://{}.rpc.example.com", chain_name);
    let ws_url = format!("ws://{}.ws.example.com", chain_name);
    let chain_id = 1; // Replace with actual chain ID
    Ok((rpc_url, ws_url, chain_id))
}