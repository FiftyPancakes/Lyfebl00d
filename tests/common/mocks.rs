use std::sync::{Arc, Mutex, RwLock as StdRwLock};
use std::collections::HashMap;
use std::str::FromStr;
use ethers::{
    providers::{Http, Provider, Ws},
    signers::LocalWallet,
    types::{U256, Bytes, TransactionRequest, H256, TransactionReceipt, Block, BlockId, Log, Filter, Transaction, Address}
};
use tokio::sync::RwLock;

use rust::{
    blockchain::BlockchainManager,
    gas_oracle::{GasOracleProvider, GasPrice},
    price_oracle::PriceOracle,
    transaction_optimizer::TransactionOptimizerTrait,
    mempool_stats::{MempoolStatsProvider, MempoolStats},
    token_registry::CoinRow,
    mempool::{MempoolMonitor, MempoolMetrics},
    errors::{BlockchainError, OptimizerError, MempoolError, GasError, PriceError},
    types::{ProtocolType, FeeData},
    config::ExecutionMode,
    predict::PredictiveEngine,
};

use super::{DeployedPair, DeployedToken};
use async_trait::async_trait;
use rust::{
    errors::DexError,
    pool_manager::{PoolManagerTrait, PoolStaticMetadata},
    types::{PoolInfo},
};

// === Mock Gas Oracle ===
#[derive(Debug)]
pub struct MockGasOracle {
    pub base_fee: Arc<StdRwLock<U256>>,
    pub priority_fee: Arc<StdRwLock<U256>>,
    pub gas_price: Arc<StdRwLock<U256>>,
    pub max_fee_per_gas: Arc<StdRwLock<U256>>,
    pub max_priority_fee_per_gas: Arc<StdRwLock<U256>>,
    pub gas_limit: Arc<StdRwLock<U256>>,
    pub eth_price_usd: Arc<StdRwLock<f64>>,
    pub zero_gas_mode: Arc<StdRwLock<bool>>,
}

impl MockGasOracle {
    pub fn new(
        base_fee: U256,
        priority_fee: U256,
        gas_price: U256,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
        gas_limit: U256,
        eth_price_usd: f64,
    ) -> Self {
        Self {
            base_fee: Arc::new(StdRwLock::new(base_fee)),
            priority_fee: Arc::new(StdRwLock::new(priority_fee)),
            gas_price: Arc::new(StdRwLock::new(gas_price)),
            max_fee_per_gas: Arc::new(StdRwLock::new(max_fee_per_gas)),
            max_priority_fee_per_gas: Arc::new(StdRwLock::new(max_priority_fee_per_gas)),
            gas_limit: Arc::new(StdRwLock::new(gas_limit)),
            eth_price_usd: Arc::new(StdRwLock::new(eth_price_usd)),
            zero_gas_mode: Arc::new(StdRwLock::new(false)),
        }
    }

    pub fn new_zero_gas() -> Self {
        Self {
            base_fee: Arc::new(StdRwLock::new(U256::zero())),
            priority_fee: Arc::new(StdRwLock::new(U256::zero())),
            gas_price: Arc::new(StdRwLock::new(U256::zero())),
            max_fee_per_gas: Arc::new(StdRwLock::new(U256::zero())),
            max_priority_fee_per_gas: Arc::new(StdRwLock::new(U256::zero())),
            gas_limit: Arc::new(StdRwLock::new(U256::from(21000))),
            eth_price_usd: Arc::new(StdRwLock::new(2000.0)),
            zero_gas_mode: Arc::new(StdRwLock::new(true)),
        }
    }

    pub fn set_base_fee(&self, value: U256) {
        *self.base_fee.write().unwrap() = value;
    }
    pub fn set_priority_fee(&self, value: U256) {
        *self.priority_fee.write().unwrap() = value;
    }
    pub fn set_gas_price(&self, value: U256) {
        *self.gas_price.write().unwrap() = value;
    }
    pub fn set_max_fee_per_gas(&self, value: U256) {
        *self.max_fee_per_gas.write().unwrap() = value;
    }
    pub fn set_max_priority_fee_per_gas(&self, value: U256) {
        *self.max_priority_fee_per_gas.write().unwrap() = value;
    }
    pub fn set_gas_limit(&self, value: U256) {
        *self.gas_limit.write().unwrap() = value;
    }
    pub fn set_eth_price_usd(&self, value: f64) {
        *self.eth_price_usd.write().unwrap() = value;
    }
    
    pub fn set_zero_gas_mode(&self, enabled: bool) {
        *self.zero_gas_mode.write().unwrap() = enabled;
    }
}

#[async_trait::async_trait]
impl GasOracleProvider for MockGasOracle {
    async fn get_gas_price(
        &self,
        _chain_name: &str,
        _block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        if *self.zero_gas_mode.read().unwrap() {
            Ok(GasPrice {
                base_fee: U256::zero(),
                priority_fee: U256::zero(),
            })
        } else {
            Ok(GasPrice {
                base_fee: *self.base_fee.read().unwrap(),
                priority_fee: *self.priority_fee.read().unwrap(),
            })
        }
    }

    async fn get_fee_data(
        &self,
        _chain_name: &str,
        _block_number: Option<u64>,
    ) -> Result<FeeData, GasError> {
        Ok(FeeData {
            base_fee_per_gas: Some(*self.base_fee.read().unwrap()),
            max_fee_per_gas: Some(*self.max_fee_per_gas.read().unwrap()),
            max_priority_fee_per_gas: Some(*self.max_priority_fee_per_gas.read().unwrap()),
            gas_price: Some(*self.gas_price.read().unwrap()),
            block_number: 0,
            chain_name: "ethereum".to_string(),
        })
    }

    fn get_chain_name_from_id(&self, _chain_id: u64) -> Option<String> {
        Some("ethereum".to_string())
    }

    fn estimate_protocol_gas(&self, _protocol: &ProtocolType) -> u64 {
        21000
    }

    async fn get_gas_price_with_block(
        &self,
        chain_name: &str,
        block_number: Option<u64>,
    ) -> Result<GasPrice, GasError> {
        self.get_gas_price(chain_name, block_number).await
    }

    async fn estimate_gas_cost_native(
        &self,
        _chain_name: &str,
        gas_units: u64,
    ) -> Result<f64, GasError> {
        let gas_price = self.get_gas_price("ethereum", None).await?;
        let effective_price = gas_price.effective_price();
        let cost_wei = effective_price.saturating_mul(U256::from(gas_units));
        let cost_eth = cost_wei.as_u128() as f64 / 1e18;
        Ok(cost_eth * *self.eth_price_usd.read().unwrap())
    }
}

// === Mock Price Oracle ===
#[derive(Debug)]
pub struct MockPriceOracle {
    prices: Arc<Mutex<HashMap<Address, f64>>>,
    default_price: Arc<Mutex<f64>>,
    pair_ratios: Arc<Mutex<HashMap<(Address, Address), f64>>>,
    token_volatility: Arc<Mutex<HashMap<Address, f64>>>,
    default_volatility: Arc<Mutex<f64>>,
    pool_liquidity: Arc<Mutex<HashMap<Address, f64>>>,
    default_liquidity: Arc<Mutex<f64>>,
}

impl MockPriceOracle {
    pub fn new(default_price: f64) -> Self {
        Self {
            prices: Arc::new(Mutex::new(HashMap::new())),
            default_price: Arc::new(Mutex::new(default_price)),
            pair_ratios: Arc::new(Mutex::new(HashMap::new())),
            token_volatility: Arc::new(Mutex::new(HashMap::new())),
            default_volatility: Arc::new(Mutex::new(0.1)),
            pool_liquidity: Arc::new(Mutex::new(HashMap::new())),
            default_liquidity: Arc::new(Mutex::new(1000000.0)),
        }
    }

    pub fn set_price(&self, token: Address, price: f64) {
        self.prices.lock().unwrap().insert(token, price);
    }

    pub fn set_default_price(&self, price: f64) {
        *self.default_price.lock().unwrap() = price;
    }

    pub fn set_pair_ratio(&self, token_a: Address, token_b: Address, ratio: f64) {
        self.pair_ratios.lock().unwrap().insert((token_a, token_b), ratio);
    }

    pub fn set_token_volatility(&self, token: Address, volatility: f64) {
        self.token_volatility.lock().unwrap().insert(token, volatility);
    }

    pub fn set_default_volatility(&self, volatility: f64) {
        *self.default_volatility.lock().unwrap() = volatility;
    }

    pub fn set_pool_liquidity(&self, pool: rust::types::PoolInfo, liquidity: f64) {
        self.pool_liquidity.lock().unwrap().insert(pool.address, liquidity);
    }

    pub fn set_default_liquidity(&self, liquidity: f64) {
        *self.default_liquidity.lock().unwrap() = liquidity;
    }
}

#[async_trait::async_trait]
impl PriceOracle for MockPriceOracle {
    async fn get_token_price_usd(&self, _chain_name: &str, token: Address) -> Result<f64, PriceError> {
        Ok(self.prices.lock().unwrap().get(&token).copied().unwrap_or(*self.default_price.lock().unwrap()))
    }

    async fn get_token_price_usd_at(&self, chain_name: &str, token: Address, _block_number: Option<u64>, _unix_ts: Option<u64>) -> Result<f64, PriceError> {
        self.get_token_price_usd(chain_name, token).await
    }

    async fn get_pair_price_ratio(&self, _chain_name: &str, token_a: Address, token_b: Address) -> Result<f64, PriceError> {
        Ok(self.pair_ratios.lock().unwrap().get(&(token_a, token_b)).copied().unwrap_or(1.0))
    }

    async fn get_historical_prices_batch(&self, _chain_name: &str, tokens: &[Address], _block_number: u64) -> Result<HashMap<Address, f64>, PriceError> {
        let mut result = HashMap::new();
        let default_price = *self.default_price.lock().unwrap();
        for token in tokens {
            result.insert(*token, self.prices.lock().unwrap().get(token).copied().unwrap_or(default_price));
        }
        Ok(result)
    }

    async fn get_token_volatility(&self, _chain_name: &str, token: Address, _timeframe_seconds: u64) -> Result<f64, PriceError> {
        Ok(self.token_volatility.lock().unwrap().get(&token).copied().unwrap_or(*self.default_volatility.lock().unwrap()))
    }

    async fn calculate_liquidity_usd(&self, pool_info: &rust::types::PoolInfo) -> Result<f64, PriceError> {
        Ok(self.pool_liquidity.lock().unwrap().get(&pool_info.address).copied().unwrap_or(*self.default_liquidity.lock().unwrap()))
    }


    async fn get_pair_price_at_block(&self, chain_name: &str, token_a: Address, token_b: Address, _block_number: u64) -> Result<f64, PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }

    async fn get_pair_price(&self, chain_name: &str, token_a: Address, token_b: Address) -> Result<f64, PriceError> {
        self.get_pair_price_ratio(chain_name, token_a, token_b).await
    }

    async fn convert_token_to_usd(&self, chain_name: &str, amount: U256, token: Address) -> Result<f64, PriceError> {
        let price = self.get_token_price_usd(chain_name, token).await?;
        let amount_f64 = amount.as_u128() as f64 / 1e18;
        Ok(amount_f64 * price)
    }

    async fn convert_native_to_usd(&self, _chain_name: &str, amount_native: U256) -> Result<f64, PriceError> {
        let amount_f64 = amount_native.as_u128() as f64 / 1e18;
        Ok(amount_f64 * 2000.0) // Assume ETH price of $2000
    }

    async fn estimate_token_conversion(&self, _chain_name: &str, amount_in: U256, _token_in: Address, _token_out: Address) -> Result<U256, PriceError> {
        Ok(amount_in) // Simple 1:1 conversion for mock
    }

    async fn estimate_swap_value_usd(&self, chain_name: &str, token_in: Address, _token_out: Address, amount_in: U256) -> Result<f64, PriceError> {
        self.convert_token_to_usd(chain_name, amount_in, token_in).await
    }
}

// === Mock Transaction Optimizer ===
#[derive(Debug)]
pub struct MockTransactionOptimizer {
    pub should_revert: Arc<StdRwLock<bool>>,
    pub simulate_gas: Arc<StdRwLock<U256>>,
    pub simulate_cost: Arc<StdRwLock<f64>>,
    pub wallet_address: Arc<StdRwLock<Address>>,
}

impl MockTransactionOptimizer {
    pub fn new() -> Self {
        Self {
            should_revert: Arc::new(StdRwLock::new(false)),
            simulate_gas: Arc::new(StdRwLock::new(U256::from(21000))),
            simulate_cost: Arc::new(StdRwLock::new(0.01)),
            wallet_address: Arc::new(StdRwLock::new(Address::zero())),
        }
    }

    pub fn set_should_revert(&self, value: bool) {
        *self.should_revert.write().unwrap() = value;
    }

    pub fn set_simulate_gas(&self, value: U256) {
        *self.simulate_gas.write().unwrap() = value;
    }

    pub fn set_simulate_cost(&self, value: f64) {
        *self.simulate_cost.write().unwrap() = value;
    }

    pub fn set_wallet_address(&self, value: Address) {
        *self.wallet_address.write().unwrap() = value;
    }
}

#[async_trait::async_trait]
impl TransactionOptimizerTrait for MockTransactionOptimizer {
    async fn auto_optimize_and_submit(&self, _tx: TransactionRequest, _use_flashbots_hint: Option<bool>) -> Result<H256, OptimizerError> {
        if *self.should_revert.read().unwrap() {
            Err(OptimizerError::Timeout("Mock revert".to_string()))
        } else {
            Ok(H256::zero())
        }
    }

    async fn submit_transaction(&self, _tx: ethers::types::transaction::eip2718::TypedTransaction, _submission_strategy: rust::transaction_optimizer::SubmissionStrategy) -> Result<H256, OptimizerError> {
        if *self.should_revert.read().unwrap() {
            Err(OptimizerError::Timeout("Mock revert".to_string()))
        } else {
            Ok(H256::zero())
        }
    }

    async fn submit_bundle(&self, _txs: Vec<ethers::types::transaction::eip2718::TypedTransaction>, _target_block: Option<u64>) -> Result<rust::transaction_optimizer::BundleStatus, OptimizerError> {
        if *self.should_revert.read().unwrap() {
            Err(OptimizerError::Timeout("Mock revert".to_string()))
        } else {
            Ok(rust::transaction_optimizer::BundleStatus::Pending)
        }
    }

    async fn simulate_trade_submission(&self, _route: &rust::types::ArbitrageRoute, _block_number: u64) -> Result<(U256, f64), OptimizerError> {
        Ok((*self.simulate_gas.read().unwrap(), *self.simulate_cost.read().unwrap()))
    }

    fn get_wallet_address(&self) -> Address {
        *self.wallet_address.read().unwrap()
    }

    async fn paper_trade(&self, _trade_id: &str, _route: &rust::types::ArbitrageRoute, _block_number: u64, _profit_usd: f64, _gas_cost_usd: f64) -> Result<H256, OptimizerError> {
        Ok(H256::zero())
    }
}

// === Mock Blockchain Manager ===
pub struct MockBlockchainManager {
    pool_manager: RwLock<Option<Arc<dyn rust::pool_manager::PoolManagerTrait + Send + Sync>>>,
    gas_oracle: Option<Arc<dyn GasOracleProvider + Send + Sync>>,
}

impl MockBlockchainManager {
    pub fn new() -> Self {
        Self {
            pool_manager: RwLock::new(None),
            gas_oracle: Some(Arc::new(MockGasOracle::new_zero_gas())),
        }
    }
}

impl std::fmt::Debug for MockBlockchainManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBlockchainManager").finish()
    }
}

#[async_trait::async_trait]
impl BlockchainManager for MockBlockchainManager {
    fn get_chain_name(&self) -> &str {
        "ethereum"
    }

    fn get_chain_id(&self) -> u64 {
        1
    }

    fn get_weth_address(&self) -> Address {
        Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap()
    }

    fn get_wallet_address(&self) -> Address {
        Address::zero()
    }

    fn get_uar_address(&self) -> Option<Address> {
        None
    }

    async fn submit_transaction(&self, _tx: TransactionRequest) -> Result<H256, BlockchainError> {
        Ok(H256::zero())
    }

    async fn check_health(&self) -> Result<rust::blockchain::HealthStatus, BlockchainError> {
        Ok(rust::blockchain::HealthStatus {
            connected: true,
            block_number: 1000,
            latency_ms: 50,
            peer_count: Some(10),
            chain_id: 1,
            timestamp: std::time::Instant::now(),
        })
    }

    fn get_provider(&self) -> Arc<Provider<Http>> {
        unimplemented!("Mock provider not implemented")
    }

    fn get_provider_http(&self) -> Option<Arc<Provider<Http>>> {
        None
    }

    fn get_ws_provider(&self) -> Result<Arc<Provider<Ws>>, BlockchainError> {
        Err(BlockchainError::NotAvailable("Mock WS provider not available".to_string()))
    }

    fn get_wallet(&self) -> &LocalWallet {
        unimplemented!("Mock wallet not implemented")
    }

    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Live
    }

    async fn submit_raw_transaction(&self, _tx: TransactionRequest, _gas_price: GasPrice) -> Result<H256, BlockchainError> {
        Ok(H256::zero())
    }

    async fn get_transaction_receipt(&self, _tx_hash: H256) -> Result<Option<TransactionReceipt>, BlockchainError> {
        Ok(None)
    }

    async fn get_transaction(&self, _tx_hash: H256) -> Result<Option<Transaction>, BlockchainError> {
        Ok(None)
    }

    async fn get_latest_block(&self) -> Result<Block<H256>, BlockchainError> {
        Err(BlockchainError::NotAvailable("Mock latest block not available".to_string()))
    }

    async fn get_block(&self, _block_id: BlockId) -> Result<Option<Block<H256>>, BlockchainError> {
        Ok(None)
    }

    async fn estimate_gas(&self, _tx: &TransactionRequest, _block: Option<BlockId>) -> Result<U256, BlockchainError> {
        Ok(U256::from(21000))
    }

    async fn get_gas_oracle(&self) -> Result<Arc<dyn GasOracleProvider + Send + Sync>, BlockchainError> {
        self.gas_oracle.as_ref()
            .cloned()
            .ok_or_else(|| BlockchainError::ModeError("No gas oracle configured".to_string()))
    }

    async fn get_balance(&self, _address: Address, _block: Option<BlockId>) -> Result<U256, BlockchainError> {
        Ok(U256::zero())
    }

    async fn get_token_decimals(&self, _token: Address, _block: Option<BlockId>) -> Result<u8, BlockchainError> {
        Ok(18)
    }

    async fn get_logs(&self, _filter: &Filter) -> Result<Vec<Log>, BlockchainError> {
        Ok(vec![])
    }

    async fn call(&self, _tx: &TransactionRequest, _block: Option<BlockId>) -> Result<Bytes, BlockchainError> {
        Ok(Bytes::new())
    }

    async fn get_code(&self, _address: Address, _block: Option<BlockId>) -> Result<Bytes, BlockchainError> {
        Ok(Bytes::new())
    }

    async fn check_token_allowance(&self, _token: Address, _owner: Address, _spender: Address) -> Result<U256, BlockchainError> {
        Ok(U256::zero())
    }

    async fn simulate_transaction(&self, _tx: &TransactionRequest) -> Result<bool, BlockchainError> {
        Ok(true)
    }

    async fn get_transaction_count(&self, _address: Address, _block: Option<BlockId>) -> Result<U256, BlockchainError> {
        Ok(U256::zero())
    }

    async fn get_token_decimals_batch(&self, _tokens: &[Address], _block: Option<BlockId>) -> Result<Vec<Option<u8>>, BlockchainError> {
        Ok(vec![Some(18); _tokens.len()])
    }

    async fn set_block_context(&self, _block_number: u64) -> Result<(), BlockchainError> {
        Ok(())
    }

    async fn clear_block_context(&self) {
        // No-op for mock
    }

    async fn get_current_block_context(&self) -> Option<BlockId> {
        None
    }

    async fn get_current_block_number(&self) -> Result<u64, BlockchainError> {
        Ok(0)
    }

    async fn sync_nonce_state(&self) -> Result<(), BlockchainError> {
        Ok(())
    }

    fn is_db_backend(&self) -> bool {
        false
    }

    fn get_chain_config(&self) -> Result<&rust::config::PerChainConfig, BlockchainError> {
        Err(BlockchainError::NotAvailable("Mock chain config not available".to_string()))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    // Note: get_pool_state is not part of the BlockchainManager trait
    // This method is removed to match the trait definition

    async fn estimate_time_to_target(&self, _target_block: u64) -> Result<std::time::Duration, BlockchainError> {
        Ok(std::time::Duration::from_secs(12))
    }

    fn is_l2(&self) -> bool {
        false
    }

    async fn get_pool_type(&self, _pool_address: Address) -> Result<ProtocolType, BlockchainError> {
        Ok(ProtocolType::UniswapV2)
    }

    fn get_native_token_address(&self) -> Address {
        Address::zero()
    }

    async fn get_token_price_ratio(&self, _token0: Address, _token1: Address) -> Result<f64, BlockchainError> {
        Ok(1.0)
    }

    async fn get_eth_price_usd(&self) -> Result<f64, BlockchainError> {
        Ok(2000.0)
    }

    async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        Ok(0)
    }

    async fn get_current_nonce(&self) -> Result<U256, BlockchainError> {
        Ok(U256::zero())
    }
}

// === Mock Mempool Monitor ===
pub struct MockMempoolMonitor {
    pub stats: Arc<StdRwLock<MempoolStats>>,
}

impl MockMempoolMonitor {
    pub fn new(stats: MempoolStats) -> Self {
        Self {
            stats: Arc::new(StdRwLock::new(stats)),
        }
    }

    pub fn set_stats(&self, stats: MempoolStats) {
        *self.stats.write().unwrap() = stats;
    }
}

#[async_trait::async_trait]
impl MempoolStatsProvider for MockMempoolMonitor {
    async fn get_stats(&self) -> Result<MempoolStats, MempoolError> {
        Ok(self.stats.read().unwrap().clone())
    }
}

// === Mock Token Registry ===
pub struct MockTokenRegistry {
    pub tokens: Arc<StdRwLock<Vec<Address>>>,
    pub token_info: Arc<StdRwLock<HashMap<Address, CoinRow>>>,
}

impl MockTokenRegistry {
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(StdRwLock::new(vec![])),
            token_info: Arc::new(StdRwLock::new(HashMap::new())),
        }
    }

    pub fn set_tokens(&self, tokens: Vec<Address>) {
        *self.tokens.write().unwrap() = tokens;
    }

    pub fn set_token_info(&self, address: Address, info: CoinRow) {
        self.token_info.write().unwrap().insert(address, info);
    }
}

#[async_trait::async_trait]
impl rust::token_registry::TokenRegistry for MockTokenRegistry {
    async fn get_token(&self, address: ethers::types::Address) -> Result<Option<rust::token_registry::TokenInfo>, eyre::ErrReport> {
        Ok(self.token_info.read().unwrap().get(&address).map(|coin_row| {
            rust::token_registry::TokenInfo {
                address: coin_row.address,
                symbol: coin_row.symbol.clone(),
                decimals: coin_row.decimals,
                name: coin_row.name.clone(),
            }
        }))
    }

    async fn get_all_tokens(&self) -> Result<Vec<rust::token_registry::TokenInfo>, eyre::ErrReport> {
        Ok(self.token_info.read().unwrap().values().map(|coin_row| {
            rust::token_registry::TokenInfo {
                address: coin_row.address,
                symbol: coin_row.symbol.clone(),
                decimals: coin_row.decimals,
                name: coin_row.name.clone(),
            }
        }).collect())
    }

    async fn is_registered(&self, address: ethers::types::Address) -> Result<bool, eyre::ErrReport> {
        Ok(self.token_info.read().unwrap().contains_key(&address))
    }

    async fn register_token(&mut self, info: rust::token_registry::TokenInfo) -> Result<(), eyre::ErrReport> {
        let coin_row = rust::token_registry::CoinRow {
            address: info.address,
            symbol: info.symbol,
            decimals: info.decimals,
            name: info.name,
            price_usd: None,
        };
        self.token_info.write().unwrap().insert(info.address, coin_row);
        Ok(())
    }
}

// === Mock Mempool Monitor Implementation ===
pub struct MockMempoolMonitorImpl {
    pub metrics: Arc<StdRwLock<MempoolMetrics>>,
}

impl MockMempoolMonitorImpl {
    pub fn new(metrics: MempoolMetrics) -> Self {
        Self {
            metrics: Arc::new(StdRwLock::new(metrics)),
        }
    }

    pub fn set_metrics(&self, metrics: MempoolMetrics) {
        *self.metrics.write().unwrap() = metrics;
    }
}

#[async_trait::async_trait]
impl MempoolMonitor for MockMempoolMonitorImpl {
    async fn start(&self) -> Result<(), MempoolError> {
        Ok(())
    }

    async fn shutdown(&self) {
        // No-op for mock
    }

    fn subscribe_to_raw_transactions(&self) -> tokio::sync::broadcast::Receiver<ethers::types::Transaction> {
        let (_, receiver) = tokio::sync::broadcast::channel(100);
        receiver
    }

    fn get_metrics(&self) -> MempoolMetrics {
        self.metrics.read().unwrap().clone()
    }

    fn get_monitor_type(&self) -> &'static str {
        "mock"
    }
}

// === Mock Cross Chain Manager ===
pub struct MockCrossChainManager;

impl MockCrossChainManager {
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for MockCrossChainManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockCrossChainManager").finish()
    }
}

// === Mock UAR Contract ===
#[derive(Debug)]
pub struct MockUarContract {
    pub trusted_targets: Arc<StdRwLock<HashMap<Address, bool>>>,
    pub owner: Arc<StdRwLock<Address>>,
    pub flash_loan_success: Arc<StdRwLock<bool>>,
}

impl MockUarContract {
    pub fn new(owner: Address) -> Self {
        Self {
            trusted_targets: Arc::new(StdRwLock::new(HashMap::new())),
            owner: Arc::new(StdRwLock::new(owner)),
            flash_loan_success: Arc::new(StdRwLock::new(true)),
        }
    }

    pub fn add_trusted_target(&self, target: Address) {
        self.trusted_targets.write().unwrap().insert(target, true);
    }

    pub fn remove_trusted_target(&self, target: Address) {
        self.trusted_targets.write().unwrap().remove(&target);
    }

    pub fn is_trusted_target(&self, target: Address) -> bool {
        self.trusted_targets.read().unwrap().get(&target).copied().unwrap_or(false)
    }

    pub fn set_owner(&self, owner: Address) {
        *self.owner.write().unwrap() = owner;
    }

    pub fn get_owner(&self) -> Address {
        *self.owner.read().unwrap()
    }

    pub fn set_flash_loan_success(&self, success: bool) {
        *self.flash_loan_success.write().unwrap() = success;
    }
}

// === Mock Predictive Engine ===
#[derive(Debug)]
pub struct MockPredictiveEngine;

#[async_trait::async_trait]
impl PredictiveEngine for MockPredictiveEngine {
    async fn predict<'a>(
        &'a self,
        _tx_context: &'a rust::types::TransactionContext,
        _block_number: Option<u64>,
    ) -> Result<Option<rust::types::MEVPrediction>, rust::errors::PredictiveEngineError> {
        Ok(None)
    }

    async fn update_model<'a>(
        &'a self,
        _result: &'a rust::types::ExecutionResult,
    ) -> Result<(), rust::errors::PredictiveEngineError> {
        Ok(())
    }

    async fn get_performance_metrics<'a>(&'a self) -> Result<rust::types::ModelPerformance, rust::errors::PredictiveEngineError> {
        Ok(rust::types::ModelPerformance::default())
    }
} 

#[derive(Debug)]
pub struct MockPoolManager {
    pools: HashMap<Address, Arc<PoolStaticMetadata>>,
    pools_by_token: HashMap<Address, Vec<Address>>,
    all_pools: Vec<Arc<PoolStaticMetadata>>,
}

impl MockPoolManager {
    pub fn new() -> Self {
        Self {
            pools: HashMap::new(),
            pools_by_token: HashMap::new(),
            all_pools: Vec::new(),
        }
    }

    pub fn add_test_pools(&mut self, pairs: &[DeployedPair], tokens: &[DeployedToken]) {
        for pair in pairs {
            let token0_info = tokens.iter().find(|t| t.address == pair.token0).unwrap();
            let token1_info = tokens.iter().find(|t| t.address == pair.token1).unwrap();

            let pool_address = pair.address;

            let metadata = Arc::new(PoolStaticMetadata {
                pool_address,
                token0: pair.token0,
                token1: pair.token1,
                token0_decimals: token0_info.decimals,
                token1_decimals: token1_info.decimals,
                fee: Some(3000),
                protocol_type: ProtocolType::UniswapV2,
                dex_name: "TestDex".to_string(),
                chain_name: "anvil".to_string(),
                creation_block: 0,
                latest_token0_reserve: None,
                latest_token1_reserve: None,
                latest_sqrt_price_x96: None,
                latest_liquidity: None,
                latest_tick: None,
            });

            self.pools.insert(pool_address, metadata.clone());
            self.pools_by_token.entry(pair.token0).or_default().push(pool_address);
            self.pools_by_token.entry(pair.token1).or_default().push(pool_address);
            self.all_pools.push(metadata);
        }
    }
}

#[async_trait]
impl PoolManagerTrait for MockPoolManager {
    async fn get_pools_for_pair(
        &self,
        _chain: &str,
        a: Address,
        b: Address,
    ) -> Result<Vec<Address>, DexError> {
        println!("MockPoolManager::get_pools_for_pair called with a={:?}, b={:?}", a, b);
        println!("MockPoolManager has {} pools total", self.pools.len());
        println!("Pools by token a ({:?}): {:?}", a, self.pools_by_token.get(&a));
        println!("Pools by token b ({:?}): {:?}", b, self.pools_by_token.get(&b));
        
        let mut result = Vec::new();
        
        // Check pools that contain token a
        if let Some(pools) = self.pools_by_token.get(&a) {
            for pool_addr in pools {
                if let Some(metadata) = self.pools.get(pool_addr) {
                    println!("Checking pool {:?}: token0={:?}, token1={:?}", pool_addr, metadata.token0, metadata.token1);
                    if (metadata.token0 == a && metadata.token1 == b) || 
                       (metadata.token0 == b && metadata.token1 == a) {
                        println!("Found matching pool: {:?}", pool_addr);
                        result.push(*pool_addr);
                    }
                }
            }
        }
        
        // Also check pools that contain token b (in case a doesn't have any pools)
        if let Some(pools) = self.pools_by_token.get(&b) {
            for pool_addr in pools {
                if let Some(metadata) = self.pools.get(pool_addr) {
                    println!("Checking pool {:?}: token0={:?}, token1={:?}", pool_addr, metadata.token0, metadata.token1);
                    if (metadata.token0 == a && metadata.token1 == b) || 
                       (metadata.token0 == b && metadata.token1 == a) {
                        println!("Found matching pool: {:?}", pool_addr);
                        if !result.contains(pool_addr) {
                            result.push(*pool_addr);
                        }
                    }
                }
            }
        }
        
        println!("MockPoolManager::get_pools_for_pair returning {} pools", result.len());
        Ok(result)
    }

    async fn get_pool_info(
        &self,
        _chain: &str,
        pool: Address,
        _block: Option<u64>,
    ) -> Result<PoolInfo, DexError> {
        if let Some(metadata) = self.pools.get(&pool) {
            Ok(PoolInfo {
                address: pool,
                pool_address: pool,
                token0: metadata.token0,
                token1: metadata.token1,
                token0_decimals: metadata.token0_decimals,
                token1_decimals: metadata.token1_decimals,
                fee: metadata.fee,
                fee_bps: metadata.fee.unwrap_or(3000),
                protocol_type: metadata.protocol_type.clone(),
                chain_name: metadata.chain_name.clone(),
                creation_block_number: Some(metadata.creation_block),
                last_updated: 0,
                v2_reserve0: Some(ethers::types::U256::from_dec_str("1000000000000000000000").unwrap()),
                v2_reserve1: Some(ethers::types::U256::from_dec_str("1000000000000000000000").unwrap()),
                v2_block_timestamp_last: None,
                v3_sqrt_price_x96: None,
                v3_liquidity: None,
                v3_ticks: None,
                v3_tick_current: None,
                v3_tick_data: None,
                reserves0: ethers::types::U256::from_dec_str("1000000000000000000000").unwrap(),
                reserves1: ethers::types::U256::from_dec_str("1000000000000000000000").unwrap(),
                liquidity_usd: 1_000_000.0,
            })
        } else {
            // Return a default PoolInfo to avoid breaking tests that might query a pool not explicitly added.
            Ok(PoolInfo {
                address: pool,
                pool_address: pool,
                token0: Address::zero(),
                token1: Address::zero(),
                token0_decimals: 18,
                token1_decimals: 18,
                fee: Some(3000),
                fee_bps: 3000,
                protocol_type: ProtocolType::UniswapV2,
                chain_name: "anvil".to_string(),
                creation_block_number: Some(0),
                last_updated: 0,
                v2_reserve0: Some(ethers::types::U256::from_dec_str("1000000000000000000000").unwrap()),
                v2_reserve1: Some(ethers::types::U256::from_dec_str("1000000000000000000000").unwrap()),
                v2_block_timestamp_last: None,
                v3_sqrt_price_x96: None,
                v3_liquidity: None,
                v3_ticks: None,
                v3_tick_current: None,
                v3_tick_data: None,
                reserves0: ethers::types::U256::from_dec_str("1000000000000000000000").unwrap(),
                reserves1: ethers::types::U256::from_dec_str("1000000000000000000000").unwrap(),
                liquidity_usd: 1_000_000.0,
            })
        }
    }

    async fn get_all_pools(
        &self,
        _chain: &str,
        prot_filter: Option<ProtocolType>,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        let pools = match prot_filter {
            Some(protocol) => self.all_pools.iter()
                .filter(|meta| meta.protocol_type == protocol)
                .cloned()
                .collect(),
            None => self.all_pools.clone(),
        };
        Ok(pools)
    }

    async fn get_pools_created_in_block_range(
        &self,
        chain_name: &str,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        let pools = self
            .all_pools
            .iter()
            .filter(|meta| {
                meta.chain_name == chain_name
                    && meta.creation_block >= start_block
                    && meta.creation_block <= end_block
            })
            .cloned()
            .collect();
        Ok(pools)
    }

    async fn get_metadata_for_pool(&self, pool_address: Address) -> Option<Arc<PoolStaticMetadata>> {
        self.pools.get(&pool_address).cloned()
    }

    fn chain_name_by_id(&self, _chain_id: u64) -> Result<String, DexError> {
        Ok("anvil".to_string())
    }

    async fn set_state_oracle(&self, _state_oracle: Arc<rust::pool_states::PoolStateOracle>) -> Result<(), rust::errors::PoolManagerError> {
        Ok(())
    }

    async fn get_pools_for_pair_at_block(
        &self,
        chain: &str,
        a: Address,
        b: Address,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError> {
        println!("MockPoolManager::get_pools_for_pair_at_block called with a={:?}, b={:?}, block={}", a, b, block_number);

        // First get pools active at the block
        let active_pools = self.get_pools_active_at_block(chain, block_number).await?;
        let active_pool_addresses: std::collections::HashSet<_> = active_pools.iter()
            .map(|meta| meta.pool_address)
            .collect();

        // Filter the pair pools to only include active ones
        let pair_pools = self.get_pools_for_pair(chain, a, b).await?;
        let filtered_pools: Vec<Address> = pair_pools.into_iter()
            .filter(|pool_addr| active_pool_addresses.contains(pool_addr))
            .collect();

        println!("MockPoolManager::get_pools_for_pair_at_block returning {} pools", filtered_pools.len());
        Ok(filtered_pools)
    }

    async fn get_pools_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Arc<PoolStaticMetadata>>, DexError> {
        println!("MockPoolManager::get_pools_active_at_block called with chain={}, block={}", chain_name, block_number);

        let pools = self
            .all_pools
            .iter()
            .filter(|meta| {
                meta.chain_name == chain_name
                    && meta.creation_block <= block_number
            })
            .cloned()
            .collect::<Vec<_>>();

        println!("MockPoolManager::get_pools_active_at_block returning {} pools", pools.len());
        Ok(pools)
    }

    async fn get_tokens_active_at_block(
        &self,
        chain_name: &str,
        block_number: u64,
    ) -> Result<Vec<Address>, DexError> {
        println!("MockPoolManager::get_tokens_active_at_block called with chain={}, block={}", chain_name, block_number);

        // Get active pools and extract unique tokens
        let pools = self.get_pools_active_at_block(chain_name, block_number).await?;
        let mut tokens = std::collections::HashSet::new();

        for pool in pools {
            tokens.insert(pool.token0);
            tokens.insert(pool.token1);
        }

        let token_vec: Vec<Address> = tokens.into_iter().collect();
        println!("MockPoolManager::get_tokens_active_at_block returning {} tokens", token_vec.len());
        Ok(token_vec)
    }

    async fn get_total_liquidity_for_token_units(
        &self,
        _chain_name: &str,
        _token: Address,
        _block_number: Option<u64>,
    ) -> Result<f64, DexError> {
        Ok(1000000.0) // Mock liquidity value
    }

    async fn set_block_context(&self, _block_number: u64) -> Result<(), rust::errors::PoolManagerError> {
        Ok(())
    }

    async fn reload_registry_for_backtest(&self, _block_number: u64) -> Result<(), rust::errors::PoolManagerError> {
        Ok(())
    }

} 