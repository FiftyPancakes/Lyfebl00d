use std::sync::Arc;
use anyhow::Result;
use ethers::types::Address;

pub struct RealTestHarness {
    pub chain_id: u64,
    chain_name: String,
    config: Arc<rust::config::Config>,
    blockchain: Arc<dyn rust::blockchain::BlockchainManager + Send + Sync>,
    pool_manager: Arc<dyn rust::pool_manager::PoolManagerTrait + Send + Sync>,
    pool_state_oracle: Arc<rust::pool_states::PoolStateOracle>,
    price_oracle: Arc<dyn rust::price_oracle::PriceOracle + Send + Sync>,
    gas_oracle: Arc<dyn rust::gas_oracle::GasOracleProvider + Send + Sync>,
    arbitrage_engine: Arc<rust::types::ArbitrageEngine>,
}

impl RealTestHarness {
    pub async fn new_for_chain(chain_name: &str) -> Result<Self> {
        let config = Arc::new(rust::config::Config::load_from_directory("config").await
            .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?);

        let quote_source = Arc::new(rust::quote_source::MultiAggregatorQuoteSource::new(Vec::new()));

        let blockchain = Arc::new(rust::blockchain::BlockchainManagerImpl::new(
            &config,
            chain_name,
            rust::config::ExecutionMode::Live,
            None,
        ).await
            .map_err(|e| anyhow::anyhow!("Failed to create blockchain manager: {}", e))?) as Arc<dyn rust::blockchain::BlockchainManager + Send + Sync>;

        let chain_config = config.chain_config.chains.get(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Chain config not found for {}", chain_name))?;
        let chain_id = chain_config.chain_id;

        let price_oracle: Arc<dyn rust::price_oracle::PriceOracle + Send + Sync> = Arc::new(
            rust::price_oracle::live_price_provider::LivePriceProvider::new(
                config.clone(),
                blockchain.clone(),
                quote_source.clone(),
            ).await
                .map_err(|e| anyhow::anyhow!("Failed to create price oracle: {}", e))?
        );

        let gas_oracle = rust::gas_oracle::create_gas_oracle_provider(config.clone(), blockchain.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to create gas oracle: {}", e))?;

        let pool_manager = Arc::new(
            rust::pool_manager::PoolManager::new_without_state_oracle(
                config.clone(),
            ).await
                .map_err(|e| anyhow::anyhow!("Failed to create pool manager: {}", e))?
        ) as Arc<dyn rust::pool_manager::PoolManagerTrait + Send + Sync>;

        let mut chain_map: std::collections::HashMap<String, Arc<dyn rust::blockchain::BlockchainManager>> = std::collections::HashMap::new();
        chain_map.insert(chain_name.to_string(), blockchain.clone());

        let pool_state_oracle = Arc::new(rust::pool_states::PoolStateOracle::new(
            config.clone(),
            chain_map,
            None,
            pool_manager.clone(),
        ).map_err(|e| anyhow::anyhow!("Failed to create pool state oracle: {}", e))?);

        let arbitrage_engine = Arc::new(rust::types::ArbitrageEngine {
            config: Arc::new(config.module_config.as_ref().ok_or_else(|| anyhow::anyhow!("missing module config"))?.arbitrage_settings.clone()),
            full_config: config.clone(),
            price_oracle: price_oracle.clone(),
            risk_manager: Arc::new(tokio::sync::RwLock::new(None)),
            blockchain_manager: blockchain.clone(),
            quote_source: quote_source.clone(),
            global_metrics: Arc::new(rust::types::ArbitrageMetrics::new()),
            alpha_engine: None,
            swap_loader: None,
        });

        Ok(Self {
            chain_id,
            chain_name: chain_name.to_string(),
            config,
            blockchain,
            pool_manager,
            pool_state_oracle,
            price_oracle,
            gas_oracle,
            arbitrage_engine,
        })
    }

    pub async fn get_current_block(&self) -> Result<u64> {
        // Note: BlockchainManager trait is not used in this file
        self.blockchain.get_block_number().await
            .map_err(|e| anyhow::anyhow!("Failed to get block number: {}", e))
    }

    pub async fn find_arbitrage_opportunities(&self, block: Option<u64>) -> Result<Vec<rust::types::ArbitrageOpportunity>> {
        use rust::types::ArbitrageEngineTrait;
        
        let target_block = if let Some(b) = block { b } else { self.get_current_block().await? };

        let swaps = self.pool_state_oracle
            .get_swap_events_for_block(&self.chain_name, target_block)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get swap events: {}", e))?;

        let mut dex_swaps: Vec<rust::types::DexSwapEvent> = Vec::with_capacity(swaps.len());
        for s in swaps.iter() {
            let dex_swap = rust::types::DexSwapEvent {
                chain_name: s.chain_name.clone(),
                protocol: s.protocol.clone(),
                transaction_hash: s.tx_hash,
                block_number: s.block_number,
                log_index: s.log_index,
                pool_address: s.pool_address,
                data: s.data.clone(),
                unix_ts: Some(s.unix_ts),
                token0: s.token0,
                token1: s.token1,
                price_t0_in_t1: None,
                price_t1_in_t0: None,
                reserve0: None,
                reserve1: None,
                transaction: None,
                transaction_metadata: None,
                token_in: Some(s.token_in),
                token_out: Some(s.token_out),
                amount_in: Some(s.amount_in),
                amount_out: Some(s.amount_out),
                pool_id: None,
                buyer: None,
                sold_id: None,
                tokens_sold: None,
                bought_id: None,
                tokens_bought: None,
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                token0_decimals: None,
                token1_decimals: None,
                fee_tier: None,
                protocol_type: Some(rust::types::ProtocolType::from(&s.protocol)),
            };
            dex_swaps.push(dex_swap);
        }

        let opportunities = self.arbitrage_engine
            .find_opportunities(&dex_swaps)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to find arbitrage opportunities: {}", e))?;

        Ok(opportunities)
    }

    pub async fn get_pool_info(&self, pool: Address, block: Option<u64>) -> Result<rust::types::PoolInfo> {
        self.pool_manager.get_pool_info(&self.chain_name, pool, block).await
            .map_err(|e| anyhow::anyhow!("Failed to get pool info: {}", e))
    }

    pub async fn get_token_price(&self, token: Address) -> Result<f64> {
        self.price_oracle.get_token_price_usd(&self.chain_name, token).await
            .map_err(|e| anyhow::anyhow!("Failed to get token price: {}", e))
    }

    pub async fn get_gas_price(&self) -> Result<rust::gas_oracle::GasPrice> {
        self.gas_oracle.get_gas_price(&self.chain_name, None).await
            .map_err(|e| anyhow::anyhow!("Failed to get gas price: {}", e))
    }
}

