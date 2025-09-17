// # Transaction Optimizer V2 - Implementation
//
// Core trait implementation and additional strategies
// This file is included directly into transaction_optimizer.rs via include!()
// so it doesn't need its own imports

// ======================== Additional Submission Strategies ========================

#[derive(Debug)]
struct FlashbotsStrategy {
    middleware: Arc<ethers_flashbots::FlashbotsMiddleware<
        Arc<ethers::providers::Provider<ethers::providers::Http>>,
        LocalWallet
    >>,
    provider: Arc<dyn crate::blockchain::BlockchainManager>,
    nonce_manager: Arc<NonceManager>,
    block_offset: u64,
}

#[async_trait]
impl SubmissionStrategyTrait for FlashbotsStrategy {
    async fn submit(&self, tx: TransactionRequest) -> Result<H256, OptimizerError> {
        let bribe_amount = self.calculate_bribe_amount(&tx).await;
        let bundle = self.create_flashbots_bundle(tx, bribe_amount).await?;
        
        let block_number = self.provider.get_provider().get_block_number().await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;
        let target_block = block_number
            .checked_add(self.block_offset.into())
            .ok_or_else(|| OptimizerError::Provider("Block number overflow".to_string()))?;
        
        self.submit_bundle(&bundle, target_block.as_u64()).await
    }
    
    fn supports_mev_protection(&self) -> bool {
        true
    }
    
    async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError> {
        let gas = self.provider.estimate_gas(tx, None).await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        let bribe = self.calculate_bribe_amount(tx).await;
        Ok(gas.saturating_add(bribe))
    }
}

impl FlashbotsStrategy {
    async fn calculate_bribe_amount(&self, tx: &TransactionRequest) -> U256 {
        let gas_estimate = tx.gas.unwrap_or(U256::from(300_000));
        let base_gas_cost = gas_estimate.saturating_mul(U256::from(20_000_000_000u64));
        let bribe = base_gas_cost / U256::from(10);
        let min_bribe = U256::from(1_000_000_000_000_000u64);
        
        if bribe < min_bribe { min_bribe } else { bribe }
    }
    
    async fn create_flashbots_bundle(&self, tx: TransactionRequest, _bribe_amount: U256) -> Result<Vec<Bytes>, OptimizerError> {
        let wallet = self.provider.get_wallet();
        let sender = wallet.address();
        let chain_id = self.provider.get_chain_id();
        
        let arb_nonce = self.nonce_manager.get_next_nonce(sender).await?;
        
        let mut arb_tx = tx;
        arb_tx.nonce = Some(arb_nonce);
        arb_tx.from = Some(sender);
        arb_tx.chain_id = Some(chain_id.into());
        
        let arb_eip1559 = ethers::types::Eip1559TransactionRequest {
            from: Some(sender),
            to: arb_tx.to,
            value: arb_tx.value,
            data: arb_tx.data,
            gas: arb_tx.gas,
            max_fee_per_gas: arb_tx.gas_price,
            max_priority_fee_per_gas: arb_tx.gas_price.map(|p| p / U256::from(10)),
            nonce: Some(arb_nonce),
            chain_id: Some(chain_id.into()),
            access_list: Default::default(),
        };
        
        let arb_typed_tx: ethers::types::transaction::eip2718::TypedTransaction = arb_eip1559.into();
        let arb_signature = wallet.sign_transaction(&arb_typed_tx).await
            .map_err(|e| OptimizerError::Signing(e.to_string()))?;
        let arb_raw = arb_typed_tx.rlp_signed(&arb_signature);
        
        Ok(vec![arb_raw])
    }
    
    async fn submit_bundle(&self, signed_transactions: &[Bytes], target_block: u64) -> Result<H256, OptimizerError> {
        if signed_transactions.is_empty() {
            return Err(OptimizerError::Config("Empty bundle".to_string()));
        }
        
        let mut bundle = ethers_flashbots::BundleRequest::new()
            .set_block(target_block.into())
            .set_simulation_block(target_block.saturating_sub(1).into())
            .set_simulation_timestamp(chrono::Utc::now().timestamp() as u64);
        
        for raw_tx in signed_transactions {
            bundle = bundle.push_transaction(raw_tx.clone());
        }
        
        let simulation = self.middleware.simulate_bundle(&bundle).await;
        match simulation {
            Ok(sim_result) => {
                if sim_result.gas_used == U256::zero() {
                    return Err(OptimizerError::SubmissionFinal(format!("Bundle simulation failed: {:?}", sim_result)));
                }
                debug!(
                    target_block = target_block,
                    gas_used = %sim_result.gas_used,
                    "Bundle simulation successful"
                );
            }
            Err(e) => {
                return Err(OptimizerError::SubmissionFinal(format!("Bundle simulation error: {}", e)));
            }
        }
        
        let bundle_response = self.middleware.send_bundle(&bundle).await
            .map_err(|e| OptimizerError::SubmissionFinal(format!("Bundle submission failed: {}", e)))?;
        
        let bundle_hash = bundle_response.bundle_hash
            .ok_or_else(|| OptimizerError::SubmissionFinal("Bundle hash not available".to_string()))?;
        
        info!(
            bundle_hash = %bundle_hash,
            target_block = target_block,
            "Flashbots bundle submitted"
        );
        
        Ok(bundle_hash)
    }
}

#[derive(Debug)]
struct PrivateRelayStrategy {
    name: String,
    endpoint: String,
    provider: Arc<dyn crate::blockchain::BlockchainManager>,
    nonce_manager: Arc<NonceManager>,
}

#[async_trait]
impl SubmissionStrategyTrait for PrivateRelayStrategy {
    async fn submit(&self, mut tx: TransactionRequest) -> Result<H256, OptimizerError> {
        let relay_provider = Arc::new(
            ethers::providers::Provider::<ethers::providers::Http>::try_from(&self.endpoint)
                .map_err(|e| OptimizerError::Provider(e.to_string()))?
        );
        
        let sender = tx.from.unwrap_or_else(|| self.provider.get_wallet().address());
        let nonce = self.nonce_manager.get_next_nonce(sender).await?;
        tx.nonce = Some(nonce);
        tx.from = Some(sender);
        
        let wallet = self.provider.get_wallet();
        let typed_tx: ethers::types::transaction::eip2718::TypedTransaction = tx.into();
        let signature = wallet.sign_transaction(&typed_tx).await
            .map_err(|e| OptimizerError::Signing(e.to_string()))?;
        let raw_tx = typed_tx.rlp_signed(&signature);
        
        let pending_tx = relay_provider.send_raw_transaction(raw_tx).await
            .map_err(|e| OptimizerError::SubmissionFinal(e.to_string()))?;
        
        Ok(pending_tx.tx_hash())
    }
    
    fn supports_mev_protection(&self) -> bool {
        true
    }
    
    async fn estimate_cost(&self, tx: &TransactionRequest) -> Result<U256, OptimizerError> {
        self.provider.estimate_gas(tx, None).await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))
    }
}

// ======================== Main Trait Implementation ========================

#[async_trait]
impl crate::transaction_optimizer::TransactionOptimizerTrait for TransactionOptimizerV2 {
    #[instrument(skip(self, tx), fields(to = ?tx.to, value = ?tx.value))]
    async fn auto_optimize_and_submit(
        &self,
        mut tx: TransactionRequest,
        use_flashbots_hint: Option<bool>,
    ) -> Result<H256, OptimizerError> {
        // Circuit breaker check
        if self.circuit_breaker.is_tripped().await {
            return Err(OptimizerError::circuit_breaker_open());
        }
        
        // Validate transaction
        self.validate_transaction(&tx)?;
        
        // Check daily limits
        let tx_value = tx.value.unwrap_or_default();
        if !self.daily_stats.can_submit(&self.config.security, tx_value).await {
            return Err(OptimizerError::DailyLimitExceeded);
        }
        
        // Acquire execution permit
        let permit = self.execution_semaphore.acquire().await
            .map_err(|_| OptimizerError::Concurrency("Failed to acquire submission permit".to_string()))?;
        let _permit_guard = scopeguard::guard(permit, |p| drop(p));
        
        // Record submission latency
        let timer = SUBMISSION_LATENCY.start_timer();
        
        // Prepare transaction
        tx = self.prepare_transaction(tx).await?;
        
        // Select submission strategy
        let strategy_name = self.select_submission_strategy(&tx, use_flashbots_hint)?;
        
        // Submit via selected strategy
        let result = self.submit_via_strategy(tx.clone(), &strategy_name).await;
        
        // Record metrics
        timer.observe_duration();
        match &result {
            Ok(hash) => {
                TX_SUBMITTED.inc();
                self.daily_stats.record_transaction(tx_value).await?;
                self.track_submission(*hash, tx, strategy_name).await;
                self.circuit_breaker.record_success().await;
            }
            Err(_) => {
                TX_FAILED.inc();
                self.circuit_breaker.record_failure().await;
            }
        }
        
        result
    }
    
    async fn submit_transaction(
        &self,
        tx: TransactionRequest,
        gas_price: crate::gas_oracle::GasPrice,
    ) -> Result<H256, OptimizerError> {
        debug!("TransactionOptimizerV2::submit_transaction called");
        
        if self.circuit_breaker.is_tripped().await {
            return Err(OptimizerError::circuit_breaker_open());
        }
        
        self.validate_transaction(&tx)?;
        
        let sender = tx.from.unwrap_or_else(|| self.provider.get_wallet().address());
        let nonce = self.nonce_manager.get_next_nonce(sender).await?;
        
        let mut prepared_tx = self.prepare_transaction(tx).await?;
        prepared_tx.nonce = Some(nonce);
        
        let tx_hash = self.retry_with_backoff(
            || self.provider.submit_raw_transaction(prepared_tx.clone(), gas_price),
            self.config.base_config.max_retry_attempts,
        ).await?;
        
        self.track_submission(tx_hash, prepared_tx, "direct".to_string()).await;
        
        info!("Transaction submitted successfully: {:?}", tx_hash);
        Ok(tx_hash)
    }
    
    async fn wait_for_confirmation(
        &self,
        tx_hash: H256,
        confirmations: usize,
    ) -> Result<TransactionReceipt, OptimizerError> {
        let timeout_duration = Duration::from_secs(self.config.base_config.confirmation_timeout_secs);
        let start_time = Instant::now();
        
        loop {
            if start_time.elapsed() > timeout_duration {
                return Err(OptimizerError::Timeout("Confirmation timeout".to_string()));
            }
            
            if let Ok(Some(receipt)) = self.provider.get_provider().get_transaction_receipt(tx_hash).await {
                if let Some(block_number) = receipt.block_number {
                    let latest_block = self.provider.get_provider().get_block_number().await
                        .map_err(|e| OptimizerError::Provider(e.to_string()))?;
                    
                    if latest_block.saturating_sub(U64::from(block_number)) >= U64::from(confirmations) {
                        TX_CONFIRMED.inc();
                        self.pending_transactions.remove(&tx_hash).await;
                        return Ok(receipt);
                    }
                }
            }
            
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    
    async fn simulate_swap(
        &self,
        route: &SwapRoute,
        params: &SwapParams,
    ) -> Result<U256, OptimizerError> {
        // Check simulation cache first
        let cache_key = self.compute_simulation_cache_key(route, params);
        
        {
            let mut cache = self.simulation_cache.lock().await;
            if let Some(cached) = cache.get(&cache_key) {
                if cached.cached_at.elapsed() < self.config.performance.simulation_cache_ttl {
                    return Ok(cached.gas_used);
                }
            }
        }
        
        // Perform simulation
        let arbitrage_route = self.convert_swap_route_to_arbitrage(route, params)?;
        let tx = self.encode_arbitrage_transaction(&arbitrage_route, U256::zero()).await?;
        
        let (success, output) = self.simulate_transaction(&tx, None).await?;
        
        if !success {
            return Err(OptimizerError::Simulation("Swap simulation failed".to_string()));
        }
        
        let amount_out = if let Some(output_data) = output {
            if output_data.len() >= 32 {
                U256::from_big_endian(&output_data[output_data.len() - 32..])
            } else {
                return Err(OptimizerError::Simulation("Invalid output data".to_string()));
            }
        } else {
            return Err(OptimizerError::Simulation("No output data".to_string()));
        };
        
        // Cache the result
        {
            let mut cache = self.simulation_cache.lock().await;
            cache.put(cache_key, SimulationResult {
                success: true,
                output: None,
                gas_used: amount_out,
                cached_at: Instant::now(),
            });
        }
        
        Ok(amount_out)
    }
    
    async fn execute_trade(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
        block_number: u64,
    ) -> Result<H256, OptimizerError> {
        if self.circuit_breaker.is_tripped().await {
            return Err(OptimizerError::circuit_breaker_open());
        }
        
        self.provider.set_block_context(block_number).await
            .map_err(|e| OptimizerError::Provider(format!("Failed to set block context: {}", e)))?;
        
        let tx = self.build_trade_transaction(trade_id, route).await?;
        
        self.provider.clear_block_context().await;
        
        let use_flashbots = self.is_mev_sensitive(&tx) && self.flashbots_middleware.is_some();
        self.auto_optimize_and_submit(tx, Some(use_flashbots)).await
    }
    
    async fn simulate_submission(
        &self,
        route: &ArbitrageRoute,
        block_number: u64,
    ) -> Result<(U256, f64), OptimizerError> {
        self.simulate_trade_submission(route, block_number).await
    }
    
    async fn simulate_trade_submission(
        &self,
        route: &ArbitrageRoute,
        block_number: u64,
    ) -> Result<(U256, f64), OptimizerError> {
        debug!(
            target: "transaction_optimizer::simulate_trade_submission",
            block_number,
            route_legs = route.legs.len(),
            "Starting trade submission simulation"
        );
        
        let tx = self.build_trade_transaction("simulated_trade", route).await?;
        
        let (success, _) = self.simulate_transaction(&tx, Some(block_number)).await?;
        
        if !success {
            return Err(OptimizerError::Simulation("Trade simulation failed".to_string()));
        }
        
        let gas_used = self.provider.estimate_gas(
            &tx,
            Some(BlockId::Number(BlockNumber::Number(block_number.into())))
        ).await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        
        let gas_price = self.gas_oracle
            .get_gas_price(&self.provider.get_chain_name(), Some(block_number))
            .await
            .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
        
        let total_gas_price = gas_price.base_fee.saturating_add(gas_price.priority_fee);
        let gas_cost_wei = total_gas_price
            .checked_mul(gas_used)
            .ok_or_else(|| OptimizerError::GasEstimation("Overflow in gas cost calculation".to_string()))?;
        
        let chain_name = self.provider.get_chain_name();
        let weth_address = self.provider.get_weth_address();
        let eth_price_usd = self.price_oracle
            .get_token_price_usd_at(&chain_name, weth_address, Some(block_number), None)
            .await
            .map_err(|e| OptimizerError::Provider(format!("Failed to get ETH/USD price: {}", e)))?;
        
        let gas_cost_eth = self.u256_to_f64(gas_cost_wei, 18);
        let gas_cost_usd = gas_cost_eth * eth_price_usd;
        
        GAS_PRICE_GAUGE.set(self.u256_to_f64(total_gas_price, 9));
        
        Ok((gas_used, gas_cost_usd))
    }
    
    async fn paper_trade(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
        block_number: u64,
        profit_usd: f64,
        gas_cost_usd: f64,
    ) -> Result<H256, OptimizerError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let net_profit_usd = profit_usd - gas_cost_usd;
        let roi_percentage = if gas_cost_usd > 0.0 {
            (net_profit_usd / gas_cost_usd) * 100.0
        } else {
            0.0
        };
        
        info!(
            target: "paper_trading",
            trade_id,
            block_number,
            timestamp,
            route_legs = route.legs.len(),
            profit_usd,
            gas_cost_usd,
            net_profit_usd,
            roi_percentage,
            "PAPER TRADE EXECUTED"
        );
        
        let digest = keccak256(format!("paper_trade_{}_{}", trade_id, timestamp).as_bytes());
        let dummy_hash = H256::from_slice(&digest);
        
        self.store_paper_trade_data(
            trade_id,
            route,
            block_number,
            profit_usd,
            gas_cost_usd,
            net_profit_usd,
            timestamp
        ).await?;
        
        Ok(dummy_hash)
    }
    
    fn get_wallet_address(&self) -> Address {
        self.provider.get_wallet().address()
    }
    
    async fn submit_flashbots_bundle(
        &self,
        signed_transactions: &[Bytes],
        target_block: u64,
    ) -> Result<H256, OptimizerError> {
        let strategies = self.submission_strategies.read().await;
        if let Some(strategy) = strategies.get("flashbots") {
            if let Some(flashbots) = strategy.as_any().downcast_ref::<FlashbotsStrategy>() {
                return flashbots.submit_bundle(signed_transactions, target_block).await;
            }
        }
        
        Err(OptimizerError::Config("Flashbots not configured".to_string()))
    }
    
    async fn check_bundle_status(
        &self,
        bundle_hash: H256,
        target_block: u64,
    ) -> Result<BundleStatus, OptimizerError> {
        self.check_bundle_status_efficient(bundle_hash, target_block).await
    }
    
    async fn shutdown(&self) {
        self.shutdown_signal.notify_waiters();
        if let Some(handle) = self.monitor_handle.write().await.take() {
            let _ = handle.await;
        }
    }
}

// ======================== Helper Methods for TransactionOptimizerV2 ========================

impl TransactionOptimizerV2 {
    async fn prepare_transaction(&self, mut tx: TransactionRequest) -> Result<TransactionRequest, OptimizerError> {
        let sender = tx.from.unwrap_or_else(|| self.provider.get_wallet().address());
        tx.from = Some(sender);
        
        if tx.gas.is_none() {
            let gas_estimate = self.provider.estimate_gas(&tx.clone().into(), None).await
                .map_err(|e| OptimizerError::GasEstimation(e.to_string()))?;
            tx.gas = Some(gas_estimate);
        }
        
        if tx.chain_id.is_none() {
            tx.chain_id = Some(self.provider.get_chain_id().into());
        }
        
        Ok(tx)
    }
    
    fn select_submission_strategy(&self, tx: &TransactionRequest, hint: Option<bool>) -> Result<String, OptimizerError> {
        let use_flashbots = hint.unwrap_or_else(|| {
            self.is_mev_sensitive(tx) && self.flashbots_middleware.is_some()
        });
        
        if use_flashbots {
            Ok("flashbots".to_string())
        } else if self.is_mev_sensitive(tx) && !self.config.base_config.private_relays.is_empty() {
            Ok("private_relay".to_string())
        } else {
            Ok("public".to_string())
        }
    }
    
    async fn submit_via_strategy(&self, tx: TransactionRequest, strategy_name: &str) -> Result<H256, OptimizerError> {
        let strategies = self.submission_strategies.read().await;
        let strategy = strategies.get(strategy_name)
            .ok_or_else(|| OptimizerError::Config(format!("Strategy {} not found", strategy_name)))?;
        
        self.retry_with_backoff(
            || strategy.submit(tx.clone()),
            self.config.base_config.max_retry_attempts,
        ).await
    }
    
    async fn track_submission(&self, tx_hash: H256, tx: TransactionRequest, strategy: String) {
        let metadata = TransactionMetadata {
            tx,
            submission_time: Instant::now(),
            gas_price: TxGasPrice::Legacy { gas_price: U256::zero() },
            submission_method: match strategy.as_str() {
                "flashbots" => SubmissionStrategy::Flashbots,
                "private_relay" => SubmissionStrategy::PrivateRelay {
                    name: "default".to_string(),
                    endpoint: "".to_string(),
                },
                _ => SubmissionStrategy::PublicMempool,
            },
            retry_count: 0,
            nonce: U256::zero(),
            created_at: SystemTime::now(),
        };
        
        self.pending_transactions.insert(tx_hash, metadata).await;
    }
    
    fn is_mev_sensitive(&self, tx: &TransactionRequest) -> bool {
        if tx.data.as_ref().map_or(true, |data| data.is_empty()) && 
           tx.value.unwrap_or_default() > U256::zero() {
            return false;
        }
        tx.data.as_ref().map_or(false, |data| !data.is_empty())
    }
    
    async fn simulate_transaction(
        &self,
        tx: &TransactionRequest,
        block_number: Option<u64>,
    ) -> Result<(bool, Option<Bytes>), OptimizerError> {
        let block_id = block_number.map(|num| BlockId::from(num));
        
        match self.provider.call(tx, block_id).await {
            Ok(result_bytes) => Ok((true, Some(result_bytes))),
            Err(e) => {
                warn!("Transaction simulation failed: {}", e);
                Ok((false, None))
            }
        }
    }
    
    fn compute_simulation_cache_key(&self, route: &SwapRoute, params: &SwapParams) -> (H256, u64) {
        let data = format!("{:?}{:?}", route, params);
        let hash = H256::from_slice(&keccak256(data.as_bytes()));
        (hash, 0)
    }
    
    fn convert_swap_route_to_arbitrage(&self, route: &SwapRoute, params: &SwapParams) -> Result<ArbitrageRoute, OptimizerError> {
        if route.pools.is_empty() || route.path.is_empty() {
            return Err(OptimizerError::Config("Invalid swap route".to_string()));
        }
        
        let mut legs = Vec::new();
        
        for (i, pool_address) in route.pools.iter().enumerate() {
            let token_in = route.path.get(i).copied()
                .ok_or_else(|| OptimizerError::Config("Invalid path length".to_string()))?;
            let token_out = route.path.get(i + 1).copied()
                .ok_or_else(|| OptimizerError::Config("Invalid path length".to_string()))?;
            
            let amount_in = if i == 0 {
                params.amount_in
            } else {
                route.amounts.get(i).copied().unwrap_or_default()
            };
            
            let min_amount_out = if i == route.pools.len() - 1 {
                params.min_amount_out.unwrap_or_default()
            } else {
                U256::zero()
            };
            
            legs.push(crate::types::ArbitrageRouteLeg {
                dex_name: "UniswapV2".to_string(),
                protocol_type: crate::types::ProtocolType::UniswapV2,
                pool_address: *pool_address,
                token_in,
                token_out,
                amount_in,
                min_amount_out,
                fee_bps: 30,
                expected_out: None,
            });
        }
        
        Ok(ArbitrageRoute {
            legs,
            initial_amount_in_wei: params.amount_in,
            amount_out_wei: params.min_amount_out.unwrap_or_default(),
            profit_usd: 0.0,
            estimated_gas_cost_wei: U256::zero(),
            price_impact_bps: 0,
            route_description: "Swap route".to_string(),
            risk_score: 0.1,
            chain_id: self.provider.get_chain_id(),
            created_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            mev_risk_score: 0.5,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: route.path.first().copied().unwrap_or_default(),
            token_out: route.path.last().copied().unwrap_or_default(),
            amount_in: params.amount_in,
            amount_out: params.min_amount_out.unwrap_or_default(),
        })
    }
    
    async fn build_trade_transaction(
        &self,
        trade_id: &str,
        route: &ArbitrageRoute,
    ) -> Result<TransactionRequest, OptimizerError> {
        debug!(
            target: "transaction_optimizer::build_trade_transaction",
            trade_id,
            route_legs = route.legs.len(),
            "Building arbitrage execution transaction"
        );
        
        let min_profit_wei = route.estimated_gas_cost_wei.saturating_mul(U256::from(2));
        let mut tx = self.encode_arbitrage_transaction(route, min_profit_wei).await?;
        tx.from = Some(self.provider.get_wallet().address());
        
        Ok(tx)
    }
    
    async fn encode_arbitrage_transaction(
        &self,
        route: &ArbitrageRoute,
        min_profit_wei: U256,
    ) -> Result<TransactionRequest, OptimizerError> {
        let calldata = self.encode_arbitrage_calldata(route, min_profit_wei).await?;
        
        let tx = TransactionRequest {
            from: Some(self.provider.get_wallet().address()),
            to: Some(NameOrAddress::Address(self.arbitrage_executor_address)),
            value: Some(U256::zero()),
            data: Some(calldata),
            gas: None,
            gas_price: None,
            nonce: None,
            chain_id: Some(self.provider.get_chain_id().into()),
            ..Default::default()
        };
        
        Ok(tx)
    }
    
    async fn encode_arbitrage_calldata(
        &self,
        route: &ArbitrageRoute,
        min_profit_wei: U256,
    ) -> Result<Bytes, OptimizerError> {
        let legs_tokens: Vec<Token> = route.legs.iter().map(|leg| {
            Token::Tuple(vec![
                Token::Address(leg.pool_address),
                Token::Address(leg.token_in),
                Token::Address(leg.token_out),
                Token::Uint(leg.amount_in),
                Token::Uint(leg.min_amount_out),
                Token::Uint(U256::from(leg.fee_bps)),
                Token::Uint(U256::from(match leg.protocol_type {
                    crate::types::ProtocolType::UniswapV2 => 0u8,
                    crate::types::ProtocolType::UniswapV3 => 1u8,
                    crate::types::ProtocolType::UniswapV4 => 2u8,
                    crate::types::ProtocolType::SushiSwap => 3u8,
                    crate::types::ProtocolType::Curve => 4u8,
                    crate::types::ProtocolType::Balancer => 5u8,
                    crate::types::ProtocolType::Other(_) => 255u8,
                })),
            ])
        }).collect();
        
        let function_params = vec![
            Token::Array(legs_tokens),
            Token::Uint(route.initial_amount_in_wei),
            Token::Uint(min_profit_wei),
            Token::Address(route.legs.first()
                .ok_or_else(|| OptimizerError::Config("Route has no legs".to_string()))?.token_in),
            Token::Address(route.legs.last()
                .ok_or_else(|| OptimizerError::Config("Route has no legs".to_string()))?.token_out),
        ];
        
        let calldata = encode(&function_params);
        let selector = &keccak256(
            "executeArbitrage((address,address,address,uint256,uint256,uint16,uint8)[],uint256,uint256,address,address)"
                .as_bytes()
        )[0..4];
        
        let mut data = selector.to_vec();
        data.extend_from_slice(&calldata);
        
        Ok(Bytes::from(data))
    }
    
    fn u256_to_f64(&self, val: U256, decimals: u8) -> f64 {
        if let Some(x) = val.try_into().ok().map(|x: u128| x) {
            return (x as f64) / 10f64.powi(decimals as i32);
        }
        let s = val.to_string();
        s.parse::<f64>().unwrap_or(0.0) / 10f64.powi(decimals as i32)
    }
}

// ======================== Builder Pattern ========================

pub struct TransactionBuilder {
    request: TransactionRequest,
    optimizer: Arc<TransactionOptimizerV2>,
    mev_protection: bool,
    simulation_required: bool,
}

impl TransactionBuilder {
    pub fn new(optimizer: Arc<TransactionOptimizerV2>) -> Self {
        Self {
            request: TransactionRequest::default(),
            optimizer,
            mev_protection: false,
            simulation_required: false,
        }
    }
    
    pub fn to(mut self, address: Address) -> Self {
        self.request.to = Some(NameOrAddress::Address(address));
        self
    }
    
    pub fn value(mut self, amount: U256) -> Self {
        self.request.value = Some(amount);
        self
    }
    
    pub fn data(mut self, data: Bytes) -> Self {
        self.request.data = Some(data);
        self
    }
    
    pub fn gas(mut self, gas: U256) -> Self {
        self.request.gas = Some(gas);
        self
    }
    
    pub fn with_mev_protection(mut self) -> Self {
        self.mev_protection = true;
        self
    }
    
    pub fn require_simulation(mut self) -> Self {
        self.simulation_required = true;
        self
    }
    
    pub async fn submit(self) -> Result<H256, OptimizerError> {
        if self.simulation_required {
            let (success, _) = self.optimizer.simulate_transaction(&self.request, None).await?;
            if !success {
                return Err(OptimizerError::Simulation("Pre-submission simulation failed".to_string()));
            }
        }
        
        self.optimizer.auto_optimize_and_submit(self.request, Some(self.mev_protection)).await
    }
}

// ======================== As Any for Strategy Downcasting ========================

trait AsAny {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl AsAny for dyn SubmissionStrategyTrait {
    fn as_any(&self) -> &dyn std::any::Any {
        self as &dyn std::any::Any
    }
}

// ======================== TransactionOptimizerTrait Implementation ========================

#[async_trait]
impl TransactionOptimizerTrait for TransactionOptimizerV2 {
    async fn submit_transaction(
        &self,
        tx: TypedTransaction,
        submission_strategy: SubmissionStrategy,
    ) -> Result<H256, OptimizerError> {
        // Convert TypedTransaction to TransactionRequest
        let tx_request = match tx {
            TypedTransaction::Legacy(req) => req,
            TypedTransaction::Eip2930(req) => req.tx,
            TypedTransaction::Eip1559(req) => req.tx,
        };
        
        // Use the appropriate submission method based on strategy
        match submission_strategy {
            SubmissionStrategy::Standard => {
                self.provider.submit_transaction(tx_request).await
                    .map_err(|e| OptimizerError::Provider(e.to_string()))
            }
            SubmissionStrategy::Flashbots => {
                // Create flashbots strategy and submit
                let flashbots_strategy = FlashbotsStrategy {
                    middleware: self.flashbots_middleware.clone()
                        .ok_or_else(|| OptimizerError::Config("Flashbots not configured".to_string()))?,
                    provider: self.provider.clone(),
                    nonce_manager: self.nonce_manager.clone(),
                    block_offset: 1,
                };
                flashbots_strategy.submit(tx_request).await
            }
            SubmissionStrategy::Private => {
                // Submit via private mempool
                self.provider.submit_transaction_private(tx_request).await
                    .map_err(|e| OptimizerError::Provider(e.to_string()))
            }
            SubmissionStrategy::GasEscalation => {
                // Submit with gas escalation
                self.submit_with_gas_escalation(tx_request).await
            }
            SubmissionStrategy::BundleWithBackrun => {
                // This requires multiple transactions, so we'll just submit normally
                self.provider.submit_transaction(tx_request).await
                    .map_err(|e| OptimizerError::Provider(e.to_string()))
            }
        }
    }
    
    async fn submit_bundle(
        &self,
        txs: Vec<TypedTransaction>,
        target_block: Option<u64>,
    ) -> Result<BundleStatus, OptimizerError> {
        if txs.is_empty() {
            return Err(OptimizerError::InvalidInput("Empty bundle".to_string()));
        }
        
        let flashbots_middleware = self.flashbots_middleware.clone()
            .ok_or_else(|| OptimizerError::Config("Flashbots not configured".to_string()))?;
        
        let mut bundle_txs = Vec::new();
        for tx in txs {
            let tx_request = match tx {
                TypedTransaction::Legacy(req) => req,
                TypedTransaction::Eip2930(req) => req.tx,
                TypedTransaction::Eip1559(req) => req.tx,
            };
            bundle_txs.push(tx_request);
        }
        
        let block_number = self.provider.get_provider().get_block_number().await
            .map_err(|e| OptimizerError::Provider(e.to_string()))?;
        
        let target_block = target_block.unwrap_or(block_number.as_u64() + 1);
        
        // Create and submit bundle
        let bundle = flashbots_middleware
            .send_bundle(&bundle_txs)
            .block(target_block.into())
            .await
            .map_err(|e| OptimizerError::Flashbots(e.to_string()))?;
        
        Ok(BundleStatus::Submitted { 
            bundle_hash: format!("{:?}", bundle),
        })
    }
    
    fn get_wallet_address(&self) -> Address {
        self.provider.get_wallet().address()
    }
    
    async fn simulate_trade_submission(
        &self,
        route: &crate::types::ArbitrageRoute,
        block_number: u64,
    ) -> Result<(U256, f64), OptimizerError> {
        // Estimate gas for the route
        let gas_estimate = self.estimate_gas_for_route(route, block_number).await?;
        
        // Get gas price at the block
        let gas_price = self.gas_oracle.get_gas_price_for_block(
            &route.chain_name,
            block_number,
        ).await
        .map_err(|e| OptimizerError::GasEstimation(format!("Failed to get gas price: {}", e)))?;
        
        // Calculate gas cost in USD
        let gas_cost_wei = gas_estimate.saturating_mul(gas_price.base_fee);
        let gas_cost_usd = self.price_oracle.get_eth_price_usd(block_number).await
            .map_err(|e| OptimizerError::Provider(format!("Failed to get ETH price: {}", e)))?
            * (gas_cost_wei.as_u128() as f64 / 1e18);
        
        Ok((gas_estimate, gas_cost_usd))
    }
    
    async fn estimate_gas_for_route(
        &self,
        route: &crate::types::ArbitrageRoute,
        block_number: u64,
    ) -> Result<U256, OptimizerError> {
        // Check cache first
        let cache_key = (self.hash_route(route), block_number);
        {
            let cache = self.simulation_cache.lock().await;
            if let Some(cached_result) = cache.get(&cache_key) {
                return Ok(cached_result.gas_used);
            }
        }
        
        // Base gas costs for different operations
        let mut total_gas = U256::from(21000); // Base transaction cost
        
        // Add gas for each swap leg
        for leg in &route.legs {
            let gas_per_swap = match leg.protocol {
                crate::types::ProtocolType::UniswapV2 | 
                crate::types::ProtocolType::SushiSwap |
                crate::types::ProtocolType::PancakeSwap => U256::from(110000),
                crate::types::ProtocolType::UniswapV3 => U256::from(150000),
                crate::types::ProtocolType::Curve => U256::from(180000),
                crate::types::ProtocolType::Balancer |
                crate::types::ProtocolType::BalancerV2 => U256::from(140000),
                _ => U256::from(120000), // Default estimate
            };
            total_gas = total_gas.saturating_add(gas_per_swap);
        }
        
        // Add buffer for complex routing
        if route.legs.len() > 2 {
            total_gas = total_gas.saturating_mul(U256::from(110)).checked_div(U256::from(100))
                .unwrap_or(total_gas);
        }
        
        // Cache the result
        {
            let mut cache = self.simulation_cache.lock().await;
            cache.put(
                cache_key,
                SimulationResult {
                    gas_used: total_gas,
                    success: true,
                    revert_reason: None,
                    timestamp: std::time::SystemTime::now(),
                },
            );
        }
        
        Ok(total_gas)
    }
    
    fn hash_route(&self, route: &crate::types::ArbitrageRoute) -> H256 {
        let mut data = Vec::new();
        data.extend_from_slice(route.chain_name.as_bytes());
        for leg in &route.legs {
            data.extend_from_slice(leg.from_token.as_bytes());
            data.extend_from_slice(leg.to_token.as_bytes());
            data.extend_from_slice(&leg.pool.as_bytes());
        }
        H256::from_slice(&ethers::utils::keccak256(&data))
    }
}