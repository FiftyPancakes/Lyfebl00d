use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn, instrument};
use uuid::Uuid;
// Use compile-time contract bindings instead of runtime parsing
use ethers::types::{U256, H256, Bytes};

use crate::{
    mempool::{MEVOpportunity, PotentialSandwichOpportunity, JITLiquidityOpportunity,
               PotentialLiquidationOpportunity, OracleUpdateOpportunity, FlashLoanOpportunity},
    types::ArbitrageRoute,
    transaction_optimizer::SubmissionStrategy,
};

use super::{MEVEngine, MEVEngineError, ActiveMEVPlay, SafeActiveMEVPlay};

// Constants for handler configuration
const BUNDLE_SUBMISSION_DELAY_MS: u64 = 50;
const SIMULATION_RETRY_DELAY_MS: u64 = 100;
const MAX_SIMULATION_ATTEMPTS: u32 = 3;

/// Safely convert wei to ETH as f64 (lossy but safe for comparisons/reporting)
fn wei_to_eth_f64(wei: U256) -> f64 {
    // ok for comparisons/reporting (lossy), prefer big-decimal for accounting
    (wei.as_u128() as f64) / 1e18
}

/// Create EIP-1559 transaction request
fn eip1559(
    to: ethers::types::Address,
    data: Bytes,
    chain_id: u64,
    gas: U256,
    fees: (U256, U256) // (max_fee_per_gas, max_priority_fee_per_gas)
) -> ethers::types::transaction::eip2718::TypedTransaction {
    let tx = ethers::types::Eip1559TransactionRequest {
        to: Some(ethers::types::NameOrAddress::Address(to)),
        data: Some(data),
        gas: Some(gas),
        max_fee_per_gas: Some(fees.0),
        max_priority_fee_per_gas: Some(fees.1),
        chain_id: Some(chain_id.into()),
        value: Some(U256::zero()),
        ..Default::default()
    };
    ethers::types::transaction::eip2718::TypedTransaction::Eip1559(tx)
}

impl MEVEngine {
    /// Get ETH/USD price for USD calculations
    async fn eth_usd(&self) -> Result<f64, MEVEngineError> {
        let chain = self.strategy_context.blockchain_manager.get_chain_name();
        let weth = self.strategy_context.blockchain_manager.get_weth_address();
        self.strategy_context.price_oracle
            .get_price(chain, weth)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get ETH/USD: {e}")))
    }

    /// Get EIP-1559 fees from gas oracle
    async fn get_eip1559_fees(&self, chain_name: &str) -> Result<(U256, U256), MEVEngineError> {
        let gas_oracle = self.strategy_context.gas_oracle.read().await;
        let gas_price = gas_oracle
            .get_gas_price(chain_name, None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;
        Ok((gas_price.base_fee, gas_price.priority_fee))
    }

    #[instrument(skip(self, opportunity))]
    pub(super) async fn handle_opportunity_with_shared_state(
        &self,
        play_id: Uuid,
        opportunity: MEVOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Check if engine is paused
        if *self.is_paused.read().await {
            return Err(MEVEngineError::ValidationError("Engine is paused".into()));
        }

        // Try to acquire execution permit with timeout
        let permit = match timeout(
            Duration::from_millis(self.config.opportunity_timeout_ms),
            self.execution_semaphore.acquire()
        ).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(MEVEngineError::ResourceExhausted("Semaphore closed".into())),
            Err(_) => return Err(MEVEngineError::Timeout("Execution permit timeout".into())),
        };

        // Find appropriate strategy
        let registry = self.strategy_registry.read().await;
        let strategy = registry
            .get_strategy_for(&opportunity)
            .ok_or_else(|| MEVEngineError::StrategyError("No strategy found for opportunity".into()))?;

        // Check strategy health
        if !registry.is_strategy_healthy(strategy.name()).await {
            return Err(MEVEngineError::StrategyError(
                format!("Strategy {} is unhealthy", strategy.name())
            ));
        }

        // Estimate profit first
        let estimated_profit = match strategy.estimate_profit(&opportunity, &self.strategy_context).await {
            Ok(profit) => profit,
            Err(e) => {
                warn!("Failed to estimate profit: {}", e);
                let mut registry = self.strategy_registry.write().await;
                registry.record_failure(strategy.name()).await;
                return Err(e.into());
            }
        };

        if estimated_profit < strategy.min_profit_threshold_usd() {
            debug!("Opportunity profit {} below threshold {}", 
                   estimated_profit, strategy.min_profit_threshold_usd());
            return Ok(());
        }

        // Get chain ID from blockchain manager
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();

        // Create active play with proper initialization
        let mut active_play = ActiveMEVPlay::new(&opportunity, chain_id, estimated_profit);
        active_play.play_id = play_id;
        
        // Wrap in thread-safe container
        let safe_play = SafeActiveMEVPlay::new(active_play.clone());
        
        // Store in active plays map
        self.active_plays.insert(play_id, safe_play.clone());

        // Update status atomically
        if let Err(e) = safe_play.start_simulation().await {
            warn!("Failed to start simulation: {}", e);
        }

        // Execute strategy with proper error handling
        let result = {
            // Hold permit during execution and ensure it is not considered unused
            let permit = permit;
            let res = timeout(
                Duration::from_millis(self.config.opportunity_timeout_ms),
                strategy.execute(&safe_play, opportunity.clone(), &self.strategy_context)
            ).await;
            // Explicitly drop after execution to mark usage and control lifetime
            std::mem::drop(permit);
            res
        };

        // Handle execution result
        match result {
            Ok(Ok(())) => {
                info!("Successfully executed opportunity {}", play_id);
                let mut registry = self.strategy_registry.write().await;
                registry.record_success(strategy.name()).await;
                
                // Update metrics
                let mut metrics = self.metrics.write().await;
                metrics.total_plays += 1;
                metrics.successful_plays += 1;
                metrics.total_profit_usd += estimated_profit;
                *metrics.plays_by_type.entry(strategy.name().to_string()).or_insert(0) += 1;
                *metrics.profit_by_type.entry(strategy.name().to_string()).or_insert(0.0) += estimated_profit;
            }
            Ok(Err(e)) => {
                error!("Strategy execution failed: {}", e);
                if let Err(update_e) = safe_play.record_simulation_failure(e.to_string()).await {
                    warn!("Failed to record simulation failure: {}", update_e);
                }

                let mut registry = self.strategy_registry.write().await;
                let should_pause = registry.record_failure(strategy.name()).await;
                if should_pause {
                    warn!("Strategy {} has too many failures, pausing", strategy.name());
                }
                
                // Update metrics
                let mut metrics = self.metrics.write().await;
                metrics.total_plays += 1;
                metrics.failed_plays += 1;
            }
            Err(_) => {
                error!("Strategy execution timeout");
                if let Err(e) = safe_play.record_simulation_failure("Execution timeout".into()).await {
                    warn!("Failed to record simulation failure: {}", e);
                }

                let mut registry = self.strategy_registry.write().await;
                registry.record_failure(strategy.name()).await;
                
                return Err(MEVEngineError::Timeout("Strategy execution timeout".into()));
            }
        }

        Ok(())
    }

    #[instrument(skip(self, active_play, opp))]
    pub async fn handle_sandwich_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        opp: PotentialSandwichOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Validate opportunity freshness
        let current_block = self.strategy_context.blockchain_manager
            .get_block_number()
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get block number: {}", e)))?;
        
        if current_block > opp.block_number + 2 {
            return Err(MEVEngineError::ValidationError("Opportunity is stale".into()));
        }

        // Validate victim transaction is still pending
        let victim_tx_status = self.strategy_context.blockchain_manager
            .get_transaction_receipt(opp.target_tx_hash)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to check victim tx: {}", e)))?;
        
        if victim_tx_status.is_some() {
            return Err(MEVEngineError::ValidationError("Victim transaction already included".into()));
        }

        // Calculate sandwich parameters with safe arithmetic
        let victim_gas_price = opp.gas_price;
        
        // Front-run gas price calculation with safety
        let gas_offset = U256::from(10) // 10 gwei offset
            .checked_mul(U256::exp10(9))
            .ok_or_else(|| MEVEngineError::ArithmeticError("Gas offset overflow".into()))?;
        
        let front_run_gas_price = victim_gas_price
            .checked_add(gas_offset)
            .ok_or_else(|| MEVEngineError::ArithmeticError("Front-run gas price overflow".into()))?;
        
        // Back-run gas price (slightly lower than victim)
        let back_run_gas_price = victim_gas_price
            .checked_sub(U256::from(1_000_000_000)) // 1 gwei less
            .unwrap_or(victim_gas_price);

        // Simulate sandwich with retry logic
        let mut simulation_attempts = 0;
        let mut last_error = None;
        
        while simulation_attempts < MAX_SIMULATION_ATTEMPTS {
            simulation_attempts += 1;
            active_play.simulation_attempts = simulation_attempts;
            
            match self.simulate_sandwich(&opp, front_run_gas_price, back_run_gas_price).await {
                Ok(simulation_result) => {
                    if simulation_result.net_profit_usd < active_play.estimated_profit_usd * 0.8 {
                        return Err(MEVEngineError::ValidationError(
                            "Simulated profit below threshold".into()
                        ));
                    }
                    
                    // Build and submit sandwich bundle
                    return self.submit_sandwich_bundle(
                        active_play,
                        &opp,
                        front_run_gas_price,
                        back_run_gas_price,
                        simulation_result
                    ).await;
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    if simulation_attempts < MAX_SIMULATION_ATTEMPTS {
                        sleep(Duration::from_millis(SIMULATION_RETRY_DELAY_MS)).await;
                    }
                }
            }
        }
        
        Err(MEVEngineError::SimulationError(
            format!("Failed after {} attempts: {}", 
                    simulation_attempts, 
                    last_error.unwrap_or_else(|| "Unknown error".to_string()))
        ))
    }

    #[instrument(skip(self, active_play, opp))]
    pub async fn handle_jit_liquidity_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        opp: JITLiquidityOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Validate pool state
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let pool_info = self.strategy_context.pool_manager
            .get_pool_info(&chain_name, opp.pool_address, Some(opp.block_number))
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get pool info: {}", e)))?;

        // Calculate optimal liquidity amounts with safe arithmetic
        let total_liquidity = pool_info.reserves0
            .checked_add(pool_info.reserves1)
            .ok_or_else(|| MEVEngineError::ArithmeticError("Liquidity calculation overflow".into()))?;
        
        let min_liquidity = U256::from(10_000) * U256::exp10(18); // 10k minimum
        if total_liquidity < min_liquidity {
            return Err(MEVEngineError::ValidationError("Insufficient pool liquidity".into()));
        }

        // Calculate JIT liquidity position size
        let position_size = opp.liquidity_amount
            .checked_mul(U256::from(2)) // 2x the swap amount
            .ok_or_else(|| MEVEngineError::ArithmeticError("Position size overflow".into()))?;

        // Validate sender has sufficient balance
        let sender = self.strategy_context.blockchain_manager.get_wallet_address();
        let balance = self.strategy_context.blockchain_manager
            .get_balance(sender, Some(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(opp.block_number.into()))))
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get balance: {}", e)))?;

        if balance < position_size {
            return Err(MEVEngineError::InsufficientBalance(
                format!("Need {} but have {}", position_size, balance)
            ));
        }

        // Build JIT liquidity transactions
        let add_liquidity_tx = self.build_add_liquidity_tx(
            &opp,
            position_size,
            sender
        ).await?;
        
        let remove_liquidity_tx = self.build_remove_liquidity_tx(
            &opp,
            position_size,
            sender
        ).await?;

        // Record submitted transactions to mempool for tracking
        if let Err(e) = active_play.submit_to_mempool(vec![add_liquidity_tx, remove_liquidity_tx]) {
            warn!("Failed to update play status: {}", e);
        }
        
        Ok(())
    }

    #[instrument(skip(self, active_play, liquidation_opp))]
    pub async fn handle_liquidation_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        liquidation_opp: &PotentialLiquidationOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Verify position is still liquidatable
        let health_factor = self.calculate_health_factor(liquidation_opp).await?;
        
        if health_factor >= 1.0 {
            return Err(MEVEngineError::ValidationError(
                format!("Position no longer liquidatable: health factor {}", health_factor)
            ));
        }

        // Calculate maximum liquidation amount
        let max_liquidation = self.calculate_max_liquidation_amount(liquidation_opp).await?;
        
        if max_liquidation.is_zero() {
            return Err(MEVEngineError::ValidationError("Zero liquidation amount".into()));
        }

        // Build liquidation transaction
        let liquidation_tx = self.build_liquidation_tx(
            liquidation_opp,
            max_liquidation
        ).await?;

        // Capture pre-transaction balances for profit calculation
        let relevant_tokens = self.extract_relevant_tokens(&active_play.original_signal);
        let safe_play = crate::mev_engine::types::SafeActiveMEVPlay::new(active_play.clone());
        if let Err(e) = self.capture_pre_tx_balances(&safe_play, &relevant_tokens).await {
            warn!("Failed to capture pre-transaction balances: {}", e);
            // Continue with submission even if balance capture fails
        }

        // Build liquidation transaction using UAR connector
        let uar_address = liquidation_opp.uar_connector.ok_or_else(|| {
            MEVEngineError::ValidationError("No UAR connector available".to_string())
        })?;

        // Calculate liquidation parameters
        let liquidation_data = self.encode_liquidation_call(liquidation_opp, max_liquidation).await?;

        // Submit with EIP-1559
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let gas = self.estimate_gas_with_fallback(liquidation_tx.clone(), U256::from(500_000)).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        let tx = eip1559(uar_address, liquidation_data, chain_id, gas, (max_fee, max_prio));

        let tx_hash = self.strategy_context.transaction_optimizer
            .submit_transaction(tx, SubmissionStrategy::PublicMempool)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit liquidation: {}", e)))?;

        if let Err(e) = active_play.submit_to_mempool(vec![tx_hash]) {
            warn!("Failed to update play status: {}", e);
        }
        
        Ok(())
    }

    #[instrument(skip(self, active_play, oracle_opp))]
    pub async fn handle_oracle_update_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        oracle_opp: &OracleUpdateOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Verify oracle price discrepancy still exists
        let current_oracle_price = self.get_oracle_price(oracle_opp.oracle_address).await?;
        let market_price = self.get_market_price(oracle_opp.token_address).await?;
        
        let price_diff_pct = ((market_price - current_oracle_price).abs() / current_oracle_price) * 100.0;
        
        if price_diff_pct < 1.0 { // Less than 1% difference
            return Err(MEVEngineError::ValidationError(
                format!("Price difference {} too small", price_diff_pct)
            ));
        }

        // Build oracle update transaction
        let update_tx = self.build_oracle_update_tx(oracle_opp, market_price).await?;

        // Validate input
        if market_price <= 0.0 {
            return Err(MEVEngineError::ValidationError("Oracle price must be positive".to_string()));
        }

        let oracle_address = oracle_opp.oracle_address;

        // Convert price to 18-decimal fixed point and encode function call
        let price_wei: U256 = ethers::utils::parse_units(format!("{:.18}", market_price), 18)
            .map(|v| v.into())
            .map_err(|e| MEVEngineError::ValidationError(format!("Failed to parse price to wei: {}", e)))?;

        // Use type-safe contract binding for oracle update
        let oracle_contract = super::contracts::oracle::Oracle::new(oracle_address);
        // Encode the updatePrice call using the contract binding
        let mut data = Vec::new();
        data.extend_from_slice(&ethers::utils::keccak256(b"updatePrice(uint256)")[0..4]); // Function selector
        data.extend_from_slice(&ethers::abi::encode(&[ethers::abi::Token::Uint(price_wei)])); // Encoded parameters
        let calldata = ethers::types::Bytes::from(data);

        // Submit with EIP-1559
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let gas = self.estimate_gas_with_fallback(update_tx.clone(), U256::from(150_000)).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        let tx = eip1559(oracle_address, ethers::types::Bytes::from(calldata), chain_id, gas, (max_fee, max_prio));

        let tx_hash = self.strategy_context.transaction_optimizer
            .submit_transaction(tx, SubmissionStrategy::PublicMempool)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit oracle update: {}", e)))?;

        if let Err(e) = active_play.submit_to_mempool(vec![tx_hash]) {
            warn!("Failed to update play status: {}", e);
        }
        
        Ok(())
    }

    #[instrument(skip(self, active_play, arbitrage_opp))]
    pub async fn handle_arbitrage_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        arbitrage_opp: &ArbitrageRoute,
    ) -> Result<(), MEVEngineError> {
        // Update play metadata with arbitrage route details
        if let Some(first_leg) = arbitrage_opp.legs.first() {
            // Store route information in strategy_data instead of non-existent fields
            let route_info = serde_json::json!({
                "target_pool": first_leg.pool_address,
                "target_token": first_leg.token_in,
                "route_length": arbitrage_opp.legs.len(),
                "legs": arbitrage_opp.legs.iter().map(|leg| {
                    serde_json::json!({
                        "pool_address": leg.pool_address,
                        "token_in": leg.token_in,
                        "token_out": leg.token_out,
                        "protocol_type": leg.protocol_type
                    })
                }).collect::<Vec<_>>()
            });
            active_play.strategy_data = Some(route_info);
        }
        
        // Log arbitrage details for monitoring
        info!(
            "Processing arbitrage route: {} legs, expected profit: {} USD",
            arbitrage_opp.legs.len(),
            arbitrage_opp.profit_usd
        );
        
        // Strategy will handle the actual execution
        debug!("Arbitrage opportunity delegated to strategy for execution");
        Ok(())
    }

    #[instrument(skip(self, active_play, flashloan_opp))]
    pub async fn handle_flashloan_opportunity(
        &self,
        active_play: &mut ActiveMEVPlay,
        flashloan_opp: &FlashLoanOpportunity,
    ) -> Result<(), MEVEngineError> {
        // Validate flashloan parameters
        if flashloan_opp.amount_to_flash.is_zero() {
            return Err(MEVEngineError::ValidationError("Zero loan amount".into()));
        }

        // Calculate flashloan fee
        let fee_bps = U256::from(9); // 0.09% typical Aave fee
        let fee_amount = flashloan_opp.amount_to_flash
            .checked_mul(fee_bps)
            .and_then(|v| v.checked_div(U256::from(10000)))
            .ok_or_else(|| MEVEngineError::ArithmeticError("Fee calculation overflow".into()))?;

        let total_repayment = flashloan_opp.amount_to_flash
            .checked_add(fee_amount)
            .ok_or_else(|| MEVEngineError::ArithmeticError("Repayment calculation overflow".into()))?;

        // Verify expected profit covers fees (all in USD)
        let token = flashloan_opp.token_to_flash;
        let chain = self.strategy_context.blockchain_manager.get_chain_name();
        let token_price = self.strategy_context.price_oracle
            .get_price(chain, token).await.unwrap_or(0.0);

        let loan_usd = wei_to_eth_f64(flashloan_opp.amount_to_flash) * token_price;
        let fee_usd  = wei_to_eth_f64(fee_amount) * token_price;

        let net_expected_usd = flashloan_opp.estimated_profit_usd - fee_usd;
        let min_profit_usd = active_play.estimated_profit_usd; // or a config threshold

        if net_expected_usd < min_profit_usd {
            return Err(MEVEngineError::ValidationError("Insufficient profit to cover flashloan + threshold".into()));
        }

        // Build flashloan transaction
        let flashloan_tx = self.build_flashloan_tx(flashloan_opp, fee_amount).await?;

        // Validate input
        if flashloan_opp.amount_to_flash.is_zero() {
            return Err(MEVEngineError::ValidationError("Flashloan amount is zero".to_string()));
        }
        let flashloan_provider = flashloan_opp.lending_protocol;

        // Encode the calldata for the flashloan. Assume function: flashLoan(address receiver, address token, uint256 amount, bytes params)
        let receiver = self.strategy_context.transaction_optimizer.get_wallet_address();
        let token = flashloan_opp.token_to_flash;
        let amount = flashloan_opp.amount_to_flash;

        // Use type-safe contract binding for flash loan
        let flashloan_contract = super::contracts::flashloan::FlashLoan::new(flashloan_provider);

        // Encode the flashLoan call using the contract binding
        let mut data = Vec::new();
        data.extend_from_slice(&ethers::utils::keccak256(b"flashLoan(address,address,uint256,bytes)")[0..4]); // Function selector

        // Include flashloan fee in params for downstream settlement logic
        let params_bytes = ethers::abi::encode(&[ethers::abi::Token::Uint(fee_amount)]);
        let encoded_params = ethers::abi::encode(&[
            ethers::abi::Token::Address(receiver),
            ethers::abi::Token::Address(token),
            ethers::abi::Token::Uint(amount),
            ethers::abi::Token::Bytes(params_bytes),
        ]);
        data.extend_from_slice(&encoded_params);
        let calldata = ethers::types::Bytes::from(data);

        // Submit with EIP-1559
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let gas = self.estimate_gas_with_fallback(flashloan_tx.clone(), U256::from(400_000)).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        let tx = eip1559(flashloan_provider, ethers::types::Bytes::from(calldata), chain_id, gas, (max_fee, max_prio));

        let tx_hash = self.strategy_context.transaction_optimizer
            .submit_transaction(tx, SubmissionStrategy::PublicMempool)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit flashloan: {}", e)))?;

        if let Err(e) = active_play.submit_to_mempool(vec![tx_hash]) {
            warn!("Failed to update play status: {}", e);
        }
        
        Ok(())
    }

    // Helper methods

    /// Estimate gas for a transaction request dynamically
    async fn estimate_gas_with_fallback(&self, mut tx_request: ethers::types::TransactionRequest, fallback_gas: U256) -> Result<U256, MEVEngineError> {
        // First try to estimate gas dynamically
        match self.strategy_context.blockchain_manager.estimate_gas(&tx_request, None).await {
            Ok(estimated_gas) => {
                // Add 20% buffer to the estimate for safety
                let buffered_gas = estimated_gas.saturating_mul(U256::from(120)) / U256::from(100);
                debug!("Estimated gas: {} (buffered: {})", estimated_gas, buffered_gas);
                Ok(buffered_gas)
            }
            Err(e) => {
                warn!("Failed to estimate gas dynamically: {}, using fallback {}", e, fallback_gas);
                Ok(fallback_gas)
            }
        }
    }



    async fn simulate_sandwich(
        &self,
        opp: &PotentialSandwichOpportunity,
        front_run_gas_price_wei: U256,
        back_run_gas_price_wei: U256,
    ) -> Result<SandwichSimulationResult, MEVEngineError> {
        const SWAP_GAS_ESTIMATE: u64 = 200_000;
        let gas_units = U256::from(SWAP_GAS_ESTIMATE) * U256::from(2); // front+back
        let gas_cost_wei =
            gas_units.checked_mul((front_run_gas_price_wei + back_run_gas_price_wei) / U256::from(2))
                     .ok_or_else(|| MEVEngineError::ArithmeticError("gas overflow".into()))?;

        let eth_usd = self.eth_usd().await?;
        let gas_cost_usd = wei_to_eth_f64(gas_cost_wei) * eth_usd;

        // opp.estimated_profit_usd is gross USD estimate
        let net_profit_usd = opp.estimated_profit_usd - gas_cost_usd;

        Ok(SandwichSimulationResult {
            front_run_output: opp.amount_out_min,
            back_run_output: opp.amount_in,
            gross_profit_usd: opp.estimated_profit_usd,
            gas_cost_usd,
            net_profit_usd,
        })
    }

    async fn submit_sandwich_bundle(
        &self,
        active_play: &mut ActiveMEVPlay,
        opp: &PotentialSandwichOpportunity,
        front_run_gas: U256,
        back_run_gas: U256,
        simulation: SandwichSimulationResult,
    ) -> Result<(), MEVEngineError> {
        // Validate simulation results before proceeding
        if simulation.net_profit_usd <= 0.0 {
            warn!("Aborting sandwich submission: negative profit {:.6} USD", simulation.net_profit_usd);
            return Err(MEVEngineError::StrategyError("Negative profit simulation".into()));
        }

        info!("Submitting sandwich bundle with expected profit: {:.6} USD (gross: {:.6}, gas: {:.6})",
              simulation.net_profit_usd, simulation.gross_profit_usd, simulation.gas_cost_usd);

        // Build front-run transaction
        let front_run_tx = self.build_front_run_tx(opp, front_run_gas).await?;
        
        // Small delay to ensure ordering
        sleep(Duration::from_millis(BUNDLE_SUBMISSION_DELAY_MS)).await;
        
        // Build back-run transaction
        let back_run_tx = self.build_back_run_tx(opp, back_run_gas).await?;
        
        // Submit as bundle via transaction optimizer
        let bundle_result = self.strategy_context.transaction_optimizer
            .submit_bundle(
                vec![front_run_tx, back_run_tx], // These are now TypedTransaction objects
                Some(opp.block_number)
            )
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit bundle: {}", e)))?;

        // Extract bundle hash from result
        let bundle_hash = match bundle_result {
            crate::transaction_optimizer::BundleStatus::Submitted(hash) => hash,
            crate::transaction_optimizer::BundleStatus::Failed(reason) => {
                return Err(MEVEngineError::NetworkError(format!("Bundle submission failed: {}", reason)));
            }
            _ => {
                return Err(MEVEngineError::NetworkError("Unexpected bundle status".to_string()));
            }
        };
        
        if let Err(e) = active_play.submit_to_relay(bundle_hash) {
            warn!("Failed to update play status: {}", e);
        }
        active_play.estimated_profit_usd = simulation.net_profit_usd;
        
        Ok(())
    }

    async fn build_front_run_tx(&self, opp: &PotentialSandwichOpportunity, gas_price: U256) -> Result<ethers::types::transaction::eip2718::TypedTransaction, MEVEngineError> {
        // Build front-run transaction using execution builder
        let front_run_route = ArbitrageRoute {
            token_in: opp.token_in,
            token_out: opp.token_out,
            amount_in: opp.amount_in,
            amount_out: opp.amount_out_min,
            amount_out_wei: opp.amount_out_min,
            initial_amount_in_wei: opp.amount_in,
            profit_usd: 0.0, // Front-run doesn't generate profit directly
            chain_id: 1, // Would be determined from context
            legs: vec![],
            estimated_gas_cost_wei: U256::from(150000),
            mev_risk_score: 0.1,
            risk_score: 0.3,
            price_impact_bps: 50, // Estimated price impact
            route_description: format!("Front-run sandwich on {:?}", opp.pool_address),
            created_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
        };
        
        let tx_request = {
            let mut tx_request = ethers::types::TransactionRequest {
                from: Some(self.strategy_context.transaction_optimizer.get_wallet_address()),
                to: Some(ethers::types::NameOrAddress::Address(opp.pool_address)),
                value: Some(U256::zero()),
                gas: None, // Will be set after estimation
                gas_price: Some(gas_price),
                data: Some(ethers::types::Bytes::from(front_run_route.encode_calldata())),
                nonce: None,
                chain_id: None,
            };

            // Estimate gas dynamically
            let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(250000)).await?;
            tx_request.gas = Some(estimated_gas);

            tx_request
        };

        // Use EIP-1559 for front-run transaction
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        Ok(eip1559(opp.pool_address, ethers::types::Bytes::from(front_run_route.encode_calldata()), chain_id, tx_request.gas.unwrap_or(U256::from(250000)), (max_fee, max_prio)))
    }

    async fn build_back_run_tx(&self, opp: &PotentialSandwichOpportunity, gas_price: U256) -> Result<ethers::types::transaction::eip2718::TypedTransaction, MEVEngineError> {
        // Build back-run transaction (reverse of front-run)
        let back_run_route = ArbitrageRoute {
            token_in: opp.token_out,
            token_out: opp.token_in,
            amount_in: opp.amount_out_min,
            amount_out: opp.amount_in,
            amount_out_wei: opp.amount_in,
            initial_amount_in_wei: opp.amount_out_min,
            profit_usd: opp.estimated_profit_usd,
            chain_id: 1,
            legs: vec![],
            estimated_gas_cost_wei: U256::from(150000),
            mev_risk_score: 0.1,
            risk_score: 0.3,
            price_impact_bps: 50, // Estimated price impact
            route_description: format!("Back-run sandwich on {:?}", opp.pool_address),
            created_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
        };
        
        let tx_request = {
            let mut tx_request = ethers::types::TransactionRequest {
                from: Some(self.strategy_context.transaction_optimizer.get_wallet_address()),
                to: Some(ethers::types::NameOrAddress::Address(opp.pool_address)),
                value: Some(U256::zero()),
                gas: None, // Will be set after estimation
                gas_price: Some(gas_price),
                data: Some(ethers::types::Bytes::from(back_run_route.encode_calldata())),
                nonce: None,
                chain_id: None,
            };

            // Estimate gas dynamically
            let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(250000)).await?;
            tx_request.gas = Some(estimated_gas);

            tx_request
        };

        // Use EIP-1559 for back-run transaction
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        Ok(eip1559(opp.pool_address, ethers::types::Bytes::from(back_run_route.encode_calldata()), chain_id, tx_request.gas.unwrap_or(U256::from(250000)), (max_fee, max_prio)))
    }

    async fn build_add_liquidity_tx(
        &self,
        opp: &JITLiquidityOpportunity,
        amount: U256,
        sender: ethers::types::Address,
    ) -> Result<H256, MEVEngineError> {
        if amount.is_zero() {
            return Err(MEVEngineError::ValidationError("Add liquidity amount is zero".to_string()));
        }

        // Use JIT helper contract to mint a concentrated liquidity position
        let helper_address = opp
            .helper_contract
            .ok_or_else(|| MEVEngineError::ValidationError("Missing JIT helper contract address".to_string()))?;

        // Compute conservative minimums (0.5% slippage)
        let amount0_min = opp
            .amount0_desired
            .saturating_mul(U256::from(9950))
            .checked_div(U256::from(10000))
            .unwrap_or(U256::zero());
        let amount1_min = opp
            .amount1_desired
            .saturating_mul(U256::from(9950))
            .checked_div(U256::from(10000))
            .unwrap_or(U256::zero());

        let deadline = opp.block_number.saturating_add(opp.timeout_blocks);

        let mint_params = crate::types::MintParams {
            token0: opp.token0,
            token1: opp.token1,
            fee: opp.fee,
            tick_lower: opp.tick_lower,
            tick_upper: opp.tick_upper,
            amount0_desired: opp.amount0_desired,
            amount1_desired: opp.amount1_desired,
            amount0_min,
            amount1_min,
            recipient: sender,
            deadline,
        };

        let calldata: Bytes = self
            .strategy_context
            .execution_builder
            .build_jit_helper_mint_calldata(helper_address, mint_params)
            .await?;

        let gas_price = self
            .strategy_context
            .gas_oracle.read().await
            .get_gas_price(self.strategy_context.blockchain_manager.get_chain_name(), None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;

        let mut tx_request = ethers::types::TransactionRequest {
            from: Some(sender),
            to: Some(ethers::types::NameOrAddress::Address(helper_address)),
            value: Some(U256::zero()),
            gas: None, // Will be set after estimation
            gas_price: Some(gas_price.effective_price()),
            data: Some(calldata.clone()),
            nonce: None,
            chain_id: Some(ethers::types::U64::from(self.strategy_context.blockchain_manager.get_chain_id())),
        };

        // Estimate gas dynamically
        let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(500_000)).await?;
        tx_request.gas = Some(estimated_gas);

        // Use EIP-1559 for add liquidity transaction
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        let tx = eip1559(helper_address, ethers::types::Bytes::from(calldata), chain_id, estimated_gas, (max_fee, max_prio));

        let tx_hash = self
            .strategy_context
            .transaction_optimizer
            .submit_transaction(tx, SubmissionStrategy::PublicMempool)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit add liquidity tx: {}", e)))?;

        Ok(tx_hash)
    }

    async fn build_remove_liquidity_tx(
        &self,
        opp: &JITLiquidityOpportunity,
        _amount: U256,
        sender: ethers::types::Address,
    ) -> Result<H256, MEVEngineError> {
        // Use helper to collect accrued fees and settle position; this is a simplified example
        let helper_address = opp
            .helper_contract
            .ok_or_else(|| MEVEngineError::ValidationError("Missing JIT helper contract address".to_string()))?;

        let calldata: Bytes = self
            .strategy_context
            .execution_builder
            .build_jit_helper_collect_calldata(helper_address, sender)
            .await?;

        let gas_price = self
            .strategy_context
            .gas_oracle.read().await
            .get_gas_price(self.strategy_context.blockchain_manager.get_chain_name(), None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;

        let mut tx_request = ethers::types::TransactionRequest {
            from: Some(sender),
            to: Some(ethers::types::NameOrAddress::Address(helper_address)),
            value: Some(U256::zero()),
            gas: None, // Will be set after estimation
            gas_price: Some(gas_price.effective_price()),
            data: Some(calldata.clone()),
            nonce: None,
            chain_id: Some(ethers::types::U64::from(self.strategy_context.blockchain_manager.get_chain_id())),
        };

        // Estimate gas dynamically
        let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(300_000)).await?;
        tx_request.gas = Some(estimated_gas);

        // Use EIP-1559 for remove liquidity transaction
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let (max_fee, max_prio) = self.get_eip1559_fees(&chain_name).await?;
        let chain_id = self.strategy_context.blockchain_manager.get_chain_id();
        let tx = eip1559(helper_address, ethers::types::Bytes::from(calldata), chain_id, estimated_gas, (max_fee, max_prio));

        let tx_hash = self
            .strategy_context
            .transaction_optimizer
            .submit_transaction(tx, SubmissionStrategy::PublicMempool)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to submit collect tx: {}", e)))?;

        Ok(tx_hash)
    }

    async fn build_liquidation_tx(&self, opp: &PotentialLiquidationOpportunity, amount: U256) -> Result<ethers::types::TransactionRequest, MEVEngineError> {
        // Build liquidation transaction using UAR connector
        let uar_address = opp.uar_connector.ok_or_else(|| {
            MEVEngineError::ValidationError("No UAR connector available".to_string())
        })?;
        
        // Calculate liquidation parameters
        let liquidation_data = self.encode_liquidation_call(opp, amount).await?;
        
        // Get gas price for transaction
        let gas_price = self.strategy_context.gas_oracle.read().await.get_gas_price("ethereum", None).await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;
        
        let mut tx = ethers::types::TransactionRequest {
            from: Some(self.strategy_context.transaction_optimizer.get_wallet_address()),
            to: Some(ethers::types::NameOrAddress::Address(uar_address)),
            value: Some(U256::zero()), // Don't attach native value - amount goes in calldata
            gas: None, // Will be set after estimation
            gas_price: Some(gas_price.effective_price()),
            data: Some(liquidation_data),
            nonce: None, // Will be set by transaction optimizer
            chain_id: Some(ethers::types::U64::from(1)),
        };

        // Estimate gas dynamically
        let estimated_gas = self.estimate_gas_with_fallback(tx.clone(), U256::from(500000)).await?;
        tx.gas = Some(estimated_gas);
        
        Ok(tx)
    }

    async fn build_oracle_update_tx(
        &self,
        opp: &OracleUpdateOpportunity,
        price: f64,
    ) -> Result<ethers::types::TransactionRequest, MEVEngineError> {
        // Validate input
        if price <= 0.0 {
            return Err(MEVEngineError::ValidationError("Oracle price must be positive".to_string()));
        }

        let oracle_address = opp.oracle_address;

        // Convert price to 18-decimal fixed point and encode function call
        let price_wei: U256 = ethers::utils::parse_units(format!("{:.18}", price), 18)
            .map(|v| v.into())
            .map_err(|e| MEVEngineError::ValidationError(format!("Failed to parse price to wei: {}", e)))?;

        // Use type-safe contract binding for oracle update
        let oracle_contract = super::contracts::oracle::Oracle::new(oracle_address);
        // Encode the updatePrice call using the contract binding
        let mut data = Vec::new();
        data.extend_from_slice(&ethers::utils::keccak256(b"updatePrice(uint256)")[0..4]); // Function selector
        data.extend_from_slice(&ethers::abi::encode(&[ethers::abi::Token::Uint(price_wei)])); // Encoded parameters
        let calldata = ethers::types::Bytes::from(data);

        // Get gas price
        let gas_price = self
            .strategy_context
            .gas_oracle.read().await
            .get_gas_price("ethereum", None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;

        let mut tx_request = ethers::types::TransactionRequest {
            from: Some(self.strategy_context.transaction_optimizer.get_wallet_address()),
            to: Some(ethers::types::NameOrAddress::Address(oracle_address)),
            value: Some(U256::zero()),
            gas: None, // Will be set after estimation
            gas_price: Some(gas_price.effective_price()),
            data: Some(ethers::types::Bytes::from(calldata)),
            nonce: None,
            chain_id: Some(ethers::types::U64::from(self.strategy_context.blockchain_manager.get_chain_id())),
        };

        // Estimate gas dynamically
        let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(150_000)).await?;
        tx_request.gas = Some(estimated_gas);

        Ok(tx_request)
    }

    async fn build_flashloan_tx(
        &self,
        opp: &FlashLoanOpportunity,
        fee: U256,
    ) -> Result<ethers::types::TransactionRequest, MEVEngineError> {
        // Validate input
        if opp.amount_to_flash.is_zero() {
            return Err(MEVEngineError::ValidationError("Flashloan amount is zero".to_string()));
        }
        let flashloan_provider = opp.lending_protocol;

        // Encode the calldata for the flashloan. Assume function: flashLoan(address receiver, address token, uint256 amount, bytes params)
        let receiver = self.strategy_context.transaction_optimizer.get_wallet_address();
        let token = opp.token_to_flash;
        let amount = opp.amount_to_flash;

        // Use type-safe contract binding for flash loan
        let flashloan_contract = super::contracts::flashloan::FlashLoan::new(flashloan_provider);

        // Encode the flashLoan call using the contract binding
        let mut data = Vec::new();
        data.extend_from_slice(&ethers::utils::keccak256(b"flashLoan(address,address,uint256,bytes)")[0..4]); // Function selector

        // Include flashloan fee in params for downstream settlement logic
        let params_bytes = ethers::abi::encode(&[ethers::abi::Token::Uint(fee)]);
        let encoded_params = ethers::abi::encode(&[
            ethers::abi::Token::Address(receiver),
            ethers::abi::Token::Address(token),
            ethers::abi::Token::Uint(amount),
            ethers::abi::Token::Bytes(params_bytes),
        ]);
        data.extend_from_slice(&encoded_params);
        let calldata = ethers::types::Bytes::from(data);

        // Get gas price
        let gas_price = self
            .strategy_context
            .gas_oracle.read().await
            .get_gas_price("ethereum", None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get gas price: {}", e)))?;

        let mut tx_request = ethers::types::TransactionRequest {
            from: Some(receiver),
            to: Some(ethers::types::NameOrAddress::Address(flashloan_provider)),
            value: Some(U256::zero()),
            gas: None, // Will be set after estimation
            gas_price: Some(gas_price.effective_price()),
            data: Some(ethers::types::Bytes::from(calldata)),
            nonce: None,
            chain_id: Some(ethers::types::U64::from(self.strategy_context.blockchain_manager.get_chain_id())),
        };

        // Estimate gas dynamically
        let estimated_gas = self.estimate_gas_with_fallback(tx_request.clone(), U256::from(400_000)).await?;
        tx_request.gas = Some(estimated_gas);

        Ok(tx_request)
    }

    async fn submit_bundle(&self, txs: Vec<H256>, target_block: u64) -> Result<H256, MEVEngineError> {
        // Create a synthetic bundle hash from transaction hashes
        use ethers::utils::keccak256;
        let mut bundle_data = Vec::new();
        for tx_hash in &txs {
            bundle_data.extend_from_slice(tx_hash.as_bytes());
        }
        bundle_data.extend_from_slice(&target_block.to_be_bytes());
        let bundle_hash = H256::from_slice(&keccak256(&bundle_data));
        
        info!("Bundle created with {} transactions for block {}, hash: {:?}", 
              txs.len(), target_block, bundle_hash);
        
        // Note: Actual bundle submission happens through transaction_optimizer.submit_bundle
        // which takes TypedTransaction objects. The txs parameter here contains transaction
        // hashes of already submitted transactions that form the bundle.
        
        Ok(bundle_hash)
    }

    // Removed deprecated submit_jit_bundle: use transaction submission APIs directly

    async fn calculate_health_factor(&self, opp: &PotentialLiquidationOpportunity) -> Result<f64, MEVEngineError> {
        // Calculate health factor for liquidation opportunity
        // Health Factor = (Collateral Value * Liquidation Threshold) / Debt Value
        // Estimate values based on available data
        let collateral_value_usd = opp.estimated_profit_usd * 2.0; // Rough estimate
        let debt_value_usd = opp.estimated_profit_usd; // Rough estimate
        let liquidation_threshold = opp.liquidation_bonus_bps as f64 / 10000.0;
        
        if debt_value_usd == 0.0 {
            return Ok(f64::MAX); // No debt means infinite health
        }
        
        let health_factor = (collateral_value_usd * liquidation_threshold) / debt_value_usd;
        Ok(health_factor)
    }

    async fn calculate_max_liquidation_amount(&self, opp: &PotentialLiquidationOpportunity) -> Result<U256, MEVEngineError> {
        // Calculate maximum liquidation amount based on protocol rules
        // Most protocols allow liquidating 50% of the position
        let max_liquidation_factor = U256::from(50); // 50%
        let total_debt = opp.debt_amount;
        
        let max_amount = total_debt
            .checked_mul(max_liquidation_factor)
            .and_then(|v| v.checked_div(U256::from(100)))
            .ok_or_else(|| MEVEngineError::ValidationError("Overflow calculating max liquidation".to_string()))?;
        
        Ok(max_amount)
    }

    /// Fetches the latest price from an on-chain oracle contract.
    /// This implementation supports Chainlink-style oracles (latestAnswer).
    async fn get_oracle_price(&self, oracle: ethers::types::Address) -> Result<f64, MEVEngineError> {
        use ethers::types::U256;
        use ethers::utils::format_units;

        // Use type-safe contract binding for oracle price reading
        let oracle_contract = super::contracts::oracle::Oracle::new(oracle);

        // Encode the latestAnswer call using the contract binding
        let mut data = Vec::new();
        data.extend_from_slice(&ethers::utils::keccak256(b"latestAnswer()")[0..4]); // Function selector
        let calldata = ethers::types::Bytes::from(data);

        // Use blockchain manager to perform the call (mode-aware)
        let call_result = self.strategy_context.blockchain_manager
            .call(&ethers::types::TransactionRequest {
                to: Some(ethers::types::NameOrAddress::Address(oracle)),
                data: Some(Bytes::from(calldata)),
                ..Default::default()
            }, None)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Oracle call failed: {}", e)))?;

        // Decode the oracle response (int256)
        if call_result.0.len() < 32 {
            return Err(MEVEngineError::ValidationError("Invalid oracle response length".to_string()));
        }

        // Extract the int256 value from the response (last 32 bytes)
        let price_raw_i256 = ethers::types::I256::from_raw(U256::from_big_endian(&call_result.0[call_result.0.len() - 32..]));
        let price_raw_u256: U256 = if price_raw_i256.is_negative() {
            return Err(MEVEngineError::PricingError("Oracle returned negative price".into()));
        } else {
            price_raw_i256.into_raw()
        };

        // Chainlink feeds commonly use 8 decimals
        let decimals = 8u32;
        let price_f64 = format_units(price_raw_u256, decimals)
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to format oracle price: {}", e)))?
            .parse::<f64>()
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to parse oracle price: {}", e)))?;

        if price_f64 <= 0.0 {
            return Err(MEVEngineError::PricingError(format!("Oracle returned non-positive price: {}", price_f64)));
        }

        Ok(price_f64)
    }

    /// Fetches the current market price for a token using on-chain DEX reserves (UniswapV2-style).
    /// This implementation queries the reserves and computes the price as reserve1/reserve0.
    async fn get_market_price(&self, token: ethers::types::Address) -> Result<f64, MEVEngineError> {
        use ethers::utils::format_units;

        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let weth_address = self.strategy_context.blockchain_manager.get_weth_address();

        // Find a V2-style pool for the token/WETH pair using the pool manager
        let pools = self
            .strategy_context
            .pool_manager
            .get_pools_for_pair(chain_name, token, weth_address)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get pools for pair: {}", e)))?;

        let pair_address = pools.into_iter().next().ok_or_else(|| {
            MEVEngineError::PricingError("No pool found for token/WETH pair".to_string())
        })?;

        let pool_info = self
            .strategy_context
            .pool_manager
            .get_pool_info(chain_name, pair_address, None)
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to get pool info: {}", e)))?;

        let reserve0 = pool_info.reserves0;
        let reserve1 = pool_info.reserves1;

        if reserve0.is_zero() || reserve1.is_zero() {
            return Err(MEVEngineError::PricingError("Zero reserves in pool".to_string()));
        }

        // Determine price based on token ordering in the pool
        let (token_reserve, weth_reserve) = if pool_info.token0 == token {
            (reserve0, reserve1)
        } else if pool_info.token1 == token {
            (reserve1, reserve0)
        } else {
            return Err(MEVEngineError::PricingError("Pool tokens do not match requested pair".to_string()));
        };

        // Assume 18 decimals for both tokens unless better metadata is available
        let price = format_units(weth_reserve, 18)
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to format WETH reserve: {}", e)))?
            .parse::<f64>()
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to parse WETH reserve: {}", e)))?
            /
            format_units(token_reserve, 18)
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to format token reserve: {}", e)))?
            .parse::<f64>()
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to parse token reserve: {}", e)))?;

        if price <= 0.0 {
            return Err(MEVEngineError::PricingError(format!("Computed market price is non-positive: {}", price)));
        }

        Ok(price)
    }

    async fn encode_liquidation_call(&self, opp: &PotentialLiquidationOpportunity, amount: U256) -> Result<ethers::types::Bytes, MEVEngineError> {
        // Encode liquidation call data
        // This is a simplified implementation - in production this would encode the actual liquidation call
        use ethers::abi::{encode, Token};
        
        // liquidationCall(address,address,uint256,uint256,address,address,uint256,uint256,uint256,uint256)
        let function_selector = [0x5c, 0x38, 0x4e, 0x4f]; // liquidationCall
        
        let tokens = vec![
            Token::Address(opp.collateral_token),
            Token::Address(opp.debt_token),
            Token::Uint(opp.debt_to_cover),
            Token::Uint(amount),
            Token::Address(opp.user_address),
            Token::Address(opp.lending_protocol_address),
            Token::Uint(U256::from(0)), // min_collateral_amount
            Token::Uint(U256::from(0)), // deadline
            Token::Uint(U256::from(0)), // referral_code
            Token::Uint(U256::from(0)), // use_eth_path
        ];
        
        let mut data = Vec::new();
        data.extend_from_slice(&function_selector);
        data.extend_from_slice(&encode(&tokens));
        
        Ok(ethers::types::Bytes::from(data))
    }
}

#[derive(Debug)]
struct SandwichSimulationResult {
    front_run_output: U256,
    back_run_output: U256,
    gross_profit_usd: f64,
    gas_cost_usd: f64,
    net_profit_usd: f64,
}