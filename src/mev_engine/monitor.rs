use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn, instrument};
use ethers::types::{H256, U256, U64};
use ethers::providers::Middleware;
use ethers::utils::keccak256;
use serde_json::json;
use hex;

use super::{MEVEngine, MEVEngineError, MEVPlayStatus, SafeActiveMEVPlay};
use crate::mempool::MEVOpportunity;
// keep Uuid dependency indirect via types; no direct use here

// Monitoring constants
const BLOCK_INCLUSION_TIMEOUT: u64 = 30; // 30 seconds
const CONFIRMATION_BLOCKS: u64 = 2;
const MAX_PENDING_BLOCKS: u64 = 10;
const RECEIPT_CHECK_RETRIES: u32 = 3;
const RECEIPT_CHECK_DELAY_MS: u64 = 1000;

impl MEVEngine {
    #[instrument(skip(self, shutdown_rx))]
    pub(super) async fn active_play_monitor_loop(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        info!("Starting active play monitor");
        let mut monitor_interval = interval(Duration::from_secs(self.config.status_check_interval_secs));
        
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Monitor received shutdown signal");
                        break;
                    }
                }
                _ = monitor_interval.tick() => {
                    if let Err(e) = Arc::clone(&self).monitor_active_plays().await {
                        error!("Error monitoring active plays: {}", e);
                    }
                }
            }
        }
        
        info!("Active play monitor stopped");
    }

    async fn monitor_active_plays(self: Arc<Self>) -> Result<(), MEVEngineError> {
        let current_block = self.strategy_context.blockchain_manager
            .get_block_number()
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get block number: {}", e)))?;

        // First, collect all plays without blocking
        let all_plays: Vec<(uuid::Uuid, SafeActiveMEVPlay)> = self.active_plays
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        // Then filter them asynchronously
        let mut plays_to_monitor = Vec::new();
        for (play_id, play_wrapper) in all_plays {
            let needs_monitoring = play_wrapper.read(|play| play.status.requires_monitoring()).await;
            if needs_monitoring {
                plays_to_monitor.push((play_id, play_wrapper));
            }
        }

        debug!("Monitoring {} active plays", plays_to_monitor.len());

        // Monitor each play concurrently
        let mut monitoring_tasks = Vec::new();
        for (play_id, play_wrapper) in plays_to_monitor {
            let self_clone = Arc::clone(&self);
            let play_wrapper_clone = play_wrapper.clone();
            let task = tokio::spawn(async move {
                if let Err(e) = self_clone.check_play_status(&play_wrapper_clone, current_block).await {
                    warn!("Error checking play {} status: {}", play_id, e);
                }
            });
            monitoring_tasks.push(task);
        }

        // Wait for all monitoring tasks with timeout
        let results = timeout(
            Duration::from_secs(30),
            futures::future::join_all(monitoring_tasks)
        ).await;

        if results.is_err() {
            warn!("Some play monitoring tasks timed out");
        }

        Ok(())
    }

    #[instrument(skip(self, play_wrapper))]
    async fn check_play_status(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
        current_block: u64,
    ) -> Result<(), MEVEngineError> {
        let status = play_wrapper.read(|play| play.status.clone()).await;
        
        match status {
            MEVPlayStatus::SubmittedToRelay(bundle_hash) => {
                self.check_bundle_status(&bundle_hash, &play_wrapper, current_block).await?;
            }
            MEVPlayStatus::SubmittedToMempool(tx_hashes) => {
                self.check_tx_statuses(&tx_hashes, &play_wrapper, current_block).await?;
            }
            MEVPlayStatus::PendingInclusion => {
                self.check_pending_inclusion(&play_wrapper, current_block).await?;
            }
            MEVPlayStatus::PendingConfirmation => {
                self.check_pending_confirmation(&play_wrapper, current_block).await?;
            }
            _ => {}
        }
        
        Ok(())
    }

    async fn check_bundle_status(
        &self,
        bundle_hash: &H256,
        play_wrapper: &SafeActiveMEVPlay,
        current_block: u64,
    ) -> Result<(), MEVEngineError> {
        // Check if bundle was included via Flashbots API
        let bundle_status = self.query_bundle_status(bundle_hash).await?;
        
        match bundle_status {
            BundleStatus::Included(block_number) => {
                info!("Bundle {} included in block {}", bundle_hash, block_number);
                
                if let Err(e) = play_wrapper.mark_pending_confirmation(block_number).await {
                    warn!("Failed to update play status: {}", e);
                }
            }
            BundleStatus::Failed(reason) => {
                warn!("Bundle {} failed: {}", bundle_hash, reason);
                
                if let Err(e) = play_wrapper.confirm_failure(format!("Bundle not included: {}", reason)).await {
                    warn!("Failed to update play status: {}", e);
                }
            }
            BundleStatus::Pending => {
                // Check if bundle is too old
                let target_block = play_wrapper.read(|play| play.target_block).await;
                
                if let Some(target) = target_block {
                    if current_block > target + MAX_PENDING_BLOCKS {
                        warn!("Bundle {} expired (target block {} passed)", bundle_hash, target);
                        
                        if let Err(e) = play_wrapper.mark_expired().await {
                            warn!("Failed to update play status: {}", e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn check_tx_statuses(
        &self,
        tx_hashes: &[H256],
        play_wrapper: &SafeActiveMEVPlay,
        current_block: u64,
    ) -> Result<(), MEVEngineError> {
        let mut all_confirmed = true;
        let mut any_failed = false;
        let mut failure_reason = None;
        let mut inclusion_block = None;

        for tx_hash in tx_hashes {
            let receipt = self.get_transaction_receipt_with_retry(tx_hash).await?;
            
            match receipt {
                Some(receipt) => {
                    if receipt.status == Some(U64::from(0)) {
                        // Transaction failed
                        any_failed = true;
                        failure_reason = Some(format!("Transaction {} reverted", tx_hash));
                        
                        // Try to get revert reason
                        if let Ok(reason) = self.get_revert_reason(tx_hash).await {
                            failure_reason = Some(reason);
                        }
                    } else {
                        // Transaction succeeded
                        if inclusion_block.is_none() {
                            inclusion_block = receipt.block_number.map(|n| n.as_u64());
                        }
                        
                        // Check if sufficiently confirmed
                        if let Some(block_num) = receipt.block_number {
                            if current_block < block_num.as_u64() + CONFIRMATION_BLOCKS {
                                all_confirmed = false;
                            }
                        }
                    }
                }
                None => {
                    // Transaction still pending
                    all_confirmed = false;
                    
                    // Check if transaction is too old
                    let age = play_wrapper.read(|play| play.age()).await;
                    if age > Duration::from_secs(self.config.max_play_age_secs) {
                        any_failed = true;
                        failure_reason = Some("Transaction timeout".to_string());
                    }
                }
            }
        }

        // Update play status based on results
        if any_failed {
            let failure_reason_str = failure_reason.clone().unwrap_or_else(|| "Transaction failed".to_string());
            if let Err(e) = play_wrapper.confirm_failure(failure_reason_str.clone()).await {
                warn!("Failed to update play status: {}", e);
            }

            // Update metadata separately if we have a specific reason
            if let Some(reason) = failure_reason {
                play_wrapper.update_atomic(|play| {
                    play.metadata.revert_reason = Some(reason);
                }).await;
            }
        } else if all_confirmed {
            // Capture post-transaction balances before calculating profit
            let relevant_tokens = self.extract_relevant_tokens_from_play(&play_wrapper).await;
            if let Err(e) = self.capture_post_tx_balances(&play_wrapper, &relevant_tokens).await {
                warn!("Failed to capture post-transaction balances: {}", e);
                // Continue with profit calculation even if balance capture fails
            }

            // Calculate actual profit
            let actual_profit = match self.calculate_actual_profit(&play_wrapper).await {
                Ok(profit) => profit,
                Err(e) => {
                    warn!("Failed to calculate actual profit: {}", e);
                    play_wrapper.read(|play| play.estimated_profit_usd).await
                }
            };

            if let Err(e) = play_wrapper.confirm_success(actual_profit).await {
                warn!("Failed to update play status: {}", e);
            }

            // Update metrics
            let mut metrics = self.metrics.write().await;
            metrics.successful_plays += 1;
            metrics.total_profit_usd += actual_profit;
        } else if inclusion_block.is_some() {
            if let Some(block_num) = inclusion_block {
                if let Err(e) = play_wrapper.mark_pending_confirmation(block_num).await {
                    warn!("Failed to update play status: {}", e);
                }
            }
        }
        
        Ok(())
    }

    async fn check_pending_inclusion(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
        current_block: u64,
    ) -> Result<(), MEVEngineError> {
        let (target_block, age) = play_wrapper.read(|play| {
            (play.target_block, play.age())
        }).await;

        // Check if opportunity has expired
        if let Some(target) = target_block {
            if current_block > target + MAX_PENDING_BLOCKS {
                if let Err(e) = play_wrapper.mark_expired().await {
                    warn!("Failed to update play status: {}", e);
                }
                return Ok(());
            }
        }

        // Check if play is too old
        if age > Duration::from_secs(self.config.max_play_age_secs) {
            play_wrapper.update_atomic(|play| {
                play.status = MEVPlayStatus::Expired;
            }).await;
        }

        Ok(())
    }

    async fn check_pending_confirmation(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
        current_block: u64,
    ) -> Result<(), MEVEngineError> {
        let inclusion_block = play_wrapper.read(|play| play.inclusion_block_number).await;
        
        if let Some(block_num) = inclusion_block {
            if current_block >= block_num + CONFIRMATION_BLOCKS {
                // Capture post-transaction balances before calculating profit
                let relevant_tokens = self.extract_relevant_tokens_from_play(&play_wrapper).await;
                if let Err(e) = self.capture_post_tx_balances(&play_wrapper, &relevant_tokens).await {
                    warn!("Failed to capture post-transaction balances: {}", e);
                    // Continue with profit calculation even if balance capture fails
                }

                // Transaction is confirmed
                let actual_profit = match self.calculate_actual_profit(&play_wrapper).await {
                    Ok(profit) => profit,
                    Err(e) => {
                        warn!("Failed to calculate actual profit: {}", e);
                        play_wrapper.read(|play| play.estimated_profit_usd).await
                    }
                };

                if let Err(e) = play_wrapper.confirm_success(actual_profit).await {
                    warn!("Failed to update play status: {}", e);
                }

                info!("Play confirmed with profit: ${}", actual_profit);
            }
        }
        
        Ok(())
    }

    async fn calculate_actual_profit(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
    ) -> Result<f64, MEVEngineError> {
        let (pre_tx_balances, post_tx_balances, tx_hashes, estimated_gas_used, max_fee_per_gas) = play_wrapper.read(|play| {
            (
                play.pre_tx_balances.clone(),
                play.post_tx_balances.clone(),
                play.submitted_tx_hashes.clone(),
                play.gas_used,
                play.max_fee_per_gas
            )
        }).await;

        // If we have balance tracking data, use the balance delta method
        if let (Some(pre_balances), Some(post_balances)) = (pre_tx_balances, post_tx_balances) {
            return self.calculate_profit_from_balance_deltas(pre_balances, post_balances, &tx_hashes).await;
        }

        // Fallback to legacy event log parsing method if balance tracking not available
        warn!("No balance tracking data available, falling back to event log parsing");
        self.calculate_actual_profit_legacy(play_wrapper).await
    }

    /// Calculate profit using balance delta method (industry standard)
    async fn calculate_profit_from_balance_deltas(
        &self,
        pre_balances: std::collections::HashMap<ethers::types::Address, U256>,
        post_balances: std::collections::HashMap<ethers::types::Address, U256>,
        tx_hashes: &[H256],
    ) -> Result<f64, MEVEngineError> {
        let mut total_profit_usd = 0.0;
        let mut total_gas_cost_usd = 0.0;

        // Get our wallet address
        let our_wallet = self.strategy_context.blockchain_manager.get_uar_address()
            .ok_or_else(|| MEVEngineError::ConfigError("UAR address not configured".into()))?;

        // Calculate balance deltas for all tokens
        for (token_address, pre_balance) in &pre_balances {
            let post_balance = post_balances.get(token_address).copied().unwrap_or(U256::zero());
            let balance_delta = post_balance.saturating_sub(*pre_balance);

            if balance_delta.is_zero() {
                continue;
            }

            // Get token decimals for proper conversion
            let decimals = if token_address == &self.strategy_context.blockchain_manager.get_weth_address() {
                // Native ETH/WETH has 18 decimals
                18u8
            } else {
                self.strategy_context.blockchain_manager
                    .get_token_decimals(*token_address, None)
                    .await
                    .unwrap_or(18) // Default to 18 if unable to fetch
            };

            // Convert balance delta to USD
            let token_amount = balance_delta.as_u128() as f64 / (10_f64.powi(decimals as i32));

            if *token_address == self.strategy_context.blockchain_manager.get_weth_address() {
                // For WETH, use ETH price
                let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
                let eth_price = self.strategy_context.price_oracle
                    .get_price(chain_name, *token_address)
                    .await
                    .map_err(|e| MEVEngineError::PricingError(format!("Failed to fetch ETH price: {}", e)))?;

                let balance_change_usd = token_amount * eth_price;
                total_profit_usd += balance_change_usd;
                debug!("WETH balance delta: {} tokens (${:.2})", token_amount, balance_change_usd);
            } else {
                // For other tokens, get token-specific price
                let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
                let token_price = self.strategy_context.price_oracle
                    .get_price(&chain_name, *token_address)
                    .await
                    .unwrap_or(1.0); // Default to $1 if price fetch fails

                let balance_change_usd = token_amount * token_price;
                total_profit_usd += balance_change_usd;
                debug!("Token {} balance delta: {} tokens (${:.2})", token_address, token_amount, balance_change_usd);
            }
        }

        // Calculate gas costs from transaction receipts
        for tx_hash in tx_hashes {
            if let Some(receipt) = self.get_transaction_receipt_with_retry(tx_hash).await? {
                if let (Some(gas_used), Some(effective_gas_price)) = (receipt.gas_used, receipt.effective_gas_price) {
                    let gas_cost_wei = gas_used.saturating_mul(effective_gas_price);
                    let gas_cost_eth = gas_cost_wei.as_u128() as f64 / 1e18;

                    let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
                    let eth_price = self.strategy_context.price_oracle
                        .get_price(chain_name, self.strategy_context.blockchain_manager.get_weth_address())
                        .await
                        .map_err(|e| MEVEngineError::PricingError(format!("Failed to fetch ETH price: {}", e)))?;

                    let gas_cost_usd = gas_cost_eth * eth_price;
                    total_gas_cost_usd += gas_cost_usd;
                    debug!("Transaction {} gas cost: ${:.2}", tx_hash, gas_cost_usd);
                }
            }
        }

        let net_profit = total_profit_usd - total_gas_cost_usd;
        debug!("Balance delta profit calculation: gross=${:.2}, gas=${:.2}, net=${:.2}",
               total_profit_usd, total_gas_cost_usd, net_profit);

        Ok(net_profit.max(0.0))
    }

    /// Capture pre-transaction balances for all relevant tokens
    pub async fn capture_pre_tx_balances(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
        relevant_tokens: &[ethers::types::Address],
    ) -> Result<(), MEVEngineError> {
        let our_wallet = self.strategy_context.blockchain_manager.get_uar_address()
            .ok_or_else(|| MEVEngineError::ConfigError("UAR address not configured".into()))?;

        let mut balances = std::collections::HashMap::new();

        // Always track ETH/WETH balance
        let weth_address = self.strategy_context.blockchain_manager.get_weth_address();
        let eth_balance = self.strategy_context.blockchain_manager
            .get_balance(our_wallet, None)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get ETH balance: {}", e)))?;
        balances.insert(weth_address, eth_balance);

        // Track balances for all relevant tokens
        for token_address in relevant_tokens {
            if *token_address != weth_address { // Avoid double-tracking WETH
                let token_balance = self.get_erc20_balance(*token_address, our_wallet).await?;
                balances.insert(*token_address, token_balance);
            }
        }

        // Update the play with pre-transaction balances
        play_wrapper.update_atomic(|play| {
            play.pre_tx_balances = Some(balances);
        }).await;

        debug!("Captured pre-transaction balances for {} tokens", relevant_tokens.len());
        Ok(())
    }

    /// Capture post-transaction balances for all relevant tokens
    pub async fn capture_post_tx_balances(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
        relevant_tokens: &[ethers::types::Address],
    ) -> Result<(), MEVEngineError> {
        let our_wallet = self.strategy_context.blockchain_manager.get_uar_address()
            .ok_or_else(|| MEVEngineError::ConfigError("UAR address not configured".into()))?;

        let mut balances = std::collections::HashMap::new();

        // Always track ETH/WETH balance
        let weth_address = self.strategy_context.blockchain_manager.get_weth_address();
        let eth_balance = self.strategy_context.blockchain_manager
            .get_balance(our_wallet, None)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to get ETH balance: {}", e)))?;
        balances.insert(weth_address, eth_balance);

        // Track balances for all relevant tokens
        for token_address in relevant_tokens {
            if *token_address != weth_address { // Avoid double-tracking WETH
                let token_balance = self.get_erc20_balance(*token_address, our_wallet).await?;
                balances.insert(*token_address, token_balance);
            }
        }

        // Update the play with post-transaction balances
        play_wrapper.update_atomic(|play| {
            play.post_tx_balances = Some(balances);
        }).await;

        debug!("Captured post-transaction balances for {} tokens", relevant_tokens.len());
        Ok(())
    }

    /// Get ERC20 token balance for an address using contract call
    async fn get_erc20_balance(
        &self,
        token_address: ethers::types::Address,
        owner_address: ethers::types::Address,
    ) -> Result<U256, MEVEngineError> {
        // ERC20 balanceOf function selector: keccak256("balanceOf(address)")[0..4]
        let selector = &ethers::utils::keccak256(b"balanceOf(address)")[0..4];

        // Encode the function call data
        let mut data = Vec::new();
        data.extend_from_slice(selector);
        data.extend_from_slice(&ethers::abi::encode(&[ethers::abi::Token::Address(owner_address)]));

        // Create transaction request for the call
        let tx_request = ethers::types::TransactionRequest {
            to: Some(ethers::types::NameOrAddress::Address(token_address)),
            data: Some(ethers::types::Bytes::from(data)),
            ..Default::default()
        };

        // Execute the call
        let result = self.strategy_context.blockchain_manager
            .call(&tx_request, None)
            .await
            .map_err(|e| MEVEngineError::NetworkError(format!("Failed to call balanceOf: {}", e)))?;

        // Decode the result (uint256)
        if result.len() < 32 {
            return Err(MEVEngineError::NetworkError("Invalid balanceOf response".into()));
        }

        let balance = U256::from_big_endian(&result[result.len() - 32..]);
        Ok(balance)
    }

    /// Extract relevant token addresses from an active play for balance tracking
    async fn extract_relevant_tokens_from_play(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
    ) -> Vec<ethers::types::Address> {
        let original_signal = play_wrapper.read(|play| play.original_signal.clone()).await;
        self.extract_relevant_tokens(&original_signal)
    }

    /// Extract relevant token addresses from an MEV opportunity for balance tracking
    pub fn extract_relevant_tokens(&self, opportunity: &MEVOpportunity) -> Vec<ethers::types::Address> {
        let mut tokens = Vec::new();

        match opportunity {
            MEVOpportunity::Sandwich(sandwich) => {
                tokens.push(sandwich.token_in);
                tokens.push(sandwich.token_out);
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address()); // Always track ETH
            }
            MEVOpportunity::Liquidation(liquidation) => {
                tokens.push(liquidation.collateral_token);
                tokens.push(liquidation.debt_token);
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address()); // Always track ETH
            }
            MEVOpportunity::JITLiquidity(jit) => {
                tokens.push(jit.token0);
                tokens.push(jit.token1);
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address()); // Always track ETH
            }
            MEVOpportunity::Arbitrage(_) => {
                // For arbitrage, we need to extract tokens from the route
                // This would require access to the ArbitrageRoute structure
                // For now, just track common stablecoins and ETH
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address());
                // Add common stablecoin addresses if available
            }
            MEVOpportunity::OracleUpdate(oracle) => {
                tokens.push(oracle.token_address);
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address()); // Always track ETH
            }
            MEVOpportunity::FlashLoan(flash) => {
                tokens.push(flash.token_to_flash);
                tokens.push(self.strategy_context.blockchain_manager.get_weth_address()); // Always track ETH
            }
        }

        // Remove duplicates
        tokens.sort();
        tokens.dedup();
        tokens
    }

    /// Legacy profit calculation method using event log parsing (deprecated)
    async fn calculate_actual_profit_legacy(
        &self,
        play_wrapper: &SafeActiveMEVPlay,
    ) -> Result<f64, MEVEngineError> {
        let (tx_hashes, estimated_gas_used, max_fee_per_gas) = play_wrapper.read(|play| {
            (play.submitted_tx_hashes.clone(), play.gas_used, play.max_fee_per_gas)
        }).await;

        if tx_hashes.is_empty() {
            return Ok(0.0);
        }

        let mut total_gas_cost = U256::zero();
        let mut total_value_captured = U256::zero();
        let mut actual_gas_used = U256::zero();

        // ERC20 Transfer event signature - keccak256("Transfer(address,address,uint256)")
        let transfer_event_topic = H256::from_slice(&keccak256(b"Transfer(address,address,uint256)"));
        // Swap event signatures for various DEXes
        let uniswap_v2_swap_topic = H256::from_slice(&keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)"));
        let uniswap_v3_swap_topic = H256::from_slice(&keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)"));

        // Get our wallet address for profit tracking
        let our_wallet = self.strategy_context.blockchain_manager.get_uar_address()
            .ok_or_else(|| MEVEngineError::ConfigError("UAR address not configured".into()))?;

        for tx_hash in &tx_hashes {
            if let Some(receipt) = self.get_transaction_receipt_with_retry(tx_hash).await? {
                // Track actual gas used
                let receipt_gas_used = receipt.gas_used.unwrap_or(U256::zero());
                actual_gas_used = actual_gas_used
                    .checked_add(receipt_gas_used)
                    .ok_or_else(|| MEVEngineError::ArithmeticError("Gas used overflow".into()))?;

                // Calculate gas cost
                let gas_price = receipt.effective_gas_price.unwrap_or(max_fee_per_gas.unwrap_or(U256::zero()));

                let gas_cost = receipt_gas_used
                    .checked_mul(gas_price)
                    .ok_or_else(|| MEVEngineError::ArithmeticError("Gas cost overflow".into()))?;

                total_gas_cost = total_gas_cost
                    .checked_add(gas_cost)
                    .ok_or_else(|| MEVEngineError::ArithmeticError("Total gas cost overflow".into()))?;

                // Parse logs to calculate value captured from arbitrage
                for log in receipt.logs {
                    if !log.topics.is_empty() {
                        let topic0 = log.topics[0];

                        // Track transfers to our address (profit extraction)
                        if topic0 == transfer_event_topic && log.topics.len() >= 3 {
                            // Check if transfer is TO our address (topics[2] is the 'to' address)
                            // The address in topics is padded to 32 bytes
                            let mut padded_our_address = [0u8; 32];
                            padded_our_address[12..].copy_from_slice(our_wallet.as_bytes());
                            let our_address_topic = H256::from_slice(&padded_our_address);

                            if log.topics[2] == our_address_topic {
                                // Extract amount from data field
                                if log.data.len() >= 32 {
                                    let amount = U256::from_big_endian(&log.data[0..32]);

                                    // Get token address and fetch its decimals for proper conversion
                                    let token_address = log.address;
                                    let decimals = self.strategy_context.blockchain_manager
                                        .get_token_decimals(token_address, None)
                                        .await
                                        .unwrap_or(18); // Default to 18 if unable to fetch

                                    // Get token price for USD conversion
                                    let chain_name_for_price = self.strategy_context.blockchain_manager.get_chain_name();
                                    let token_price = self.strategy_context.price_oracle
                                        .get_price(&chain_name_for_price, token_address)
                                        .await
                                        .unwrap_or(1.0); // Default to $1 for stablecoins if price fetch fails

                                    // Convert to USD value
                                    let token_amount = amount.as_u128() as f64 / (10_f64.powi(decimals as i32));
                                    let value_usd = token_amount * token_price;

                                    // Add to total captured value (now in USD)
                                    total_value_captured = total_value_captured
                                        .checked_add(U256::from((value_usd * 1e6) as u128)) // Store as 6 decimal USD
                                        .ok_or_else(|| MEVEngineError::ArithmeticError("Value captured overflow".into()))?;
                                }
                            }
                        }

                        // Track swap events for additional metrics
                        if topic0 == uniswap_v2_swap_topic || topic0 == uniswap_v3_swap_topic {
                            // Parse swap amounts for detailed analysis
                            debug!("Swap event detected in tx {}: {:?}", tx_hash, log);
                        }
                    }
                }
            }
        }

        // Validate gas usage against estimate
        if let Some(estimated_gas) = estimated_gas_used {
            if estimated_gas > U256::zero() && actual_gas_used > estimated_gas {
                let actual_gas = actual_gas_used.as_u64();
                let estimated = estimated_gas.as_u64();
                let gas_overrun_percentage = ((actual_gas.saturating_sub(estimated)) as f64 / estimated as f64) * 100.0;
                warn!("Gas usage exceeded estimate by {:.2}%", gas_overrun_percentage);
            }
        }

        // Convert gas cost to USD
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let eth_price = self.strategy_context.price_oracle
            .get_price(chain_name, self.strategy_context.blockchain_manager.get_weth_address())
            .await
            .map_err(|e| MEVEngineError::PricingError(format!("Failed to fetch ETH price: {}", e)))?;

        let gas_cost_usd = (total_gas_cost.as_u128() as f64 / 1e18) * eth_price;

        // Calculate actual profit: value captured minus gas costs
        let value_captured_usd = if total_value_captured > U256::zero() {
            // Assume captured value is in USDC/USDT (6 decimals) or convert from token price
            total_value_captured.as_u128() as f64 / 1e6
        } else {
            // Fall back to estimated profit if we couldn't parse value from logs
            play_wrapper.read(|play| play.estimated_profit_usd).await
        };

        let actual_profit = value_captured_usd - gas_cost_usd;

        Ok(actual_profit.max(0.0))
    }

    async fn get_transaction_receipt_with_retry(
        &self,
        tx_hash: &H256,
    ) -> Result<Option<ethers::types::TransactionReceipt>, MEVEngineError> {
        let mut attempts = 0;
        
        while attempts < RECEIPT_CHECK_RETRIES {
            match self.strategy_context.blockchain_manager
                .get_transaction_receipt(*tx_hash)
                .await
            {
                Ok(receipt) => return Ok(receipt),
                Err(e) => {
                    attempts += 1;
                    if attempts < RECEIPT_CHECK_RETRIES {
                        debug!("Retrying receipt check for {} (attempt {})", tx_hash, attempts);
                        tokio::time::sleep(Duration::from_millis(RECEIPT_CHECK_DELAY_MS)).await;
                    } else {
                        return Err(MEVEngineError::NetworkError(
                            format!("Failed to get receipt after {} attempts: {}", attempts, e)
                        ));
                    }
                }
            }
        }
        
        Ok(None)
    }

    async fn query_bundle_status(&self, bundle_hash: &H256) -> Result<BundleStatus, MEVEngineError> {
        // Get the provider client for potential direct RPC calls
        let provider = self.strategy_context.blockchain_manager.get_provider();
        
        // Construct the JSON-RPC request for Flashbots bundle status
        let bundle_hash_str = format!("0x{:x}", bundle_hash);
        
        // Get the Flashbots relay endpoint
        let chain_name = self.strategy_context.blockchain_manager.get_chain_name();
        let flashbots_relay_url = match &*chain_name {
            "ethereum" => "https://relay.flashbots.net",
            "goerli" => "https://relay-goerli.flashbots.net", 
            "sepolia" => "https://relay-sepolia.flashbots.net",
            _ => {
                // For non-Flashbots chains, use the provider to check transaction receipts directly
                debug!("Non-Flashbots chain, using provider to check bundle transactions");
                return self.check_bundle_transactions_status_with_provider(bundle_hash, Arc::clone(&provider)).await;
            }
        };
        
        // Try to use the provider's request method for Flashbots RPC if available
        // This allows us to leverage connection pooling and configured middleware
        // Using the correct Flashbots API method: flashbots_getBundleStats
        let request_result = provider.request::<_, serde_json::Value>(
            "flashbots_getBundleStats",
            vec![json!({
                "bundleHash": bundle_hash_str.clone(),
                "blockNumber": format!("0x{:x}", self.strategy_context.blockchain_manager.get_block_number().await.unwrap_or(0))
            })]
        ).await;
        
        match request_result {
            Ok(result) => {
                // Successfully got response from provider
                // Check if bundle is included (isSimulated and isSentToMiners fields)
                let is_simulated = result.get("isSimulated").and_then(|v| v.as_bool()).unwrap_or(false);
                let is_sent = result.get("isSentToMiners").and_then(|v| v.as_bool()).unwrap_or(false);
                
                if let Some(considered_by_builders) = result.get("consideredByBuildersAt").and_then(|v| v.as_array()) {
                    if !considered_by_builders.is_empty() {
                        // Bundle was considered by builders, check if included
                        if let Some(sealed_by_builders) = result.get("sealedByBuildersAt").and_then(|v| v.as_array()) {
                            if !sealed_by_builders.is_empty() {
                                // Bundle was sealed/included - don't parse block numbers from sealedByBuildersAt
                        // Instead, determine inclusion via transaction receipts
                        debug!("Bundle {} was sealed by builders, checking transaction receipts for inclusion block", bundle_hash);
                        return self.check_bundle_transactions_status(bundle_hash).await;
                            }
                        }
                    }
                }
                
                if !is_simulated {
                    return Ok(BundleStatus::Failed("Bundle failed simulation".to_string()));
                }
                
                if is_simulated && is_sent {
                    return Ok(BundleStatus::Pending);
                }
                
                Ok(BundleStatus::Pending)
            }
            Err(provider_err) => {
                // Provider doesn't support this method or failed, fallback to HTTP client
                debug!("Provider request failed ({}), falling back to HTTP client", provider_err);
                
                let current_block = self.strategy_context.blockchain_manager.get_block_number().await.unwrap_or(0);
                let request = json!({
                    "jsonrpc": "2.0",
                    "method": "flashbots_getBundleStats",
                    "params": [{
                        "bundleHash": &bundle_hash_str,
                        "blockNumber": format!("0x{:x}", current_block)
                    }],
                    "id": 1
                });
                
                let http_client = match reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                {
                    Ok(client) => client,
                    Err(e) => {
                        warn!("Failed to create HTTP client: {}. Using provider for transaction checks", e);
                        return self.check_bundle_transactions_status_with_provider(bundle_hash, Arc::clone(&provider)).await;
                    }
                };
                let response = match http_client
                    .post(flashbots_relay_url)
                    .header("Content-Type", "application/json")
                    .json(&request)
                    .send()
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("Failed to query Flashbots relay: {}. Using provider for transaction checks", e);
                        return self.check_bundle_transactions_status_with_provider(bundle_hash, Arc::clone(&provider)).await;
                    }
                };
                
                // Parse the response
                match response.json::<serde_json::Value>().await {
                    Ok(json_response) => {
                        if let Some(result) = json_response.get("result") {
                            // Check if bundle is included (isSimulated and isSentToMiners fields)
                            let is_simulated = result.get("isSimulated").and_then(|v| v.as_bool()).unwrap_or(false);
                            let is_sent = result.get("isSentToMiners").and_then(|v| v.as_bool()).unwrap_or(false);
                            
                            if let Some(considered_by_builders) = result.get("consideredByBuildersAt").and_then(|v| v.as_array()) {
                                if !considered_by_builders.is_empty() {
                                    // Bundle was considered by builders, check if included
                                    if let Some(sealed_by_builders) = result.get("sealedByBuildersAt").and_then(|v| v.as_array()) {
                                        if !sealed_by_builders.is_empty() {
                                            // Bundle was sealed/included - don't parse block numbers from sealedByBuildersAt
                                            // Instead, determine inclusion via transaction receipts
                                            debug!("Bundle {} was sealed by builders, checking transaction receipts for inclusion block", bundle_hash);
                                            return self.check_bundle_transactions_status(bundle_hash).await;
                                        }
                                    }
                                }
                            }
                            
                            if !is_simulated {
                                return Ok(BundleStatus::Failed("Bundle failed simulation".to_string()));
                            }
                            
                            if is_simulated && is_sent {
                                return Ok(BundleStatus::Pending);
                            }
                        }
                        
                        // If we can't parse the expected format, treat as pending
                        debug!("Unexpected Flashbots response format: {:?}", json_response);
                        Ok(BundleStatus::Pending)
                    }
                    Err(e) => {
                        warn!("Failed to parse Flashbots response: {}. Using provider for transaction checks", e);
                        self.check_bundle_transactions_status_with_provider(bundle_hash, Arc::clone(&provider)).await
                    }
                }
            }
        }
    }
    
    async fn check_bundle_transactions_status(&self, bundle_hash: &H256) -> Result<BundleStatus, MEVEngineError> {
        // Fallback method: check if any transactions from the bundle are included
        // This requires tracking bundle transactions separately
        
        // Try to get bundle transactions from active plays
        for entry in self.active_plays.iter() {
            let play_wrapper = entry.value();
            let (play_bundle_hash, tx_hashes) = play_wrapper.read(|play| {
                (play.submitted_bundle_hash.clone(), play.submitted_tx_hashes.clone())
            }).await;
            
            if let Some(stored_bundle_hash) = play_bundle_hash {
                if stored_bundle_hash == *bundle_hash && !tx_hashes.is_empty() {
                    // Check if any transaction is included
                    for tx_hash in &tx_hashes {
                        if let Some(receipt) = self.get_transaction_receipt_with_retry(tx_hash).await? {
                            if let Some(block_number) = receipt.block_number {
                                return Ok(BundleStatus::Included(block_number.as_u64()));
                            }
                        }
                    }
                    
                    // All transactions checked, none included
                    return Ok(BundleStatus::Pending);
                }
            }
        }
        
        // Bundle not found in active plays
        debug!("Bundle {} not found in active plays, treating as pending", bundle_hash);
        Ok(BundleStatus::Pending)
    }
    
    async fn check_bundle_transactions_status_with_provider(
        &self,
        bundle_hash: &H256,
        provider: Arc<ethers::providers::Provider<ethers::providers::Http>>
    ) -> Result<BundleStatus, MEVEngineError> {
        // Use the provided provider to check transaction status
        // This is more efficient than creating new connections
        
        // Try to get bundle transactions from active plays
        for entry in self.active_plays.iter() {
            let play_wrapper = entry.value();
            let (play_bundle_hash, tx_hashes) = play_wrapper.read(|play| {
                (play.submitted_bundle_hash.clone(), play.submitted_tx_hashes.clone())
            }).await;
            
            if let Some(stored_bundle_hash) = play_bundle_hash {
                if stored_bundle_hash == *bundle_hash && !tx_hashes.is_empty() {
                    // Check transactions using the provider
                    for tx_hash in &tx_hashes {
                        match provider.get_transaction_receipt(*tx_hash).await {
                            Ok(Some(receipt)) => {
                                if let Some(block_number) = receipt.block_number {
                                    info!("Bundle {} transaction {} included in block {}", 
                                          bundle_hash, tx_hash, block_number);
                                    return Ok(BundleStatus::Included(block_number.as_u64()));
                                }
                            }
                            Ok(None) => {
                                // Transaction not yet mined
                                debug!("Transaction {} from bundle {} not yet mined", tx_hash, bundle_hash);
                            }
                            Err(e) => {
                                // Log error but continue checking other transactions
                                debug!("Error checking transaction {} receipt: {}", tx_hash, e);
                            }
                        }
                    }
                    
                    // Check if transactions might have been dropped
                    // by comparing against current block number
                    match provider.get_block_number().await {
                        Ok(current_block) => {
                            let submission_block = play_wrapper.read(|play| play.target_block.unwrap_or(0)).await;
                            let blocks_passed = current_block.as_u64().saturating_sub(submission_block);
                            
                            if blocks_passed > MAX_PENDING_BLOCKS {
                                warn!("Bundle {} transactions not included after {} blocks, likely dropped",
                                      bundle_hash, blocks_passed);
                                return Ok(BundleStatus::Failed(
                                    format!("Bundle not included after {} blocks", blocks_passed)
                                ));
                            }
                        }
                        Err(e) => {
                            debug!("Failed to get current block number: {}", e);
                        }
                    }
                    
                    return Ok(BundleStatus::Pending);
                }
            }
        }
        
        // Bundle not found in active plays
        debug!("Bundle {} not found in active plays, treating as pending", bundle_hash);
        Ok(BundleStatus::Pending)
    }

    async fn get_revert_reason(&self, tx_hash: &H256) -> Result<String, MEVEngineError> {
        // Try to get revert reason using debug_traceTransaction
        let provider = self.strategy_context.blockchain_manager.get_provider();

        // Use debug_traceTransaction to get the revert reason
        let trace_request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [format!("0x{:x}", tx_hash), {
                "tracer": "callTracer",
                "tracerConfig": {
                    "onlyTopCall": false,
                    "withLog": false
                }
            }],
            "id": 1
        });

        match provider.request::<_, serde_json::Value>("debug_traceTransaction", vec![
            serde_json::Value::String(format!("0x{:x}", tx_hash)),
            serde_json::json!({
                "tracer": "callTracer"
            })
        ]).await {
            Ok(trace_result) => {
                // Parse the trace result to extract revert reason
                if let Some(error) = trace_result.get("error") {
                    if let Some(error_msg) = error.as_str() {
                        return Ok(format!("Transaction reverted: {}", error_msg));
                    }
                }

                // Look for revert in the trace output
                if let Some(output) = trace_result.get("output") {
                    if let Some(output_str) = output.as_str() {
                        if output_str.starts_with("0x08c379a") { // Error(string) function selector
                            // Try to decode the error string
                            if let Ok(decoded) = self.decode_error_string(output_str) {
                                return Ok(format!("Transaction reverted: {}", decoded));
                            }
                        }
                    }
                }

                // Fallback to generic revert message
                Ok(format!("Transaction {} reverted", tx_hash))
            }
            Err(e) => {
                debug!("Failed to trace transaction {}: {}", tx_hash, e);
                // Fallback to generic revert message
                Ok(format!("Transaction {} reverted", tx_hash))
            }
        }
    }

    /// Decode error string from hex-encoded revert data
    fn decode_error_string(&self, hex_data: &str) -> Result<String, MEVEngineError> {
        if hex_data.len() < 138 { // Minimum length for Error(string) with empty string
            return Err(MEVEngineError::ValidationError("Insufficient data for error decoding".into()));
        }

        // Skip function selector (4 bytes) and string offset (32 bytes) = 36 bytes total
        let string_offset = 36;
        let length_offset = string_offset + 32; // String length is at offset 68

        if hex_data.len() < length_offset + 64 {
            return Err(MEVEngineError::ValidationError("Insufficient data for string length".into()));
        }

        // Extract string length (32 bytes, big-endian)
        let length_hex = &hex_data[length_offset..length_offset + 64];
        let length = u64::from_str_radix(length_hex, 16)
            .map_err(|_| MEVEngineError::ValidationError("Invalid string length".into()))?;

        if length == 0 {
            return Ok("".to_string());
        }

        // Extract string data
        let data_start = length_offset + 64;
        let data_end = data_start + (length as usize * 2); // 2 hex chars per byte

        if hex_data.len() < data_end {
            return Err(MEVEngineError::ValidationError("Insufficient data for string content".into()));
        }

        let string_hex = &hex_data[data_start..data_end];

        // Convert hex to string
        let bytes = hex::decode(string_hex)
            .map_err(|_| MEVEngineError::ValidationError("Invalid hex in error string".into()))?;

        String::from_utf8(bytes)
            .map_err(|_| MEVEngineError::ValidationError("Invalid UTF-8 in error string".into()))
    }
}

#[derive(Debug)]
enum BundleStatus {
    Included(u64),
    Failed(String),
    Pending,
}