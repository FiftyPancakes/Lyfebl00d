use crate::{
    errors::{MEVEngineError},
    mempool::{PotentialLiquidationOpportunity, JITLiquidityOpportunity},
    types::{PoolInfo, RateEstimate, ProtocolType, FeeData},
    pool_manager::PoolManagerTrait,
    blockchain::BlockchainManager,
    price_oracle::PriceOracle,
    dex_math,
    config::Config,
};
use crate::gas_oracle::GasOracleProvider;
use ethers::types::{Address, U256};
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, instrument};

/// Production-grade simulation engine that uses proper DEX math for all calculations
pub struct SimulationEngine {
    pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    blockchain_manager: Arc<dyn BlockchainManager>,
    price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    config: Arc<Config>,
}

impl SimulationEngine {
    pub fn new(
        pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
        blockchain_manager: Arc<dyn BlockchainManager>,
        price_oracle: Arc<dyn PriceOracle + Send + Sync>,
        gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            pool_manager,
            blockchain_manager,
            price_oracle,
            gas_oracle,
            config,
        }
    }

    /// Choose the pool that maximizes amount_out for the given hop, using current (possibly mutated)
    /// pool state if available; otherwise fetch from the pool_manager.
    async fn select_best_pool_for_hop(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        pool_states: &HashMap<Address, PoolInfo>,
        block_number: Option<u64>,
    ) -> Result<(Address, PoolInfo, U256), MEVEngineError> {
        let chain = self.blockchain_manager.get_chain_name();
        let pools = self.pool_manager
            .get_pools_for_pair(&chain, token_in, token_out)
            .await
            .map_err(|e| MEVEngineError::SimulationError(format!("pool lookup failed: {e}")))?;

        let mut best: Option<(Address, PoolInfo, U256)> = None;

        for addr in pools {
            let info = if let Some(s) = pool_states.get(&addr) {
                s.clone()
            } else {
                self.pool_manager
                    .get_pool_info(&chain, addr, block_number)
                    .await
                    .map_err(|e| MEVEngineError::SimulationError(format!("pool info fetch failed: {e}")))?
            };

            if let Ok(out) = dex_math::get_amount_out(&info, token_in, amount_in) {
                match &best {
                    None => best = Some((addr, info, out)),
                    Some((_, _, best_out)) if out > *best_out => best = Some((addr, info, out)),
                    _ => {}
                }
            }
        }

        best.ok_or_else(|| MEVEngineError::SimulationError(
            format!("no viable pool for {token_in:?} -> {token_out:?} (amount_in={amount_in})")
        ))
    }

    /// Safe U256 to f64 conversion with decimal handling
    #[inline]
    fn u256_to_f64_with_decimals(x: U256, decimals: u8) -> f64 {
        let base = U256::exp10(decimals as usize);
        let q = x / base;
        let r = x % base;
        (q.as_u128() as f64) + (r.as_u128() as f64) / 10f64.powi(decimals as i32)
    }

    /// Calculate effective gas price in wei (EIP-1559 aware)
    fn effective_gas_price_wei(fee: &FeeData) -> U256 {
        let base = fee.base_fee_per_gas.unwrap_or_default();
        let prio = fee.max_priority_fee_per_gas.unwrap_or_default();
        let eip = base.saturating_add(prio);
        if eip.is_zero() { fee.gas_price.unwrap_or_default() } else { eip }
    }

    /// Simulate a multi-hop swap through multiple pools with state tracking
    #[instrument(skip(self, initial_pool_states), fields(path_length = path.len()))]
    pub async fn simulate_multihop_swap(
        &self,
        path: &[Address],
        amount_in_initial: U256,
        initial_pool_states: &HashMap<Address, PoolInfo>,
        block_number: Option<u64>,
    ) -> Result<(U256, HashMap<Address, PoolInfo>), MEVEngineError> {
        if path.len() < 2 {
            return Err(MEVEngineError::StrategyError(
                "swap path must contain ≥ 2 tokens".into(),
            ));
        }

        let mut current_amount_in = amount_in_initial;
        let mut pool_states = initial_pool_states.clone();

        for (hop_index, window) in path.windows(2).enumerate() {
            let (token_in, token_out) = (window[0], window[1]);

            let (pool_addr, pool_info_before, amount_out) = self
                .select_best_pool_for_hop(token_in, token_out, current_amount_in, &pool_states, block_number)
                .await?;

            let (amount_out_final, new_pool_state) = self
                .simulate_swap_and_get_new_state(&pool_info_before, token_in, token_out, current_amount_in)
                .await?;

            // sanity: DEX math inside simulate should match the selection preview
            debug_assert_eq!(amount_out, amount_out_final);

            if amount_out_final.is_zero() && !current_amount_in.is_zero() {
                return Err(MEVEngineError::SimulationError(
                    format!("zero output on hop {} ({token_in:?}→{token_out:?})", hop_index + 1)
                ));
            }

            current_amount_in = amount_out_final;
            pool_states.insert(pool_addr, new_pool_state);
        }

        Ok((current_amount_in, pool_states))
    }

    /// Simulate multihop swap and return final state
    pub async fn simulate_multihop_swap_and_get_final_state(
        &self,
        path: &[Address],
        amount_in_initial: U256,
        block_number: Option<u64>,
    ) -> Result<(U256, HashMap<Address, PoolInfo>), MEVEngineError> {
        let initial_pool_states = HashMap::new();
        self.simulate_multihop_swap(path, amount_in_initial, &initial_pool_states, block_number).await
    }

    /// Simulate multihop swap with optional initial state and return final state
    pub async fn simulate_multihop_swap_and_get_final_state_with_state(
        &self,
        path: &[Address],
        amount_in_initial: U256,
        initial_pool_states: Option<&HashMap<Address, PoolInfo>>,
        block_number: Option<u64>,
    ) -> Result<(U256, HashMap<Address, PoolInfo>), MEVEngineError> {
        let empty_map = HashMap::new();
        let initial_pool_states = initial_pool_states.unwrap_or(&empty_map);
        self.simulate_multihop_swap(path, amount_in_initial, initial_pool_states, block_number).await
    }

    /// Simulate multihop swap with provided state and return rate estimate
    #[instrument(skip(self, pool_states), fields(path_length = path.len()))]
    pub async fn simulate_multihop_swap_with_state(
        &self,
        path: &[Address],
        amount_in_initial: U256,
        pool_states: &HashMap<Address, PoolInfo>,
        block_number: Option<u64>,
    ) -> Result<RateEstimate, MEVEngineError> {
        let (final_amount, updated_states) = self
            .simulate_multihop_swap(path, amount_in_initial, pool_states, block_number)
            .await?;

        // Calculate total gas estimate for the multihop
        let gas_estimate = path.len().saturating_sub(1) as u64 * 150_000; // Conservative estimate per hop
        
        // Get gas price from oracle
        let chain = self.blockchain_manager.get_chain_name();
        let gas_price_result = self.gas_oracle.get_gas_price(&chain, block_number).await;
        
        let gas_cost_wei = if let Ok(gas_price) = gas_price_result {
            let effective_price = gas_price.base_fee.saturating_add(gas_price.priority_fee);
            U256::from(gas_estimate)
                .checked_mul(effective_price)
                .unwrap_or(U256::zero())
        } else {
            // Fallback gas cost estimate
            U256::from(gas_estimate)
                .checked_mul(U256::from(20_000_000_000u64)) // 20 gwei fallback
                .unwrap_or(U256::zero())
        };

        // Get the first pool used for the first hop
        let first_hop_pool = {
            let (token_in, token_out) = (path[0], path[1]);
            let (addr, _, _) = self
                .select_best_pool_for_hop(token_in, token_out, amount_in_initial, pool_states, block_number)
                .await?;
            addr
        };

        Ok(RateEstimate {
            amount_out: final_amount,
            fee_bps: 3000, // Default fee for multihop
            price_impact_bps: self.calculate_multihop_price_impact(path, amount_in_initial, &updated_states).await?,
            gas_estimate: gas_cost_wei,
            pool_address: first_hop_pool, // ✅ an actual pool
        })
    }

    /// Core swap simulation using proper DEX math
    #[instrument(skip(self, pool), fields(pool_address = %pool.address, protocol = ?pool.protocol_type))]
    pub async fn simulate_swap_and_get_new_state(
        &self,
        pool: &PoolInfo,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Result<(U256, PoolInfo), MEVEngineError> {
        if amount_in.is_zero() {
            return Ok((U256::zero(), pool.clone()));
        }

        // Validate token pair exists in pool
        if (pool.token0 != token_in && pool.token1 != token_in) ||
           (pool.token0 != token_out && pool.token1 != token_out) {
            return Err(MEVEngineError::SimulationError(
                format!("Invalid token pair {token_in:?}->{token_out:?} for pool {}", pool.address)
            ));
        }

        if token_in == token_out {
            return Err(MEVEngineError::SimulationError(
                "Cannot swap token to itself".to_string()
            ));
        }

        // Use proper dex_math calculations
        let amount_out = dex_math::get_amount_out(pool, token_in, amount_in)
            .map_err(|e| MEVEngineError::SimulationError(format!("DEX math calculation failed: {e:?}")))?;

        // Calculate new pool state after swap
        let new_pool_state = self.calculate_new_pool_state(pool, token_in, token_out, amount_in, amount_out)?;

        debug!(
            pool_address = %pool.address,
            protocol = ?pool.protocol_type,
            token_in = %token_in,
            token_out = %token_out,
            amount_in = %amount_in,
            amount_out = %amount_out,
            "Swap simulation completed"
        );

        Ok((amount_out, new_pool_state))
    }

    /// Simulate liquidation with configuration-driven parameters
    #[instrument(skip(self, opp), fields(debt_token = %opp.debt_token, collateral_token = %opp.collateral_token))]
    pub async fn simulate_liquidation_and_swap(
        &self,
        opp: &PotentialLiquidationOpportunity,
    ) -> Result<(U256, U256, U256, Address), MEVEngineError> {
        // Get liquidation parameters from config instead of hardcoded values
        let liquidation_config = self.config.module_config.as_ref().and_then(|m| m.liquidation_config.as_ref()).ok_or_else(|| MEVEngineError::Configuration("Liquidation config not found".to_string()))?;
        let bonus_bps = liquidation_config.liquidation_bonus_bps.unwrap_or(500);
        let flash_fee_bps = liquidation_config.flash_loan_fee_bps.unwrap_or(9);

        // Calculate flash loan fee safely
        let flash_fee = if opp.uar_connector.is_some() {
            opp.debt_to_cover
                .checked_mul(U256::from(flash_fee_bps))
                .and_then(|v| v.checked_div(U256::from(10_000u64)))
                .ok_or_else(|| MEVEngineError::SimulationError("Flash fee calculation overflow".to_string()))?
        } else {
            U256::zero()
        };

        let total_debt = opp.debt_to_cover
            .checked_add(flash_fee)
            .ok_or_else(|| MEVEngineError::SimulationError("Total debt calculation overflow".to_string()))?;

        // Estimate token conversion using proper price oracles and DEX routing
        let collateral_equivalent = self.estimate_token_conversion_precise(
            total_debt,
            opp.debt_token,
            opp.collateral_token,
            None, // Use current block
        ).await?;

        // Calculate liquidation bonus safely
        let bonus_amount = collateral_equivalent
            .checked_mul(U256::from(bonus_bps))
            .and_then(|v| v.checked_div(U256::from(10_000u64)))
            .ok_or_else(|| MEVEngineError::SimulationError("Bonus calculation overflow".to_string()))?;

        let collateral_received = collateral_equivalent
            .checked_add(bonus_amount)
            .ok_or_else(|| MEVEngineError::SimulationError("Collateral received calculation overflow".to_string()))?;

        debug!(
            debt_to_cover = %opp.debt_to_cover,
            flash_fee = %flash_fee,
            collateral_equivalent = %collateral_equivalent,
            bonus_amount = %bonus_amount,
            collateral_received = %collateral_received,
            "Liquidation simulation completed"
        );

        Ok((
            collateral_received,
            collateral_equivalent,
            flash_fee,
            opp.collateral_token,
        ))
    }

    /// Simulate JIT liquidity with configuration-driven parameters
    #[instrument(skip(self, opp), fields(pool_address = %opp.pool_address, fee = %opp.fee))]
    pub async fn simulate_jit_liquidity(
        &self,
        opp: &JITLiquidityOpportunity,
    ) -> Result<(U256, U256), MEVEngineError> {
        // Get JIT liquidity parameters from config instead of hardcoded values
        let jit_config = self.config.module_config.as_ref().and_then(|m| m.jit_liquidity_config.as_ref()).ok_or_else(|| MEVEngineError::Configuration("JIT config not found".to_string()))?;
        let interacted_fraction = jit_config.expected_interaction_fraction.unwrap_or(0.15);
        let fee_multiplier = jit_config.fee_multiplier.unwrap_or(1.0);

        // Calculate fee rate safely
        let fee_rate = (opp.fee as f64 / 1_000_000.0) * fee_multiplier;
        if !(0.0..=1.0).contains(&fee_rate) {
            return Err(MEVEngineError::SimulationError(
                format!("Invalid fee rate calculated: {}", fee_rate)
            ));
        }

        let chain = self.blockchain_manager.get_chain_name();
        
        // Get prices and decimals with proper error handling
        let (p0_usd_res, p1_usd_res, d0_res, d1_res) = tokio::join!(
            self.price_oracle.get_token_price_usd(&chain, opp.token0),
            self.price_oracle.get_token_price_usd(&chain, opp.token1),
            self.blockchain_manager.get_token_decimals(opp.token0, None),
            self.blockchain_manager.get_token_decimals(opp.token1, None)
        );
        let (p0_usd, p1_usd, d0, d1) = (p0_usd_res?, p1_usd_res?, d0_res.map_err(|e| MEVEngineError::Blockchain(e.to_string()))?, d1_res.map_err(|e| MEVEngineError::Blockchain(e.to_string()))?);

        if p0_usd <= 0.0 || p1_usd <= 0.0 {
            return Err(MEVEngineError::SimulationError(
                format!("Invalid token prices: token0=${}, token1=${}", p0_usd, p1_usd)
            ));
        }

        // Calculate token values in USD with safe arithmetic using improved conversion
        let v0 = Self::u256_to_f64_with_decimals(opp.amount0_desired, d0) * p0_usd;
        let v1 = Self::u256_to_f64_with_decimals(opp.amount1_desired, d1) * p1_usd;
        let total_value = v0 + v1;

        if total_value <= 0.0 {
            return Ok((U256::zero(), U256::zero()));
        }

        // Calculate expected fees based on liquidity interaction
        let fees_usd = total_value * interacted_fraction * fee_rate;
        
        // Distribute fees proportionally to deposited value
        let fees0_usd = fees_usd * (v0 / total_value);
        let fees1_usd = fees_usd * (v1 / total_value);

        // Convert fees back to token amounts with safe conversion
        let fees0_tokens_raw = (fees0_usd / p0_usd * 10f64.powi(d0 as i32))
            .max(0.0)
            .min(u128::MAX as f64);
        let fees1_tokens_raw = (fees1_usd / p1_usd * 10f64.powi(d1 as i32))
            .max(0.0)
            .min(u128::MAX as f64);

        // Ensure results are finite before casting
        let fees0_tokens = if fees0_tokens_raw.is_finite() {
            fees0_tokens_raw.round() as u128
        } else {
            return Err(MEVEngineError::SimulationError(
                format!("JIT fee calculation overflow for token0: {}", fees0_tokens_raw)
            ));
        };

        let fees1_tokens = if fees1_tokens_raw.is_finite() {
            fees1_tokens_raw.round() as u128
        } else {
            return Err(MEVEngineError::SimulationError(
                format!("JIT fee calculation overflow for token1: {}", fees1_tokens_raw)
            ));
        };

        debug!(
            pool_address = %opp.pool_address,
            fee_rate = %fee_rate,
            interacted_fraction = %interacted_fraction,
            total_value_usd = %total_value,
            fees_usd = %fees_usd,
            fees0_tokens = %fees0_tokens,
            fees1_tokens = %fees1_tokens,
            "JIT liquidity simulation completed"
        );

        Ok((U256::from(fees0_tokens), U256::from(fees1_tokens)))
    }

    /// Simple token conversion (legacy interface)
    #[instrument(skip(self), fields(token_in = %token_in, token_out = %token_out))]
    pub async fn estimate_token_conversion(
        &self,
        amount_in: U256,
        token_in: Address,
        token_out: Address,
    ) -> Result<U256, MEVEngineError> {
        self.estimate_token_conversion_precise(amount_in, token_in, token_out, None).await
    }

    /// Precise token conversion using actual DEX routing when possible, price oracle as fallback
    #[instrument(skip(self), fields(token_in = %token_in, token_out = %token_out))]
    pub async fn estimate_token_conversion_precise(
        &self,
        amount_in: U256,
        token_in: Address,
        token_out: Address,
        block_number: Option<u64>,
    ) -> Result<U256, MEVEngineError> {
        if token_in == token_out || amount_in.is_zero() {
            return Ok(amount_in);
        }

        let chain = self.blockchain_manager.get_chain_name();
        
        // Try to find a direct pool first for more accurate conversion
        if let Ok(pools) = self.pool_manager.get_pools_for_pair(&chain, token_in, token_out).await {
            if !pools.is_empty() {
                // select best instead of pools[0]
                let (_addr, _pool_info, amount_out) = self
                    .select_best_pool_for_hop(token_in, token_out, amount_in, &HashMap::new(), block_number)
                    .await?;
                debug!(%token_in, %token_out, %amount_in, %amount_out, "Token conversion via best direct pool");
                return Ok(amount_out);
            }
        }

        // Fallback to price oracle conversion
        let (price_in_res, price_out_res, dec_in_res, dec_out_res) = tokio::join!(
            self.price_oracle.get_token_price_usd_at(&chain, token_in, block_number, None),
            self.price_oracle.get_token_price_usd_at(&chain, token_out, block_number, None),
            self.blockchain_manager.get_token_decimals(token_in, None),
            self.blockchain_manager.get_token_decimals(token_out, None)
        );

        let (price_in, price_out, dec_in, dec_out) = (price_in_res?, price_out_res?, dec_in_res.map_err(|e| MEVEngineError::Blockchain(e.to_string()))?, dec_out_res.map_err(|e| MEVEngineError::Blockchain(e.to_string()))?);

        if price_in <= 0.0 || price_out <= 0.0 {
            return Err(MEVEngineError::PricingError(
                format!("Invalid token prices: {token_in}=${price_in}, {token_out}=${price_out}")
            ));
        }

        // Safe conversion calculation using improved U256→f64 conversion
        let amount_in_std = Self::u256_to_f64_with_decimals(amount_in, dec_in);
        let usd_value = amount_in_std * price_in;
        let amount_out_std = usd_value / price_out;
        let amount_out_raw = amount_out_std * 10f64.powi(dec_out as i32);
        
        // Ensure result doesn't overflow and is finite
        let amount_out = if amount_out_raw.is_finite() && amount_out_raw >= 0.0 && amount_out_raw <= u128::MAX as f64 {
            amount_out_raw.round() as u128
        } else {
            return Err(MEVEngineError::SimulationError(
                format!("Token conversion result overflow or invalid: {}", amount_out_raw)
            ));
        };

        debug!(
            token_in = %token_in,
            token_out = %token_out,
            amount_in = %amount_in,
            amount_out = %amount_out,
            price_in = %price_in,
            price_out = %price_out,
            "Token conversion via price oracle"
        );

        Ok(U256::from(amount_out))
    }

    /// Calculate new pool state after a swap
    pub fn calculate_new_pool_state(
        &self,
        pool: &PoolInfo,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        amount_out: U256,
    ) -> Result<PoolInfo, MEVEngineError> {
        // Validate that token_out is the other token in the pool
        let expected_token_out = if pool.token0 == token_in {
            pool.token1
        } else if pool.token1 == token_in {
            pool.token0
        } else {
            return Err(MEVEngineError::SimulationError(
                format!("Token {} is not in pool with tokens {} and {}", token_in, pool.token0, pool.token1)
            ));
        };

        if token_out != expected_token_out {
            return Err(MEVEngineError::SimulationError(
                format!("Token out mismatch: expected {}, got {}", expected_token_out, token_out)
            ));
        }

        let mut new_pool = pool.clone();
        
        match pool.protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                // Update V2 reserves
                if let (Some(reserve0), Some(reserve1)) = (pool.v2_reserve0, pool.v2_reserve1) {
                    if pool.token0 == token_in {
                        new_pool.v2_reserve0 = Some(
                            reserve0.checked_add(amount_in)
                                .ok_or_else(|| MEVEngineError::SimulationError("V2 reserve0 update overflow".to_string()))?
                        );
                        new_pool.v2_reserve1 = Some(
                            reserve1.checked_sub(amount_out)
                                .ok_or_else(|| MEVEngineError::SimulationError("V2 reserve1 update underflow".to_string()))?
                        );
                    } else {
                        new_pool.v2_reserve1 = Some(
                            reserve1.checked_add(amount_in)
                                .ok_or_else(|| MEVEngineError::SimulationError("V2 reserve1 update overflow".to_string()))?
                        );
                        new_pool.v2_reserve0 = Some(
                            reserve0.checked_sub(amount_out)
                                .ok_or_else(|| MEVEngineError::SimulationError("V2 reserve0 update underflow".to_string()))?
                        );
                    }
                    // Keep reserves0/reserves1 in sync
                    new_pool.reserves0 = new_pool.v2_reserve0.unwrap();
                    new_pool.reserves1 = new_pool.v2_reserve1.unwrap();
                }
            }
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => {
                // For V3, we need to update the sqrt price and potentially tick data
                // This is a simplified update - in a full implementation, we'd calculate new sqrt_price_x96
                if pool.token0 == token_in {
                    new_pool.reserves0 = pool.reserves0.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("V3 reserve0 update overflow".to_string()))?;
                    new_pool.reserves1 = pool.reserves1.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("V3 reserve1 update underflow".to_string()))?;
                } else {
                    new_pool.reserves1 = pool.reserves1.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("V3 reserve1 update overflow".to_string()))?;
                    new_pool.reserves0 = pool.reserves0.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("V3 reserve0 update underflow".to_string()))?;
                }
                // Note: In production, we would also update sqrt_price_x96, tick data, etc.
            }
            ProtocolType::Curve | ProtocolType::Balancer => {
                // Update virtual reserves for Curve/Balancer
                if pool.token0 == token_in {
                    new_pool.reserves0 = pool.reserves0.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve0 update overflow".to_string()))?;
                    new_pool.reserves1 = pool.reserves1.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve1 update underflow".to_string()))?;
                } else {
                    new_pool.reserves1 = pool.reserves1.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve1 update overflow".to_string()))?;
                    new_pool.reserves0 = pool.reserves0.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve0 update underflow".to_string()))?;
                }
            }
            ProtocolType::Unknown | ProtocolType::Other(_) => {
                // For unknown protocols, do basic reserve updates
                if pool.token0 == token_in {
                    new_pool.reserves0 = pool.reserves0.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve0 update overflow".to_string()))?;
                    new_pool.reserves1 = pool.reserves1.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve1 update underflow".to_string()))?;
                } else {
                    new_pool.reserves1 = pool.reserves1.checked_add(amount_in)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve1 update overflow".to_string()))?;
                    new_pool.reserves0 = pool.reserves0.checked_sub(amount_out)
                        .ok_or_else(|| MEVEngineError::SimulationError("Reserve0 update underflow".to_string()))?;
                }
            }
        }
        
        Ok(new_pool)
    }

    /// Calculate price impact for a multihop swap
    async fn calculate_multihop_price_impact(
        &self,
        path: &[Address],
        amount_in: U256,
        pool_states: &HashMap<Address, PoolInfo>,
    ) -> Result<u32, MEVEngineError> {
        let mut total_impact = 0u32;
        let mut current_amount = amount_in;

        for window in path.windows(2) {
            let (token_in, token_out) = (window[0], window[1]);
            
            // Do the same selection and (if missing) fetch pool info rather than skipping
            let (pool_addr, pool_info, amount_out) = self
                .select_best_pool_for_hop(token_in, token_out, current_amount, pool_states, None)
                .await?; // reuse latest mutated state if present

            debug!(%pool_addr, "Calculating price impact for hop.");
            let impact = dex_math::calculate_price_impact(&pool_info, token_in, current_amount, amount_out)
                .map_err(|e| MEVEngineError::SimulationError(format!("Price impact failed: {e:?}")))?;
            let impact_u32 = impact.as_u128().min(u32::MAX as u128) as u32;
            total_impact = total_impact.saturating_add(impact_u32);
            current_amount = amount_out;
        }

        Ok(total_impact.min(10000)) // Cap at 100%
    }
}