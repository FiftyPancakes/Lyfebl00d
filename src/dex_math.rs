//! # High-Precision DEX Mathematical Library
//!
//! This module provides a robust, safe, and gas-efficient library for performing the
//! core mathematical calculations required for interacting with various DEX protocols.
//! It is designed to be a collection of pure functions that are deterministic and
//! free of side effects (i.e., no network calls).
//!
//! ## Core Principles
//!
//! - **Safety:** All arithmetic operations use `checked_` or `saturating_` methods to
//!   prevent overflows and underflows, which could otherwise panic and crash the application.
//! - **Precision:** Intermediate calculations involving multiplication of large numbers
//!   are performed using `U512` to avoid loss of precision and ensure correctness.
//! - **Clarity:** Logic for each DEX protocol is encapsulated in its own set of
//!   clearly named and documented functions.
//! - **Statelessness:** Functions operate only on the input data provided (e.g., `PoolInfo`)
//!   and do not perform any external state lookups. The caller is responsible for
//!   providing up-to-date state.

use crate::{
    errors::DexMathError,
    types::{PoolInfo, ProtocolType},
};
use ethers::types::{Address, U256, U512};
use tracing::{instrument, warn};
use std::collections::BTreeMap;
use once_cell::sync::Lazy;

//================================================================================================//
//                                         CONSTANTS                                             //
//================================================================================================//

/// High-precision constant for 10,000 (used for basis points).
pub fn precision_10k() -> U256 { U256::from(10_000) }
/// High-precision constant for 1,000,000.
pub fn precision_1m() -> U256 { U256::from(1_000_000) }
/// Constant for 2^96, used in Uniswap V3 calculations.
pub fn q96() -> U256 { U256::from(1) << 96 }
/// Constant for 2^192, used in Uniswap V3 calculations.
pub fn q192() -> U256 { U256::from(1) << 192 }

/// Uniswap V3 tick constants
pub const MIN_TICK: i32 = -887272;
pub const MAX_TICK: i32 = 887272;
pub const MIN_SQRT_RATIO: U256 = U256([4295128739, 0, 0, 0]);
// MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
pub const MAX_SQRT_RATIO: U256 = U256([0xfffd8963efd1fc6a, 0x0fffcb933bd6fad37, 0xaa48a66f76fc, 0x0]);

/// Default gas estimates for swaps on different protocols.
/// These are rough estimates based on typical swap operations.
static GAS_ESTIMATES: Lazy<BTreeMap<ProtocolType, u64>> = Lazy::new(|| {
    let mut m = BTreeMap::new();
    m.insert(ProtocolType::UniswapV2, 120_000);
    m.insert(ProtocolType::SushiSwap, 120_000);
    m.insert(ProtocolType::UniswapV3, 150_000);
    m.insert(ProtocolType::Curve, 250_000);
    m.insert(ProtocolType::Balancer, 300_000);
    m
});

//================================================================================================//
//                                       PUBLIC INTERFACE                                        //
//================================================================================================//

/// Returns an estimate of gas required for a swap operation on the given pool.
/// This is a static estimate based on the protocol type.
#[instrument(skip_all)]
pub fn estimate_gas(pool_info: &PoolInfo) -> u64 {
    // Use GAS_ESTIMATES static map for protocol-based lookup
    if let Some(estimate) = GAS_ESTIMATES.get(&pool_info.protocol_type) {
        *estimate
    } else {
        // Fallback to hardcoded values for unknown protocols
        match pool_info.protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => 120_000,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => 150_000,
            ProtocolType::Curve => 250_000,
            ProtocolType::Balancer => 300_000,
            ProtocolType::Unknown | ProtocolType::Other(_) => 200_000,
        }
    }
}

#[instrument(skip(pool_info), fields(pool = %pool_info.address, token_in = %token_in, amount_in = %amount_in))]
pub fn get_amount_out(pool_info: &PoolInfo, token_in: Address, amount_in: U256) -> Result<U256, DexMathError> {
    if amount_in.is_zero() { return Ok(U256::zero()); }
    validate_token_in_pool(pool_info, token_in).map_err(|e| {
        DexMathError::InvalidV3State(format!(
            "Token validation failed for pool {}: {}. Token {} is not one of the pool's tokens ({} or {}).",
            pool_info.address, e, token_in, pool_info.token0, pool_info.token1
        ))
    })?;
    match &pool_info.protocol_type {
        ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
            calculate_v2_amount_out(pool_info, token_in, amount_in).map_err(|e| {
                DexMathError::InvalidV3State(format!(
                    "V2 swap calculation failed for pool {} ({}): {}. Input: {} of token {}, reserves: {} / {}",
                    pool_info.address, pool_info.protocol_type, e,
                    amount_in, token_in, pool_info.v2_reserve0.unwrap_or_default(), pool_info.v2_reserve1.unwrap_or_default()
                ))
            })
        }
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => {
            calculate_v3_with_v2_fallback(pool_info, token_in, amount_in, true)
        }
        ProtocolType::Curve | ProtocolType::Balancer | ProtocolType::Unknown | ProtocolType::Other(_) => {
            Err(DexMathError::InvalidV3State(format!(
                "Protocol {} is not supported for amount_out calculations. Pool: {}, Token: {}, Amount: {}. \
                 Currently supported: UniswapV2, SushiSwap, UniswapV3, UniswapV4.",
                pool_info.protocol_type, pool_info.address, token_in, amount_in
            )))
        }
    }
}

#[instrument(skip(pool_info), fields(pool = %pool_info.address, token_in = %token_in, amount_out = %amount_out))]
pub fn get_amount_in(pool_info: &PoolInfo, token_in: Address, amount_out: U256) -> Result<U256, DexMathError> {
    if amount_out.is_zero() { return Ok(U256::zero()); }
    validate_token_in_pool(pool_info, token_in)?;

    match &pool_info.protocol_type {
        ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
            calculate_v2_amount_in(pool_info, token_in, amount_out)
        }
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => {
            calculate_v3_amount_in(pool_info, token_in, amount_out)
        }
        ProtocolType::Curve | ProtocolType::Balancer | ProtocolType::Unknown | ProtocolType::Other(_) => {
            Err(DexMathError::InvalidV3State(format!(
                "Protocol {} is not supported for amount_in calculations. Pool: {}, Token: {}, Amount: {}. \
                 Currently supported: UniswapV2, SushiSwap, UniswapV3, UniswapV4.",
                pool_info.protocol_type, pool_info.address, token_in, amount_out
            )))
        }
    }
}

#[instrument(skip(pool_info), fields(pool = %pool_info.address, token_in = %token_in, amount_in = %amount_in, amount_out = %amount_out))]
pub fn calculate_price_impact(
    pool_info: &PoolInfo,
    token_in: Address,
    amount_in: U256,
    amount_out: U256,
) -> Result<U256, DexMathError> {
    validate_token_in_pool(pool_info, token_in)?;

    match pool_info.protocol_type {
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => {
            calculate_v3_price_impact(pool_info, token_in, amount_in, amount_out)
        }
        _ => {
            let (reserve_in, reserve_out) = match pool_info.protocol_type {
                ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                    get_v2_reserves(pool_info, token_in)?
                }
                _ => return Err(DexMathError::InvalidV3State(format!(
                    "Price impact calculation not supported for protocol {}. Pool: {}, Token: {}. \
                     Supported protocols: UniswapV2, SushiSwap, UniswapV3, UniswapV4.",
                    pool_info.protocol_type, pool_info.address, token_in
                ))),
            };

            if reserve_in.is_zero() || reserve_out.is_zero() {
                return Err(DexMathError::InsufficientLiquidity);
            }

            // Calculate the exchange rate before and after the trade
            // Use high precision arithmetic to avoid precision loss
            let before_rate = U512::from(reserve_out).checked_mul(U512::from(precision_1m()))
                .ok_or(DexMathError::Overflow)?
                .checked_div(U512::from(reserve_in))
                .ok_or(DexMathError::DivisionByZero)?;

            let after_rate = U512::from(reserve_out.checked_sub(amount_out).ok_or(DexMathError::InsufficientLiquidity)?)
                .checked_mul(U512::from(precision_1m()))
                .ok_or(DexMathError::Overflow)?
                .checked_div(U512::from(reserve_in.checked_add(amount_in).ok_or(DexMathError::Overflow)?))
                .ok_or(DexMathError::DivisionByZero)?;

            if before_rate == U512::zero() {
                return Err(DexMathError::InvalidV3State(format!(
                    "Invalid pool state for price impact calculation. Pool: {}, reserves: {} / {}, rate calculation resulted in zero.",
                    pool_info.address, reserve_in, reserve_out
                )));
            }

            // Price impact as a percentage (basis points)
            let price_diff = if before_rate > after_rate {
                before_rate.checked_sub(after_rate).ok_or(DexMathError::Overflow)?
            } else {
                after_rate.checked_sub(before_rate).ok_or(DexMathError::Overflow)?
            };

            // Convert price impact to basis points (10000 = 100%)
            let price_impact = price_diff
                .checked_mul(U512::from(precision_10k()))
                .ok_or(DexMathError::Overflow)?
                .checked_div(before_rate)
                .ok_or(DexMathError::DivisionByZero)?;

            // Convert back to U256, handling potential overflow
            if price_impact > U512::from(U256::MAX) {
                warn!("Price impact calculation resulted in overflow, returning maximum U256");
                Ok(U256::MAX)
            } else {
                Ok(U256::try_from(price_impact).unwrap_or(U256::MAX))
            }
        }
    }
}

//================================================================================================//
//                                       V2 CALCULATIONS                                         //
//================================================================================================//

#[instrument(skip(pool_info), fields(pool = %pool_info.address))]
fn get_v2_reserves(pool_info: &PoolInfo, token_in: Address) -> Result<(U256, U256), DexMathError> {
    let reserve0 = pool_info.v2_reserve0.ok_or_else(|| {
        DexMathError::InvalidV3State(format!(
            "Missing reserve0 data for V2 pool {}. This field is required for V2 calculations. \
             Pool may need to be refreshed or data source updated.",
            pool_info.address
        ))
    })?;
    let reserve1 = pool_info.v2_reserve1.ok_or_else(|| {
        DexMathError::InvalidV3State(format!(
            "Missing reserve1 data for V2 pool {}. This field is required for V2 calculations. \
             Pool may need to be refreshed or data source updated.",
            pool_info.address
        ))
    })?;

    if token_in == pool_info.token0 {
        Ok((reserve0, reserve1))
    } else if token_in == pool_info.token1 {
        Ok((reserve1, reserve0))
    } else {
        Err(DexMathError::InvalidV3State(format!(
            "Token {} is not a token in this pool. Pool tokens: {} and {}.",
            token_in, pool_info.token0, pool_info.token1
        )))
    }
}

#[instrument(skip(pool_info), fields(pool = %pool_info.address))]
fn calculate_v2_amount_out(pool_info: &PoolInfo, token_in: Address, amount_in: U256) -> Result<U256, DexMathError> {
    let (reserve_in, reserve_out) = get_v2_reserves(pool_info, token_in)?;

    if reserve_in.is_zero() || reserve_out.is_zero() {
        return Err(DexMathError::InsufficientLiquidity);
    }

    // Use unified fee from pool_info.fee_bps, fallback to legacy optional fee if present
    let fee_bps = if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(30) };
    let fee_multiplier = 10_000u64.saturating_sub(fee_bps as u64);
    let fee_divisor = 10_000u64;

    // Formula: amountOut = (amountIn * fee_multiplier * reserveOut) / (reserveIn * fee_divisor + amountIn * fee_multiplier)
    let amount_in_with_fee = amount_in.checked_mul(U256::from(fee_multiplier))
        .ok_or(DexMathError::Overflow)?;

    let numerator = amount_in_with_fee.checked_mul(reserve_out)
        .ok_or(DexMathError::Overflow)?;

    let denominator = reserve_in.checked_mul(U256::from(fee_divisor))
        .ok_or(DexMathError::Overflow)?
        .checked_add(amount_in_with_fee)
        .ok_or(DexMathError::Overflow)?;

    if denominator.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }

    Ok(numerator.checked_div(denominator).unwrap_or(U256::zero()))
}

#[instrument(skip(pool_info), fields(pool = %pool_info.address))]
fn calculate_v2_amount_in(pool_info: &PoolInfo, token_in: Address, amount_out: U256) -> Result<U256, DexMathError> {
    let (reserve_in, reserve_out) = get_v2_reserves(pool_info, token_in)?;

    if reserve_in.is_zero() || reserve_out.is_zero() {
        return Err(DexMathError::InsufficientLiquidity);
    }

    if amount_out >= reserve_out {
        return Err(DexMathError::InsufficientLiquidity);
    }

    // Use unified fee from pool_info.fee_bps, fallback to legacy optional fee if present
    let fee_bps = if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(30) };
    let fee_multiplier = 10_000u64.saturating_sub(fee_bps as u64);
    let fee_divisor = 10_000u64;

    // Formula: amountIn = (reserveIn * amountOut * fee_divisor) / ((reserveOut - amountOut) * fee_multiplier) + 1
    let numerator = reserve_in.checked_mul(amount_out)
        .ok_or(DexMathError::Overflow)?
        .checked_mul(U256::from(fee_divisor))
        .ok_or(DexMathError::Overflow)?;

    let denominator = reserve_out.checked_sub(amount_out)
        .ok_or(DexMathError::InsufficientLiquidity)?
        .checked_mul(U256::from(fee_multiplier))
        .ok_or(DexMathError::Overflow)?;

    if denominator.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }

    let result = numerator.checked_div(denominator)
        .unwrap_or(U256::zero())
        .checked_add(U256::one()) // Add 1 to round up
        .ok_or(DexMathError::Overflow)?;

    Ok(result)
}

//================================================================================================//
//                                       V3 HELPER FUNCTIONS                                      //
//================================================================================================//

/// Validates that all required V3 pool data is present and returns the validated data
fn validate_v3_pool_data(pool_info: &PoolInfo) -> Result<(U256, u128, i32, i32), DexMathError> {
    let sqrt_price_x96 = pool_info.v3_sqrt_price_x96.ok_or_else(|| {
        DexMathError::InvalidV3State(format!(
            "Missing sqrt_price_x96 for V3 pool {}. This field is required for V3 calculations. \
             Pool may need to be refreshed or data source updated.",
            pool_info.address
        ))
    })?;

    let liquidity = pool_info.v3_liquidity.ok_or_else(|| {
        DexMathError::InvalidV3State(format!(
            "Missing liquidity for V3 pool {}. This field is required for V3 calculations. \
             Pool may need to be refreshed or data source updated.",
            pool_info.address
        ))
    })?;

    let current_tick = pool_info.v3_tick_current.ok_or_else(|| {
        DexMathError::InvalidV3State(format!(
            "Missing current tick for V3 pool {}. This field is required for V3 calculations. \
             Pool may need to be refreshed or data source updated.",
            pool_info.address
        ))
    })?;

    let fee = if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(3000) };
    let tick_spacing = get_tick_spacing(fee);

    Ok((sqrt_price_x96, liquidity, current_tick, tick_spacing))
}

/// Validates and extracts tick data from V3 pool, filtering only initialized ticks
fn validate_and_extract_tick_data(pool_info: &PoolInfo) -> Result<std::collections::BTreeMap<i32, i128>, DexMathError> {
    let ticks_map = match &pool_info.v3_tick_data {
        Some(map) => map,
        None => {
            return Err(DexMathError::InvalidV3State(format!(
                "Missing tick data for V3 pool {}. Tick data is required for accurate V3 swap calculations. \
                 Pool may need to be refreshed with tick information.",
                pool_info.address
            )));
        }
    };

    // Create a BTreeMap of initialized ticks only for efficient iteration
    let mut initialized_ticks = std::collections::BTreeMap::new();
    for (&tick, tick_data) in ticks_map.iter() {
        if tick_data.liquidity_gross > 0 {
            initialized_ticks.insert(tick, tick_data.liquidity_net);
        }
    }

    if initialized_ticks.is_empty() {
        return Err(DexMathError::InvalidV3State(format!(
            "No initialized ticks found for V3 pool {}. Pool may be empty or data is incomplete.",
            pool_info.address
        )));
    }

    Ok(initialized_ticks)
}

/// Attempts to fall back to V2 calculation when V3 data is incomplete
/// Returns true if fallback is possible, false otherwise
fn can_fallback_to_v2(pool_info: &PoolInfo) -> bool {
    pool_info.v2_reserve0.is_some() &&
    pool_info.v2_reserve1.is_some() &&
    pool_info.v2_reserve0.unwrap() > U256::zero() &&
    pool_info.v2_reserve1.unwrap() > U256::zero()
}

/// Attempts V3 calculation with fallback to V2 if V3 data is incomplete
fn calculate_v3_with_v2_fallback(
    pool_info: &PoolInfo,
    token_in: Address,
    amount_in: U256,
    is_amount_out: bool,
) -> Result<U256, DexMathError> {
    // First try V3 calculation
    let v3_result = if is_amount_out {
        calculate_v3_amount_out(pool_info, token_in, amount_in)
    } else {
        calculate_v3_amount_in(pool_info, token_in, amount_in)
    };

    match v3_result {
        Ok(result) => Ok(result),
        Err(_) if can_fallback_to_v2(pool_info) => {
            // Fallback to V2 calculation
            warn!(
                "V3 calculation failed for pool {}, falling back to V2 calculation. \
                 This may result in less accurate results for concentrated liquidity pools.",
                pool_info.address
            );

            if is_amount_out {
                calculate_v2_amount_out(pool_info, token_in, amount_in)
            } else {
                calculate_v2_amount_in(pool_info, token_in, amount_in)
            }
        }
        Err(e) => Err(e), // Re-throw the original V3 error if no fallback possible
    }
}

//================================================================================================//
//                                       V3 CALCULATIONS                                         //
//================================================================================================//

/// Calculates the amount out for a Uniswap V3 swap given input amount
fn calculate_v3_amount_out(pool_info: &PoolInfo, token_in: Address, amount_in: U256) -> Result<U256, DexMathError> {
    let (sqrt_price_x96, liquidity, current_tick, tick_spacing) = validate_v3_pool_data(pool_info)?;

    if liquidity == 0 {
        return Err(DexMathError::InsufficientLiquidity);
    }

    let fee = if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(30) };
    let amount_in_after_fee = amount_in
        .checked_mul(U256::from(10_000u64.saturating_sub(fee as u64)))
        .ok_or(DexMathError::Overflow)?
        .checked_div(U256::from(10_000))
        .ok_or(DexMathError::DivisionByZero)?;

    let zero_for_one = token_in == pool_info.token0;

    // Check if tick data is available. If not, perform a 'within-tick' swap calculation.
    if pool_info.v3_tick_data.is_none() {
        warn!(
            "V3 tick data not available for pool {}. Performing within-tick swap calculation. \
             Result may be inaccurate if swap crosses a tick.",
            pool_info.address
        );

        let sqrt_price_limit = if zero_for_one {
            MIN_SQRT_RATIO + 1
        } else {
            MAX_SQRT_RATIO - 1
        };

        let (amount_in_step, amount_out_step, _sqrt_price_new) = compute_swap_step(
            sqrt_price_x96,
            sqrt_price_limit,
            liquidity,
            amount_in_after_fee,
            zero_for_one,
        )?;

        // If amount_in_step is less than amount_in_after_fee, it means the swap would cross a tick
        if amount_in_step < amount_in_after_fee {
            return Err(DexMathError::InvalidV3State(
                "Swap requires more liquidity than available in the current tick range, and tick data is missing.".to_string()
            ));
        }
        return Ok(amount_out_step);
    }

    let initialized_ticks = validate_and_extract_tick_data(pool_info)?;

    swap_v3_exact_input_corrected(
        sqrt_price_x96,
        liquidity,
        amount_in_after_fee,
        zero_for_one,
        current_tick,
        tick_spacing,
        &initialized_ticks,
    )
}

/// Calculates the amount in for a Uniswap V3 swap given desired output
fn calculate_v3_amount_in(pool_info: &PoolInfo, token_in: Address, amount_out: U256) -> Result<U256, DexMathError> {
    let (sqrt_price_x96, liquidity, current_tick, tick_spacing) = validate_v3_pool_data(pool_info)?;

    if liquidity == 0 {
        return Err(DexMathError::InsufficientLiquidity);
    }

    let fee = if pool_info.fee_bps > 0 { pool_info.fee_bps } else { pool_info.fee.unwrap_or(30) };
    let zero_for_one = token_in == pool_info.token0;
    
    if pool_info.v3_tick_data.is_none() {
        warn!(
            "V3 tick data not available for pool {}. Performing within-tick swap calculation for amount_in. \
             Result may be inaccurate if swap crosses a tick.",
            pool_info.address
        );
        let sqrt_price_limit = if zero_for_one {
            MIN_SQRT_RATIO + 1
        } else {
            MAX_SQRT_RATIO - 1
        };

        let (amount_in_required, amount_out_calculated, _, _) = swap_v3_exact_output_corrected(
            sqrt_price_x96,
            liquidity,
            amount_out,
            zero_for_one,
            current_tick,
            tick_spacing,
            &BTreeMap::new(), // No ticks, so this will be a within-tick calculation
            sqrt_price_limit,
        )?;

        if amount_out_calculated < amount_out {
            return Err(DexMathError::InvalidV3State(
                "Swap requires more liquidity than available in the current tick range, and tick data is missing.".to_string()
            ));
        }

        let amount_in_with_fees = amount_in_required
            .checked_mul(U256::from(10_000))
            .ok_or(DexMathError::Overflow)?
            .checked_div(U256::from(10_000u64.saturating_sub(fee as u64)))
            .ok_or(DexMathError::DivisionByZero)?;

        return Ok(amount_in_with_fees);
    }

    let initialized_ticks = validate_and_extract_tick_data(pool_info)?;

    // Use the exact output swap function to calculate required input
    let sqrt_price_limit = if zero_for_one {
        MIN_SQRT_RATIO + 1
    } else {
        MAX_SQRT_RATIO - 1
    };

    let (amount_in_required, _, _, _) = swap_v3_exact_output_corrected(
        sqrt_price_x96,
        liquidity,
        amount_out,
        zero_for_one,
        current_tick,
        tick_spacing,
        &initialized_ticks,
        sqrt_price_limit,
    )?;

    // Account for fees - the amount_in_required is after fees, we need to calculate the total input including fees
    let amount_in_with_fees = amount_in_required
        .checked_mul(U256::from(10_000))
        .ok_or(DexMathError::Overflow)?
        .checked_div(U256::from(10_000u64.saturating_sub(fee as u64)))
        .ok_or(DexMathError::DivisionByZero)?;

    Ok(amount_in_with_fees)
}

/// Corrected V3 swap logic for exact input amount that only considers initialized ticks
fn swap_v3_exact_input_corrected(
    mut sqrt_price_x96: U256,
    mut liquidity: u128,
    mut amount_in: U256,
    zero_for_one: bool,
    mut current_tick: i32,
    tick_spacing: i32,
    initialized_ticks: &std::collections::BTreeMap<i32, i128>,
) -> Result<U256, DexMathError> {
    let mut amount_out = U256::zero();

    // Price limits for the swap
    let sqrt_price_limit = if zero_for_one { 
        MIN_SQRT_RATIO + 1 
    } else { 
        MAX_SQRT_RATIO - 1 
    };

    while !amount_in.is_zero() && sqrt_price_x96 != sqrt_price_limit {
        // Find the next initialized tick in the direction of the swap
        let tick_next = if zero_for_one {
            // Moving down (price decreasing), find next initialized tick below current
            initialized_ticks.range(..current_tick)
                .next_back()
                .map(|(&tick, _)| tick)
                .unwrap_or(MIN_TICK)
        } else {
            // Moving up (price increasing), find next initialized tick above current
            initialized_ticks.range((current_tick + 1)..)
                .next()
                .map(|(&tick, _)| tick)
                .unwrap_or(MAX_TICK)
        };

        // Validate that the found tick is properly aligned with tick spacing
        if tick_next != MIN_TICK && tick_next != MAX_TICK {
            if tick_next % tick_spacing != 0 {
                return Err(DexMathError::InvalidV3State(format!(
                    "Tick {} is not aligned with tick spacing {}", 
                    tick_next, 
                    tick_spacing
                )));
            }
        }

        let sqrt_price_next = get_sqrt_ratio_at_tick(tick_next)?;
        let sqrt_price_target = if zero_for_one {
            if sqrt_price_next < sqrt_price_limit { sqrt_price_limit } else { sqrt_price_next }
        } else {
            if sqrt_price_next > sqrt_price_limit { sqrt_price_limit } else { sqrt_price_next }
        };

        // Compute swap within the current tick range
        let (amount_in_step, amount_out_step, sqrt_price_new) = compute_swap_step(
            sqrt_price_x96,
            sqrt_price_target,
            liquidity,
            amount_in,
            zero_for_one,
        )?;

        amount_in = amount_in.checked_sub(amount_in_step).ok_or(DexMathError::Overflow)?;
        amount_out = amount_out.checked_add(amount_out_step).ok_or(DexMathError::Overflow)?;
        sqrt_price_x96 = sqrt_price_new;

        // If we've reached the target tick and it's an initialized tick, cross it and update liquidity
        if sqrt_price_x96 == sqrt_price_next && tick_next != current_tick {
            if let Some(&liquidity_net) = initialized_ticks.get(&tick_next) {
                if zero_for_one {
                    // For token0 -> token1, subtract liquidity_net when moving down
                    if liquidity_net >= 0 {
                        liquidity = liquidity.checked_sub(liquidity_net as u128)
                            .ok_or(DexMathError::Overflow)?;
                    } else {
                        liquidity = liquidity.checked_add((-liquidity_net) as u128)
                            .ok_or(DexMathError::Overflow)?;
                    }
                } else {
                    // For token1 -> token0, add liquidity_net when moving up
                    if liquidity_net >= 0 {
                        liquidity = liquidity.checked_add(liquidity_net as u128)
                            .ok_or(DexMathError::Overflow)?;
                    } else {
                        liquidity = liquidity.checked_sub((-liquidity_net) as u128)
                            .ok_or(DexMathError::Overflow)?;
                    }
                }
            }
            
            // Validate that the new current tick is properly aligned with tick spacing
            if tick_next != MIN_TICK && tick_next != MAX_TICK {
                if tick_next % tick_spacing != 0 {
                    return Err(DexMathError::InvalidV3State(format!(
                        "New current tick {} is not aligned with tick spacing {}", 
                        tick_next, 
                        tick_spacing
                    )));
                }
            }
            
            current_tick = tick_next;
        }

        // Break if we've hit the price limit or reached tick boundaries
        if sqrt_price_x96 == sqrt_price_limit {
            break;
        }
        
        // If we couldn't make progress, break to avoid infinite loops
        if amount_in_step.is_zero() && amount_out_step.is_zero() {
            break;
        }
    }

    Ok(amount_out)
}

/// Corrected V3 exact output swap logic that only considers initialized ticks
fn swap_v3_exact_output_corrected(
    mut sqrt_price_x96: U256,
    mut liquidity: u128,
    mut amount_out_remaining: U256,
    zero_for_one: bool,
    mut current_tick: i32,
    tick_spacing: i32,
    initialized_ticks: &std::collections::BTreeMap<i32, i128>,
    sqrt_price_limit_x96: U256,
) -> Result<(U256, U256, i32, U256), DexMathError> {
    let mut amount_in_accum = U256::zero();
    let mut amount_out_accum = U256::zero();

    if liquidity == 0 {
        return Err(DexMathError::InsufficientLiquidity);
    }

    if sqrt_price_x96 == U256::zero() || sqrt_price_limit_x96 == U256::zero() {
        return Err(DexMathError::InvalidV3State("Invalid sqrt price".to_string()));
    }

    while !amount_out_remaining.is_zero() && sqrt_price_x96 != sqrt_price_limit_x96 {
        // Find the next initialized tick in the direction of the swap
        let tick_next = if zero_for_one {
            // Moving down (price decreasing), find next initialized tick below current
            initialized_ticks.range(..current_tick)
                .next_back()
                .map(|(&tick, _)| tick)
                .unwrap_or(MIN_TICK)
        } else {
            // Moving up (price increasing), find next initialized tick above current
            initialized_ticks.range((current_tick + 1)..)
                .next()
                .map(|(&tick, _)| tick)
                .unwrap_or(MAX_TICK)
        };

        // Validate that the found tick is properly aligned with tick spacing
        if tick_next != MIN_TICK && tick_next != MAX_TICK {
            if tick_next % tick_spacing != 0 {
                return Err(DexMathError::InvalidV3State(format!(
                    "Tick {} is not aligned with tick spacing {}", 
                    tick_next, 
                    tick_spacing
                )));
            }
        }

        let sqrt_price_next = get_sqrt_ratio_at_tick(tick_next)?;
        let sqrt_price_target = if zero_for_one {
            if sqrt_price_next < sqrt_price_limit_x96 { sqrt_price_limit_x96 } else { sqrt_price_next }
        } else {
            if sqrt_price_next > sqrt_price_limit_x96 { sqrt_price_limit_x96 } else { sqrt_price_next }
        };

        let (amount_in_step, amount_out_step, sqrt_price_new) = if zero_for_one {
            let max_amount_out = get_amount1_delta(sqrt_price_target, sqrt_price_x96, liquidity, false)?;
            if amount_out_remaining >= max_amount_out {
                let amount_in = get_amount0_delta(sqrt_price_target, sqrt_price_x96, liquidity, true)?;
                (amount_in, max_amount_out, sqrt_price_target)
            } else {
                let sqrt_price_new = get_next_sqrt_price_from_amount1(sqrt_price_x96, liquidity, amount_out_remaining, false)?;
                let amount_in = get_amount0_delta(sqrt_price_new, sqrt_price_x96, liquidity, true)?;
                (amount_in, amount_out_remaining, sqrt_price_new)
            }
        } else {
            let max_amount_out = get_amount0_delta(sqrt_price_x96, sqrt_price_target, liquidity, false)?;
            if amount_out_remaining >= max_amount_out {
                let amount_in = get_amount1_delta(sqrt_price_x96, sqrt_price_target, liquidity, true)?;
                (amount_in, max_amount_out, sqrt_price_target)
            } else {
                let sqrt_price_new = get_next_sqrt_price_from_amount0(sqrt_price_x96, liquidity, amount_out_remaining, false)?;
                let amount_in = get_amount1_delta(sqrt_price_x96, sqrt_price_new, liquidity, true)?;
                (amount_in, amount_out_remaining, sqrt_price_new)
            }
        };

        amount_in_accum = amount_in_accum.checked_add(amount_in_step).ok_or(DexMathError::Overflow)?;
        amount_out_accum = amount_out_accum.checked_add(amount_out_step).ok_or(DexMathError::Overflow)?;
        amount_out_remaining = amount_out_remaining.checked_sub(amount_out_step).ok_or(DexMathError::Overflow)?;
        sqrt_price_x96 = sqrt_price_new;

        // If we've reached the target tick and it's an initialized tick, cross it and update liquidity
        if sqrt_price_x96 == sqrt_price_next && tick_next != current_tick {
            if let Some(&liquidity_net) = initialized_ticks.get(&tick_next) {
                if zero_for_one {
                    // For token0 -> token1, subtract liquidity_net when moving down
                    if liquidity_net >= 0 {
                        liquidity = liquidity.checked_sub(liquidity_net as u128)
                            .ok_or(DexMathError::Overflow)?;
                    } else {
                        liquidity = liquidity.checked_add((-liquidity_net) as u128)
                            .ok_or(DexMathError::Overflow)?;
                    }
                } else {
                    // For token1 -> token0, add liquidity_net when moving up
                    if liquidity_net >= 0 {
                        liquidity = liquidity.checked_add(liquidity_net as u128)
                            .ok_or(DexMathError::Overflow)?;
                    } else {
                        liquidity = liquidity.checked_sub((-liquidity_net) as u128)
                            .ok_or(DexMathError::Overflow)?;
                    }
                }
            }

            // If liquidity hits zero after crossing tick, return InsufficientLiquidity
            if liquidity == 0 {
                return Err(DexMathError::InsufficientLiquidity);
            }

            current_tick = tick_next;
        }

        // Break if we've hit the price limit
        if sqrt_price_x96 == sqrt_price_limit_x96 {
            break;
        }
        
        // If we couldn't make progress, break to avoid infinite loops
        if amount_in_step.is_zero() && amount_out_step.is_zero() {
            break;
        }
    }

    Ok((amount_in_accum, amount_out_accum, current_tick, sqrt_price_x96))
}

/// Compute a single swap step within a price range
#[inline]
fn compute_swap_step(
    sqrt_price_current: U256,
    sqrt_price_target: U256,
    liquidity: u128,
    amount_remaining: U256,
    zero_for_one: bool,
) -> Result<(U256, U256, U256), DexMathError> {
    // Use the full amount_remaining without artificial limits
    // The swap logic will naturally handle large amounts by crossing ticks
    let effective_amount_remaining = amount_remaining;
    
    if zero_for_one {
        // Token0 -> Token1 swap
        let amount_in_max = get_amount0_delta(sqrt_price_target, sqrt_price_current, liquidity, true)?;
        
        if effective_amount_remaining >= amount_in_max {
            // We can swap to the target price
            let amount_out = get_amount1_delta(sqrt_price_target, sqrt_price_current, liquidity, false)?;
            Ok((amount_in_max, amount_out, sqrt_price_target))
        } else {
            // Partial swap within the range
            let sqrt_price_new = get_next_sqrt_price_from_amount0(sqrt_price_current, liquidity, effective_amount_remaining, true)?;
            let amount_out = get_amount1_delta(sqrt_price_new, sqrt_price_current, liquidity, false)?;
            Ok((effective_amount_remaining, amount_out, sqrt_price_new))
        }
    } else {
        // Token1 -> Token0 swap
        let amount_in_max = get_amount1_delta(sqrt_price_current, sqrt_price_target, liquidity, true)?;
        
        if effective_amount_remaining >= amount_in_max {
            // We can swap to the target price
            let amount_out = get_amount0_delta(sqrt_price_current, sqrt_price_target, liquidity, false)?;
            Ok((amount_in_max, amount_out, sqrt_price_target))
        } else {
            // Partial swap within the range
            let sqrt_price_new = get_next_sqrt_price_from_amount1(sqrt_price_current, liquidity, effective_amount_remaining, true)?;
            let amount_out = get_amount0_delta(sqrt_price_current, sqrt_price_new, liquidity, false)?;
            Ok((effective_amount_remaining, amount_out, sqrt_price_new))
        }
    }
}

/// Calculate the amount of token0 delta between two sqrt prices using numerically stable formula
#[inline]
fn get_amount0_delta(
    sqrt_ratio_a: U256,
    sqrt_ratio_b: U256,
    liquidity: u128,
    round_up: bool,
) -> Result<U256, DexMathError> {
    let (sqrt_lower, sqrt_upper) = if sqrt_ratio_a > sqrt_ratio_b {
        (sqrt_ratio_b, sqrt_ratio_a)
    } else {
        (sqrt_ratio_a, sqrt_ratio_b)
    };

    if sqrt_lower.is_zero() || sqrt_upper.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }

    // Use the numerically stable formula: liquidity * (1/sqrt_lower - 1/sqrt_upper)
    // Which translates to: liquidity * q96 * (sqrt_upper - sqrt_lower) / (sqrt_lower * sqrt_upper)

    let liquidity_512 = U512::from(liquidity);
    let sqrt_lower_512 = U512::from(sqrt_lower);
    let sqrt_upper_512 = U512::from(sqrt_upper);
    let q96_512 = U512::from(q96());

    // Calculate numerator: liquidity * q96 * (sqrt_upper - sqrt_lower)
    let price_diff = sqrt_upper.checked_sub(sqrt_lower).ok_or(DexMathError::Overflow)?;
    let numerator = liquidity_512
        .checked_mul(q96_512)
        .ok_or(DexMathError::Overflow)?
        .checked_mul(U512::from(price_diff))
        .ok_or(DexMathError::Overflow)?;

    // Calculate denominator: sqrt_lower * sqrt_upper
    let denominator = sqrt_lower_512
        .checked_mul(sqrt_upper_512)
        .ok_or(DexMathError::Overflow)?;

    if denominator.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }

    // Perform the division
    let mut amount_512 = numerator
        .checked_div(denominator)
        .ok_or(DexMathError::DivisionByZero)?;

    // Handle rounding
    if round_up {
        let remainder = numerator.checked_rem(denominator).unwrap_or_default();
        if remainder > U512::zero() {
            amount_512 = amount_512.checked_add(U512::one()).ok_or(DexMathError::Overflow)?;
        }
    }

    // Convert back to U256
    if amount_512 > U512::from(U256::MAX) {
        return Err(DexMathError::Overflow);
    }

    Ok(U256::try_from(amount_512).map_err(|_| DexMathError::Overflow)?)
}

/// Calculate the amount of token1 delta between two sqrt prices
#[inline]
fn get_amount1_delta(
    sqrt_ratio_a: U256,
    sqrt_ratio_b: U256,
    liquidity: u128,
    round_up: bool,
) -> Result<U256, DexMathError> {
    let (sqrt_lower, sqrt_upper) = if sqrt_ratio_a > sqrt_ratio_b {
        (sqrt_ratio_b, sqrt_ratio_a)
    } else {
        (sqrt_ratio_a, sqrt_ratio_b)
    };

    // Calculate using high-precision arithmetic: liquidity * (sqrt_upper - sqrt_lower) / q96
    let price_diff = sqrt_upper.checked_sub(sqrt_lower).ok_or(DexMathError::Overflow)?;
    let liquidity_512 = U512::from(liquidity);
    let q96_512 = U512::from(q96());

    let numerator = liquidity_512
        .checked_mul(U512::from(price_diff))
        .ok_or(DexMathError::Overflow)?;

    let mut amount_512 = numerator
        .checked_div(q96_512)
        .ok_or(DexMathError::DivisionByZero)?;

    // Handle rounding
    if round_up {
        let remainder = numerator.checked_rem(q96_512).unwrap_or_default();
        if remainder > U512::zero() {
            amount_512 = amount_512.checked_add(U512::one()).ok_or(DexMathError::Overflow)?;
        }
    }

    // Convert back to U256
    if amount_512 > U512::from(U256::MAX) {
        return Err(DexMathError::Overflow);
    }

    Ok(U256::try_from(amount_512).map_err(|_| DexMathError::Overflow)?)
}

/// Get the next sqrt price when adding token0
/// Get the next sqrt price when swapping an amount of token0
fn get_next_sqrt_price_from_amount0(
    sqrt_price: U256,
    liquidity: u128,
    amount: U256,
    add: bool, // true if adding token0 to the pool (amount in)
) -> Result<U256, DexMathError> {
    if amount.is_zero() {
        return Ok(sqrt_price);
    }
    let liquidity_u256 = U256::from(liquidity);

    // This calculation requires U512 arithmetic to avoid overflow
    let liquidity_q96_512 = U512::from(liquidity_u256)
        .checked_mul(U512::from(q96()))
        .ok_or(DexMathError::Overflow)?;

    if add {
        // Formula for adding token0:
        // sqrtP_new = (L * q96 * sqrtP) / (L * q96 + amount * sqrtP)
        
        // Calculate amount * sqrtP using U512
        let amount_sqrt_product = U512::from(amount)
            .checked_mul(U512::from(sqrt_price))
            .ok_or(DexMathError::Overflow)?;

        // Denominator (L * q96 + amount * sqrtP)
        let denominator = liquidity_q96_512
            .checked_add(amount_sqrt_product)
            .ok_or(DexMathError::Overflow)?;

        if denominator.is_zero() {
            return Err(DexMathError::DivisionByZero);
        }

        // Numerator (L * q96 * sqrtP)
        let numerator = liquidity_q96_512
            .checked_mul(U512::from(sqrt_price))
            .ok_or(DexMathError::Overflow)?;
        
        let new_sqrt_price_512 = numerator
            .checked_div(denominator)
            .ok_or(DexMathError::DivisionByZero)?;

        // The result must fit back into U256
        if new_sqrt_price_512 > U512::from(U256::MAX) {
            return Err(DexMathError::Overflow);
        }
        Ok(U256::try_from(new_sqrt_price_512).unwrap())

    } else {
        // This 'else' branch is for swaps of exact output, where we calculate price change from removing token0.
        // Formula for removing token0:
        // sqrtP_new = (L * q96 * sqrtP) / (L * q96 - amount * sqrtP)
        
        let amount_sqrt_product = U512::from(amount)
            .checked_mul(U512::from(sqrt_price))
            .ok_or(DexMathError::Overflow)?;

        if liquidity_q96_512 < amount_sqrt_product {
            return Err(DexMathError::InsufficientLiquidity);
        }
        
        let denominator = liquidity_q96_512
            .checked_sub(amount_sqrt_product)
            .ok_or(DexMathError::Overflow)?;

        if denominator.is_zero() {
            return Err(DexMathError::DivisionByZero);
        }

        let numerator = liquidity_q96_512
            .checked_mul(U512::from(sqrt_price))
            .ok_or(DexMathError::Overflow)?;

        let new_sqrt_price_512 = numerator
            .checked_div(denominator)
            .ok_or(DexMathError::DivisionByZero)?;
            
        if new_sqrt_price_512 > U512::from(U256::MAX) {
            return Err(DexMathError::Overflow);
        }
        Ok(U256::try_from(new_sqrt_price_512).unwrap())
    }
}

/// Get the next sqrt price when adding token1
fn get_next_sqrt_price_from_amount1(
    sqrt_price: U256,
    liquidity: u128,
    amount: U256,
    add: bool,
) -> Result<U256, DexMathError> {
    if add {
        // Use mul_div to avoid overflow: amount * q96 / liquidity
        let quotient = mul_div(amount, q96(), U256::from(liquidity))?;
        sqrt_price.checked_add(quotient).ok_or(DexMathError::Overflow)
    } else {
        // Use mul_div to avoid overflow: amount * q96 / liquidity
        let quotient = mul_div(amount, q96(), U256::from(liquidity))?;
        sqrt_price.checked_sub(quotient).ok_or(DexMathError::Overflow)
    }
}

/// Calculate price impact for V3 swaps
fn calculate_v3_price_impact(pool_info: &PoolInfo, token_in: Address, amount_in: U256, amount_out: U256) -> Result<U256, DexMathError> {
    let sqrt_price_x96 = pool_info.v3_sqrt_price_x96.ok_or(DexMathError::InvalidV3State("Missing sqrt_price_x96".to_string()))?;
    let zero_for_one = token_in == pool_info.token0;

    // Calculate spot price before swap in 1e6 fixed-point units
    let price_before_1e6 = if zero_for_one {
        // price = (sqrt^2)/Q192, then scale to 1e6
        mul_div(mul_div(sqrt_price_x96, sqrt_price_x96, q192())?, precision_1m(), U256::one())?
    } else {
        // inverse price: for token1->token0, invert the price
        let direct_price_1e6 = mul_div(mul_div(sqrt_price_x96, sqrt_price_x96, q192())?, precision_1m(), U256::one())?;
        mul_div(precision_1m(), direct_price_1e6, U256::one())?
    };

    // Calculate effective price of the swap (already in 1e6 units: amount_out * 1e6 / amount_in)
    let effective_price_1e6 = mul_div(amount_out, precision_1m(), amount_in)?;

    // Calculate price impact
    let price_diff_1e6 = if price_before_1e6 > effective_price_1e6 {
        price_before_1e6.checked_sub(effective_price_1e6).ok_or(DexMathError::Overflow)?
    } else {
        effective_price_1e6.checked_sub(price_before_1e6).ok_or(DexMathError::Overflow)?
    };

    // Convert to basis points from consistent 1e6 values
    let price_impact = mul_div(price_diff_1e6, precision_10k(), price_before_1e6)?;

    Ok(price_impact)
}



/// Get tick spacing based on fee tier
fn get_tick_spacing(fee: u32) -> i32 {
    match fee {
        100 => 1,    // 0.01%
        500 => 10,   // 0.05%
        3000 => 60,  // 0.3%
        10000 => 200, // 1%
        _ => 60,     // Default to 0.3% spacing
    }
}



//================================================================================================//
//                                    HELPER FUNCTIONS                                          //
//================================================================================================//

/// Validates that the given token is one of the pool's tokens
fn validate_token_in_pool(pool_info: &PoolInfo, token_in: Address) -> Result<(), DexMathError> {
    if token_in != pool_info.token0 && token_in != pool_info.token1 {
        return Err(DexMathError::InvalidToken);
    }
    Ok(())
}

/// Division with rounding up
pub fn div_round_up(a: U256, b: U256) -> Result<U256, DexMathError> {
    if b.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }
    
    let quotient = a.checked_div(b).ok_or(DexMathError::DivisionByZero)?;
    let remainder = a.checked_rem(b).ok_or(DexMathError::DivisionByZero)?;
    
    if remainder.is_zero() {
        Ok(quotient)
    } else {
        quotient.checked_add(U256::one()).ok_or(DexMathError::Overflow)
    }
}

//================================================================================================//
//                               ADVANCED MATHEMATICAL UTILITIES                               //
//================================================================================================//

/// Calculates square root using Newton's method for U256.
/// This is used for Uniswap V3 price calculations.
pub fn sqrt_u256(y: U256) -> Result<U256, DexMathError> {
    if y.is_zero() {
        return Ok(U256::zero());
    }

    if y == U256::one() {
        return Ok(U256::one());
    }

    // Initial guess
    let mut z = y;
    let mut x = y.checked_div(U256::from(2))
        .ok_or(DexMathError::DivisionByZero)?
        .checked_add(U256::one())
        .ok_or(DexMathError::Overflow)?;

    // Newton's method iterations
    let mut iteration_count = 0;
    const MAX_ITERATIONS: usize = 256;

    while x < z && iteration_count < MAX_ITERATIONS {
        z = x;
        x = y.checked_div(x)
            .ok_or(DexMathError::DivisionByZero)?
            .checked_add(x)
            .ok_or(DexMathError::Overflow)?
            .checked_div(U256::from(2))
            .ok_or(DexMathError::DivisionByZero)?;
        iteration_count += 1;
    }

    // Ensure we return the floor of the square root
    while z.checked_mul(z).ok_or(DexMathError::Overflow)? > y {
        z = z.checked_sub(U256::one()).ok_or(DexMathError::Overflow)?;
    }

    Ok(z)
}

/// Calculates sqrt price from tick for Uniswap V3
/// This is the accurate implementation of TickMath.getSqrtRatioAtTick
pub fn get_sqrt_ratio_at_tick(tick: i32) -> Result<U256, DexMathError> {
    if tick < MIN_TICK || tick > MAX_TICK {
        return Err(DexMathError::InvalidV3State(format!("Tick {} out of bounds [{}, {}]", tick, MIN_TICK, MAX_TICK)));
    }

    let abs_tick = if tick < 0 { 
        (-tick) as u32 
    } else { 
        tick as u32 
    };

    let mut ratio = if abs_tick & 0x1 != 0 {
        U256::from_dec_str("340265354078544963557816517032075149313").unwrap()
    } else {
        U256::from(1) << 128
    };

    if abs_tick & 0x2 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("340248342086729790484326174814286782778").unwrap())?;
    }
    if abs_tick & 0x4 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("340214320654664324051920982716015181260").unwrap())?;
    }
    if abs_tick & 0x8 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("340146279804372648236100729048003433010").unwrap())?;
    }
    if abs_tick & 0x10 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("340010263488231146823593991679159461444").unwrap())?;
    }
    if abs_tick & 0x20 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("339738377640345403697157401104375502016").unwrap())?;
    }
    if abs_tick & 0x40 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("339195258003219555707060102825153631437").unwrap())?;
    }
    if abs_tick & 0x80 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("338111622100601834656805679988414492116").unwrap())?;
    }
    if abs_tick & 0x100 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("335954724994790223023589805789778977700").unwrap())?;
    }
    if abs_tick & 0x200 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("331682121138379247127172139029665121542").unwrap())?;
    }
    if abs_tick & 0x400 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("323299236684853023288211250268160618739").unwrap())?;
    }
    if abs_tick & 0x800 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("307163716377032838986818679792566494060").unwrap())?;
    }
    if abs_tick & 0x1000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("277268403626896220199687919605761273479").unwrap())?;
    }
    if abs_tick & 0x2000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("225923453940433267636798934304364539094").unwrap())?;
    }
    if abs_tick & 0x4000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("149997214084966997727330243047573785013").unwrap())?;
    }
    if abs_tick & 0x8000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("66119101136024775247248025602762854035").unwrap())?;
    }
    if abs_tick & 0x10000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("12847376061790713180796276485854237480").unwrap())?;
    }
    if abs_tick & 0x20000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("485053260817517664689734240087928014").unwrap())?;
    }
    if abs_tick & 0x40000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("691302230200181411898734117117509").unwrap())?;
    }
    if abs_tick & 0x80000 != 0 {
        ratio = mul_shift(ratio, U256::from_dec_str("1404880482626894131621879").unwrap())?;
    }

    if tick > 0 {
        ratio = U256::MAX.checked_div(ratio).ok_or(DexMathError::DivisionByZero)?;
    }

    // Shift to get the final result with proper rounding for positive ticks
    let shifted = ratio >> 32;
    if tick > 0 {
        let remainder = ratio & ((U256::one() << 32) - U256::one());
        if remainder > U256::zero() {
            return Ok(shifted.checked_add(U256::one()).ok_or(DexMathError::Overflow)?);
        }
    }
    Ok(shifted)
}

/// Helper function for multiplying and shifting for tick math
fn mul_shift(val: U256, mul: U256) -> Result<U256, DexMathError> {
    let product = U512::from(val).checked_mul(U512::from(mul)).ok_or(DexMathError::Overflow)?;
    Ok(U256::try_from(product >> 128).unwrap_or(U256::MAX))
}

/// High-precision multiplication with U512 intermediate to avoid overflow
#[inline]
pub fn mul_div(a: U256, b: U256, denominator: U256) -> Result<U256, DexMathError> {
    if denominator.is_zero() {
        return Err(DexMathError::DivisionByZero);
    }

    // Use U512 for intermediate calculation
    let product = U512::from(a).checked_mul(U512::from(b))
        .ok_or(DexMathError::Overflow)?;

    let result = product.checked_div(U512::from(denominator))
        .ok_or(DexMathError::DivisionByZero)?;

    // Check if result fits in U256
    if result > U512::from(U256::MAX) {
        return Err(DexMathError::Overflow);
    }

    // Safe conversion since we checked bounds
    Ok(U256::try_from(result).unwrap_or(U256::MAX))
}

/// Calculates the amount of token0 and token1 for a given liquidity amount in a V3 position
pub fn get_amounts_for_liquidity(
    sqrt_ratio_a_x96: U256,
    sqrt_ratio_b_x96: U256,
    liquidity: u128,
) -> Result<(U256, U256), DexMathError> {
    let (sqrt_ratio_lower, sqrt_ratio_upper) = if sqrt_ratio_a_x96 > sqrt_ratio_b_x96 {
        (sqrt_ratio_b_x96, sqrt_ratio_a_x96)
    } else {
        (sqrt_ratio_a_x96, sqrt_ratio_b_x96)
    };

    let amount0 = get_amount0_delta(sqrt_ratio_lower, sqrt_ratio_upper, liquidity, true)?;
    let amount1 = get_amount1_delta(sqrt_ratio_lower, sqrt_ratio_upper, liquidity, true)?;

    Ok((amount0, amount1))
}