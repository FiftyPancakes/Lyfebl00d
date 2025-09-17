//! High-precision arithmetic module for financial calculations
//! 
//! This module provides precise decimal arithmetic to replace f64 calculations
//! that can suffer from precision loss in financial contexts.

use ethers::types::U256;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::{Add, Sub, Mul, Div, AddAssign, SubAssign};
use std::cmp::Ordering;

/// Precision levels for different calculation contexts
/// These are now loaded from config, with fallback values
pub const PRICE_PRECISION: u32 = 18; // Fallback value
pub const PROFIT_PRECISION: u32 = 18; // Fallback value
pub const PERCENTAGE_PRECISION: u32 = 4; // Fallback value for BPS calculations
pub const USD_PRECISION: u32 = 8; // Fallback value for USD values

/// High-precision decimal number using U256 with configurable precision
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreciseDecimal {
    /// Raw value scaled by 10^precision
    pub value: U256,
    /// Number of decimal places
    pub precision: u32,
}

impl PreciseDecimal {
    /// Create a new PreciseDecimal from a U256 value and precision
    pub fn new(value: U256, precision: u32) -> Self {
        Self { value, precision }
    }

    /// Create from integer with specified precision
    pub fn from_integer(integer: u64, precision: u32) -> Self {
        let scale = U256::from(10).pow(U256::from(precision));
        Self::new(U256::from(integer) * scale, precision)
    }

    /// Create from f64 (use with caution, for compatibility only)
    pub fn from_f64(value: f64, precision: u32) -> Result<Self, PrecisionError> {
        if !value.is_finite() {
            return Err(PrecisionError::InvalidInput("Non-finite f64 value".to_string()));
        }
        
        let scale = 10_f64.powi(precision as i32);
        let scaled_value = value * scale;
        
        if scaled_value < 0.0 {
            return Err(PrecisionError::NegativeValue);
        }
        
        // Convert to string to avoid precision loss
        let scaled_str = format!("{:.0}", scaled_value);
        let scaled_u256 = U256::from_dec_str(&scaled_str)
            .map_err(|_| PrecisionError::Overflow)?;
            
        Ok(Self::new(scaled_u256, precision))
    }

    /// Create from U256 token amount with token decimals
    pub fn from_token_amount(amount: U256, token_decimals: u8, target_precision: u32) -> Self {
        if token_decimals as u32 == target_precision {
            Self::new(amount, target_precision)
        } else if token_decimals as u32 > target_precision {
            // Scale down
            let scale_down = U256::from(10).pow(U256::from(token_decimals as u32 - target_precision));
            Self::new(amount / scale_down, target_precision)
        } else {
            // Scale up
            let scale_up = U256::from(10).pow(U256::from(target_precision - token_decimals as u32));
            Self::new(amount * scale_up, target_precision)
        }
    }

    /// Convert to f64 (use with caution due to potential precision loss)
    pub fn to_f64(&self) -> f64 {
        let scale = 10_f64.powi(self.precision as i32);
        let value_str = self.value.to_string();
        
        // Parse as f64 and divide by scale
        value_str.parse::<f64>().unwrap_or(0.0) / scale
    }

    /// Convert to U256 with specified decimals
    pub fn to_token_amount(&self, token_decimals: u8) -> U256 {
        if self.precision == token_decimals as u32 {
            self.value
        } else if self.precision > token_decimals as u32 {
            // Scale down
            let scale_down = U256::from(10).pow(U256::from(self.precision - token_decimals as u32));
            self.value / scale_down
        } else {
            // Scale up
            let scale_up = U256::from(10).pow(U256::from(token_decimals as u32 - self.precision));
            self.value * scale_up
        }
    }

    /// Get the raw value
    pub fn raw_value(&self) -> U256 {
        self.value
    }

    /// Get precision
    pub fn precision(&self) -> u32 {
        self.precision
    }

    /// Check if zero
    pub fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    /// Convert to same precision for operations
    fn normalize_precision(a: &Self, b: &Self) -> (Self, Self) {
        let max_precision = a.precision.max(b.precision);
        (a.with_precision(max_precision), b.with_precision(max_precision))
    }

    /// Convert to specified precision
    pub fn with_precision(&self, new_precision: u32) -> Self {
        if self.precision == new_precision {
            *self
        } else if self.precision < new_precision {
            // Scale up
            let scale_up = U256::from(10).pow(U256::from(new_precision - self.precision));
            Self::new(self.value * scale_up, new_precision)
        } else {
            // Scale down
            let scale_down = U256::from(10).pow(U256::from(self.precision - new_precision));
            Self::new(self.value / scale_down, new_precision)
        }
    }

    /// Multiply by basis points (1 bps = 0.0001 = 1/10000)
    pub fn mul_bps(&self, bps: u32) -> Result<Self, PrecisionError> {
        let bps_decimal = PreciseDecimal::new(U256::from(bps), PERCENTAGE_PRECISION);
        let divisor = PreciseDecimal::new(U256::from(10000), PERCENTAGE_PRECISION);
        let product = self.checked_mul(&bps_decimal);
        Ok(product.checked_div(&divisor))
    }

    /// Calculate percentage change: (new - old) / old * 100
    pub fn percentage_change(&self, old_value: &Self) -> Result<Self, PrecisionError> {
        if old_value.is_zero() {
            return Err(PrecisionError::DivisionByZero);
        }
        
        let (normalized_self, normalized_old) = Self::normalize_precision(self, old_value);
        let difference = normalized_self.checked_sub(&normalized_old);
        let ratio = difference.checked_div(&normalized_old);
        let hundred = PreciseDecimal::from_integer(100, ratio.precision);
        
        Ok(ratio.checked_mul(&hundred))
    }

    /// Calculate profit margin: profit / revenue * 100
    pub fn profit_margin(&self, revenue: &Self) -> Result<Self, PrecisionError> {
        if revenue.is_zero() {
            return Err(PrecisionError::DivisionByZero);
        }
        
        let margin = self.percentage_change(&PreciseDecimal::new(U256::zero(), self.precision))?;
        Ok(margin.checked_div(revenue))
    }
}

/// Errors that can occur during precision arithmetic
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrecisionError {
    Overflow,
    DivisionByZero,
    NegativeValue,
    InvalidInput(String),
    PrecisionMismatch,
}

impl Display for PrecisionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrecisionError::Overflow => write!(f, "Arithmetic overflow"),
            PrecisionError::DivisionByZero => write!(f, "Division by zero"),
            PrecisionError::NegativeValue => write!(f, "Negative value not allowed"),
            PrecisionError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            PrecisionError::PrecisionMismatch => write!(f, "Precision mismatch"),
        }
    }
}

impl std::error::Error for PrecisionError {}

// Arithmetic operations
impl Add for PreciseDecimal {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let (a, b) = Self::normalize_precision(&self, &rhs);
        let result_value = a.value
            .checked_add(b.value)
            .unwrap_or(U256::max_value()); // Saturate on overflow
        Self::new(result_value, a.precision)
    }
}

impl Add for &PreciseDecimal {
    type Output = PreciseDecimal;

    fn add(self, rhs: Self) -> Self::Output {
        (*self).add(*rhs)
    }
}

impl Sub for PreciseDecimal {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let (a, b) = Self::normalize_precision(&self, &rhs);
        let result_value = a.value
            .checked_sub(b.value)
            .unwrap_or(U256::zero()); // Saturate on underflow
        Self::new(result_value, a.precision)
    }
}

impl Sub for &PreciseDecimal {
    type Output = PreciseDecimal;

    fn sub(self, rhs: Self) -> Self::Output {
        (*self).sub(*rhs)
    }
}

impl Mul for PreciseDecimal {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        let result_precision = self.precision.max(rhs.precision);
        let scale_factor = U256::from(10)
            .checked_pow(U256::from(self.precision.min(rhs.precision)))
            .unwrap_or(U256::max_value());
        
        let product = self.value
            .checked_mul(rhs.value)
            .unwrap_or(U256::max_value());
            
        let result_value = product
            .checked_div(scale_factor)
            .unwrap_or(U256::zero());
            
        Self::new(result_value, result_precision)
    }
}

impl Mul for &PreciseDecimal {
    type Output = PreciseDecimal;

    fn mul(self, rhs: Self) -> Self::Output {
        (*self).mul(*rhs)
    }
}

impl Div for PreciseDecimal {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        if rhs.value.is_zero() {
            return Self::new(U256::max_value(), self.precision); // Return max value on division by zero
        }
        
        let result_precision = self.precision.max(rhs.precision);
        let scale_factor = U256::from(10)
            .checked_pow(U256::from(result_precision))
            .unwrap_or(U256::max_value());
        
        let numerator = self.value
            .checked_mul(scale_factor)
            .unwrap_or(U256::max_value());
        
        let result_value = numerator
            .checked_div(rhs.value)
            .unwrap_or(U256::zero());
        
        Self::new(result_value, result_precision)
    }
}

impl Div for &PreciseDecimal {
    type Output = PreciseDecimal;

    fn div(self, rhs: Self) -> Self::Output {
        (*self).div(*rhs)
    }
}

// Add safe arithmetic methods
impl PreciseDecimal {
    /// Safe addition that returns the result directly
    pub fn checked_add(&self, rhs: &Self) -> Self {
        (*self).add(*rhs)
    }

    /// Safe subtraction that returns the result directly
    pub fn checked_sub(&self, rhs: &Self) -> Self {
        (*self).sub(*rhs)
    }

    /// Safe multiplication that returns the result directly
    pub fn checked_mul(&self, rhs: &Self) -> Self {
        (*self).mul(*rhs)
    }

    /// Saturating addition with fallback
    pub fn saturating_add(&self, rhs: &Self, _fallback: Self) -> Self {
        self.checked_add(rhs)
    }

    /// Saturating subtraction with fallback
    pub fn saturating_sub(&self, rhs: &Self, _fallback: Self) -> Self {
        self.checked_sub(rhs)
    }

    /// Saturating multiplication with fallback
    pub fn saturating_mul(&self, rhs: &Self, _fallback: Self) -> Self {
        self.checked_mul(rhs)
    }
}

impl AddAssign for PreciseDecimal {
    fn add_assign(&mut self, rhs: Self) {
        let result = self.checked_add(&rhs);
        *self = result;
    }
}

impl SubAssign for PreciseDecimal {
    fn sub_assign(&mut self, rhs: Self) {
        let result = self.checked_sub(&rhs);
        *self = result;
    }
}

impl PartialOrd for PreciseDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PreciseDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let (a, b) = Self::normalize_precision(self, other);
        a.value.cmp(&b.value)
    }
}

impl Display for PreciseDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = U256::from(10).pow(U256::from(self.precision));
        let integer_part = self.value / scale;
        let fractional_part = self.value % scale;
        
        if fractional_part.is_zero() {
            write!(f, "{}", integer_part)
        } else {
            let fractional_str = format!("{:0width$}", fractional_part, width = self.precision as usize);
            let trimmed = fractional_str.trim_end_matches('0');
            if trimmed.is_empty() {
                write!(f, "{}", integer_part)
            } else {
                write!(f, "{}.{}", integer_part, trimmed)
            }
        }
    }
}

/// Utility functions for common financial calculations
pub mod finance {
    use super::*;

    /// Calculate profit in USD with proper precision
    pub fn calculate_profit_usd(
        token_amount_out: U256,
        token_price_usd: PreciseDecimal,
        token_decimals: u8,
        gas_cost_usd: PreciseDecimal,
    ) -> Result<PreciseDecimal, PrecisionError> {
        let amount_precise = PreciseDecimal::from_token_amount(
            token_amount_out, 
            token_decimals, 
            USD_PRECISION
        );
        
        // Calculate revenue in USD
        let revenue = amount_precise * token_price_usd.with_precision(USD_PRECISION);
        
        // Calculate profit (revenue - gas_cost_usd)
        let profit = revenue - gas_cost_usd;
        
        Ok(profit)
    }

    /// Calculate price impact in basis points
    pub fn calculate_price_impact_bps(
        expected_amount: U256,
        actual_amount: U256,
        token_decimals: u8,
    ) -> Result<u32, PrecisionError> {
        if expected_amount.is_zero() {
            return Err(PrecisionError::DivisionByZero);
        }

        let expected = PreciseDecimal::from_token_amount(expected_amount, token_decimals, PRICE_PRECISION);
        let actual = PreciseDecimal::from_token_amount(actual_amount, token_decimals, PRICE_PRECISION);
        
        // Calculate price impact as percentage
        let impact_abs = if expected > actual {
            expected - actual
        } else {
            actual - expected
        };
        
        // Calculate percentage impact and convert to basis points
        let percentage_impact = (impact_abs / expected) * PreciseDecimal::from_integer(100, impact_abs.precision);
        let impact_bps = percentage_impact * PreciseDecimal::from_integer(100, percentage_impact.precision);
        
        Ok(impact_bps.to_f64() as u32)
    }

    /// Calculate slippage-adjusted amount
    pub fn apply_slippage(
        amount: U256,
        slippage_bps: u32,
        token_decimals: u8,
        is_minimum: bool, // true for minimum out, false for maximum in
    ) -> U256 {
        let amount_precise = PreciseDecimal::from_token_amount(amount, token_decimals, PRICE_PRECISION);
        let adjustment = amount_precise.mul_bps(slippage_bps).unwrap_or_else(|_| {
            // On error, return the original amount without adjustment
            amount_precise
        });
        
        let result = if is_minimum {
            adjustment.saturating_sub(&amount_precise, PreciseDecimal::new(U256::zero(), PRICE_PRECISION))
        } else {
            adjustment.saturating_add(&amount_precise, amount_precise)
        };
        
        result.to_token_amount(token_decimals)
    }

    /// Calculate compound growth rate
    pub fn compound_rate(
        initial_value: PreciseDecimal,
        final_value: PreciseDecimal,
        periods: u32,
    ) -> Result<PreciseDecimal, PrecisionError> {
        if initial_value.is_zero() {
            return Err(PrecisionError::DivisionByZero);
        }

        if periods == 0 {
            return Err(PrecisionError::InvalidInput("Periods must be greater than 0".to_string()));
        }

        // Growth ratio
        let ratio = final_value / initial_value;
        
        // Newton-Raphson method for nth root calculation
        // For (ratio)^(1/periods) - 1
        if periods == 1 {
            // Simple case: single period
            let one = PreciseDecimal::from_integer(1, ratio.precision);
            return Ok(ratio - one);
        }
        
        // Use binary search for precision root calculation
        let one = PreciseDecimal::from_integer(1, PROFIT_PRECISION);
        let mut low = PreciseDecimal::new(U256::zero(), PROFIT_PRECISION);
        let mut high = ratio.with_precision(PROFIT_PRECISION);
        let epsilon = PreciseDecimal::new(U256::from(1), PROFIT_PRECISION); // Minimal precision unit
        
        // Binary search iterations (64 should be enough for U256 precision)
        for _ in 0..64 {
            let mid = (low + high) / PreciseDecimal::from_integer(2, PROFIT_PRECISION);
            
            // Calculate mid^periods
            let mut power = one;
            let base = mid + one;
            for _ in 0..periods {
                power = power * base;
            }
            
            // Compare with target ratio
            let target_ratio = ratio.with_precision(PROFIT_PRECISION);
            if power > target_ratio {
                // Check if we're within epsilon
                let diff = power - target_ratio;
                if diff.value < epsilon.value {
                    return Ok(mid);
                }
                high = mid;
            } else {
                let diff = target_ratio - power;
                if diff.value < epsilon.value {
                    return Ok(mid);
                }
                low = mid;
            }
        }
        
        // Return best approximation after max iterations
        Ok((low + high) / PreciseDecimal::from_integer(2, PROFIT_PRECISION))
    }
}

// Add safe division methods
impl PreciseDecimal {
    /// Safe division that returns the result directly
    pub fn checked_div(&self, rhs: &Self) -> Self {
        (*self).div(*rhs)
    }

    /// Safe division with a fallback value
    pub fn saturating_div(&self, rhs: &Self, _fallback: Self) -> Self {
        self.checked_div(rhs)
    }
}