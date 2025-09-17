//! Generic utility functions

/// Bounds `v` between `min` and `max`.
pub fn bounded(v: u32, min: u32, max: u32) -> u32 {
    if v < min {
        min
    } else if v > max {
        max
    } else {
        v
    }
}

/// Lossless coercion from u32→usize with explicit cast.
pub fn coerce_u32_usize(v: u32) -> usize {
    v as usize
}

/// Safe arithmetic operations for U256
pub mod safe_math {
    use ethers::types::U256;

    /// Safe addition that returns None on overflow
    pub fn checked_add(a: U256, b: U256) -> Option<U256> {
        a.checked_add(b)
    }

    /// Safe multiplication that returns None on overflow
    pub fn checked_mul(a: U256, b: U256) -> Option<U256> {
        a.checked_mul(b)
    }

    /// Safe subtraction that returns None on underflow
    pub fn checked_sub(a: U256, b: U256) -> Option<U256> {
        a.checked_sub(b)
    }

    /// Safe division that returns None on division by zero
    pub fn checked_div(a: U256, b: U256) -> Option<U256> {
        if b == U256::zero() {
            None
        } else {
            Some(a / b)
        }
    }

    /// Saturating addition (returns max value on overflow)
    pub fn saturating_add(a: U256, b: U256) -> U256 {
        a.checked_add(b).unwrap_or(U256::max_value())
    }

    /// Saturating multiplication (returns max value on overflow)
    pub fn saturating_mul(a: U256, b: U256) -> U256 {
        a.checked_mul(b).unwrap_or(U256::max_value())
    }

    /// Saturating subtraction (returns zero on underflow)
    pub fn saturating_sub(a: U256, b: U256) -> U256 {
        a.checked_sub(b).unwrap_or(U256::zero())
    }
}

/// Normalizes token units by adjusting for decimals
/// Converts a U256 token amount to a f64 by dividing by 10^decimals
pub fn normalize_units(amount: ethers::types::U256, decimals: u8) -> f64 {
    if decimals == 0 {
        // Convert U256 to f64 directly if no decimals
        let mut bytes = [0u8; 32];
        amount.to_little_endian(&mut bytes);
        let mut result = 0u128;
        for &byte in bytes.iter().rev().take(16) {
            result = result * 256 + byte as u128;
        }
        result as f64
    } else {
        // Calculate 10^decimals as U256
        let divisor = ethers::types::U256::from(10).pow(ethers::types::U256::from(decimals));
        
        // Convert both to f64 for division
        let amount_f64 = {
            let mut bytes = [0u8; 32];
            amount.to_little_endian(&mut bytes);
            let mut result = 0u128;
            for &byte in bytes.iter().rev().take(16) {
                result = result * 256 + byte as u128;
            }
            result as f64
        };
        
        let divisor_f64 = {
            let mut bytes = [0u8; 32];
            divisor.to_little_endian(&mut bytes);
            let mut result = 0u128;
            for &byte in bytes.iter().rev().take(16) {
                result = result * 256 + byte as u128;
            }
            result as f64
        };
        
        amount_f64 / divisor_f64
    }
} 