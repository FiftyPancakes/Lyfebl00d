//! # Core Type Definitions
//!
//! This module serves as the single source of truth for all shared data structures
//! used throughout the arbitrage bot. Centralizing these types ensures consistency,
//! promotes decoupling between modules, and simplifies serialization and data management.

//! Canonical numeric types for safety and precision
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use ethers::core::types::{Block, H256, Log, Transaction, U256, I256, TransactionRequest, Bytes};
use ethers::providers::Ws;
use rust_decimal::Decimal;
use num_traits::ToPrimitive;
use tracing::warn;
use serde::{Deserialize, Serialize};
use solana_client::rpc_response::RpcLogsResponse;
use solana_sdk::slot_history::Slot;

use crate::config::{ArbitrageSettings, FactoryConfig};
use crate::errors::{DexError, ArbitrageError};
use crate::gas_oracle::GasOracleProvider;
use crate::price_oracle::PriceOracle;
use crate::risk::RiskManager;
use crate::pool_states::PoolStateOracle;
use crate::pool_manager::PoolManagerTrait;
use crate::config::Config;
use crate::path::Path;
use tokio::sync::{RwLock, Mutex, mpsc, broadcast, Semaphore};
use std::sync::atomic::AtomicUsize;

use crate::errors::NumericSafetyError;
use ethers::types::Address;

//================================================================================================//
//                                  CANONICAL NUMERIC TYPES                                       //
//================================================================================================//

/// Token amount with decimal-aware arithmetic and overflow protection
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TokenAmount {
    /// Raw amount in base units (no decimals)
    pub raw: U256,
    /// Decimal places for this token
    pub decimals: u8,
}

impl TokenAmount {
    /// Create a new TokenAmount with bounds checking
    pub fn new(raw: U256, decimals: u8) -> Result<Self, NumericSafetyError> {
        if decimals > 18 {
            return Err(NumericSafetyError::InvalidDecimals(decimals));
        }
        Ok(Self { raw, decimals })
    }

    /// Convert to normalized 1e18 representation for consistent math
    pub fn to_scaled_1e18(&self) -> Result<U256, NumericSafetyError> {
        if self.decimals > 18 {
            return Err(NumericSafetyError::InvalidDecimals(self.decimals));
        }

        let scale_factor = 10u128.pow((18 - self.decimals) as u32);
        self.raw.checked_mul(U256::from(scale_factor))
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "decimal scaling".to_string(),
                details: "Multiplication overflow".to_string(),
            })
    }

    /// Convert from 1e18 normalized representation back to token decimals
    pub fn from_scaled_1e18(scaled: U256, decimals: u8) -> Result<Self, NumericSafetyError> {
        if decimals > 18 {
            return Err(NumericSafetyError::InvalidDecimals(decimals));
        }

        let scale_factor = 10u128.pow((18 - decimals) as u32);
        let raw = scaled.checked_div(U256::from(scale_factor))
            .ok_or(NumericSafetyError::DivisionByZero {
                operation: "decimal scaling".to_string(),
            })?;

        Self::new(raw, decimals)
    }

    /// Check if amount is zero
    pub fn is_zero(&self) -> bool {
        self.raw.is_zero()
    }

    /// Get raw amount for contract calls
    pub fn to_raw(&self) -> U256 {
        self.raw
    }
}

/// Gas cost structure with EIP-1559 support
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasCost {
    /// Base fee in wei
    pub base: u128,
    /// Priority fee in wei
    pub priority: u128,
    /// Gas limit
    pub limit: u128,
}

impl GasCost {
    /// Calculate total gas cost in wei
    pub fn total_cost(&self) -> Result<u128, NumericSafetyError> {
        let base_cost = self.base.checked_mul(self.limit)
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "base fee calculation".to_string(),
                details: "Base fee multiplication overflow".to_string(),
            })?;
        let priority_cost = self.priority.checked_mul(self.limit)
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "priority fee calculation".to_string(),
                details: "Priority fee multiplication overflow".to_string(),
            })?;

        base_cost.checked_add(priority_cost)
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "total gas cost calculation".to_string(),
                details: "Total gas cost addition overflow".to_string(),
            })
    }

    /// Create from legacy gas price
    pub fn from_legacy(gas_price: u128, limit: u128) -> Self {
        Self {
            base: gas_price,
            priority: 0,
            limit,
        }
    }
}

impl Default for GasCost {
    fn default() -> Self {
        Self {
            base: 0,
            priority: 0,
            limit: 0,
        }
    }
}

/// USD amount with precision handling
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UsdAmount {
    /// Amount in micro USD (1e6 = 1 USD)
    pub micro_usd: u128,
}

impl UsdAmount {
    /// Create from decimal USD amount
    pub fn from_decimal(decimal: Decimal) -> Result<Self, NumericSafetyError> {
        let micro_usd = decimal.checked_mul(Decimal::from(1_000_000))
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "USD decimal conversion".to_string(),
                details: "Multiplication overflow".to_string(),
            })?
            .to_u128()
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "USD decimal conversion".to_string(),
                details: "Value out of bounds for u128".to_string(),
            })?;

        Ok(Self { micro_usd })
    }

    /// Convert to decimal USD
    pub fn to_decimal(&self) -> Decimal {
        Decimal::from(self.micro_usd) / Decimal::from(1_000_000)
    }
}

/// Basis points (BPS) type for precise percentage calculations
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Bps(pub u16); // 0-10000 = 0%-100%

impl Bps {
    /// Create from percentage (0.0 to 100.0)
    pub fn from_percent(percent: f64) -> Result<Self, NumericSafetyError> {
        if percent < 0.0 || percent > 100.0 {
            return Err(NumericSafetyError::InvalidPercentage(percent));
        }
        let bps = (percent * 100.0).round() as u16;
        if bps > 10_000 {
            return Err(NumericSafetyError::InvalidBps(bps));
        }
        Ok(Self(bps))
    }

    /// Convert to percentage
    pub fn to_percent(&self) -> f64 {
        self.0 as f64 / 100.0
    }

    /// Apply BPS to a token amount
    pub fn apply_to(&self, amount: TokenAmount) -> Result<TokenAmount, NumericSafetyError> {
        let scaled = amount.to_scaled_1e18()?;
        let bps_u256 = U256::from(self.0);
        let result_scaled = scaled.checked_mul(bps_u256)
            .ok_or_else(|| NumericSafetyError::Overflow {
                operation: "BPS application".to_string(),
                details: "Multiplication overflow".to_string(),
            })?
            .checked_div(U256::from(10_000))
            .ok_or(NumericSafetyError::DivisionByZero {
                operation: "BPS application".to_string(),
            })?;

        TokenAmount::from_scaled_1e18(result_scaled, amount.decimals)
    }
}

/// Guarded execution plan with safety invariants
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    /// The transaction request to execute
    pub tx: TransactionRequest,
    /// Minimum return amount (slippage protection)
    pub min_return: TokenAmount,
    /// Deadline timestamp
    pub deadline: u64,
    /// Recipient address (must not be zero)
    pub recipient: Address,
    /// Estimated gas cost
    pub gas_estimate: GasCost,
    /// Expected profit in USD (for scoring)
    pub expected_profit_usd: Option<UsdAmount>,
}

impl Plan {
    /// Assert all safety invariants before execution
    pub fn assert_invariants(&self) -> Result<(), NumericSafetyError> {
        // Check deadline is not expired
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| NumericSafetyError::ExpiredDeadline(0))?
            .as_secs();
        if self.deadline <= now {
            return Err(NumericSafetyError::ExpiredDeadline(self.deadline));
        }

        // Check recipient is not zero address
        if self.recipient == Address::zero() {
            return Err(NumericSafetyError::ZeroRecipient);
        }

        // Check minimum return is not zero
        if self.min_return.is_zero() {
            return Err(NumericSafetyError::ZeroMinReturn);
        }

        // Check gas limit is reasonable
        if self.gas_estimate.limit == 0 || self.gas_estimate.limit > 10_000_000 {
            return Err(NumericSafetyError::InvalidGasLimit(self.gas_estimate.limit));
        }

        // Verify calldata length is reasonable
        if let Some(ref data) = self.tx.data {
            if data.len() > 65_536 { // 64KB limit
                return Err(NumericSafetyError::CalldataTooLarge(data.len()));
            }
        }

        Ok(())
    }

    /// Calculate total cost including gas in USD
    pub fn total_cost_usd(&self, gas_price_usd: f64) -> Result<UsdAmount, NumericSafetyError> {
        let gas_cost_wei = self.gas_estimate.total_cost()?;
        let gas_price_decimal = Decimal::try_from(gas_price_usd).map_err(|_| NumericSafetyError::Conversion {
            from: "f64".to_string(),
            to: "Decimal".to_string(),
        })?;
        let gas_cost_usd_decimal = Decimal::from(gas_cost_wei) * gas_price_decimal / Decimal::from(1_000_000_000_000_000_000u64); // Convert wei to ETH, then to USD
        UsdAmount::from_decimal(gas_cost_usd_decimal)
    }
}

/// Block-tagged value wrapper for deterministic execution
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BlockTagged<T> {
    /// The actual value
    pub value: T,
    /// Block number this value is from
    pub block_number: u64,
    /// Chain this value is from
    pub chain: String,
    /// Timestamp when this value was retrieved (for staleness checks)
    pub timestamp: u64,
}

impl<T> BlockTagged<T> {
    /// Create a new block-tagged value
    pub fn new(value: T, block_number: u64, chain: String) -> Self {
        Self {
            value,
            block_number,
            chain,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Check if this value is from the same block and chain as another
    pub fn same_block(&self, other: &BlockTagged<T>) -> bool {
        self.block_number == other.block_number && self.chain == other.chain
    }

    /// Check if this value is stale (older than max_age_seconds)
    pub fn is_stale(&self, max_age_seconds: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.timestamp) > max_age_seconds
    }
}

/// Block-tagged price data
pub type BlockTaggedPrice = BlockTagged<f64>;

/// Block-tagged token amount
pub type BlockTaggedAmount = BlockTagged<TokenAmount>;

/// Block-tagged gas cost
pub type BlockTaggedGasCost = BlockTagged<GasCost>;

/// Simplified pool state for block tagging
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PoolState {
    pub token0: Address,
    pub token1: Address,
    pub reserve0: U256,
    pub reserve1: U256,
    pub fee: u32,
}

/// Strategy execution context with block consistency enforcement
#[derive(Clone, Debug)]
pub struct StrategyContext {
    /// Target block number for all operations in this context
    pub target_block: u64,
    /// Chain name
    pub chain: String,
    /// All price data must be from this block
    pub price_data: HashMap<Address, BlockTaggedPrice>,
    /// All pool state data must be from this block
    pub pool_states: HashMap<Address, BlockTagged<PoolState>>,
    /// Gas costs for the target block
    pub gas_cost: Option<BlockTaggedGasCost>,
}

impl StrategyContext {
    /// Create a new strategy context for a specific block
    pub fn new(target_block: u64, chain: String) -> Self {
        Self {
            target_block,
            chain,
            price_data: HashMap::new(),
            pool_states: HashMap::new(),
            gas_cost: None,
        }
    }

    /// Add price data, enforcing block consistency
    pub fn add_price(&mut self, token: Address, price: BlockTaggedPrice) -> Result<(), &'static str> {
        if !price.same_block(&BlockTagged::new(0.0, self.target_block, self.chain.clone())) {
            return Err("Price data must be from target block");
        }
        self.price_data.insert(token, price);
        Ok(())
    }

    /// Add pool state data, enforcing block consistency
    pub fn add_pool_state(&mut self, pool: Address, state: BlockTagged<PoolState>) -> Result<(), &'static str> {
        if !state.same_block(&BlockTagged::new(PoolState::default(), self.target_block, self.chain.clone())) {
            return Err("Pool state must be from target block");
        }
        self.pool_states.insert(pool, state);
        Ok(())
    }

    /// Get price for a token, ensuring it's from the correct block
    pub fn get_price(&self, token: &Address) -> Result<&BlockTaggedPrice, &'static str> {
        self.price_data.get(token)
            .ok_or("Price not available for token")
    }

    /// Get pool state, ensuring it's from the correct block
    pub fn get_pool_state(&self, pool: &Address) -> Result<&BlockTagged<PoolState>, &'static str> {
        self.pool_states.get(pool)
            .ok_or("Pool state not available")
    }

    /// Verify all data is from the same block
    pub fn verify_block_consistency(&self) -> Result<(), &'static str> {
        let price_reference = BlockTagged::new(0.0, self.target_block, self.chain.clone());
        let state_reference = BlockTagged::new(PoolState::default(), self.target_block, self.chain.clone());
        let gas_reference = BlockTagged::new(GasCost::default(), self.target_block, self.chain.clone());

        for price in self.price_data.values() {
            if !price.same_block(&price_reference) {
                return Err("Price data block mismatch");
            }
        }

        for state in self.pool_states.values() {
            if !state.same_block(&state_reference) {
                return Err("Pool state block mismatch");
            }
        }

        if let Some(ref gas) = self.gas_cost {
            if !gas.same_block(&gas_reference) {
                return Err("Gas cost block mismatch");
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub is_open: bool,
    pub failure_count: u32,
    pub last_failure_time: u64,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            is_open: false,
            failure_count: 0,
            last_failure_time: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenPairStats {
    pub total_volume_usd: f64,
    pub total_trades: u64,
    pub avg_trade_size_usd: f64,
    pub volatility_24h: f64,
    pub price_correlation: f64,
    pub last_updated: u64,
    pub volatility: f64,
    pub cointegration_score: f64,
    pub mean_price: f64,
    pub std_dev: f64,
    pub price_momentum: f64,
    pub price_observations: Vec<(f64, u64)>, // (price, block_number)
    pub volume_observations: Vec<(f64, u64)>, // (volume_usd, block_number)
    pub last_update: std::time::Instant,
    pub mean_abs_diff: f64,
    pub mean_sq_diff: f64,
}

impl Default for TokenPairStats {
    fn default() -> Self {
        Self {
            total_volume_usd: 0.0,
            total_trades: 0,
            avg_trade_size_usd: 0.0,
            volatility_24h: 0.0,
            price_correlation: 0.0,
            last_updated: 0,
            volatility: 0.0,
            cointegration_score: 0.0,
            mean_price: 0.0,
            std_dev: 0.0,
            price_momentum: 0.0,
            price_observations: Vec::new(),
            volume_observations: Vec::new(),
            last_update: std::time::Instant::now(),
            mean_abs_diff: 0.0,
            mean_sq_diff: 0.0,
        }
    }
}

impl TokenPairStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_sufficient_data(&self) -> bool {
        self.price_observations.len() >= 10 && self.volume_observations.len() >= 10
    }

    pub fn add_price_observation(&mut self, price: f64, block_number: u64) {
        self.price_observations.push((price, block_number));
        
        // Keep only the last 1000 observations for memory efficiency
        if self.price_observations.len() > 1000 {
            self.price_observations.remove(0);
        }
        
        // Update statistical measures
        self.update_price_statistics();
        self.last_update = std::time::Instant::now();
    }

    pub fn add_volume_observation(&mut self, volume_usd: f64, block_number: u64) {
        self.volume_observations.push((volume_usd, block_number));
        
        // Keep only the last 1000 observations for memory efficiency  
        if self.volume_observations.len() > 1000 {
            self.volume_observations.remove(0);
        }
        
        // Update volume statistics
        self.update_volume_statistics();
        self.last_update = std::time::Instant::now();
    }

    fn update_price_statistics(&mut self) {
        if self.price_observations.len() < 2 {
            return;
        }

        let prices: Vec<f64> = self.price_observations.iter().map(|(price, _)| *price).collect();
        
        // Calculate mean price
        self.mean_price = prices.iter().sum::<f64>() / prices.len() as f64;
        
        // Calculate standard deviation
        let variance = prices.iter()
            .map(|price| {
                let diff = price - self.mean_price;
                diff * diff
            })
            .sum::<f64>() / prices.len() as f64;
        self.std_dev = variance.sqrt();
        
        // Calculate volatility as coefficient of variation
        if self.mean_price > 0.0 {
            self.volatility = self.std_dev / self.mean_price;
        }
        
        // Calculate price momentum (rate of change over recent observations)
        if prices.len() >= 10 {
            let recent_prices = &prices[prices.len().saturating_sub(10)..];
            let start_price = recent_prices[0];
            let end_price = recent_prices[recent_prices.len() - 1];
            
            if start_price > 0.0 {
                self.price_momentum = (end_price - start_price) / start_price;
            }
        }
    }

    fn update_volume_statistics(&mut self) {
        if self.volume_observations.is_empty() {
            return;
        }

        let volumes: Vec<f64> = self.volume_observations.iter().map(|(volume, _)| *volume).collect();
        
        // Update total volume and trade statistics
        self.total_volume_usd = volumes.iter().sum::<f64>();
        self.total_trades = volumes.len() as u64;
        
        if self.total_trades > 0 {
            self.avg_trade_size_usd = self.total_volume_usd / self.total_trades as f64;
        }
    }
}

//================================================================================================//
//                                    CORE BLOCKCHAIN PRIMITIVES                                  //
//================================================================================================//

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ChainType {
    Evm,
    Solana,
}

impl FromStr for ChainType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "evm" => Ok(ChainType::Evm),
            "solana" => Ok(ChainType::Solana),
            _ => Err(format!("Unknown chain type: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
    Error(String),
}

//================================================================================================//
//                                    DEX & PROTOCOL DEFINITIONS                                  //
//================================================================================================//

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProtocolType {
    UniswapV2,
    UniswapV3,
    UniswapV4,
    SushiSwap,
    Curve,
    Balancer,
    Unknown,
    Other(String),
}

impl Default for ProtocolType {
    fn default() -> Self {
        ProtocolType::UniswapV2
    }
}

impl std::fmt::Display for ProtocolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolType::UniswapV2 => write!(f, "UniswapV2"),
            ProtocolType::UniswapV3 => write!(f, "UniswapV3"),
            ProtocolType::UniswapV4 => write!(f, "UniswapV4"),
            ProtocolType::SushiSwap => write!(f, "SushiSwap"),
            ProtocolType::Curve => write!(f, "Curve"),
            ProtocolType::Balancer => write!(f, "Balancer"),
            ProtocolType::Unknown => write!(f, "Unknown"),
            ProtocolType::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::str::FromStr for ProtocolType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "uniswapv2" | "uniswap_v2" | "uniswap-v2" => Ok(ProtocolType::UniswapV2),
            "uniswapv3" | "uniswap_v3" | "uniswap-v3" => Ok(ProtocolType::UniswapV3),
            "uniswapv4" | "uniswap_v4" | "uniswap-v4" => Ok(ProtocolType::UniswapV4),
            "sushiswap" | "sushi" => Ok(ProtocolType::SushiSwap),
            "curve" => Ok(ProtocolType::Curve),
            "balancer" => Ok(ProtocolType::Balancer),
            other => Ok(ProtocolType::Other(other.to_string())),
        }
    }
}

impl TryFrom<i16> for ProtocolType {
    type Error = String;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ProtocolType::Unknown),    // Unknown protocol type
            1 => Ok(ProtocolType::UniswapV2),  // UniswapV2 and similar V2 protocols
            2 => Ok(ProtocolType::UniswapV3),  // UniswapV3
            3 => Ok(ProtocolType::Curve),      // Curve
            4 => Ok(ProtocolType::Balancer),   // Balancer
            5 => Ok(ProtocolType::SushiSwap),  // SushiSwap
            6 => Ok(ProtocolType::UniswapV2),  // PancakeSwap V2 (uses V2 logic)
            7 => Ok(ProtocolType::UniswapV3),  // PancakeSwap V3 (uses V3 logic)
            8 => Ok(ProtocolType::UniswapV4),  // UniswapV4
            10 => Ok(ProtocolType::UniswapV2), // TraderJoe (uses V2 logic)
            99 => Ok(ProtocolType::Other("Various".to_string())), // Catch-all for other protocols
            _ => Ok(ProtocolType::Unknown),    // Default to Unknown for any other values
        }
    }
}

impl From<&DexProtocol> for ProtocolType {
    fn from(dex: &DexProtocol) -> Self {
        use DexProtocol::*;
        match dex {
            // V2
            UniswapV2
            | SushiSwap
            | SushiSwapV2
            | PancakeSwap
            | PancakeSwapV2
            | TraderJoe
            | ShibaSwap
            | Camelot
            | Velodrome
            | Solidly
            | FraxSwap
            | Clipper
            | Maverick
            | Smardex
            | SyncSwap
            | Ambient
            | Carbon
            | Bancor
            | Kyber
            | Dodo => ProtocolType::UniswapV2,

            // V3
            UniswapV3 | UniswapV4 | PancakeSwapV3 => ProtocolType::UniswapV3,

            // other
            Curve => ProtocolType::Curve,
            Balancer => ProtocolType::Balancer,
            Unknown | Other(_) => ProtocolType::Other("Unknown".to_string()),
        }
    }
}

//================================================================================================//
//                                          EVENT TYPES                                           //
//================================================================================================//

/// A unified enum representing all possible real-time events the system can process.
#[derive(Debug, Clone)]
pub enum ChainEvent {
    EvmNewHead {
        chain_name: String,
        block: Block<H256>,
    },
    EvmLog {
        chain_name: String,
        log: Log,
    },
    EvmPendingTx {
        chain_name: String,
        tx: ethers::types::Transaction,
    },
    SolanaLog {
        chain_name: String,
        response: RpcLogsResponse,
    },
    SolanaNewSlot {
        chain_name: String,
        slot: Slot,
    },
    NewMarketRegime(MarketRegime),
    NewBlock {
        block_number: u64,
        timestamp: u64,
    },
    OpportunityExecuted {
        opportunity_id: String,
        profit_usd: f64,
        tx_hash: H256,
    },
}

/// A structured representation of a decoded `Swap` event from any supported DEX.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub chain_name: String,
    pub pool_address: Address,
    pub tx_hash: H256,
    pub block_number: u64,
    pub log_index: U256,
    pub unix_ts: u64,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out: U256,
    pub sender: Address,
    pub recipient: Address,
    // Optional enriched data
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
    pub protocol: DexProtocol,
    // CRITICAL: Protocol-specific raw data
    pub data: SwapData,
    // CRITICAL: Add base pool tokens for downstream logic
    pub token0: Option<Address>,
    pub token1: Option<Address>,
    // Additional fields for comprehensive swap event representation
    pub price_t0_in_t1: Option<Decimal>,
    pub price_t1_in_t0: Option<Decimal>,
    pub reserve0: Option<U256>,
    pub reserve1: Option<U256>,
    pub transaction: Option<TransactionData>,
    pub transaction_metadata: Option<serde_json::Value>,
    pub pool_id: Option<H256>,
    pub buyer: Option<Address>,
    pub sold_id: Option<i128>,
    pub tokens_sold: Option<U256>,
    pub bought_id: Option<i128>,
    pub tokens_bought: Option<U256>,
    pub sqrt_price_x96: Option<U256>,
    pub liquidity: Option<U256>,
    pub tick: Option<i32>,
    pub token0_decimals: Option<u8>,
    pub token1_decimals: Option<u8>,
    pub fee_tier: Option<u32>,
    pub protocol_type: Option<ProtocolType>,
    pub v3_amount0: Option<I256>,
    pub v3_amount1: Option<I256>,
    pub v3_tick_data: Option<String>, // JSON serialized V3TickData
    pub price_from_in_usd: Option<Decimal>,
    pub price_to_in_usd: Option<Decimal>,
    pub token0_price_usd: Option<Decimal>,
    pub token1_price_usd: Option<Decimal>,
    pub pool_liquidity_usd: Option<Decimal>,
}

//================================================================================================//
//                                   POOL & LIQUIDITY TYPES                                       //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TickData {
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

/// A comprehensive, protocol-agnostic representation of a liquidity pool's state.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct PoolInfo {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
    pub token0_decimals: u8,
    pub token1_decimals: u8,
    pub protocol_type: ProtocolType,
    pub chain_name: String,

    // Common fields
    pub fee: Option<u32>, // fee_bps
    pub last_updated: u64,

    // V2-specific fields
    pub v2_reserve0: Option<U256>,
    pub v2_reserve1: Option<U256>,
    pub v2_block_timestamp_last: Option<u32>,

    // V3-specific fields
    pub v3_sqrt_price_x96: Option<U256>,
    pub v3_liquidity: Option<u128>,
    pub v3_ticks: Option<i32>,
    pub v3_tick_data: Option<std::collections::HashMap<i32, TickData>>,
    pub v3_tick_current: Option<i32>,

    // Additional fields for path finding
    pub reserves0: U256,
    pub reserves1: U256,
    pub liquidity_usd: f64,
    pub pool_address: Address,
    pub fee_bps: u32,
    pub creation_block_number: Option<u64>,
}

impl PoolInfo {
    // Associated constants for pattern matching compatibility
    pub const V2: ProtocolType = ProtocolType::UniswapV2;
    pub const V3: ProtocolType = ProtocolType::UniswapV3;
    pub const CURVE: ProtocolType = ProtocolType::Curve;
    pub const BALANCER: ProtocolType = ProtocolType::Balancer;
    pub const UNSUPPORTED: ProtocolType = ProtocolType::Other(String::new());
}

impl std::hash::Hash for PoolInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.token0.hash(state);
        self.token1.hash(state);
        self.token0_decimals.hash(state);
        self.token1_decimals.hash(state);
        self.protocol_type.hash(state);
        self.chain_name.hash(state);
        self.fee.hash(state);
        self.last_updated.hash(state);
        self.v2_reserve0.hash(state);
        self.v2_reserve1.hash(state);
        self.v2_block_timestamp_last.hash(state);
        self.v3_sqrt_price_x96.hash(state);
        self.v3_liquidity.hash(state);
        self.v3_ticks.hash(state);
        self.v3_tick_current.hash(state);
        self.reserves0.hash(state);
        self.reserves1.hash(state);
        self.liquidity_usd.to_bits().hash(state);
        self.pool_address.hash(state);
        self.fee_bps.hash(state);
        self.creation_block_number.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PoolMetadata {
    pub address: Address,
    pub protocol_type: ProtocolType,
    pub token0: Address,
    pub token1: Address,
    pub fee_bps: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurvePoolData {
    pub token_addresses: Vec<Address>,
    pub token_indices: std::collections::HashMap<Address, usize>,
    pub a_param: u64,
    pub fee: u64, // Basis points
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancerPoolData {
    pub tokens: Vec<Address>,
    pub weights: Vec<u64>,
    pub swap_fee: u64, // Basis points
}

/// Metadata for a token, as used by the price oracle and other modules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenMetadata {
    pub address: Address,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
    pub chain_id: u64,
    pub is_stablecoin: bool,
    pub is_native: bool,
    pub logo_uri: Option<String>,
}



//================================================================================================//
//                                ARBITRAGE & ROUTING TYPES                                       //
//================================================================================================//

/// A lightweight definition of a single step in a swap route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbitrageRouteLeg {
    pub dex_name: String,
    pub protocol_type: ProtocolType,
    pub pool_address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub min_amount_out: U256,
    pub fee_bps: u32,
    pub expected_out: Option<U256>,
}

/// Represents a single swap leg in an arbitrage route
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapLeg {
    pub pool_address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub min_amount_out: U256,
    pub expected_out: Option<U256>,
    pub dex: String,
}

/// Represents a complete, multi-hop swap route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArbitrageRoute {
    pub legs: Vec<ArbitrageRouteLeg>,
    pub initial_amount_in_wei: U256,
    pub amount_out_wei: U256,
    pub profit_usd: f64,
    pub estimated_gas_cost_wei: U256,
    pub price_impact_bps: u32,
    pub route_description: String,
    pub risk_score: f64,
    pub chain_id: u64,
    pub created_at_ns: u64, // nanoseconds since UNIX epoch
    pub mev_risk_score: f64,
    pub flashloan_amount_wei: Option<U256>,
    pub flashloan_fee_wei: Option<U256>,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out: U256,
}

impl Default for ArbitrageRoute {
    fn default() -> Self {
        Self {
            legs: vec![],
            initial_amount_in_wei: U256::zero(),
            amount_out_wei: U256::zero(),
            profit_usd: 0.0,
            estimated_gas_cost_wei: U256::zero(),
            price_impact_bps: 0,
            route_description: String::new(),
            risk_score: 0.0,
            chain_id: 0,
            created_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            mev_risk_score: 0.0,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: Address::zero(),
            token_out: Address::zero(),
            amount_in: U256::zero(),
            amount_out: U256::zero(),
        }
    }
}

impl ArbitrageRoute {
    pub fn to_tx(&self, wallet_address: Address) -> TransactionRequest {
        let mut tx = TransactionRequest::new();
        tx.from = Some(wallet_address.into());
        if let Some(first_leg) = self.legs.first() {
            tx.to = Some(first_leg.pool_address.into());
        }
        tx.value = Some(self.amount_in);
        tx.data = Some(self.encode_calldata());
        tx
    }

    pub fn encode_calldata(&self) -> Bytes {
        // This is a simplified implementation - in production you'd encode the actual DEX calls
        let mut calldata = Vec::new();
        for leg in &self.legs {
            // Encode swap call for each leg
            // For now, use a default sender address - in production this should be passed properly
            let sender_address = Address::zero();
            let swap_data = self.encode_swap_call(leg, sender_address);
            calldata.extend(swap_data);
        }
        Bytes::from(calldata)
    }

    fn encode_swap_call(&self, leg: &ArbitrageRouteLeg, sender_address: Address) -> Vec<u8> {
        // Simplified swap encoding - in production this would be protocol-specific
        match leg.protocol_type {
            ProtocolType::UniswapV2 => self.encode_uniswap_v2_swap(leg, sender_address),
            ProtocolType::UniswapV3 => self.encode_uniswap_v3_swap(leg, sender_address),
            ProtocolType::Curve => self.encode_curve_swap(leg, sender_address),
            _ => Vec::new(),
        }
    }

    fn encode_uniswap_v2_swap(&self, leg: &ArbitrageRouteLeg, sender_address: Address) -> Vec<u8> {
        use ethers::abi::{encode, Token};
        
        // swapExactTokensForTokens(uint256,uint256,address[],address,uint256)
        let function_selector = [0x38, 0xed, 0x17, 0x39]; // swapExactTokensForTokens
        
        let tokens = vec![
            Token::Uint(leg.amount_in),
            Token::Uint(leg.min_amount_out),
            Token::Array(vec![Token::Address(leg.token_in), Token::Address(leg.token_out)]), // path
            Token::Address(sender_address), // recipient
            Token::Uint(U256::from(u64::MAX)), // deadline (far future)
        ];
        
        let mut data = Vec::new();
        data.extend_from_slice(&function_selector);
        data.extend_from_slice(&encode(&tokens));
        data
    }

    fn encode_uniswap_v3_swap(&self, leg: &ArbitrageRouteLeg, sender_address: Address) -> Vec<u8> {
        use ethers::abi::{encode, Token};
        
        // exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))
        let function_selector = [0x41, 0x4b, 0xf3, 0x89]; // exactInputSingle
        
        // Build the struct parameter
        let params = Token::Tuple(vec![
            Token::Address(leg.token_in),
            Token::Address(leg.token_out),
            Token::Uint(U256::from(3000)), // fee tier (0.3% default)
            Token::Address(sender_address), // recipient
            Token::Uint(U256::from(u64::MAX)), // deadline
            Token::Uint(leg.amount_in),
            Token::Uint(leg.min_amount_out),
            Token::Uint(U256::zero()), // sqrtPriceLimitX96 (no limit)
        ]);
        
        let mut data = Vec::new();
        data.extend_from_slice(&function_selector);
        data.extend_from_slice(&encode(&[params]));
        data
    }

    fn encode_curve_swap(&self, leg: &ArbitrageRouteLeg, _sender_address: Address) -> Vec<u8> {
        use ethers::abi::{encode, Token};
        
        // exchange(int128,int128,uint256,uint256)
        let function_selector = [0x3d, 0xf0, 0x21, 0x24]; // exchange
        
        // For Curve, we need to determine the pool's token indices
        // This is a simplified version - in production, you'd look up the actual indices
        let token_in_index = 0i128; // Would be determined from pool configuration
        let token_out_index = 1i128;
        
        let tokens = vec![
            Token::Int(U256::from(token_in_index)),
            Token::Int(U256::from(token_out_index)),
            Token::Uint(leg.amount_in),
            Token::Uint(leg.min_amount_out),
        ];
        
        let mut data = Vec::new();
        data.extend_from_slice(&function_selector);
        data.extend_from_slice(&encode(&tokens));
        data
    }

    /// Create ArbitrageRoute from Path with context for proper configuration
    pub fn from_path_with_context(p: &Path, context: &StrategyContext) -> Self {
        use tracing::debug;

        debug!("Converting Path to ArbitrageRoute with context");

        // Build legs using the Path's legs field for correct per-hop amounts
        let legs: Vec<ArbitrageRouteLeg> = p.legs.iter().map(|leg| {
            let dex_name = match &leg.protocol_type {
                ProtocolType::UniswapV2 => "Uniswap V2",
                ProtocolType::UniswapV3 => "Uniswap V3",
                ProtocolType::UniswapV4 => "Uniswap V4",
                ProtocolType::SushiSwap => "SushiSwap",
                ProtocolType::Curve => "Curve",
                ProtocolType::Balancer => "Balancer",
                ProtocolType::Other(name) => name,
                ProtocolType::Unknown => "Unknown",
            }.to_string();

            ArbitrageRouteLeg {
                dex_name,
                protocol_type: leg.protocol_type.clone(),
                pool_address: leg.pool_address,
                token_in: leg.from_token,
                token_out: leg.to_token,
                amount_in: leg.amount_in,
                min_amount_out: leg.amount_in, // Use amount_in as min_amount_out for now
                fee_bps: leg.fee,
                expected_out: Some(leg.amount_out),
            }
        }).collect();

        // Get chain ID from context instead of hardcoded value
        let chain_id = context.chain.parse::<u64>().unwrap_or(1); // Default to 1 (Ethereum mainnet) if parsing fails

        debug!("ArbitrageRoute created with {} legs for chain {}", legs.len(), chain_id);
        Self {
            legs,
            initial_amount_in_wei: p.initial_amount_in_wei,
            amount_out_wei: p.expected_output,
            profit_usd: 0.0, // Will be calculated later
            estimated_gas_cost_wei: p.estimated_gas_cost_wei,
            price_impact_bps: p.price_impact_bps,
            route_description: format!(
                "{} → {} ({})",
                p.token_path.first().unwrap_or(&Address::zero()),
                p.token_path.last().unwrap_or(&Address::zero()),
                if p.token_path.first() == p.token_path.last() { "Cyclic" } else { "Linear" }
            ),
            risk_score: 0.0,
            chain_id,
            created_at_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64, // Use microseconds to avoid truncation
            mev_risk_score: 0.0,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: *p.token_path.first().unwrap_or(&Address::zero()),
            token_out: *p.token_path.last().unwrap_or(&Address::zero()),
            amount_in: p.initial_amount_in_wei,
            amount_out: p.expected_output,
        }
    }
}

/// A fully validated and quantified arbitrage opportunity, ready for risk assessment and execution.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbitrageOpportunity {
    pub route: ArbitrageRoute,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub profit_usd: f64,
    pub profit_token_amount: U256,
    pub profit_token: Address,
    pub estimated_gas_cost_wei: U256,
    pub risk_score: f64,
    pub mev_risk_score: f64,
    pub simulation_result: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub chain_id: u64,
}

impl ArbitrageOpportunity {
    pub fn id(&self) -> String {
        format!("{}-{}", self.block_number, self.route.route_description)
    }
}

//================================================================================================//
//                                   EXECUTION & STATE TYPES                                      //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Pending,
    Confirmed,
    Failed,
    Dropped,
    Replaced,
    Submitted,
}


/// Represents the reconstructed state of a DEX pool at a specific block, derived from historical swaps.
/// Used for backtesting and simulation where on-chain state is not directly accessible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MintParams {
    pub token0: Address,
    pub token1: Address,
    pub fee: u32,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub amount0_desired: U256,
    pub amount1_desired: U256,
    pub amount0_min: U256,
    pub amount1_min: U256,
    pub recipient: Address,
    pub deadline: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PoolStateFromSwaps {
    pub pool_address: Address,
    pub token0: Address,
    pub token1: Address,
    pub reserve0: U256,
    pub reserve1: U256,
    pub sqrt_price_x96: Option<U256>, // For Uniswap V3-style pools
    pub liquidity: Option<U256>,      // For Uniswap V3-style pools
    pub tick: Option<i32>,            // For Uniswap V3-style pools
    pub v3_tick_data: Option<std::collections::HashMap<i32, TickData>>, // For Uniswap V3-style pools
    pub v3_tick_current: Option<i32>, // For Uniswap V3-style pools
    pub fee_tier: u32,
    pub protocol: DexProtocol,
    pub protocol_type: ProtocolType,
    pub block_number: u64,
    pub timestamp: u64,
    pub token0_decimals: u32,
    pub token1_decimals: u32,
    pub price0_cumulative_last: Option<U256>,
    pub price1_cumulative_last: Option<U256>,
    pub dex_protocol: DexProtocol,
    /// Optional pool-specific timestamp from getReserves (V2) or similar
    /// This can be used to detect stale pool state relative to block timestamp
    pub last_reserve_update_timestamp: Option<u32>,
}

impl Default for PoolStateFromSwaps {
    fn default() -> Self {
        Self {
            pool_address: Address::zero(),
            token0: Address::zero(),
            token1: Address::zero(),
            reserve0: U256::zero(),
            reserve1: U256::zero(),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            v3_tick_data: None,
            v3_tick_current: None,
            fee_tier: 0,
            protocol: DexProtocol::UniswapV2,
            protocol_type: ProtocolType::UniswapV2,
            block_number: 0,
            timestamp: 0,
            token0_decimals: 18,
            token1_decimals: 18,
            price0_cumulative_last: None,
            price1_cumulative_last: None,
            dex_protocol: DexProtocol::UniswapV2,
            last_reserve_update_timestamp: None,
        }
    }
}


#[derive(Clone)]
pub struct ArbitrageEngine {
    pub config: Arc<ArbitrageSettings>,
    pub full_config: Arc<Config>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub risk_manager: Arc<tokio::sync::RwLock<Option<Arc<dyn RiskManager + Send + Sync>>>>,
    pub blockchain_manager: Arc<dyn crate::blockchain::BlockchainManager + Send + Sync>,
    pub quote_source: Arc<dyn crate::quote_source::AggregatorQuoteSource + Send + Sync>,
    pub global_metrics: Arc<ArbitrageMetrics>,
    pub alpha_engine: Option<Arc<crate::alpha::engine::AlphaEngine>>,
    pub swap_loader: Option<Arc<crate::swap_loader::HistoricalSwapLoader>>,
}

impl ArbitrageEngine {
    /// Get the path finder for this engine
    pub async fn get_path_finder(&self) -> Result<Arc<dyn crate::path::PathFinder + Send + Sync>, crate::errors::ArbitrageError> {
        let module_config = self.full_config.module_config.as_ref()
            .ok_or_else(|| crate::errors::ArbitrageError::RiskCheckFailed("No module config available".to_string()))?;
        
        let path_finder_settings = module_config.path_finder_settings.clone();
        let pool_manager = self.get_pool_manager().await?;
        let blockchain_manager = self.blockchain_manager.clone();
        let _quote_source = self.quote_source.clone();
        let gas_oracle = self.get_gas_oracle().await?;
        let price_oracle = self.price_oracle.clone();
        let chain_id = blockchain_manager.get_chain_id();
        let chain_name = blockchain_manager.get_chain_name();
        let strict_backtest = matches!(self.full_config.mode, crate::config::ExecutionMode::Backtest);
        
        // Get router tokens from config
        let router_tokens = self.full_config.chain_config.chains
            .get(chain_name)
            .and_then(|c| c.router_tokens.clone())
            .unwrap_or_default();
        
        let path_finder = crate::path::AStarPathFinder::new(
            path_finder_settings,
            pool_manager,
            blockchain_manager,
            self.full_config.clone(),
            Arc::new(crate::gas_oracle::GasOracle::new(gas_oracle.clone())),
            price_oracle,
            chain_id,
            strict_backtest,
            router_tokens,
        ).map_err(|e| crate::errors::ArbitrageError::PathFinding(e))?;
        
        Ok(Arc::new(path_finder) as Arc<dyn crate::path::PathFinder + Send + Sync>)
    }

    /// Robust historical price fallback that never defaults to $1 for unknown tokens.
    /// Order: historical at block → live current (warn) → known stablecoins → known WETH conservative → error
    async fn get_token_price_with_fallback(
        &self,
        chain_name: &str,
        token: Address,
        block: u64,
    ) -> Result<f64, crate::errors::PriceError> {
        use crate::errors::PriceError;

        // 1) Try exact historical price at block
        if let Ok(price) = self
            .price_oracle
            .get_token_price_usd_at(chain_name, token, Some(block), None)
            .await
        {
            if price.is_finite() && price > 0.0 {
                return Ok(price);
            }
        }

        // 2) Try current live price with a warning
        if let Ok(price) = self.price_oracle.get_token_price_usd(chain_name, token).await {
            if price.is_finite() && price > 0.0 {
                tracing::warn!(
                    token = ?token,
                    block,
                    "Using current live price for historical block; historical price unavailable"
                );
                return Ok(price);
            }
        }

        // 3) Stablecoin shortcut: if token is configured as a stablecoin, assume $1.00
        if let Some(chain_cfg) = self.full_config.chain_config.chains.get(chain_name) {
            if let Some(stables) = &chain_cfg.reference_stablecoins {
                if stables.iter().any(|addr| *addr == token) {
                    return Ok(1.0);
                }
            }
            // 4) WETH fallback: if token equals WETH, use a conservative bound (e.g., $1000) to avoid $1 default
            if chain_cfg.weth_address == token {
                // Conservative fallback bound for WETH (adjusted per request)
                return Ok(2000.0);
            }
        }

        // 5) No price available
        Err(PriceError::NotAvailable(format!(
            "No price available for token {:?} at block {} on {}",
            token, block, chain_name
        )))
    }

    /// Dynamic gas estimation based on route hops and current gas price with a 20% safety buffer.
    async fn estimate_arbitrage_gas(
        &self,
        chain_name: &str,
        route: &ArbitrageRoute,
        block_number: u64,
    ) -> Result<U256, crate::errors::ArbitrageError> {
        let base_gas: u64 = 21_000;
        let per_swap_gas: u64 = 150_000;
        let num_swaps = route.legs.len() as u64;
        let total_units = base_gas.saturating_add(per_swap_gas.saturating_mul(num_swaps));

        // Add 20% buffer
        let total_units_with_buffer = total_units
            .saturating_mul(120)
            .saturating_div(100);

        // Fetch gas price from oracle
        let gas_price = self
            .get_gas_oracle()
            .await?
            .get_gas_price(chain_name, Some(block_number))
            .await
            .map_err(|e| crate::errors::ArbitrageError::BlockchainQueryFailed(format!(
                "Failed to fetch gas price: {}",
                e
            )))?;
        let effective_price = gas_price.effective_price();

        Ok(U256::from(total_units_with_buffer).saturating_mul(effective_price))
    }

    /// Get the pool manager for this engine
    pub async fn get_pool_manager(&self) -> Result<Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>, crate::errors::ArbitrageError> {
        let _module_config = self.full_config.module_config.as_ref()
            .ok_or_else(|| crate::errors::ArbitrageError::RiskCheckFailed("No module config available".to_string()))?;
        
        // Create historical swap loader if in backtest mode
        let historical_swap_loader = match &self.full_config.mode {
            crate::config::ExecutionMode::Backtest => {
                // Get shared database pool instead of creating a new one
                let db_pool = crate::database::get_shared_database_pool(&self.full_config)
                    .await
                    .map_err(|e| crate::errors::ArbitrageError::RiskCheckFailed(format!("Failed to get shared database pool: {}", e)))?;

                let chain_config = Arc::new(self.full_config.chain_config.clone());

                // Use the existing swap loader if available, otherwise create one (backwards compatibility)
                if let Some(existing_swap_loader) = &self.swap_loader {
                    Some(existing_swap_loader.clone())
                } else {
                    use std::sync::atomic::{AtomicUsize, Ordering};
                    static SWAP_LOADER_COUNT: AtomicUsize = AtomicUsize::new(0);
                    let count = SWAP_LOADER_COUNT.fetch_add(1, Ordering::Relaxed);
                    warn!("CREATING HistoricalSwapLoader #{} in ArbitrageEngine::get_pool_manager - this should only happen once per backtest session", count + 1);
                    let swap_loader = Arc::new(crate::swap_loader::HistoricalSwapLoader::new(
                        db_pool,
                        chain_config,
                    ).await.map_err(|e| crate::errors::ArbitrageError::RiskCheckFailed(format!("Failed to create historical swap loader: {}", e)))?);
                    Some(swap_loader)
                }
            },
            _ => None,
        };
        
        // Step 1: Create pool manager without state oracle to break circular dependency
        let pool_manager = Arc::new(crate::pool_manager::PoolManager::new_without_state_oracle(
            self.full_config.clone(),
        ).await.map_err(|e| crate::errors::ArbitrageError::RiskCheckFailed(format!("Failed to create pool manager: {}", e)))?);
        
        // Step 2: Create blockchain managers map for pool state oracle
        let mut pool_state_chains: std::collections::HashMap<String, Arc<dyn crate::blockchain::BlockchainManager>> = std::collections::HashMap::new();
        pool_state_chains.insert(
            self.blockchain_manager.get_chain_name().to_string(), 
            self.blockchain_manager.clone() as Arc<dyn crate::blockchain::BlockchainManager>
        );
        
        // Step 3: Create pool state oracle with the pool manager
        let pool_state_oracle = Arc::new(crate::pool_states::PoolStateOracle::new(
            self.full_config.clone(),
            pool_state_chains,
            historical_swap_loader,
            pool_manager.clone() as Arc<dyn crate::pool_manager::PoolManagerTrait>,
        ).map_err(|e| crate::errors::ArbitrageError::RiskCheckFailed(format!("Failed to create pool state oracle: {}", e)))?);
        
        // Step 4: Set the pool state oracle back into the pool manager
        pool_manager.set_state_oracle(pool_state_oracle).await
            .map_err(|e| crate::errors::ArbitrageError::RiskCheckFailed(format!("Failed to set state oracle: {}", e)))?;
        
        Ok(pool_manager as Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>)
    }

    /// Get the gas oracle for this engine
    pub async fn get_gas_oracle(&self) -> Result<Arc<dyn crate::gas_oracle::GasOracleProvider + Send + Sync>, crate::errors::ArbitrageError> {
        let blockchain_manager = self.blockchain_manager.clone();

        // First check if the blockchain manager already has a gas oracle configured
        // This handles backtest mode where HistoricalGasProvider is set via set_gas_oracle()
        if let Ok(existing_gas_oracle) = blockchain_manager.get_gas_oracle().await {
            return Ok(existing_gas_oracle);
        }

        // Fallback: create a new LiveGasProvider for live mode
        let module_config = self.full_config.module_config.as_ref()
            .ok_or_else(|| crate::errors::ArbitrageError::RiskCheckFailed("No module config available".to_string()))?;

        let gas_oracle_config = module_config.gas_oracle_config.clone();
        let chain_name = blockchain_manager.get_chain_name();

        // Get chain_id from configuration
        let chain_id = self.full_config.chain_config.chains
            .get::<str>(chain_name.as_ref())
            .map(|c| c.chain_id)
            .ok_or_else(|| crate::errors::ArbitrageError::RiskCheckFailed(
                format!("Chain ID not found for chain {}", chain_name)
            ))?;

        let gas_oracle_provider = crate::gas_oracle::live_gas_provider::LiveGasProvider::new(
            chain_name.to_string(),
            chain_id,
            self.full_config.clone(),
            gas_oracle_config,
            blockchain_manager,
        );
        
        Ok(Arc::new(gas_oracle_provider) as Arc<dyn crate::gas_oracle::GasOracleProvider + Send + Sync>)
    }

    /// Get the transaction optimizer for this engine
    pub async fn get_transaction_optimizer(&self) -> Result<Arc<dyn crate::transaction_optimizer::TransactionOptimizerTrait + Send + Sync>, crate::errors::ArbitrageError> {
        let module_config = self.full_config.module_config.as_ref()
            .ok_or_else(|| crate::errors::ArbitrageError::RiskCheckFailed("No module config available".to_string()))?;
        
        let transaction_optimizer_settings = module_config.transaction_optimizer_settings.clone();
        let blockchain_manager = self.blockchain_manager.clone();
        let gas_oracle = self.get_gas_oracle().await?;
        let price_oracle = self.price_oracle.clone();
        
        // Default arbitrage executor address - should be configured per chain in production
        let arbitrage_executor_address = crate::types::Address::from_str("0x1234567890123456789012345678901234567890")
            .unwrap_or_else(|_| crate::types::Address::zero());

        // Create EnhancedOptimizerConfig with production defaults
        let enhanced_config = crate::transaction_optimizer::EnhancedOptimizerConfig {
            base_config: Arc::new(transaction_optimizer_settings.clone()),
            monitoring: crate::transaction_optimizer::MonitoringConfig::default(),
            security: crate::transaction_optimizer::SecurityConfig {
                max_transaction_value: U256::from_dec_str("1000000000000000000000").unwrap(), // 1000 ETH
                max_gas_limit: U256::from(10_000_000u64),
                blacklisted_addresses: std::collections::HashSet::new(),
                whitelisted_addresses: std::collections::HashSet::new(),
                require_whitelist: false,
                max_daily_transactions: 1000,
                max_daily_value: U256::from_dec_str("10000000000000000000000").unwrap(), // 10000 ETH
            },
            performance: crate::transaction_optimizer::PerformanceConfig {
                simulation_cache_size: 1000,
                simulation_cache_ttl: std::time::Duration::from_secs(300),
                max_concurrent_retries: 3,
                retry_jitter_factor: 0.1,
                bundle_status_check_method: crate::transaction_optimizer::BundleCheckMethod::Hybrid,
            },
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: std::time::Duration::from_secs(300),
        };

        let transaction_optimizer = crate::transaction_optimizer::TransactionOptimizer::new(
            Arc::new(transaction_optimizer_settings),
            enhanced_config,
            blockchain_manager,
            gas_oracle,
            price_oracle,
            None, // mempool_stats_provider is optional
            arbitrage_executor_address,
        ).await.map_err(|e| crate::errors::ArbitrageError::Submission(format!("Failed to create transaction optimizer: {}", e)))?;
        
        let optimizer_obj: Arc<dyn crate::transaction_optimizer::TransactionOptimizerTrait + Send + Sync> = transaction_optimizer;
        Ok(optimizer_obj)
    }
}

#[async_trait]
pub trait ArbitrageEngineTrait: Send + Sync {
    async fn find_opportunities(
        &self,
        swap_events: &[DexSwapEvent],
    ) -> Result<Vec<ArbitrageOpportunity>, ArbitrageError>;

    async fn execute_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<ExecutionResult, ArbitrageError>;
}

#[async_trait]
impl ArbitrageEngineTrait for ArbitrageEngine {
    async fn find_opportunities(
        &self,
        swap_events: &[DexSwapEvent],
    ) -> Result<Vec<ArbitrageOpportunity>, ArbitrageError> {
        // Get the current block number from the first swap event, or use a default
        let block_number = swap_events.first()
            .map(|event| event.block_number)
            .unwrap_or(0);

        // First, try to get opportunities from AlphaEngine if available
        if let Some(alpha_engine) = &self.alpha_engine {
            match alpha_engine.get_arbitrage_opportunities(block_number).await {
                Ok(opportunities) => {
                    if !opportunities.is_empty() {
                        tracing::debug!("AlphaEngine found {} arbitrage opportunities for block {}", opportunities.len(), block_number);
                        return Ok(opportunities);
                    }
                }
                Err(e) => {
                    tracing::warn!("AlphaEngine failed to get opportunities: {}", e);
                }
            }
        }

        // Fall back to pathfinder-based approach if AlphaEngine is not available or failed
        tracing::debug!("Using pathfinder-based arbitrage detection for block {}", block_number);
        
        // Convert DexSwapEvent to SwapEvent for compatibility
        let swap_events_converted: Vec<SwapEvent> = swap_events.iter().map(|dex_event| {
            SwapEvent {
                chain_name: dex_event.chain_name.clone(),
                pool_address: dex_event.pool_address,
                tx_hash: dex_event.transaction_hash,
                block_number: dex_event.block_number,
                log_index: dex_event.log_index,
                unix_ts: dex_event.unix_ts.unwrap_or(0),
                token_in: dex_event.token_in.unwrap_or(Address::zero()),
                token_out: dex_event.token_out.unwrap_or(Address::zero()),
                amount_in: dex_event.amount_in.unwrap_or(U256::zero()),
                amount_out: dex_event.amount_out.unwrap_or(U256::zero()),
                sender: Address::zero(), // Not available in DexSwapEvent
                recipient: Address::zero(), // Not available in DexSwapEvent
                gas_used: None,
                effective_gas_price: None,
                protocol: dex_event.protocol.clone(),
                data: match &dex_event.data {
                    SwapData::UniswapV2 { sender, to, amount0_in, amount1_in, amount0_out, amount1_out } => {
                        crate::types::SwapData::UniswapV2 {
                            sender: *sender,
                            to: *to,
                            amount0_in: *amount0_in,
                            amount1_in: *amount1_in,
                            amount0_out: *amount0_out,
                            amount1_out: *amount1_out,
                        }
                    },
                    SwapData::UniswapV3 { sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick } => {
                        crate::types::SwapData::UniswapV3 {
                            sender: *sender,
                            recipient: *recipient,
                            amount0: *amount0,
                            amount1: *amount1,
                            sqrt_price_x96: *sqrt_price_x96,
                            liquidity: *liquidity,
                            tick: *tick,
                        }
                    },
                    SwapData::Curve { buyer, sold_id, tokens_sold, bought_id, tokens_bought } => {
                        crate::types::SwapData::Curve {
                            buyer: *buyer,
                            sold_id: *sold_id,
                            tokens_sold: *tokens_sold,
                            bought_id: *bought_id,
                            tokens_bought: *tokens_bought,
                        }
                    },
                    SwapData::Balancer { pool_id, token_in, token_out, amount_in, amount_out } => {
                        crate::types::SwapData::Balancer {
                            pool_id: *pool_id,
                            token_in: *token_in,
                            token_out: *token_out,
                            amount_in: *amount_in,
                            amount_out: *amount_out,
                        }
                    },
                },
                token0: dex_event.token0,
                token1: dex_event.token1,
                price_t0_in_t1: dex_event.price_t0_in_t1,
                price_t1_in_t0: dex_event.price_t1_in_t0,
                reserve0: dex_event.reserve0,
                reserve1: dex_event.reserve1,
                transaction: dex_event.transaction.clone(),
                transaction_metadata: dex_event.transaction_metadata.clone(),
                pool_id: dex_event.pool_id,
                buyer: dex_event.buyer,
                sold_id: dex_event.sold_id,
                tokens_sold: dex_event.tokens_sold,
                bought_id: dex_event.bought_id,
                tokens_bought: dex_event.tokens_bought,
                sqrt_price_x96: dex_event.sqrt_price_x96,
                liquidity: dex_event.liquidity,
                tick: dex_event.tick,
                token0_decimals: dex_event.token0_decimals,
                token1_decimals: dex_event.token1_decimals,
                fee_tier: dex_event.fee_tier,
                protocol_type: dex_event.protocol_type.clone(),
                v3_amount0: None, // Not available in DexSwapEvent
                v3_amount1: None, // Not available in DexSwapEvent
                v3_tick_data: None, // Not available in DexSwapEvent
                price_from_in_usd: None,
                price_to_in_usd: None,
                token0_price_usd: None,
                token1_price_usd: None,
                pool_liquidity_usd: None,
            }
        }).collect();

        // Use the pathfinder to find arbitrage opportunities
        let path_finder = self.get_path_finder().await?;
        let pool_manager = self.get_pool_manager().await?;
        let _price_oracle = self.price_oracle.clone();

        // Validate pool states from swap events for accurate path finding
        for event in &swap_events_converted {
            if let Ok(_pool_info) = pool_manager.get_pool_info(&event.chain_name, event.pool_address, Some(event.block_number)).await {
                tracing::debug!("Pool {} validated for block {}", event.pool_address, event.block_number);
            } else {
                tracing::debug!("Pool {} not found or invalid for block {}", event.pool_address, event.block_number);
            }
        }

        let mut opportunities = Vec::new();

        // Extract unique tokens from the swap events
        let mut affected_tokens = std::collections::HashSet::new();
        for event in &swap_events_converted {
            if event.token_in != Address::zero() {
                affected_tokens.insert(event.token_in);
            }
            if event.token_out != Address::zero() {
                affected_tokens.insert(event.token_out);
            }
        }

        // For each affected token, try to find cyclic arbitrage paths
        for &start_token in &affected_tokens {
            // Use a reasonable scan amount (e.g., 1 ETH worth)
            let scan_amount = U256::from(1_000_000_000_000_000_000u128); // 1 ETH in wei

            // Find circular arbitrage paths starting from this token
            match path_finder
                .find_circular_arbitrage_paths(start_token, scan_amount, Some(block_number))
                .await
            {
                Ok(paths) => {
                    for path in paths {
                        // Calculate profit in wei
                        let profit_wei = path.expected_output.saturating_sub(scan_amount);
                        
                        if profit_wei.is_zero() {
                            continue; // Skip unprofitable paths
                        }

                        // Convert profit to USD - use the actual chain name
                        let chain_name = &swap_events_converted.first()
                            .map(|e| e.chain_name.clone())
                            .unwrap_or_else(|| "ethereum".to_string());

                        // Fetch token decimals at the block to avoid assuming 18
                        let profit_token_decimals = self.blockchain_manager
                            .get_token_decimals(
                                start_token,
                                Some(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(block_number.into())))
                            )
                            .await
                            .unwrap_or(18u8);

                        // Robust price lookup with fallback that never defaults to $1 for unknown tokens
                        let price_result = self
                            .get_token_price_with_fallback(chain_name, start_token, block_number)
                            .await;

                        let profit_usd = match price_result {
                            Ok(price) => {
                                let profit_f64 = normalize_units(profit_wei, profit_token_decimals);
                                profit_f64 * price
                            }
                            Err(err) => {
                                tracing::warn!(
                                    token = ?start_token,
                                    block = block_number,
                                    error = %err,
                                    "Skipping opportunity: no reliable price available for token"
                                );
                                continue;
                            }
                        };

                        // Get minimum profit threshold from config or use a conservative default ($0.10)
                        let min_profit_usd = self.full_config
                            .module_config
                            .as_ref()
                            .map(|m| m.arbitrage_settings.min_profit_usd)
                            .unwrap_or(0.10);

                        // Skip if profit is too small
                        if profit_usd < min_profit_usd {
                            tracing::debug!("Skipping opportunity with profit ${:.2} < min ${:.2}", profit_usd, min_profit_usd);
                            continue;
                        }
                        
                        tracing::info!("Found profitable opportunity: ${:.2} USD", profit_usd);

                        // Convert Path to ArbitrageRoute
                        let chain_id = self.blockchain_manager.get_chain_id();
                        let route = ArbitrageRoute::from_path(&path, chain_id);
                        
                        // Create ArbitrageOpportunity
                        let opportunity = ArbitrageOpportunity {
                            route,
                            block_number,
                            block_timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            profit_usd,
                            profit_token_amount: profit_wei,
                            profit_token: start_token,
                            // Prefer path/route-based estimate; otherwise compute dynamically from hops and current gas price
                            estimated_gas_cost_wei: {
                                if !path.estimated_gas_cost_wei.is_zero() {
                                    path.estimated_gas_cost_wei
                                } else if !opportunities.is_empty() {
                                    // Will be set after route creation below
                                    U256::zero()
                                } else {
                                    U256::zero()
                                }
                            },
                            risk_score: 0.2,
                            mev_risk_score: 0.1,
                            simulation_result: None,
                            metadata: None,
                            token_in: start_token,
                            token_out: start_token, // For cyclic arbitrage, start and end are the same
                            amount_in: scan_amount,
                            chain_id: self.blockchain_manager.get_chain_id(),
                        };

                        // If gas cost was not provided, compute a dynamic estimate now using gas oracle and route length
                        let mut opportunity = opportunity;
                        if opportunity.estimated_gas_cost_wei.is_zero() {
                            if let Some(gas_estimate) = self
                                .estimate_arbitrage_gas(chain_name, &opportunity.route, block_number)
                                .await
                                .ok()
                            {
                                opportunity.estimated_gas_cost_wei = gas_estimate;
                            }
                        }

                        opportunities.push(opportunity);
                    }
                }
                Err(e) => {
                    tracing::debug!("PathFinder failed for token {:?}: {:?}", start_token, e);
                    continue;
                }
            }
        }

        tracing::debug!("Found {} arbitrage opportunities from {} swap events", opportunities.len(), swap_events.len());
        Ok(opportunities)
    }

    async fn execute_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<ExecutionResult, ArbitrageError> {
        // Placeholder implementation - in a real scenario, this would execute the arbitrage
        Ok(ExecutionResult {
            tx_hash: H256::zero(),
            status: ExecutionStatus::Failed,
            gas_used: None,
            effective_gas_price: None,
            block_number: None,
            execution_time_ms: 0,
            route_info: None,
            error: Some("Not implemented".to_string()),
            predicted_profit_usd: opportunity.profit_usd,
            prediction_id: H256::zero(),
            success: false,
            actual_profit_usd: None,
            failure_reason: Some("ArbitrageEngineTrait not fully implemented".to_string()),
        })
    }
}

/// Information about the availability of price data for a given token and block.
/// Used by price oracles and backtest loaders to indicate whether historical or live data is available.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataAvailabilityInfo {
    pub chain_name: String,
    pub total_swaps: i64,
    pub range_swaps: i64,
    pub min_block: i64,
    pub max_block: i64,
    pub requested_start: u64,
    pub requested_end: u64,
}

/// Types of MEV attacks, used for labeling and analytics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MEVAttackType {
    Sandwich,
    Arbitrage,
    Liquidation,
    Frontrun,
    Backrun,
    TimeBandit,
    Censoring,
    Other(u8),
}

impl std::fmt::Display for MEVAttackType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MEVAttackType::Sandwich => write!(f, "Sandwich"),
            MEVAttackType::Arbitrage => write!(f, "Arbitrage"),
            MEVAttackType::Liquidation => write!(f, "Liquidation"),
            MEVAttackType::Frontrun => write!(f, "Frontrun"),
            MEVAttackType::Backrun => write!(f, "Backrun"),
            MEVAttackType::TimeBandit => write!(f, "TimeBandit"),
            MEVAttackType::Censoring => write!(f, "Censoring"),
            MEVAttackType::Other(id) => write!(f, "Other({})", id),
        }
    }
}



/// Supported DEX protocols for swaps and pool state reconstruction.
/// This enum is used throughout the system for protocol-specific logic and serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DexProtocol {
    UniswapV2,
    UniswapV3,
    Curve,
    Balancer,
    SushiSwap,
    SushiSwapV2,
    PancakeSwap,
    PancakeSwapV2,
    PancakeSwapV3,
    UniswapV4,
    Bancor,
    TraderJoe,
    Kyber,
    Dodo,
    ShibaSwap,
    Camelot,
    Velodrome,
    Solidly,
    FraxSwap,
    Clipper,
    Maverick,
    Smardex,
    SyncSwap,
    Ambient,
    Carbon,
    Unknown,
    Other(u16), // For unknown or custom protocols, store a numeric identifier
}

impl std::fmt::Display for DexProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DexProtocol::UniswapV2 => write!(f, "UniswapV2"),
            DexProtocol::UniswapV3 => write!(f, "UniswapV3"),
            DexProtocol::Curve => write!(f, "Curve"),
            DexProtocol::Balancer => write!(f, "Balancer"),
            DexProtocol::SushiSwap => write!(f, "SushiSwap"),
            DexProtocol::SushiSwapV2 => write!(f, "SushiSwapV2"),
            DexProtocol::PancakeSwap => write!(f, "PancakeSwap"),
            DexProtocol::PancakeSwapV2 => write!(f, "PancakeSwapV2"),
            DexProtocol::PancakeSwapV3 => write!(f, "PancakeSwapV3"),
            DexProtocol::UniswapV4 => write!(f, "UniswapV4"),
            DexProtocol::Bancor => write!(f, "Bancor"),
            DexProtocol::TraderJoe => write!(f, "TraderJoe"),
            DexProtocol::Kyber => write!(f, "Kyber"),
            DexProtocol::Dodo => write!(f, "Dodo"),
            DexProtocol::ShibaSwap => write!(f, "ShibaSwap"),
            DexProtocol::Camelot => write!(f, "Camelot"),
            DexProtocol::Velodrome => write!(f, "Velodrome"),
            DexProtocol::Solidly => write!(f, "Solidly"),
            DexProtocol::FraxSwap => write!(f, "FraxSwap"),
            DexProtocol::Clipper => write!(f, "Clipper"),
            DexProtocol::Maverick => write!(f, "Maverick"),
            DexProtocol::Smardex => write!(f, "Smardex"),
            DexProtocol::SyncSwap => write!(f, "SyncSwap"),
            DexProtocol::Ambient => write!(f, "Ambient"),
            DexProtocol::Carbon => write!(f, "Carbon"),
            DexProtocol::Unknown => write!(f, "Unknown"),
            DexProtocol::Other(id) => write!(f, "Other({})", id),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionResult {
    pub tx_hash: H256,
    pub status: ExecutionStatus,
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
    pub block_number: Option<u64>,
    pub execution_time_ms: u64,
    pub route_info: Option<RouteInfo>, // Simplified from ArbitrageRoute for logging
    pub error: Option<String>,
    pub predicted_profit_usd: f64,
    pub prediction_id: H256,
    pub success: bool,
    pub actual_profit_usd: Option<f64>,
    pub failure_reason: Option<String>,
}

impl From<tokio::time::error::Elapsed> for ExecutionResult {
    fn from(e: tokio::time::error::Elapsed) -> Self {
        Self {
            tx_hash: H256::zero(),
            status: ExecutionStatus::Failed,
            error: Some(format!("Timeout: {}", e)),
            ..Default::default()
        }
    }
}

impl Default for ExecutionResult {
    fn default() -> Self {
        Self {
            tx_hash: H256::zero(),
            status: ExecutionStatus::Pending,
            gas_used: None,
            effective_gas_price: None,
            block_number: None,
            execution_time_ms: 0,
            route_info: None,
            error: None,
            predicted_profit_usd: 0.0,
            prediction_id: H256::zero(),
            success: false,
            actual_profit_usd: None,
            failure_reason: None,
        }
    }
}


/// A unique identifier for an arbitrage route, used for deduplication and tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RouteId(pub u64);

/// Trait for converting between different representations of arbitrage routes.
/// This enables protocol-agnostic handling and serialization of routes.
pub trait RouteConverter: Send + Sync {
    /// Converts a protocol-specific route into a canonical, protocol-agnostic representation.
    fn to_route_info(&self, route: &ArbitrageRoute) -> RouteInfo;

    /// Optionally, reconstructs a protocol-specific route from a canonical representation.
    fn from_route_info(&self, info: &RouteInfo) -> Option<ArbitrageRoute>;
}

/// Metrics for tracking arbitrage engine performance and outcomes.
/// This struct is designed for dependency injection and encapsulates all relevant Prometheus metrics.
#[derive(Clone)]
pub struct ArbitrageMetrics {
    pub opportunities_found: &'static prometheus::IntCounterVec,
    pub opportunities_executed: &'static prometheus::IntCounterVec,
    pub trades_confirmed: &'static prometheus::IntCounter,
    pub trades_failed: &'static prometheus::IntCounterVec,
    pub profit_usd: &'static prometheus::HistogramVec,
    pub opportunities_skipped: &'static prometheus::IntCounterVec,
    pub execution_pipeline_duration_ms: &'static prometheus::HistogramVec,
    pub gas_cost_usd: &'static prometheus::HistogramVec,
    pub active_tasks: &'static prometheus::IntGaugeVec,
    pub component_health: &'static prometheus::IntGaugeVec,
}

impl ArbitrageMetrics {
    pub fn new() -> Self {
        use crate::metrics;
        Self {
            opportunities_found: &metrics::OPPORTUNITIES_FOUND,
            opportunities_executed: &metrics::OPPORTUNITIES_EXECUTED,
            trades_confirmed: &metrics::TRADES_CONFIRMED,
            trades_failed: &metrics::TRADES_FAILED,
            profit_usd: &metrics::PROFIT_USD,
            opportunities_skipped: &metrics::OPPORTUNITIES_SKIPPED,
            execution_pipeline_duration_ms: &metrics::EXECUTION_PIPELINE_DURATION_MS,
            gas_cost_usd: &metrics::GAS_COST_USD,
            active_tasks: &metrics::ACTIVE_TASKS,
            component_health: &metrics::COMPONENT_HEALTH,
        }
    }
}



#[derive(Debug, Clone, Serialize)]
pub struct RouteInfo {
    pub description: String,
    pub profit_usd: f64,
}

//================================================================================================//
//                                 UTILITY & CONVERSION                                           //
//================================================================================================//

/// Converts a U256 'amount' in 'decimals' units to f64 for calculations.
/// This version is robust and avoids always returning zero for small amounts.
pub fn normalize_units(amount: U256, decimals: u8) -> f64 {
    if decimals >= 77 {
        return 0.0;
    }
    let divisor = 10f64.powi(decimals as i32);
    if divisor == 0.0 {
        return 0.0;
    }
    let hi = (amount >> 128).as_u128() as f64;
    let lo = (amount & U256::from(u128::MAX)).as_u128() as f64;
    let scale = 2_f64.powi(128);
    ((hi * scale) + lo) / divisor
}

/// Execution priority for pool operations, e.g., for sorting or scheduling.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ExecutionPriority {
    Low,
    Normal,
    High,
    Default,
}

/// Factory trait for producing new provider instances for a given chain.
/// This enables dependency injection and testability for all provider usage.
#[derive(Debug, Clone)]
pub enum WrappedProvider {
    Evm(Arc<ethers::providers::Provider<Ws>>),
    Solana(Arc<solana_client::nonblocking::pubsub_client::PubsubClient>),
}

/// A custom error type for provider-related failures.
#[derive(Debug, thiserror::Error)]
pub enum TypesProviderError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Unsupported chain type")]
    UnsupportedChain,
    #[error("Internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait ProviderFactory: Debug + Send + Sync {
    async fn create_provider(&self, url: &str) -> Result<WrappedProvider, TypesProviderError>;
}

/// A default, production-ready implementation of the provider factory.
#[derive(Debug, Default)]
pub struct DefaultProviderFactory;

//================================================================================================//
//                                    SWAP & EVENT TYPES                                          //
//================================================================================================//

/// Protocol-specific swap data for different DEX protocols.
/// This enum contains the raw decoded data from swap events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwapData {
    UniswapV2 {
        sender: Address,
        to: Address,
        amount0_in: U256,
        amount1_in: U256,
        amount0_out: U256,
        amount1_out: U256,
    },
    UniswapV3 {
        sender: Address,
        recipient: Address,
        amount0: I256,
        amount1: I256,
        sqrt_price_x96: U256,
        liquidity: u128,
        tick: i32,
    },
    Curve {
        buyer: Address,
        sold_id: i128,
        tokens_sold: U256,
        bought_id: i128,
        tokens_bought: U256,
    },
    Balancer {
        pool_id: H256,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        amount_out: U256,
    },
}

/// Extended swap event with additional metadata for database storage and analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexSwapEvent {
    pub chain_name: String,
    pub protocol: DexProtocol,
    pub transaction_hash: H256,
    pub block_number: u64,
    pub log_index: U256,
    pub pool_address: Address,
    pub data: SwapData,
    pub unix_ts: Option<u64>,
    pub token0: Option<Address>,
    pub token1: Option<Address>,
    pub price_t0_in_t1: Option<Decimal>,
    pub price_t1_in_t0: Option<Decimal>,
    pub reserve0: Option<U256>,
    pub reserve1: Option<U256>,
    pub transaction: Option<TransactionData>,
    pub transaction_metadata: Option<serde_json::Value>,
    pub token_in: Option<Address>,
    pub token_out: Option<Address>,
    pub amount_in: Option<U256>,
    pub amount_out: Option<U256>,
    pub pool_id: Option<H256>,
    pub buyer: Option<Address>,
    pub sold_id: Option<i128>,
    pub tokens_sold: Option<U256>,
    pub bought_id: Option<i128>,
    pub tokens_bought: Option<U256>,
    pub sqrt_price_x96: Option<U256>,
    pub liquidity: Option<U256>,
    pub tick: Option<i32>,
    pub token0_decimals: Option<u8>,
    pub token1_decimals: Option<u8>,
    pub fee_tier: Option<u32>,
    pub protocol_type: Option<ProtocolType>,
}

/// Event signatures for different DEX protocols.
/// Used for identifying and decoding swap events from blockchain logs.
#[derive(Debug, Clone)]
pub struct EventSignatures {
    pub uniswap_v2_swap: String,
    pub uniswap_v3_swap: String,
    pub curve_token_exchange: String,
    pub balancer_swap: String,
}

/// Configuration for a specific DEX protocol.
#[derive(Debug, Clone)]
pub struct DexConfig {
    pub protocol: DexProtocol,
}

/// Cache for storing timestamp data with thread-safe access.
pub type TsCache = Arc<tokio::sync::Mutex<std::collections::HashMap<u64, u64>>>;

/// Transaction data with comprehensive metadata for analysis and storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransactionData {
    pub hash: H256,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub block_number: Option<U256>,
    pub block_hash: Option<H256>,
    pub gas: Option<U256>,
    pub gas_price: Option<U256>,
    pub gas_used: Option<U256>,
    pub value: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub transaction_type: Option<U256>,
    pub cumulative_gas_used: Option<U256>,
    pub chain_id: Option<U256>,
    pub base_fee_per_gas: Option<U256>,
    pub priority_fee_per_gas: Option<U256>,
}



impl DexProtocol {
    /// Creates a DexProtocol from a string representation.
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "uniswapv2" | "uniswap_v2" | "uni_v2" => Some(DexProtocol::UniswapV2),
            "uniswapv3" | "uniswap_v3" | "uni_v3" => Some(DexProtocol::UniswapV3),
            "curve" => Some(DexProtocol::Curve),
            "balancer" => Some(DexProtocol::Balancer),
            "sushiswap" | "sushi" => Some(DexProtocol::SushiSwap),
            "sushiswapv2" | "sushi_v2" => Some(DexProtocol::SushiSwapV2),
            "pancakeswap" | "pancake" => Some(DexProtocol::PancakeSwap),
            "pancakeswapv2" | "pancake_v2" => Some(DexProtocol::PancakeSwapV2),
            "pancakeswapv3" | "pancake_v3" => Some(DexProtocol::PancakeSwapV3),
            "uniswapv4" | "uniswap_v4" | "uni_v4" => Some(DexProtocol::UniswapV4),
            "bancor" => Some(DexProtocol::Bancor),
            "traderjoe" | "trader_joe" => Some(DexProtocol::TraderJoe),
            "kyber" | "kyberswap" => Some(DexProtocol::Kyber),
            "dodo" => Some(DexProtocol::Dodo),
            "shibaswap" | "shiba" => Some(DexProtocol::ShibaSwap),
            "camelot" => Some(DexProtocol::Camelot),
            "velodrome" => Some(DexProtocol::Velodrome),
            "solidly" => Some(DexProtocol::Solidly),
            "fraxswap" | "frax" => Some(DexProtocol::FraxSwap),
            "clipper" => Some(DexProtocol::Clipper),
            "maverick" => Some(DexProtocol::Maverick),
            "smardex" => Some(DexProtocol::Smardex),
            "syncswap" => Some(DexProtocol::SyncSwap),
            "ambient" => Some(DexProtocol::Ambient),
            "carbon" => Some(DexProtocol::Carbon),
            "unknown" => Some(DexProtocol::Unknown),
            _ => None,
        }
    }

    pub fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(DexProtocol::UniswapV2),
            2 => Some(DexProtocol::UniswapV3),
            3 => Some(DexProtocol::Curve),
            4 => Some(DexProtocol::Balancer),
            5 => Some(DexProtocol::SushiSwap),
            6 => Some(DexProtocol::SushiSwapV2),
            7 => Some(DexProtocol::PancakeSwap),
            8 => Some(DexProtocol::PancakeSwapV2),
            9 => Some(DexProtocol::PancakeSwapV3),
            10 => Some(DexProtocol::UniswapV4),
            11 => Some(DexProtocol::Bancor),
            12 => Some(DexProtocol::TraderJoe),
            13 => Some(DexProtocol::Kyber),
            14 => Some(DexProtocol::Dodo),
            15 => Some(DexProtocol::ShibaSwap),
            16 => Some(DexProtocol::Camelot),
            17 => Some(DexProtocol::Velodrome),
            18 => Some(DexProtocol::Solidly),
            19 => Some(DexProtocol::FraxSwap),
            20 => Some(DexProtocol::Clipper),
            21 => Some(DexProtocol::Maverick),
            22 => Some(DexProtocol::Smardex),
            23 => Some(DexProtocol::SyncSwap),
            24 => Some(DexProtocol::Ambient),
            25 => Some(DexProtocol::Carbon),
            26 => Some(DexProtocol::Unknown),
            _ => Some(DexProtocol::Other(id)),
        }
    }
}

pub struct FactoryDescriptor {
    pub factory_address: Address,
    pub protocol_type: ProtocolType,
    pub creation_event_topic: H256,
    pub blockchain_manager: Arc<dyn crate::blockchain::BlockchainManager + Send + Sync>,
}

impl FactoryDescriptor {
    pub fn new(
        config: &FactoryConfig,
        blockchain_manager: Arc<dyn crate::blockchain::BlockchainManager + Send + Sync>,
    ) -> Result<Self, DexError> {
        let creation_event_topic = H256::from_str(&config.creation_event_topic.trim_start_matches("0x"))
            .map_err(|e| DexError::Config(format!("Invalid creation event topic: {}", e)))?;
        
        Ok(Self {
            factory_address: config.address,
            protocol_type: config.protocol_type.clone(),
            creation_event_topic,
            blockchain_manager,
        })
    }

    pub async fn scan_all_pairs(
        &self,
        since_block: u64,
    ) -> Result<Vec<Address>, crate::errors::DexError> {
        let filter = ethers::types::Filter::new()
            .address(self.factory_address)
            .topic0(self.creation_event_topic)
            .from_block(since_block);

        let logs = self.blockchain_manager.get_logs(&filter).await
            .map_err(|e| crate::errors::DexError::Other(format!("Failed to get logs: {}", e)))?;

        let mut pairs = Vec::new();
        for log in logs {
            if let Some(pair_address) = log.topics.get(1) {
                if pair_address.as_bytes().len() >= 20 {
                    let address = Address::from_slice(&pair_address.as_bytes()[12..]);
                    pairs.push(address);
                }
            }
        }

        Ok(pairs)
    }
}

//================================================================================================//
//                                CROSS-CHAIN & BRIDGE TYPES                                      //
//================================================================================================//

/// Represents a single-chain swap leg, which can be part of a larger cross-chain execution.
/// This is a flexible structure for the `CrossChainPathfinder`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct CrossChainSwapLeg {
    /// The sequence of token addresses to trade through.
    pub path: Vec<Address>,
    /// The corresponding pools for each step in the path.
    pub pools: Vec<Address>,
    /// The input and output amounts for each step.
    pub amounts: Vec<U256>,
    /// The estimated gas required for the swap.
    pub gas_used: U256,
    /// The expected output amount for the swap, if known.
    pub(crate) expected_output: Option<U256>,
}
/// Used by GasOracleProvider and related modules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeeData {
    /// The base fee per gas (EIP-1559), if available.
    pub base_fee_per_gas: Option<U256>,
    /// The max priority fee per gas (EIP-1559), if available.
    pub max_priority_fee_per_gas: Option<U256>,
    /// The max fee per gas (EIP-1559), if available.
    pub max_fee_per_gas: Option<U256>,
    /// The legacy gas price (pre-EIP-1559), if available.
    pub gas_price: Option<U256>,
    /// The block number this fee data corresponds to.
    pub block_number: u64,
    /// The chain name this fee data is for.
    pub chain_name: String,
}



/// A reference to a single-chain swap, used for cross-chain planning.
pub type SwapRoute = CrossChainSwapLeg;

/// A standardized quote from a bridge protocol, returned by a `BridgeAdapter`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BridgeQuote {
    /// The unique identifier for the chain the transfer is originating from.
    pub source_chain_id: u64,
    /// The unique identifier for the chain the transfer is targeting.
    pub dest_chain_id: u64,
    /// The token being sent from the source chain.
    pub from_token: Address,
    /// The token being received on the destination chain.
    pub to_token: Address,
    /// The amount of `from_token` to be bridged.
    pub amount_in: U256,
    /// The estimated amount of `to_token` that will be received.
    pub amount_out: U256,
    /// The total estimated fees for the bridge transfer, in the source chain's native asset.
    pub fee: U256,
    /// The estimated gas cost for the source-side transaction.
    pub estimated_gas: U256,
    /// Any additional, protocol-specific data needed to execute the bridge transaction.
    pub bridge_specific_data: serde_json::Value,
    /// The name of the bridge protocol providing the quote.
    pub bridge_name: String,
    /// The total fees in USD (if available).
    #[serde(default)]
    pub fee_usd: Option<f64>,
}

/// A complete, executable plan for a cross-chain arbitrage.
/// This plan is constructed by the `CrossChainPathfinder` and consumed by the `ArbitrageEngine`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossChainExecutionPlan {
    /// The optional swap to perform on the source chain before bridging.
    pub source_swap: Option<CrossChainSwapLeg>,
    /// The quote from the bridge provider for the cross-chain transfer.
    pub bridge_quote: BridgeQuote,
    /// The optional swap to perform on the destination chain after bridging.
    pub dest_swap: Option<CrossChainSwapLeg>,
    /// The calculated synergy score for this cross-chain opportunity.
    #[serde(default)]
    pub synergy_score: f64,
    /// The calculated net profit in USD.
    #[serde(default)]
    pub net_profit_usd: f64,
    /// A compact fingerprint for tracking route performance.
    #[serde(default)]
    pub fingerprint: String,
}


#[derive(Debug, Clone)]
pub struct CrossChainRoute {
    pub source_chain: String,
    pub dest_chain: String,
    pub bridges: Vec<BridgeOption>,
    pub estimated_time: std::time::Duration,
    pub total_fee: ethers::types::U256,
    pub reliability_score: f64,
}

#[derive(Debug, Clone)]
pub struct BridgeOption {
    pub protocol: String,
    pub router: ethers::types::Address,
    pub fee: ethers::types::U256,
    pub min_amount: ethers::types::U256,
    pub max_amount: ethers::types::U256,
    pub estimated_time: std::time::Duration,
}

#[deprecated(
    since = "0.2.0",
    note = "Replaced by `CrossChainExecutionPlan`. This was part of a polling-based, unreliable architecture."
)]
#[derive(Debug, Clone)]
pub struct CrossChainExecutionState {
    pub plan: CrossChainExecutionPlan, // Note: This would ideally be the *old* plan struct
    pub status: ExecutionStatus,
    pub start_time: std::time::Instant,
    pub source_tx: Option<ethers::types::H256>,
    pub bridge_tx: Option<ethers::types::H256>,
    pub dest_tx: Option<ethers::types::H256>,
    pub error: Option<String>,
}

//================================================================================================//
//                                CROSS-CHAIN & BRIDGE RESULT TYPES                               //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeInfo {
    pub protocol: String,
    pub source_chain: String,
    pub dest_chain: String,
    pub token: ethers::types::Address,
    pub amount: ethers::types::U256,
    pub fee: ethers::types::U256,
    pub estimated_time: std::time::Duration,
    // Add more fields as needed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSignal {
    pub volatility: f64,
    pub anomaly_score: f64,
    // Add more fields as needed
}

// ===================== ADDED FOR DEX COMPATIBILITY ===================== //

/// Snapshot of a pool's state for analytics/backtesting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSnapshot {
    pub pool_address: Address,
    pub reserve0: U256,
    pub reserve1: U256,
    pub token0: Address,
    pub token1: Address,
}

/// Lightweight rate estimate for a swap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateEstimate {
    pub amount_out: U256,
    pub fee_bps: u32,
    pub price_impact_bps: u32,
    pub gas_estimate: U256,
    pub pool_address: Address,
}

/// Token approval mode for DEX interactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenApprovalMode {
    Unlimited,
    Exact,
    AsNeeded,
    Approve,
    Permit,
    Permit2,
}

impl std::str::FromStr for TokenApprovalMode {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "unlimited" => Ok(TokenApprovalMode::Unlimited),
            "exact" => Ok(TokenApprovalMode::Exact),
            "asneeded" | "as_needed" => Ok(TokenApprovalMode::AsNeeded),
            "approve" => Ok(TokenApprovalMode::Approve),
            "permit" => Ok(TokenApprovalMode::Permit),
            "permit2" => Ok(TokenApprovalMode::Permit2),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DexSettings {
    pub chain_id: u64,
    pub concurrency_limit: usize,
    pub execution_timeout_secs: u64,
    pub path_finding_timeout_ms: u64,
    pub simulate_swaps: bool,
    pub token_approval_mode: String,
    pub approval_gas_limit: u64,
    pub cache_ttl_seconds: u64,
    pub default_slippage_bps: u32,
    pub max_price_impact_bps: u32,
    pub min_liquidity_usd: f64,
    pub priority_dexes: Vec<String>,
    pub swap_gas_limit_base: u64,
    pub swap_gas_limit_per_hop: u64,
}

/// Maps a protocol name string (case-insensitive) to the internal DexProtocol enum.
/// Returns None if the protocol is not recognized.
pub fn protocol_name_to_internal(name: &str) -> Option<DexProtocol> {
    match name.to_lowercase().as_str() {
        "uniswapv2" | "uniswap_v2" | "uni_v2" | "uni2" => Some(DexProtocol::UniswapV2),
        "uniswapv3" | "uniswap_v3" | "uni_v3" | "uni3" => Some(DexProtocol::UniswapV3),
        "uniswapv4" | "uniswap_v4" | "uni_v4" | "uni4" => Some(DexProtocol::UniswapV4),
        "sushiswap" | "sushi" => Some(DexProtocol::SushiSwap),
        "pancakeswap" | "pancake" | "pcs" => Some(DexProtocol::PancakeSwap),
        "curve" => Some(DexProtocol::Curve),
        "balancer" => Some(DexProtocol::Balancer),
        "kyber" | "kyberswap" => Some(DexProtocol::Kyber),
        "dodo" => Some(DexProtocol::Dodo),
        "shibaswap" | "shiba" => Some(DexProtocol::ShibaSwap),
        "camelot" => Some(DexProtocol::Camelot),
        "velodrome" => Some(DexProtocol::Velodrome),
        "solidly" => Some(DexProtocol::Solidly),
        "fraxswap" | "frax" => Some(DexProtocol::FraxSwap),
        "clipper" => Some(DexProtocol::Clipper),
        "maverick" => Some(DexProtocol::Maverick),
        "smardex" => Some(DexProtocol::Smardex),
        "syncswap" => Some(DexProtocol::SyncSwap),
        "ambient" => Some(DexProtocol::Ambient),
        "carbon" => Some(DexProtocol::Carbon),
        "unknown" => Some(DexProtocol::Unknown),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub opportunities_processed: u64,
    pub opportunities_executed: u64,
    pub total_profit_usd: f64,
    pub avg_execution_time_ms: f64,
    pub cache_hit_rate: f64,
    pub circuit_breaker_trips: u64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    // Additional fields for chatbot operator
    pub avg_profit_per_trade: f64,
    pub avg_gas_cost_usd: f64,
    pub success_rate: f64,
    pub successful_trades: u64,
    pub failed_trades: u64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            opportunities_processed: 0,
            opportunities_executed: 0,
            total_profit_usd: 0.0,
            avg_execution_time_ms: 0.0,
            cache_hit_rate: 0.0,
            circuit_breaker_trips: 0,
            last_updated: chrono::Utc::now(),
            avg_profit_per_trade: 0.0,
            avg_gas_cost_usd: 0.0,
            success_rate: 0.0,
            successful_trades: 0,
            failed_trades: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub tx_hash: ethers::types::H256,
    pub status: ExecutionStatus,
    pub profit_usd: f64,
    pub token_in: ethers::types::Address,
    pub token_out: ethers::types::Address,
    pub amount_in: ethers::types::U256,
    pub amount_out: ethers::types::U256,
    pub gas_used: Option<ethers::types::U256>,
    pub gas_price: Option<ethers::types::U256>,
    pub block_number: Option<u64>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub details: Option<serde_json::Value>,
}

/// Enhanced gas price structure with additional fields for different gas pricing models
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GasPrice {
    /// The block's base fee per gas, in Wei.
    pub base_fee: U256,
    /// The estimated priority fee per gas to ensure transaction inclusion, in Wei.
    pub priority_fee: U256,
    /// The timestamp of the block from which the gas price was derived.
    pub timestamp: u64,
    /// Legacy gas price (pre-EIP-1559)
    pub legacy: Option<U256>,
    /// Fast gas price for quick inclusion
    pub fast: U256,
}

impl GasPrice {
    /// Calculate the effective gas price (base_fee + priority_fee)
    pub fn effective_price(&self) -> U256 {
        self.base_fee.saturating_add(self.priority_fee)
    }
}

/// Utility function to convert U256 to f64 with proper decimal handling
pub fn u256_to_f64(amount: U256) -> f64 {
    // Convert to u128 first to avoid overflow, then to f64
    // This is a simplified version - in production you'd want to handle decimals properly
    amount.as_u128() as f64 / 1e18
}

/// Transaction gas pricing models for EVM transactions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxGasPrice {
    /// Legacy gas pricing (pre-EIP-1559)
    Legacy {
        gas_price: U256,
    },
    /// EIP-1559 gas pricing with base fee and priority fee
    Eip1559 {
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    },
}

impl TxGasPrice {
    /// Calculate the effective gas price for fee estimation
    pub fn effective_price(&self) -> U256 {
        match self {
            TxGasPrice::Legacy { gas_price } => *gas_price,
            TxGasPrice::Eip1559 { max_fee_per_gas, .. } => *max_fee_per_gas,
        }
    }

    /// Convert from gas oracle price to transaction gas price
    pub fn from_oracle_price(oracle_price: &crate::gas_oracle::GasPrice) -> Self {
        TxGasPrice::Eip1559 {
            max_fee_per_gas: oracle_price.base_fee.saturating_add(oracle_price.priority_fee),
            max_priority_fee_per_gas: oracle_price.priority_fee,
        }
    }

    /// Convert from legacy gas price
    pub fn legacy(gas_price: U256) -> Self {
        TxGasPrice::Legacy { gas_price }
    }

    /// Convert from EIP-1559 parameters
    pub fn eip1559(max_fee_per_gas: U256, max_priority_fee_per_gas: U256) -> Self {
        TxGasPrice::Eip1559 {
            max_fee_per_gas,
            max_priority_fee_per_gas,
        }
    }
}

/// Enumeration of possible sources for price data, enabling better debugging
/// and analysis of price quality in backtesting scenarios.
#[derive(Debug, Clone, PartialEq)]
pub enum PriceSource {
    /// Price retrieved directly from the database
    Database,
    /// Price derived from WETH-stablecoin swap data
    DerivedFromSwaps,
    /// Price fetched from external APIs (e.g., CoinGecko)
    ExternalApi,
    /// Hardcoded fallback price used when all other sources fail
    FallbackDefault,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceData {
    /// The USD price of the token.
    pub price: f64,
    /// The block number at which this price was observed.
    pub block_number: u64,
    /// The address of the token.
    pub token_address: Address,
    /// The timestamp of the block.
    pub unix_ts: u64,
    /// The source of this price data for debugging and analysis.
    pub source: PriceSource,
}


//================================================================================================//
//                                DECODED TRANSACTION TYPES                                       //
//================================================================================================//

///
/// # Decoded Transaction Types
///
/// This module defines the structured representations of raw blockchain
/// transactions. These types are the output of the `decoder` module and serve
/// as the primary input for the `enricher` and subsequent stages in the
/// predictive pipeline. By creating a clear, well-defined data model, we
/// ensure that each component in the system has a consistent and understandable
/// view of the transactions it processes.
///
/// Represents a decoded swap transaction.
/// This struct contains the core parameters extracted from a swap function call,
/// such as those found in Uniswap-like DEX routers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Swap {
    pub amount_in: U256,
    pub amount_out_min: U256,
    pub path: Vec<Address>,
    pub to: Address,
    pub deadline: U256,
}

/// An enum representing all possible types of decoded transactions.
/// As the system evolves to understand more transaction types (e.g., liquidity
/// additions/removals, liquidations), new variants can be added here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DecodedTx {
    /// A decentralized exchange swap.
    Swap(Swap),
    /// A transaction that is not recognized or supported by the decoder.
    Unknown,
    // Future variants can be added here, for example:
    // AddLiquidity(LiquidityAdd),
    // RemoveLiquidity(LiquidityRemove),
}

//================================================================================================//
//                                PREDICTION & CONTEXT TYPES                                      //
//================================================================================================//

#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub tx: Transaction,
    pub block_timestamp: u64,
    pub gas_price_history: Vec<f64>,
    pub related_pools: Vec<PoolContextInfo>,
    pub token_market_data: HashMap<Address, TokenMarketInfo>,
    pub market_conditions: MarketConditions,
    pub mempool_context: MempoolContext,
    pub route_performance: RoutePerformanceStats,
}

#[derive(Debug, Clone)]
pub struct PoolContextInfo {
    pub pool_info: PoolInfo,
    pub liquidity_usd: f64,
    pub volume_24h_usd: f64,
}

#[derive(Debug, Clone)]
pub struct TokenMarketInfo {
    pub price_usd: f64,
    pub volatility_24h: f64,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub regime: MarketRegime,
    pub overall_volatility: f64,
    pub gas_price_gwei: f64,
}

#[derive(Debug, Clone)]
pub struct MempoolContext {
    pub pending_tx_count: u64,
    pub avg_pending_tx_count: u64,
    pub gas_price_percentiles: Vec<f64>,
    pub congestion_score: f64,
}

#[derive(Debug, Clone)]
pub struct RoutePerformanceStats {
    pub total_attempts: u64,
    pub successful_attempts: u64,
    pub avg_execution_time_ms: f64,
    pub avg_slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MarketRegime {
    HighVolatility,
    LowVolatility,
    Trending,
    Sideways,
    Ranging,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEVPrediction {
    pub prediction_id: H256,
    pub transaction_hash: H256,
    pub predicted_profit_usd: f64,
    pub confidence_score: f64,
    pub risk_score: f64,
    pub mev_type: MEVType,
    pub estimated_success_rate: f64,
    pub features: PredictionFeatures,
    pub competition_level: CompetitionLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionFeatures {
    pub gas_price_volatility: f64,
    pub pool_liquidity_ratio: f64,
    pub token_volatility: f64,
    pub mempool_congestion: f64,
    pub historical_success_rate: f64,
    pub market_regime: MarketRegime,
    pub time_of_day_factor: f64,
    pub weekend_factor: f64,
}

impl Default for PredictionFeatures {
    fn default() -> Self {
        Self {
            gas_price_volatility: 0.0,
            pool_liquidity_ratio: 0.0,
            token_volatility: 0.0,
            mempool_congestion: 0.0,
            historical_success_rate: 0.0,
            market_regime: MarketRegime::Sideways,
            time_of_day_factor: 0.0,
            weekend_factor: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CompetitionLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MEVType {
    NoMEV,
    Sandwich,
    Frontrun,
    Backrun,
    Liquidation,
    JIT,
    StatisticalArbitrage,
}

#[derive(Debug, Clone)]
pub struct ModelPerformance {
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
    pub total_predictions: u64,
    pub successful_predictions: u64,
    pub true_positives: u64,
    pub false_positives: u64,
    pub false_negatives: u64,
    pub cumulative_actual_profit_usd: f64,
    pub cumulative_predicted_profit_usd: f64,
}

impl Default for ModelPerformance {
    fn default() -> Self {
        Self {
            accuracy: 0.0,
            precision: 0.0,
            recall: 0.0,
            f1_score: 0.0,
            total_predictions: 0,
            successful_predictions: 0,
            true_positives: 0,
            false_positives: 0,
            false_negatives: 0,
            cumulative_actual_profit_usd: 0.0,
            cumulative_predicted_profit_usd: 0.0,
        }
    }
}

/// Result of executing a single leg of an arbitrage route
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegExecutionResult {
    pub leg_index: usize,
    pub amount_in: U256,
    pub amount_out: U256,
    pub gas_used: U256,
    pub transaction_hash: H256,
    pub block_number: u64,
}

// Add token_registry field to ChainInfra
pub struct ChainInfra {
    pub chain_name: String,
    pub blockchain_manager: Arc<dyn crate::blockchain::BlockchainManager + Send + Sync>,
    pub pool_manager: Arc<dyn crate::pool_manager::PoolManagerTrait + Send + Sync>,
    pub path_finder: Arc<dyn crate::path::PathFinder + Send + Sync>,
    pub risk_manager: Arc<dyn crate::risk::RiskManager + Send + Sync>,
    pub transaction_optimizer: Arc<dyn crate::transaction_optimizer::TransactionOptimizerTrait + Send + Sync>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    pub pool_state_oracle: Arc<PoolStateOracle>,
    pub token_registry: Arc<dyn crate::token_registry::TokenRegistry + Send + Sync>,
}

// Add cross_chain_pathfinder field to AlphaEngine
pub struct AlphaEngine {
    pub config: Arc<Config>,
    pub settings: crate::config::AlphaEngineSettings,
    pub chain_infras: Arc<RwLock<HashMap<String, ChainInfra>>>,
    pub opportunity_receiver: Arc<Mutex<mpsc::Receiver<ArbitrageOpportunity>>>,
    pub event_broadcaster: broadcast::Sender<crate::types::ChainEvent>,
    pub metrics: Arc<ArbitrageMetrics>,
    pub running: Arc<RwLock<bool>>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    pub statistical_pair_stats: Arc<RwLock<HashMap<(Address, Address), TokenPairStats>>>,
    pub execution_semaphore: Arc<Semaphore>,
    pub active_scanners: Arc<AtomicUsize>,
    pub current_market_regime: Arc<RwLock<MarketRegime>>,
    pub price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    pub gas_oracle: Arc<dyn GasOracleProvider + Send + Sync>,
    pub pool_state_oracle: Arc<PoolStateOracle>,
    pub cross_chain_pathfinder: Option<Arc<crate::cross::CrossChainPathfinder>>,
}

// Add universal arbitrage executor action types and action struct
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UARActionType {
    Swap,
    WrapNative,
    UnwrapNative,
    TransferProfit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UARAction {
    pub action_type: UARActionType,
    pub target: Option<Address>,
    pub token_in: Option<Address>,
    pub amount_in: Option<U256>,
    pub call_data: Option<Bytes>,
}

impl std::fmt::Debug for ArbitrageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArbitrageEngine")
            .field("config", &"Arc<ArbitrageSettings>")
            .field("full_config", &"Arc<Config>")
            .field("price_oracle", &"Arc<dyn PriceOracle + Send + Sync>")
            .field("risk_manager", &"Arc<RwLock<Option<Arc<dyn RiskManager + Send + Sync>>>>")
            .field("blockchain_manager", &"Arc<dyn BlockchainManager + Send + Sync>")
            .field("quote_source", &"Arc<dyn AggregatorQuoteSource + Send + Sync>")
            .field("global_metrics", &"Arc<ArbitrageMetrics>")
            .field("alpha_engine", &"Option<Arc<crate::alpha::engine::AlphaEngine>>")
            .finish()
    }
}