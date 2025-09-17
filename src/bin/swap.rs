// RUST_LOG=swaps=trace cargo run --bin swaps 2>&1 | tee swap.log
//
use ethers::prelude::*;
use ethers::types::{Address, Filter, Log, H256, U256, U512, I256, BlockId, BlockNumber};
use ethers::abi::Abi;
use ethers::contract::Contract;
use ethers::providers::Provider;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::ops::DerefMut;
use web3::futures::{TryFutureExt, SinkExt};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{Result, Context, anyhow};
use tokio_postgres::NoTls;
use tokio::time::sleep;
use tracing::{info, warn, error, debug};
use tracing_subscriber::EnvFilter;
use futures::future::join_all;
use tracing::trace;

use serde::{Serialize, Deserialize};

use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use num_traits::ToPrimitive;
use clap::Parser;
use std::env;
use bytes::Bytes;
use tokio::sync::RwLock;
use std::collections::BTreeMap;

use hex;

// --- Multicall Contract Configuration ---
const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11"; // Default; prefer per-chain config if provided
const MULTICALL3_ABI: &str = r#"[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call3[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]"#;

// --- Event Signature Constants ---
const UNISWAP_V2_SWAP_TOPIC: H256 = H256([
    0xd7, 0x8a, 0xd9, 0x5f, 0xa4, 0x6c, 0x99, 0x4b, 0x65, 0x51, 0xd0, 0xda, 0x85, 0xfc, 0x27, 0x5f,
    0xe6, 0x13, 0xce, 0x37, 0x65, 0x7f, 0xb8, 0xd5, 0xe3, 0xd1, 0x30, 0x84, 0x01, 0x59, 0xd8, 0x22,
]);
const UNISWAP_V3_SWAP_TOPIC: H256 = H256([
    0xc4, 0x20, 0x79, 0xf9, 0x4a, 0x63, 0x50, 0xd7, 0xe6, 0x23, 0x5f, 0x97, 0x17, 0x49, 0x7f, 0x2b,
    0x79, 0xe5, 0x7f, 0x63, 0x65, 0x27, 0xae, 0xd3, 0x36, 0xdb, 0xcd, 0x54, 0x4d, 0xde, 0x3d, 0x9a,
]);
const UNISWAP_V3_MINT_TOPIC: H256 = H256([
    0x7a, 0x53, 0x6f, 0x5b, 0xf6, 0x0a, 0xc3, 0x32, 0x6c, 0x7a, 0xe9, 0xd7, 0xff, 0x52, 0x06, 0x13,
    0x7d, 0xc9, 0x65, 0x60, 0x7d, 0x17, 0xc9, 0x37, 0x8a, 0x84, 0x9a, 0x91, 0x6f, 0xa8, 0x9e, 0x91,
]);
const UNISWAP_V3_BURN_TOPIC: H256 = H256([
    0x0c, 0x39, 0x60, 0x89, 0x7b, 0xd1, 0x0d, 0xca, 0x32, 0x11, 0x6a, 0x2c, 0x00, 0x73, 0x3f, 0x37,
    0x2c, 0x1d, 0xa2, 0x5c, 0x22, 0x60, 0x9f, 0x1c, 0x04, 0x7e, 0x33, 0x7f, 0x8d, 0x7b, 0x1c, 0x3e,
]);
const UNISWAP_V3_POOL_CREATED_TOPIC: H256 = H256([
    0x78, 0x30, 0xc1, 0x58, 0x93, 0x9b, 0x9a, 0x44, 0x84, 0x8d, 0x14, 0x2b, 0x6a, 0x0c, 0x0d, 0x1e,
    0x3c, 0x0a, 0x87, 0x87, 0x5b, 0x6d, 0xa2, 0x7a, 0xa4, 0x5a, 0x1e, 0x0c, 0x71, 0x42, 0x6c, 0x22,
]);
const UNISWAP_V2_POOL_CREATED_TOPIC: H256 = H256([
    0x0d, 0x36, 0x18, 0x31, 0xc7, 0x93, 0xc7, 0xa8, 0x02, 0x05, 0x0b, 0x94, 0x6b, 0xf8, 0xf4, 0x1e,
    0xf6, 0xb1, 0x11, 0x58, 0x20, 0x6c, 0xa3, 0x1a, 0x0e, 0x58, 0x8b, 0x0f, 0x8e, 0x23, 0x1a, 0xf1,
]);
const CURVE_TOKEN_EXCHANGE_TOPIC: H256 = H256([
    0x8b, 0x3e, 0x96, 0xf2, 0xb8, 0x89, 0xfa, 0x0c, 0xbb, 0x5f, 0x38, 0x7d, 0x19, 0x75, 0xc4, 0x09,
    0xbe, 0x25, 0xce, 0xab, 0x06, 0x35, 0xed, 0xa5, 0x07, 0xc2, 0xce, 0x94, 0x7b, 0x9e, 0x92, 0x32,
]);
const BALANCER_V2_SWAP_TOPIC: H256 = H256([
    0x21, 0x70, 0xc7, 0x41, 0x8e, 0xf3, 0x09, 0x09, 0x61, 0x69, 0x76, 0x11, 0x60, 0x29, 0x3d, 0x21,
    0x79, 0x41, 0x9c, 0x65, 0xaa, 0xf6, 0x2c, 0xc8, 0x7c, 0x5c, 0x29, 0x54, 0xfd, 0xff, 0x6e, 0xf4,
]);

// --- Project modules ---
use rust::config::{BacktestConfig, ChainSettings, Config};
use rust::errors::BlockchainError;
use rust::types::{SwapEvent, SwapData, DexProtocol, TransactionData};
use rust::rate_limiter::{GlobalRateLimiterManager, ChainRateLimiter};
use rust::dex_math;
use rust::metrics::{COPY_ROWS_COUNTER, COPY_BYTES_GAUGE};

// --- CLI Definition ---

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = "High-fidelity historical swap data ingestion engine.")]
struct Args {
    /// The name of the chain to ingest swaps for (e.g., "ethereum", "polygon").
    #[arg(long)]
    chain: String,

    /// Enable pool discovery mode instead of swap ingestion
    #[arg(long)]
    discover_pools: bool,

    /// Config directory path (default: "config")
    #[arg(long, default_value = "config")]
    config_dir: String,

    /// Specific protocol to discover pools for (optional filter)
    #[arg(long)]
    protocol: Option<String>,

    /// Start block for pool discovery (optional)
    #[arg(long)]
    from_block: Option<u64>,

    /// End block for pool discovery (optional)
    #[arg(long)]
    to_block: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbiParseConfig {
    pub dexes: HashMap<String, AbiDexConfig>,
    pub flash_loan_providers: HashMap<String, FlashLoanProvider>,
    pub tokens: HashMap<String, TokenConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbiDexConfig {
    pub abi: String,
    pub protocol: String,
    pub version: String,
    pub event_signatures: HashMap<String, String>,
    pub topic0: HashMap<String, String>,
    pub swap_event_field_map: HashMap<String, String>,
    pub swap_signatures: HashMap<String, String>,
    pub factory_addresses: Option<HashMap<String, String>>,
    pub vault_address: Option<HashMap<String, String>>,
    pub init_code_hash: Option<String>,
    pub default_fee_bps: Option<u32>,
    pub supported_fee_tiers: Option<Vec<u32>>,
    pub quoter_address: Option<String>,
    pub quoter_address_v1: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlashLoanProvider {
    pub abi: String,
    pub protocol: String,
    pub version: String,
    pub flash_loan_fee_bps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenConfig {
    pub decimals: u8,
    pub symbol: String,
    pub name: String,
}

// --- Configuration Structs ---
// Dynamic contract types
type DynamicContract = Contract<Provider<ethers::providers::Http>>;

// Protocol type mapping for database storage
fn get_protocol_type_id(protocol: &DexProtocol) -> i16 {
    match protocol {
        DexProtocol::UniswapV2 => 1,
        DexProtocol::UniswapV3 => 2,  // Correct: V3 is protocol_type 2
        DexProtocol::Curve => 3,       // Correct: Curve is protocol_type 3
        DexProtocol::Balancer => 4,    // Correct: Balancer is protocol_type 4
        DexProtocol::SushiSwap => 5,
        DexProtocol::SushiSwapV2 => 5, // Same as SushiSwap
        DexProtocol::PancakeSwap => 6, // PancakeSwap V2 is protocol_type 6
        DexProtocol::PancakeSwapV2 => 6, // Same as PancakeSwap
        DexProtocol::PancakeSwapV3 => 7, // PancakeSwap V3 is protocol_type 7
        DexProtocol::UniswapV4 => 8,
        DexProtocol::Bancor => 9,
        DexProtocol::TraderJoe => 10,
        DexProtocol::Kyber => 11,
        DexProtocol::Dodo => 12,
        DexProtocol::ShibaSwap => 13,
        DexProtocol::Camelot => 14,
        DexProtocol::Velodrome => 15,
        DexProtocol::Solidly => 16,
        DexProtocol::FraxSwap => 17,
        DexProtocol::Clipper => 18,
        DexProtocol::Maverick => 19,
        DexProtocol::Smardex => 20,
        DexProtocol::SyncSwap => 21,
        DexProtocol::Ambient => 22,
        DexProtocol::Carbon => 23,
        DexProtocol::Unknown => 0,
        DexProtocol::Other(id) => *id as i16,
    }
}

// --- Contract ABI Management ---
#[derive(Clone)]
pub struct ContractABIs {
    pub uniswap_v2_pair_abi: Abi,
    pub uniswap_v3_pool_abi: Abi,
    pub curve_pool_abi: Abi,
    pub balancer_pool_abi: Abi,
    pub erc20_abi: Abi,
    pub uniswap_v2_factory_abi: Abi,
    pub uniswap_v3_factory_abi: Abi,
}

impl ContractABIs {
    pub fn from_config(config: &AbiParseConfig) -> Result<Self> {
        let uniswap_v2_pair_abi = {
            let dex_config = config.dexes.get("UniswapV2")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'UniswapV2' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'UniswapV2'")?
        };

        let uniswap_v3_pool_abi = {
            let dex_config = config.dexes.get("UniswapV3")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'UniswapV3' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'UniswapV3'")?
        };

        let curve_pool_abi = {
            let dex_config = config.dexes.get("Curve")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'Curve' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'Curve'")?
        };

        let balancer_pool_abi = {
            let dex_config = config.dexes.get("Balancer")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'Balancer' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'Balancer'")?
        };

        let erc20_abi = {
            let dex_config = config.dexes.get("ERC20")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'ERC20' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'ERC20'")?
        };

        let uniswap_v2_factory_abi = {
            let dex_config = config.dexes.get("UniswapV2")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'UniswapV2' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'UniswapV2' as factory ABI")?
        };

        let uniswap_v3_factory_abi = {
            let dex_config = config.dexes.get("UniswapV3")
                .ok_or_else(|| anyhow::anyhow!("ABI config for 'UniswapV3' not found"))?;
            let abi_str = &dex_config.abi;
            serde_json::from_str::<Abi>(abi_str)
                .context("Failed to parse ABI for 'UniswapV3' as factory ABI")?
        };

        Ok(Self {
            uniswap_v2_pair_abi,
            uniswap_v3_pool_abi,
            curve_pool_abi,
            balancer_pool_abi,
            erc20_abi,
            uniswap_v2_factory_abi,
            uniswap_v3_factory_abi,
        })
    }
    
    pub fn create_v2_pair_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.uniswap_v2_pair_abi.clone(), provider)
    }
    
    pub fn create_v3_pool_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.uniswap_v3_pool_abi.clone(), provider)
    }
    
    pub fn create_curve_pool_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.curve_pool_abi.clone(), provider)
    }
    
    pub fn create_balancer_pool_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.balancer_pool_abi.clone(), provider)
    }
    
    pub fn create_erc20_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.erc20_abi.clone(), provider)
    }
    
    pub fn create_v2_factory_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.uniswap_v2_factory_abi.clone(), provider)
    }
    
    pub fn create_v3_factory_contract(&self, address: Address, provider: Arc<Provider<ethers::providers::Http>>) -> DynamicContract {
        Contract::new(address, self.uniswap_v3_factory_abi.clone(), provider)
    }
}

// --- Helper Functions for V3 Event Decoding ---
fn h256_topic_addr(t: &H256) -> Address {
    Address::from_slice(&t.as_bytes()[12..32]) // last 20 bytes
}

// --- Database Formatting Helpers ---
fn format_bytea<A: AsRef<[u8]>>(bytes: A) -> String {
    let b = bytes.as_ref();
    if b.is_empty() { "\\N".into() } else { format!("\\x{}", hex::encode(b)) }
}

fn format_h256_bytea(h: &H256) -> String {
    format!("\\x{}", hex::encode(h.as_bytes()))
}

fn format_optional_h256(v: Option<H256>) -> String {
    match v {
        Some(x) => format!("\\x{}", hex::encode(x.as_bytes())),
        None => "\\N".to_string(),
    }
}

fn topic_i24_as_i32(t: &H256) -> i32 {
    // ticks are int24, but topics are 32 bytes; sign-extend properly
    let v = I256::from_raw(U256::from_big_endian(t.as_bytes())).as_i32();
    v.clamp(-8_388_608, 8_388_607) // [-2^23, 2^23-1]
}

// --- Mathematical Constants and Utilities ---
fn q96() -> U256 {
    U256::from(2).pow(U256::from(96))
}

/// Calculate V3 price from sqrt_price_x96 with decimal scaling
/// price_t0_in_t1 = (sqrt_price_x96² / 2^192) * 10^(dec1 - dec0)
fn price_v3_q96(sqrt_price_x96: U256, dec0: u8, dec1: u8) -> Option<Decimal> {
    let q192 = U256::from(2).pow(U256::from(192));
    let num = dex_math::mul_div(sqrt_price_x96, sqrt_price_x96, U256::one()).ok()?; // sqrt^2
    let raw = dex_math::mul_div(num, U256::exp10(18), q192).ok()?; // scale to 1e18
    let scale_pow = (dec1 as i32) - (dec0 as i32);
    let scale = if scale_pow >= 0 {
        U256::exp10(scale_pow as usize)
    } else {
        // divide later
        U256::exp10((-scale_pow) as usize)
    };
    let scaled = if scale_pow >= 0 {
        dex_math::mul_div(raw, scale, U256::one()).ok()?
    } else {
        dex_math::mul_div(raw, U256::one(), scale).ok()?
    };
    Decimal::from_str(&scaled.to_string()).ok()
        .and_then(|d| Decimal::from_u128(1_000_000_000_000_000_000u128).map(|e| d / e))
}

fn calculate_v3_reserves_from_liquidity_and_sqrt_price(
    liquidity: u128,
    sqrt_price_x96: U256,
) -> Result<(U256, U256)> {
    if liquidity == 0 || sqrt_price_x96.is_zero() {
        return Ok((U256::zero(), U256::zero()));
    }

    let q96_val = q96();
    let liquidity_u256 = U256::from(liquidity);

    // Calculate reserve1 (token1)
    // Formula: y = (L * sqrtP) / Q96
    let product1 = U512::from(liquidity_u256).checked_mul(U512::from(sqrt_price_x96))
        .ok_or_else(|| anyhow::anyhow!(
            "Overflow calculating reserve1: liquidity={}, sqrt_price={}", 
            liquidity_u256, sqrt_price_x96
        ))?;
    let reserve1_u512 = product1.checked_div(U512::from(q96_val))
        .ok_or_else(|| anyhow::anyhow!("Division by zero in reserve1 calculation"))?;

    // Calculate reserve0 (token0)
    // Formula: x = (L * Q96) / sqrtP
    let product0 = U512::from(liquidity_u256).checked_mul(U512::from(q96_val))
        .ok_or_else(|| anyhow::anyhow!(
            "Overflow calculating reserve0: liquidity={}, q96={}", 
            liquidity_u256, q96_val
        ))?;
    let reserve0_u512 = product0.checked_div(U512::from(sqrt_price_x96))
        .ok_or_else(|| anyhow::anyhow!(
            "Division by zero in reserve0 calculation: sqrt_price={}", 
            sqrt_price_x96
        ))?;

    // Convert back to U256, handling potential overflow
    let reserve0: U256 = reserve0_u512.try_into()
        .map_err(|_| anyhow::anyhow!("reserve0 calculation overflowed U256"))?;
    let reserve1: U256 = reserve1_u512.try_into()
        .map_err(|_| anyhow::anyhow!("reserve1 calculation overflowed U256"))?;

    Ok((reserve0, reserve1))
}

fn decode_int256_to_i256(bytes: &[u8]) -> Result<I256> {
    if bytes.len() != 32 {
        return Err(anyhow::anyhow!("Invalid bytes length for I256: {}", bytes.len()));
    }
    
    // Use I256::from_raw to handle two's complement representation correctly
    // This handles both positive and negative values properly
    let u256_value = U256::from_big_endian(bytes);
    let i256_value = I256::from_raw(u256_value);
    
    Ok(i256_value)
}

// --- Enriched Swap Event Structure ---
#[derive(Debug, Clone)]
pub struct SwapDbRow {
    // --- Core schema: must match trades.rs ---
    pub chain_name: String,
    pub pool_id: String,
    pub pool_address: String,
    pub block_number: Option<i64>,
    pub tx_hash: String,
    pub tx_from_address: Option<String>,
    pub from_token_amount: Option<Decimal>,
    pub to_token_amount: Option<Decimal>,
    pub price_from_in_currency_token: Option<Decimal>,
    pub price_to_in_currency_token: Option<Decimal>,
    pub price_from_in_usd: Option<Decimal>,
    pub price_to_in_usd: Option<Decimal>,
    pub block_timestamp: Option<i64>,
    pub kind: Option<String>,
    pub volume_in_usd: Option<Decimal>,
    pub from_token_address: Option<String>,
    pub to_token_address: Option<String>,
    pub first_seen: i64,
    // --- Enrichment fields for backtesting (nullable, after core columns) ---
    pub gas_used: Option<Decimal>,
    pub gas_price: Option<Decimal>,
    pub gas_fee: Option<Decimal>,
    pub liquidity: Option<Decimal>,
    pub pool_liquidity_usd: Option<Decimal>,
    pub tick: Option<Decimal>,
    pub sqrt_price_x96: Option<Decimal>,
    pub v3_amount0: Option<Decimal>,
    pub v3_amount1: Option<Decimal>,
    pub v3_sender: Option<String>,
    pub v3_recipient: Option<String>,
    pub buyer: Option<String>,
    pub sold_id: Option<Decimal>,
    pub tokens_sold: Option<Decimal>,
    pub bought_id: Option<Decimal>,
    pub tokens_bought: Option<Decimal>,
    pub pool_id_balancer: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub amount_in: Option<Decimal>,
    pub amount_out: Option<Decimal>,
    pub token0_reserve: Option<Decimal>,
    pub token1_reserve: Option<Decimal>,
    pub tx_to: Option<String>,
    pub tx_gas: Option<Decimal>,
    pub tx_gas_price: Option<Decimal>,
    pub tx_gas_used: Option<Decimal>,
    pub tx_value: Option<Decimal>,
    pub tx_block_number: Option<i64>,
    pub tx_max_fee_per_gas: Option<Decimal>,
    pub tx_max_priority_fee_per_gas: Option<Decimal>,
    pub tx_transaction_type: Option<i32>,
    pub tx_chain_id: Option<i64>,
    pub tx_cumulative_gas_used: Option<Decimal>,

    // --- V3 Tick Data (JSONB for flexible storage) ---
    pub v3_tick_data: Option<String>, // JSON serialized V3TickData
}

pub struct EnrichedSwapEvent {
    pub base: SwapEvent,
    pub fee_tier: Option<u32>,
    pub token0_decimals: Option<u8>,
    pub token1_decimals: Option<u8>,
    pub gas_used: Option<U256>,
    pub v3_tick_data: Option<V3TickData>,
}

impl EnrichedSwapEvent {
    pub fn from_base(base: SwapEvent) -> Self {
        Self {
            base,
            fee_tier: None,
            token0_decimals: None,
            token1_decimals: None,
            gas_used: None,
            v3_tick_data: None,
        }
    }
    
    pub fn is_enriched(&self) -> bool {
        self.fee_tier.is_some() || 
        self.token0_decimals.is_some() || 
        self.token1_decimals.is_some() || 
        self.gas_used.is_some()
    }
    
    pub fn to_db_row(&self) -> SwapDbRow {
        let base = &self.base;
        // --- Map all fields to match the core schema ---
        let (from_token_amount, to_token_amount, from_token_address, to_token_address, price_from_in_currency_token, price_to_in_currency_token, price_from_in_usd, price_to_in_usd) = match &base.data {
            SwapData::UniswapV2 { amount0_in, amount1_in, amount0_out, amount1_out, .. } => {
                // Use directionality to determine which token is "from" and "to"
                // For simplicity, assume amount0_in > 0 means token0 is from, token1 is to
                if *amount0_in > U256::zero() {
                    (
                        Some(Decimal::from_str(&amount0_in.to_string()).unwrap_or_default()),
                        Some(Decimal::from_str(&amount1_out.to_string()).unwrap_or_default()),
                        base.token0.map(|a| format!("0x{:x}", a)), // Use token from swap event
                        base.token1.map(|a| format!("0x{:x}", a)), // Use token from swap event
                        base.price_t0_in_t1,
                        base.price_t1_in_t0,
                        None, // No USD price available without pool data
                        None, // No USD price available without pool data
                    )
                } else {
                    (
                        Some(Decimal::from_str(&amount1_in.to_string()).unwrap_or_default()),
                        Some(Decimal::from_str(&amount0_out.to_string()).unwrap_or_default()),
                        base.token1.map(|a| format!("0x{:x}", a)), // Use token from swap event
                        base.token0.map(|a| format!("0x{:x}", a)), // Use token from swap event
                        base.price_t1_in_t0,
                        base.price_t0_in_t1,
                        None, // No USD price available without pool data
                        None, // No USD price available without pool data
                    )
                }
            }
            SwapData::UniswapV3 { amount0, amount1, .. } => {
                if amount0.is_negative() && amount1.is_positive() {
                    // token1 in, token0 out
                    (
                        Some(Decimal::from_str(&amount1.as_u128().to_string()).unwrap_or_default()), // from = token1
                        Some(Decimal::from_str(&amount0.unsigned_abs().as_u128().to_string()).unwrap_or_default()), // to = token0
                        base.token1.map(|a| format!("0x{:x}", a)),
                        base.token0.map(|a| format!("0x{:x}", a)),
                        base.price_t1_in_t0,
                        base.price_t0_in_t1,
                        None, None
                    )
                } else if amount1.is_negative() && amount0.is_positive() {
                    // token0 in, token1 out
                    (
                        Some(Decimal::from_str(&amount0.as_u128().to_string()).unwrap_or_default()), // from = token0
                        Some(Decimal::from_str(&amount1.unsigned_abs().as_u128().to_string()).unwrap_or_default()), // to = token1
                        base.token0.map(|a| format!("0x{:x}", a)),
                        base.token1.map(|a| format!("0x{:x}", a)),
                        base.price_t0_in_t1,
                        base.price_t1_in_t0,
                        None, None
                    )
                } else {
                    // degenerate; leave None or try to infer
                    (None, None, None, None, None, None, None, None)
                }
            }
            SwapData::Curve { tokens_sold, tokens_bought, sold_id, .. } => {
                // For Curve, we use token addresses from swap data when available
                let token0_addr = base.token0.map(|a| format!("0x{:x}", a));
                let token1_addr = base.token1.map(|a| format!("0x{:x}", a));

                // Assume sold_id 0 = token0, sold_id 1 = token1
                let (from_addr, to_addr) = if *sold_id == 0 {
                    (token0_addr.clone(), token1_addr.clone())
                } else {
                    (token1_addr.clone(), token0_addr.clone())
                };

                (
                    Some(Decimal::from_str(&tokens_sold.to_string()).unwrap_or_default()),
                    Some(Decimal::from_str(&tokens_bought.to_string()).unwrap_or_default()),
                    from_addr,
                    to_addr,
                    None, None, // No currency token prices available
                    None, None // No USD prices available without pool data
                )
            },
            SwapData::Balancer { amount_in, amount_out, token_in, token_out, .. } => {
                let from_addr = Some(format!("0x{:x}", token_in));
                let to_addr = Some(format!("0x{:x}", token_out));

                (
                    Some(Decimal::from_str(&amount_in.to_string()).unwrap_or_default()),
                    Some(Decimal::from_str(&amount_out.to_string()).unwrap_or_default()),
                    from_addr,
                    to_addr,
                    None, None, // No currency token prices available
                    None, None // No USD prices available without pool data
                )
            },
        };
        SwapDbRow {
            chain_name: base.chain_name.clone(),
            // Use pool_address hex text as the identifier instead of cg pool_id
            pool_id: format!("0x{:x}", base.pool_address),
            pool_address: format!("0x{:x}", base.pool_address),
            block_number: Some(base.block_number as i64),
            tx_hash: format!("0x{:x}", base.tx_hash),
            tx_from_address: base.transaction.as_ref().and_then(|t| t.from.map(|a| format!("0x{:x}", a))),
            from_token_amount,
            to_token_amount,
            price_from_in_currency_token,
            price_to_in_currency_token,
            price_from_in_usd,
            price_to_in_usd,
            block_timestamp: Some(base.unix_ts as i64),
            kind: None, // SwapEvent doesn't have a kind field
            // Calculate actual swap volume from amounts and prices
            volume_in_usd: {
                // Calculate volume based on token amounts and USD prices
                let mut volume = None;
                
                // Determine correct decimals based on which token is from/to
                let (from_decimals, to_decimals) = match from_token_address {
                    Some(ref addr) => {
                        // If from_token_address matches token0, use token0_decimals for from, token1_decimals for to
                        if addr == &base.token0.map(|a| format!("0x{:x}", a)).unwrap_or_default() {
                            (self.token0_decimals, self.token1_decimals)
                        } else {
                            (self.token1_decimals, self.token0_decimals)
                        }
                    },
                    None => (self.token0_decimals, self.token1_decimals) // fallback
                };

                if let (Some(from_amount), Some(from_price_usd)) = (from_token_amount, price_from_in_usd) {
                    // Adjust for token decimals if available
                    let decimals_adjustment = if let Some(decimals) = from_decimals {
                        Decimal::from(10_u64.pow(decimals as u32))
                    } else {
                        Decimal::from(10_u64.pow(18)) // Default to 18 decimals
                    };

                    // volume = amount / decimals_adjustment * price
                    if let Some(adjusted_amount) = from_amount.checked_div(decimals_adjustment) {
                        if let Some(final_volume) = adjusted_amount.checked_mul(from_price_usd) {
                            // Sanity check: volume should be reasonable (< $1 billion per swap)
                            if final_volume < Decimal::from(1_000_000_000_u64) && final_volume > Decimal::ZERO {
                                volume = Some(final_volume);
                            }
                        }
                    }
                } else if let (Some(to_amount), Some(to_price_usd)) = (to_token_amount, price_to_in_usd) {
                    // Fallback to output token for volume calculation
                    let decimals_adjustment = if let Some(decimals) = to_decimals {
                        Decimal::from(10_u64.pow(decimals as u32))
                    } else {
                        Decimal::from(10_u64.pow(18))
                    };

                    if let Some(adjusted_amount) = to_amount.checked_div(decimals_adjustment) {
                        if let Some(final_volume) = adjusted_amount.checked_mul(to_price_usd) {
                            // Sanity check
                            if final_volume < Decimal::from(1_000_000_000_u64) && final_volume > Decimal::ZERO {
                                volume = Some(final_volume);
                            }
                        }
                    }
                }
                
                volume
            },
            from_token_address,
            to_token_address,
            first_seen: base.unix_ts as i64,
            // --- Enrichment fields ---
            gas_used: self.gas_used.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default()),
            gas_price: base.transaction.as_ref().and_then(|t| t.gas_price.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default())),
            gas_fee: None, // Compute if needed
            liquidity: base.liquidity.and_then(|l| Decimal::from_str(&l.to_string()).ok()), // Use available liquidity data
            pool_liquidity_usd: None, // Would need price data to calculate USD value
            tick: base.tick.map(|t| Decimal::from_str(&t.to_string()).unwrap_or_default()),
            sqrt_price_x96: base.sqrt_price_x96.map(|p| Decimal::from_str(&p.to_string()).unwrap_or_default()),
            v3_amount0: base.v3_amount0.map(|a| Decimal::from_str(&a.to_string()).unwrap_or_default()),
            v3_amount1: base.v3_amount1.map(|a| Decimal::from_str(&a.to_string()).unwrap_or_default()),
            v3_sender: match &base.data { SwapData::UniswapV3 { sender, .. } => Some(format!("0x{:x}", sender)), _ => None },
            v3_recipient: match &base.data { SwapData::UniswapV3 { recipient, .. } => Some(format!("0x{:x}", recipient)), _ => None },
            buyer: match &base.data { SwapData::Curve { buyer, .. } => Some(format!("0x{:x}", buyer)), _ => None },
            sold_id: match &base.data { SwapData::Curve { sold_id, .. } => Some(Decimal::from_str(&sold_id.to_string()).unwrap_or_default()), _ => None },
            tokens_sold: match &base.data { SwapData::Curve { tokens_sold, .. } => Some(Decimal::from_str(&tokens_sold.to_string()).unwrap_or_default()), _ => None },
            bought_id: match &base.data { SwapData::Curve { bought_id, .. } => Some(Decimal::from_str(&bought_id.to_string()).unwrap_or_default()), _ => None },
            tokens_bought: match &base.data { SwapData::Curve { tokens_bought, .. } => Some(Decimal::from_str(&tokens_bought.to_string()).unwrap_or_default()), _ => None },
            pool_id_balancer: match &base.data { SwapData::Balancer { pool_id, .. } => Some(format!("0x{:x}", pool_id)), _ => None },
            token_in: Some(format!("0x{:x}", base.token_in)),
            token_out: Some(format!("0x{:x}", base.token_out)),
            amount_in: Some(Decimal::from_str(&base.amount_in.to_string()).unwrap_or_default()),
            amount_out: Some(Decimal::from_str(&base.amount_out.to_string()).unwrap_or_default()),
            token0_reserve: base.reserve0.map(|r| Decimal::from_str(&r.to_string()).unwrap_or_default()),
            token1_reserve: base.reserve1.map(|r| Decimal::from_str(&r.to_string()).unwrap_or_default()),
            tx_to: base.transaction.as_ref().and_then(|t| t.to.map(|a| format!("0x{:x}", a))),
            tx_gas: base.transaction.as_ref().and_then(|t| t.gas.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default()))
                .or_else(|| self.gas_used.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default())),
            tx_gas_price: base.transaction.as_ref().and_then(|t| t.gas_price.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default()))
                .or_else(|| base.effective_gas_price.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default())),
            tx_gas_used: self.gas_used.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default())
                .or_else(|| base.transaction.as_ref().and_then(|t| t.gas_used.map(|g| Decimal::from_str(&g.to_string()).unwrap_or_default()))),
            tx_value: base.transaction.as_ref().and_then(|t| t.value.map(|v| Decimal::from_str(&v.to_string()).unwrap_or_default())),
            tx_block_number: base.transaction.as_ref().and_then(|t| t.block_number.map(|b| b.as_u64() as i64)),
            tx_max_fee_per_gas: base.transaction.as_ref().and_then(|t| t.max_fee_per_gas.map(|f| Decimal::from_str(&f.to_string()).unwrap_or_default())),
            tx_max_priority_fee_per_gas: base.transaction.as_ref().and_then(|t| t.max_priority_fee_per_gas.map(|f| Decimal::from_str(&f.to_string()).unwrap_or_default())),
            tx_transaction_type: base.transaction.as_ref().and_then(|t| t.transaction_type.map(|tt| tt.as_u64() as i32)),
            tx_chain_id: base.transaction.as_ref().and_then(|t| t.chain_id.map(|c| c.as_u64() as i64)),
            tx_cumulative_gas_used: base.transaction.as_ref().and_then(|t| t.cumulative_gas_used.map(|c| Decimal::from_str(&c.to_string()).unwrap_or_default())),

            // --- V3 Tick Data ---
            v3_tick_data: self.v3_tick_data.as_ref().and_then(|tick_data| {
                serde_json::to_string(tick_data).ok()
            }),
        }
    }
}

// --- V3 Tick Data Structures ---

/// Represents a single tick position in a Uniswap V3 pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickPosition {
    pub tick: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}

/// Complete tick data for a V3 pool at a specific block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3TickData {
    pub pool_address: Address,
    pub block_number: u64,
    pub initialized_ticks: BTreeMap<i32, TickPosition>,
    pub tick_spacing: i32,
    pub max_liquidity_per_tick: u128,
}

/// V3 Tick Data Fetcher - reconstructs tick bitmap from historical events
#[derive(Clone)]
pub struct V3TickDataFetcher {
    provider: Arc<Provider<Http>>,
    rate_limiter: Arc<ChainRateLimiter>,
    contract_abis: ContractABIs,
}

impl V3TickDataFetcher {
    pub fn new(
        provider: Arc<Provider<Http>>,
        rate_limiter: Arc<ChainRateLimiter>,
        contract_abis: ContractABIs,
    ) -> Self {
        Self {
            provider,
            rate_limiter,
            contract_abis,
        }
    }

    /// Fetch complete tick data for a V3 pool at a specific block
    /// This reconstructs the tick bitmap by replaying all Mint/Burn events from pool creation
    pub async fn fetch_tick_data_at_block(
        &self,
        pool_address: &Address,
        target_block: u64,
    ) -> Result<V3TickData> {
        info!("Fetching V3 tick data for pool {} at block {}", pool_address, target_block);

        // First, get pool creation block and tick spacing
        let (creation_block, tick_spacing) = self.get_pool_creation_info(pool_address).await?;

        if target_block < creation_block {
            return Err(anyhow::anyhow!(
                "Target block {} is before pool creation block {}",
                target_block,
                creation_block
            ));
        }

        // Get all Mint and Burn events from creation to target block
        let mint_events = self.fetch_mint_events(pool_address, creation_block, target_block).await?;
        let burn_events = self.fetch_burn_events(pool_address, creation_block, target_block).await?;

        // Reconstruct tick data by replaying events
        let mut initialized_ticks = BTreeMap::new();

        // Process Mint events
        for event in mint_events {
            let tick_lower = event.tick_lower;
            let tick_upper = event.tick_upper;
            let amount = event.amount;

            // Update tick positions
            initialized_ticks
                .entry(tick_lower)
                .or_insert_with(|| TickPosition {
                    tick: tick_lower,
                    liquidity_net: 0,
                    liquidity_gross: 0,
                })
                .liquidity_net += amount as i128;

            initialized_ticks
                .entry(tick_upper)
                .or_insert_with(|| TickPosition {
                    tick: tick_upper,
                    liquidity_net: 0,
                    liquidity_gross: 0,
                })
                .liquidity_net -= amount as i128;

            // Update gross liquidity
            if let Some(tick) = initialized_ticks.get_mut(&tick_lower) {
                tick.liquidity_gross += amount;
            }
            if let Some(tick) = initialized_ticks.get_mut(&tick_upper) {
                tick.liquidity_gross += amount;
            }
        }

        // Process Burn events (reverse of Mint)
        for event in burn_events {
            let tick_lower = event.tick_lower;
            let tick_upper = event.tick_upper;
            let amount = event.amount;

            // Update tick positions
            initialized_ticks
                .entry(tick_lower)
                .or_insert_with(|| TickPosition {
                    tick: tick_lower,
                    liquidity_net: 0,
                    liquidity_gross: 0,
                })
                .liquidity_net -= amount as i128;

            initialized_ticks
                .entry(tick_upper)
                .or_insert_with(|| TickPosition {
                    tick: tick_upper,
                    liquidity_net: 0,
                    liquidity_gross: 0,
                })
                .liquidity_net += amount as i128;

            // Update gross liquidity
            if let Some(tick) = initialized_ticks.get_mut(&tick_lower) {
                tick.liquidity_gross -= amount;
            }
            if let Some(tick) = initialized_ticks.get_mut(&tick_upper) {
                tick.liquidity_gross -= amount;
            }
        }

        // Remove ticks with zero gross liquidity
        initialized_ticks.retain(|_, tick| tick.liquidity_gross > 0);

        // Get max liquidity per tick from the pool
        let max_liquidity_per_tick = self.get_max_liquidity_per_tick(pool_address, target_block).await?;

        let tick_data = V3TickData {
            pool_address: *pool_address,
            block_number: target_block,
            initialized_ticks,
            tick_spacing,
            max_liquidity_per_tick,
        };

        info!(
            "Fetched tick data for pool {}: {} initialized ticks",
            pool_address,
            tick_data.initialized_ticks.len()
        );

        Ok(tick_data)
    }

    /// Get pool creation information
    async fn get_pool_creation_info(&self, pool_address: &Address) -> Result<(u64, i32)> {
        let contract = self.contract_abis.create_v3_pool_contract(*pool_address, self.provider.clone());
        let tick_spacing = self.rate_limiter.execute_rpc_call("get_tick_spacing", || {
            let c = contract.clone();
            async move {
                match c.method::<(), i32>("tickSpacing", ()) {
                    Ok(method) => method.call().await.map_err(|e| BlockchainError::Provider(e.to_string())),
                    Err(e) => Err(BlockchainError::Provider(format!("Method creation failed: {}", e))),
                }
            }
        }).await?;

        let latest = self.provider.get_block_number().await?.as_u64();
        let (mut lo, mut hi) = (0u64, latest);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let code = self.provider.get_code(*pool_address, Some(mid.into())).await?;
            if code.as_ref().is_empty() { lo = mid + 1; } else { hi = mid; }
        }
        Ok((lo, tick_spacing))
    }

    /// Fetch Mint events for a pool
    async fn fetch_mint_events(
        &self,
        pool_address: &Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<MintEvent>> {
        let filter = Filter::new()
            .address(*pool_address)
            .topic0(UNISWAP_V3_MINT_TOPIC)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.rate_limiter.execute_rpc_call("get_mint_logs", || {
            self.provider.get_logs(&filter).map_err(|e| BlockchainError::Provider(format!("Failed to get mint logs: {}", e)))
        }).await?;

        let mut mint_events = Vec::new();
        for log in logs {
            if let Ok(event) = self.decode_mint_event(&log) {
                mint_events.push(event);
            }
        }

        Ok(mint_events)
    }

    /// Fetch Burn events for a pool
    async fn fetch_burn_events(
        &self,
        pool_address: &Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<BurnEvent>> {
        let filter = Filter::new()
            .address(*pool_address)
            .topic0(UNISWAP_V3_BURN_TOPIC)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.rate_limiter.execute_rpc_call("get_burn_logs", || {
            self.provider.get_logs(&filter).map_err(|e| BlockchainError::Provider(format!("Failed to get burn logs: {}", e)))
        }).await?;

        let mut burn_events = Vec::new();
        for log in logs {
            if let Ok(event) = self.decode_burn_event(&log) {
                burn_events.push(event);
            }
        }

        Ok(burn_events)
    }

    /// Decode Mint event
    fn decode_mint_event(&self, log: &Log) -> Result<MintEvent> {
        if log.topics.len() < 4 || log.data.len() < 128 {
            return Err(anyhow::anyhow!("Invalid Mint event"));
        }
        // topics[1]=owner, [2]=tickLower, [3]=tickUpper
        let owner      = h256_topic_addr(&log.topics[1]);
        let tick_lower = topic_i24_as_i32(&log.topics[2]);
        let tick_upper = topic_i24_as_i32(&log.topics[3]);

        // data: sender(32), amount(32), amount0(32), amount1(32)
        let sender  = Address::from_slice(&log.data[12..32]);
        let amount  = U256::from_big_endian(&log.data[32..64]).as_u128();
        let amount0 = U256::from_big_endian(&log.data[64..96]);
        let amount1 = U256::from_big_endian(&log.data[96..128]);

        Ok(MintEvent { sender, owner, tick_lower, tick_upper, amount, amount0, amount1 })
    }

    /// Decode Burn event
    fn decode_burn_event(&self, log: &Log) -> Result<BurnEvent> {
        if log.topics.len() < 4 || log.data.len() < 96 {
            return Err(anyhow::anyhow!("Invalid Burn event"));
        }
        let owner      = h256_topic_addr(&log.topics[1]);
        let tick_lower = topic_i24_as_i32(&log.topics[2]);
        let tick_upper = topic_i24_as_i32(&log.topics[3]);

        // data: amount(32), amount0(32), amount1(32)
        let amount  = U256::from_big_endian(&log.data[0..32]).as_u128();
        let amount0 = U256::from_big_endian(&log.data[32..64]);
        let amount1 = U256::from_big_endian(&log.data[64..96]);

        Ok(BurnEvent { owner, tick_lower, tick_upper, amount, amount0, amount1 })
    }

    /// Get max liquidity per tick from pool
    async fn get_max_liquidity_per_tick(&self, pool_address: &Address, block_number: u64) -> Result<u128> {
        let contract = self.contract_abis.create_v3_pool_contract(*pool_address, self.provider.clone());
        let at = BlockId::Number(BlockNumber::Number(block_number.into()));

        self.rate_limiter.execute_rpc_call("get_max_liquidity_per_tick", || {
            let contract_clone = contract.clone();
            async move {
                match contract_clone.method::<(), u128>("maxLiquidityPerTick", ()) {
                    Ok(method) => method.block(at).call().await.map_err(|e| BlockchainError::Provider(format!("Failed to call maxLiquidityPerTick: {}", e))),
                    Err(e) => Err(BlockchainError::Provider(format!("Failed to create maxLiquidityPerTick call: {}", e))),
                }
            }
        }).await.map_err(|e| anyhow::anyhow!("Failed to get max liquidity per tick: {}", e))
    }
}

/// Mint event data
#[derive(Debug, Clone)]
struct MintEvent {
    pub sender: Address,
    pub owner: Address,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub amount: u128,
    pub amount0: U256,
    pub amount1: U256,
}

/// Burn event data
#[derive(Debug, Clone)]
struct BurnEvent {
    pub owner: Address,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub amount: u128,
    pub amount0: U256,
    pub amount1: U256,
}


// --- Swap Fetcher Implementation ---


#[derive(Clone)]
pub struct SwapFetcher {
    provider: Arc<Provider<Http>>,
    rate_limiter: Arc<ChainRateLimiter>,
    contract_abis: ContractABIs,
    token_decimals_cache: Arc<RwLock<HashMap<Address, u8>>>,
    metrics: Arc<IngestionMetrics>,
    // Event signature topics loaded from abi_parse.json (fallback to built-in constants)
    uniswap_v2_swap_topic: H256,
    uniswap_v3_swap_topic: H256,
    curve_token_exchange_topic: H256,
    balancer_v2_swap_topic: H256,
    // Cache for pool token addresses
    pool_tokens_cache: Arc<RwLock<HashMap<Address, ((Option<Address>, Option<Address>), Instant)>>>,
    // Chain-specific multicall address
    multicall_address: Address,
    // V3 tick data fetcher for high-fidelity backtesting
    v3_tick_fetcher: V3TickDataFetcher,
}

/// Metrics tracking for swap ingestion
#[derive(Debug, Default)]
pub struct IngestionMetrics {
    pub blocks_processed: AtomicU64,
    pub swaps_ingested: AtomicU64,
    pub rpc_calls: AtomicU64,
    pub rpc_errors: AtomicU64,
    pub db_writes: AtomicU64,
    pub db_errors: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub retry_attempts: AtomicU64,
    pub skipped_blocks: AtomicU64,
}

/// Checkpoint for ingestion progress
// IngestionCheckpoint struct removed - using only backtest.json ranges

impl IngestionMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn record_block_processed(&self, count: u64) {
        self.blocks_processed.fetch_add(count, Ordering::Relaxed);
    }
    
    pub fn record_swaps_ingested(&self, count: u64) {
        self.swaps_ingested.fetch_add(count, Ordering::Relaxed);
    }
    
    pub fn record_rpc_call(&self) {
        self.rpc_calls.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_rpc_error(&self) {
        self.rpc_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_db_write(&self) {
        self.db_writes.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_db_error(&self) {
        self.db_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_retry(&self) {
        self.retry_attempts.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_skipped_blocks(&self, count: u64) {
        self.skipped_blocks.fetch_add(count, Ordering::Relaxed);
    }
    
    pub fn get_summary(&self) -> String {
        format!(
            "Metrics Summary:\n\
             - Blocks processed: {}\n\
             - Swaps ingested: {}\n\
             - RPC calls: {} (errors: {})\n\
             - DB writes: {} (errors: {})\n\
             - Cache hits: {} / misses: {}\n\
             - Retry attempts: {}\n\
             - Skipped blocks: {}",
            self.blocks_processed.load(Ordering::Relaxed),
            self.swaps_ingested.load(Ordering::Relaxed),
            self.rpc_calls.load(Ordering::Relaxed),
            self.rpc_errors.load(Ordering::Relaxed),
            self.db_writes.load(Ordering::Relaxed),
            self.db_errors.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.retry_attempts.load(Ordering::Relaxed),
            self.skipped_blocks.load(Ordering::Relaxed)
        )
    }
}

#[derive(Debug, Clone)]
pub struct PoolData {
    pub pool_address: Address,
    pub pool_id: String,
    pub pool_name: Option<String>,
    pub dex_id: String,
    pub dex_name: String,
    pub protocol_type: i16,
    pub source: String,
    pub token0_address: Address,
    pub token1_address: Address,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_name: Option<String>,
    pub token1_name: Option<String>,
    pub token0_decimals: Option<i32>,
    pub token1_decimals: Option<i32>,
    pub fee_tier: Option<i32>,
    pub token0_price_usd: Option<Decimal>,
    pub token1_price_usd: Option<Decimal>,
    pub token0_price_native: Option<Decimal>,
    pub token1_price_native: Option<Decimal>,
    pub fdv_usd: Option<Decimal>,
    pub market_cap_usd: Option<Decimal>,
    pub reserve_in_usd: Option<Decimal>,
    pub price_change_5m: Option<Decimal>,
    pub price_change_1h: Option<Decimal>,
    pub price_change_6h: Option<Decimal>,
    pub price_change_24h: Option<Decimal>,
    pub volume_usd_5m: Option<Decimal>,
    pub volume_usd_1h: Option<Decimal>,
    pub volume_usd_6h: Option<Decimal>,
    pub volume_usd_24h: Option<Decimal>,
    pub volume_usd_7d: Option<Decimal>,
    pub tx_24h_buys: Option<i64>,
    pub tx_24h_sells: Option<i64>,
    pub tx_24h_buyers: Option<i64>,
    pub tx_24h_sellers: Option<i64>,
    pub is_active: Option<bool>,
    pub pool_created_at: Option<i64>,
    pub first_seen: i64,
    pub last_updated: i64,
    pub token0_reserve_onchain: Option<String>,
    pub token1_reserve_onchain: Option<String>,
    pub sqrt_price_x96_onchain: Option<String>,
    pub tick_onchain: Option<i32>,
    pub liquidity_onchain: Option<String>,
    pub liquidity_usd: Option<Decimal>,
}

async fn save_swaps_to_postgres_bulk(
    client: &mut tokio_postgres::Client,
    swaps: &[SwapEvent],
    pool_info_map: &HashMap<Address, PoolData>,
) -> Result<()> {
    if swaps.is_empty() {
        return Ok(());
    }

    info!("Bulk saving {} swaps to database", swaps.len());

    let transaction = client.transaction().await?;

    transaction.execute(
        "CREATE TEMPORARY TABLE temp_swaps (LIKE swaps INCLUDING DEFAULTS) ON COMMIT DROP",
        &[],
    ).await?;

    let mut csv_data = String::with_capacity(swaps.len() * 1024);
    let network_id = swaps.first().map_or("", |s| &s.chain_name);
    let chain_name = network_id.to_string();

    for swap in swaps {
        let pool_data = pool_info_map.get(&swap.pool_address);
        let protocol_type = pool_data.map_or(0, |p| p.protocol_type as i16);

        let token0_addr_effective = swap.token0.filter(|&a| !a.is_zero()).or_else(|| pool_data.and_then(|p| Some(p.token0_address)));
        let token1_addr_effective = swap.token1.filter(|&a| !a.is_zero()).or_else(|| pool_data.and_then(|p| Some(p.token1_address)));

        let token0_decimals_field = swap.token0_decimals.map_or("\\N".to_string(), |d| d.to_string());
        let token1_decimals_field = swap.token1_decimals.map_or("\\N".to_string(), |d| d.to_string());
        let fee_tier_field = swap.fee_tier.map_or("\\N".to_string(), |f| f.to_string());

        let fields = [
            // 1-7: core ids
            escape_string_for_tsv(&network_id),
            escape_string_for_tsv(&chain_name),
            format_bytea(&swap.pool_address),
            format_h256_bytea(&swap.tx_hash),
            swap.log_index.low_u64().to_string(),
            (swap.block_number as i64).to_string(),
            (swap.unix_ts as i64).to_string(),

            // 8-10: pool meta
            format_optional_h256(swap.pool_id),
            pool_data.map_or("\\N".to_string(), |p| escape_string_for_tsv(&p.dex_name)),
            protocol_type.to_string(),

            // 11-15: token meta (fallback to pool meta)
            token0_addr_effective.map_or("\\N".to_string(), |a| format_bytea(&a)),
            token1_addr_effective.map_or("\\N".to_string(), |a| format_bytea(&a)),
            token0_decimals_field,
            token1_decimals_field,
            fee_tier_field,

            // 16-19: amounts (v2 style columns)
            match &swap.data {
                SwapData::UniswapV2 { amount0_in, .. } => if *amount0_in > U256::zero() { amount0_in.to_string() } else { "0".into() },
                SwapData::UniswapV3 { amount0, .. }    => if amount0.is_positive()     { amount0.as_u128().to_string() } else { "0".into() },
                _ => "0".into(),
            },
            match &swap.data {
                SwapData::UniswapV2 { amount1_in, .. } => if *amount1_in > U256::zero() { amount1_in.to_string() } else { "0".into() },
                SwapData::UniswapV3 { amount1, .. }    => if amount1.is_positive()     { amount1.as_u128().to_string() } else { "0".into() },
                _ => "0".into(),
            },
            match &swap.data {
                SwapData::UniswapV2 { amount0_out, .. } => if *amount0_out > U256::zero() { amount0_out.to_string() } else { "0".into() },
                SwapData::UniswapV3 { amount0, .. }     => if amount0.is_negative()       { amount0.unsigned_abs().as_u128().to_string() } else { "0".into() },
                _ => "0".into(),
            },
            match &swap.data {
                SwapData::UniswapV2 { amount1_out, .. } => if *amount1_out > U256::zero() { amount1_out.to_string() } else { "0".into() },
                SwapData::UniswapV3 { amount1, .. }     => if amount1.is_negative()       { amount1.unsigned_abs().as_u128().to_string() } else { "0".into() },
                _ => "0".into(),
            },

            // 20-21: prices
            swap.price_t0_in_t1.map_or("\\N".into(), |p| p.to_string()),
            swap.price_t1_in_t0.map_or("\\N".into(), |p| p.to_string()),

            // 22-26: pool state
            swap.reserve0.map_or("\\N".into(), |r| r.to_string()),
            swap.reserve1.map_or("\\N".into(), |r| r.to_string()),
            swap.sqrt_price_x96.map_or("\\N".into(), |p| p.to_string()),
            swap.liquidity.map_or("\\N".into(), |l| l.to_string()),
            swap.tick.map_or("\\N".into(), |t| t.to_string()),

            // 27-30: v3 fields (sender/recipient may be present only for v3)
            match &swap.data { SwapData::UniswapV3 { sender, .. }    => format_bytea(sender),    _ => "\\N".into() },
            match &swap.data { SwapData::UniswapV3 { recipient, .. } => format_bytea(recipient), _ => "\\N".into() },
            swap.v3_amount0.map_or("\\N".into(), |a| a.to_string()),
            swap.v3_amount1.map_or("\\N".into(), |a| a.to_string()),

            // 31-36: tx meta (receipt-derived if available)
            swap.gas_used.map_or("\\N".into(), |v| v.to_string()),
            swap.effective_gas_price.map_or("\\N".into(), |v| v.to_string()),
            match &swap.transaction { Some(t) => t.from.map_or("\\N".into(), |a| format_bytea(&a)), None => "\\N".into() },
            match &swap.transaction { Some(t) => t.to.map_or("\\N".into(),   |a| format_bytea(&a)), None => "\\N".into() },
            match &swap.transaction { Some(t) => t.value.map_or("\\N".into(),|v| v.to_string()),    None => "\\N".into() },
            "\\N".into(), // tx_data not available here

            // 37: v3 tick data JSON (TSV-safe)
            swap.v3_tick_data.as_ref().map(|j| escape_string_for_tsv(j)).unwrap_or_else(|| "\\N".into()),
        ];

        let line = fields.join("\t");
        csv_data.push_str(&line);
        csv_data.push('\n');
    }

    if csv_data.is_empty() {
        info!("No new swap data to save for this batch.");
        return Ok(());
    }

    let copy_statement = "\
        COPY temp_swaps (\
            network_id, chain_name, pool_address, transaction_hash, log_index, block_number, unix_ts,\
            pool_id, dex_name, protocol_type,\
            token0_address, token1_address, token0_decimals, token1_decimals, fee_tier,\
            amount0_in, amount1_in, amount0_out, amount1_out,\
            price_t0_in_t1, price_t1_in_t0,\
            token0_reserve, token1_reserve,\
            sqrt_price_x96, liquidity, tick,\
            v3_sender, v3_recipient, v3_amount0, v3_amount1,\
            gas_used, gas_price, tx_origin, tx_to, tx_value, tx_data,\
            v3_tick_data\
        ) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')";

    // Validate CSV data for UTF-8 compliance before sending
    if let Err(e) = std::str::from_utf8(csv_data.as_bytes()) {
        error!(
            "CSV data contains invalid UTF-8 at byte position {} (length: {}, error: {})",
            e.valid_up_to(),
            csv_data.len(),
            e
        );
        return Err(anyhow::anyhow!("Invalid UTF-8 in CSV data at position {}: {}", e.valid_up_to(), e));
    }

    let copy_sink = transaction
        .copy_in(copy_statement)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start COPY: {}", e))?;
    let mut copy_writer = std::pin::pin!(copy_sink);
    let csv_bytes = csv_data.into_bytes();
    let csv_size = csv_bytes.len() as i64;
    copy_writer.send(Bytes::from(csv_bytes)).await
        .map_err(|e| anyhow::anyhow!("Failed to send COPY data: {}", e))?;
    copy_writer.finish().await
        .map_err(|e| anyhow::anyhow!("Failed to finish COPY: {}", e))?;

    // Record copy metrics
    COPY_ROWS_COUNTER.inc_by(swaps.len() as u64);
    COPY_BYTES_GAUGE.set(csv_size);

    // Upsert from temporary table to main table with correct column mapping
    let upsert_result = transaction.execute(
        "INSERT INTO swaps (\
            network_id, chain_name, pool_address, transaction_hash, log_index, block_number, unix_ts,\
            pool_id, dex_name, protocol_type,\
            token0_address, token1_address, token0_decimals, token1_decimals, fee_tier,\
            amount0_in, amount1_in, amount0_out, amount1_out,\
            price_t0_in_t1, price_t1_in_t0,\
            token0_reserve, token1_reserve,\
            sqrt_price_x96, liquidity, tick,\
            v3_sender, v3_recipient, v3_amount0, v3_amount1,\
            gas_used, gas_price, tx_origin, tx_to, tx_value, tx_data,\
            v3_tick_data\
        ) SELECT \
            network_id, chain_name, pool_address, transaction_hash, log_index, block_number, unix_ts,\
            pool_id, dex_name, protocol_type,\
            token0_address, token1_address, token0_decimals, token1_decimals, fee_tier,\
            amount0_in, amount1_in, amount0_out, amount1_out,\
            price_t0_in_t1, price_t1_in_t0,\
            token0_reserve, token1_reserve,\
            sqrt_price_x96, liquidity, tick,\
            v3_sender, v3_recipient, v3_amount0, v3_amount1,\
            gas_used, gas_price, tx_origin, tx_to, tx_value, tx_data,\
            v3_tick_data\
        FROM temp_swaps\
        ON CONFLICT (pool_address, transaction_hash, log_index) DO UPDATE SET\
            network_id = EXCLUDED.network_id,\
            chain_name = EXCLUDED.chain_name,\
            block_number = EXCLUDED.block_number,\
            unix_ts = EXCLUDED.unix_ts,\
            pool_id = EXCLUDED.pool_id,\
            dex_name = EXCLUDED.dex_name,\
            protocol_type = EXCLUDED.protocol_type,\
            token0_address = EXCLUDED.token0_address,\
            token1_address = EXCLUDED.token1_address,\
            token0_decimals = EXCLUDED.token0_decimals,\
            token1_decimals = EXCLUDED.token1_decimals,\
            fee_tier = EXCLUDED.fee_tier,\
            amount0_in = EXCLUDED.amount0_in,\
            amount1_in = EXCLUDED.amount1_in,\
            amount0_out = EXCLUDED.amount0_out,\
            amount1_out = EXCLUDED.amount1_out,\
            price_t0_in_t1 = EXCLUDED.price_t0_in_t1,\
            price_t1_in_t0 = EXCLUDED.price_t1_in_t0,\
            token0_reserve = EXCLUDED.token0_reserve,\
            token1_reserve = EXCLUDED.token1_reserve,\
            sqrt_price_x96 = EXCLUDED.sqrt_price_x96,\
            liquidity = EXCLUDED.liquidity,\
            tick = EXCLUDED.tick,\
            v3_sender = EXCLUDED.v3_sender,\
            v3_recipient = EXCLUDED.v3_recipient,\
            v3_amount0 = EXCLUDED.v3_amount0,\
            v3_amount1 = EXCLUDED.v3_amount1,\
            gas_used = EXCLUDED.gas_used,\
            gas_price = EXCLUDED.gas_price,\
            tx_origin = EXCLUDED.tx_origin,\
            tx_to = EXCLUDED.tx_to,\
            tx_value = EXCLUDED.tx_value,\
            tx_data = EXCLUDED.tx_data,\
            v3_tick_data = EXCLUDED.v3_tick_data",
        &[]
    ).await?;
    
    // Commit the transaction
    transaction.commit().await?;
    
    info!("Successfully bulk saved {} swaps to database (upserted {} rows)", swaps.len(), upsert_result);
    Ok(())
}

// Helper functions for CSV formatting - kept for save_swaps_to_postgres_bulk
#[allow(dead_code)]
fn format_bytes_for_csv(bytes: &[u8]) -> String {
    // For PostgreSQL COPY with TEXT format, bytea fields need \\x prefix
    // The single backslash in the string becomes the escape sequence in COPY
    if bytes.is_empty() {
        "\\N".to_string()
    } else {
        // hex::encode always produces valid ASCII (0-9, a-f)
        format!("\\x{}", hex::encode(bytes))
    }
}

/// Escape a string for safe inclusion in TSV format (tab-separated values)
/// This ensures no tabs, newlines, or invalid UTF-8 sequences corrupt the data
#[allow(dead_code)]
fn escape_string_for_tsv(s: &str) -> String {
    // Ensure ASCII-only output for COPY TEXT compatibility; drop any non-ASCII
    // Convert to lossy UTF-8 to avoid panics, then filter to safe ASCII
    let valid_utf8 = String::from_utf8_lossy(s.as_bytes());
    let mut out = String::with_capacity(valid_utf8.len());
    for c in valid_utf8.chars() {
        if c.is_ascii() {
            match c {
                '\t' => out.push(' '),
                '\n' | '\r' => out.push(' '),
                '\0' => {},
                c if c.is_control() => {},
                _ => out.push(c),
            }
        } else {
            // Drop non-ASCII entirely
        }
    }
    if out.is_empty() { "INVALID_UTF8".to_string() } else { out }
}

// Add this helper function for safely converting any potentially invalid string
#[allow(dead_code)]
fn sanitize_string(s: &str) -> String {
    escape_string_for_tsv(s)
}

// Add this helper for optional strings with better UTF-8 handling
#[allow(dead_code)]
fn format_optional_string_safe(value: &Option<String>) -> String {
    match value {
        Some(v) => sanitize_string(v),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn format_optional_i64(value: Option<i64>) -> String {
    match value {
        Some(v) => sanitize_string(&v.to_string()),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn format_optional_i32(value: Option<i32>) -> String {
    match value {
        Some(v) => sanitize_string(&v.to_string()),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn format_optional_decimal(value: Option<&Decimal>) -> String {
    match value {
        Some(v) => sanitize_string(&v.to_string()),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn format_optional_string(value: &Option<String>) -> String {
    match value {
        Some(v) => escape_string_for_tsv(v),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn format_optional_bool(value: Option<bool>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "\\N".to_string(),
    }
}

#[allow(dead_code)]
fn escape_csv_field(field: &str) -> String {
    // Use the more robust escape_string_for_tsv function
    escape_string_for_tsv(field)
}

// Helper function to format hex addresses for text columns (not bytea)
fn format_hex_string_safe(bytes: &[u8]) -> String {
    // hex::encode produces only ASCII characters (0-9, a-f)
    // which are always valid UTF-8, but we sanitize to be extra safe
    let hex = format!("0x{}", hex::encode(bytes));
    escape_string_for_tsv(&hex)
}

/// Retry helper function with exponential backoff
async fn retry_with_backoff<F, T, Fut>(
    operation: F,
    operation_name: &str,
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    backoff_multiplier: f64,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = Duration::from_millis(initial_delay_ms);
    let mut attempt = 0;
    
    loop {
        attempt += 1;
        
        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    info!("{} succeeded after {} attempts", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(e) if attempt >= max_retries => {
                error!("{} failed after {} attempts: {}", operation_name, attempt, e);
                return Err(e);
            }
            Err(e) => {
                warn!("{} attempt {} failed: {}, retrying in {:?}", 
                      operation_name, attempt, e, delay);
                sleep(delay).await;
                
                // Calculate next delay with exponential backoff
                let next_delay_ms = (delay.as_millis() as f64 * backoff_multiplier) as u64;
                delay = Duration::from_millis(next_delay_ms.min(max_delay_ms));
            }
        }
    }
}

/// Validate pool address to prevent malicious inputs
async fn validate_pool_address(provider: &Arc<Provider<Http>>, address: &Address, target_block: u64) -> Result<()> {
    // Check for zero address
    if *address == Address::zero() {
        return Err(anyhow!("Invalid pool address: zero address"));
    }

    // Check if address has code (reject EOAs)
    let code = provider.get_code(*address, Some(target_block.into())).await
        .map_err(|e| anyhow!("Failed to get code for address {}: {}", address, e))?;

    if code.as_ref().is_empty() {
        return Err(anyhow!("Invalid pool address: {} is an EOA (no code) at block {}", address, target_block));
    }

    // Could add additional checks here for known malicious addresses
    // from a blacklist database

    Ok(())
}


impl SwapFetcher {
    fn get_multicall_contract(&self) -> Result<Contract<Provider<Http>>> {
        let abi: Abi = serde_json::from_str(MULTICALL3_ABI)
            .map_err(|e| anyhow::anyhow!("Failed to parse Multicall3 ABI: {e}"))?;
        Ok(Contract::new(self.multicall_address, abi, self.provider.clone()))
    }

    pub async fn new_with_provider(
        provider: Arc<Provider<Http>>,
        chain_name: &str,
        rate_limiter_manager: &GlobalRateLimiterManager,
        _db_client: &tokio_postgres::Client,
        config_dir: &str,
    ) -> Result<Self> {
        info!("Creating rate limiter for chain: {}", chain_name);
        let rate_limiter = rate_limiter_manager.get_or_create_chain_limiter(
            chain_name.to_string(),
            None,
            None,
        );

        info!("Initializing SwapFetcher for chain: {} (listening to all swaps)", chain_name);
        let token_decimals_cache = Arc::new(RwLock::new(HashMap::new()));
        let pool_tokens_cache: Arc<RwLock<HashMap<Address, ((Option<Address>, Option<Address>), Instant)>>> = Arc::new(RwLock::new(HashMap::new()));

        // Load and validate chain configuration
        info!("Loading chain configuration...");
        let config = Config::load_from_directory(config_dir).await
            .map_err(|e| anyhow::anyhow!("Failed to load chain configuration: {}", e))?;
        let per_chain_config = config.get_chain_config(chain_name)
            .map_err(|e| anyhow::anyhow!("Chain '{}' not found in config: {}", chain_name, e))?;

        // Validate chain ID matches provider
        let provider_chain_id = provider.get_chainid().await
            .map_err(|e| anyhow::anyhow!("Failed to get provider chain ID: {}", e))?;
        if provider_chain_id.as_u64() != per_chain_config.chain_id {
            return Err(anyhow::anyhow!(
                "Chain ID mismatch: provider reports {}, config specifies {} for chain {}",
                provider_chain_id, per_chain_config.chain_id, chain_name
            ));
        }

        // Get multicall address from config or use default
        let multicall_address = per_chain_config.multicall_address
            .unwrap_or_else(|| Address::from_str(MULTICALL3_ADDRESS)
                .expect("Default MULTICALL3_ADDRESS should be valid"));

        info!("Loading ABI configuration...");
        let abi_config = tokio::time::timeout(
            Duration::from_secs(10), // 10 second timeout for loading ABI config
            load_abi_parse_config()
        ).await
            .map_err(|_| anyhow::anyhow!("Timeout loading ABI configuration"))?
            .map_err(|e| anyhow::anyhow!("Failed to load ABI configuration: {}", e))?;

        info!("Creating contract ABIs...");
        let contract_abis = ContractABIs::from_config(&abi_config)?;
        info!("Contract ABIs created successfully");

        // Resolve topics from config with safe fallbacks
        let parse_topic = |dex: &str, key: &str| -> Option<H256> {
            let dex_cfg = abi_config.dexes.get(dex)?;
            let topic_map = &dex_cfg.topic0;
            let topic_hex = topic_map.get(key)?;
            let cleaned = topic_hex.trim_start_matches("0x");
            if cleaned.len() != 64 { return None; }
            let bytes = hex::decode(cleaned).ok()?;
            if bytes.len() != 32 { return None; }
            Some(H256::from_slice(&bytes))
        };
        let uniswap_v2_swap_topic = parse_topic("UniswapV2", "Swap").unwrap_or(UNISWAP_V2_SWAP_TOPIC);
        let uniswap_v3_swap_topic = parse_topic("UniswapV3", "Swap").unwrap_or(UNISWAP_V3_SWAP_TOPIC);
        let curve_token_exchange_topic = parse_topic("Curve", "TokenExchange").unwrap_or(CURVE_TOKEN_EXCHANGE_TOPIC);
        let balancer_v2_swap_topic = parse_topic("Balancer", "Swap").unwrap_or(BALANCER_V2_SWAP_TOPIC);

        info!("SwapFetcher initialized with topics:");
        info!("  UniswapV2: {:?}", uniswap_v2_swap_topic);
        info!("  UniswapV3: {:?}", uniswap_v3_swap_topic);
        info!("  Curve: {:?}", curve_token_exchange_topic);
        info!("  Balancer: {:?}", balancer_v2_swap_topic);

        // Initialize V3 tick data fetcher
        let v3_tick_fetcher = V3TickDataFetcher::new(
            provider.clone(),
            rate_limiter.clone(),
            contract_abis.clone(),
        );


        Ok(SwapFetcher {
            provider,
            rate_limiter,
            contract_abis,
            token_decimals_cache,
            metrics: Arc::new(IngestionMetrics::new()),
            uniswap_v2_swap_topic,
            uniswap_v3_swap_topic,
            curve_token_exchange_topic,
            balancer_v2_swap_topic,
            pool_tokens_cache,
            multicall_address,
            v3_tick_fetcher,
        })
    }

    /// Batch fetch blocks using concurrent requests with semaphore
    pub async fn batch_fetch_blocks(&self, nums: &[u64]) -> Result<HashMap<u64, Block<H256>>> {
        use futures::stream::{self, StreamExt};
        let sem = Arc::new(tokio::sync::Semaphore::new(100));
        let futs = nums.iter().map(|&n| {
            let p = self.provider.clone();
            let s = sem.clone();
            async move {
                let _g = s.acquire().await.unwrap();
                p.get_block(n).await.map(|opt| (n, opt))
            }
        });
        let mut out = HashMap::new();
        let results = stream::iter(futs).buffer_unordered(100).collect::<Vec<_>>().await;
        for r in results {
            if let Ok((n, Some(b))) = r { out.insert(n, b); }
        }
        Ok(out)
    }
    
    /// Use multicall to fetch multiple contract states in one call
    pub async fn multicall_fetch_reserves(&self, pools: &[(Address, u64)]) -> Result<HashMap<(Address, u64), (U256, U256)>> {
        if pools.is_empty() {
            return Ok(HashMap::new());
        }

        // Group calls by block_number
        let mut by_block: HashMap<u64, Vec<Address>> = HashMap::new();
        for (addr, bn) in pools {
            by_block.entry(*bn).or_default().push(*addr);
        }

        let mut results = HashMap::new();
        let multicall_contract = self.get_multicall_contract()?;

        for (bn, addrs) in by_block {
            let calls: Vec<_> = addrs.iter().map(|&addr| {
                let calldata = self.contract_abis.create_v2_pair_contract(addr, self.provider.clone())
                    .method::<(), (U256, U256, u32)>("getReserves", ())
                    .unwrap()
                    .calldata()
                    .unwrap();
                (addr, true, ethers::types::Bytes::from(calldata.0))
            }).collect();

            // Execute multicall at specific block
            let at = BlockId::Number(BlockNumber::Number(bn.into()));
            let response: Vec<(bool, ethers::types::Bytes)> = self.rate_limiter.execute_rpc_call("multicall_reserves", || {
                let multicall_clone = multicall_contract.clone();
                let calls_clone = calls.clone();
                async move {
                    multicall_clone
                        .method::<(Vec<(Address, bool, ethers::types::Bytes)>,), Vec<(bool, ethers::types::Bytes)>>("aggregate3", (calls_clone,))
                        .map_err(|e| BlockchainError::Provider(e.to_string()))?
                        .block(at)
                        .call()
                        .await
                        .map_err(|e| BlockchainError::Provider(e.to_string()))
                }
            }).await?;

            for (i, (success, data)) in response.iter().enumerate() {
                if *success {
                    if let Ok(tokens) = ethers::abi::decode(
                        &[
                            ethers::abi::ParamType::Uint(112), // reserve0
                            ethers::abi::ParamType::Uint(112), // reserve1
                            ethers::abi::ParamType::Uint(32),  // blockTimestampLast
                        ],
                        data.as_ref(),
                    ) {
                        if tokens.len() >= 2 {
                            let r0 = tokens[0].clone().into_uint().unwrap_or_default();
                            let r1 = tokens[1].clone().into_uint().unwrap_or_default();
                            results.insert((addrs[i], bn), (r0, r1));
                        }
                    }
                }
            }
        }
        Ok(results)
    }
    
    /// Batch fetch token decimals using multicall
    pub async fn batch_fetch_v3_pool_state(&self, pools: &[(Address, u64)]) -> Result<HashMap<Address, (U256, i32, u128)>> {
        let mut results = HashMap::new();
        
        if pools.is_empty() {
            return Ok(results);
        }
        
        info!("Fetching V3 state for {} pools", pools.len());
        
        // Process in batches to avoid overwhelming the RPC
        let chunk_size = 10;
        for chunk in pools.chunks(chunk_size) {
            let mut futures = vec![];

            for &(pool_address, block_number) in chunk {
                let provider = self.provider.clone();
                let contract_abis = self.contract_abis.clone();

                // Create async block to keep contract alive during execution
                let future = async move {
                    let contract = contract_abis.create_v3_pool_contract(pool_address, provider);
                    let at = BlockId::Number(BlockNumber::Number(block_number.into()));
                    let slot0_result = match contract.method::<_, (U256, i32, u16, u16, u16, u8, bool)>("slot0", ()) {
                        Ok(m) => m.block(at).call().await,
                        Err(e) => { tracing::debug!("slot0 method construction failed: {}", e); Err(e.into()) }
                    };
                    let liquidity_result = match contract.method::<_, u128>("liquidity", ()) {
                        Ok(m) => m.block(at).call().await,
                        Err(e) => { tracing::debug!("liquidity method construction failed: {}", e); Err(e.into()) }
                    };
                    (pool_address, slot0_result, liquidity_result)
                };

                futures.push(future);
            }
            
            // Execute futures concurrently
            let results_batch = futures::future::join_all(futures).await;
            for (pool_address, slot0_result, liquidity_result) in results_batch {
                if let (Ok(slot0), Ok(liquidity)) = (slot0_result, liquidity_result) {
                    results.insert(pool_address, (slot0.0, slot0.1, liquidity));
                }
            }
            
            // Small delay between batches
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        
        info!("Fetched V3 state for {}/{} pools", results.len(), pools.len());
        Ok(results)
    }
    
    /// Fetch token addresses for a pool based on its protocol
    pub async fn fetch_pool_tokens(&self, pool_address: &Address, block_number: u64) -> (Option<Address>, Option<Address>) {
        let cache = self.pool_tokens_cache.read().await;
        if let Some((tokens, timestamp)) = cache.get(pool_address) {
            if tokens.0.is_some() && tokens.1.is_some() {
                return tokens.clone();
            }
            // If tokens are None, check TTL
            if timestamp.elapsed() < Duration::from_secs(300) { // 5 minute TTL
                return tokens.clone();
            }
        }
        drop(cache); // Release read lock

        let result = match self.detect_pool_protocol(pool_address, block_number).await {
            Ok(DexProtocol::UniswapV3) => {
                self.fetch_v3_pool_tokens(pool_address, block_number).await
            }
            Ok(DexProtocol::UniswapV2) | Ok(DexProtocol::SushiSwapV2) => {
                self.fetch_v2_pool_tokens(pool_address, block_number).await
            }
            _ => (None, None),
        };
        self.pool_tokens_cache.write().await.insert(*pool_address, (result.clone(), Instant::now()));
        result
    }

    /// Detect pool protocol by trying different contract interfaces
    async fn detect_pool_protocol(&self, pool_address: &Address, block_number: u64) -> Result<DexProtocol> {
        let v3_contract = self.contract_abis.create_v3_pool_contract(*pool_address, self.provider.clone());
        let at = BlockId::Number(BlockNumber::Number(block_number.into()));
        if let Ok(method) = v3_contract.method::<(), i32>("tickSpacing", ()) {
            if method.block(at).call().await.is_ok() {
                return Ok(DexProtocol::UniswapV3);
            }
        }

        let v2_contract = self.contract_abis.create_v2_pair_contract(*pool_address, self.provider.clone());
        if let Ok(method) = v2_contract.method::<(), Address>("factory", ()) {
            if method.block(at).call().await.is_ok() {
                return Ok(DexProtocol::UniswapV2);
            }
        }
        Err(anyhow::anyhow!("Unknown protocol for pool {}", pool_address))
    }

    /// Fetch token addresses from V3 pool contract
    async fn fetch_v3_pool_tokens(&self, pool_address: &Address, block_number: u64) -> (Option<Address>, Option<Address>) {
        let contract = self.contract_abis.create_v3_pool_contract(*pool_address, self.provider.clone());
        let at = BlockId::Number(BlockNumber::Number(block_number.into()));
        let token0_call = self.rate_limiter.execute_rpc_call("v3_token0", || {
            let c = contract.clone();
            async move {
                match c.method::<(), Address>("token0", ()) {
                    Ok(method) => method.block(at).call().await.map_err(|e| BlockchainError::Provider(e.to_string())),
                    Err(e) => Err(BlockchainError::Provider(format!("Method creation failed: {}", e))),
                }
            }
        });
        let token1_call = self.rate_limiter.execute_rpc_call("v3_token1", || {
            let c = contract.clone();
            async move {
                match c.method::<(), Address>("token1", ()) {
                    Ok(method) => method.block(at).call().await.map_err(|e| BlockchainError::Provider(e.to_string())),
                    Err(e) => Err(BlockchainError::Provider(format!("Method creation failed: {}", e))),
                }
            }
        });
        let (token0, token1) = tokio::join!(token0_call, token1_call);
        (token0.ok(), token1.ok())
    }

    /// Fetch token addresses from V2 pool contract
    async fn fetch_v2_pool_tokens(&self, pool_address: &Address, block_number: u64) -> (Option<Address>, Option<Address>) {
        let contract = self.contract_abis.create_v2_pair_contract(*pool_address, self.provider.clone());
        let at = BlockId::Number(BlockNumber::Number(block_number.into()));
        let token0_call = self.rate_limiter.execute_rpc_call("v2_token0", || {
            let c = contract.clone();
            async move {
                match c.method::<(), Address>("token0", ()) {
                    Ok(method) => method.block(at).call().await.map_err(|e| BlockchainError::Provider(e.to_string())),
                    Err(e) => Err(BlockchainError::Provider(format!("Method creation failed: {}", e))),
                }
            }
        });
        let token1_call = self.rate_limiter.execute_rpc_call("v2_token1", || {
            let c = contract.clone();
            async move {
                match c.method::<(), Address>("token1", ()) {
                    Ok(method) => method.block(at).call().await.map_err(|e| BlockchainError::Provider(e.to_string())),
                    Err(e) => Err(BlockchainError::Provider(format!("Method creation failed: {}", e))),
                }
            }
        });
        let (token0, token1) = tokio::join!(token0_call, token1_call);
        (token0.ok(), token1.ok())
    }

    pub async fn batch_fetch_decimals(&self, tokens: &[Address]) -> Result<HashMap<Address, u8>> {
        if tokens.is_empty() {
            return Ok(HashMap::new());
        }
        
        // Check cache first
        let cache = self.token_decimals_cache.read().await;
        let mut decimals_map = HashMap::new();
        let mut uncached_tokens = Vec::new();
        
        for token in tokens {
            if let Some(&decimals) = cache.get(token) {
                decimals_map.insert(*token, decimals);
                self.metrics.record_cache_hit();
            } else {
                uncached_tokens.push(*token);
                self.metrics.record_cache_miss();
            }
        }
        drop(cache);
        
        if uncached_tokens.is_empty() {
            return Ok(decimals_map);
        }
        
        // Use multicall for uncached tokens
        let multicall_address = self.multicall_address;
        let multicall_abi: Abi = serde_json::from_str(MULTICALL3_ABI)?;
        let multicall = Contract::new(multicall_address, multicall_abi, self.provider.clone());
        
        const BATCH_SIZE: usize = 200;
        for chunk in uncached_tokens.chunks(BATCH_SIZE) {
            let mut calls = Vec::new();
            
            for token in chunk {
                let function = self.contract_abis
                    .erc20_abi
                    .function("decimals")?;
                let call_data = function.encode_input(&[])?;
                
                calls.push((
                    *token,
                    true, // allowFailure
                    Bytes::from(call_data),
                ));
            }
            
            let result = multicall
                .method::<_, Vec<(bool, Bytes)>>("aggregate3", (calls,))?
                .call()
                .await?;
            
            let mut cache = self.token_decimals_cache.write().await;
            for (i, (success, data)) in result.iter().enumerate() {
                if *success && data.len() >= 32 {
                    // Decode the 32-byte big-endian U256 return data
                    let decimals_u256 = U256::from_big_endian(&data[data.len() - 32..]);
                    // Cast to u8 with bounds check (decimals should be 0-255)
                    if let Ok(decimals) = u8::try_from(decimals_u256.as_u64()) {
                        cache.insert(chunk[i], decimals);
                        decimals_map.insert(chunk[i], decimals);
                    } else {
                        // Invalid decimals value, default to 18
                        cache.insert(chunk[i], 18);
                        decimals_map.insert(chunk[i], 18);
                    }
                } else {
                    // Default to 18 for failed calls or invalid data
                    cache.insert(chunk[i], 18);
                    decimals_map.insert(chunk[i], 18);
                }
            }
        }
        
        Ok(decimals_map)
    }
    
    /// Enhanced enrichment with parallel batch operations
    pub async fn enrich_swaps_optimized(&self, swaps: &mut Vec<SwapEvent>) -> Result<()> {
        if swaps.is_empty() {
            return Ok(());
        }
        
        let start = std::time::Instant::now();
        
        // Collect unique data points needed
        let unique_blocks: HashSet<u64> = swaps.iter().map(|s| s.block_number).collect();
        let unique_txs: HashSet<H256> = swaps.iter().map(|s| s.tx_hash).collect();
        let unique_v2_pools: HashSet<(Address, u64)> = swaps
            .iter()
            .filter(|s| matches!(s.data, SwapData::UniswapV2 { .. }))
            .map(|s| (s.pool_address, s.block_number.saturating_sub(1)))
            .collect();
        let unique_tokens: HashSet<Address> = swaps
            .iter()
            .flat_map(|s| vec![s.token0, s.token1])
            .filter_map(|t| t)
            .collect();
        let unique_v3_pools: HashSet<Address> = swaps
            .iter()
            .filter(|s| s.protocol == DexProtocol::UniswapV3 || s.protocol == DexProtocol::PancakeSwapV3)
            .map(|s| s.pool_address)
            .collect();
        
        info!("Enriching {} swaps: {} blocks, {} txs, {} V2 pools, {} V3 pools, {} tokens",
              swaps.len(), unique_blocks.len(), unique_txs.len(), unique_v2_pools.len(), unique_v3_pools.len(), unique_tokens.len());
        
        // Parallel batch fetching
        let blocks_vec: Vec<u64> = unique_blocks.into_iter().collect();
        let txs_vec: Vec<H256> = unique_txs.into_iter().collect();
        let pools_vec: Vec<(Address, u64)> = unique_v2_pools.into_iter().collect();
        let tokens_vec: Vec<Address> = unique_tokens.into_iter().collect();
        
        // Collect V3 pools for tick data fetching before consuming unique_v3_pools
        let v3_pools_for_tick_data: Vec<Address> = unique_v3_pools.iter().cloned().collect();
        let v3_pools_vec: Vec<(Address, u64)> = unique_v3_pools.into_iter()
            .map(|pool_addr| {
                let block_num = swaps.iter()
                    .find(|s| s.pool_address == pool_addr)
                    .map(|s| s.block_number)
                    .unwrap_or(0);
                (pool_addr, block_num)
            })
            .collect();

        // Fetch V3 tick data for high-fidelity backtesting (stored in swaps table only)
        let v3_tick_data_futures: Vec<_> = v3_pools_for_tick_data.iter()
            .map(|pool_addr| {
                let fetcher = self.v3_tick_fetcher.clone();
                let block_num = swaps.iter()
                    .find(|s| s.pool_address == *pool_addr)
                    .map(|s| s.block_number)
                    .unwrap_or(0);
                async move {
                    match fetcher.fetch_tick_data_at_block(pool_addr, block_num).await {
                        Ok(tick_data) => Some((*pool_addr, tick_data)),
                        Err(e) => {
                            warn!("Failed to fetch V3 tick data for pool {} at block {}: {}", pool_addr, block_num, e);
                            None
                        }
                    }
                }
            })
            .collect();

        let (blocks, receipts, reserves, v3_states, decimals, v3_tick_results) = tokio::join!(
            self.batch_fetch_blocks(&blocks_vec),
            self.batch_fetch_receipts(&txs_vec),
            self.multicall_fetch_reserves(&pools_vec),
            self.batch_fetch_v3_pool_state(&v3_pools_vec),
            self.batch_fetch_decimals(&tokens_vec),
            futures::future::join_all(v3_tick_data_futures)
        );

        let blocks = blocks?;
        let receipts = receipts?;
        let reserves = reserves?;
        let v3_states = v3_states?;
        let decimals = decimals?;

        // Apply fetched decimals to cache for later use
        {
            let mut cache = self.token_decimals_cache.write().await;
            for (token_addr, &dec) in &decimals {
                cache.insert(*token_addr, dec);
            }
        }

        // Build V3 tick data map (for storage in swaps table only)
        let mut v3_tick_data_map = HashMap::new();
        for result in v3_tick_results.into_iter().flatten() {
            v3_tick_data_map.insert(result.0, result.1);
        }

        info!("Fetched V3 tick data for {}/{} pools (stored in swaps table)", v3_tick_data_map.len(), v3_pools_for_tick_data.len());

        // Apply enrichments
        for swap in swaps.iter_mut() {
            // Add V3 tick data to the swap event (stored in swaps table as JSON)
            if swap.protocol == DexProtocol::UniswapV3 || swap.protocol == DexProtocol::PancakeSwapV3 {
                if let Some(tick_data) = v3_tick_data_map.get(&swap.pool_address) {
                    if let Ok(json_tick_data) = serde_json::to_string(tick_data) {
                        swap.v3_tick_data = Some(json_tick_data);
                    }
                }
            }

            // Block timestamp
            if let Some(block) = blocks.get(&swap.block_number) {
                swap.unix_ts = block.timestamp.as_u64();
            }
            
            // Transaction receipt - ensure gas data is always populated
            if let Some(receipt) = receipts.get(&swap.tx_hash) {
                swap.gas_used = receipt.gas_used.map(|g| U256::from(g.as_u64()));
                swap.effective_gas_price = receipt.effective_gas_price;
                
                // Apply all available receipt data for transaction metadata
                if swap.transaction.is_none() {
                    swap.transaction = Some(TransactionData {
                        hash: swap.tx_hash,
                        from: Some(receipt.from),
                        to: receipt.to,
                        block_number: receipt.block_number.map(|n| U256::from(n.as_u64())),
                        block_hash: receipt.block_hash,
                        gas: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                        gas_price: receipt.effective_gas_price,
                        gas_used: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                        value: None,
                        max_fee_per_gas: None,
                        max_priority_fee_per_gas: None,
                        transaction_type: receipt.transaction_type.map(|t| U256::from(t.as_u64())),
                        cumulative_gas_used: Some(receipt.cumulative_gas_used),
                        chain_id: None,
                        base_fee_per_gas: None,
                        priority_fee_per_gas: None,
                    });
                }
            } else {
                // If receipt not in cache, fetch directly
                if let Ok(receipt) = self.provider.get_transaction_receipt(swap.tx_hash).await {
                    if let Some(receipt) = receipt {
                        swap.gas_used = receipt.gas_used.map(|g| U256::from(g.as_u64()));
                        swap.effective_gas_price = receipt.effective_gas_price;
                        
                        // Populate transaction data
                        if swap.transaction.is_none() {
                            swap.transaction = Some(TransactionData {
                                hash: swap.tx_hash,
                                from: Some(receipt.from),
                                to: receipt.to,
                                block_number: receipt.block_number.map(|n| U256::from(n.as_u64())),
                                block_hash: receipt.block_hash,
                                gas: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                                gas_price: receipt.effective_gas_price,
                                gas_used: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                                value: None,
                                max_fee_per_gas: None,
                                max_priority_fee_per_gas: None,
                                transaction_type: receipt.transaction_type.map(|t| U256::from(t.as_u64())),
                                cumulative_gas_used: Some(receipt.cumulative_gas_used),
                                chain_id: None, // Will be set from provider if needed
                                base_fee_per_gas: None, // TransactionReceipt doesn't have this field
                                priority_fee_per_gas: None, // TransactionReceipt doesn't have this field
                            });
                        }
                    }
                }
            }
            
            // V2 reserves - fetch for all V2-like protocols (UniswapV2, SushiSwap, PancakeSwap V2)
            if matches!(swap.data, SwapData::UniswapV2 { .. }) || swap.protocol == DexProtocol::SushiSwap || swap.protocol == DexProtocol::PancakeSwap {
                let key = (swap.pool_address, swap.block_number.saturating_sub(1));
                if let Some((reserve0, reserve1)) = reserves.get(&key) {
                    swap.reserve0 = Some(*reserve0);
                    swap.reserve1 = Some(*reserve1);

                    if let (Some(dec0), Some(dec1)) = (swap.token0_decimals, swap.token1_decimals) {
                        if !reserve0.is_zero() && !reserve1.is_zero() {
                            if let (Ok(n), Ok(d)) = (
                                dex_math::mul_div(*reserve1, U256::exp10(dec0 as usize), U256::one()),
                                dex_math::mul_div(*reserve0, U256::exp10(dec1 as usize), U256::one())
                            ) {
                                if !d.is_zero() {
                                    if let Ok(p01_q) = dex_math::mul_div(n, U256::exp10(18), d) {
                                        swap.price_t0_in_t1 = Decimal::from_str(&p01_q.to_string()).ok()
                                            .and_then(|d_val| Decimal::from_u128(1_000_000_000_000_000_000).map(|e| d_val / e));
                                        swap.price_t1_in_t0 = swap.price_t0_in_t1.and_then(|p| if p.is_zero() { None } else { Some(Decimal::from(1) / p) });
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Fallback to direct fetch if not in batch results
                    let contract = self.contract_abis.create_v3_pool_contract(swap.pool_address, self.provider.clone());
                    
                    // Fetch slot0 for sqrt_price and tick
                    if let Ok(slot0) = match contract.method::<_, (U256, i32, u16, u16, u16, u8, bool)>("slot0", ()) { Ok(m) => m.call().await, Err(e) => Err(ethers::contract::ContractError::AbiError(e)) } {
                        swap.sqrt_price_x96 = Some(slot0.0);
                        swap.tick = Some(slot0.1);
                    }
                    
                    // Fetch liquidity
                    if let Ok(liquidity) = match contract.method::<_, u128>("liquidity", ()) { Ok(m) => m.call().await, Err(e) => Err(ethers::contract::ContractError::AbiError(e)) } {
                        swap.liquidity = Some(U256::from(liquidity));
                    }
                }
            } else if matches!(swap.protocol, DexProtocol::UniswapV3 | DexProtocol::PancakeSwapV3) {
                if let Some(&(sqrt_price_x96, tick, liquidity)) = v3_states.get(&swap.pool_address) {
                    swap.sqrt_price_x96 = Some(sqrt_price_x96);
                    swap.tick = Some(tick);
                    swap.liquidity = Some(U256::from(liquidity));

                    // Optional: virtual reserves for V3
                    if let Ok((r0, r1)) = calculate_v3_reserves_from_liquidity_and_sqrt_price(liquidity, sqrt_price_x96) {
                        swap.reserve0 = Some(r0);
                        swap.reserve1 = Some(r1);
                    }
                }
            }

            // V3 price calculation from sqrt_price_x96 with decimal adjustment
            if let (Some(d0), Some(d1)) = (swap.token0_decimals, swap.token1_decimals) {
                // V3 price calculation from sqrt_price_x96 with decimal adjustment
                if let Some(sqrt_price_x96) = swap.sqrt_price_x96 {
                    swap.price_t0_in_t1 = price_v3_q96(sqrt_price_x96, d0, d1);
                    swap.price_t1_in_t0 = swap.price_t0_in_t1.and_then(|p| if p.is_zero() { None } else { Some(Decimal::from(1) / p) });
                }
            }
        }
        
        info!("Enrichment completed in {:.2}s", start.elapsed().as_secs_f64());
        Ok(())
    }
    
    /// Batch fetch transaction receipts
    async fn batch_fetch_receipts(&self, tx_hashes: &[H256]) -> Result<HashMap<H256, TransactionReceipt>> {
        use futures::{stream, StreamExt};
        use tokio::sync::Semaphore;
        use std::sync::Arc;

        if tx_hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let sem = Arc::new(Semaphore::new(100)); // tune as needed
        let provider = self.provider.clone();

        let futs = tx_hashes.iter().cloned().map(|h| {
            let p = provider.clone();
            let s = sem.clone();
            async move {
                let _g = s.acquire().await.unwrap();
                p.get_transaction_receipt(h).await.map(|o| (h, o))
            }
        });

        let mut out = HashMap::with_capacity(tx_hashes.len());
        let results = stream::iter(futs).buffer_unordered(100).collect::<Vec<_>>().await;
        for r in results {
            match r {
                Ok((h, Some(rcpt))) => { out.insert(h, rcpt); }
                Ok((_h, None)) => {} // pending / pruned
                Err(e) => { warn!("receipt fetch error: {}", e); }
            }
        }
        Ok(out)
    }
    
    pub async fn enrich_swaps(&self, swaps: &mut Vec<SwapEvent>, _rpc_url: &str) -> Result<()> {
        if swaps.is_empty() {
            return Ok(());
        }
        
        // Collect unique addresses for batch lookups
        let mut unique_pool_addresses = HashSet::new();
        let mut unique_token_addresses = HashSet::new();
        let mut unique_block_numbers = HashSet::new();
        let mut unique_transaction_hashes = HashSet::new();
        
        for swap in swaps.iter() {
            unique_pool_addresses.insert(swap.pool_address);
            unique_block_numbers.insert(swap.block_number);
            unique_transaction_hashes.insert(swap.tx_hash);
        }
        
        // Batch fetch block timestamps
        let mut block_timestamps = HashMap::new();
        for &block_number in &unique_block_numbers {
            match self.rate_limiter.execute_rpc_call("get_block", || {
                self.provider.get_block(block_number).map_err(|e| BlockchainError::Provider(format!("Failed to get block: {}", e)))
            }).await {
                Ok(Some(block)) => {
                    block_timestamps.insert(block_number, block.timestamp.as_u64());
                }
                Ok(None) => {
                    warn!("Block {} not found", block_number);
                }
                Err(e) => {
                    warn!("Failed to fetch block {}: {}", block_number, e);
                }
            }
        }
        
        // Batch fetch transaction receipts
        let mut transaction_receipts = HashMap::new();
        for &tx_hash in &unique_transaction_hashes {
            match self.rate_limiter.execute_rpc_call("get_transaction_receipt", || {
                self.provider.get_transaction_receipt(tx_hash).map_err(|e| BlockchainError::Provider(format!("Failed to get transaction receipt: {}", e)))
            }).await {
                Ok(Some(receipt)) => {
                    transaction_receipts.insert(tx_hash, receipt);
                }
                Ok(None) => {
                    warn!("Transaction receipt {} not found", tx_hash);
                }
                Err(e) => {
                    warn!("Failed to fetch transaction receipt {}: {}", tx_hash, e);
                }
            }
        }
        
        // Batch fetch pre-trade V2 reserves for accurate price calculation
        // Now trying to fetch for all V2-like swaps based on protocol detection
        let mut v2_pool_reserves = HashMap::new();
        let mut v2_pools_to_fetch = HashSet::new();

        for swap in swaps.iter() {
            // For V2-like protocols, try to fetch reserves regardless of pre-loaded data
            if matches!(swap.protocol, DexProtocol::UniswapV2 | DexProtocol::SushiSwap | DexProtocol::PancakeSwap) {
                v2_pools_to_fetch.insert((swap.pool_address, swap.block_number));
            }
        }
        
        // Fetch reserves at block before swap occurred (pre-trade state)
        let mut successful_reserves = 0;
        let mut failed_reserves = 0;
        let total_v2_pools = v2_pools_to_fetch.len();
        
        info!("Attempting to fetch reserves for {} V2 pools", total_v2_pools);
        
        for (pool_address, block_number) in v2_pools_to_fetch {
            let pre_trade_block = if block_number > 0 { block_number - 1 } else { block_number };
            
            // First check if the contract exists and has code
            let contract_exists = match self.rate_limiter.execute_rpc_call("get_code", || {
                self.provider.get_code(pool_address, Some(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(pre_trade_block.into()))))
                    .map_err(|e| BlockchainError::Provider(format!("Failed to get contract code: {}", e)))
            }).await {
                Ok(code) => !code.is_empty(),
                Err(e) => {
                    warn!("Failed to check if contract exists for pool 0x{} at block {}: {}", hex::encode(pool_address.as_bytes()), pre_trade_block, e);
                    false
                }
            };
            
            if !contract_exists {
                warn!("Contract 0x{} does not exist or has no code at block {}", hex::encode(pool_address.as_bytes()), pre_trade_block);
                failed_reserves += 1;
                continue;
            }
            
            // Try to get reserves with fallback methods
            let reserves_found = false;
            
            // Try standard getReserves first
            match self.rate_limiter.execute_rpc_call("get_reserves", || async {
                let contract = self.contract_abis.create_v2_pair_contract(pool_address, self.provider.clone());
                let call = contract.method::<(), (U256, U256, u32)>("getReserves", ())
                    .map_err(|e| BlockchainError::Provider(format!("Failed to create getReserves call: {}", e)))?;
                
                call.block(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(pre_trade_block.into())))
                    .call()
                    .await
                    .map_err(|e| BlockchainError::Provider(format!("Failed to call getReserves: {}", e)))
            }).await {
                Ok((reserve0, reserve1, _block_timestamp_last)) => {
                    v2_pool_reserves.insert((pool_address, block_number), (reserve0, reserve1));
                    successful_reserves += 1;
                }
                Err(e) => {
                    // Check if it's a "contract doesn't exist" error vs "method doesn't exist" error
                    let error_msg = e.to_string();
                    if error_msg.contains("Invalid name") || error_msg.contains("failed to decode empty bytes") {
                        // Try alternative method names that some DEXes use
                        let contract = self.contract_abis.create_v2_pair_contract(pool_address, self.provider.clone());
                        
                        // Try "reserves" (some DEXes use this)
                        if let Ok(call) = contract.method::<(), (U256, U256, u32)>("reserves", ()) {
                            if let Ok(result) = call.block(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(pre_trade_block.into()))).call().await {
                                let (reserve0, reserve1, _) = result;
                                v2_pool_reserves.insert((pool_address, block_number), (reserve0, reserve1));
                                successful_reserves += 1;
                            }
                        }
                        
                        // Try "getReserves0" and "getReserves1" (some DEXes use separate functions)
                        if !reserves_found {
                            if let (Ok(call0), Ok(call1)) = (
                                contract.method::<(), U256>("getReserves0", ()),
                                contract.method::<(), U256>("getReserves1", ())
                            ) {
                                if let (Ok(reserve0), Ok(reserve1)) = (
                                    call0.block(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(pre_trade_block.into()))).call().await,
                                    call1.block(ethers::types::BlockId::Number(ethers::types::BlockNumber::Number(pre_trade_block.into()))).call().await
                                ) {
                                    v2_pool_reserves.insert((pool_address, block_number), (reserve0, reserve1));
                                    successful_reserves += 1;
                                }
                            }
                        }
                        
                        if !reserves_found {
                            warn!("Pool 0x{} at block {} is not a standard UniswapV2 pair (no getReserves/reserves method)", hex::encode(pool_address.as_bytes()), pre_trade_block);
                            failed_reserves += 1;
                        }
                    } else {
                        warn!("Failed to fetch reserves for pool 0x{} at block {}: {}", hex::encode(pool_address.as_bytes()), pre_trade_block, e);
                        failed_reserves += 1;
                    }
                }
            }
        }
        
        info!("Reserves fetching complete: {}/{} V2 pools successful, {} failed", successful_reserves, total_v2_pools, failed_reserves);
        
        // Enrich each swap with pool data and fetched data
        for swap in swaps.iter_mut() {
            // Set timestamp from block data
            if swap.unix_ts == 0 {
                swap.unix_ts = block_timestamps.get(&swap.block_number).copied().unwrap_or(0);
            }
            
            // Collect token addresses for decimals lookup (we'll fetch them dynamically)
            if let Some(token0) = swap.token0 {
                unique_token_addresses.insert(token0);
            }
            if let Some(token1) = swap.token1 {
                unique_token_addresses.insert(token1);
            }
            
            // Enrich with transaction receipt data
            if let Some(receipt) = transaction_receipts.get(&swap.tx_hash) {
                // Set gas_used directly on the swap for the EnrichedSwapEvent
                swap.gas_used = receipt.gas_used.map(|g| U256::from(g.as_u64()));
                
                swap.transaction = Some(TransactionData {
                    hash: swap.tx_hash,
                    from: Some(receipt.from),
                    to: receipt.to,
                    gas: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                    gas_price: receipt.effective_gas_price,
                    gas_used: receipt.gas_used.map(|g| U256::from(g.as_u64())),
                    value: Some(U256::zero()), // Swap transactions typically have 0 value
                    block_number: Some(U256::from(swap.block_number)),
                    block_hash: receipt.block_hash,
                    max_fee_per_gas: None, // TransactionReceipt doesn't have this field
                    max_priority_fee_per_gas: None, // TransactionReceipt doesn't have this field
                    transaction_type: receipt.transaction_type.map(|t| U256::from(t.as_u64())),
                    cumulative_gas_used: Some(receipt.cumulative_gas_used),
                    chain_id: None, // Will be set from provider if needed
                    base_fee_per_gas: None, // TransactionReceipt doesn't have this field
                    priority_fee_per_gas: None, // TransactionReceipt doesn't have this field
                });
            }
            
            // Calculate V3 reserves if we have the data
            if let SwapData::UniswapV3 { sqrt_price_x96, liquidity, .. } = &swap.data {
                if let Ok((reserve0, reserve1)) = calculate_v3_reserves_from_liquidity_and_sqrt_price(
                    *liquidity,
                    *sqrt_price_x96,
                ) {
                    swap.reserve0 = Some(reserve0);
                    swap.reserve1 = Some(reserve1);
                }

                // Calculate V3 prices with decimal scaling
                if let (Some(d0), Some(d1)) = (swap.token0_decimals, swap.token1_decimals) {
                    if let Some(price_t0_in_t1) = price_v3_q96(*sqrt_price_x96, d0, d1) {
                        swap.price_t0_in_t1 = Some(price_t0_in_t1);
                        if !price_t0_in_t1.is_zero() {
                            swap.price_t1_in_t0 = Some(Decimal::ONE / price_t0_in_t1);
                        }
                    }
                }
            }
            
            // Calculate V2 prices from pre-trade reserves for accurate backtesting
            // Note: V2 liquidity calculation would require token prices which we don't fetch here
            // For now, V2 swaps will have liquidity = None (only V3 swaps have liquidity data)
            if let SwapData::UniswapV2 { .. } = &swap.data {
                if let Some((reserve0, reserve1)) = v2_pool_reserves.get(&(swap.pool_address, swap.block_number)) {
                    // Set the pre-trade reserves
                    swap.reserve0 = Some(*reserve0);
                    swap.reserve1 = Some(*reserve1);
                    
                    // Calculate pre-trade prices from reserves using safe arithmetic from dex_math
                    if !reserve0.is_zero() && !reserve1.is_zero() {
                        // Use dex_math's mul_div for safe price calculation
                        let _precision = U256::from(10).pow(U256::from(18)); // 18 decimal precision
                        
                        // price_t0_in_t1 = reserve1 / reserve0 (1e18 scaled)
                        if let Ok(p01_q) = dex_math::mul_div(*reserve1, U256::exp10(18), *reserve0) {
                            swap.price_t0_in_t1 = Decimal::from_str(&p01_q.to_string()).ok()
                                .and_then(|d| Decimal::from_u128(1_000_000_000_000_000_000u128).map(|e| d / e));
                        }

                        // price_t1_in_t0 = reserve0 / reserve1 (1e18 scaled)
                        if let Ok(p10_q) = dex_math::mul_div(*reserve0, U256::exp10(18), *reserve1) {
                            swap.price_t1_in_t0 = Decimal::from_str(&p10_q.to_string()).ok()
                                .and_then(|d| Decimal::from_u128(1_000_000_000_000_000_000u128).map(|e| d / e));
                        }
                    }
                }
            }

        }
        
        // Batch fetch token decimals for all unique tokens
        let mut token_decimals_batch = Vec::new();
        for token_address in unique_token_addresses {
            if !self.token_decimals_cache.read().await.contains_key(&token_address) {
                token_decimals_batch.push(token_address);
            }
        }
        
        // Fetch decimals in batches to avoid overwhelming the RPC
        const BATCH_SIZE: usize = 50;
        for chunk in token_decimals_batch.chunks(BATCH_SIZE) {
            for &token_address in chunk {
                match self.rate_limiter.execute_rpc_call("decimals", || async {
                    let contract = self.contract_abis.create_erc20_contract(token_address, self.provider.clone());
                    contract.method::<(), u8>("decimals", ())
                        .map_err(|e| BlockchainError::Provider(format!("Failed to create method call: {}", e)))?
                        .call()
                        .await
                        .map_err(|e| BlockchainError::Provider(format!("Failed to call decimals method: {}", e)))
                }).await {
                    Ok(decimals) => {
                        self.token_decimals_cache.write().await.insert(token_address, decimals);
                    }
                    Err(e) => {
                        warn!("Failed to fetch decimals for token {}: {}", token_address, e);
                        // Use default decimals for unknown tokens
                        self.token_decimals_cache.write().await.insert(token_address, 18);
                    }
                }
            }
        }
        
        // Apply token decimals to swaps
        for swap in swaps.iter_mut() {
            if let (Some(token0), Some(token1)) = (swap.token0, swap.token1) {
                swap.token0_decimals = self.token_decimals_cache.read().await.get(&token0).copied();
                swap.token1_decimals = self.token_decimals_cache.read().await.get(&token1).copied();
            }
            
            // Populate V3 amount0/amount1 from SwapData
            if let SwapData::UniswapV3 { amount0, amount1, .. } = &swap.data {
                swap.v3_amount0 = Some(*amount0);
                swap.v3_amount1 = Some(*amount1);
            }

            // Backfill Uniswap V3 token assignments if missing
            if matches!(swap.protocol, DexProtocol::UniswapV3) {
                // Token0/token1/fee/decimals will be populated dynamically from swap data
                // No pre-loaded pool metadata available

                // If token_in/token_out are zero-address, infer from signed V3 amounts and token0/token1
                if (swap.token_in == Address::zero() || swap.token_out == Address::zero())
                    && swap.token0.is_some()
                    && swap.token1.is_some()
                {
                    if let (Some(a0), Some(a1)) = (swap.v3_amount0, swap.v3_amount1) {
                        let (t0, t1) = match (swap.token0, swap.token1) {
                            (Some(a), Some(b)) => (a, b),
                            _ => continue,
                        };
                        if a0.is_positive() && a1.is_negative() {
                            swap.token_in = t0;
                            swap.token_out = t1;
                            swap.amount_in = U256::from(a0.as_u128());
                            swap.amount_out = U256::from(a1.abs().as_u128());
                        } else if a1.is_positive() && a0.is_negative() {
                            swap.token_in = t1;
                            swap.token_out = t0;
                            swap.amount_in = U256::from(a1.as_u128());
                            swap.amount_out = U256::from(a0.abs().as_u128());
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Fetch ALL swaps in the given block range without any pool filtering
    pub async fn fetch_all_swaps_batch(
        &self,
        from_block: u64,
        to_block: u64,
        chain: &str,
        _rpc_url: &str,
    ) -> Result<Vec<SwapEvent>> {
        let mut all_swaps = Vec::new();

        info!("Fetching ALL swaps for blocks {}-{} (no pool filtering)", from_block, to_block);

        // Create filter with relevant topics but NO address filtering
        let filter = Filter::new()
            .from_block(from_block)
            .to_block(to_block)
            .topic0(vec![
                self.uniswap_v2_swap_topic,
                self.uniswap_v3_swap_topic,
                self.curve_token_exchange_topic,
                self.balancer_v2_swap_topic,
            ]);

        match self.rate_limiter.execute_rpc_call("get_logs_all", || {
            self.provider.get_logs(&filter).map_err(|e| BlockchainError::Provider(format!("Failed to get logs: {}", e)))
        }).await {
            Ok(logs) => {
                info!("Received {} logs for blocks {}-{}", logs.len(), from_block, to_block);
                for log in logs {
                                            match self.decode_swap_log(&log, chain).await {
                            Ok(swap_event) => {
                                all_swaps.push(swap_event);
                        }
                        Err(e) => {
                            warn!("Failed to decode swap log at {:?}: {}", log.address, e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch logs for blocks {}-{}: {}", from_block, to_block, e);
                return Err(e.into());
            }
        }

        info!("Total swaps found for blocks {}-{}: {}", from_block, to_block, all_swaps.len());
        Ok(all_swaps)
    }

    pub async fn fetch_swaps_batch_with_addresses_and_transactions(
        &self,
        from_block: u64,
        to_block: u64,
        chain: &str,
        _rpc_url: &str,
        pool_addresses: &[Address],
    ) -> Result<Vec<SwapEvent>> {
        let mut all_swaps = Vec::new();
        
        // Split large address lists to avoid RPC limits
        const MAX_ADDRESSES_PER_REQUEST: usize = 1000;
        let address_chunks: Vec<&[Address]> = pool_addresses
            .chunks(MAX_ADDRESSES_PER_REQUEST)
            .collect();
        
        info!("Fetching swaps for blocks {}-{} with {} address chunks", from_block, to_block, address_chunks.len());
        
        for (chunk_index, address_chunk) in address_chunks.iter().enumerate() {
            info!("Processing address chunk {}/{} ({} addresses)", chunk_index + 1, address_chunks.len(), address_chunk.len());
            
            // Create efficient filter with relevant topics
            let filter = Filter::new()
                .from_block(from_block)
                .to_block(to_block)
                .topic0(vec![
                    self.uniswap_v2_swap_topic,
                    self.uniswap_v3_swap_topic,
                    self.curve_token_exchange_topic,
                    self.balancer_v2_swap_topic,
                ])
                .address(address_chunk.to_vec());
            
            match self.rate_limiter.execute_rpc_call("get_logs", || {
                self.provider.get_logs(&filter).map_err(|e| BlockchainError::Provider(format!("Failed to get logs: {}", e)))
            }).await {
                Ok(logs) => {
                    info!("Received {} logs for address chunk {}", logs.len(), chunk_index + 1);
                    for log in logs {
                        match self.decode_swap_log(&log, chain).await {
                            Ok(swap_event) => {
                                // Log protocol type for debugging
                                if matches!(swap_event.protocol, DexProtocol::UniswapV3) {
                                    trace!("Successfully decoded V3 swap at block {} pool {:?}", 
                                          swap_event.block_number, swap_event.pool_address);
                                }
                                all_swaps.push(swap_event);
                            }
                            Err(e) => {
                                // Log the failure to understand what's happening
                                warn!("Failed to decode swap log at {:?}: {}", 
                                      log.address, e);
                                
                                // Log topic0 to see what event it was
                                if let Some(topic0) = log.topics.get(0) {
                                    debug!("Failed log topic0: {:?}", topic0);
                                    
                                    // Check if it's a V3 swap that failed
                                    if topic0 == &self.uniswap_v3_swap_topic {
                                        error!("V3 SWAP DECODE FAILED! Block: {:?}, Topics: {}, Data len: {}", 
                                              log.block_number, log.topics.len(), log.data.len());
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch logs for address chunk {}: {}", chunk_index, e);
                    // Continue with next chunk instead of failing completely
                }
            }
        }
        
        info!("Total swaps found for blocks {}-{}: {}", from_block, to_block, all_swaps.len());
        Ok(all_swaps)
    }
    
    async fn decode_swap_log(&self, log: &Log, chain: &str) -> Result<SwapEvent> {
        let block_number = log.block_number.ok_or_else(|| anyhow::anyhow!("No block number"))?;
        let transaction_hash = log.transaction_hash.ok_or_else(|| anyhow::anyhow!("No transaction hash"))?;
        
        // Decode based on topic0 (event signature) - this determines the protocol
        let topic0 = log.topics.get(0).ok_or_else(|| anyhow::anyhow!("No topic0 found"))?;
        
        let (data, protocol) = match topic0 {
            // UniswapV2-style Swap event: Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
            // Topics: [signature, sender, to]
            t if t == &self.uniswap_v2_swap_topic => {
                if log.topics.len() < 3 {
                    return Err(anyhow::anyhow!("Invalid UniswapV2 swap log: insufficient topics"));
                }
                
                let sender = h256_topic_addr(&log.topics[1]);
                let to = h256_topic_addr(&log.topics[2]);
                
                // Data contains 4 U256 values: amount0In, amount1In, amount0Out, amount1Out
                let amount0_in = U256::from_big_endian(&log.data[0..32]);
                let amount1_in = U256::from_big_endian(&log.data[32..64]);
                let amount0_out = U256::from_big_endian(&log.data[64..96]);
                let amount1_out = U256::from_big_endian(&log.data[96..128]);
                
                (SwapData::UniswapV2 {
                    sender,
                    to,
                    amount0_in,
                    amount1_in,
                    amount0_out,
                    amount1_out,
                }, DexProtocol::UniswapV2)
            },
            
            // This code is not commented out; it is active and required for decoding UniswapV3 swap events.
            // The block below matches logs with the UniswapV3 swap event signature and decodes the event data.
            t if t == &self.uniswap_v3_swap_topic => {
                if log.topics.len() < 3 {
                    return Err(anyhow::anyhow!("Invalid UniswapV3 swap log: insufficient topics"));
                }
                
                let sender = h256_topic_addr(&log.topics[1]);
                let recipient = h256_topic_addr(&log.topics[2]);
                
                // V3 Swap event data layout (5 words of 32 bytes each = 160 bytes total):
                // [0..32]: amount0 (int256)
                // [32..64]: amount1 (int256)
                // [64..96]: sqrtPriceX96 (uint160, right-aligned in 32 bytes)
                // [96..128]: liquidity (uint128, right-aligned in 32 bytes)
                // [128..160]: tick (int24, right-aligned in 32 bytes)
                
                if log.data.len() < 160 {
                    return Err(anyhow::anyhow!("Invalid UniswapV3 swap log: insufficient data"));
                }
                
                let amount0 = decode_int256_to_i256(&log.data[0..32])?;
                let amount1 = decode_int256_to_i256(&log.data[32..64])?;
                let sqrt_price_x96 = U256::from_big_endian(&log.data[64..96]);
                let liquidity = U256::from_big_endian(&log.data[96..128]).as_u128();
                let tick = I256::from_raw(U256::from_big_endian(&log.data[128..160])).as_i32();
                
                (SwapData::UniswapV3 {
                    sender,
                    recipient,
                    amount0,
                    amount1,
                    sqrt_price_x96,
                    liquidity,
                    tick,
                }, DexProtocol::UniswapV3)
            },
            
            // Curve TokenExchange event: TokenExchange(address indexed buyer, int128 sold_id, uint256 tokens_sold, int128 bought_id, uint256 tokens_bought)
            // Topics: [signature, buyer]
            t if t == &self.curve_token_exchange_topic => {
                if log.topics.len() < 2 {
                    return Err(anyhow::anyhow!("Invalid Curve swap log: insufficient topics"));
                }
                
                let buyer = h256_topic_addr(&log.topics[1]);
                
                // sold_id and bought_id are int128, decode them directly
                let sold_id = decode_int256_to_i256(&log.data[0..32])?.as_i128();
                let tokens_sold = U256::from_big_endian(&log.data[32..64]);
                let bought_id = decode_int256_to_i256(&log.data[64..96])?.as_i128();
                let tokens_bought = U256::from_big_endian(&log.data[96..128]);
                
                (SwapData::Curve {
                    buyer,
                    sold_id,
                    tokens_sold,
                    bought_id,
                    tokens_bought,
                }, DexProtocol::Curve)
            },
            
            // Balancer Swap event: Swap(bytes32 indexed poolId, address indexed tokenIn, address indexed tokenOut, uint256 amountIn, uint256 amountOut)
            // Topics: [signature, poolId, tokenIn, tokenOut]
            t if t == &self.balancer_v2_swap_topic => {
                if log.topics.len() < 4 {
                    return Err(anyhow::anyhow!("Invalid Balancer swap log: insufficient topics"));
                }
                
                let pool_id = H256::from(log.topics[1]);
                let token_in = h256_topic_addr(&log.topics[2]);
                let token_out = h256_topic_addr(&log.topics[3]);
                let amount_in = U256::from_big_endian(&log.data[0..32]);
                let amount_out = U256::from_big_endian(&log.data[32..64]);
                
                (SwapData::Balancer {
                    pool_id,
                    token_in,
                    token_out,
                    amount_in,
                    amount_out,
                }, DexProtocol::Balancer)
            },
            
            _ => {
                // Unknown event signature - return basic event
                return Err(anyhow::anyhow!("Unknown event signature: {:?}", topic0));
            }
        };
        
        // For backtesting, we need to fetch token addresses from pool contracts
        // This is critical for the swap loader to work properly
        let (token0, token1) = self.fetch_pool_tokens(&log.address, block_number.as_u64()).await;
        
        // Extract swap direction and amounts based on protocol
        let (token_in, token_out, amount_in, amount_out, sender, recipient) = match &data {
            SwapData::UniswapV2 { sender, to, amount0_in, amount1_in, amount0_out, amount1_out } => {
                if *amount0_in > U256::zero() && *amount1_out > U256::zero() {
                    (token0.unwrap_or(Address::zero()), token1.unwrap_or(Address::zero()), 
                     *amount0_in, *amount1_out, *sender, *to)
                } else if *amount1_in > U256::zero() && *amount0_out > U256::zero() {
                    (token1.unwrap_or(Address::zero()), token0.unwrap_or(Address::zero()), 
                     *amount1_in, *amount0_out, *sender, *to)
                } else {
                    (Address::zero(), Address::zero(), U256::zero(), U256::zero(), *sender, *to)
                }
            },
            SwapData::UniswapV3 { sender, recipient, amount0, amount1, .. } => {
                if amount0.is_positive() && amount1.is_negative() {
                    (token0.unwrap_or(Address::zero()), token1.unwrap_or(Address::zero()),
                     U256::from(amount0.as_u128()), U256::from(amount1.abs().as_u128()), 
                     *sender, *recipient)
                } else if amount1.is_positive() && amount0.is_negative() {
                    (token1.unwrap_or(Address::zero()), token0.unwrap_or(Address::zero()),
                     U256::from(amount1.as_u128()), U256::from(amount0.abs().as_u128()),
                     *sender, *recipient)
                } else {
                    (Address::zero(), Address::zero(), U256::zero(), U256::zero(), *sender, *recipient)
                }
            },
            SwapData::Balancer { token_in, token_out, amount_in, amount_out, .. } => {
                (*token_in, *token_out, *amount_in, *amount_out, Address::zero(), Address::zero())
            },
            SwapData::Curve { buyer, tokens_sold, tokens_bought, .. } => {
                // For Curve, we need to map coin indices to addresses
                // This is a simplified version - in production you'd look up the actual token addresses
                let token_in = token0.unwrap_or(Address::zero());
                let token_out = token1.unwrap_or(Address::zero());
                (token_in, token_out, *tokens_sold, *tokens_bought, *buyer, *buyer)
            },
        };
        
        // Extract V3-specific values
        let (v3_amount0, v3_amount1, sqrt_price_x96, liquidity, tick) = match &data {
            SwapData::UniswapV3 { amount0, amount1, sqrt_price_x96, liquidity, tick, .. } => {
                (Some(*amount0), Some(*amount1), Some(*sqrt_price_x96), Some(U256::from(*liquidity)), Some(*tick))
            },
            _ => (None, None, None, None, None),
        };
        
        // Extract Curve-specific values
        let (buyer, sold_id, tokens_sold, bought_id, tokens_bought) = match &data {
            SwapData::Curve { buyer, sold_id, tokens_sold, bought_id, tokens_bought } => {
                (Some(*buyer), Some(*sold_id), Some(*tokens_sold), Some(*bought_id), Some(*tokens_bought))
            },
            _ => (None, None, None, None, None),
        };
        
        // Extract Balancer pool_id
        let pool_id = match &data {
            SwapData::Balancer { pool_id, .. } => Some(*pool_id),
            _ => None,
        };
        
        Ok(SwapEvent {
            chain_name: chain.to_string(),
            protocol,
            tx_hash: transaction_hash,
            block_number: block_number.as_u64(),
            log_index: log.log_index.unwrap_or_default(),
            pool_address: log.address,
            unix_ts: 0, // Will be populated during enrichment
            token_in,
            token_out,
            amount_in,
            amount_out,
            sender,
            recipient,
            gas_used: None, // Will be populated during enrichment
            effective_gas_price: None, // Will be populated during enrichment
            data,
            token0,
            token1,
            price_t0_in_t1: None, // Will be populated during enrichment
            price_t1_in_t0: None, // Will be populated during enrichment
            reserve0: None, // Will be populated during enrichment
            reserve1: None, // Will be populated during enrichment
            transaction: None,
            transaction_metadata: None,
            pool_id,
            buyer,
            sold_id,
            tokens_sold,
            bought_id,
            tokens_bought,
            sqrt_price_x96,
            liquidity,
            tick,
            token0_decimals: None, // Will be populated during enrichment
            token1_decimals: None, // Will be populated during enrichment
            fee_tier: None, // Will be populated during enrichment
            protocol_type: Some(match protocol {
                DexProtocol::UniswapV2 => rust::types::ProtocolType::UniswapV2,
                DexProtocol::UniswapV3 => rust::types::ProtocolType::UniswapV3,
                DexProtocol::UniswapV4 => rust::types::ProtocolType::UniswapV4,
                DexProtocol::SushiSwap => rust::types::ProtocolType::SushiSwap,
                DexProtocol::Curve => rust::types::ProtocolType::Curve,
                DexProtocol::Balancer => rust::types::ProtocolType::Balancer,
                DexProtocol::Other(id) => rust::types::ProtocolType::Other(format!("Other({})", id)),
                DexProtocol::Unknown => rust::types::ProtocolType::Other("Unknown".to_string()),
                _ => rust::types::ProtocolType::Other("Other".to_string()),
            }),
            v3_amount0,
            v3_amount1,
            v3_tick_data: None,
        })
    }
}

// Optimized main ingestion loop
pub async fn fetch_swaps_with_optimal_batching(
    fetcher: &SwapFetcher,
    from_block: u64,
    to_block: u64,
    chain: &str,
    pool_addresses: &[Address],
) -> Result<Vec<SwapEvent>> {
    let mut all_swaps = Vec::new();
    
    // Use larger address chunks with batch RPC
    const MAX_ADDRESSES: usize = 2000; // Can be larger with batch requests
    const PARALLEL_CHUNKS: usize = 4; // Process multiple chunks in parallel
    
    let address_chunks: Vec<Vec<Address>> = pool_addresses
        .chunks(MAX_ADDRESSES)
        .map(|c| c.to_vec())
        .collect();
    
    // Process chunks in parallel
    let chunk_futures = address_chunks
        .chunks(PARALLEL_CHUNKS)
        .map(|chunk_group| {
            let fetcher = fetcher.clone(); // Assuming SwapFetcher is cloneable
            let chain = chain.to_string();
            
            async move {
                let mut chunk_swaps = Vec::new();
                for addresses in chunk_group {
                    let filter = Filter::new()
                        .from_block(from_block)
                        .to_block(to_block)
                        .topic0(vec![
                            fetcher.uniswap_v2_swap_topic,
                            fetcher.uniswap_v3_swap_topic,
                            fetcher.curve_token_exchange_topic,
                            fetcher.balancer_v2_swap_topic,
                        ])
                        .address(addresses.clone());
                    
                    match fetcher.rate_limiter.execute_rpc_call("get_logs", || {
                        let filter_clone = filter.clone();
                        let provider_clone = fetcher.provider.clone();
                        async move {
                            provider_clone.get_logs(&filter_clone)
                                .await
                                .map_err(|e| BlockchainError::Provider(format!("Failed to get logs: {}", e)))
                        }
                    }).await {
                        Ok(logs) => {
                            for log in logs {
                                if let Ok(swap) = fetcher.decode_swap_log(&log, &chain).await {
                                    chunk_swaps.push(swap);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to fetch logs: {}", e);
                        }
                    }
                }
                chunk_swaps
            }
        });
    
    let results = join_all(chunk_futures).await;
    for chunk_swaps in results {
        all_swaps.extend(chunk_swaps);
    }
    
    // Use optimized enrichment
    fetcher.enrich_swaps_optimized(&mut all_swaps).await?;
    
    Ok(all_swaps)
}

// Database optimization: Use prepared statements and connection pooling
pub struct OptimizedDbWriter {
    pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>,
    #[allow(dead_code)]
    prepared_statements: HashMap<String, tokio_postgres::Statement>,
}

impl OptimizedDbWriter {
    pub async fn new(database_url: &str) -> Result<Self> {
        let config = database_url.parse::<tokio_postgres::Config>()?;
        let manager = bb8_postgres::PostgresConnectionManager::new(config, NoTls);
        let pool = bb8::Pool::builder()
            .max_size(10)
            .build(manager)
            .await?;
        
        // Prepared statements could be added here for frequently used queries
        // For now, we're just using bulk COPY operations which don't need prepared statements
        let prepared_statements = HashMap::new();
        
        Ok(Self {
            pool: Arc::new(pool),
            prepared_statements,
        })
    }
    
    pub async fn bulk_insert_swaps(&self, swaps: &[SwapEvent]) -> Result<()> {
        if swaps.is_empty() {
            return Ok(());
        }
        
        info!("Bulk saving {} swaps to database", swaps.len());
        
        let mut conn = self.pool.get().await
            .map_err(|e| anyhow!("Failed to get connection from pool: {}", e))?;
        
        // Convert to database connection type
        let db_conn = conn.deref_mut();
        
        // Prepare the INSERT statement with ALL columns from swaps table
        let sql = "INSERT INTO swaps (
            network_id, chain_name, pool_address, transaction_hash, log_index, block_number, unix_ts, protocol_type,
            token0_address, token1_address, token0_decimals, token1_decimals, fee_tier,
            amount0_in, amount1_in, amount0_out, amount1_out,
            price_t0_in_t1, price_t1_in_t0,
            token0_reserve, token1_reserve,
            sqrt_price_x96, liquidity, tick,
            v3_sender, v3_recipient, v3_amount0, v3_amount1,
            pool_id_balancer, token_in, token_out, amount_in, amount_out,
            buyer, sold_id, tokens_sold, bought_id, tokens_bought,
            gas_used, gas_price, tx_origin, tx_to, tx_value, tx_data,
            tx_from, tx_gas, tx_gas_price, tx_gas_used, tx_block_number,
            tx_max_fee_per_gas, tx_max_priority_fee_per_gas, tx_transaction_type, tx_chain_id, tx_cumulative_gas_used,
            price_from_in_usd, price_to_in_usd, volume_in_usd, pool_liquidity_usd,
            v3_tick_data,
            pool_id_str, tx_hash, tx_from_address, from_token_amount, to_token_amount,
            price_from_in_currency_token, price_to_in_currency_token, block_timestamp, kind,
            from_token_address, to_token_address, first_seen
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
            $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
            $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73
        ) ON CONFLICT (pool_address, transaction_hash, log_index) DO NOTHING";

        let statement = db_conn.prepare(sql).await?;
        
        let mut success_count = 0;
        let mut error_count = 0;
        
        // Helper to synthesize a reasonable dex_name when pool metadata is missing
        fn synthesize_dex_name(protocol_type_id: i16) -> String {
            match protocol_type_id {
                1 => "uniswap_v2".to_string(),
                2 => "uniswap_v3".to_string(),
                3 => "curve".to_string(),
                4 => "balancer".to_string(),
                5 => "sushiswap".to_string(),
                _ => "other".to_string(),
            }
        }

        for swap in swaps {
            // Get pool data for enrichment
            // No pool data available - working with dynamically discovered pools
            
            // Prepare swap amounts separately using safe arithmetic - as Decimal for PostgreSQL
            let (amount0_in, amount1_in, amount0_out, amount1_out) = match &swap.data {
                SwapData::UniswapV2 { amount0_in, amount1_in, amount0_out, amount1_out, .. } => {
                    (
                        if amount0_in > &U256::zero() { 
                            Decimal::from_str(&amount0_in.to_string()).ok()
                        } else { 
                            Some(Decimal::ZERO) 
                        },
                        if amount1_in > &U256::zero() { 
                            Decimal::from_str(&amount1_in.to_string()).ok()
                        } else { 
                            Some(Decimal::ZERO) 
                        },
                        if amount0_out > &U256::zero() { 
                            Decimal::from_str(&amount0_out.to_string()).ok()
                        } else { 
                            Some(Decimal::ZERO) 
                        },
                        if amount1_out > &U256::zero() { 
                            Decimal::from_str(&amount1_out.to_string()).ok()
                        } else { 
                            Some(Decimal::ZERO) 
                        }
                    )
                },
                SwapData::UniswapV3 { amount0, amount1, .. } => {
                    let amount0_in = if amount0.is_positive() { 
                        Decimal::from_str(&amount0.as_u128().to_string()).ok()
                    } else { 
                        Some(Decimal::ZERO) 
                    };
                    let amount1_in = if amount1.is_positive() { 
                        Decimal::from_str(&amount1.as_u128().to_string()).ok()
                    } else { 
                        Some(Decimal::ZERO) 
                    };
                    let amount0_out = if amount0.is_negative() { 
                        Decimal::from_str(&amount0.abs().as_u128().to_string()).ok()
                    } else { 
                        Some(Decimal::ZERO) 
                    };
                    let amount1_out = if amount1.is_negative() { 
                        Decimal::from_str(&amount1.abs().as_u128().to_string()).ok()
                    } else { 
                        Some(Decimal::ZERO) 
                    };
                    (amount0_in, amount1_in, amount0_out, amount1_out)
                },
                _ => (Some(Decimal::ZERO), Some(Decimal::ZERO), Some(Decimal::ZERO), Some(Decimal::ZERO)),
            };

            let from_token_amount = Decimal::from_str(&swap.amount_in.to_string()).ok();
            let to_token_amount = Decimal::from_str(&swap.amount_out.to_string()).ok();

            let (price_from_in_currency_token, price_to_in_currency_token) = if swap.token0.is_some() && swap.token_in == swap.token0.unwrap() {
                (swap.price_t0_in_t1, swap.price_t1_in_t0)
            } else {
                (swap.price_t1_in_t0, swap.price_t0_in_t1)
            };
            
            // Volume calculation not available without pool data for price information

            // Execute parameterized INSERT statement (like cg_pools.rs does)
            // Determine protocol_type id used for DB
            let protocol_type_id: i16 = get_protocol_type_id(&swap.protocol) as i16;

            // Synthesize dex_name from protocol since no pool metadata available
            let dex_name_param: Option<String> = Some(synthesize_dex_name(protocol_type_id));

            // Serialize v3_tick_data to JSONB
            let v3_tick_data_json = swap.v3_tick_data.as_ref();

            let (from_token_address_str, to_token_address_str) = {
                let token0_address = swap.token0.map(|a| format!("0x{:x}", a)).unwrap_or_default();
                let token1_address = swap.token1.map(|a| format!("0x{:x}", a)).unwrap_or_default();
                (token0_address, token1_address)
            };

            match db_conn.execute(&statement, &[
                // 80 parameters matching all columns in swaps table
                &swap.chain_name,  // network_id
                &swap.chain_name,  // chain_name
                &swap.pool_address.as_bytes(),  // pool_address
                &swap.tx_hash.as_bytes(),  // transaction_hash
                &(swap.log_index.low_u64() as i64),  // log_index
                &(swap.block_number as i64),  // block_number
                &(swap.unix_ts as i64),  // unix_ts
                
                // Pool metadata
                &dex_name_param,  // dex_name
                &protocol_type_id,  // protocol_type

                // Token information
                &swap.token0.map(|a| a.as_bytes().to_vec()),  // token0_address
                &swap.token1.map(|a| a.as_bytes().to_vec()),  // token1_address
                &swap.token0_decimals.map(|d| d as i32),  // token0_decimals
                &swap.token1_decimals.map(|d| d as i32),  // token1_decimals
                &swap.fee_tier.map(|f| f as i32),  // fee_tier

                // V3 specific
                &match &swap.data { SwapData::UniswapV3 { sender, .. } => Some(sender.as_bytes().to_vec()), _ => None },  // v3_sender
                &match &swap.data { SwapData::UniswapV3 { recipient, .. } => Some(recipient.as_bytes().to_vec()), _ => None },  // v3_recipient
                &swap.v3_amount0.and_then(|a| Decimal::from_str(&a.to_string()).ok()),  // v3_amount0
                &swap.v3_amount1.and_then(|a| Decimal::from_str(&a.to_string()).ok()),  // v3_amount1
                
                // Balancer specific
                &match &swap.data { SwapData::Balancer { pool_id, .. } => Some(pool_id.as_bytes().to_vec()), _ => None },  // pool_id_balancer
                &Some(swap.token_in.as_bytes().to_vec()),  // token_in
                &Some(swap.token_out.as_bytes().to_vec()),  // token_out
                &Decimal::from_str(&swap.amount_in.to_string()).ok(),  // amount_in
                &Decimal::from_str(&swap.amount_out.to_string()).ok(),  // amount_out

                // Curve specific
                &swap.buyer.map(|b| b.as_bytes().to_vec()),  // buyer
                &swap.sold_id.map(|v| v as i32),  // sold_id
                &swap.tokens_sold.and_then(|d| Decimal::from_str(&d.to_string()).ok()),  // tokens_sold
                &swap.bought_id.map(|v| v as i32),  // bought_id
                &swap.tokens_bought.and_then(|d| Decimal::from_str(&d.to_string()).ok()),  // tokens_bought

                // Transaction metadata
                &swap.gas_used.map(|g| g.as_u64() as i64),  // gas_used
                &swap.effective_gas_price.and_then(|p| Decimal::from_str(&p.to_string()).ok()),  // gas_price
                &swap.transaction.as_ref().and_then(|t| t.from.map(|a| a.as_bytes().to_vec())),  // tx_origin
                &swap.transaction.as_ref().and_then(|t| t.to.map(|a| a.as_bytes().to_vec())),  // tx_to
                &swap.transaction.as_ref().and_then(|t| t.from.map(|a| a.as_bytes().to_vec())),  // tx_from
                &swap.transaction.as_ref().and_then(|t| t.gas.map(|g| g.as_u64() as i64)),  // tx_gas
                &swap.transaction.as_ref().and_then(|t| t.gas_price.and_then(|p| Decimal::from_str(&p.to_string()).ok())),  // tx_gas_price
                &swap.transaction.as_ref().and_then(|t| t.gas_used.map(|g| g.as_u64() as i64)),  // tx_gas_used
                &swap.transaction.as_ref().and_then(|t| t.value.and_then(|v| Decimal::from_str(&v.to_string()).ok())),  // tx_value
                &swap.transaction.as_ref().and_then(|t| t.block_number.map(|b| b.as_u64() as i64)),  // tx_block_number
                &swap.transaction.as_ref().and_then(|t| t.max_fee_per_gas.and_then(|f| Decimal::from_str(&f.to_string()).ok())),  // tx_max_fee_per_gas
                &swap.transaction.as_ref().and_then(|t| t.max_priority_fee_per_gas.and_then(|f| Decimal::from_str(&f.to_string()).ok())),  // tx_max_priority_fee_per_gas
                &swap.transaction.as_ref().and_then(|t| t.transaction_type.map(|t| t.as_u64() as i32)),  // tx_transaction_type
                &swap.transaction.as_ref().and_then(|t| t.chain_id.map(|c| c.as_u64() as i64)),  // tx_chain_id
                &swap.transaction.as_ref().and_then(|t| t.cumulative_gas_used.and_then(|c| Decimal::from_str(&c.to_string()).ok())),  // tx_cumulative_gas_used
                
                // USD values
                &None::<Decimal>,  // price_from_in_usd
                &None::<Decimal>,  // price_to_in_usd
                &None::<Decimal>,  // volume_in_usd
                &None::<Decimal>,  // pool_liquidity_usd

                // V3 Tick Data (combined in swaps table)
                &v3_tick_data_json,  // v3_tick_data

                // Legacy fields
                &Some(format!("0x{:x}", swap.pool_address)),  // pool_id_str
                &Some(format!("0x{:x}", swap.tx_hash)),  // tx_hash
                &swap.token0_decimals.map(|d| d as i32),  // token0_decimals
                &swap.token1_decimals.map(|d| d as i32),  // token1_decimals
                &swap.fee_tier.map(|f| f as i32),  // fee_tier
                &swap.reserve0.map(|r| r.to_string()),  // token0_reserve
                &swap.reserve1.map(|r| r.to_string()),  // token1_reserve
                &swap.sqrt_price_x96.map(|p| p.to_string()),  // sqrt_price_x96
                &swap.liquidity.map(|l| l.to_string()),  // liquidity
                &swap.tick.map(|t| t.to_string()),  // tick
                &swap.v3_amount0.map(|a| a.to_string()),  // v3_amount0
                &swap.v3_amount1.map(|a| a.to_string()),  // v3_amount1
                &swap.transaction.as_ref().and_then(|t| t.from.map(|a| format!("0x{:x}", a))),  // tx_from_address
                &from_token_amount,  // from_token_amount
                &to_token_amount,  // to_token_amount
                &price_from_in_currency_token,  // price_from_in_currency_token
                &price_to_in_currency_token,  // price_to_in_currency_token
                &Some(swap.unix_ts as i64),  // block_timestamp
                &Some("swap".to_string()),  // kind
                &Some(format!("0x{:x}", swap.token_in)),  // from_token_address
                &Some(format!("0x{:x}", swap.token_out)),  // to_token_address
                &Some(swap.unix_ts as i64),  // first_seen
            ]).await {
                Ok(rows_affected) => {
                    success_count += 1;
                    if success_count <= 3 {  // Log first few for debugging
                        debug!("Inserted swap {}: {} (rows affected: {})", success_count, 
                            format!("0x{}", hex::encode(swap.tx_hash.as_bytes())), rows_affected);
                    }
                }
                Err(e) => {
                    error_count += 1;
                    error!("Failed to insert swap: {} - Error: {}",
                        format!("0x{}", hex::encode(swap.tx_hash.as_bytes())), e);

                    // Debug: Log detailed parameter information on first error
                    if error_count == 1 {
                        error!("=== DEBUGGING INSERT FAILURE ===");
                        error!("SQL: {}", sql);
                        error!("Swap details:");
                        error!("  chain_name: {}", swap.chain_name);
                        error!("  pool_address: 0x{}", hex::encode(swap.pool_address.as_bytes()));
                        error!("  tx_hash: 0x{}", hex::encode(swap.tx_hash.as_bytes()));
                        error!("  log_index: {}", swap.log_index.low_u64());
                        error!("  block_number: {}", swap.block_number);
                        error!("  unix_ts: {}", swap.unix_ts);
                        error!("  protocol: {:?}", swap.protocol);

                        // Log parameter values that will be passed
                        let params = &[
                            &swap.chain_name as &(dyn tokio_postgres::types::ToSql + Sync),
                            &swap.chain_name,
                            &swap.pool_address.as_bytes().to_vec(),
                            &swap.tx_hash.as_bytes().to_vec(),
                            &(swap.log_index.low_u64() as i64),
                            &(swap.block_number as i64),
                            &(swap.unix_ts as i64),
                            &dex_name_param,
                            &protocol_type_id,
                        ];
                        error!("First 10 parameters prepared successfully");
                        error!("=== END DEBUGGING INSERT FAILURE ===");
                    }

                    if error_count > 10 {
                        error!("Too many consecutive insert errors ({}), aborting batch", error_count);
                        return Err(anyhow!("Too many insert errors: {}", e));
                    }
                }
            }
        }
        
        info!("Successfully bulk inserted {} swaps, {} failed", success_count, error_count);
        
        if error_count > 0 {
            warn!("Some swaps failed to insert: {} out of {}", error_count, swaps.len());
        }
        
        Ok(())
    }



}



async fn load_backtest_config() -> Result<BacktestConfig> {
    let config_path = "config/backtest.json";
    let config_content = tokio::fs::read_to_string(config_path).await
        .with_context(|| format!("Failed to read config file: {}", config_path))?;
    
    let config: BacktestConfig = serde_json::from_str(&config_content)
        .with_context(|| format!("Failed to parse config file: {}", config_path))?;
    
    Ok(config)
}

async fn load_abi_parse_config() -> Result<AbiParseConfig> {
    let config_path = "config/abi_parse.json";
    let content = tokio::fs::read_to_string(config_path).await
        .with_context(|| format!("Failed to read ABI config at {}", config_path))?;
    
    match serde_json::from_str::<AbiParseConfig>(&content) {
        Ok(config) => Ok(config),
        Err(e) => {
            error!("JSON parsing error in {}: {}", config_path, e);
            Err(anyhow::anyhow!("Failed to parse abi_parse.json: {}", e))
        }
    }
}

async fn connect_db() -> Result<tokio_postgres::Client> {
    let database_url = env::var("DATABASE_URL").or_else(|_| -> Result<String, anyhow::Error> {
        // Build URL from individual components if DATABASE_URL is not set
        Ok(format!(
            "host={} port={} user={} password={} dbname={}",
            env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string()),
            env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string()),
            env::var("DB_USER").context("DB_USER environment variable must be set")?,
            env::var("DB_PASSWORD").context("DB_PASSWORD environment variable must be set")?,
            env::var("DB_NAME").context("DB_NAME environment variable must be set")?
        ))
    })?;
    
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await
        .context("Failed to connect to database")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres connection error: {}", e);
        }
    });
    Ok(client)
}

async fn setup_database(client: &tokio_postgres::Client) -> Result<()> {
    // Checkpoint table removed - using only backtest.json ranges
    
    // Drop and recreate the swaps table with the correct schema
    info!("Dropping existing tables to recreate with correct schema");
    client.execute("DROP TABLE IF EXISTS swaps CASCADE", &[]).await?;
    
    info!("Creating swaps table with production-grade schema for backtesting");
    client
        .batch_execute(
            "CREATE TABLE swaps (
                -- Core identifiers
                network_id TEXT NOT NULL,
                chain_name TEXT NOT NULL,
                pool_address BYTEA NOT NULL,
                transaction_hash BYTEA NOT NULL,
                log_index BIGINT NOT NULL,
                block_number BIGINT NOT NULL,
                unix_ts BIGINT,

                -- Pool metadata
                protocol_type SMALLINT NOT NULL,

                -- Swap amounts (V2 style)
                amount0_in NUMERIC,
                amount1_in NUMERIC,
                amount0_out NUMERIC,
                amount1_out NUMERIC,

                -- V3 swap specifics
                v3_sender BYTEA,
                v3_recipient BYTEA,
                v3_amount0 NUMERIC,
                v3_amount1 NUMERIC,
                v3_tick_data NUMERIC,

                -- Balancer specific fields
                pool_id_balancer BYTEA,
                token_in BYTEA,
                token_out BYTEA,
                amount_in NUMERIC,
                amount_out NUMERIC,

                -- Curve specific fields
                buyer BYTEA,
                sold_id INTEGER,
                tokens_sold NUMERIC,
                bought_id INTEGER,
                tokens_bought NUMERIC,

                -- Transaction metadata
                gas_used BIGINT,
                gas_price NUMERIC,
                tx_origin BYTEA,
                tx_to BYTEA,
                tx_from BYTEA,
                tx_gas BIGINT,
                tx_gas_price NUMERIC,
                tx_gas_used BIGINT,
                tx_value NUMERIC,
                tx_block_number BIGINT,
                tx_max_fee_per_gas NUMERIC,
                tx_max_priority_fee_per_gas NUMERIC,
                tx_transaction_type INTEGER,
                tx_chain_id BIGINT,
                tx_cumulative_gas_used NUMERIC,
                
                -- Legacy/compatibility fields
                tx_hash TEXT,

                -- Metadata
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Primary key
                PRIMARY KEY (pool_address, transaction_hash, log_index),

                -- Unique constraint to prevent duplicates
                UNIQUE (network_id, block_number, log_index, pool_address)
            )"
        )
        .await
        .context("Failed to create swaps table")?;

    // Create indexes for optimal query performance
    client
        .batch_execute(
            "-- Primary lookup indexes
             CREATE INDEX idx_swaps_network_block ON swaps(network_id, block_number);
             CREATE INDEX idx_swaps_pool_block ON swaps(pool_address, block_number DESC, log_index DESC);
             CREATE INDEX idx_swaps_chain_block ON swaps(chain_name, block_number);
             
             -- Token and pool indexes
             CREATE INDEX idx_swaps_pool_protocol ON swaps(pool_address, protocol_type);
             
             -- Time-based indexes
             CREATE INDEX idx_swaps_unix_ts ON swaps(unix_ts) WHERE unix_ts IS NOT NULL;
             
             -- Transaction indexes
             CREATE INDEX idx_swaps_tx_hash_bytea ON swaps(transaction_hash);
             CREATE INDEX idx_swaps_tx_hash_text ON swaps(tx_hash) WHERE tx_hash IS NOT NULL;"
        )
        .await
        .context("Failed to create indexes")?;

    info!("Database schema setup complete - all data stored in swaps table");
    Ok(())
}

fn main() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async_main())
}

async fn async_main() -> Result<()> {
    // Set up panic handler to log details before exiting
    std::panic::set_hook(Box::new(|panic_info| {
        let location = panic_info.location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown location".to_string());
        
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic payload".to_string()
        };
        
        error!("PANIC at {}: {}", location, message);
        eprintln!("PANIC at {}: {}", location, message);
        eprintln!("Run with RUST_BACKTRACE=1 for a full backtrace");
        
        // Exit with error code
        std::process::exit(1);
    }));
    
    // Initialize tracing subscriber for logging
    // Set hyper logs to warn level to reduce verbosity
    let filter = EnvFilter::from_default_env()
        .add_directive(match "hyper=warn".parse() { Ok(d) => d, Err(_) => Default::default() });
    
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_ansi(false)  // Disable ANSI color codes
        .init();

    info!("Starting high-fidelity swap ingestion engine");
    
    let args = Args::parse();

    info!("Swap ingestion mode - Target chain: {}", args.chain);
    
    // Load configuration
    info!("Loading backtest configuration...");
    let config = load_backtest_config().await?;
    info!("Configuration loaded successfully");
    
    let chain_config = config.chains.get(&args.chain)
        .ok_or_else(|| anyhow::anyhow!("Chain {} not found in configuration", args.chain))?;
    info!("Chain configuration found for: {}", args.chain);
    
    // Initialize rate limiter manager
    info!("Initializing rate limiter manager...");
    let settings = Arc::new(ChainSettings {
        mode: rust::config::ExecutionMode::Live,
        global_rps_limit: 200,  // Increased since batch requests count as 1
        default_chain_rps_limit: 100,  // Increased
        default_max_concurrent_requests: 50,  // Much higher for parallel processing
        rate_limit_burst_size: 20,  // Larger burst
        rate_limit_timeout_secs: 30,
        rate_limit_max_retries: 3,
        rate_limit_initial_backoff_ms: 100,
        rate_limit_backoff_multiplier: 2.0,
        rate_limit_max_backoff_ms: 5000,
        rate_limit_jitter_factor: 0.1,
        batch_size: 100,  // Much larger batches
        min_batch_delay_ms: 50,  // Reduced delay
        max_blocks_per_query: Some(2000),  // Increased
        log_fetch_concurrency: Some(10),  // More concurrent fetches
    });
    let rate_limiter_manager = GlobalRateLimiterManager::new(settings);
    info!("Rate limiter manager initialized");
    
    // Initialize provider
    info!("Initializing provider with URL: {}", chain_config.backtest_archival_rpc_url);
    let provider = Provider::<Http>::try_from(&chain_config.backtest_archival_rpc_url)?;
    let provider = Arc::new(provider);
    info!("Provider initialized successfully");
    
    // Prepare Postgres connection
    info!("Connecting to database...");
    let client = connect_db().await?;
    info!("Database connection established");
        
    // Setup database schema
    info!("Setting up database schema...");
    setup_database(&client).await?;
    info!("Database schema setup complete");
    
    // Create optimized DB writer
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        let host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string());
        let user = env::var("DB_USER").unwrap_or_else(|_| "postgres".to_string());
        let pass = env::var("DB_PASSWORD").unwrap_or_default();
        let name = env::var("DB_NAME").unwrap_or_else(|_| "arbitrage".to_string());
        format!("host={} port={} user={} password={} dbname={}", host, port, user, pass, name)
    });
    let db_writer = OptimizedDbWriter::new(&database_url).await?;
    info!("Optimized DB writer initialized");
    
    println!("Initializing universal swap ingestion for chain: {} (listening to ALL swaps)...", args.chain);
    info!("Creating SwapFetcher...");
    let fetcher = SwapFetcher::new_with_provider(provider.clone(), &args.chain, &rate_limiter_manager, &client, &args.config_dir).await?;
    info!("SwapFetcher initialized successfully");

    // --- Main Ingestion Loop with Resume Logic ---
    // Always start from the backtest config start block
    let mut current_block = chain_config.backtest_start_block;
    let mut total_swaps_processed = 0usize;
    info!("Starting from block {} as configured in backtest.json", current_block);

    // Use the end block from backtest config instead of latest block
    let target_end_block = chain_config.backtest_end_block;
    let latest_block = provider.get_block_number().await?.as_u64();
    let end_block = target_end_block.min(latest_block); // Don't go beyond chain tip

    const CHUNK_SIZE: u64 = 200; // Increased for better performance with batch processing
    const SAVE_INTERVAL: u64 = 1; // Save every chunk for immediate feedback

    info!("Ingesting from block {} to {} for chain {} (backtest config: {} to {})",
          current_block, end_block, args.chain,
          chain_config.backtest_start_block, chain_config.backtest_end_block);
    info!("Listening to ALL swaps on all pools - no address filtering applied");

    let mut total_blocks_processed = 0;
    let mut chunk_count = 0;
    let mut swaps_buffer = Vec::new();

    while current_block <= end_block {
        let chunk_start = Instant::now();
        let to_block = (current_block + CHUNK_SIZE - 1).min(end_block);
        let batch_blocks = to_block - current_block + 1;
        
        info!("Processing block range: {} - {} ({} blocks, chunk {})", current_block, to_block, batch_blocks, chunk_count + 1);

        // Use retry logic for fetching and enriching swaps with optimized methods
        let swaps = match retry_with_backoff(
            || async {
                fetcher.metrics.record_rpc_call();
                
                // First fetch ALL swaps in the block range
                let mut raw_swaps = fetcher.fetch_all_swaps_batch(
                    current_block,
                    to_block,
                    &args.chain,
                    &chain_config.backtest_archival_rpc_url,
                ).await?;
                
                // Then use optimized enrichment
                fetcher.enrich_swaps_optimized(&mut raw_swaps).await?;
                
                Ok(raw_swaps)
            },
            &format!("Fetch and enrich swaps for blocks {}-{}", current_block, to_block),
            3,  // max_retries
            100,  // initial_delay_ms
            5000,  // max_delay_ms
            2.0,  // backoff_multiplier
        ).await {
            Ok(s) => {
                fetcher.metrics.record_block_processed(batch_blocks);
                s
            },
            Err(e) => {
                error!("Failed to fetch swaps for blocks {}-{} after retries: {}. Skipping chunk.", 
                       current_block, to_block, e);
                fetcher.metrics.record_rpc_error();
                fetcher.metrics.record_skipped_blocks(batch_blocks);
                current_block = to_block + 1;
                continue;
            }
        };

        if !swaps.is_empty() {
            info!("Found {} swaps for blocks {}-{}", swaps.len(), current_block, to_block);
            
            // Add to buffer
            let swaps_count = swaps.len();
            swaps_buffer.extend(swaps);
            total_swaps_processed += swaps_count;
            fetcher.metrics.record_swaps_ingested(swaps_count as u64);
            
            info!("Buffer now contains {} swaps (total processed: {})", swaps_buffer.len(), total_swaps_processed);
        } else {
            info!("No swaps found for blocks {}-{}", current_block, to_block);
        }

        chunk_count += 1;
        total_blocks_processed += batch_blocks;
        
        // Performance metrics
        let chunk_duration = chunk_start.elapsed();
        let blocks_per_second = batch_blocks as f64 / chunk_duration.as_secs_f64();
        info!("Chunk processed in {:.2}s ({:.1} blocks/sec, {:.1} swaps/sec)", 
              chunk_duration.as_secs_f64(), 
              blocks_per_second,
              swaps_buffer.len() as f64 / chunk_duration.as_secs_f64());
        
        // Save to database periodically
        info!("Chunk {} completed. Save interval: {}, Should save: {}", chunk_count, SAVE_INTERVAL, chunk_count % SAVE_INTERVAL == 0);
        if chunk_count % SAVE_INTERVAL == 0 || current_block + CHUNK_SIZE > end_block {
            if !swaps_buffer.is_empty() {
                info!("Saving {} buffered swaps to database...", swaps_buffer.len());
                fetcher.metrics.record_db_write();
                match db_writer.bulk_insert_swaps(&swaps_buffer).await {
                    Ok(_) => {
                        info!("Successfully saved {} swaps to database. Total: {} swaps, {} blocks processed", 
                              swaps_buffer.len(), total_swaps_processed, total_blocks_processed);
                        
                        // Check database count
                        let db_count: i64 = client.query_one(
                            "SELECT COUNT(*) FROM swaps WHERE chain_name = $1", 
                            &[&args.chain]
                        ).await?.get(0);
                        info!("Database now contains {} total swaps for {}", db_count, args.chain);
                        // Checkpoint functionality removed - no intermediate saves
                    },
                    Err(e) => {
                        error!("Failed to save {} swaps to database: {:#}", swaps_buffer.len(), e);
                        fetcher.metrics.record_db_error();
                    }
                }
                swaps_buffer.clear();
            } else {
                info!("No swaps to save (buffer empty)");
            }
        }
        
        current_block = to_block + 1;
        
        // Small delay to avoid overwhelming the RPC
        sleep(Duration::from_millis(100)).await;
    }
    
    // Save any remaining swaps in the buffer
    if !swaps_buffer.is_empty() {
        info!("Saving final {} buffered swaps to database...", swaps_buffer.len());
        match db_writer.bulk_insert_swaps(&swaps_buffer).await {
            Ok(_) => {
                info!("Successfully saved final {} swaps to database", swaps_buffer.len());
            },
            Err(e) => {
                error!("Failed to save final {} swaps to database: {:#}", swaps_buffer.len(), e);
            }
        }
    }
    
    info!("Ingestion completed. Processed {} blocks and {} swaps for chain {}", 
          total_blocks_processed, total_swaps_processed, args.chain);
    
    // Print final metrics summary
    info!("\n{}", fetcher.metrics.get_summary());
    
    Ok(())
}