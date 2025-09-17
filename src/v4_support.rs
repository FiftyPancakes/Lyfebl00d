//! Uniswap V4 Support Module
//! 
//! This module provides comprehensive support for Uniswap V4 pools, which use
//! a singleton PoolManager contract and identify pools by 32-byte IDs rather
//! than individual addresses.

use ethers::providers::{Http, Provider};
use ethers::types::{Address, H256, U256};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// V4 Pool Key structure that uniquely identifies a pool
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct V4PoolKey {
    /// The lower currency of the pool (sorted numerically)
    /// For native ETH, use address(0)
    pub currency0: Address,
    
    /// The higher currency of the pool (sorted numerically)
    pub currency1: Address,
    
    /// The pool LP fee, capped at 1_000_000
    /// If the highest bit is 1, the pool has a dynamic fee
    pub fee: u32,
    
    /// Ticks that involve positions must be a multiple of tick spacing
    pub tick_spacing: i32,
    
    /// The hooks contract address (can be zero address if no hooks)
    pub hooks: Address,
}

impl V4PoolKey {
    /// Calculate the pool ID from the pool key
    /// This matches the Solidity: keccak256(abi.encode(poolKey))
    pub fn to_pool_id(&self) -> H256 {
        use ethers::abi::encode;
        use ethers::utils::keccak256;
        
        let encoded = encode(&[
            ethers::abi::Token::Address(self.currency0),
            ethers::abi::Token::Address(self.currency1),
            ethers::abi::Token::Uint(U256::from(self.fee)),
            ethers::abi::Token::Int(U256::from(self.tick_spacing)),
            ethers::abi::Token::Address(self.hooks),
        ]);
        
        H256::from(keccak256(encoded))
    }
    
    /// Sort currencies to ensure currency0 < currency1
    pub fn new(
        token0: Address,
        token1: Address,
        fee: u32,
        tick_spacing: i32,
        hooks: Address,
    ) -> Self {
        let (currency0, currency1) = if token0 < token1 {
            (token0, token1)
        } else {
            (token1, token0)
        };
        
        Self {
            currency0,
            currency1,
            fee,
            tick_spacing,
            hooks,
        }
    }
}

/// V4 Pool metadata stored in our system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4PoolMetadata {
    /// The pool key that defines this pool
    pub pool_key: V4PoolKey,
    
    /// The calculated pool ID (32-byte hash)
    pub pool_id: H256,
    
    /// The chain this pool exists on
    pub chain_name: String,
    
    /// The DEX name (e.g., "uniswap-v4-ethereum")
    pub dex_name: String,
    
    /// Token0 decimals
    pub token0_decimals: u8,
    
    /// Token1 decimals  
    pub token1_decimals: u8,
    
    /// Block number when this pool was created
    pub creation_block: u64,
    
    /// The singleton PoolManager contract address for this chain
    pub pool_manager_address: Address,
}

/// V4 Pool Registry for managing V4 pools separately
pub struct V4PoolRegistry {
    /// Map from pool ID (as hex string) to metadata
    pools_by_id: Arc<RwLock<HashMap<String, V4PoolMetadata>>>,
    
    /// Map from chain name to PoolManager contract address
    pool_managers: Arc<RwLock<HashMap<String, Address>>>,
    
    /// Map from token pair to list of V4 pools (for quick lookup)
    pools_by_pair: Arc<RwLock<HashMap<(Address, Address), Vec<H256>>>>,
}

impl V4PoolRegistry {
    pub fn new() -> Self {
        Self {
            pools_by_id: Arc::new(RwLock::new(HashMap::new())),
            pool_managers: Arc::new(RwLock::new(HashMap::new())),
            pools_by_pair: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Initialize known V4 PoolManager addresses for each chain
    pub async fn init_pool_managers(&self) {
        let mut managers = self.pool_managers.write().await;
        
        // Mainnet V4 PoolManager addresses (these would be from deployment)
        // Using placeholder addresses for now - replace with actual deployed addresses
        managers.insert(
            "ethereum".to_string(),
            "0x0000000000000000000000000000000000000000".parse().unwrap(),
        );
        managers.insert(
            "base".to_string(),
            "0x0000000000000000000000000000000000000000".parse().unwrap(),
        );
        managers.insert(
            "arbitrum".to_string(),
            "0x0000000000000000000000000000000000000000".parse().unwrap(),
        );
        managers.insert(
            "bsc".to_string(),
            "0x0000000000000000000000000000000000000000".parse().unwrap(),
        );
    }
    
    /// Register a V4 pool from database data
    pub async fn register_pool(
        &self,
        pool_id_str: &str,
        token0: Address,
        token1: Address,
        fee: u32,
        tick_spacing: i32,
        hooks: Address,
        token0_decimals: u8,
        token1_decimals: u8,
        chain_name: String,
        dex_name: String,
        creation_block: u64,
    ) -> Result<(), String> {
        // Parse the pool ID
        let pool_id = pool_id_str.parse::<H256>()
            .map_err(|e| format!("Invalid V4 pool ID {}: {}", pool_id_str, e))?;
        
        // Create the pool key
        let pool_key = V4PoolKey::new(token0, token1, fee, tick_spacing, hooks);
        
        // For V4 pools from the database, we trust the stored pool ID
        // We can't reliably recalculate it without the exact tick_spacing and hooks values
        // that were used when the pool was created on-chain
        // Note: The calculated ID would only match if we had the exact original parameters
        
        // Get the PoolManager address for this chain
        let pool_manager_address = {
            let managers = self.pool_managers.read().await;
            managers.get(&chain_name)
                .copied()
                .unwrap_or_else(|| "0x0000000000000000000000000000000000000000".parse().unwrap())
        };
        
        // Create metadata
        let metadata = V4PoolMetadata {
            pool_key: pool_key.clone(),
            pool_id,
            chain_name,
            dex_name,
            token0_decimals,
            token1_decimals,
            creation_block,
            pool_manager_address,
        };
        
        // Store in registry
        {
            let mut pools = self.pools_by_id.write().await;
            pools.insert(pool_id_str.to_string(), metadata);
        }
        
        // Update pair index
        {
            let mut pairs = self.pools_by_pair.write().await;
            let pair_key = if token0 < token1 {
                (token0, token1)
            } else {
                (token1, token0)
            };
            pairs.entry(pair_key)
                .or_insert_with(Vec::new)
                .push(pool_id);
        }
        
        Ok(())
    }
    
    /// Get pool metadata by ID
    pub async fn get_pool(&self, pool_id: &str) -> Option<V4PoolMetadata> {
        let pools = self.pools_by_id.read().await;
        pools.get(pool_id).cloned()
    }
    
    /// Get all V4 pools for a token pair
    pub async fn get_pools_for_pair(&self, token0: Address, token1: Address) -> Vec<V4PoolMetadata> {
        let pair_key = if token0 < token1 {
            (token0, token1)
        } else {
            (token1, token0)
        };
        
        let pairs = self.pools_by_pair.read().await;
        let pools = self.pools_by_id.read().await;
        
        if let Some(pool_ids) = pairs.get(&pair_key) {
            pool_ids.iter()
                .filter_map(|id| {
                    let id_str = format!("{:#x}", id);
                    pools.get(&id_str).cloned()
                })
                .collect()
        } else {
            Vec::new()
        }
    }
    
    /// Get the PoolManager address for a chain
    pub async fn get_pool_manager(&self, chain: &str) -> Option<Address> {
        let managers = self.pool_managers.read().await;
        managers.get(chain).copied()
    }
    
    /// Get total number of registered V4 pools
    pub async fn pool_count(&self) -> usize {
        let pools = self.pools_by_id.read().await;
        pools.len()
    }
}

/// V4 State reader for querying pool state onchain
pub struct V4StateReader {
    /// Reference to the V4 registry
    registry: Arc<V4PoolRegistry>,
}

impl V4StateReader {
    pub fn new(registry: Arc<V4PoolRegistry>) -> Self {
        Self { registry }
    }
    
    /// Get current state of a V4 pool
    /// This would use StateLibrary-like logic to read from the PoolManager
    pub async fn get_pool_state(
        &self,
        pool_id: &H256,
        provider: &Provider<Http>,
    ) -> Result<V4PoolState, String> {
        // Get pool metadata
        let _pool_metadata = self.registry
            .get_pool(&format!("{:#x}", pool_id))
            .await
            .ok_or_else(|| format!("V4 pool not found: {:#x}", pool_id))?;
        
        // In production, this would call the PoolManager contract to get state
        // Using StateLibrary's slot calculations to read via eth_getStorageAt
        // For now, return a placeholder
        let _ = provider; // suppress unused warning until integrated
        Ok(V4PoolState {
            sqrt_price_x96: U256::zero(),
            tick: 0,
            liquidity: 0,
            fee_growth_global0: U256::zero(),
            fee_growth_global1: U256::zero(),
        })
    }

    /// Convenience: resolve a pool by token pair and fee, then fetch state
    pub async fn get_pool_state_by_pair(
        &self,
        token0: Address,
        token1: Address,
        fee: u32,
        provider: Arc<Provider<Http>>,
    ) -> Result<V4PoolState, String> {
        // Find a matching pool in the registry
        let candidates = self.registry.get_pools_for_pair(token0, token1).await;
        let selected = candidates
            .into_iter()
            .find(|meta| meta.pool_key.fee == fee)
            .ok_or_else(|| "No V4 pool found for pair with requested fee".to_string())?;
        self.get_pool_state(&selected.pool_id, provider.as_ref()).await
    }
}

/// V4 Pool state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4PoolState {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub fee_growth_global0: U256,
    pub fee_growth_global1: U256,
}

/// Helper to determine if a string is a V4 pool ID (66 chars starting with 0x)
pub fn is_v4_pool_id(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Helper to determine if we need V4 handling based on protocol type
pub fn is_v4_protocol(protocol_type: i16) -> bool {
    protocol_type == 8
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pool_key_to_id() {
        let key = V4PoolKey::new(
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".parse().unwrap(), // USDC
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".parse().unwrap(), // WETH
            3000,
            60,
            Address::zero(),
        );
        
        let pool_id = key.to_pool_id();
        assert_eq!(pool_id.as_bytes().len(), 32);
    }
    
    #[test]
    fn test_is_v4_pool_id() {
        assert!(is_v4_pool_id("0x06249abc099011fec3a5550a7a6d1f12ba90ad9b9c59cd13b32a83b5c1fad7ee"));
        assert!(!is_v4_pool_id("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")); // Regular address
        assert!(!is_v4_pool_id("not_hex"));
    }
}
