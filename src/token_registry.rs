//! Token registry for managing token information

use ethers::types::Address;
use std::collections::HashMap;
use eyre::Result;
use sqlx::PgPool;

/// Token information
#[derive(Debug, Clone, PartialEq)]
pub struct TokenInfo {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
    pub name: String,
}

/// Database row representation of a token
#[derive(Debug, Clone)]
pub struct CoinRow {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
    pub name: String,
    pub price_usd: Option<f64>,
}

/// Trait for token registry implementations
#[async_trait::async_trait]
pub trait TokenRegistry: Send + Sync {
    /// Get token info by address
    async fn get_token(&self, address: Address) -> Result<Option<TokenInfo>>;

    /// Get all registered tokens
    async fn get_all_tokens(&self) -> Result<Vec<TokenInfo>>;

    /// Check if token is registered
    async fn is_registered(&self, address: Address) -> Result<bool>;

    /// Register a token
    async fn register_token(&mut self, info: TokenInfo) -> Result<()>;
}

/// In-memory token registry for caching token information
pub struct InMemoryTokenRegistry {
    tokens: HashMap<Address, TokenInfo>,
}

impl InMemoryTokenRegistry {
    /// Create a new in-memory token registry
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl TokenRegistry for InMemoryTokenRegistry {
    async fn get_token(&self, address: Address) -> Result<Option<TokenInfo>> {
        Ok(self.tokens.get(&address).cloned())
    }

    async fn get_all_tokens(&self) -> Result<Vec<TokenInfo>> {
        Ok(self.tokens.values().cloned().collect())
    }

    async fn is_registered(&self, address: Address) -> Result<bool> {
        Ok(self.tokens.contains_key(&address))
    }

    async fn register_token(&mut self, info: TokenInfo) -> Result<()> {
        self.tokens.insert(info.address, info);
        Ok(())
    }
}

impl Default for InMemoryTokenRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// CoinGecko-based token registry implementation
pub struct CoinGeckoTokenRegistry {
    db_pool: std::sync::Arc<PgPool>,
    chain_name: String,
    cache: HashMap<Address, TokenInfo>,
}

impl CoinGeckoTokenRegistry {
    /// Create a new CoinGecko token registry
    pub fn new(db_pool: std::sync::Arc<PgPool>, chain_name: String) -> Self {
        Self {
            db_pool,
            chain_name,
            cache: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl TokenRegistry for CoinGeckoTokenRegistry {
    async fn get_token(&self, address: Address) -> Result<Option<TokenInfo>> {
        // Check cache first
        if let Some(token) = self.cache.get(&address) {
            return Ok(Some(token.clone()));
        }

        // TODO: Implement database lookup for CoinGecko data
        // For now, return None
        Ok(None)
    }

    async fn get_all_tokens(&self) -> Result<Vec<TokenInfo>> {
        Ok(self.cache.values().cloned().collect())
    }

    async fn is_registered(&self, address: Address) -> Result<bool> {
        Ok(self.cache.contains_key(&address))
    }

    async fn register_token(&mut self, info: TokenInfo) -> Result<()> {
        self.cache.insert(info.address, info);
        Ok(())
    }
}
