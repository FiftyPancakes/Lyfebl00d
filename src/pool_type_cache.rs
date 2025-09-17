//! Pool Type Cache
//!
//! Caches pool types to avoid repeated detection attempts that can fail and slow down the system.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use ethers::types::Address;
use std::time::{Duration, Instant};
use tracing::{debug, info};
use crate::types::ProtocolType;

/// Entry in the pool type cache
#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub protocol_type: ProtocolType,
    pub timestamp: Instant,
    pub detection_method: DetectionMethod,
}

/// How the pool type was detected
#[derive(Clone, Debug)]
pub enum DetectionMethod {
    Bytecode,
    MethodCall,
    Configured,
    Default,
}

/// Cache for pool types to avoid repeated detection
#[derive(Clone)]
pub struct PoolTypeCache {
    cache: Arc<RwLock<HashMap<Address, CacheEntry>>>,
    ttl: Duration,
    default_type: ProtocolType,
}

impl PoolTypeCache {
    /// Create a new pool type cache
    pub fn new(ttl_seconds: u64, default_type: ProtocolType) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(ttl_seconds),
            default_type,
        }
    }

    /// Get a pool type from cache if available and not expired
    pub async fn get(&self, address: Address) -> Option<ProtocolType> {
        let cache = self.cache.read().await;
        if let Some(entry) = cache.get(&address) {
            if entry.timestamp.elapsed() < self.ttl {
                debug!(
                    "Pool type cache hit for {:?}: {:?} (detected via {:?})",
                    address, entry.protocol_type, entry.detection_method
                );
                return Some(entry.protocol_type.clone());
            } else {
                debug!("Pool type cache expired for {:?}", address);
            }
        }
        None
    }

    /// Store a pool type in cache
    pub async fn set(&self, address: Address, protocol_type: ProtocolType, method: DetectionMethod) {
        let mut cache = self.cache.write().await;
        let entry = CacheEntry {
            protocol_type: protocol_type.clone(),
            timestamp: Instant::now(),
            detection_method: method.clone(),
        };
        cache.insert(address, entry);
        debug!(
            "Cached pool type for {:?}: {:?} (via {:?})",
            address, protocol_type, method
        );
    }

    /// Pre-populate cache with known pool types (e.g., from test setup)
    pub async fn bulk_set(&self, pools: Vec<(Address, ProtocolType)>) {
        let mut cache = self.cache.write().await;
        for (address, protocol_type) in pools {
            let entry = CacheEntry {
                protocol_type: protocol_type.clone(),
                timestamp: Instant::now(),
                detection_method: DetectionMethod::Configured,
            };
            cache.insert(address, entry);
        }
        info!("Pre-populated pool type cache with {} entries", cache.len());
    }

    /// Clear expired entries from cache
    pub async fn clear_expired(&self) {
        let mut cache = self.cache.write().await;
        let now = Instant::now();
        cache.retain(|addr, entry| {
            let expired = now.duration_since(entry.timestamp) >= self.ttl;
            if expired {
                debug!("Removing expired pool type cache entry for {:?}", addr);
            }
            !expired
        });
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        let now = Instant::now();
        let mut active = 0;
        let mut expired = 0;
        let mut by_method = HashMap::new();

        for entry in cache.values() {
            if now.duration_since(entry.timestamp) < self.ttl {
                active += 1;
            } else {
                expired += 1;
            }
            *by_method.entry(format!("{:?}", entry.detection_method)).or_insert(0) += 1;
        }

        CacheStats {
            total_entries: cache.len(),
            active_entries: active,
            expired_entries: expired,
            entries_by_method: by_method,
        }
    }

    /// Clear all cache entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        let count = cache.len();
        cache.clear();
        info!("Cleared {} entries from pool type cache", count);
    }
}

/// Statistics about the cache
#[derive(Debug)]
pub struct CacheStats {
    pub total_entries: usize,
    pub active_entries: usize,
    pub expired_entries: usize,
    pub entries_by_method: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let cache = PoolTypeCache::new(60, ProtocolType::UniswapV2);
        let addr = Address::random();

        // Initially empty
        assert!(cache.get(addr).await.is_none());

        // Set and get
        cache.set(addr, ProtocolType::UniswapV3, DetectionMethod::MethodCall).await;
        assert_eq!(cache.get(addr).await, Some(ProtocolType::UniswapV3));

        // Stats
        let stats = cache.stats().await;
        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.active_entries, 1);
        assert_eq!(stats.expired_entries, 0);
    }

    #[tokio::test]
    async fn test_cache_expiry() {
        let cache = PoolTypeCache::new(0, ProtocolType::UniswapV2); // 0 second TTL
        let addr = Address::random();

        cache.set(addr, ProtocolType::UniswapV3, DetectionMethod::Bytecode).await;
        
        // Should be expired immediately
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(cache.get(addr).await.is_none());

        // Stats should show expired
        let stats = cache.stats().await;
        assert_eq!(stats.expired_entries, 1);
    }

    #[tokio::test]
    async fn test_bulk_set() {
        let cache = PoolTypeCache::new(60, ProtocolType::UniswapV2);
        
        let pools = vec![
            (Address::random(), ProtocolType::UniswapV2),
            (Address::random(), ProtocolType::UniswapV3),
            (Address::random(), ProtocolType::SushiSwap),
        ];

        cache.bulk_set(pools.clone()).await;

        // Verify all entries are present
        for (addr, expected_type) in &pools {
            assert_eq!(cache.get(*addr).await, Some(expected_type.clone()));
        }

        let stats = cache.stats().await;
        assert_eq!(stats.total_entries, 3);
        assert_eq!(*stats.entries_by_method.get("Configured").unwrap(), 3);
    }
}