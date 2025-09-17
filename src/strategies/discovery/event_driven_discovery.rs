use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, RwLock, Semaphore, Mutex};
use tokio::time::{interval, Duration, sleep};
use tracing::{debug, warn, info, error};
use ethers::types::{Address, Log, H256, U256, I256};
use ethers::abi::RawLog;
use ethers::contract::EthEvent;
use serde::{Serialize, Deserialize};
use once_cell::sync::Lazy;

use crate::{
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    types::{
        ArbitrageRoute, ArbitrageRouteLeg, ProtocolType, ChainEvent, PoolInfo,
    },
    strategies::discovery::{DiscoveryConfig, ScannerContext, DiscoveryScanner, ScannerStats},
    dex_math,
    alpha::circuit_breaker::CircuitBreaker,
    path::types::Path,
};

/// Production-grade DEX event configurations loaded from abi_parse.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexEventConfig {
    pub protocol: String,
    pub version: String,
    pub topic0: HashMap<String, String>,
    pub swap_event_field_map: HashMap<String, String>,
    pub factory_addresses: Option<HashMap<String, String>>,
    pub supported_fee_tiers: Option<Vec<u32>>,
}

/// Comprehensive ABI parse configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbiParseConfig {
    pub dexes: HashMap<String, DexEventConfig>,
}

/// Load and parse the ABI configuration
pub static ABI_CONFIG: Lazy<AbiParseConfig> = Lazy::new(|| {
    let abi_json = include_str!("../../../config/abi_parse.json");
    serde_json::from_str(abi_json).expect("Invalid abi_parse.json")
});

/// Event type classification for efficient routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EventType {
    Swap,
    Sync,
    Mint,
    Burn,
    Flash,
}

/// Event metadata for processing
#[derive(Debug, Clone)]
struct EventMetadata {
    pub event_type: EventType,
    pub protocol: ProtocolType,
    pub pool_address: Address,
    pub token0: Option<Address>,
    pub token1: Option<Address>,
    pub amount0: Option<U256>,
    pub amount1: Option<U256>,
    pub timestamp: u64,
}

/// Uniswap V2 Swap Event structure for ABI decoding
#[derive(Clone, Debug, EthEvent)]
#[ethevent(name = "Swap", abi = "Swap(address,uint256,uint256,uint256,uint256,address)")]
struct UniswapV2SwapEvent {
    #[ethevent(indexed)]
    pub sender: Address,
    pub amount0_in: U256,
    pub amount1_in: U256,
    pub amount0_out: U256,
    pub amount1_out: U256,
    #[ethevent(indexed)]
    pub to: Address,
}

/// Uniswap V3 Swap Event structure for ABI decoding
#[derive(Clone, Debug, EthEvent)]
#[ethevent(name = "Swap", abi = "Swap(address,address,int256,int256,uint160,uint128,int24)")]
struct UniswapV3SwapEvent {
    #[ethevent(indexed)]
    pub sender: Address,
    #[ethevent(indexed)]
    pub recipient: Address,
    pub amount0: I256,
    pub amount1: I256,
    pub sqrt_price_x96: U256,
    pub liquidity: U256,
    pub tick: i32,
}

/// Sync Event structure for reserve updates
#[derive(Clone, Debug, EthEvent)]
#[ethevent(name = "Sync", abi = "Sync(uint112,uint112)")]
struct SyncEvent {
    pub reserve0: U256,
    pub reserve1: U256,
}

/// Performance metrics for monitoring
#[derive(Debug)]
struct DiscoveryMetrics {
    pub events_received: AtomicU64,
    pub events_processed: AtomicU64,
    pub events_filtered: AtomicU64,
    pub opportunities_found: AtomicU64,
    pub opportunities_sent: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub pool_resolution_errors: AtomicU64,
    pub path_finding_errors: AtomicU64,
    pub batch_processing_time_ms: AtomicU64,
    pub last_opportunity_timestamp: AtomicU64,
}

impl Default for DiscoveryMetrics {
    fn default() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            events_filtered: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            opportunities_sent: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            pool_resolution_errors: AtomicU64::new(0),
            path_finding_errors: AtomicU64::new(0),
            batch_processing_time_ms: AtomicU64::new(0),
            last_opportunity_timestamp: AtomicU64::new(0),
        }
    }
}

/// Opportunity cache entry with expiration
#[derive(Debug, Clone)]
struct CachedOpportunity {
    pub hash: String,
    pub created_at: Instant,
    pub profit_usd: f64,
}

/// Event batch for processing
#[derive(Debug)]
struct EventBatch {
    pub events: Vec<EventMetadata>,
    pub received_at: Instant,
}

/// Event-driven discovery service with production-grade features
pub struct EventDrivenDiscoveryService {
    // Core components
    context: ScannerContext,
    config: Arc<RwLock<DiscoveryConfig>>,
    running: Arc<AtomicBool>,
    event_broadcaster: broadcast::Sender<ChainEvent>,

    // Event processing
    event_queue: Arc<Mutex<VecDeque<EventBatch>>>,
    affected_pools: Arc<RwLock<HashMap<Address, EventMetadata>>>,

    // Caching and deduplication
    opportunity_cache: Arc<RwLock<HashMap<String, CachedOpportunity>>>,
    pool_info_cache: Arc<RwLock<HashMap<(Address, Option<u64>), (PoolInfo, Instant)>>>,
    recent_events: Arc<RwLock<VecDeque<((H256, u64), Instant)>>>,
    discovered_opportunities: Arc<RwLock<Vec<MEVOpportunity>>>,

    // Rate limiting and circuit breaking
    pool_resolution_semaphore: Arc<Semaphore>,
    path_finding_semaphore: Arc<Semaphore>,
    circuit_breaker: Arc<CircuitBreaker>,

    // Metrics
    metrics: Arc<DiscoveryMetrics>,

    // DEX event mappings
    topic_to_event: HashMap<H256, (EventType, ProtocolType)>,
}

impl EventDrivenDiscoveryService {
    pub fn new(
        context: ScannerContext,
        event_broadcaster: broadcast::Sender<ChainEvent>,
    ) -> Self {
        // Build topic to event type mapping from ABI config
        let mut topic_to_event = HashMap::new();
        
        for (dex_name, dex_config) in &ABI_CONFIG.dexes {
            let protocol_type = match dex_name.as_str() {
                "UniswapV2" => ProtocolType::UniswapV2,
                "UniswapV3" => ProtocolType::UniswapV3,
                "SushiSwap" => ProtocolType::SushiSwap,
                "Curve" => ProtocolType::Curve,
                "Balancer" => ProtocolType::Balancer,
                "PancakeSwap" | "PancakeSwapV2" => ProtocolType::UniswapV2,
                "PancakeSwapV3" => ProtocolType::UniswapV3,
                _ => ProtocolType::Unknown,
            };

            if protocol_type == ProtocolType::Unknown {
                continue;
            }

            // Map event topics
            for (event_name, topic_hex) in &dex_config.topic0 {
                if let Ok(decoded) = hex::decode(topic_hex.trim_start_matches("0x")) {
                    if decoded.len() == 32 {
                        let topic = H256::from_slice(&decoded);
                            let event_type = match event_name.as_str() {
                            "Swap" => EventType::Swap,
                            "Sync" => EventType::Sync,
                            "Mint" => EventType::Mint,
                            "Burn" => EventType::Burn,
                            "Flash" | "FlashLoan" => EventType::Flash,
                            _ => continue,
                        };
                        topic_to_event.insert(topic, (event_type, protocol_type.clone()));
                    }
                }
            }
        }
        
        // Add any missing standard topics
        topic_to_event.insert(
            H256::from_slice(&hex::decode("1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1").unwrap()),
            (EventType::Sync, ProtocolType::UniswapV2)
        );
        topic_to_event.insert(
            H256::from_slice(&hex::decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822").unwrap()),
            (EventType::Swap, ProtocolType::UniswapV2)
        );
        topic_to_event.insert(
            H256::from_slice(&hex::decode("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67").unwrap()),
            (EventType::Swap, ProtocolType::UniswapV3)
        );
        
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            5,              // failure_threshold
            Duration::from_secs(60),  // timeout
        ));
        
        Self {
            context,
            config: Arc::new(RwLock::new(DiscoveryConfig::default())),
            running: Arc::new(AtomicBool::new(false)),
            event_broadcaster,
            event_queue: Arc::new(Mutex::new(VecDeque::new())),
            affected_pools: Arc::new(RwLock::new(HashMap::new())),
            opportunity_cache: Arc::new(RwLock::new(HashMap::new())),
            pool_info_cache: Arc::new(RwLock::new(HashMap::new())),
            recent_events: Arc::new(RwLock::new(VecDeque::new())),
            discovered_opportunities: Arc::new(RwLock::new(Vec::new())),
            pool_resolution_semaphore: Arc::new(Semaphore::new(50)),
            path_finding_semaphore: Arc::new(Semaphore::new(10)),
            circuit_breaker,
            metrics: Arc::new(DiscoveryMetrics::default()),
            topic_to_event,
        }
    }

    /// Start the event-driven discovery service
    pub async fn start(&self) -> Result<(), ArbitrageError> {
        info!("Starting EventDrivenDiscoveryService with production features");
        self.running.store(true, Ordering::SeqCst);
        
        // Start background tasks
        self.start_event_listener().await;
        self.start_batch_processor().await;
        self.start_cache_cleaner().await;
        self.start_metrics_reporter().await;
        
        Ok(())
    }

    /// Stop the service gracefully
    pub async fn stop(&self) -> Result<(), ArbitrageError> {
        info!("Stopping EventDrivenDiscoveryService");
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for pending batches to complete
        sleep(Duration::from_millis(500)).await;
        
        Ok(())
    }

    /// Start the event listener task
    async fn start_event_listener(&self) {
        let mut event_receiver = self.event_broadcaster.subscribe();
        let event_queue = self.event_queue.clone();
        let recent_events = self.recent_events.clone();
        let metrics = self.metrics.clone();
        let running = self.running.clone();
        let topic_to_event = self.topic_to_event.clone();
        
        tokio::spawn(async move {
            let mut current_batch = Vec::new();
            let mut batch_start = Instant::now();
            
            while running.load(Ordering::SeqCst) {
                tokio::select! {
                    // Receive events
                    recv_result = event_receiver.recv() => {
                        match recv_result {
                            Ok(ChainEvent::EvmLog { chain_name: _, log }) => {
                                metrics.events_received.fetch_add(1, Ordering::Relaxed);
                                
                                // Check for duplicate events
                                let is_duplicate = {
                                    let mut recent = recent_events.write().await;
                                    let now = Instant::now();

                                    // Clean old entries
                                    while let Some((_, timestamp)) = recent.front() {
                                        if now.duration_since(*timestamp) > Duration::from_secs(60) {
                                            recent.pop_front();
                                        } else {
                                            break;
                                        }
                                    }

                                    // Check if we've seen this event (include both transaction_hash and log_index)
                                    let tx_hash = log.transaction_hash.unwrap_or(H256::zero());
                                    let log_index = log.log_index.unwrap_or(U256::zero()).as_u64();
                                    let event_key = (tx_hash, log_index);

                                    if recent.iter().any(|(k, _)| *k == event_key) {
                                        true
                                    } else {
                                        recent.push_back((event_key, now));
                                        false
                                    }
                                };
                                
                                if is_duplicate {
                                    metrics.events_filtered.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                
                                // Parse event metadata
                                if let Some(metadata) = Self::parse_event_metadata(&log, &topic_to_event).await {
                                    current_batch.push(metadata);
                                    
                                    // Batch is full or timeout
                                    if current_batch.len() >= 100 || 
                                       batch_start.elapsed() > Duration::from_millis(50) {
                                        let mut queue = event_queue.lock().await;
                                        queue.push_back(EventBatch {
                                            events: std::mem::take(&mut current_batch),
                                            received_at: Instant::now(),
                                        });
                                        batch_start = Instant::now();
                                    }
                                }
                            }
                            Ok(_) => {} // Ignore other event types
                            Err(broadcast::error::RecvError::Lagged(count)) => {
                                warn!("Event receiver lagged by {} events", count);
                                metrics.events_filtered.fetch_add(count, Ordering::Relaxed);
                            }
                            Err(e) => {
                                error!("Event receiver error: {:?}", e);
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    
                    // Periodic batch flush
                    _ = sleep(Duration::from_millis(50)) => {
                        if !current_batch.is_empty() {
                            let mut queue = event_queue.lock().await;
                            queue.push_back(EventBatch {
                                events: std::mem::take(&mut current_batch),
                                received_at: Instant::now(),
                            });
                            batch_start = Instant::now();
                        }
                    }
                }
            }
        });
    }

    /// Start the batch processor task
    async fn start_batch_processor(&self) {
        let event_queue = self.event_queue.clone();
        let affected_pools = self.affected_pools.clone();
        let context = self.context.clone();
        let config = self.config.clone();
        let opportunity_cache = self.opportunity_cache.clone();
        let pool_info_cache = self.pool_info_cache.clone();
        let pool_resolution_semaphore = self.pool_resolution_semaphore.clone();
        let path_finding_semaphore = self.path_finding_semaphore.clone();
        let circuit_breaker = self.circuit_breaker.clone();
        let metrics = self.metrics.clone();
        let running = self.running.clone();
        let discovered_opportunities = self.discovered_opportunities.clone();

        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                // Get next batch
                let batch = {
                    let mut queue = event_queue.lock().await;
                    queue.pop_front()
                };
                
                if let Some(batch) = batch {
                    let start_time = Instant::now();
                    let batch_size = batch.events.len();
                    
                    // Process batch with circuit breaker
                    if circuit_breaker.is_tripped().await {
                        warn!("Circuit breaker is tripped, skipping batch processing");
                        metrics.events_filtered.fetch_add(batch_size as u64, Ordering::Relaxed);
                    } else {
                        match Self::process_event_batch_internal(
                            &pool_info_cache,
                            &batch,
                            &affected_pools,
                            &context,
                            &config,
                            &opportunity_cache,
                            &pool_resolution_semaphore,
                            &path_finding_semaphore,
                            &metrics,
                        ).await {
                            Ok(opportunities) => {
                                circuit_breaker.record_success().await;
                                metrics.events_processed.fetch_add(batch_size as u64, Ordering::Relaxed);
                                let elapsed = start_time.elapsed().as_millis() as u64;
                                metrics.batch_processing_time_ms.store(elapsed, Ordering::Relaxed);

                                // Store discovered opportunities
                                {
                                    let mut discovered = discovered_opportunities.write().await;
                                    discovered.extend(opportunities);
                                }
                            }
                            Err(e) => {
                                circuit_breaker.record_failure().await;
                                error!("Batch processing failed: {:?}", e);
                            }
                        }
                    }
                } else {
                    // No batches, sleep briefly
                    sleep(Duration::from_millis(10)).await;
                }
            }
        });
    }

    /// Process a batch of events
    #[tracing::instrument(skip_all, level = "debug", fields(batch_size = batch.events.len()))]
    async fn process_event_batch_internal(
        pool_info_cache: &Arc<RwLock<HashMap<(Address, Option<u64>), (PoolInfo, Instant)>>>,
        batch: &EventBatch,
        affected_pools: &Arc<RwLock<HashMap<Address, EventMetadata>>>,
        context: &ScannerContext,
        config: &Arc<RwLock<DiscoveryConfig>>,
        opportunity_cache: &Arc<RwLock<HashMap<String, CachedOpportunity>>>,
        pool_resolution_semaphore: &Arc<Semaphore>,
        path_finding_semaphore: &Arc<Semaphore>,
        metrics: &Arc<DiscoveryMetrics>,
    ) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        // Group events by pool
        let mut pools_to_process = HashMap::new();
        {
            let mut affected = affected_pools.write().await;
            for event in &batch.events {
                affected.insert(event.pool_address, event.clone());
                pools_to_process.insert(event.pool_address, event.clone());
            }
        }
        
        // Process pools sequentially to collect opportunities
        let mut all_opportunities = Vec::new();
        let chain_name = context.get_chain_name();

        for (pool_address, _event) in pools_to_process {
            let _permit = pool_resolution_semaphore.acquire().await
                .map_err(|e| ArbitrageError::Config(format!("Semaphore error: {}", e)))?;

            // Check pool info cache first
            let pool_info = {
                let cache_key = (pool_address, None);
                let mut cache = pool_info_cache.write().await;
                let now = Instant::now();

                // Clean expired entries (cache for 5 minutes)
                cache.retain(|_, (_, timestamp)| now.duration_since(*timestamp) < Duration::from_secs(300));

                if let Some((cached_info, _)) = cache.get(&cache_key) {
                    Some(cached_info.clone())
                } else {
                    // Fetch from pool manager and cache it
                    match context.chain_infra.pool_manager.get_pool_info(&chain_name, pool_address, None).await {
                        Ok(info) => {
                            cache.insert(cache_key, (info.clone(), now));
                            Some(info)
                        }
                        Err(_) => None,
                    }
                }
            };

            if let Some(pool_info) = pool_info {
                // Find arbitrage opportunities for affected tokens
                if let Some(opportunity) = Self::find_opportunities_for_pool(
                    &pool_info,
                    &context,
                    &config,
                    &opportunity_cache,
                    &path_finding_semaphore,
                    &metrics,
                ).await {
                    all_opportunities.push(opportunity);
                }
            } else {
                debug!("Failed to resolve pool {}: cache miss and fetch failed", pool_address);
                metrics.pool_resolution_errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(all_opportunities)
    }

    /// Find arbitrage opportunities for a specific pool
    async fn find_opportunities_for_pool(
        pool_info: &PoolInfo,
        context: &ScannerContext,
        config: &Arc<RwLock<DiscoveryConfig>>,
        opportunity_cache: &Arc<RwLock<HashMap<String, CachedOpportunity>>>,
        path_finding_semaphore: &Arc<Semaphore>,
        metrics: &Arc<DiscoveryMetrics>,
    ) -> Option<MEVOpportunity> {
        let _permit = match path_finding_semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => return None,
        };
        
        let config = config.read().await;
        
        // Find cyclic arbitrage paths
        let test_amount = U256::from(1000000000000000000u128); // 1 ETH in wei as test amount
        let paths = match context.path_finder.find_circular_arbitrage_paths(
            pool_info.token0,
            test_amount,
            None,
        ).await {
            Ok(paths) => paths,
            Err(e) => {
                debug!("Path finding failed for pool {}: {:?}", pool_info.address, e);
                metrics.path_finding_errors.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        
        // Process each path and return first opportunity found
        for path in paths {
            if let Some(opportunity) = Self::evaluate_path_opportunity(
                &path,
                context,
                &config,
                opportunity_cache,
                metrics,
            ).await {
                metrics.opportunities_sent.fetch_add(1, Ordering::Relaxed);
                return Some(opportunity);
            }
        }

        None
    }

    /// Parse event metadata from log
    async fn parse_event_metadata(
        log: &Log,
        topic_to_event: &HashMap<H256, (EventType, ProtocolType)>,
    ) -> Option<EventMetadata> {
        if log.topics.is_empty() {
            return None;
        }
        
        let topic0 = log.topics[0];
        let (event_type, protocol) = topic_to_event.get(&topic0)?;
        
        // Extract pool address and tokens based on protocol
        let pool_address = log.address;
        let mut token0 = None;
        let mut token1 = None;
        let mut amount0 = None;
        let mut amount1 = None;
        
        match (event_type, protocol) {
            (EventType::Swap, ProtocolType::UniswapV2 | ProtocolType::SushiSwap) => {
                // Use ABI decoding for V2 Swap events
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                if let Ok(swap_event) = UniswapV2SwapEvent::decode_log(&raw_log) {
                    // Calculate net amounts (out - in) for the swap direction
                    amount0 = Some(swap_event.amount0_out.saturating_sub(swap_event.amount0_in));
                    amount1 = Some(swap_event.amount1_out.saturating_sub(swap_event.amount1_in));
                } else {
                    // Fallback to manual parsing if ABI decoding fails
                    debug!("Failed to decode V2 Swap event, falling back to manual parsing");
                    if log.data.len() >= 128 {
                        let a0_in  = U256::from_big_endian(&log.data[ 0.. 32]);
                        let a1_in  = U256::from_big_endian(&log.data[32.. 64]);
                        let a0_out = U256::from_big_endian(&log.data[64.. 96]);
                        let a1_out = U256::from_big_endian(&log.data[96..128]);
                        amount0 = Some(a0_out.saturating_sub(a0_in));
                        amount1 = Some(a1_out.saturating_sub(a1_in));
                    }
                }
            }
            (EventType::Swap, ProtocolType::UniswapV3) => {
                // Use ABI decoding for V3 Swap events
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                if let Ok(swap_event) = UniswapV3SwapEvent::decode_log(&raw_log) {
                    // Take absolute values for amounts (negative means token was sent out)
                    amount0 = if swap_event.amount0 < I256::zero() {
                        Some((-swap_event.amount0).into_raw())
                    } else {
                        Some(swap_event.amount0.into_raw())
                    };
                    amount1 = if swap_event.amount1 < I256::zero() {
                        Some((-swap_event.amount1).into_raw())
                    } else {
                        Some(swap_event.amount1.into_raw())
                    };
                } else {
                    // Fallback to manual parsing if ABI decoding fails
                    debug!("Failed to decode V3 Swap event, falling back to manual parsing");
                    if log.data.len() >= 160 {
                        let a0 = I256::from_raw(U256::from_big_endian(&log.data[0..32]));
                        let a1 = I256::from_raw(U256::from_big_endian(&log.data[32..64]));
                        amount0 = if a0 < I256::zero() { Some((-a0).into_raw()) } else { Some(a0.into_raw()) };
                        amount1 = if a1 < I256::zero() { Some((-a1).into_raw()) } else { Some(a1.into_raw()) };
                    }
                }
            }
            (EventType::Swap, ProtocolType::Balancer) => {
                // Balancer swap has different structure
                if log.topics.len() >= 3 {
                    token0 = Some(Address::from(log.topics[1]));
                    token1 = Some(Address::from(log.topics[2]));
                }
                if log.data.len() >= 64 {
                    amount0 = Some(U256::from_big_endian(&log.data[0..32]));
                    amount1 = Some(U256::from_big_endian(&log.data[32..64]));
                }
            }
            (EventType::Sync, _) => {
                // Use ABI decoding for Sync events
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                if let Ok(sync_event) = SyncEvent::decode_log(&raw_log) {
                    amount0 = Some(sync_event.reserve0);
                    amount1 = Some(sync_event.reserve1);
                } else {
                    // Fallback to manual parsing
                    debug!("Failed to decode Sync event, falling back to manual parsing");
                    if log.data.len() >= 64 {
                        amount0 = Some(U256::from_big_endian(&log.data[0..32]));
                        amount1 = Some(U256::from_big_endian(&log.data[32..64]));
                    }
                }
            }
            _ => {}
        }
        
        Some(EventMetadata {
            event_type: *event_type,
            protocol: protocol.clone(),
            pool_address,
            token0,
            token1,
            amount0,
            amount1,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        })
    }

    /// Evaluate a path for arbitrage opportunity
    async fn evaluate_path_opportunity(
        path: &Path,
        context: &ScannerContext,
        config: &DiscoveryConfig,
        opportunity_cache: &Arc<RwLock<HashMap<String, CachedOpportunity>>>,
        metrics: &Arc<DiscoveryMetrics>,
    ) -> Option<MEVOpportunity> {
        let _chain_name = context.get_chain_name();

        // Generate cache key
        let cache_key = Self::generate_path_cache_key(path);
        
        // Check cache
        {
            let cache = opportunity_cache.read().await;
            if let Some(cached) = cache.get(&cache_key) {
                if cached.created_at.elapsed() < Duration::from_secs(30) {
                    metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        }
        metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        // Calculate accurate profit using dex_math
        let test_amount = context.get_test_amount_for_token(path.token_in);
        let mut current_amount = test_amount;
        let mut gas_cost = U256::zero();
        
        for step in &path.legs {
            // Get pool info
            let pool_info = match context.chain_infra.pool_manager
                .get_pool_info(&context.get_chain_name(), step.pool_address, None).await {
                Ok(info) => info,
                Err(_) => return None,
            };
            
            // Calculate output amount using production math
            match dex_math::get_amount_out(&pool_info, step.from_token, current_amount) {
                Ok(amount_out) => {
                    current_amount = amount_out;
                    gas_cost = gas_cost.saturating_add(U256::from(dex_math::estimate_gas(&pool_info)));
                }
                Err(e) => {
                    debug!("Math calculation failed: {:?}", e);
                    return None;
                }
            }
        }
        
        // Check if profitable
        if current_amount <= test_amount {
            return None;
        }
        
        let profit = current_amount.saturating_sub(test_amount);
        
        // Convert to USD
        let chain_name = context.get_chain_name();
        let token_price = context.price_oracle
            .get_token_price_usd(&chain_name, path.token_in).await.ok()?;
        
        // Safe unit scaling to avoid truncation
        let profit_usd = Self::u256_to_f64_units(profit, 18) * token_price;
        
        // Get gas price and calculate gas cost in USD
        let gas_price = match context.chain_infra.gas_oracle.get_gas_price(&chain_name, None).await {
            Ok(price) => price.effective_price(),
            Err(_) => U256::from(20000000000u64), // Default 20 gwei
        };
        let native_token = context.blockchain_manager.get_weth_address();
        let eth_price = context.price_oracle.get_token_price_usd(&chain_name, native_token).await.ok()?;
        
        let gas_cost_usd = (Self::u256_to_f64_units(gas_cost, 0) * Self::u256_to_f64_units(gas_price, 0) / 1e18) * eth_price;
        
        let net_profit_usd = profit_usd - gas_cost_usd;
        
        if net_profit_usd < config.min_profit_usd {
            return None;
        }
        
        // Update cache
        {
            let mut cache = opportunity_cache.write().await;
            cache.insert(cache_key.clone(), CachedOpportunity {
                hash: cache_key,
                created_at: Instant::now(),
                profit_usd: net_profit_usd,
            });
        }
        
        metrics.opportunities_found.fetch_add(1, Ordering::Relaxed);
        metrics.last_opportunity_timestamp.store(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Ordering::Relaxed
        );
        
        // Create arbitrage route with computed profit
        let route = Self::path_to_arbitrage_route(path, profit, gas_cost, net_profit_usd);

        Some(MEVOpportunity::Arbitrage(route))
    }

    /// Generate cache key for path with enhanced context
    fn generate_path_cache_key(path: &Path) -> String {
        let pools_and_fees: Vec<String> = path.legs.iter()
            .map(|leg| format!("{:?}_{}", leg.pool_address, leg.fee))
            .collect();
        let directions: Vec<String> = path.legs.iter()
            .map(|leg| format!("{:?}->{:?}", leg.from_token, leg.to_token))
            .collect();
        format!("{:?}:{}:{}", path.token_in, pools_and_fees.join(":"), directions.join(":"))
    }

    /// Convert Path to ArbitrageRoute
    fn path_to_arbitrage_route(path: &Path, _profit: U256, gas_cost: U256, profit_usd: f64) -> ArbitrageRoute {
        let legs: Vec<ArbitrageRouteLeg> = path.legs.iter().map(|leg| {
            ArbitrageRouteLeg {
                dex_name: Self::normalize_dex_name(leg.protocol_type.clone()),
                protocol_type: leg.protocol_type.clone(),
                pool_address: leg.pool_address,
                token_in: leg.from_token,
                token_out: leg.to_token,
                amount_in: leg.amount_in,
                min_amount_out: leg.amount_out,
                fee_bps: leg.fee,
                expected_out: Some(leg.amount_out),
            }
        }).collect();

        let chain_id = 1; // Default to Ethereum mainnet, should be passed in from context

        ArbitrageRoute {
            legs,
            initial_amount_in_wei: path.initial_amount_in_wei,
            amount_out_wei: path.expected_output,
            profit_usd,
            estimated_gas_cost_wei: gas_cost,
            price_impact_bps: path.price_impact_bps,
            route_description: format!("Arbitrage path with {} hops", path.legs.len()),
            risk_score: path.mev_resistance_score,
            chain_id,
            created_at_ns: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64,
            mev_risk_score: path.mev_resistance_score,
            flashloan_amount_wei: None,
            flashloan_fee_wei: None,
            token_in: path.token_in,
            token_out: path.token_out,
            amount_in: path.initial_amount_in_wei,
            amount_out: path.expected_output,
        }
    }

    /// Start cache cleaner task
    async fn start_cache_cleaner(&self) {
        let opportunity_cache = self.opportunity_cache.clone();
        let running = self.running.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                // Clean expired entries
                let now = Instant::now();
                let mut cache = opportunity_cache.write().await;
                cache.retain(|_, entry| {
                    now.duration_since(entry.created_at) < Duration::from_secs(300)
                });
            }
        });
    }

    /// Start metrics reporter task
    async fn start_metrics_reporter(&self) {
        let metrics = self.metrics.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));

            while running.load(Ordering::SeqCst) {
                interval.tick().await;

                let events_received = metrics.events_received.load(Ordering::Relaxed);
                let events_processed = metrics.events_processed.load(Ordering::Relaxed);
                let opportunities_found = metrics.opportunities_found.load(Ordering::Relaxed);
                let cache_hits = metrics.cache_hits.load(Ordering::Relaxed);
                let cache_misses = metrics.cache_misses.load(Ordering::Relaxed);
                let batch_time = metrics.batch_processing_time_ms.load(Ordering::Relaxed);

                let cache_hit_rate = if cache_hits + cache_misses > 0 {
                    (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "EventDiscovery metrics - Received: {}, Processed: {}, Opportunities: {}, Cache hit rate: {:.1}%, Batch time: {}ms",
                    events_received, events_processed, opportunities_found, cache_hit_rate, batch_time
                );
            }
        });
    }

    /// Safe conversion from U256 to f64 with proper decimal scaling
    fn u256_to_f64_units(x: U256, decimals: u32) -> f64 {
        // Use ethers format_units for accurate conversion
        let s = ethers::utils::format_units(x, decimals as i32)
            .unwrap_or_else(|_| "0".into());
        s.parse::<f64>().unwrap_or(0.0)
    }

    /// Normalize DEX name for consistent display
    fn normalize_dex_name(protocol_type: crate::types::ProtocolType) -> String {
        match protocol_type {
            crate::types::ProtocolType::UniswapV2 => "Uniswap V2".to_string(),
            crate::types::ProtocolType::UniswapV3 => "Uniswap V3".to_string(),
            crate::types::ProtocolType::UniswapV4 => "Uniswap V4".to_string(),
            crate::types::ProtocolType::SushiSwap => "SushiSwap".to_string(),
            crate::types::ProtocolType::Curve => "Curve".to_string(),
            crate::types::ProtocolType::Balancer => "Balancer".to_string(),
            crate::types::ProtocolType::Unknown => "DEX".to_string(),
            crate::types::ProtocolType::Other(name) => name,
        }
    }


}

/// DiscoveryScanner trait implementation
#[async_trait::async_trait]
impl DiscoveryScanner for EventDrivenDiscoveryService {
    fn name(&self) -> &'static str {
        "EventDrivenDiscovery"
    }

    async fn initialize(&self, _config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        self.start().await
    }

    async fn shutdown(&self) -> Result<(), ArbitrageError> {
        self.stop().await
    }

    async fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn get_stats(&self) -> ScannerStats {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let last_opportunity = self.metrics.last_opportunity_timestamp.load(Ordering::Relaxed);
        let last_scan_time = if last_opportunity > 0 {
            Some(now.saturating_sub(last_opportunity))
        } else {
            None
        };
        
        ScannerStats {
            opportunities_found: self.metrics.opportunities_found.load(Ordering::Relaxed),
            opportunities_sent: self.metrics.opportunities_sent.load(Ordering::Relaxed),
            last_scan_time,
            errors: self.metrics.pool_resolution_errors.load(Ordering::Relaxed) + self.metrics.path_finding_errors.load(Ordering::Relaxed),
            avg_scan_duration_ms: self.metrics.batch_processing_time_ms.load(Ordering::Relaxed) as f64,
        }
    }

    async fn scan(&self, _block_number: Option<u64>) -> Result<Vec<MEVOpportunity>, ArbitrageError> {
        // Return discovered opportunities and clear the vector
        let mut opportunities = self.discovered_opportunities.write().await;
        let result = opportunities.clone();
        opportunities.clear();
        Ok(result)
    }

    async fn update_config(&self, config: DiscoveryConfig) -> Result<(), ArbitrageError> {
        *self.config.write().await = config;
        Ok(())
    }

    async fn get_config(&self) -> DiscoveryConfig {
        self.config.read().await.clone()
    }
}
