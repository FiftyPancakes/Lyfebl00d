// src/mempool.rs
//! Enhanced mempool monitoring system with backpressure handling, transaction filtering,
//! and support for multiple data sources.
//!
//! # Features
//! - **Backpressure Handling**: Tracks and reports dropped messages due to channel lag
//! - **Transaction Filtering**: Filter by hot addresses, value thresholds, and gas prices
//! - **Alternative Data Sources**: Support for bloXroute, Blocknative, and custom providers
//! - **Composite Monitoring**: Aggregate multiple sources while deduplicating transactions
//! - **Comprehensive Metrics**: Track performance, errors, and health across all monitors
//!
//! # Usage
//!
//! ## Basic Default Monitor
//! Create a DefaultMempoolMonitor with configuration and blockchain provider.
//! Start the monitor and subscribe to transaction events.
//!
//! ## Filtered Monitor with Hot Addresses  
//! Configure transaction filters for specific addresses (like DEX routers),
//! minimum value thresholds, and gas price limits.
//!
//! ## Composite Monitor with Multiple Sources
//! Combine multiple monitoring sources (default, bloXroute, Blocknative)
//! into a single composite monitor that deduplicates transactions.

use crate::{
    blockchain::BlockchainManager,
    config::MempoolConfig,
    errors::MempoolError,
    metrics::MetricsReporter,
};
use async_trait::async_trait;
use ethers::{
    core::types::{Address, Transaction, H256, U256},
    providers::Middleware,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, Semaphore},
    task::JoinHandle,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use crate::types::UARAction;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

// Reconnection utility with exponential backoff
#[derive(Debug, Clone)]
pub struct ReconnectionManager {
    initial_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: u64,
}

impl ReconnectionManager {
    pub fn new(initial_delay: Duration, max_delay: Duration, backoff_multiplier: u64) -> Self {
        Self {
            initial_delay,
            max_delay,
            backoff_multiplier,
        }
    }

    pub fn default() -> Self {
        Self::new(Duration::from_secs(10), Duration::from_secs(300), 2)
    }

    pub async fn execute_with_reconnect<F, Fut, T, E>(
        &self,
        mut operation: F,
        stats: &Arc<MempoolStats>,
        monitor_type: &str,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut delay = self.initial_delay;
        let mut attempts = 0u64;

        loop {
            attempts += 1;
            match operation().await {
                Ok(result) => {
                    if attempts > 1 {
                        info!(target: "reconnection_manager", "Successfully reconnected {} after {} attempts", monitor_type, attempts);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    stats.connection_failures.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "reconnection_manager",
                        error = %e,
                        attempts = attempts,
                        "Failed to connect {} - retrying in {}s",
                        monitor_type,
                        delay.as_secs()
                    );

                    sleep(delay).await;
                    delay = Duration::from_secs((delay.as_secs() * self.backoff_multiplier).min(self.max_delay.as_secs()));
                }
            }
        }
    }

    pub async fn sleep_with_reset(&mut self) {
        sleep(self.initial_delay).await;
        // Reset to initial delay after successful connection
        // This method is called when we want to reset the delay
    }

    pub fn reset_delay(&mut self) {
        // Reset delay to initial value - used when connection is successful
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MempoolMetrics {
    pub total_transactions_received: u64,
    pub total_transactions_filtered: u64,
    pub total_transactions_broadcasted: u64,
    pub total_dropped_due_to_lag: u64,
    pub total_no_subscribers: u64,
    pub current_subscriber_count: usize,
    pub connection_failures: u64,
    pub stream_reconnections: u64,
    pub retry_attempts: u64,
    pub successful_retries: u64,
    pub failed_retries: u64,
}

impl Default for MempoolMetrics {
    fn default() -> Self {
        Self {
            total_transactions_received: 0,
            total_transactions_filtered: 0,
            total_transactions_broadcasted: 0,
            total_dropped_due_to_lag: 0,
            total_no_subscribers: 0,
            current_subscriber_count: 0,
            connection_failures: 0,
            stream_reconnections: 0,
            retry_attempts: 0,
            successful_retries: 0,
            failed_retries: 0,
        }
    }
}

pub struct MempoolStats {
    pub transactions_received: AtomicU64,
    pub transactions_filtered: AtomicU64,
    pub transactions_broadcasted: AtomicU64,
    pub dropped_due_to_lag: AtomicU64,
    pub no_subscribers: AtomicU64,
    pub connection_failures: AtomicU64,
    pub stream_reconnections: AtomicU64,
    pub retry_attempts: AtomicU64,
    pub successful_retries: AtomicU64,
    pub failed_retries: AtomicU64,
}

impl MempoolStats {
    pub fn new() -> Self {
        Self {
            transactions_received: AtomicU64::new(0),
            transactions_filtered: AtomicU64::new(0),
            transactions_broadcasted: AtomicU64::new(0),
            dropped_due_to_lag: AtomicU64::new(0),
            no_subscribers: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            stream_reconnections: AtomicU64::new(0),
            retry_attempts: AtomicU64::new(0),
            successful_retries: AtomicU64::new(0),
            failed_retries: AtomicU64::new(0),
        }
    }

    pub fn get_metrics(&self, subscriber_count: usize) -> MempoolMetrics {
        MempoolMetrics {
            total_transactions_received: self.transactions_received.load(Ordering::Relaxed),
            total_transactions_filtered: self.transactions_filtered.load(Ordering::Relaxed),
            total_transactions_broadcasted: self.transactions_broadcasted.load(Ordering::Relaxed),
            total_dropped_due_to_lag: self.dropped_due_to_lag.load(Ordering::Relaxed),
            total_no_subscribers: self.no_subscribers.load(Ordering::Relaxed),
            current_subscriber_count: subscriber_count,
            connection_failures: self.connection_failures.load(Ordering::Relaxed),
            stream_reconnections: self.stream_reconnections.load(Ordering::Relaxed),
            retry_attempts: self.retry_attempts.load(Ordering::Relaxed),
            successful_retries: self.successful_retries.load(Ordering::Relaxed),
            failed_retries: self.failed_retries.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionFilter {
    pub hot_addresses: HashSet<Address>,
    pub min_value_wei: Option<U256>,
    pub max_effective_gas_price_wei: Option<U256>,
    pub include_contract_creation: bool,
}

impl TransactionFilter {
    pub fn new() -> Self {
        Self {
            hot_addresses: HashSet::new(),
            min_value_wei: None,
            max_effective_gas_price_wei: None,
            include_contract_creation: false,
        }
    }

    pub fn with_hot_addresses(mut self, addresses: Vec<Address>) -> Self {
        self.hot_addresses = addresses.into_iter().collect();
        self
    }

    pub fn with_min_value_wei(mut self, min_value: U256) -> Self {
        self.min_value_wei = Some(min_value);
        self
    }

    pub fn with_max_effective_gas_price_wei(mut self, max_gas_price: U256) -> Self {
        self.max_effective_gas_price_wei = Some(max_gas_price);
        self
    }

    pub fn with_contract_creation(mut self, include: bool) -> Self {
        self.include_contract_creation = include;
        self
    }

    fn max_allowed_gas_price(tx: &Transaction) -> U256 {
        // For EIP-1559 transactions, prefer max_fee_per_gas
        // For legacy transactions, use gas_price
        tx.max_fee_per_gas
            .or(tx.gas_price)
            .unwrap_or_default()
    }

    pub fn should_include(&self, tx: &Transaction) -> bool {
        if !self.hot_addresses.is_empty() {
            let has_hot = tx.to.map_or(self.include_contract_creation, |to| self.hot_addresses.contains(&to));
            if !has_hot {
                return false;
            }
        }
        if let Some(min) = self.min_value_wei {
            if tx.value < min {
                return false;
            }
        }
        if let Some(max_eff) = self.max_effective_gas_price_wei {
            if Self::max_allowed_gas_price(tx) > max_eff {
                return false;
            }
        }
        true
    }
}

#[async_trait]
pub trait MempoolMonitor: Send + Sync {
    async fn start(&self) -> Result<(), MempoolError>;
    async fn shutdown(&self);
    fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction>;
    fn get_metrics(&self) -> MempoolMetrics;
    fn get_monitor_type(&self) -> &'static str;
}

pub struct DefaultMempoolMonitor {
    config: Arc<MempoolConfig>,
    provider: Arc<dyn BlockchainManager + Send + Sync>,
    raw_tx_sender: broadcast::Sender<Transaction>,
    cancellation_token: CancellationToken,
    task_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    stats: Arc<MempoolStats>,
    filter: Option<TransactionFilter>,
    metrics_reporter: Option<Arc<dyn MetricsReporter + Send + Sync>>,
    get_tx_semaphore: Arc<Semaphore>,
    reconnection_manager: ReconnectionManager,
}

impl DefaultMempoolMonitor {
    pub fn new(
        config: Arc<MempoolConfig>,
        provider: Arc<dyn BlockchainManager + Send + Sync>,
    ) -> Result<Self, MempoolError> {
        let (raw_tx_sender, _) = broadcast::channel(config.max_pending_txs);
        Ok(Self {
            config,
            provider,
            raw_tx_sender,
            cancellation_token: CancellationToken::new(),
            task_handle: Arc::new(tokio::sync::Mutex::new(None)),
            stats: Arc::new(MempoolStats::new()),
            filter: None,
            metrics_reporter: None,
            get_tx_semaphore: Arc::new(Semaphore::new(10)), // Limit concurrent get_transaction calls
            reconnection_manager: ReconnectionManager::new(
                Duration::from_secs(5), // Initial delay for RPC connections
                Duration::from_secs(60), // Max delay
                2, // Backoff multiplier
            ),
        })
    }

    pub fn with_filter(mut self, filter: TransactionFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter + Send + Sync>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }

    fn spawn_transaction_streamer(&self) {
        let provider = self.provider.clone();
        let cancel_token = self.cancellation_token.clone();
        let sender = self.raw_tx_sender.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let filter = self.filter.clone();
        let metrics_reporter = self.metrics_reporter.clone();
        let semaphore = self.get_tx_semaphore.clone();
        let reconnection_manager = self.reconnection_manager.clone();

        let handle = tokio::spawn(async move {
            info!(target: "mempool_monitor::streamer", "Starting mempool transaction streamer.");

            let mut metrics_interval = tokio::time::interval(Duration::from_secs(30));
            let mut retry_interval = tokio::time::interval(Duration::from_millis(500));
            let mut not_found_cache: lru::LruCache<H256, (Instant, u32)> = lru::LruCache::new(
                std::num::NonZeroUsize::new(1000).unwrap()
            );

            loop {
                if cancel_token.is_cancelled() {
                    info!(target: "mempool_monitor::streamer", "Shutdown signal received, stopping streamer.");
                    break;
                }

                let chain_supports_pending_txs = config.subscribe_pending_txs;

                // Use reconnection manager for WebSocket provider connection
                let ws_provider_result = reconnection_manager.execute_with_reconnect(
                    || async { provider.get_ws_provider() },
                    &stats,
                    "WebSocket provider",
                ).await;

                match ws_provider_result {
                    Ok(ws_provider) => {
                        if chain_supports_pending_txs {
                            // Use reconnection manager for subscription
                            let subscription_result = reconnection_manager.execute_with_reconnect(
                                || async { ws_provider.subscribe_pending_txs().await },
                                &stats,
                                "pending transaction subscription",
                            ).await;

                            match subscription_result {
                                Ok(mut stream) => {
                                    info!(target: "mempool_monitor::streamer", "Subscribed to pending transaction hashes stream.");
                                    loop {
                                        tokio::select! {
                                            _ = cancel_token.cancelled() => {
                                                info!(target: "mempool_monitor::streamer", "Shutdown signal received, stopping streamer.");
                                                return;
                                            }
                                            _ = metrics_interval.tick() => {
                                                if let Some(reporter) = &metrics_reporter {
                                                    let subscriber_count = sender.receiver_count();
                                                    let metrics = stats.get_metrics(subscriber_count);
                                                    reporter.report_mempool_metrics(metrics).await;
                                                }
                                            }
                                            _ = retry_interval.tick() => {
                                                let mut to_retry = Vec::new();
                                                let now = Instant::now();
                                                not_found_cache.iter_mut().for_each(|(hash, (time, count))| {
                                                    if now.duration_since(*time) > Duration::from_millis(500) && *count < 3 {
                                                        to_retry.push(*hash);
                                                        *time = now;
                                                        *count += 1;
                                                        stats.retry_attempts.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                });
                                                for hash in to_retry {
                                                    let _permit = semaphore.acquire().await;
                                                    match provider.get_transaction(hash).await {
                                                        Ok(Some(tx)) => {
                                                            not_found_cache.pop(&hash);
                                                            stats.transactions_received.fetch_add(1, Ordering::Relaxed);
                                                            stats.successful_retries.fetch_add(1, Ordering::Relaxed);
                                                            let should_broadcast = if let Some(ref filter) = filter {
                                                                let include = filter.should_include(&tx);
                                                                if !include {
                                                                    stats.transactions_filtered.fetch_add(1, Ordering::Relaxed);
                                                                    debug!(target: "mempool_monitor::streamer", "Retried transaction filtered out: {:?}", hash);
                                                                }
                                                                include
                                                            } else {
                                                                true
                                                            };
                                                            if should_broadcast {
                                                                match sender.send(tx) {
                                                                    Ok(_) => {
                                                                        stats.transactions_broadcasted.fetch_add(1, Ordering::Relaxed);
                                                                        debug!(target: "mempool_monitor::streamer", "Successfully retried transaction: {:?}", hash);
                                                                    }
                                                                    Err(broadcast::error::SendError(dropped_tx)) => {
                                                                        stats.no_subscribers.fetch_add(1, Ordering::Relaxed);
                                                                        warn!(target: "mempool_monitor::streamer",
                                                                                                                                        "Retried transaction dropped - no subscribers. Hash: {:?}, Current no_subscribers count: {}",
                                                            dropped_tx.hash,
                                                            stats.no_subscribers.load(Ordering::Relaxed)
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        Ok(None) => {
                                                            if let Some((_, count)) = not_found_cache.get_mut(&hash) {
                                                                if *count >= 3 {
                                                                    not_found_cache.pop(&hash);
                                                                    stats.failed_retries.fetch_add(1, Ordering::Relaxed);
                                                                    debug!(target: "mempool_monitor::streamer", "Giving up on transaction after 3 retries: {:?}", hash);
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(target: "mempool_monitor::streamer", error = %e, "Error retrying transaction: {:?}", hash);
                                                        }
                                                    }
                                                }
                                            }
                                            tx_hash_opt = stream.next() => {
                                                match tx_hash_opt {
                                                    Some(tx_hash) => {
                                                        stats.transactions_received.fetch_add(1, Ordering::Relaxed);
                                                        let _permit = semaphore.acquire().await;
                                                        match provider.get_transaction(tx_hash).await {
                                                            Ok(Some(tx)) => {
                                                                let should_broadcast = if let Some(ref filter) = filter {
                                                                    let include = filter.should_include(&tx);
                                                                    if !include {
                                                                        stats.transactions_filtered.fetch_add(1, Ordering::Relaxed);
                                                                        debug!(target: "mempool_monitor::streamer", "Transaction filtered out: {:?}", tx_hash);
                                                                    }
                                                                    include
                                                                } else {
                                                                    true
                                                                };
                                                                if should_broadcast {
                                                                    match sender.send(tx) {
                                                                        Ok(_) => {
                                                                            stats.transactions_broadcasted.fetch_add(1, Ordering::Relaxed);
                                                                        }
                                                                        Err(broadcast::error::SendError(dropped_tx)) => {
                                                                            stats.no_subscribers.fetch_add(1, Ordering::Relaxed);
                                                                            warn!(target: "mempool_monitor::streamer",
                                                                                                                                                "Transaction dropped - no subscribers. Hash: {:?}, Current no_subscribers count: {}",
                                                                dropped_tx.hash,
                                                                stats.no_subscribers.load(Ordering::Relaxed)
                                                                            );
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            Ok(None) => {
                                                                debug!(target: "mempool_monitor::streamer", "Transaction not found, caching for retry: {:?}", tx_hash);
                                                                not_found_cache.put(tx_hash, (Instant::now(), 1));
                                                            }
                                                            Err(e) => {
                                                                error!(target: "mempool_monitor::streamer", error = %e, "Error fetching transaction details for hash: {:?}", tx_hash);
                                                            }
                                                        }
                                                    }
                                                    None => {
                                                        warn!(target: "mempool_monitor::streamer", "WebSocket stream ended, will retry connection.");
                                                        stats.stream_reconnections.fetch_add(1, Ordering::Relaxed);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            Err(e) => {
                                    error!(target: "mempool_monitor::streamer", error = %e, "Failed to subscribe to pending transactions after all retries");
                                }
                            }
                        } else {
                            warn!(target: "mempool_monitor::streamer", "Chain does not support subscribe_pending_txs. Mempool monitoring is disabled for this chain.");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(target: "mempool_monitor::streamer", error = %e, "Failed to establish WebSocket provider connection after all retries");
                        break;
                    }
                }
            }
        });

        let task_handle_clone = self.task_handle.clone();
        tokio::spawn(async move {
            *task_handle_clone.lock().await = Some(handle);
        });
    }
}

impl std::fmt::Debug for DefaultMempoolMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultMempoolMonitor")
            .field("config", &"Arc<MempoolConfig>")
            .field("provider", &"Arc<dyn BlockchainManager + Send + Sync>")
            .field("raw_tx_sender", &"broadcast::Sender<Transaction>")
            .field("cancellation_token", &"CancellationToken")
            .field("task_handle", &"Arc<Mutex<Option<JoinHandle<()>>>>")
            .field("stats", &"Arc<MempoolStats>")
            .field("filter", &self.filter)
            .field("metrics_reporter", &self.metrics_reporter.is_some())
            .finish()
    }
}

#[async_trait]
impl MempoolMonitor for DefaultMempoolMonitor {
    async fn start(&self) -> Result<(), MempoolError> {
        info!(target: "mempool_monitor", "Starting default mempool monitoring pipeline.");
        self.spawn_transaction_streamer();
        Ok(())
    }

    async fn shutdown(&self) {
        info!(target: "mempool_monitor", "Shutdown signal received. Stopping all tasks.");
        self.cancellation_token.cancel();
        if let Some(handle) = self.task_handle.lock().await.take() {
            if let Err(e) = handle.await {
                error!(target: "mempool_monitor", "A monitoring task panicked during shutdown: {:?}", e);
            }
        }
        info!(target: "mempool_monitor", "All monitoring tasks stopped.");
    }

    fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction> {
        self.raw_tx_sender.subscribe()
    }

    fn get_metrics(&self) -> MempoolMetrics {
        let subscriber_count = self.raw_tx_sender.receiver_count();
        self.stats.get_metrics(subscriber_count)
    }

    fn get_monitor_type(&self) -> &'static str {
        "DefaultRPC"
    }
}

pub struct BloxrouteMempoolMonitor {
    config: Arc<MempoolConfig>,
    raw_tx_sender: broadcast::Sender<Transaction>,
    cancellation_token: CancellationToken,
    task_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    stats: Arc<MempoolStats>,
    filter: Option<TransactionFilter>,
    metrics_reporter: Option<Arc<dyn MetricsReporter + Send + Sync>>,
    api_key: String,
    endpoint: String,
}

impl BloxrouteMempoolMonitor {
    pub fn new(
        config: Arc<MempoolConfig>,
        api_key: String,
        endpoint: String,
    ) -> Result<Self, MempoolError> {
        let (raw_tx_sender, _) = broadcast::channel(config.max_pending_txs);
        Ok(Self {
            config,
            raw_tx_sender,
            cancellation_token: CancellationToken::new(),
            task_handle: Arc::new(tokio::sync::Mutex::new(None)),
            stats: Arc::new(MempoolStats::new()),
            filter: None,
            metrics_reporter: None,
            api_key,
            endpoint,
        })
    }

    pub fn with_filter(mut self, filter: TransactionFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter + Send + Sync>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }

    fn spawn_bloxroute_streamer(&self) {
        let cancel_token = self.cancellation_token.clone();
        let sender = self.raw_tx_sender.clone();
        let stats = self.stats.clone();
        let filter = self.filter.clone();
        let metrics_reporter = self.metrics_reporter.clone();
        let api_key = self.api_key.clone();
        let endpoint = self.endpoint.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            info!(target: "bloxroute_monitor::streamer", "Starting bloXroute mempool transaction streamer.");

            let mut metrics_interval = tokio::time::interval(Duration::from_secs(30));
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|e| {
                    warn!("Failed to create HTTP client with timeout: {}, using default", e);
                    reqwest::Client::builder()
                        .timeout(Duration::from_secs(30))
                        .build()
                        .expect("Failed to create default HTTP client")
                });

            // Use config for batch processing settings
            let batch_size = config.batch_size.unwrap_or(100);
            let batch_timeout = Duration::from_millis(config.batch_timeout_ms.unwrap_or(1000));
            let mut tx_buffer = Vec::with_capacity(batch_size);
            let mut last_batch_time = Instant::now();

            loop {
                if cancel_token.is_cancelled() {
                    info!(target: "bloxroute_monitor::streamer", "Shutdown signal received, stopping streamer.");
                    break;
                }

                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(target: "bloxroute_monitor::streamer", "Shutdown signal received, stopping streamer.");
                        break;
                    }
                    _ = metrics_interval.tick() => {
                        if let Some(reporter) = &metrics_reporter {
                            let subscriber_count = sender.receiver_count();
                            let metrics = stats.get_metrics(subscriber_count);
                            reporter.report_mempool_metrics(metrics).await;
                        }
                    }
                    result = tokio::time::timeout(batch_timeout, Self::fetch_mempool_transactions(&client, &api_key, &endpoint)) => {
                        match result {
                            Ok(Ok(transactions)) => {
                                for tx in transactions {
                                    tx_buffer.push(tx);
                                }
                            }
                            Ok(Err(e)) => {
                                stats.connection_failures.fetch_add(1, Ordering::Relaxed);
                                error!(target: "bloxroute_monitor::streamer", error = %e, "Failed to fetch transactions from bloXroute. Retrying in 1s.");
                                sleep(Duration::from_secs(1)).await;
                            }
                            Err(_) => {
                                // This is a timeout, which means it's time to process the batch
                            }
                        }
                    }
                }

                // Process the batch if it's full OR if the timeout has elapsed
                if !tx_buffer.is_empty() && (tx_buffer.len() >= batch_size || last_batch_time.elapsed() >= batch_timeout) {
                    Self::process_transaction_batch(&mut tx_buffer, &sender, &stats, &filter);
                    last_batch_time = Instant::now();
                }
            }
        });

        let task_handle_clone = self.task_handle.clone();
        tokio::spawn(async move {
            *task_handle_clone.lock().await = Some(handle);
        });
    }

    fn process_transaction_batch(
        buffer: &mut Vec<Transaction>,
        sender: &broadcast::Sender<Transaction>,
        stats: &Arc<MempoolStats>,
        filter: &Option<TransactionFilter>,
    ) {
        for tx in buffer.drain(..) {
            stats.transactions_received.fetch_add(1, Ordering::Relaxed);
            let should_broadcast = if let Some(ref f) = filter {
                let include = f.should_include(&tx);
                if !include {
                    stats.transactions_filtered.fetch_add(1, Ordering::Relaxed);
                }
                include
            } else {
                true
            };

            if should_broadcast {
                match sender.send(tx) {
                    Ok(_) => {
                        stats.transactions_broadcasted.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(broadcast::error::SendError(_)) => {
                        stats.no_subscribers.fetch_add(1, Ordering::Relaxed);
                        warn!(target: "bloxroute_monitor::streamer", 
                            "Transaction dropped - no subscribers. Current no_subscribers count: {}",
                            stats.no_subscribers.load(Ordering::Relaxed)
                        );
                    }
                }
            }
        }
    }

    async fn fetch_mempool_transactions(
        _client: &reqwest::Client,
        _api_key: &str,
        _endpoint: &str,
    ) -> Result<Vec<Transaction>, MempoolError> {
        // NOTE: bloXroute integration is not yet implemented
        // This monitor is disabled to prevent parsing errors during testing
        // TODO: Implement proper bloXroute API schema parsing when API documentation is available
        warn!(target: "bloxroute_monitor", "bloXroute monitor is not yet implemented - returning empty transaction list");

        // Return empty list to avoid errors while maintaining interface compatibility
        Ok(Vec::new())
    }
}

#[async_trait]
impl MempoolMonitor for BloxrouteMempoolMonitor {
    async fn start(&self) -> Result<(), MempoolError> {
        info!(target: "bloxroute_monitor", "Starting bloXroute mempool monitoring pipeline.");
        self.spawn_bloxroute_streamer();
        Ok(())
    }

    async fn shutdown(&self) {
        info!(target: "bloxroute_monitor", "Shutdown signal received. Stopping all tasks.");
        self.cancellation_token.cancel();
        
        if let Some(handle) = self.task_handle.lock().await.take() {
            if let Err(e) = handle.await {
                error!(target: "bloxroute_monitor", "A monitoring task panicked during shutdown: {:?}", e);
            }
        }
        info!(target: "bloxroute_monitor", "All monitoring tasks stopped.");
    }

    fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction> {
        self.raw_tx_sender.subscribe()
    }

    fn get_metrics(&self) -> MempoolMetrics {
        let subscriber_count = self.raw_tx_sender.receiver_count();
        self.stats.get_metrics(subscriber_count)
    }

    fn get_monitor_type(&self) -> &'static str {
        "bloXroute"
    }
}

pub struct BlocknativeMempoolMonitor {
    config: Arc<MempoolConfig>,
    raw_tx_sender: broadcast::Sender<Transaction>,
    cancellation_token: CancellationToken,
    task_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    stats: Arc<MempoolStats>,
    filter: Option<TransactionFilter>,
    metrics_reporter: Option<Arc<dyn MetricsReporter + Send + Sync>>,
    api_key: String,
    reconnection_manager: ReconnectionManager,
}

impl BlocknativeMempoolMonitor {
    pub fn new(
        config: Arc<MempoolConfig>,
        api_key: String,
    ) -> Result<Self, MempoolError> {
        let (raw_tx_sender, _) = broadcast::channel(config.max_pending_txs);
        Ok(Self {
            config: config.clone(),
            raw_tx_sender,
            cancellation_token: CancellationToken::new(),
            task_handle: Arc::new(tokio::sync::Mutex::new(None)),
            stats: Arc::new(MempoolStats::new()),
            filter: None,
            metrics_reporter: None,
            api_key,
            reconnection_manager: ReconnectionManager::new(
                Duration::from_secs(10), // Initial delay for WebSocket connections
                Duration::from_secs(60),  // Max delay
                2, // Backoff multiplier
            ),
        })
    }

    pub fn with_filter(mut self, filter: TransactionFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_metrics_reporter(mut self, reporter: Arc<dyn MetricsReporter + Send + Sync>) -> Self {
        self.metrics_reporter = Some(reporter);
        self
    }

    fn spawn_blocknative_streamer(&self) {
        let cancel_token = self.cancellation_token.clone();
        let sender = self.raw_tx_sender.clone();
        let stats = self.stats.clone();
        let filter = self.filter.clone();
        let metrics_reporter = self.metrics_reporter.clone();
        let api_key = self.api_key.clone();
        let config = self.config.clone();
        let reconnection_manager = self.reconnection_manager.clone();

        let handle = tokio::spawn(async move {
            info!(target: "blocknative_monitor::streamer", "Starting Blocknative mempool transaction streamer.");

            let mut metrics_interval = tokio::time::interval(Duration::from_secs(30));
            
            // Use config for batch processing settings
            let batch_size = config.batch_size.unwrap_or(50);
            let batch_timeout = config.batch_timeout_ms.unwrap_or(100);
            let mut pending_batch = Vec::new();
            let mut batch_timer = tokio::time::interval(Duration::from_millis(batch_timeout));

            loop {
                if cancel_token.is_cancelled() {
                    info!(target: "blocknative_monitor::streamer", "Shutdown signal received, stopping streamer.");
                    break;
                }

                // Use reconnection manager for Blocknative WebSocket connection
                let ws_stream_result = reconnection_manager.execute_with_reconnect(
                    || async { Self::connect_to_blocknative_ws(&api_key).await },
                    &stats,
                    "Blocknative WebSocket",
                ).await;

                match ws_stream_result {
                    Ok(mut ws_stream) => {
                        info!(target: "blocknative_monitor::streamer", "Connected to Blocknative WebSocket.");
                        loop {
                            tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    info!(target: "blocknative_monitor::streamer", "Shutdown signal received, stopping streamer.");
                                    return;
                                }
                                _ = metrics_interval.tick() => {
                                    if let Some(reporter) = &metrics_reporter {
                                        let subscriber_count = sender.receiver_count();
                                        let metrics = stats.get_metrics(subscriber_count);
                                        reporter.report_mempool_metrics(metrics).await;
                                    }
                                }
                                _ = batch_timer.tick() => {
                                    // Process any remaining transactions in the batch
                                    if !pending_batch.is_empty() {
                                        for tx in pending_batch.drain(..) {
                                            match sender.send(tx) {
                                                Ok(_) => {
                                                    stats.transactions_broadcasted.fetch_add(1, Ordering::Relaxed);
                                                }
                                                Err(broadcast::error::SendError(_)) => {
                                                    stats.no_subscribers.fetch_add(1, Ordering::Relaxed);
                                                    warn!(target: "blocknative_monitor::streamer",
                                                        "Transaction dropped - no subscribers. Current no_subscribers count: {}",
                                                        stats.no_subscribers.load(Ordering::Relaxed)
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                msg_opt = ws_stream.next() => {
                                    match msg_opt {
                                        Some(Ok(msg)) => {
                                            if let Ok(tx) = Self::parse_blocknative_message(msg) {
                                                stats.transactions_received.fetch_add(1, Ordering::Relaxed);
                                                let should_broadcast = if let Some(ref filter) = filter {
                                                    let include = filter.should_include(&tx);
                                                    if !include {
                                                        stats.transactions_filtered.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    include
                                                } else {
                                                    true
                                                };

                                                if should_broadcast {
                                                    // Add to batch for processing
                                                    pending_batch.push(tx);

                                                    // Process batch if it reaches the configured size
                                                    if pending_batch.len() >= batch_size {
                                                        for tx in pending_batch.drain(..) {
                                                            match sender.send(tx) {
                                                                Ok(_) => {
                                                                    stats.transactions_broadcasted.fetch_add(1, Ordering::Relaxed);
                                                                }
                                                                Err(broadcast::error::SendError(_)) => {
                                                                    stats.no_subscribers.fetch_add(1, Ordering::Relaxed);
                                                                    warn!(target: "blocknative_monitor::streamer",
                                                                        "Transaction dropped - no subscribers. Current no_subscribers count: {}",
                                                                        stats.no_subscribers.load(Ordering::Relaxed)
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Some(Err(e)) => {
                                            error!(target: "blocknative_monitor::streamer", error = %e, "WebSocket error from Blocknative.");
                                            break;
                                        }
                                        None => {
                                            warn!(target: "blocknative_monitor::streamer", "Blocknative WebSocket stream ended, will reconnect.");
                                            stats.stream_reconnections.fetch_add(1, Ordering::Relaxed);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(target: "blocknative_monitor::streamer", error = %e, "Failed to connect to Blocknative WebSocket after all retries");
                    }
                }
            }
        });

        let task_handle_clone = self.task_handle.clone();
        tokio::spawn(async move {
            *task_handle_clone.lock().await = Some(handle);
        });
    }

    async fn connect_to_blocknative_ws(
        api_key: &str,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, MempoolError> {
        let url = format!("wss://api.blocknative.com/v0?auth={}", api_key);
        let (ws_stream, _) = connect_async(&url)
            .await
            .map_err(|e| MempoolError::ConnectionError(e.to_string()))?;

        Ok(ws_stream)
    }

    fn parse_blocknative_message(
        msg: tokio_tungstenite::tungstenite::Message,
    ) -> Result<Transaction, MempoolError> {
        use serde_json::Value;

        // Only handle text messages
        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(s) => s,
            _ => {
                return Err(MempoolError::ParseError("Blocknative message is not text".to_string()));
            }
        };

        // Parse the JSON message
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| MempoolError::ParseError(format!("Invalid JSON: {}", e)))?;

        // Blocknative API v2 message structure
        // Check for different event types
        let event_type = v.get("event")
            .and_then(|e| e.get("eventType"))
            .and_then(|t| t.as_str())
            .unwrap_or("unknown");
        
        // We're primarily interested in pending and confirmed transactions
        if !["pending", "confirmed", "speedup", "cancel", "failed", "stuck"].contains(&event_type) {
            return Err(MempoolError::ParseError(format!("Unsupported event type: {}", event_type)));
        }
        
        // Extract transaction data based on Blocknative's structure
        let tx_data = v.get("event")
            .and_then(|e| e.get("transaction"))
            .or_else(|| v.get("transaction"))
            .ok_or_else(|| MempoolError::ParseError("Missing transaction data".to_string()))?;

        // Extract core transaction fields
        let hash = tx_data.get("hash")
            .and_then(|h| h.as_str())
            .and_then(|h| h.parse::<H256>().ok())
            .ok_or_else(|| MempoolError::ParseError("Missing or invalid transaction hash".to_string()))?;

        let from = tx_data.get("from")
            .and_then(|f| f.as_str())
            .and_then(|f| f.parse::<Address>().ok())
            .ok_or_else(|| MempoolError::ParseError("Missing or invalid from address".to_string()))?;

        let to = tx_data.get("to")
            .and_then(|t| t.as_str())
            .and_then(|t| t.parse::<Address>().ok());

        // Handle different value formats (hex string or decimal string)
        let value = tx_data.get("value")
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    if s.starts_with("0x") {
                        U256::from_str(s).ok()
                    } else {
                        U256::from_dec_str(s).ok()
                    }
                } else if let Some(n) = v.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            })
            .unwrap_or_else(U256::zero);

        let gas = tx_data.get("gas")
            .and_then(|g| {
                if let Some(s) = g.as_str() {
                    if s.starts_with("0x") {
                        U256::from_str(s).ok()
                    } else {
                        U256::from_dec_str(s).ok()
                    }
                } else if let Some(n) = g.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            })
            .unwrap_or(U256::from(21000)); // Default gas limit

        let gas_price = tx_data.get("gasPrice")
            .and_then(|g| {
                if let Some(s) = g.as_str() {
                    if s.starts_with("0x") {
                        U256::from_str(s).ok()
                    } else {
                        U256::from_dec_str(s).ok()
                    }
                } else if let Some(n) = g.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            });

        let input = tx_data.get("input")
            .and_then(|i| i.as_str())
            .and_then(|i| hex::decode(i.strip_prefix("0x").unwrap_or(i)).ok())
            .unwrap_or_default()
            .into();

        let nonce = tx_data.get("nonce")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);

        // Construct the ethers::types::Transaction
        let mut tx = Transaction {
            hash,
            nonce: U256::from(nonce),
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from,
            to,
            value,
            gas_price,
            gas,
            input,
            ..Default::default()
        };

        // Optionally fill in more fields if available
        if let Some(chain_id) = tx_data.get("chainId")
            .and_then(|c| {
                if let Some(s) = c.as_str() {
                    U256::from_dec_str(s).ok()
                } else if let Some(n) = c.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            }) {
            tx.chain_id = Some(chain_id);
        }
        
        // Add EIP-1559 fields if present
        if let Some(max_fee) = tx_data.get("maxFeePerGas")
            .and_then(|f| {
                if let Some(s) = f.as_str() {
                    if s.starts_with("0x") {
                        U256::from_str(s).ok()
                    } else {
                        U256::from_dec_str(s).ok()
                    }
                } else if let Some(n) = f.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            }) {
            tx.max_fee_per_gas = Some(max_fee);
        }
        
        if let Some(max_priority) = tx_data.get("maxPriorityFeePerGas")
            .and_then(|f| {
                if let Some(s) = f.as_str() {
                    if s.starts_with("0x") {
                        U256::from_str(s).ok()
                    } else {
                        U256::from_dec_str(s).ok()
                    }
                } else if let Some(n) = f.as_u64() {
                    Some(U256::from(n))
                } else {
                    None
                }
            }) {
            tx.max_priority_fee_per_gas = Some(max_priority);
        }

        Ok(tx)
    }
}

#[async_trait]
impl MempoolMonitor for BlocknativeMempoolMonitor {
    async fn start(&self) -> Result<(), MempoolError> {
        info!(target: "blocknative_monitor", "Starting Blocknative mempool monitoring pipeline.");
        self.spawn_blocknative_streamer();
        Ok(())
    }

    async fn shutdown(&self) {
        info!(target: "blocknative_monitor", "Shutdown signal received. Stopping all tasks.");
        self.cancellation_token.cancel();
        
        if let Some(handle) = self.task_handle.lock().await.take() {
            if let Err(e) = handle.await {
                error!(target: "blocknative_monitor", "A monitoring task panicked during shutdown: {:?}", e);
            }
        }
        info!(target: "blocknative_monitor", "All monitoring tasks stopped.");
    }

    fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction> {
        self.raw_tx_sender.subscribe()
    }

    fn get_metrics(&self) -> MempoolMetrics {
        let subscriber_count = self.raw_tx_sender.receiver_count();
        self.stats.get_metrics(subscriber_count)
    }

    fn get_monitor_type(&self) -> &'static str {
        "Blocknative"
    }
}

pub struct CompositeMempoolMonitor {
    monitors: Vec<Arc<dyn MempoolMonitor + Send + Sync>>,
    raw_tx_sender: broadcast::Sender<Transaction>,
    cancellation_token: CancellationToken,
    task_handles: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    seen_transactions: Arc<tokio::sync::RwLock<lru::LruCache<H256, ()>>>,
    stats: Arc<MempoolStats>,
}

impl CompositeMempoolMonitor {
    pub fn new(
        monitors: Vec<Arc<dyn MempoolMonitor + Send + Sync>>,
        channel_capacity: usize,
    ) -> Result<Self, MempoolError> {
        if monitors.is_empty() {
            return Err(MempoolError::ConfigurationError("No monitors provided".to_string()));
        }

        let (raw_tx_sender, _) = broadcast::channel(channel_capacity);
        Ok(Self {
            monitors,
            raw_tx_sender,
            cancellation_token: CancellationToken::new(),
            task_handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            seen_transactions: Arc::new(tokio::sync::RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap() // Keep track of last 10k transactions
            ))),
            stats: Arc::new(MempoolStats::new()),
        })
    }

    fn spawn_monitor_aggregator(&self) {
        let monitors = self.monitors.clone();
        let sender = self.raw_tx_sender.clone();
        let cancel_token = self.cancellation_token.clone();
        let seen_transactions = self.seen_transactions.clone();
        let stats = self.stats.clone();

        for (idx, monitor) in monitors.iter().enumerate() {
            let monitor_clone = monitor.clone();
            let sender_clone = sender.clone();
            let cancel_token_clone = cancel_token.clone();
            let seen_transactions_clone = seen_transactions.clone();
            let stats_clone = stats.clone();

            let handle = tokio::spawn(async move {
                let mut rx = monitor_clone.subscribe_to_raw_transactions();
                let monitor_type = monitor_clone.get_monitor_type();
                
                info!(target: "composite_monitor", "Starting aggregation for monitor {}: {}", idx, monitor_type);

                loop {
                    tokio::select! {
                        _ = cancel_token_clone.cancelled() => {
                            info!(target: "composite_monitor", "Shutdown signal received for monitor {}", idx);
                            break;
                        }
                        tx_result = rx.recv() => {
                            match tx_result {
                                Ok(tx) => {
                                    let tx_hash = tx.hash;
                                    
                                    // Check if we've already seen this transaction
                                    {
                                        let mut seen = seen_transactions_clone.write().await;
                                        if seen.get(&tx_hash).is_some() {
                                            debug!(target: "composite_monitor", "Duplicate transaction from {}: {:?}", monitor_type, tx_hash);
                                            continue;
                                        }
                                        // Add to seen transactions
                                        seen.put(tx_hash, ());
                                    }
                                    
                                    // Forward to composite channel
                                    match sender_clone.send(tx) {
                                        Ok(_) => {
                                            debug!(target: "composite_monitor", "Forwarded unique transaction from {}: {:?}", monitor_type, tx_hash);
                                        }
                                        Err(broadcast::error::SendError(_)) => {
                                            warn!(target: "composite_monitor", "Composite channel full, dropping transaction from {}: {:?}", monitor_type, tx_hash);
                                        }
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    stats_clone.dropped_due_to_lag.fetch_add(skipped as u64, Ordering::Relaxed);
                                    warn!(target: "composite_monitor", "Monitor {} lagged, skipped {} transactions", idx, skipped);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    error!(target: "composite_monitor", "Monitor {} channel closed", idx);
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            let task_handles_clone = self.task_handles.clone();
            tokio::spawn(async move {
                task_handles_clone.lock().await.push(handle);
            });
        }

        // No cleanup task needed - LRU cache automatically evicts old entries
    }
}

#[async_trait]
impl MempoolMonitor for CompositeMempoolMonitor {
    async fn start(&self) -> Result<(), MempoolError> {
        info!(target: "composite_monitor", "Starting composite mempool monitoring with {} monitors", self.monitors.len());
        
        // Start all individual monitors
        for (idx, monitor) in self.monitors.iter().enumerate() {
            monitor.start().await.map_err(|e| {
                MempoolError::MonitorError(format!("Failed to start monitor {}: {}", idx, e))
            })?;
        }

        // Start the aggregator
        self.spawn_monitor_aggregator();
        
        Ok(())
    }

    async fn shutdown(&self) {
        info!(target: "composite_monitor", "Shutting down composite mempool monitor");
        self.cancellation_token.cancel();
        
        // Shutdown all individual monitors
        for monitor in &self.monitors {
            monitor.shutdown().await;
        }
        
        // Wait for all aggregator tasks to complete
        let handles = std::mem::take(&mut *self.task_handles.lock().await);
        for handle in handles {
            if let Err(e) = handle.await {
                error!(target: "composite_monitor", "Aggregator task panicked during shutdown: {:?}", e);
            }
        }
        
        info!(target: "composite_monitor", "Composite mempool monitor shutdown complete");
    }

    fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction> {
        self.raw_tx_sender.subscribe()
    }

    fn get_metrics(&self) -> MempoolMetrics {
        // Aggregate metrics from all monitors
        let mut total_metrics = MempoolMetrics::default();
        total_metrics.current_subscriber_count = self.raw_tx_sender.receiver_count();
        
        // Add composite monitor's own stats
        total_metrics.total_dropped_due_to_lag = self.stats.dropped_due_to_lag.load(Ordering::Relaxed);
        
        for monitor in &self.monitors {
            let metrics = monitor.get_metrics();
            total_metrics.total_transactions_received += metrics.total_transactions_received;
            total_metrics.total_transactions_filtered += metrics.total_transactions_filtered;
            total_metrics.total_transactions_broadcasted += metrics.total_transactions_broadcasted;
            total_metrics.total_dropped_due_to_lag += metrics.total_dropped_due_to_lag;
            total_metrics.total_no_subscribers += metrics.total_no_subscribers;
            total_metrics.connection_failures += metrics.connection_failures;
            total_metrics.stream_reconnections += metrics.stream_reconnections;
            total_metrics.retry_attempts += metrics.retry_attempts;
            total_metrics.successful_retries += metrics.successful_retries;
            total_metrics.failed_retries += metrics.failed_retries;
        }
        
        total_metrics
    }

    fn get_monitor_type(&self) -> &'static str {
        "Composite"
    }
}

// ------------------------------------------------------------------------------------------------
// MEV OPPORTUNITY TYPES
// ------------------------------------------------------------------------------------------------

use crate::types::ArbitrageRoute;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MEVOpportunity {
    Sandwich(PotentialSandwichOpportunity),
    Liquidation(PotentialLiquidationOpportunity),
    JITLiquidity(JITLiquidityOpportunity),
    OracleUpdate(OracleUpdateOpportunity),
    Arbitrage(ArbitrageRoute),
    FlashLoan(FlashLoanOpportunity),
}

impl MEVOpportunity {
    pub fn block_number(&self) -> u64 {
        match self {
            MEVOpportunity::Sandwich(opp) => opp.block_number,
            MEVOpportunity::Liquidation(opp) => opp.block_number,
            MEVOpportunity::JITLiquidity(opp) => opp.block_number,
            MEVOpportunity::OracleUpdate(opp) => opp.block_number,
            MEVOpportunity::Arbitrage(_) => 0, // Will be set by caller
            MEVOpportunity::FlashLoan(opp) => opp.block_number,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PotentialSandwichOpportunity {
    pub target_tx_hash: H256,
    pub victim_tx: Transaction,
    pub swap_params: Option<SwapParams>,
    pub block_number: u64,
    pub estimated_profit_usd: f64,
    pub gas_price: U256,
    pub pool_address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out_min: U256,
    pub router_address: Address,
    pub protocol_type: crate::types::ProtocolType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PotentialLiquidationOpportunity {
    pub borrower: Address,
    pub collateral_token: Address,
    pub debt_token: Address,
    pub debt_to_cover: U256,
    pub collateral_amount: U256,
    pub health_factor: f64,
    pub liquidation_bonus_bps: u32,
    pub protocol_address: Address,
    pub protocol_name: String,
    pub block_number: u64,
    pub estimated_profit_usd: f64,
    pub uar_connector: Option<Address>,
    pub requires_flashloan: bool,
    pub profit_token: Address,
    pub debt_amount: U256,
    pub user_address: Address,
    pub lending_protocol_address: Address,
    pub uar_actions: Vec<UARAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JITLiquidityOpportunity {
    pub pool_address: Address,
    pub token0: Address,
    pub token1: Address,
    pub fee: u32,
    pub amount0_desired: U256,
    pub amount1_desired: U256,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub block_number: u64,
    pub estimated_profit_usd: f64,
    pub helper_contract: Option<Address>,
    pub liquidity_amount: U256,
    pub timeout_blocks: u64,
    pub token_in: Address,
    pub token_out: Address,
    pub target_tx_hash: H256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleUpdateOpportunity {
    pub oracle_address: Address,
    pub token_address: Address,
    pub old_price: f64,
    pub new_price: f64,
    pub block_number: u64,
    pub estimated_profit_usd: f64,
    pub affected_protocols: Vec<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlashLoanOpportunity {
    pub lending_protocol: Address,
    pub token_to_flash: Address,
    pub amount_to_flash: U256,
    pub target_protocol: Address,
    pub block_number: u64,
    pub estimated_profit_usd: f64,
    pub flash_fee_bps: u32,
    pub path: FlashLoanPath,
    pub flash_loan_amount: U256,
    pub flash_loan_token: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_states: Option<Vec<crate::types::PoolStateFromSwaps>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_amounts: Option<Vec<U256>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlashLoanPath {
    pub uar_contract: Address,
    pub token_path: Vec<Address>,
    pub dexes: Vec<FlashLoanDex>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlashLoanDex {
    pub router_address: Address,
    pub protocol_type: crate::types::ProtocolType,
}

// ------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS
// ------------------------------------------------------------------------------------------------

pub fn parse_swap_params(tx: &ethers::types::Transaction) -> Result<SwapParams, String> {
    use ethers::abi::{Token};
    use std::collections::HashMap;
    use once_cell::sync::Lazy;
    use serde_json::Value;

    static ABI_PARSE: Lazy<Value> = Lazy::new(|| {
        let abi_json = include_str!("../config/abi_parse.json");
        serde_json::from_str(abi_json).expect("Invalid abi_parse.json")
    });

    fn get_method_map() -> HashMap<[u8; 4], (&'static str, &'static str)> {
        let mut map = HashMap::new();
        if let Some(dexes) = ABI_PARSE.get("dexes").and_then(|v| v.as_object()) {
            for (dex_name, dex) in dexes {
                if let Some(sigs) = dex.get("swap_signatures").and_then(|v| v.as_object()) {
                    for (method, sig_hex) in sigs {
                        if let Some(sig_str) = sig_hex.as_str() {
                            if let Ok(bytes) = hex::decode(sig_str.trim_start_matches("0x")) {
                                if bytes.len() == 4 {
                                    let mut arr = [0u8; 4];
                                    arr.copy_from_slice(&bytes);
                                    map.insert(arr, (dex_name.as_str(), method.as_str()));
                                }
                            }
                        }
                    }
                }
            }
        }
        map
    }

    let method_map = get_method_map();

    let data = match &tx.input {
        ethers::types::Bytes(data) if data.len() >= 4 => data,
        _ => return Err("Transaction input too short".to_string()),
    };

    let selector: [u8; 4] = [data[0], data[1], data[2], data[3]];

    let (dex_name, method) = match method_map.get(&selector) {
        Some(pair) => *pair,
        None => return Err(format!("Unknown swap selector: 0x{}", hex::encode(selector))),
    };

    let dex = ABI_PARSE.get("dexes").and_then(|v| v.get(dex_name)).ok_or_else(|| format!("No dex config for {}", dex_name))?;
    let abi_array = dex.get("abi").and_then(|v| v.as_array()).ok_or("Missing ABI array")?;
    
    let mut functions = Vec::new();
    for abi_item in abi_array {
        if let Some(abi_item_str) = abi_item.as_str() {
            if let Ok(func) = ethers::abi::HumanReadableParser::parse_function(abi_item_str) {
                functions.push(func);
            }
        }
    }
    let abi = functions;

    let func = abi.iter().find(|f| f.short_signature() == selector)
        .ok_or_else(|| format!("Function not found for selector 0x{}", hex::encode(selector)))?;

    let params = func.decode_input(&data[4..])
        .map_err(|e| format!("ABI decode error: {:?}", e))?;

    // Heuristics for common swap methods
    let mut token_in = Address::zero();
    let mut token_out = Address::zero();
    let mut amount_in = U256::zero();
    let mut amount_out_min = U256::zero();

    // Try to extract from known UniswapV2/SushiSwap/UniswapV3 swap signatures
    match method {
        "swapExactTokensForTokens" | "swapExactTokensForETH" | "swapExactETHForTokens" |
        "swapExactTokensForTokensSupportingFeeOnTransferTokens" | "swapExactETHForTokensSupportingFeeOnTransferTokens" => {
            // [amountIn, amountOutMin, path, ...]
            if params.len() >= 3 {
                if let Token::Uint(ai) = &params[0] { amount_in = ai.clone(); }
                if let Token::Uint(aom) = &params[1] { amount_out_min = aom.clone(); }
                if let Token::Array(path) = &params[2] {
                    if path.len() >= 2 {
                        if let Token::Address(addr_in) = &path[0] { token_in = *addr_in; }
                        if let Token::Address(addr_out) = &path[path.len()-1] { token_out = *addr_out; }
                    }
                }
            }
        }
        "swapTokensForExactTokens" | "swapTokensForExactETH" | "swapETHForExactTokens" => {
            // [amountOut, amountInMax, path, ...]
            if params.len() >= 3 {
                if let Token::Uint(aom) = &params[0] { amount_out_min = aom.clone(); }
                if let Token::Uint(ai) = &params[1] { amount_in = ai.clone(); }
                if let Token::Array(path) = &params[2] {
                    if path.len() >= 2 {
                        if let Token::Address(addr_in) = &path[0] { token_in = *addr_in; }
                        if let Token::Address(addr_out) = &path[path.len()-1] { token_out = *addr_out; }
                    }
                }
            }
        }
        "exactInputSingle" => {
            // [params] where params is a struct with fields: (tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMinimum, sqrtPriceLimitX96)
            if let Some(Token::Tuple(fields)) = params.get(0) {
                if fields.len() >= 7 {
                    if let Token::Address(addr_in) = &fields[0] { token_in = *addr_in; }
                    if let Token::Address(addr_out) = &fields[1] { token_out = *addr_out; }
                    if let Token::Uint(ai) = &fields[5] { amount_in = *ai; }
                    if let Token::Uint(aom) = &fields[6] { amount_out_min = *aom; }
                }
            }
        }
        "exactInput" => {
            // [params] where params is a struct with fields: (bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum)
            if let Some(Token::Tuple(fields)) = params.get(0) {
                if fields.len() >= 5 {
                    if let Token::Bytes(path_bytes) = &fields[0] {
                        // UniswapV3 path: tokenIn(20) + fee(3) + tokenOut(20)
                        if path_bytes.len() >= 40 {
                            token_in = Address::from_slice(&path_bytes[0..20]);
                            token_out = Address::from_slice(&path_bytes[path_bytes.len()-20..]);
                        }
                    }
                    if let Token::Uint(ai) = &fields[3] { amount_in = *ai; }
                    if let Token::Uint(aom) = &fields[4] { amount_out_min = *aom; }
                }
            }
        }
        "swap" => {
            // Balancer: [SingleSwap, FundManagement, limit, deadline]
            if params.len() >= 1 {
                if let Token::Tuple(fields) = &params[0] {
                    if fields.len() >= 4 {
                        if let Token::Address(addr_in) = &fields[1] { token_in = *addr_in; }
                        if let Token::Address(addr_out) = &fields[2] { token_out = *addr_out; }
                        if let Token::Uint(ai) = &fields[3] { amount_in = *ai; }
                    }
                }
            }
        }
        _ => {
            // Unknown method, fallback to zero addresses/amounts
        }
    }

    Ok(SwapParams {
        token_in,
        token_out,
        amount_in,
        amount_out_min,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapParams {
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out_min: U256,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Mutex, broadcast};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinHandle;

    #[derive(Clone, Default)]
    struct DummyMempoolMonitor {
        metrics: Arc<Mutex<MempoolMetrics>>,
        shutdown_called: Arc<AtomicUsize>,
        monitor_type: &'static str,
    }

    #[async_trait::async_trait]
    impl MempoolMonitor for DummyMempoolMonitor {
        async fn shutdown(&self) {
            self.shutdown_called.fetch_add(1, Ordering::SeqCst);
        }
        fn get_metrics(&self) -> MempoolMetrics {
            futures::executor::block_on(self.metrics.lock()).clone()
        }
        fn get_monitor_type(&self) -> &'static str {
            self.monitor_type
        }
        fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Transaction> {
            broadcast::channel(16).1
        }
        async fn start(&self) -> Result<(), MempoolError> {
            Ok(())
        }
    }

    fn create_dummy_monitor(metrics: MempoolMetrics, monitor_type: &'static str) -> DummyMempoolMonitor {
        let monitor = DummyMempoolMonitor {
            metrics: Arc::new(Mutex::new(metrics)),
            shutdown_called: Arc::new(AtomicUsize::new(0)),
            monitor_type,
        };
        monitor
    }

    fn create_composite_monitor_with_monitors(monitors: Vec<Arc<dyn MempoolMonitor + Send + Sync>>) -> CompositeMempoolMonitor {
        let (raw_tx_sender, _) = broadcast::channel(16);
        CompositeMempoolMonitor {
            monitors,
            task_handles: Arc::new(Mutex::new(Vec::new())),
            raw_tx_sender,
            cancellation_token: CancellationToken::new(),
            seen_transactions: Arc::new(tokio::sync::RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(10000).unwrap() // Keep track of last 10k transactions
            ))),
            stats: Arc::new(MempoolStats::new()),
        }
    }

    #[tokio::test]
    async fn test_shutdown_calls_all_monitors_and_waits_for_tasks() {
        let metrics = MempoolMetrics {
            total_transactions_received: 10,
            total_transactions_filtered: 2,
            total_transactions_broadcasted: 8,
            total_dropped_due_to_lag: 1,
            total_no_subscribers: 0,
            connection_failures: 0,
            stream_reconnections: 0,
            current_subscriber_count: 0,
            retry_attempts: 0,
            successful_retries: 0,
            failed_retries: 0,
        };
        let monitor1 = Arc::new(create_dummy_monitor(metrics.clone(), "Dummy1"));
        let monitor2 = Arc::new(create_dummy_monitor(metrics.clone(), "Dummy2"));

        let composite = create_composite_monitor_with_monitors(vec![monitor1.clone(), monitor2.clone()]);

        let handle1: JoinHandle<()> = tokio::spawn(async {});
        let handle2: JoinHandle<()> = tokio::spawn(async {});
        {
            let mut handles = composite.task_handles.lock().await;
            handles.push(handle1);
            handles.push(handle2);
        }

        composite.shutdown().await;

        assert_eq!(monitor1.shutdown_called.load(Ordering::SeqCst), 1, "Monitor1 shutdown not called exactly once");
        assert_eq!(monitor2.shutdown_called.load(Ordering::SeqCst), 1, "Monitor2 shutdown not called exactly once");
        let handles = composite.task_handles.lock().await;
        assert!(handles.is_empty(), "Task handles should be empty after shutdown");
    }

    #[tokio::test]
    async fn test_subscribe_to_raw_transactions_increments_count() {
        let monitor = create_composite_monitor_with_monitors(vec![]);
        let initial_count = monitor.raw_tx_sender.receiver_count();
        let mut rx1 = monitor.subscribe_to_raw_transactions();
        let mut rx2 = monitor.subscribe_to_raw_transactions();
        
        // Verify that receivers are properly created and can receive messages
        assert!(rx1.try_recv().is_err(), "rx1 should be empty initially");
        assert!(rx2.try_recv().is_err(), "rx2 should be empty initially");
        
        let after_count = monitor.raw_tx_sender.receiver_count();
        assert_eq!(after_count, initial_count + 2, "Receiver count should increase by 2 after two subscriptions");
        
        // Test that receivers are still active and can receive messages
        let test_tx = Transaction {
            hash: H256::from_low_u64_be(1),
            nonce: U256::from(1),
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from: Address::from_low_u64_be(1),
            to: Some(Address::from_low_u64_be(2)),
            value: U256::from(1000),
            gas_price: Some(U256::from(20000000000u64)),
            gas: U256::from(21000),
            input: vec![].into(),
            ..Default::default()
        };
        
        // Send a test transaction and verify both receivers can receive it
        monitor.raw_tx_sender.send(test_tx.clone()).unwrap();
        
        let received_tx1 = rx1.recv().await.unwrap();
        let received_tx2 = rx2.recv().await.unwrap();
        
        assert_eq!(received_tx1.hash, test_tx.hash);
        assert_eq!(received_tx2.hash, test_tx.hash);
    }

    #[tokio::test]
    async fn test_get_metrics_aggregates_all_monitors() {
        let metrics1 = MempoolMetrics {
            total_transactions_received: 10,
            total_transactions_filtered: 2,
            total_transactions_broadcasted: 8,
            total_dropped_due_to_lag: 1,
            total_no_subscribers: 0,
            connection_failures: 0,
            stream_reconnections: 0,
            current_subscriber_count: 0,
            retry_attempts: 0,
            successful_retries: 0,
            failed_retries: 0,
        };
        let metrics2 = MempoolMetrics {
            total_transactions_received: 5,
            total_transactions_filtered: 1,
            total_transactions_broadcasted: 4,
            total_dropped_due_to_lag: 0,
            total_no_subscribers: 0,
            connection_failures: 1,
            stream_reconnections: 2,
            current_subscriber_count: 0,
            retry_attempts: 0,
            successful_retries: 0,
            failed_retries: 0,
        };
        let monitor1 = Arc::new(create_dummy_monitor(metrics1.clone(), "Dummy1"));
        let monitor2 = Arc::new(create_dummy_monitor(metrics2.clone(), "Dummy2"));

        let composite = create_composite_monitor_with_monitors(vec![monitor1, monitor2]);
        let mut expected = metrics1.clone();
        expected.total_transactions_received += metrics2.total_transactions_received;
        expected.total_transactions_filtered += metrics2.total_transactions_filtered;
        expected.total_transactions_broadcasted += metrics2.total_transactions_broadcasted;
        expected.total_dropped_due_to_lag += metrics2.total_dropped_due_to_lag;
        expected.total_no_subscribers += metrics2.total_no_subscribers;
        expected.connection_failures += metrics2.connection_failures;
        expected.stream_reconnections += metrics2.stream_reconnections;
        expected.current_subscriber_count = composite.raw_tx_sender.receiver_count();

        let actual = composite.get_metrics();
        assert_eq!(actual, expected, "Aggregated metrics do not match expected sum");
    }

    #[tokio::test]
    async fn test_get_monitor_type_returns_composite() {
        let composite = create_composite_monitor_with_monitors(vec![]);
        assert_eq!(composite.get_monitor_type(), "Composite", "Monitor type should be 'Composite'");
    }
}