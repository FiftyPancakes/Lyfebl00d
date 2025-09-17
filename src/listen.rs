// src/listen.rs


use crate::{
    config::{EndpointConfig, PerChainConfig},
    errors::ListenerError,
    types::{ChainEvent, ChainType, ConnectionStatus, ProviderFactory, WrappedProvider},
};
use ethers::{
    providers::{Middleware, Ws},
    types::Filter,
};
use futures::{future::join_all, StreamExt};
use rand::Rng;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use std::{
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{mpsc, RwLock},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

//================================================================================================//
//                                         CONSTANTS                                             //
//================================================================================================//

const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);

//================================================================================================//
//                                        HELPERS                                                 //
//================================================================================================//

#[derive(Debug)]
pub struct EndpointRuntimeState {
    pub config: EndpointConfig,
    pub status: ConnectionStatus,
    pub reconnect_attempts: u32,
    pub last_connection_attempt: Option<Instant>,
    pub sub_task_cancel_token: CancellationToken,
}

//================================================================================================//
//                                     BACKOFF HELPERS                                            //================================================================================================//

/// Calculate backoff delay with randomized jitter to prevent synchronized retries across nodes
fn backoff_with_jitter(attempts: u32, base_delay: Duration, max_delay: Duration, jitter_factor: f64) -> Duration {
    let exp = attempts.saturating_sub(1).min(8);
    let mut delay = base_delay.saturating_mul(2u32.saturating_pow(exp));
    delay = delay.min(max_delay);
    let mut rng = rand::thread_rng();
    let jitter_ms = (delay.as_millis() as f64 * jitter_factor * rng.gen::<f64>()) as u64;
    delay + Duration::from_millis(jitter_ms)
}

//================================================================================================//
//                                     UNIFIED LISTENER                                           //
//================================================================================================//

/// The top-level coordinator that manages listeners for all configured chains.
#[derive(Debug, Default)]
pub struct UnifiedListener {
    listeners: HashMap<String, ChainListener>,
    cancellation_token: CancellationToken,
}

impl UnifiedListener {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn add_chain_listener(&mut self, chain_name: String, listener: ChainListener) {
        self.listeners.insert(chain_name, listener);
    }
    
    pub fn create_child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    pub fn get_listener_count(&self) -> usize {
        self.listeners.len()
    }

    #[instrument(skip(self), name = "unified_listener_start")]
    pub async fn start_all(&self) -> Result<(), ListenerError> {
        for (name, listener) in &self.listeners {
            info!(target: "unified_listener", chain = %name, "Starting listener.");
            // No outer spawn needed: start() already spawns its own task.
            if let Err(e) = listener.start().await {
                error!(target: "unified_listener", chain = %name, error = %e, "Chain listener failed to start.");
            }
        }
        info!(target: "unified_listener", "All chain listeners have been started.");
        Ok(())
    }

    #[instrument(skip(self), name = "unified_listener_shutdown")]
    pub async fn shutdown(&self) {
        info!(target: "unified_listener", "Initiating global shutdown.");
        self.cancellation_token.cancel();
        
        // Shutdown all chain listeners in parallel
        let shutdown_tasks: Vec<_> = self.listeners
            .values()
            .map(|listener| listener.shutdown())
            .collect();
        
        join_all(shutdown_tasks).await;
        info!(target: "unified_listener", "All chain listeners shut down.");
    }
}

//================================================================================================//
//                                      CHAIN LISTENER                                            //
//================================================================================================//

#[derive(Clone, Debug)]
pub struct ChainListener {
    chain_name: String,
    chain_type: ChainType,
    config: Arc<PerChainConfig>,
    endpoints: Arc<Vec<Arc<RwLock<EndpointRuntimeState>>>>,
    event_tx: mpsc::Sender<ChainEvent>,
    provider_factory: Arc<dyn ProviderFactory>,
    cancellation_token: CancellationToken,
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    next_endpoint_idx: Arc<RwLock<usize>>,
}

impl ChainListener {
    /// Get configuration values with sensible defaults for WebSocket connection management.
    fn get_connection_config(&self) -> (u32, Duration, Duration, f64) {
        let max_attempts = self.config.max_ws_reconnect_attempts.unwrap_or(5);
        let base_delay = Duration::from_millis(
            self.config.ws_reconnect_base_delay_ms.unwrap_or(100)
        );
        let max_delay = Duration::from_millis(
            self.config.ws_max_backoff_delay_ms.unwrap_or(30000) // 30 seconds
        );
        let jitter_factor = self.config.ws_backoff_jitter_factor.unwrap_or(0.2);

        (max_attempts, base_delay, max_delay, jitter_factor)
    }

    /// Calculate the next backoff delay based on attempt count and configuration.
    fn calculate_backoff_delay(
        &self,
        attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
        jitter_factor: f64,
    ) -> Duration {
        backoff_with_jitter(attempts, base_delay, max_delay, jitter_factor)
    }

    /// Creates a new ChainListener.
    ///
    /// The `parent_token` should be a child token created from `UnifiedListener::create_child_token()`
    /// to ensure proper shutdown propagation.
    pub fn new(
        config: PerChainConfig,
        event_tx: mpsc::Sender<ChainEvent>,
        provider_factory: Arc<dyn ProviderFactory>,
        parent_token: CancellationToken,
    ) -> Result<Self, ListenerError> {
        let chain_name = config.chain_name.clone();
        let chain_type = ChainType::from_str(&config.chain_type)?;

        // Filter to only WebSocket endpoints and sort by priority (lower number = higher priority)
        let mut ws_endpoints: Vec<_> = config
            .endpoints
            .iter()
            .filter(|ep_config| ep_config.ws)
            .collect();

        // Sort by priority (ascending order - lower number = higher priority)
        ws_endpoints.sort_by_key(|ep| ep.priority.unwrap_or(u32::MAX));

        let endpoints = ws_endpoints
            .into_iter()
            .map(|ep_config| {
                Arc::new(RwLock::new(EndpointRuntimeState {
                    config: ep_config.clone(),
                    status: ConnectionStatus::Disconnected,
                    reconnect_attempts: 0,
                    last_connection_attempt: None,
                    sub_task_cancel_token: CancellationToken::new(),
                }))
            })
            .collect();

        Ok(Self {
            chain_name,
            chain_type,
            config: Arc::new(config),
            endpoints: Arc::new(endpoints),
            event_tx,
            provider_factory,
            cancellation_token: parent_token,
            task_handles: Arc::new(RwLock::new(Vec::new())),
            next_endpoint_idx: Arc::new(RwLock::new(0)),
        })
    }

    /// Starts the main connection management loop for this chain.
    pub async fn start(&self) -> Result<(), ListenerError> {
        let mut handles = self.task_handles.write().await;
        let self_clone = self.clone();
        // Store chain name to avoid Send issues with tracing
        let chain_name = self_clone.chain_name.clone();
        handles.push(tokio::spawn(async move {
            info!(target: "chain_listener", chain = %chain_name, "Starting connection manager loop.");
            self_clone.connection_manager_loop().await;
        }));
        Ok(())
    }

    /// Shuts down all tasks associated with this chain listener.
    pub async fn shutdown(&self) {
        self.cancellation_token.cancel();
        self.cancel_all_sub_tasks().await;
        let mut handles = self.task_handles.write().await;
        for handle in handles.drain(..) {
            let _ = timeout(SHUTDOWN_TASK_TIMEOUT, handle).await;
        }
    }

    /// The main loop that manages endpoint selection, connection, and reconnection.
    #[instrument(skip_all, name = "connection_manager")]
    async fn connection_manager_loop(&self) {
        let chain_name = self.chain_name.clone();
        let (max_attempts, base_delay, max_delay, jitter_factor) = self.get_connection_config();

        info!(target: "chain_listener", chain = %chain_name, "Starting connection manager.");
        loop {
            select! {
                biased;
                _ = self.cancellation_token.cancelled() => {
                    info!(target: "chain_listener", "Shutdown signal received.");
                    break;
                }
                _ = async {
                    if let Some((idx, endpoint_state_arc)) = self.select_endpoint(max_attempts, base_delay, max_delay, jitter_factor).await {
                        let endpoint_url = endpoint_state_arc.read().await.config.url.clone();
                        info!(target: "chain_listener", endpoint_idx = idx, url = %mask_url(&endpoint_url), "Selected new endpoint.");

                        // Cancel any tasks from a previously active endpoint.
                        self.cancel_all_sub_tasks().await;

                        // This call will block until the connection is lost or a major error occurs.
                        let connection_result = self.connect_and_run_subscriptions(endpoint_state_arc.clone(), &endpoint_url).await;

                        if let Err(e) = connection_result {
                            let error_msg = e.to_string();
                            error!(target: "chain_listener", error = %error_msg, url = %mask_url(&endpoint_url), "Endpoint connection failed or dropped. Handling failover.");
                            let mut state = endpoint_state_arc.write().await;
                            state.status = ConnectionStatus::Error(error_msg);
                            state.reconnect_attempts += 1;
                            let delay = self.calculate_backoff_delay(state.reconnect_attempts, base_delay, max_delay, jitter_factor);
                            let attempt_num = state.reconnect_attempts;
                            warn!(target: "chain_listener", attempt = attempt_num, url = %mask_url(&endpoint_url), "Failover attempt. Retrying selection in {:?}", delay);
                            sleep(delay).await;
                        }
                    } else {
                        warn!(target: "chain_listener", "No available endpoints. Retrying in 5s.");
                        sleep(Duration::from_secs(5)).await;
                    }
                } => {}
            }
        }
        self.cancel_all_sub_tasks().await;
        info!(target: "chain_listener", "Connection manager stopped.");
    }

    /// Cancels all currently active subscription tasks for this chain listener.
    async fn cancel_all_sub_tasks(&self) {
        for endpoint_state_arc in &*self.endpoints {
            let cancel_token = endpoint_state_arc.read().await.sub_task_cancel_token.clone();
            cancel_token.cancel();
            
            // Reset the cancel token for future use
            let mut state = endpoint_state_arc.write().await;
            state.sub_task_cancel_token = CancellationToken::new();
        }
    }

    /// Selects the next endpoint to try based on round-robin and health status.
    async fn select_endpoint(
        &self,
        max_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
        jitter_factor: f64,
    ) -> Option<(usize, Arc<RwLock<EndpointRuntimeState>>)> {
        let current_time = Instant::now();
        let endpoints_len = self.endpoints.len();

        if endpoints_len == 0 {
            return None;
        }

        // Start from the round-robin index
        let start_idx = *self.next_endpoint_idx.read().await;
        let mut checked_count = 0;

        while checked_count < endpoints_len {
            let idx = (start_idx + checked_count) % endpoints_len;
            let endpoint_state_arc = &self.endpoints[idx];
            let state = endpoint_state_arc.read().await;

            // Skip if exceeded max reconnect attempts
            if state.reconnect_attempts >= max_attempts {
                checked_count += 1;
                continue;
            }

            // Skip if still in backoff period
            if let Some(last_attempt) = state.last_connection_attempt {
                let needed_backoff = backoff_with_jitter(state.reconnect_attempts.max(1), base_delay, max_delay, jitter_factor);
                if current_time.duration_since(last_attempt) < needed_backoff {
                    checked_count += 1;
                    continue;
                }
            }

            // Update round-robin index for next selection
            *self.next_endpoint_idx.write().await = (idx + 1) % endpoints_len;

            return Some((idx, endpoint_state_arc.clone()));
        }

        None
    }

    /// Connects to an endpoint and spawns the necessary subscription tasks.
    #[instrument(skip(self, endpoint_state_arc))]
    async fn connect_and_run_subscriptions(
        &self,
        endpoint_state_arc: Arc<RwLock<EndpointRuntimeState>>,
        endpoint_url: &str,
    ) -> Result<(), ListenerError> {
        let endpoint_url = endpoint_url.to_string();
        let (url, factory) = {
            let mut state = endpoint_state_arc.write().await;
            state.status = ConnectionStatus::Connecting;
            state.last_connection_attempt = Some(Instant::now());
            (state.config.url.clone(), self.provider_factory.clone())
        };
        
        info!(target: "chain_listener", url = %mask_url(&url), "Connecting to endpoint.");

        let provider = match timeout(CONNECTION_TIMEOUT, factory.create_provider(&url)).await {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(ListenerError::Connection(e.to_string())),
            Err(_) => return Err(ListenerError::Timeout("Connection timeout".into())),
        };

        let sub_cancel_token = {
            let mut state = endpoint_state_arc.write().await;
            state.status = ConnectionStatus::Connected;
            state.reconnect_attempts = 0;
            info!(target: "chain_listener", "Successfully connected.");
            state.sub_task_cancel_token.clone()
        };

        let mut task_set = JoinSet::new();

        match (self.chain_type, provider) {
            (ChainType::Evm, WrappedProvider::Evm(p)) => {
                let endpoint_url_log = endpoint_url.clone();
                let self_log = self.clone();
                let p_log = p.clone();
                let cancel_log = sub_cancel_token.clone();
                task_set.spawn(async move {
                    Self::run_evm_log_subscription(
                        self_log,
                        p_log,
                        cancel_log,
                        &endpoint_url_log,
                    ).await
                });

                let endpoint_url_block = endpoint_url.clone();
                let self_block = self.clone();
                let p_block = p.clone();
                let cancel_block = sub_cancel_token.clone();
                task_set.spawn(async move {
                    Self::run_evm_block_subscription(
                        self_block,
                        p_block,
                        cancel_block,
                        &endpoint_url_block,
                    ).await
                });

                // Conditionally spawn pending transaction subscription with full details
                if self.config.subscribe_pending_txs.unwrap_or(false) {
                    let endpoint_url_pending = endpoint_url.clone();
                    let self_pending = self.clone();
                    let cancel_pending = sub_cancel_token.clone();
                    task_set.spawn(async move {
                        Self::run_evm_pending_tx_subscription(
                            self_pending,
                            p,
                            cancel_pending,
                            &endpoint_url_pending,
                        ).await
                    });
                }
            }
            (ChainType::Solana, WrappedProvider::Solana(p)) => {
                let self_clone1 = self.clone();
                let p_clone1 = p.clone();
                let cancel_token_clone1 = sub_cancel_token.clone();
                let endpoint_url_clone1 = endpoint_url.to_string();
                task_set.spawn(async move {
                    Self::run_solana_log_subscription(
                        self_clone1,
                        p_clone1,
                        cancel_token_clone1,
                        &endpoint_url_clone1,
                    ).await
                });
                let self_clone2 = self.clone();
                let _p_clone2 = p.clone();
                let cancel_token_clone2 = sub_cancel_token.clone();
                let endpoint_url_clone2 = endpoint_url.to_string();
                task_set.spawn(async move {
                    Self::run_solana_slot_subscription(
                        self_clone2,
                        p,
                        cancel_token_clone2,
                        &endpoint_url_clone2,
                    ).await
                });
            }
            _ => {
                return Err(ListenerError::Internal(format!(
                    "Provider type mismatch for chain type {:?}",
                    self.chain_type
                )));
            }
        }

        // Wait for any task to fail (which means the connection is likely dead) or for a shutdown signal.
        select! {
            _ = self.cancellation_token.cancelled() => {
                info!(target: "chain_listener", "Global shutdown signal received, terminating subscriptions.");
                task_set.abort_all();
            },
            Some(res) = task_set.join_next() => {
                warn!(target: "chain_listener", "A subscription task ended unexpectedly: {:?}", res);
                task_set.abort_all();
            }
        }

        endpoint_state_arc.write().await.status = ConnectionStatus::Disconnected;
        Ok(())
    }

    /// Spawns a task to subscribe to EVM logs, using filter and reconnect delay from config.
    async fn run_evm_log_subscription(
        self,
        provider: Arc<ethers::providers::Provider<Ws>>,
        cancel_token: CancellationToken,
        endpoint_url: &str,
    ) {
        // Build the filter from config if available, otherwise use default.
        let filter = if let Some(ref filter_config) = self.config.log_filter {
            // Assume filter_config is a struct that can be converted to ethers::types::Filter
            filter_config.clone().into()
        } else {
            Filter::new()
        };

        let (_, base_delay, max_delay, jitter) = self.get_connection_config();
        let mut attempts: u32 = 0;
        let mut log_count: u64;
        let mut last_metric_report: std::time::Instant;

        loop {
            select! {
                biased;
                _ = cancel_token.cancelled() => {
                    info!(target: "chain_listener::sub_task", "Log subscription cancelled.");
                    break;
                },
                sub_result = provider.subscribe_logs(&filter) => {
                    match sub_result {
                        Ok(mut stream) => {
                            info!(target: "chain_listener::sub_task", "Subscribed to EVM logs.");
                            attempts = 0;
                            log_count = 0;
                            last_metric_report = std::time::Instant::now();

                            while let Some(log) = stream.next().await {
                                log_count += 1;
                                let event = ChainEvent::EvmLog { chain_name: self.chain_name.clone(), log };

                                // Log meaningful metrics periodically
                                if log_count % 1000 == 0 || last_metric_report.elapsed() > std::time::Duration::from_secs(30) {
                                    let elapsed = last_metric_report.elapsed();
                                    let per_sec = log_count as f64 / elapsed.as_secs_f64().max(1e-6);
                                    let per_min = per_sec * 60.0;

                                    info!(target: "chain_listener::metrics",
                                          chain = %self.chain_name,
                                          logs_processed = log_count,
                                          logs_per_sec = %format!("{:.1}", per_sec),
                                          logs_per_minute = %format!("{:.0}", per_min),
                                          processing_time_sec = %format!("{:.1}", elapsed.as_secs_f64()),
                                          "EVM log processing throughput");
                                    last_metric_report = std::time::Instant::now();
                                    log_count = 0;
                                }

                                if self.event_tx.send(event).await.is_err() {
                                    error!(target: "chain_listener::sub_task", "Event channel closed, stopping log subscription.");
                                    return; // Exit completely if channel is dead
                                }
                            }
                            warn!(target: "chain_listener::sub_task", url = %mask_url(endpoint_url), "EVM log stream ended. Re-subscribing...");
                        },
                        Err(e) => error!(target: "chain_listener::sub_task", error = %e, "Failed to subscribe to EVM logs. Retrying..."),
                    }
                    attempts = attempts.saturating_add(1);
                    let delay = backoff_with_jitter(attempts, base_delay, max_delay, jitter);
                    sleep(delay).await;
                }
            }
        }
    }

    /// Spawns a task to subscribe to new EVM blocks.
    async fn run_evm_block_subscription(
        self,
        provider: Arc<ethers::providers::Provider<Ws>>,
        cancel_token: CancellationToken,
        endpoint_url: &str,
    ) {
        let (_, base_delay, max_delay, jitter) = self.get_connection_config();
        let mut attempts: u32 = 0;
        let mut block_count: u64;
        let mut last_metric_report: std::time::Instant;

        loop {
            select! {
                biased;
                _ = cancel_token.cancelled() => { info!(target: "chain_listener::sub_task", "Block subscription cancelled."); break; },
                sub_result = provider.subscribe_blocks() => {
                    match sub_result {
                        Ok(mut stream) => {
                            info!(target: "chain_listener::sub_task", "Subscribed to new EVM blocks.");
                            attempts = 0;
                            block_count = 0;
                            last_metric_report = std::time::Instant::now();

                            while let Some(block) = stream.next().await {
                                block_count += 1;
                                let event = ChainEvent::EvmNewHead { chain_name: self.chain_name.clone(), block };

                                // Log meaningful block processing metrics periodically
                                if block_count % 10 == 0 || last_metric_report.elapsed() > std::time::Duration::from_secs(60) {
                                    let elapsed = last_metric_report.elapsed();
                                    let per_sec = block_count as f64 / elapsed.as_secs_f64().max(1e-6);
                                    let per_min = per_sec * 60.0;

                                    info!(target: "chain_listener::metrics",
                                          chain = %self.chain_name,
                                          blocks_processed = block_count,
                                          blocks_per_sec = %format!("{:.2}", per_sec),
                                          blocks_per_minute = %format!("{:.1}", per_min),
                                          processing_time_sec = %format!("{:.1}", elapsed.as_secs_f64()),
                                          "EVM block processing throughput");
                                    last_metric_report = std::time::Instant::now();
                                    block_count = 0;
                                }

                                // Try to send event with backpressure handling
                                let event_clone = event.clone();
                                if let Err(_) = self.event_tx.try_send(event_clone) {
                                    // If channel is full, fall back to async send with timeout
                                    if tokio::time::timeout(std::time::Duration::from_millis(100), self.event_tx.send(event)).await.is_err() {
                                        error!(target: "chain_listener::sub_task", "Event channel blocked, stopping block subscription.");
                                        return;
                                    }
                                }
                            }
                            warn!(target: "chain_listener::sub_task", url = %mask_url(endpoint_url), "EVM block stream ended. Re-subscribing...");
                        },
                        Err(e) => error!(target: "chain_listener::sub_task", error = %e, "Failed to subscribe to new blocks. Retrying..."),
                    }
                    attempts = attempts.saturating_add(1);
                    let delay = backoff_with_jitter(attempts, base_delay, max_delay, jitter);
                    sleep(delay).await;
                }
            }
        }
    }

    /// Spawns a task to subscribe to EVM pending transactions.
    async fn run_evm_pending_tx_subscription(
        self,
        provider: Arc<ethers::providers::Provider<Ws>>,
        cancel_token: CancellationToken,
        endpoint_url: &str,
    ) {
        let (_, base_delay, max_delay, jitter) = self.get_connection_config();
        let mut attempts: u32 = 0;
        let mut tx_count: u64;
        let mut last_metric_report: std::time::Instant;

        loop {
            select! {
                biased;
                _ = cancel_token.cancelled() => {
                    info!(target: "chain_listener::sub_task", "Pending transaction subscription cancelled.");
                    break;
                },
                sub_result = provider.subscribe_full_pending_txs() => {
                    match sub_result {
                        Ok(mut stream) => {
                            info!(target: "chain_listener::sub_task", "Subscribed to EVM pending transactions with full details.");
                            attempts = 0;
                            tx_count = 0;
                            last_metric_report = std::time::Instant::now();

                            while let Some(transaction) = stream.next().await {
                                tx_count += 1;

                                let event = ChainEvent::EvmPendingTx {
                                    chain_name: self.chain_name.clone(),
                                    tx: transaction,
                                };

                                // Log meaningful transaction processing metrics periodically
                                if tx_count % 1000 == 0 || last_metric_report.elapsed() > std::time::Duration::from_secs(30) {
                                    let elapsed = last_metric_report.elapsed();
                                    let per_sec = tx_count as f64 / elapsed.as_secs_f64().max(1e-6);
                                    let per_min = per_sec * 60.0;

                                    info!(target: "chain_listener::metrics",
                                          chain = %self.chain_name,
                                          pending_tx_received = tx_count,
                                          tx_per_sec = %format!("{:.1}", per_sec),
                                          tx_per_minute = %format!("{:.0}", per_min),
                                          processing_time_sec = %format!("{:.1}", elapsed.as_secs_f64()),
                                          "EVM pending transaction stream throughput");
                                    last_metric_report = std::time::Instant::now();
                                    tx_count = 0;
                                }

                                // Try to send event with backpressure handling
                                let event_clone = event.clone();
                                if let Err(_) = self.event_tx.try_send(event_clone) {
                                    // If channel is full, fall back to async send with timeout
                                    if tokio::time::timeout(std::time::Duration::from_millis(100), self.event_tx.send(event)).await.is_err() {
                                        error!(target: "chain_listener::sub_task", "Event channel blocked, stopping pending tx subscription.");
                                        return;
                                    }
                                }
                            }
                            warn!(target: "chain_listener::sub_task", url = %mask_url(endpoint_url), "EVM pending transaction stream ended. Re-subscribing...");
                        },
                        Err(e) => error!(target: "chain_listener::sub_task", error = %e, "Failed to subscribe to EVM pending transactions. Retrying..."),
                    }
                    attempts = attempts.saturating_add(1);
                    let delay = backoff_with_jitter(attempts, base_delay, max_delay, jitter);
                    sleep(delay).await;
                }
            }
        }
    }

    /// Spawns a task to subscribe to Solana logs.
    async fn run_solana_log_subscription(
        self,
        client: Arc<PubsubClient>,
        cancel_token: CancellationToken,
        endpoint_url: &str,
    ) {
        let (_, base_delay, max_delay, jitter) = self.get_connection_config();
        let mut attempts: u32 = 0;
        loop {
            select! {
                biased;
                _ = cancel_token.cancelled() => { info!(target: "chain_listener::sub_task", "Solana log subscription cancelled."); break; },
                sub_result = client.logs_subscribe(
                    solana_client::rpc_config::RpcTransactionLogsFilter::All, 
                    solana_client::rpc_config::RpcTransactionLogsConfig {
                        commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                    }
                ) => {
                    match sub_result {
                        Ok((mut stream, unsub_handle)) => {
                            info!(target: "chain_listener::sub_task", "Subscribed to Solana logs.");
                            // Cleanup subscription when function exits
                            let _unsub_handle = unsub_handle;

                            loop {
                                select! {
                                    biased;
                                    _ = cancel_token.cancelled() => {
                                        info!(target: "chain_listener::sub_task", "Solana log subscription cancelled via token.");
                                        return;
                                    }
                                    maybe_log = stream.next() => {
                                        if let Some(log_info) = maybe_log {
                                            attempts = 0;
                                            let event = ChainEvent::SolanaLog { chain_name: self.chain_name.clone(), response: log_info.value };
                                            // Try to send event with backpressure handling
                                            let event_clone = event.clone();
                                            if let Err(_) = self.event_tx.try_send(event_clone) {
                                                // If channel is full, fall back to async send with timeout
                                                if tokio::time::timeout(std::time::Duration::from_millis(100), self.event_tx.send(event)).await.is_err() {
                                                    error!(target: "chain_listener::sub_task", "Event channel blocked, stopping Solana log subscription.");
                                                    return;
                                                }
                                            }
                                        } else {
                                            warn!(target: "chain_listener::sub_task", url = %mask_url(endpoint_url), "Solana log stream ended. Re-subscribing...");
                                            break;
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => error!(target: "chain_listener::sub_task", error = %e, "Failed to subscribe to Solana logs. Retrying..."),
                    }
                    attempts = attempts.saturating_add(1);
                    let delay = backoff_with_jitter(attempts, base_delay, max_delay, jitter);
                    sleep(delay).await;
                }
            }
        }
    }

    /// Spawns a task to subscribe to new Solana slots (blocks).
    async fn run_solana_slot_subscription(
        self,
        client: Arc<PubsubClient>,
        cancel_token: CancellationToken,
        endpoint_url: &str,
    ) {
        let (_, base_delay, max_delay, jitter) = self.get_connection_config();
        let mut attempts: u32 = 0;
        loop {
            select! {
                biased;
                _ = cancel_token.cancelled() => { info!(target: "chain_listener::sub_task", "Solana slot subscription cancelled."); break; },
                sub_result = client.slot_subscribe() => {
                    match sub_result {
                        Ok((mut stream, unsub_handle)) => {
                            info!(target: "chain_listener::sub_task", "Subscribed to Solana slots.");
                            // Cleanup subscription when function exits
                            let _unsub_handle = unsub_handle;

                            loop {
                                select! {
                                    biased;
                                    _ = cancel_token.cancelled() => {
                                        info!(target: "chain_listener::sub_task", "Solana slot subscription cancelled via token.");
                                        return;
                                    }
                                    maybe_slot = stream.next() => {
                                        if let Some(slot_info) = maybe_slot {
                                            attempts = 0;
                                            // Extract the slot number from SlotInfo
                                            let slot = slot_info.slot;
                                            let event = ChainEvent::SolanaNewSlot { chain_name: self.chain_name.clone(), slot };
                                            // Try to send event with backpressure handling
                                            let event_clone = event.clone();
                                            if let Err(_) = self.event_tx.try_send(event_clone) {
                                                // If channel is full, fall back to async send with timeout
                                                if tokio::time::timeout(std::time::Duration::from_millis(100), self.event_tx.send(event)).await.is_err() {
                                                    error!(target: "chain_listener::sub_task", "Event channel blocked, stopping Solana slot subscription.");
                                                    return;
                                                }
                                            }
                                        } else {
                                            warn!(target: "chain_listener::sub_task", url = %mask_url(endpoint_url), "Solana slot stream ended. Re-subscribing...");
                                            break;
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => error!(target: "chain_listener::sub_task", error = %e, "Failed to subscribe to Solana slots. Retrying..."),
                    }
                    attempts = attempts.saturating_add(1);
                    let delay = backoff_with_jitter(attempts, base_delay, max_delay, jitter);
                    sleep(delay).await;
                }
            }
        }
    }

}

/// Mask sensitive information in URLs for logging
fn mask_url(url: &str) -> String {
    if let Some(scheme_pos) = url.find("://") {
        let scheme = &url[..scheme_pos];
        let rest = &url[scheme_pos+3..];
        let host_end = rest.find('/').unwrap_or(rest.len());
        let host = &rest[..host_end];
        return format!("{scheme}://{host}/•••");
    }
    "•••".to_string()
}