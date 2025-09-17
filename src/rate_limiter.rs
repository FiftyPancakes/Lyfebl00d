// File: src/rate_limiter.rs

use crate::errors::BlockchainError;
use crate::config::{ChainSettings, ExecutionMode};
use crate::metrics::{RPC_LATENCY_HISTOGRAM, RPC_RETRIES_COUNTER, COPY_ROWS_COUNTER, COPY_BYTES_GAUGE};
use dashmap::DashMap;
use futures::Future;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter as GovernorRateLimiter};
use std::num::NonZeroU32;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, trace, warn};
use eyre::Result;

const RATE_LIMIT_ERRORS: &[&str] = &[
    "rate limit",
    "too many requests",
    "exceeded",
    "429",
    "RateLimitError",
    "-32005",
    "You've exceeded the RPS limit",
];

#[derive(Debug, Clone)]
pub struct RpcCallMetrics {
    pub total_calls: u64,
    pub successful_calls: u64,
    pub rate_limited_calls: u64,
    pub failed_calls: u64,
    pub total_wait_time_ms: u64,
    // Removed average_wait_time_ms for on-demand calculation
}

impl Default for RpcCallMetrics {
    fn default() -> Self {
        Self {
            total_calls: 0,
            successful_calls: 0,
            rate_limited_calls: 0,
            failed_calls: 0,
            total_wait_time_ms: 0,
        }
    }
}

#[derive(Debug)]
pub struct ChainRateLimiter {
    chain: String,
    rate_limiter: Arc<DefaultDirectRateLimiter>,
    global_limiter: Arc<DefaultDirectRateLimiter>,
    concurrency_limiter: Option<Arc<Semaphore>>,
    metrics: Arc<RwLock<RpcCallMetrics>>,
    settings: Arc<ChainSettings>,
}

impl ChainRateLimiter {
    pub fn new(
        chain: &str,
        rps_limit: Option<u32>,
        max_concurrent: Option<u32>,
        global_limiter: Arc<DefaultDirectRateLimiter>,
        settings: Arc<ChainSettings>,
    ) -> Self {
        // Use configured rate limits or fall back to defaults
        let base_rps_limit = rps_limit.unwrap_or(settings.default_chain_rps_limit);

        // Set reasonable concurrent request limits
        let max_concurrent = max_concurrent
            .unwrap_or(settings.default_max_concurrent_requests)
            .min(50); // Hard cap at 50 concurrent

        // Create rate limiter with appropriate quota
        let quota = Quota::per_second(
            NonZeroU32::new(base_rps_limit)
                .unwrap_or_else(|| NonZeroU32::new(settings.default_chain_rps_limit).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(settings.rate_limit_burst_size)
                .unwrap_or_else(|| NonZeroU32::new(5).unwrap()),
        ); // Removed .max(NonZeroU32::new(1).unwrap()) as it's redundant

        let rate_limiter = Arc::new(GovernorRateLimiter::direct(quota));

        let concurrency_limiter = if max_concurrent > 0 {
            Some(Arc::new(Semaphore::new(max_concurrent as usize)))
        } else {
            None
        };

        info!(
            chain = chain,
            rps_limit = base_rps_limit,
            max_concurrent = ?max_concurrent,
            "Initialized chain rate limiter with configured settings"
        );

        Self {
            chain: chain.to_string(),
            rate_limiter,
            global_limiter,
            concurrency_limiter,
            metrics: Arc::new(RwLock::new(RpcCallMetrics::default())),
            settings,
        }
    }

    async fn wait_on_limiter(
        &self,
        limiter: &DefaultDirectRateLimiter,
        limiter_name: &str,
        method_name: &str,
    ) -> Result<(), BlockchainError> {
        if self.settings.mode == ExecutionMode::Backtest {
            trace!(
                chain = %self.chain,
                method = method_name,
                "Backtest mode: skipping {} wait",
                limiter_name
            );
            return Ok(());
        }

        let wait_start = Instant::now();
        match timeout(
            Duration::from_secs(self.settings.rate_limit_timeout_secs),
            limiter.until_ready(),
        )
        .await
        {
            Ok(_) => {
                let wait_time = wait_start.elapsed();
                if wait_time.as_millis() > 1000 {
                    debug!(
                        chain = %self.chain,
                        method = method_name,
                        limiter = limiter_name,
                        wait_ms = wait_time.as_millis(),
                        "Long rate limit wait detected"
                    );
                }
                trace!(
                    chain = %self.chain,
                    method = method_name,
                    limiter = limiter_name,
                    wait_ms = wait_time.as_millis(),
                    "Rate limit check passed"
                );
                Ok(())
            }
            Err(_) => {
                // Increment metrics inside the helper for encapsulation
                let mut metrics = self.metrics.write().await;
                metrics.rate_limited_calls += 1;

                error!(
                    chain = %self.chain,
                    method = method_name,
                    limiter = limiter_name,
                    timeout_secs = self.settings.rate_limit_timeout_secs,
                    "Rate limiter timed out"
                );
                Err(BlockchainError::RateLimitError(format!(
                    "{} timeout after {} seconds",
                    limiter_name, self.settings.rate_limit_timeout_secs
                )))
            }
        }
    }

    pub async fn execute_rpc_call<F, Fut, T>(
        &self,
        method_name: &str,
        call_fn: F,
    ) -> Result<T, BlockchainError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, BlockchainError>>,
    {
        // In backtest mode, bypass rate limiting entirely
        if self.settings.mode == ExecutionMode::Backtest {
            trace!(
                chain = %self.chain,
                method = method_name,
                "Backtest mode: bypassing rate limiter entirely"
            );
            return call_fn().await;
        }

        let start_time = Instant::now();
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_calls += 1;
        }

        let _permit = if let Some(ref sem) = self.concurrency_limiter {
            Some(
                sem.acquire()
                    .await
                    .map_err(|_| BlockchainError::RateLimitError("Concurrency semaphore closed".to_string()))?,
            )
        } else {
            None
        };

        // Refactored waiting logic
        self.wait_on_limiter(&self.global_limiter, "Global rate limiter", method_name).await?;
        self.wait_on_limiter(&self.rate_limiter, "Per-chain rate limiter", method_name).await?;

        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.settings.rate_limit_max_retries {
            if attempt > 0 {
                // Record retry metric
                RPC_RETRIES_COUNTER.with_label_values(&[method_name]).inc();
            }
            attempt += 1;

            debug!(
                chain = %self.chain,
                method = method_name,
                attempt = attempt,
                "Executing RPC call"
            );

            match timeout(Duration::from_secs(30), call_fn()).await {
                Ok(Ok(result)) => {
                    let total_time = start_time.elapsed();
                    let mut metrics = self.metrics.write().await;
                    metrics.successful_calls += 1;
                    metrics.total_wait_time_ms += total_time.as_millis() as u64;

                    // Record Prometheus metrics
                    RPC_LATENCY_HISTOGRAM.with_label_values(&[method_name]).observe(total_time.as_secs_f64());

                    debug!(
                        chain = %self.chain,
                        method = method_name,
                        attempt = attempt,
                        duration_ms = total_time.as_millis(),
                        "RPC call succeeded"
                    );
                    return Ok(result);
                }
                Ok(Err(e)) => {
                    // RPC call completed but returned an error
                    let error_str = e.to_string();

                    let is_rate_limit_error =
                        RATE_LIMIT_ERRORS.iter().any(|&pattern| error_str.contains(pattern));

                    if is_rate_limit_error && attempt < self.settings.rate_limit_max_retries {
                        // First check if we have a specific retry delay from the error
                        let mut final_backoff = self.settings.rate_limit_initial_backoff_ms;

                        // Parse the retry delay from error if available
                        if let Some(retry_after_idx) = error_str.find("\"try_again_in\"") {
                            if let Some(value_start) = error_str[retry_after_idx..].find("\"") {
                                let start = retry_after_idx + value_start + 1;
                                if let Some(value_end) = error_str[start..].find("\"") {
                                    let delay_str = &error_str[start..start + value_end];
                                    if delay_str.ends_with("ms") {
                                        if let Ok(ms) = delay_str[..delay_str.len() - 2].parse::<f64>() {
                                            final_backoff = (ms.ceil() as u64).max(10); // At least 10ms
                                        }
                                    } else if delay_str.ends_with("s") {
                                        if let Ok(s) = delay_str[..delay_str.len() - 1].parse::<f64>() {
                                            final_backoff = ((s * 1000.0).ceil() as u64).max(10);
                                        }
                                    }
                                }
                            }
                        } else {
                            // Use exponential backoff if no specific delay given
                            let jitter = (attempt as f64 * 7.0 % 10.0) / 10.0
                                * self.settings.rate_limit_jitter_factor
                                * self.settings.rate_limit_initial_backoff_ms as f64;
                            let backoff_ms = (self.settings.rate_limit_initial_backoff_ms as f64
                                * self.settings.rate_limit_backoff_multiplier
                                    .powf((attempt - 1) as f64)
                                + jitter) as u64;
                            final_backoff = backoff_ms.min(self.settings.rate_limit_max_backoff_ms);
                        }

                        warn!(
                            chain = %self.chain,
                            method = method_name,
                            attempt = attempt,
                            error = %error_str,
                            backoff_ms = final_backoff,
                            "Rate limit error, retrying with backoff"
                        );

                        let mut metrics = self.metrics.write().await;
                        metrics.rate_limited_calls += 1;

                        if self.settings.mode == ExecutionMode::Live {
                            sleep(Duration::from_millis(final_backoff)).await;
                        } else {
                            trace!(
                                chain = %self.chain,
                                method = method_name,
                                "Backtest mode: skipping real-time sleep for {}ms",
                                final_backoff
                            );
                        }
                        last_error = Some(e);
                        continue;
                    } else {
                        debug!(
                            chain = %self.chain,
                            method = method_name,
                            attempt = attempt,
                            error = %error_str,
                            "RPC call failed (non-retryable or max attempts)"
                        );

                        let mut metrics = self.metrics.write().await;
                        metrics.failed_calls += 1;

                        return Err(e);
                    }
                }
                Err(_) => {
                    // Timeout occurred - don't retry timeouts
                    let error = BlockchainError::Provider("RPC call timed out after 30 seconds".to_string());
                    debug!(
                        chain = %self.chain,
                        method = method_name,
                        attempt = attempt,
                        "RPC call timed out (non-retryable)"
                    );

                    let mut metrics = self.metrics.write().await;
                    metrics.failed_calls += 1;

                    return Err(error);
                }
            }
        }

        let error = last_error.unwrap_or_else(|| {
            BlockchainError::RateLimitError("All retry attempts exhausted".to_string())
        });

        error!(
            chain = %self.chain,
            method = method_name,
            attempts = self.settings.rate_limit_max_retries,
            "All RPC retry attempts exhausted"
        );

        let mut metrics = self.metrics.write().await;
        metrics.failed_calls += 1;

        Err(error)
    }

    pub async fn get_metrics(&self) -> RpcCallMetrics {
        // Calculate average_wait_time_ms on demand if needed by consumer
        self.metrics.read().await.clone()
    }

    pub async fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        *metrics = RpcCallMetrics::default();
    }

    /// Get the configured batch size for transaction fetching
    pub fn get_batch_size(&self) -> Option<usize> {
        Some(self.settings.batch_size)
    }

    /// Get the configured minimum delay between batches in milliseconds
    pub fn get_min_batch_delay_ms(&self) -> Option<u64> {
        Some(self.settings.min_batch_delay_ms)
    }

    /// Get the configured RPS limit for this chain
    pub fn get_rps_limit(&self) -> Option<u32> {
        Some(self.settings.default_chain_rps_limit)
    }
}

#[derive(Debug)]
pub struct GlobalRateLimiterManager {
    global_limiter: Arc<DefaultDirectRateLimiter>,
    chain_limiters: DashMap<String, Arc<ChainRateLimiter>>,
    settings: Arc<ChainSettings>,
}

impl GlobalRateLimiterManager {
    pub fn new(settings: Arc<ChainSettings>) -> Self {
        // Use the configured global limit
        let quota = Quota::per_second(
            NonZeroU32::new(settings.global_rps_limit)
                .unwrap_or_else(|| NonZeroU32::new(1).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(settings.rate_limit_burst_size)
                .unwrap_or_else(|| NonZeroU32::new(5).unwrap()),
        );

        let global_limiter = Arc::new(GovernorRateLimiter::direct(quota));

        info!(
            global_rps_limit = settings.global_rps_limit,
            "Initialized global rate limiter manager with a shared global limit."
        );

        Self {
            global_limiter,
            chain_limiters: DashMap::new(),
            settings,
        }
    }

    pub fn get_settings(&self) -> &ChainSettings {
        &self.settings
    }

    pub fn get_global_limiter(&self) -> Arc<DefaultDirectRateLimiter> {
        self.global_limiter.clone()
    }

    pub fn get_or_create_chain_limiter(
        &self,
        chain_name: String,
        rps_limit: Option<u32>,
        max_concurrent: Option<u32>,
    ) -> Arc<ChainRateLimiter> {
        if let Some(limiter) = self.chain_limiters.get(&chain_name) {
            return limiter.clone();
        }

        let effective_rps = rps_limit.unwrap_or(self.settings.default_chain_rps_limit);
        let limiter = Arc::new(ChainRateLimiter::new(
            &chain_name,
            Some(effective_rps),
            max_concurrent,
            self.global_limiter.clone(),
            self.settings.clone(),
        ));

        self.chain_limiters.insert(chain_name, limiter.clone());
        limiter
    }

    pub async fn get_all_metrics(&self) -> std::collections::HashMap<String, RpcCallMetrics> {
        let mut all_metrics = std::collections::HashMap::new();

        for entry in self.chain_limiters.iter() {
            let chain_name = entry.key().clone();
            let metrics = entry.value().get_metrics().await;
            all_metrics.insert(chain_name, metrics);
        }

        all_metrics
    }

    pub async fn reset_all_metrics(&self) {
        for entry in self.chain_limiters.iter() {
            entry.value().reset_metrics().await;
        }
        info!("Reset metrics for all chain rate limiters");
    }

    pub fn get_chain_count(&self) -> usize {
        self.chain_limiters.len()
    }
}

static GLOBAL_RATE_LIMITER_MANAGER: OnceLock<Arc<GlobalRateLimiterManager>> = OnceLock::new();

pub fn get_global_rate_limiter_manager() -> Result<&'static Arc<GlobalRateLimiterManager>, &'static str> {
    GLOBAL_RATE_LIMITER_MANAGER
        .get()
        .ok_or("Global rate limiter manager not initialized")
}

pub fn initialize_global_rate_limiter_manager(
    settings: Arc<ChainSettings>,
) -> Arc<GlobalRateLimiterManager> {
    let manager = Arc::new(GlobalRateLimiterManager::new(settings));
    let _ = GLOBAL_RATE_LIMITER_MANAGER.set(manager.clone());
    manager
}
