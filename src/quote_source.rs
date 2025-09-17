//! # External Aggregator Quote Source
//!
//! This module provides a resilient and extensible interface for fetching swap quotes
//! from external DEX aggregators like 1inch. It is designed with production-grade
//! features such as circuit breaking, rate limiting, and intelligent caching.

use crate::{
    errors::{DexError},
    types::{RateEstimate},
};
use async_trait::async_trait;
use ethers::types::{Address, U256};
use eyre::Result;
use moka::future::Cache;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, info, instrument, warn};

//================================================================================================//
//                                         CONSTANTS                                             //
//================================================================================================//

/// Default TTL for cached quotes to avoid stale data.
const QUOTE_CACHE_TTL: Duration = Duration::from_secs(15);
/// Default timeout for HTTP requests to external APIs.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Cooldown period for a circuit breaker after it has tripped.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(60);
/// Number of consecutive failures required to trip the circuit breaker.
const FAILURE_THRESHOLD_TO_TRIP: usize = 5;

//================================================================================================//
//                                      TRAIT DEFINITION                                          //
//================================================================================================//

/// A standardized interface for any aggregator quote source.
#[async_trait]
pub trait AggregatorQuoteSource: Send + Sync + fmt::Debug + std::any::Any {
    /// Gets a rate estimate for a swap.
    async fn get_rate_estimate(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
    ) -> Result<RateEstimate, DexError>;

    async fn get_rate_estimate_from_quote(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
        is_backtest: bool,
    ) -> Result<RateEstimate, DexError>;

    /// Returns the name of the aggregator implementation.
    fn name(&self) -> &'static str;
    
    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

//================================================================================================//
//                                     1INCH IMPLEMENTATION                                       //
//================================================================================================//

#[derive(Debug, Clone, Deserialize)]
pub struct OneInchQuoteReply {
    #[serde(rename = "toAmount")]
    pub to_token_amount: String,
    #[serde(rename = "estimatedGas")]
    pub estimated_gas: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct OneInchQuoteSource {
    client: Client,
    api_key: String,
    rate_limiter: Arc<Semaphore>,
    strict_backtest: Arc<AtomicBool>,
}

impl OneInchQuoteSource {
    pub fn new(api_key: String, rps_limit: u32) -> Self {
        let client = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent("alpha-bot/1.0")
            .build()
            .expect("Failed to build HTTP client");
        Self {
            client,
            api_key,
            rate_limiter: Arc::new(Semaphore::new(rps_limit.max(1) as usize)),
            strict_backtest: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_strict_backtest(&self, enabled: bool) {
        self.strict_backtest.store(enabled, Ordering::Relaxed);
    }
}

#[async_trait]
impl AggregatorQuoteSource for OneInchQuoteSource {
    fn name(&self) -> &'static str {
        "1inch"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[instrument(skip(self), fields(chain_id, from_token=%from_token, to_token=%to_token, amount=%amount))]
    async fn get_rate_estimate(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
    ) -> Result<RateEstimate, DexError> {
        self.get_rate_estimate_from_quote(chain_id, from_token, to_token, amount, false)
            .await
    }

    async fn get_rate_estimate_from_quote(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
        is_backtest: bool,
    ) -> Result<RateEstimate, DexError> {
        if is_backtest {
            return Err(DexError::Other(
                "Live quotes disabled in strict backtest mode.".to_string(),
            ));
        }

        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| DexError::Connection("Rate limiter semaphore closed.".to_string()))?;

        let url = format!(
            "https://api.1inch.dev/swap/v6.0/{}/quote?fromTokenAddress={}&toTokenAddress={}&amount={}",
            chain_id,
            format!("{:#x}", from_token),
            format!("{:#x}", to_token),
            amount
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .map_err(|e| DexError::Connection(format!("1inch request failed: {}", e)))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| DexError::Connection(format!("Failed to read 1inch response: {}", e)))?;

        if !status.is_success() {
            if status == StatusCode::BAD_REQUEST && text.contains("insufficient liquidity") {
                return Err(DexError::NoRateFound);
            }
            return Err(DexError::Other(format!(
                "1inch API error {}: {}",
                status, text
            )));
        }

        let quote: OneInchQuoteReply = serde_json::from_str(&text).map_err(|e| {
            DexError::InvalidFormat(format!("1inch JSON error: {} - Response: {}", e, text))
        })?;
        let amount_out = U256::from_dec_str(&quote.to_token_amount).map_err(|e| {
            DexError::InvalidFormat(format!(
                "Failed to parse to_token_amount as U256: {} - value: {}",
                e, quote.to_token_amount
            ))
        })?;

        Ok(RateEstimate {
            amount_out,
            fee_bps: 0,
            price_impact_bps: 0,
            gas_estimate: quote.estimated_gas.unwrap_or(200_000).into(),
            pool_address: Address::zero(),
        })
    }
}

//================================================================================================//
//                                 MULTI-AGGREGATOR & CIRCUIT BREAKER                             //
//================================================================================================//

#[derive(Debug, Clone)]
struct AggregatorState {
    breaker: QuoteCircuitBreaker,
    source: Arc<dyn AggregatorQuoteSource>,
}

/// A resilient quote source that wraps multiple aggregators, providing
/// round-robin load balancing and a circuit breaker for each source.
#[derive(Debug, Clone)]
pub struct MultiAggregatorQuoteSource {
    sources: Vec<AggregatorState>,
    last_used_idx: Arc<AtomicUsize>,
    quote_cache: Cache<(u64, Address, Address, U256), RateEstimate>,
}

impl MultiAggregatorQuoteSource {
    pub fn new(sources: Vec<Arc<dyn AggregatorQuoteSource>>) -> Self {
        let states = sources
            .into_iter()
            .map(|source| AggregatorState {
                breaker: QuoteCircuitBreaker::new(),
                source,
            })
            .collect();

        Self {
            sources: states,
            last_used_idx: Arc::new(AtomicUsize::new(0)),
            quote_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(QUOTE_CACHE_TTL)
                .build(),
        }
    }

    /// Internal helper method that contains the common logic for both quote methods.
    /// This eliminates code duplication and provides a single source of truth for
    /// caching, round-robin load balancing, and circuit breaker logic.
    async fn get_rate_estimate_internal<F, Fut>(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
        api_call_fn: F,
    ) -> Result<RateEstimate, DexError>
    where
        F: Fn(Arc<dyn AggregatorQuoteSource>) -> Fut,
        Fut: std::future::Future<Output = Result<RateEstimate, DexError>>,
    {
        let cache_key = (chain_id, from_token, to_token, amount);
        if let Some(cached) = self.quote_cache.get(&cache_key).await {
            return Ok(cached);
        }

        let num_sources = self.sources.len();
        if num_sources == 0 {
            return Err(DexError::Config(
                "No quote sources configured.".to_string(),
            ));
        }

        let start_idx = self.last_used_idx.fetch_add(1, Ordering::Relaxed) % num_sources;
        let mut last_error: Option<DexError> = None;

        for i in 0..num_sources {
            let idx = (start_idx + i) % num_sources;
            let state = &self.sources[idx];

            if state.breaker.is_open().await {
                debug!(
                    target: "quote_source",
                    source = state.source.name(),
                    "Circuit breaker is open, skipping."
                );
                continue;
            }

            let start_time = Instant::now();
            match api_call_fn(state.source.clone()).await {
                Ok(rate) => {
                    state.breaker.record_success().await;
                    self.quote_cache.insert(cache_key, rate.clone()).await;
                    info!(
                        target: "quote_source",
                        source = state.source.name(),
                        latency_ms = start_time.elapsed().as_millis(),
                        "Quote successful."
                    );
                    return Ok(rate);
                }
                Err(e) => {
                    warn!(
                        target: "quote_source",
                        source = state.source.name(),
                        error = %e,
                        "Quote source failed."
                    );
                    state.breaker.record_failure().await;
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or(DexError::NoRateFound))
    }
}

#[async_trait]
impl AggregatorQuoteSource for MultiAggregatorQuoteSource {
    fn name(&self) -> &'static str {
        "MultiAggregator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[instrument(skip(self), fields(chain_id, from_token=%from_token, to_token=%to_token, amount=%amount))]
    async fn get_rate_estimate(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
    ) -> Result<RateEstimate, DexError> {
        self.get_rate_estimate_internal(chain_id, from_token, to_token, amount, |source| {
            let source = source.clone();
            async move {
                source.get_rate_estimate(chain_id, from_token, to_token, amount).await
            }
        }).await
    }

    async fn get_rate_estimate_from_quote(
        &self,
        chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount: U256,
        is_backtest: bool,
    ) -> Result<RateEstimate, DexError> {
        if is_backtest {
            return Err(DexError::Other(
                "Live quotes disabled in strict backtest mode.".to_string(),
            ));
        }

        self.get_rate_estimate_internal(chain_id, from_token, to_token, amount, |source| {
            let source = source.clone();
            async move {
                source.get_rate_estimate_from_quote(chain_id, from_token, to_token, amount, is_backtest).await
            }
        }).await
    }
}

//================================================================================================//
//                                     CIRCUIT BREAKER                                            //
//================================================================================================//

#[derive(Debug, Clone)]
struct QuoteCircuitBreaker {
    state: Arc<RwLock<BreakerState>>,
}

#[derive(Debug, Clone)]
struct BreakerState {
    failure_count: u32,
    last_failure_time: Option<Instant>,
    open_until: Option<Instant>,
}

impl QuoteCircuitBreaker {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(BreakerState {
                failure_count: 0,
                last_failure_time: None,
                open_until: None,
            })),
        }
    }

    /// Checks if the circuit breaker is open, using a single write lock to prevent race conditions.
    async fn is_open(&self) -> bool {
        // Acquire write lock immediately to prevent race conditions
        let mut state = self.state.write().await;
        if let Some(open_until) = state.open_until {
            if Instant::now() < open_until {
                return true; // Still open
            } else {
                // Cooldown has passed, transition to half-open
                state.open_until = None;
                // Reset failure count to allow some test requests through
                state.failure_count = FAILURE_THRESHOLD_TO_TRIP as u32 / 2;
                return false; // Now half-open
            }
        }
        false // Not open
    }

    async fn record_success(&self) {
        let mut state = self.state.write().await;
        state.failure_count = 0;
        state.last_failure_time = None;
        state.open_until = None;
    }

    async fn record_failure(&self) {
        let mut state = self.state.write().await;
        state.failure_count += 1;
        state.last_failure_time = Some(Instant::now());
        if state.failure_count >= FAILURE_THRESHOLD_TO_TRIP as u32 {
            state.open_until = Some(Instant::now() + CIRCUIT_BREAKER_COOLDOWN);
            warn!(
                target: "quote_source",
                "Circuit breaker tripped! Cooling down for {} seconds.",
                CIRCUIT_BREAKER_COOLDOWN.as_secs()
            );
        }
    }
}