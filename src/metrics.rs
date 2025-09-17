//! # Global Metrics Registry
//!
//! This module defines and registers all Prometheus metrics for the entire application.
//! By centralizing metric definitions, we ensure consistency and provide a single
//! point of reference for the bot's observability surface.

use once_cell::sync::Lazy;
use warp::Reply;
use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge_vec, Encoder, HistogramVec, IntCounter, IntCounterVec,
    IntGaugeVec, TextEncoder,
};
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tracing::{info, error};
use warp::Filter;
use async_trait::async_trait;

/// Metrics for the arbitrage engine, encapsulating all relevant counters and histograms.
#[derive(Clone)]
pub struct ArbitrageMetrics {
    pub opportunities_found: &'static IntCounterVec,
    pub opportunities_executed: &'static IntCounterVec,
    pub trades_confirmed: &'static IntCounter,
    pub trades_failed: &'static IntCounterVec,
    pub profit_usd: &'static HistogramVec,
    pub opportunities_skipped: &'static IntCounterVec,
    pub execution_pipeline_duration_ms: &'static HistogramVec,
    pub gas_cost_usd: &'static HistogramVec,
    pub active_tasks: &'static IntGaugeVec,
    pub component_health: &'static IntGaugeVec,
}

impl ArbitrageMetrics {
    /// Returns a reference to the global metrics registry for the arbitrage engine.
    pub fn global() -> &'static Self {
        static INSTANCE: Lazy<ArbitrageMetrics> = Lazy::new(|| ArbitrageMetrics {
            opportunities_found: &OPPORTUNITIES_FOUND,
            opportunities_executed: &OPPORTUNITIES_EXECUTED,
            trades_confirmed: &TRADES_CONFIRMED,
            trades_failed: &TRADES_FAILED,
            profit_usd: &PROFIT_USD,
            opportunities_skipped: &OPPORTUNITIES_SKIPPED,
            execution_pipeline_duration_ms: &EXECUTION_PIPELINE_DURATION_MS,
            gas_cost_usd: &GAS_COST_USD,
            active_tasks: &ACTIVE_TASKS,
            component_health: &COMPONENT_HEALTH,
        });
        &INSTANCE
    }
}

impl std::fmt::Debug for ArbitrageMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArbitrageMetrics")
            .field("opportunities_found", &"&'static IntCounterVec")
            .field("opportunities_executed", &"&'static IntCounterVec")
            .field("trades_confirmed", &"&'static IntCounter")
            .field("trades_failed", &"&'static IntCounterVec")
            .field("profit_usd", &"&'static HistogramVec")
            .field("opportunities_skipped", &"&'static IntCounterVec")
            .field("execution_pipeline_duration_ms", &"&'static HistogramVec")
            .field("gas_cost_usd", &"&'static HistogramVec")
            .field("active_tasks", &"&'static IntGaugeVec")
            .field("component_health", &"&'static IntGaugeVec")
            .finish()
    }
}

// --- Arbitrage Engine Metrics ---
pub static OPPORTUNITIES_FOUND: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "bot_opportunities_found_total",
        "Number of potential arbitrage opportunities identified.",
        &["chain", "strategy_type"]
    ).expect("Failed to register bot_opportunities_found_total")
});
pub static OPPORTUNITIES_EXECUTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "bot_opportunities_executed_total",
        "Number of arbitrage opportunities executed.",
        &["chain"]
    ).expect("Failed to register bot_opportunities_executed_total")
});
pub static TRADES_CONFIRMED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "bot_trades_confirmed_total",
        "Number of trades successfully confirmed on-chain."
    ).expect("Failed to register bot_trades_confirmed_total")
});
pub static TRADES_FAILED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "bot_trades_failed_total",
        "Number of failed trades, labeled by reason.",
        &["reason"]
    ).expect("Failed to register bot_trades_failed_total")
});
pub static PROFIT_USD: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "bot_profit_usd",
        "Distribution of arbitrage profit in USD.",
        &["chain"]
    ).expect("Failed to register bot_profit_usd")
});

// --- Risk & Strategy Metrics ---
pub static OPPORTUNITIES_SKIPPED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "bot_opportunities_skipped_total",
        "Opportunities skipped due to risk or strategy filters.",
        &["reason"]
    ).expect("Failed to register bot_opportunities_skipped_total")
});

// --- Performance & Latency Metrics ---
pub static EXECUTION_PIPELINE_DURATION_MS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "bot_execution_pipeline_duration_ms",
        "End-to-end latency of the execution pipeline stages.",
        &["stage"]
    ).expect("Failed to register bot_execution_pipeline_duration_ms")
});

// --- Gas & Transaction Metrics ---
pub static GAS_COST_USD: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "bot_gas_cost_usd",
        "Distribution of transaction gas costs in USD.",
        &["chain"]
    ).expect("Failed to register bot_gas_cost_usd")
});

// --- System Health Metrics ---
pub static ACTIVE_TASKS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "bot_active_tasks",
        "Number of active asynchronous tasks.",
        &["task_group"]
    ).expect("Failed to register bot_active_tasks")
});
pub static COMPONENT_HEALTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "bot_component_health_status",
        "Health status of system components (1=Healthy, 0=Unhealthy).",
        &["component"]
    ).expect("Failed to register bot_component_health_status")
});

// --- DEX-Specific Metrics ---

pub static APPROVAL_COUNT: once_cell::sync::Lazy<prometheus::IntCounterVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "dex_approval_count_total",
        "Number of token approval transactions submitted.",
        &["token"]
    ).expect("Failed to register dex_approval_count_total")
});

pub static SWAP_TOTAL_COUNT: once_cell::sync::Lazy<prometheus::IntCounterVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "dex_swap_total_count",
        "Total number of swaps executed.",
        &["token_in", "token_out", "protocol", "status"]
    ).expect("Failed to register dex_swap_total_count")
});

pub static SWAP_GAS_USED: once_cell::sync::Lazy<prometheus::HistogramVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_histogram_vec!(
        "dex_swap_gas_used",
        "Gas used per swap execution.",
        &["protocol"]
    ).expect("Failed to register dex_swap_gas_used")
});

pub static SWAP_EXECUTION_TIME_MS: once_cell::sync::Lazy<prometheus::HistogramVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_histogram_vec!(
        "dex_swap_execution_time_ms",
        "Swap execution time in milliseconds.",
        &["protocol"]
    ).expect("Failed to register dex_swap_execution_time_ms")
});

pub static SWAP_PRICE_IMPACT_BPS: once_cell::sync::Lazy<prometheus::HistogramVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_histogram_vec!(
        "dex_swap_price_impact_bps",
        "Swap price impact in basis points.",
        &["token_in", "token_out", "protocol"]
    ).expect("Failed to register dex_swap_price_impact_bps")
});

pub static SWAP_FAILURES: once_cell::sync::Lazy<prometheus::IntCounterVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "dex_swap_failures_total",
        "Number of failed swaps, labeled by protocol and reason.",
        &["protocol", "reason"]
    ).expect("Failed to register dex_swap_failures_total")
});

pub static RATE_ESTIMATE_COUNT: once_cell::sync::Lazy<prometheus::IntCounterVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "dex_rate_estimate_count_total",
        "Number of rate estimate requests.",
        &["token_in", "token_out"]
    ).expect("Failed to register dex_rate_estimate_count_total")
});

// --- RPC Metrics ---

pub static RPC_LATENCY_HISTOGRAM: once_cell::sync::Lazy<prometheus::HistogramVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_histogram_vec!(
        "rpc_call_latency_seconds",
        "RPC call latency in seconds, labeled by method.",
        &["method"]
    ).expect("Failed to register rpc_call_latency_seconds")
});

pub static RPC_RETRIES_COUNTER: once_cell::sync::Lazy<prometheus::IntCounterVec> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "rpc_call_retries_total",
        "Number of RPC call retries, labeled by method.",
        &["method"]
    ).expect("Failed to register rpc_call_retries_total")
});

pub static COPY_ROWS_COUNTER: once_cell::sync::Lazy<prometheus::IntCounter> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_counter!(
        "db_copy_rows_total",
        "Total number of rows copied to database."
    ).expect("Failed to register db_copy_rows_total")
});

pub static COPY_BYTES_GAUGE: once_cell::sync::Lazy<prometheus::IntGauge> = once_cell::sync::Lazy::new(|| {
    prometheus::register_int_gauge!(
        "db_copy_bytes_current",
        "Current number of bytes in the last database copy operation."
    ).expect("Failed to register db_copy_bytes_current")
});

/// Starts the Prometheus metrics server on a separate Tokio task.
pub fn start_metrics_server(host: String, port: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        let addr: SocketAddr = format!("{}:{}", host, port)
            .parse()
            .expect("Invalid metrics server address");

        info!(target: "metrics", "Prometheus metrics server starting on http://{}", addr);

        let metrics_route = warp::path("metrics").and_then(metrics_handler);
        warp::serve(metrics_route).run(addr).await;
    })
}

/// Warp handler function to collect and encode metrics for Prometheus.
async fn metrics_handler() -> Result<warp::reply::Response, warp::Rejection> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!(target: "metrics", "Failed to encode metrics: {}", e);
        let response = warp::reply::with_status(
            "Failed to encode metrics".to_string(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        );
        return Ok(response.into_response());
    }

    let response = warp::reply::with_header(
        String::from_utf8_lossy(&buffer).to_string(),
        "Content-Type",
        encoder.format_type(),
    );
    Ok(response.into_response())
}

// Import mempool metrics type
use crate::mempool::MempoolMetrics;

/// Trait for reporting various types of metrics to external systems
#[async_trait]
pub trait MetricsReporter: Send + Sync {
    /// Report mempool monitoring metrics
    async fn report_mempool_metrics(&self, metrics: MempoolMetrics);
    
    /// Report general performance metrics
    async fn report_performance_metrics(&self, component: &str, duration_ms: u64);
    
    /// Report error metrics
    async fn report_error(&self, component: &str, error_type: &str);
    
    /// Report health status
    async fn report_health_status(&self, component: &str, is_healthy: bool);
}

/// Default implementation that reports to Prometheus metrics
pub struct PrometheusMetricsReporter;

impl PrometheusMetricsReporter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MetricsReporter for PrometheusMetricsReporter {
    async fn report_mempool_metrics(&self, metrics: MempoolMetrics) {
        // Report to Prometheus metrics - this would require appropriate counters/gauges
        info!(target: "metrics::mempool", 
            "Mempool metrics - received: {}, filtered: {}, broadcasted: {}, dropped: {}, subscribers: {}, failures: {}, reconnections: {}",
            metrics.total_transactions_received,
            metrics.total_transactions_filtered,
            metrics.total_transactions_broadcasted,
            metrics.total_dropped_due_to_lag,
            metrics.current_subscriber_count,
            metrics.connection_failures,
            metrics.stream_reconnections
        );
    }
    
    async fn report_performance_metrics(&self, component: &str, duration_ms: u64) {
        info!(target: "metrics::performance", "Component {} execution time: {}ms", component, duration_ms);
    }
    
    async fn report_error(&self, component: &str, error_type: &str) {
        error!(target: "metrics::error", "Component {} error: {}", component, error_type);
    }
    
    async fn report_health_status(&self, component: &str, is_healthy: bool) {
        info!(target: "metrics::health", "Component {} health: {}", component, if is_healthy { "healthy" } else { "unhealthy" });
    }
}