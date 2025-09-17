// src/mempool_stats.rs

//! # Mempool Statistics Provider
//!
//! This module provides an abstraction for querying real-time mempool statistics,
//! which are crucial for assessing network congestion and frontrunning risk.

use crate::errors::MempoolError;
use crate::mempool::MempoolMonitor;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, instrument};

const STATS_WINDOW: Duration = Duration::from_secs(60); // 1-minute sliding window
const MAX_SAMPLES: usize = 120; // Store up to 120 samples (e.g., one every 500ms)

/// Represents key statistics for a blockchain's mempool.
#[derive(Debug, Clone, Default)]
pub struct MempoolStats {
    /// Number of pending transactions observed in the last minute.
    pub pending_tx_count: u64,
    /// A normalized score (0.0 to 1.0) representing current congestion. Higher is more congested.
    pub congestion_score: f64,
    /// The recent trend in base fee per gas (e.g., 1.05 means a 5% increase).
    pub base_fee_per_gas_trend: f64,
    /// Percentiles for priority fees in the last window.
    pub priority_fee_percentiles: std::collections::HashMap<String, u64>,
}

/// A trait for providing mempool statistics.
#[async_trait]
pub trait MempoolStatsProvider: Send + Sync {
    /// Fetches the current mempool statistics for a given chain.
    async fn get_stats(&self) -> Result<MempoolStats, MempoolError>;
}

struct StatsSample {
    timestamp: Instant,
    base_fee: u64,
    priority_fee: u64,
}

/// A concrete implementation that calculates statistics from a live `MempoolMonitor`.
pub struct LiveMempoolStatsProvider {
    monitor: Arc<dyn MempoolMonitor>,
    recent_stats: Arc<Mutex<VecDeque<StatsSample>>>,
    last_update: Arc<Mutex<Instant>>,
}

impl LiveMempoolStatsProvider {
    pub fn new(monitor: Arc<dyn MempoolMonitor>) -> Self {
        Self {
            monitor,
            recent_stats: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_SAMPLES))),
            last_update: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Updates the internal statistics buffer from the mempool stream.
    #[instrument(skip(self), name = "mempool_stats::update")]
    async fn update_stats(&self) -> Result<(), MempoolError> {
        let mut last_update = self.last_update.lock().await;
        // Only update if it's been a little while, to avoid lock contention
        if last_update.elapsed() < Duration::from_millis(500) {
            return Ok(());
        }

        let mut receiver = self.monitor.subscribe_to_raw_transactions();
        let mut stats = self.recent_stats.lock().await;

        // Prune old samples
        let now = Instant::now();
        stats.retain(|s| now.duration_since(s.timestamp) < STATS_WINDOW);

        // Consume new samples from the channel
        while let Ok(tx) = receiver.try_recv() {
            if stats.len() >= MAX_SAMPLES {
                stats.pop_front();
            }
            stats.push_back(StatsSample {
                timestamp: now,
                base_fee: tx
                    .max_fee_per_gas
                    .map_or(0, |v| v.as_u64()),
                priority_fee: tx
                    .max_priority_fee_per_gas
                    .map_or(0, |v| v.as_u64()),
            });
        }
        *last_update = now;
        Ok(())
    }

    /// Calculates the congestion score based on recent samples.
    fn calculate_congestion(&self, samples: &VecDeque<StatsSample>) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        // More sophisticated logic can be added here, e.g., considering gas price spikes.
        // For now, congestion is a function of transaction throughput.
        let tx_rate = samples.len() as f64 / STATS_WINDOW.as_secs_f64();
        // Normalize against a hypothetical "max sane throughput" of 50 tx/s
        (tx_rate / 50.0).min(1.0)
    }

    /// Calculates the trend of the base fee.
    fn calculate_base_fee_trend(&self, samples: &VecDeque<StatsSample>) -> f64 {
        if samples.len() < 10 {
            return 1.0; // Neutral trend if not enough data
        }
        let half_point = samples.len() / 2;
        let first_half_avg = samples.iter().take(half_point).map(|s| s.base_fee).sum::<u64>() as f64 / half_point as f64;
        let second_half_avg = samples.iter().skip(half_point).map(|s| s.base_fee).sum::<u64>() as f64 / (samples.len() - half_point) as f64;

        if first_half_avg > 0.0 {
            second_half_avg / first_half_avg
        } else {
            1.0 // Neutral
        }
    }

    /// Calculates percentiles for priority fees.
    fn calculate_priority_fee_percentiles(&self, samples: &VecDeque<StatsSample>) -> std::collections::HashMap<String, u64> {
        if samples.is_empty() {
            return std::collections::HashMap::new();
        }
        let mut fees: Vec<u64> = samples.iter().map(|s| s.priority_fee).collect();
        fees.sort_unstable();
        
        let mut percentiles = std::collections::HashMap::new();
        percentiles.insert("p25".to_string(), fees.get((fees.len() as f64 * 0.25) as usize).copied().unwrap_or(0));
        percentiles.insert("p50".to_string(), fees.get((fees.len() as f64 * 0.50) as usize).copied().unwrap_or(0));
        percentiles.insert("p75".to_string(), fees.get((fees.len() as f64 * 0.75) as usize).copied().unwrap_or(0));
        percentiles.insert("p95".to_string(), fees.get((fees.len() as f64 * 0.95) as usize).copied().unwrap_or(0));
        
        percentiles
    }
}

#[async_trait]
impl MempoolStatsProvider for LiveMempoolStatsProvider {
    #[instrument(skip(self), name = "mempool_stats::get_stats")]
    async fn get_stats(&self) -> Result<MempoolStats, MempoolError> {
        self.update_stats().await?;
        let stats_guard = self.recent_stats.lock().await;

        let congestion_score = self.calculate_congestion(&stats_guard);
        let base_fee_trend = self.calculate_base_fee_trend(&stats_guard);
        let priority_percentiles = self.calculate_priority_fee_percentiles(&stats_guard);

        let result = MempoolStats {
            pending_tx_count: stats_guard.len() as u64,
            congestion_score,
            base_fee_per_gas_trend: base_fee_trend,
            priority_fee_percentiles: priority_percentiles,
        };
        
        debug!(?result, "Calculated latest mempool stats.");
        Ok(result)
    }
} 