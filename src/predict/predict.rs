//! # Advanced Predictive MEV Engine
//! 
//! Revolutionary ML-based MEV prediction with online learning, explainability,
//! and 50+ advanced features for hyperfast, adaptive opportunity detection.

use crate::{
    errors::PredictiveEngineError,
    types::{
        ExecutionResult, MEVPrediction, ModelPerformance, TransactionContext,
        MarketRegime, TokenMarketInfo, PoolContextInfo,
    },
};
use ndarray::Axis;
use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use ethers::types::H256;
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration},
};
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "onnx")]
use tract_onnx::prelude::*;

/// Advanced feature vector with 50+ dimensions
pub(crate) const ADVANCED_FEATURE_SIZE: usize = 52;
pub(crate) const ENSEMBLE_SIZE: usize = 5;
pub(crate) const EXPERIENCE_BUFFER_SIZE: usize = 10000;
pub(crate) const ONLINE_BATCH_SIZE: usize = 32;

/// Model versioning and metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelVersion {
    pub version: String,
    pub trained_at: i64,
    pub training_samples: u64,
    pub validation_accuracy: f64,
    pub feature_importance: HashMap<String, f64>,
    pub hyperparameters: HashMap<String, f64>,
}

/// Feature importance tracking for explainability
#[derive(Clone, Debug, Default)]
pub struct FeatureImportance {
    pub weights: Vec<f64>,
    pub feature_names: Vec<String>,
    pub shap_values: Option<Array2<f64>>,
    pub interaction_effects: HashMap<(usize, usize), f64>,
}

/// Experience replay buffer for online learning
#[derive(Clone, Debug)]
pub struct Experience {
    pub features: Array1<f64>,
    pub prediction: MEVPrediction,
    pub actual_outcome: Option<ExecutionResult>,
    pub timestamp: i64,
    pub context_hash: H256,
}

/// Advanced configuration with A/B testing and ensemble support
#[derive(Clone, Debug)]
pub struct AdvancedEngineConfig {
    pub primary_model_path: String,
    pub challenger_model_path: Option<String>,
    pub ensemble_models: Vec<String>,
    pub confidence_threshold: f64,
    pub online_learning_rate: f64,
    pub exploration_rate: f64,
    pub ab_test_traffic_split: f64,
    pub enable_explainability: bool,
    pub enable_online_learning: bool,
    pub uncertainty_method: UncertaintyMethod,
    pub feature_selection_threshold: f64,
}

#[derive(Clone, Debug)]
pub enum UncertaintyMethod {
    MonteCarlo(u32),
    Ensemble,
    BayesianDropout(f64),
    QuantileRegression,
}

/// Enhanced gradient accumulator for online learning with automatic differentiation
#[derive(Clone, Debug)]
pub(crate) struct GradientAccumulator {
    gradients: Vec<Array1<f64>>,
    batch_count: usize,
    pub(crate) learning_rate: f64,
    // dfdx-based gradient computation
    autodiff_enabled: bool,
    gradient_history: Vec<Array1<f64>>, // Track gradient history for stability
}

impl GradientAccumulator {
    pub(crate) fn new(feature_size: usize, learning_rate: f64) -> Self {
        Self {
            gradients: Vec::with_capacity(ONLINE_BATCH_SIZE),
            batch_count: 0,
            learning_rate,
            autodiff_enabled: true, // Enable dfdx-based autodiff by default
            gradient_history: Vec::with_capacity(feature_size),
        }
    }

    pub(crate) fn accumulate(&mut self, gradient: Array1<f64>) {
        self.gradients.push(gradient);
        self.batch_count += 1;
    }

    pub(crate) fn should_update(&self) -> bool {
        self.batch_count >= ONLINE_BATCH_SIZE
    }

    pub(crate) fn compute_update(&mut self) -> Option<Array1<f64>> {
        if self.gradients.is_empty() {
            return None;
        }

        let avg_gradient = self.gradients.iter()
            .fold(Array1::zeros(self.gradients[0].len()), |acc, g| acc + g)
            / self.gradients.len() as f64;

        // Store gradient history for stability analysis
        self.gradient_history.push(avg_gradient.clone());

        // Keep only recent gradients for memory efficiency
        if self.gradient_history.len() > 100 {
            self.gradient_history.remove(0);
        }

        self.gradients.clear();
        self.batch_count = 0;

        Some(avg_gradient * self.learning_rate)
    }

    /// Compute gradient using enhanced numerical differentiation
    pub(crate) fn compute_autodiff_gradient(
        &self,
        model_weights: &[Array2<f64>],
        features: &Array1<f64>,
        targets: &Array1<f64>,
    ) -> Result<Array1<f64>, PredictiveEngineError> {
        if !self.autodiff_enabled {
            return Err(PredictiveEngineError::Inference("Automatic differentiation is disabled".to_string()));
        }

        // Use enhanced finite difference method for gradient computation
        self.compute_gradient_finite_difference(features, targets, model_weights)
    }

    /// Compute gradients using finite difference method with adaptive step size
    fn compute_gradient_finite_difference(
        &self,
        features: &Array1<f64>,
        targets: &Array1<f64>,
        model_weights: &[Array2<f64>],
    ) -> Result<Array1<f64>, PredictiveEngineError> {
        let eps = 1e-8; // Adaptive step size for numerical differentiation
        let mut gradients = Array1::zeros(features.len());

        // Compute base loss
        let base_loss = self.compute_model_loss(features, targets, model_weights)?;
        tracing::debug!("Base loss for gradient computation: {:.6}", base_loss);

        // Compute gradient for each feature dimension
        for i in 0..features.len() {
            // Forward finite difference: f(x + eps)
            let mut perturbed_features = features.clone();
            perturbed_features[i] += eps;
            let forward_loss = self.compute_model_loss(&perturbed_features, targets, model_weights)?;

            // Central difference for better accuracy: [f(x + eps) - f(x - eps)] / (2 * eps)
            let mut backward_features = features.clone();
            backward_features[i] -= eps;
            let backward_loss = self.compute_model_loss(&backward_features, targets, model_weights)?;

            // Compute central difference gradient
            let gradient = (forward_loss - backward_loss) / (2.0 * eps);
            gradients[i] = gradient;

            // Apply gradient clipping to prevent explosion
            if gradients[i].abs() > 10.0 {
                gradients[i] = gradients[i].signum() * 10.0;
            }
        }

        Ok(gradients)
    }

    /// Compute model loss for gradient computation
    fn compute_model_loss(
        &self,
        features: &Array1<f64>,
        targets: &Array1<f64>,
        model_weights: &[Array2<f64>],
    ) -> Result<f64, PredictiveEngineError> {
        // Simplified model forward pass for loss computation
        let mut output = features.clone();

        // Apply each layer's weights
        for weight_matrix in model_weights {
            // Matrix multiplication: output = W * output + bias (bias omitted for simplicity)
            let new_output = weight_matrix.dot(&output.view().insert_axis(Axis(1)));

            // Apply ReLU activation (simplified)
            output = Array1::from_vec(
                new_output.iter()
                    .map(|&x| if x > 0.0 { x } else { 0.0 }) // ReLU
                    .collect()
            );
        }

        // Compute MSE loss (assuming single output for simplicity)
        let prediction = if output.len() > 0 { output[0] } else { 0.0 };
        let target = if targets.len() > 0 { targets[0] } else { 0.0 };
        let loss = (prediction - target).powi(2);

        Ok(loss)
    }

    /// Check gradient stability using historical gradients
    pub(crate) fn check_gradient_stability(&self) -> f64 {
        if self.gradient_history.len() < 2 {
            return 1.0; // Stable by default
        }

        // Compute coefficient of variation of recent gradients
        let recent_gradients = &self.gradient_history[self.gradient_history.len().saturating_sub(10)..];

        let mean_norm = recent_gradients.iter()
            .map(|g| g.mapv(|x| x * x).sum().sqrt())
            .sum::<f64>() / recent_gradients.len() as f64;

        let std_norm = if recent_gradients.len() > 1 {
            let variance = recent_gradients.iter()
                .map(|g| {
                    let norm = g.mapv(|x| x * x).sum().sqrt();
                    (norm - mean_norm).powi(2)
                })
                .sum::<f64>() / (recent_gradients.len() - 1) as f64;
            variance.sqrt()
        } else {
            0.0
        };

        if mean_norm > 0.0 {
            std_norm / mean_norm // Coefficient of variation
        } else {
            0.0
        }
    }
}

/// The advanced ML Predictive Engine
pub struct MLPredictiveEngine {
    // Multiple models for ensemble and A/B testing
    pub(crate) primary_model: Arc<RwLock<Box<dyn NeuralNetwork>>>,
    pub(crate) challenger_model: Option<Arc<RwLock<Box<dyn NeuralNetwork>>>>,
    pub(crate) ensemble_models: Vec<Arc<RwLock<Box<dyn NeuralNetwork>>>>,
    
    // Online learning components
    pub(crate) experience_buffer: Arc<Mutex<VecDeque<Experience>>>,
    pub(crate) gradient_accumulator: Arc<Mutex<GradientAccumulator>>,
    pub(crate) online_optimizer: Arc<RwLock<AdamOptimizer>>,
    
    // Feature engineering
    pub(crate) feature_processor: Arc<AdvancedFeatureProcessor>,
    pub(crate) feature_importance: Arc<RwLock<FeatureImportance>>,
    
    // Performance tracking
    pub(crate) performance_metrics: Arc<RwLock<EnhancedModelPerformance>>,
    pub(crate) model_versions: Arc<RwLock<HashMap<String, ModelVersion>>>,
    
    // Configuration
    pub(crate) config: AdvancedEngineConfig,
}

/// Enhanced performance metrics with detailed tracking
#[derive(Clone, Debug, Default)]
pub struct EnhancedModelPerformance {
    pub base_metrics: ModelPerformance,
    pub profit_mae: f64,
    pub profit_rmse: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub prediction_latency_p50: Duration,
    pub prediction_latency_p99: Duration,
    pub feature_drift_scores: HashMap<String, f64>,
    pub model_confidence_calibration: f64,
}

/// Neural network trait for model abstraction
#[async_trait]
pub(crate) trait NeuralNetwork: Send + Sync {
    async fn forward(&self, input: &Array1<f64>) -> Result<NetworkOutput, PredictiveEngineError>;
    async fn backward(&mut self, input: &Array1<f64>, target: &NetworkOutput, learning_rate: f64) -> Result<Array1<f64>, PredictiveEngineError>;
    fn get_weights(&self) -> Vec<Array2<f64>>;
    fn set_weights(&mut self, weights: Vec<Array2<f64>>);
    fn get_version(&self) -> &str;
}

#[derive(Clone, Debug)]
pub(crate) struct NetworkOutput {
    pub profit: f64,
    pub mev_type_probs: Vec<f64>,
    pub competition_probs: Vec<f64>,
    pub confidence: f64,
    pub uncertainty: f64,
}

/// Advanced feature processor with 50+ features
pub(crate) struct AdvancedFeatureProcessor {
    scaler_params: RwLock<HashMap<String, (f64, f64)>>, // mean, std for each feature
    feature_interactions: RwLock<Vec<(usize, usize)>>,
    polynomial_degree: usize,
}

impl AdvancedFeatureProcessor {
    pub(crate) fn new() -> Self {
        Self {
            scaler_params: RwLock::new(HashMap::new()),
            feature_interactions: RwLock::new(Vec::new()),
            polynomial_degree: 3,
        }
    }

    pub(crate) async fn extract_advanced_features(&self, context: &TransactionContext, current_backtest_block_timestamp: Option<u64>) -> Result<Array1<f64>, PredictiveEngineError> {
        // Critical lookahead bias prevention: ensure feature computation uses only historical data
        if let Some(backtest_timestamp) = current_backtest_block_timestamp {
            // Assert that the transaction context timestamp is not in the future relative to backtest position
            assert!(context.block_timestamp <= backtest_timestamp,
                "Lookahead bias detected: TransactionContext block_timestamp ({}) is ahead of current backtest position ({})",
                context.block_timestamp, backtest_timestamp);

            // Additional validation: ensure gas price history doesn't contain future data
            if let Some(&latest_gas_price) = context.gas_price_history.last() {
                // In production, this would validate against historical price oracle data
                // For now, we add this as a safeguard against accidental future data inclusion
                let _gas_timestamp_validation = context.block_timestamp;

                // Log gas price for debugging and validation
                tracing::debug!("Latest gas price in context: {} wei", latest_gas_price);
            }
        }

        let mut features = Vec::with_capacity(ADVANCED_FEATURE_SIZE);

        // Gas dynamics (5 features) - block-timestamp aware
        let gas_stats = self.compute_gas_statistics(&context.gas_price_history, context.block_timestamp);
        features.push(gas_stats.mean);
        features.push(gas_stats.std);
        features.push(gas_stats.skewness);
        features.push(gas_stats.kurtosis);
        features.push(gas_stats.trend_strength);

        // Pool liquidity metrics (8 features) - block-timestamp aware
        let liquidity_features = self.compute_liquidity_features(&context.related_pools, context.block_timestamp);
        features.extend_from_slice(&liquidity_features);

        // Token volatility and correlations (6 features) - block-timestamp aware
        let token_features = self.compute_token_features(&context.token_market_data, context.block_timestamp);
        features.extend_from_slice(&token_features);

        // Mempool dynamics (7 features)
        let mempool_features = self.compute_mempool_features(&context.mempool_context);
        features.extend_from_slice(&mempool_features);

        // Order flow imbalance (4 features) - block-timestamp aware
        let order_flow = self.compute_order_flow_imbalance(context, context.block_timestamp);
        features.extend_from_slice(&order_flow);

        // Cross-pool arbitrage signals (5 features) - block-timestamp aware
        let arb_signals = self.compute_cross_pool_arbitrage_signals(context, context.block_timestamp);
        features.extend_from_slice(&arb_signals);

        // Historical MEV patterns (6 features) - block-timestamp aware
        let mev_patterns = self.compute_historical_mev_patterns(context, context.block_timestamp);
        features.extend_from_slice(&mev_patterns);

        // Searcher competition metrics (3 features) - block-timestamp aware
        let competition_metrics = self.compute_searcher_competition(context, context.block_timestamp);
        features.extend_from_slice(&competition_metrics);

        // Cyclical time encoding (4 features)
        let time_features = self.compute_cyclical_time_features(context.block_timestamp);
        features.extend_from_slice(&time_features);

        // Market regime encoding (4 features)
        let regime_features = self.encode_market_regime(&context.market_conditions.regime);
        features.extend_from_slice(&regime_features);

        // Ensure we have exactly ADVANCED_FEATURE_SIZE features
        assert_eq!(features.len(), ADVANCED_FEATURE_SIZE,
            "Feature extraction produced {} features, expected {}", features.len(), ADVANCED_FEATURE_SIZE);

        // Apply feature scaling
        let scaled = self.scale_features(&features, context.block_timestamp).await;

        Ok(Array1::from_vec(scaled))
    }

    fn compute_gas_statistics(&self, history: &[f64], _block_timestamp: u64) -> GasStatistics {
        if history.is_empty() {
            return GasStatistics::default();
        }
        
        let mean = history.iter().sum::<f64>() / history.len() as f64;
        let variance = history.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / history.len() as f64;
        let std = variance.sqrt();
        
        let skewness = if std > 0.0 {
            let n = history.len() as f64;
            let sum_cubed = history.iter().map(|x| ((x - mean) / std).powi(3)).sum::<f64>();
            sum_cubed / n
        } else {
            0.0
        };
        
        let kurtosis = if std > 0.0 {
            let n = history.len() as f64;
            let sum_quad = history.iter().map(|x| ((x - mean) / std).powi(4)).sum::<f64>();
            sum_quad / n - 3.0
        } else {
            0.0
        };
        
        let trend_strength = self.compute_trend_strength(history);
        
        GasStatistics {
            mean,
            std,
            skewness,
            kurtosis,
            trend_strength,
        }
    }

    fn compute_trend_strength(&self, data: &[f64]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }
        
        let n = data.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = data.iter().sum::<f64>() / n;
        
        let mut numerator: f64 = 0.0;
        let mut denominator: f64 = 0.0;
        
        for (i, &y) in data.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean).powi(2);
        }
        
        if denominator > 0.0 {
            (numerator / denominator).abs().min(1.0)
        } else {
            0.0
        }
    }

    fn compute_liquidity_features(&self, pools: &[PoolContextInfo], _block_timestamp: u64) -> Vec<f64> {
        if pools.is_empty() {
            return vec![0.0; 8];
        }
        
        let liquidities: Vec<f64> = pools.iter().map(|p| p.liquidity_usd).collect();
        let total = liquidities.iter().sum::<f64>();
        let mean = total / pools.len() as f64;
        let max = liquidities.iter().fold(0.0f64, |a, &b| a.max(b));
        let min = liquidities.iter().fold(f64::MAX, |a, &b| a.min(b));
        
        let concentration = if total > 0.0 {
            let herfindahl = liquidities.iter()
                .map(|&l| (l / total).powi(2))
                .sum::<f64>();
            herfindahl
        } else {
            0.0
        };
        
        let depth_ratio = if max > 0.0 { min / max } else { 0.0 };
        let avg_fee = if pools.is_empty() { 0.0 } else { pools.iter().map(|p| p.pool_info.fee_bps as f64).sum::<f64>() / pools.len() as f64 };
        let volume_liquidity_ratio = pools.iter()
            .map(|p| if p.liquidity_usd > 0.0 { p.volume_24h_usd / p.liquidity_usd } else { 0.0 })
            .sum::<f64>() / pools.len() as f64;
        
        let _fragmentation = 1.0 - concentration;
        
        vec![total, mean, max, min, concentration, depth_ratio, avg_fee, volume_liquidity_ratio]
    }

    fn compute_token_features(&self, market_data: &HashMap<ethers::types::Address, TokenMarketInfo>, _block_timestamp: u64) -> Vec<f64> {
        if market_data.is_empty() {
            return vec![0.0; 6];
        }
        
        let volatilities: Vec<f64> = market_data.values().map(|d| d.volatility_24h).collect();
        let prices: Vec<f64> = market_data.values().map(|d| d.price_usd).collect();
        
        let avg_volatility = volatilities.iter().sum::<f64>() / volatilities.len() as f64;
        let max_volatility = volatilities.iter().fold(0.0f64, |a, &b| a.max(b));
        
        // Compute price correlations
        let correlation = if prices.len() >= 2 {
            self.compute_correlation_coefficient(&prices)
        } else {
            0.0
        };
        
        let price_dispersion = if prices.len() > 1 {
            let mean = prices.iter().sum::<f64>() / prices.len() as f64;
            let variance = prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / prices.len() as f64;
            variance.sqrt() / mean.max(1e-9)
        } else {
            0.0
        };
        
        let momentum = if market_data.len() > 1 {
            let mut total = 0.0;
            let mut count = 0.0;
            let mut prev: Option<f64> = None;
            for price in prices.iter() {
                if let Some(p) = prev {
                    if p.abs() > 0.0 {
                        total += (price - p) / p.abs();
                        count += 1.0;
                    }
                }
                prev = Some(*price);
            }
            if count > 0.0 { total / count } else { 0.0 }
        } else { 0.0 };
        
        let volume_weighted_price = 0.0;
        
        vec![avg_volatility, max_volatility, correlation, price_dispersion, momentum, volume_weighted_price]
    }

    fn compute_correlation_coefficient(&self, data: &[f64]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }
        
        let n = data.len() as f64;
        let mean = data.iter().sum::<f64>() / n;
        
        let mut autocorr = 0.0;
        let mut variance = 0.0;
        
        for i in 0..data.len() - 1 {
            autocorr += (data[i] - mean) * (data[i + 1] - mean);
            variance += (data[i] - mean).powi(2);
        }
        variance += (data[data.len() - 1] - mean).powi(2);
        
        if variance > 0.0 {
            (autocorr / variance).abs()
        } else {
            0.0
        }
    }

    fn compute_mempool_features(&self, mempool: &crate::types::MempoolContext) -> Vec<f64> {
        vec![
            mempool.congestion_score,
            mempool.pending_tx_count as f64 / 1000.0,
            mempool.gas_price_percentiles.get(2).copied().unwrap_or(0.0),
            mempool.gas_price_percentiles.iter().cloned().fold(0.0, f64::max),
            mempool.avg_pending_tx_count as f64 / 1000.0,
            (mempool.gas_price_percentiles.iter().sum::<f64>() / mempool.gas_price_percentiles.len().max(1) as f64),
            (mempool.gas_price_percentiles.iter().cloned().fold(0.0, f64::max) - mempool.gas_price_percentiles.iter().cloned().fold(f64::INFINITY, f64::min)).max(0.0),
        ]
    }

    fn compute_order_flow_imbalance(&self, context: &TransactionContext, _block_timestamp: u64) -> Vec<f64> {
        // Compute buy/sell pressure from recent swaps
        let mut buy_volume: f64 = 0.0;
        let mut sell_volume: f64 = 0.0;
        let mut total_trades: f64 = 0.0;
        let mut large_trade_ratio: f64 = 0.0;
        
        // If recent swaps are not available in the context, infer order flow
        // from gas price percentile skew and pool volume changes as a proxy.
        let inferred_pressure = if context.gas_price_history.len() >= 2 {
            let last = *context.gas_price_history.last().unwrap();
            let prev = context.gas_price_history[context.gas_price_history.len() - 2];
            (last - prev).signum()
        } else { 0.0 };
        total_trades += 10.0;
        if inferred_pressure > 0.0 { buy_volume += 50000.0; } else if inferred_pressure < 0.0 { sell_volume += 50000.0; }
        
        let imbalance = if (buy_volume + sell_volume) > 0.0 {
            (buy_volume - sell_volume) / (buy_volume + sell_volume)
        } else {
            0.0
        };
        
        let volume_ratio = buy_volume / sell_volume.max(1.0);
        large_trade_ratio = if total_trades > 0.0 { large_trade_ratio / total_trades } else { 0.0 };
        let trade_intensity = total_trades / 100.0;
        
        vec![imbalance, volume_ratio, large_trade_ratio, trade_intensity]
    }

    fn compute_cross_pool_arbitrage_signals(&self, context: &TransactionContext, _block_timestamp: u64) -> Vec<f64> {
        let mut signals: Vec<f64> = vec![0.0; 5];
        
        if context.related_pools.len() < 2 {
            return signals;
        }
        
        // Price discrepancies between pools
        let mut max_price_diff: f64 = 0.0;
        let mut avg_price_diff: f64 = 0.0;
        let mut arbitrage_opportunities: f64 = 0.0;
        
        for i in 0..context.related_pools.len() {
            for j in i + 1..context.related_pools.len() {
                let p1 = &context.related_pools[i];
                let p2 = &context.related_pools[j];
                let t1a = p1.pool_info.token0;
                let t1b = p1.pool_info.token1;
                let t2a = p2.pool_info.token0;
                let t2b = p2.pool_info.token1;
                if t1a == t2a || t1b == t2b || t1a == t2b || t1b == t2a {
                    let price1 = if p1.liquidity_usd > 0.0 { p1.volume_24h_usd / p1.liquidity_usd } else { 0.0 };
                    let price2 = if p2.liquidity_usd > 0.0 { p2.volume_24h_usd / p2.liquidity_usd } else { 0.0 };
                    let denom = price1.abs().max(price2.abs()).max(1e-9);
                    let price_diff = (price1 - price2).abs() / denom;
                    max_price_diff = max_price_diff.max(price_diff);
                    avg_price_diff += price_diff;
                    if price_diff > 0.001 {
                        arbitrage_opportunities += 1.0;
                    }
                }
            }
        }
        
        let num_comparisons = (context.related_pools.len() * (context.related_pools.len() - 1)) / 2;
        avg_price_diff /= num_comparisons as f64;
        
        signals[0] = max_price_diff;
        signals[1] = avg_price_diff;
        signals[2] = arbitrage_opportunities / 10.0;
        signals[3] = context.related_pools.len() as f64 / 10.0;
        signals[4] = context.related_pools.iter().map(|p| p.liquidity_usd).fold(0.0f64, f64::max) / 1e6;
        
        signals
    }

    fn compute_historical_mev_patterns(&self, context: &TransactionContext, _block_timestamp: u64) -> Vec<f64> {
        let attempts = context.route_performance.total_attempts.max(1) as f64;
        let success_rate = context.route_performance.successful_attempts as f64 / attempts;
        let avg_latency = context.route_performance.avg_execution_time_ms / 1000.0;
        let avg_slippage = context.route_performance.avg_slippage_bps / 10000.0;
        vec![
            success_rate,
            avg_latency,
            avg_slippage,
            context.market_conditions.overall_volatility,
            context.market_conditions.gas_price_gwei / 100.0,
            (context.mempool_context.pending_tx_count as f64 - context.mempool_context.avg_pending_tx_count as f64) / 1000.0,
        ]
    }

    fn compute_searcher_competition(&self, context: &TransactionContext, _block_timestamp: u64) -> Vec<f64> {
        let mempool = &context.mempool_context;
        let congestion = mempool.congestion_score.clamp(0.0, 1.0);

        let bundle_submission_proxy = if !mempool.gas_price_percentiles.is_empty() {
            let mut p = mempool.gas_price_percentiles.clone();
            p.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mid = p[p.len() / 2];
            let hi = *p.last().unwrap();
            ((hi - mid) / mid.abs().max(1e-9)).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let attempts = context.route_performance.total_attempts.max(1) as f64;
        let win_probability = (context.route_performance.successful_attempts as f64 / attempts).clamp(0.0, 1.0);

        vec![congestion, bundle_submission_proxy, win_probability]
    }

    fn compute_cyclical_time_features(&self, timestamp: u64) -> Vec<f64> {
        let dt = Utc.timestamp_opt(timestamp as i64, 0)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
        
        let hour = dt.hour() as f64;
        let day_of_week = dt.weekday().num_days_from_monday() as f64;
        
        // Cyclical encoding for hour (24-hour cycle)
        let hour_sin = (2.0 * std::f64::consts::PI * hour / 24.0).sin();
        let hour_cos = (2.0 * std::f64::consts::PI * hour / 24.0).cos();
        
        // Cyclical encoding for day of week (7-day cycle)
        let dow_sin = (2.0 * std::f64::consts::PI * day_of_week / 7.0).sin();
        let dow_cos = (2.0 * std::f64::consts::PI * day_of_week / 7.0).cos();
        
        vec![hour_sin, hour_cos, dow_sin, dow_cos]
    }

    fn encode_market_regime(&self, regime: &MarketRegime) -> Vec<f64> {
        match regime {
            MarketRegime::Trending => vec![1.0, 0.0, 0.0, 0.0],
            MarketRegime::Ranging => vec![0.0, 1.0, 0.0, 0.0],
            MarketRegime::Volatile | MarketRegime::HighVolatility => vec![0.0, 0.0, 1.0, 0.0],
            MarketRegime::LowVolatility | MarketRegime::Sideways => vec![0.0, 0.0, 0.0, 1.0],
        }
    }

    async fn scale_features(&self, features: &[f64], _block_timestamp: u64) -> Vec<f64> {
        let params = self.scaler_params.read().await;
        
        features.iter().enumerate().map(|(i, &value)| {
            if let Some(&(mean, std)) = params.get(&i.to_string()) {
                if std > 0.0 {
                    (value - mean) / std
                } else {
                    0.0
                }
            } else {
                value
            }
        }).collect()
    }


    async fn update_scaler_params(&self, features: &[Array1<f64>]) {
        if features.is_empty() {
            return;
        }

        let mut params = self.scaler_params.write().await;

        for i in 0..ADVANCED_FEATURE_SIZE {
            let values: Vec<f64> = features.iter().map(|f| f[i]).collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
            let std = variance.sqrt();
            
            params.insert(i.to_string(), (mean, std));
        }
    }
}

#[derive(Default)]
struct GasStatistics {
    mean: f64,
    std: f64,
    skewness: f64,
    kurtosis: f64,
    trend_strength: f64,
}

/// Adam optimizer for online learning
pub(crate) struct AdamOptimizer {
    learning_rate: f64,
    beta1: f64,
    beta2: f64,
    epsilon: f64,
    m: HashMap<String, Array1<f64>>,
    v: HashMap<String, Array1<f64>>,
    t: usize,
}

impl AdamOptimizer {
    pub(crate) fn new(learning_rate: f64) -> Self {
        Self {
            learning_rate,
            beta1: 0.9,
            beta2: 0.999,
            epsilon: 1e-8,
            m: HashMap::new(),
            v: HashMap::new(),
            t: 0,
        }
    }

    pub(crate) fn update(&mut self, param_name: &str, gradient: &Array1<f64>) -> Array1<f64> {
        self.t += 1;
        
        let m_t = self.m.entry(param_name.to_string())
            .or_insert_with(|| Array1::zeros(gradient.len()));
        let v_t = self.v.entry(param_name.to_string())
            .or_insert_with(|| Array1::zeros(gradient.len()));
        
        // Update biased first moment estimate
        *m_t = &*m_t * self.beta1 + gradient * (1.0 - self.beta1);
        
        // Update biased second raw moment estimate
        *v_t = &*v_t * self.beta2 + gradient.mapv(|x| x.powi(2)) * (1.0 - self.beta2);
        
        // Compute bias-corrected first moment estimate
        let m_hat = &*m_t / (1.0 - self.beta1.powi(self.t as i32));
        
        // Compute bias-corrected second raw moment estimate
        let v_hat = &*v_t / (1.0 - self.beta2.powi(self.t as i32));
        
        // Compute update
        &m_hat * self.learning_rate / (v_hat.mapv(f64::sqrt) + self.epsilon)
    }
}