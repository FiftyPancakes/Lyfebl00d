//! Implementation of the Advanced Predictive MEV Engine

use crate::errors::PredictiveEngineError;
use crate::types::{
    CompetitionLevel, ExecutionResult, MEVPrediction, MEVType, MarketRegime, ModelPerformance,
    PredictionFeatures, TransactionContext,
};
use ethers::types::H256;
use ethers::utils::keccak256;
use ndarray::Array1;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use super::predict::{
    AdvancedEngineConfig, AdvancedFeatureProcessor, EnhancedModelPerformance, Experience,
    FeatureImportance, GradientAccumulator, AdamOptimizer, MLPredictiveEngine, NetworkOutput,
    NeuralNetwork, UncertaintyMethod, ADVANCED_FEATURE_SIZE, EXPERIENCE_BUFFER_SIZE,
};
use super::predict_transformer::TransformerModel;
use super::predict_mod::ExecutionStrategy;

impl MLPredictiveEngine {
    /// Creates a new advanced predictive engine with multiple models and online learning
    pub async fn new(config: AdvancedEngineConfig) -> Result<Self, PredictiveEngineError> {
        info!("Initializing Advanced Predictive MEV Engine...");
        
        // Load primary model
        let primary_model = Self::load_model(&config.primary_model_path).await?;
        
        // Load challenger model for A/B testing
        let challenger_model = if let Some(path) = &config.challenger_model_path {
            Some(Self::load_model(path).await?)
        } else {
            None
        };
        
        // Load ensemble models
        let mut ensemble_models = Vec::new();
        for path in &config.ensemble_models {
            ensemble_models.push(Self::load_model(path).await?);
        }

        // Handle ensemble uncertainty method fallback
        let mut updated_config = config.clone();
        if ensemble_models.is_empty() {
            if matches!(config.uncertainty_method, UncertaintyMethod::Ensemble) {
                updated_config.uncertainty_method = UncertaintyMethod::MonteCarlo(16);
                warn!("No ensemble models provided but Ensemble uncertainty method selected. Falling back to MonteCarlo(16).");
            }
        }
        
        // Initialize online learning components
        let gradient_accumulator = Arc::new(Mutex::new(
            GradientAccumulator::new(ADVANCED_FEATURE_SIZE, config.online_learning_rate)
        ));
        
        let online_optimizer = Arc::new(RwLock::new(
            AdamOptimizer::new(config.online_learning_rate)
        ));
        
        info!("Advanced Predictive MEV Engine initialized successfully");
        
        Ok(Self {
            primary_model,
            challenger_model,
            ensemble_models,
            experience_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(EXPERIENCE_BUFFER_SIZE))),
            gradient_accumulator,
            online_optimizer,
            feature_processor: Arc::new(AdvancedFeatureProcessor::new()),
            feature_importance: Arc::new(RwLock::new(FeatureImportance::default())),
            performance_metrics: Arc::new(RwLock::new(EnhancedModelPerformance::default())),
            model_versions: Arc::new(RwLock::new(HashMap::new())),
            config: updated_config,
        })
    }
    
    async fn load_model(path: &str) -> Result<Arc<RwLock<Box<dyn NeuralNetwork>>>, PredictiveEngineError> {
        // In production, this would load different model types (ONNX, TorchScript, etc.)
        // For now, we'll create a transformer-based model
        let model = TransformerModel::load(path).await?;
        Ok(Arc::new(RwLock::new(Box::new(model))))
    }
    
    /// Make a prediction with uncertainty quantification and explainability
    pub async fn predict_with_explanation(
        &self,
        context: &TransactionContext,
        current_backtest_block_timestamp: Option<u64>,
    ) -> Result<ExplainablePrediction, PredictiveEngineError> {
        let start = Instant::now();

        // Extract advanced features with lookahead bias prevention
        let features = self.feature_processor.extract_advanced_features(context, current_backtest_block_timestamp).await?;
        
        // Determine which model to use (A/B testing) deterministically by tx hash
        let use_challenger = if self.challenger_model.is_some() {
            let mut data = Vec::with_capacity(64);
            data.extend_from_slice(context.tx.hash.as_bytes());
            data.extend_from_slice(b"ab_assignment_salt");
            let hash = keccak256(&data);
            let bucket = u64::from_be_bytes([
                hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
            ]);
            let frac = (bucket as f64) / (u64::MAX as f64);
            frac < self.config.ab_test_traffic_split
        } else {
            false
        };
        
        // Get predictions with uncertainty
        let (prediction, uncertainty, explanations) = if use_challenger && self.challenger_model.is_some() {
            self.predict_with_model(&self.challenger_model.as_ref().unwrap(), &features, context).await?
        } else {
            self.predict_with_model(&self.primary_model, &features, context).await?
        };
        
        // Update latency metrics
        let latency = start.elapsed();
        self.update_latency_metrics(latency).await;
        
        // Store experience for online learning
        if self.config.enable_online_learning {
            self.store_experience(features.clone(), prediction.clone(), context).await?;
        }
        
        Ok(ExplainablePrediction {
            prediction,
            uncertainty,
            explanations,
            model_version: if use_challenger { "challenger" } else { "primary" },
            latency,
        })
    }
    
    async fn predict_with_model(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
        context: &TransactionContext,
    ) -> Result<(MEVPrediction, UncertaintyEstimate, FeatureExplanations), PredictiveEngineError> {
        // Get base prediction
        let model_guard = model.read().await;
        let version = model_guard.get_version().to_string();
        let output = model_guard.forward(features).await?;
        drop(model_guard);
        
        // Compute uncertainty based on configured method
        let uncertainty = self.compute_uncertainty(model, features).await?;
        
        // Generate explanations if enabled
        let explanations = if self.config.enable_explainability {
            self.explain_prediction(model, features, &output).await?
        } else {
            FeatureExplanations::default()
        };
        
        // Apply confidence threshold with uncertainty adjustment
        let adjusted_confidence = output.confidence * (1.0 - uncertainty.epistemic_uncertainty);
        if adjusted_confidence < self.config.confidence_threshold {
            return Ok((
                MEVPrediction::no_opportunity(context.tx.hash),
                uncertainty,
                explanations,
            ));
        }
        
        // Decode MEV type and competition level
        let mev_type = self.decode_mev_type(&output.mev_type_probs)?;
        if mev_type == MEVType::NoMEV {
            return Ok((
                MEVPrediction::no_opportunity(context.tx.hash),
                uncertainty,
                explanations,
            ));
        }
        
        let competition_level = self.decode_competition_level(&output.competition_probs)?;
        
        // Create prediction with all advanced metrics
        let prediction = MEVPrediction {
            prediction_id: self.generate_prediction_id(context, &version),
            transaction_hash: context.tx.hash,
            predicted_profit_usd: output.profit.max(0.0),
            confidence_score: adjusted_confidence,
            risk_score: uncertainty.total_uncertainty,
            mev_type,
            estimated_success_rate: self.estimate_success_rate(&output, &uncertainty, context),
            features: self.create_prediction_features(features),
            competition_level,
        };
        
        Ok((prediction, uncertainty, explanations))
    }
    
    async fn compute_uncertainty(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
    ) -> Result<UncertaintyEstimate, PredictiveEngineError> {
        match &self.config.uncertainty_method {
            UncertaintyMethod::MonteCarlo(n_samples) => {
                self.monte_carlo_uncertainty(model, features, *n_samples).await
            }
            UncertaintyMethod::Ensemble => {
                self.ensemble_uncertainty(features).await
            }
            UncertaintyMethod::BayesianDropout(dropout_rate) => {
                self.bayesian_dropout_uncertainty(model, features, *dropout_rate).await
            }
            UncertaintyMethod::QuantileRegression => {
                self.quantile_regression_uncertainty(model, features).await
            }
        }
    }
    
    async fn monte_carlo_uncertainty(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
        n_samples: u32,
    ) -> Result<UncertaintyEstimate, PredictiveEngineError> {
        let mut predictions = Vec::with_capacity(n_samples as usize);

        // Hold read lock for entire sampling loop to avoid contention
        let model_guard = model.read().await;
        for _ in 0..n_samples {
            // Add noise to features for Monte Carlo sampling
            let noisy_features = features + &Array1::from_shape_fn(features.len(), |_| {
                thread_rng().gen_range(-0.01..0.01)
            });

            let output = model_guard.forward(&noisy_features).await?;
            predictions.push(output.profit);
        }
        drop(model_guard);
        
        let mean = predictions.iter().sum::<f64>() / predictions.len() as f64;
        let variance = predictions.iter()
            .map(|&p| (p - mean).powi(2))
            .sum::<f64>() / predictions.len() as f64;
        
        Ok(UncertaintyEstimate {
            aleatoric_uncertainty: variance.sqrt() / mean.abs().max(1.0),
            epistemic_uncertainty: self.compute_epistemic_uncertainty(&predictions),
            total_uncertainty: (variance.sqrt() / mean.abs().max(1.0)).min(1.0),
        })
    }
    
    async fn ensemble_uncertainty(
        &self,
        features: &Array1<f64>,
    ) -> Result<UncertaintyEstimate, PredictiveEngineError> {
        if self.ensemble_models.is_empty() {
            return Ok(UncertaintyEstimate::default());
        }
        
        let mut predictions = Vec::new();
        
        for model in &self.ensemble_models {
            let model_guard = model.read().await;
            let output = model_guard.forward(features).await?;
            predictions.push(output.profit);
        }
        
        let mean = predictions.iter().sum::<f64>() / predictions.len() as f64;
        let variance = predictions.iter()
            .map(|&p| (p - mean).powi(2))
            .sum::<f64>() / predictions.len() as f64;
        
        Ok(UncertaintyEstimate {
            aleatoric_uncertainty: 0.1, // Base uncertainty
            epistemic_uncertainty: variance.sqrt() / mean.abs().max(1.0),
            total_uncertainty: (variance.sqrt() / mean.abs().max(1.0) + 0.1).min(1.0),
        })
    }
    
    async fn bayesian_dropout_uncertainty(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
        dropout_rate: f64,
    ) -> Result<UncertaintyEstimate, PredictiveEngineError> {
        let n_samples = 20;
        let mut predictions = Vec::with_capacity(n_samples);
        
        for _ in 0..n_samples {
            // Apply dropout mask to features
            let dropout_mask = Array1::from_shape_fn(features.len(), |_| {
                if thread_rng().gen::<f64>() > dropout_rate { 1.0 } else { 0.0 }
            });
            let masked_features = features * &dropout_mask / (1.0 - dropout_rate);
            
            let model_guard = model.read().await;
            let output = model_guard.forward(&masked_features).await?;
            predictions.push(output.profit);
        }
        
        let mean = predictions.iter().sum::<f64>() / predictions.len() as f64;
        let variance = predictions.iter()
            .map(|&p| (p - mean).powi(2))
            .sum::<f64>() / predictions.len() as f64;
        
        Ok(UncertaintyEstimate {
            aleatoric_uncertainty: variance.sqrt() * 0.3,
            epistemic_uncertainty: variance.sqrt() * 0.7,
            total_uncertainty: variance.sqrt().min(1.0),
        })
    }
    
    async fn quantile_regression_uncertainty(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
    ) -> Result<UncertaintyEstimate, PredictiveEngineError> {
        // Get predictions at different quantiles
        let model_guard = model.read().await;
        let median = model_guard.forward(features).await?.profit;
        
        // Simulate quantile predictions (in production, use separate quantile models)
        let q25 = median * 0.75;
        let q75 = median * 1.25;
        
        let iqr = q75 - q25;
        let uncertainty = iqr / median.abs().max(1.0);
        
        Ok(UncertaintyEstimate {
            aleatoric_uncertainty: uncertainty * 0.6,
            epistemic_uncertainty: uncertainty * 0.4,
            total_uncertainty: uncertainty.min(1.0),
        })
    }
    
    fn compute_epistemic_uncertainty(&self, predictions: &[f64]) -> f64 {
        if predictions.len() < 2 {
            return 0.0;
        }
        
        // Use entropy as a measure of epistemic uncertainty
        let mean = predictions.iter().sum::<f64>() / predictions.len() as f64;
        let variance = predictions.iter()
            .map(|&p| (p - mean).powi(2))
            .sum::<f64>() / predictions.len() as f64;
        
        if variance > 0.0 {
            0.5 * (2.0 * std::f64::consts::PI * std::f64::consts::E * variance).ln() / 10.0
        } else {
            0.0
        }
    }
    
    async fn explain_prediction(
        &self,
        model: &Arc<RwLock<Box<dyn NeuralNetwork>>>,
        features: &Array1<f64>,
        output: &NetworkOutput,
    ) -> Result<FeatureExplanations, PredictiveEngineError> {
        // Compute SHAP-like values using perturbation
        let baseline = Array1::zeros(features.len());
        let model_guard = model.read().await;
        let baseline_output = model_guard.forward(&baseline).await?;
        
        let mut shap_values = Vec::with_capacity(features.len());
        let mut feature_importance = Vec::with_capacity(features.len());
        
        for i in 0..features.len() {
            // Perturb single feature
            let mut perturbed = baseline.clone();
            perturbed[i] = features[i];
            let perturbed_output = model_guard.forward(&perturbed).await?;
            
            let contribution = perturbed_output.profit - baseline_output.profit;
            shap_values.push(contribution);
            feature_importance.push(contribution.abs());
        }
        
        // Normalize importance scores
        let total_importance = feature_importance.iter().sum::<f64>();
        if total_importance > 0.0 {
            for importance in &mut feature_importance {
                *importance /= total_importance;
            }
        }
        
        // Identify top contributing features
        let mut indexed_importance: Vec<(usize, f64)> = feature_importance.iter()
            .enumerate()
            .map(|(i, &imp)| (i, imp))
            .collect();
        indexed_importance.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        let top_features = indexed_importance.iter()
            .take(10)
            .map(|(idx, imp)| FeatureContribution {
                feature_name: self.get_feature_name(*idx),
                contribution: shap_values[*idx],
                importance: *imp,
            })
            .collect();
        
        Ok(FeatureExplanations {
            shap_values: Array1::from_vec(shap_values),
            feature_importance: Array1::from_vec(feature_importance),
            top_contributing_features: top_features,
            decision_path: self.extract_decision_path(features, output),
            counterfactual: self.generate_counterfactual(features, output).await,
        })
    }
    
    fn get_feature_name(&self, index: usize) -> String {
        let names = vec![
            "gas_mean", "gas_std", "gas_skewness", "gas_kurtosis", "gas_trend",
            "liquidity_total", "liquidity_mean", "liquidity_max", "liquidity_min",
            "liquidity_concentration", "depth_ratio", "avg_fee", "volume_liquidity_ratio",
            "token_avg_volatility", "token_max_volatility", "token_correlation",
            "price_dispersion", "momentum", "vwap",
            "mempool_congestion", "pending_tx_count", "avg_gas_price", "gas_p90",
            "competing_searchers", "bundle_rate", "reorg_probability",
            "order_imbalance", "volume_ratio", "large_trade_ratio", "trade_intensity",
            "max_price_diff", "avg_price_diff", "arb_opportunities", "pool_count", "max_liquidity",
            "historical_success_rate", "avg_profit", "max_profit", "profit_volatility",
            "recent_success", "time_since_success",
            "searcher_count", "bundle_submission", "estimated_competitors", "win_probability",
            "builder_preference", "inclusion_probability", "avg_bundle_value",
            "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        ];
        
        names.get(index).unwrap_or(&"unknown").to_string()
    }
    
    fn extract_decision_path(&self, features: &Array1<f64>, output: &NetworkOutput) -> Vec<String> {
        let mut path = Vec::new();
        
        // Identify key decision points
        if output.confidence > 0.8 {
            path.push("High confidence prediction".to_string());
        }
        
        if features[0] > 1.0 { // High gas volatility
            path.push("Elevated gas price volatility detected".to_string());
        }
        
        if features[27] > 0.5 { // Order imbalance
            path.push("Significant order flow imbalance".to_string());
        }
        
        if features[31] > 0.01 { // Arbitrage opportunities
            path.push("Cross-pool arbitrage opportunity identified".to_string());
        }
        
        path
    }
    
    async fn generate_counterfactual(&self, features: &Array1<f64>, output: &NetworkOutput) -> Option<Counterfactual> {
        // Find minimal feature changes that would change the prediction
        // This is a simplified version - in production, use optimization algorithms
        
        if output.profit < 100.0 {
            // Find what would make this profitable
            let mut suggested_changes = Vec::new();
            
            if features[27] < 0.3 { // Low order imbalance
                suggested_changes.push("Increase order flow imbalance to >0.5".to_string());
            }
            
            if features[5] < 1e6 { // Low liquidity
                suggested_changes.push("Target pools with >$1M liquidity".to_string());
            }
            
            Some(Counterfactual {
                suggested_changes,
                expected_profit_change: 200.0,
            })
        } else {
            None
        }
    }
    
    /// Online learning: update model with new experience
    pub async fn learn_from_experience(
        &self,
        result: &ExecutionResult,
    ) -> Result<(), PredictiveEngineError> {
        if !self.config.enable_online_learning {
            return Ok(());
        }
        
        let mut buffer = self.experience_buffer.lock().await;
        
        // Find matching experience
        let experience = buffer.iter_mut()
            .find(|exp| exp.prediction.prediction_id == result.prediction_id);
        
        if let Some(exp) = experience {
            exp.actual_outcome = Some(result.clone());
            
            // Compute gradient
            let gradient = self.compute_gradient(&exp.features, result).await?;
            
            // Accumulate gradient
            let mut accumulator = self.gradient_accumulator.lock().await;
            accumulator.accumulate(gradient);
            
            // Apply update if batch is ready
            if accumulator.should_update() {
                if let Some(update) = accumulator.compute_update() {
                    self.apply_model_update(update).await?;
                }
            }
        }
        
        Ok(())
    }
    
    async fn compute_gradient(
        &self,
        features: &Array1<f64>,
        result: &ExecutionResult,
    ) -> Result<Array1<f64>, PredictiveEngineError> {
        let model_guard = self.primary_model.read().await;
        let output = model_guard.forward(features).await?;
        let model_weights = model_guard.get_weights();
        drop(model_guard);

        // Compute loss gradient
        let _predicted = output.profit;
        let actual = result.actual_profit_usd.unwrap_or(0.0);
        let targets = Array1::from_vec(vec![actual]);

        // Use enhanced automatic differentiation for gradient computation
        let accumulator = self.gradient_accumulator.lock().await;

        match accumulator.compute_autodiff_gradient(&model_weights, features, &targets) {
            Ok(autodiff_gradient) => {
                // Check gradient stability
                let stability = accumulator.check_gradient_stability();
                if stability > 0.5 {
                    warn!("Gradient instability detected (coefficient of variation: {:.3}). Using fallback computation.", stability);
                    // Fallback to simplified computation if gradients are unstable
                    let lr = self.gradient_accumulator.lock().await.learning_rate;
                    let mut model = self.primary_model.write().await;
                    let fallback_gradient = model.backward(features, &output, lr).await?;
                    Ok(fallback_gradient)
                } else {
                    Ok(autodiff_gradient)
                }
            }
            Err(e) => {
                warn!("Automatic differentiation failed: {}. Using fallback computation.", e);
                // Fallback to simplified computation
                let lr = self.gradient_accumulator.lock().await.learning_rate;
                let mut model = self.primary_model.write().await;
                let fallback_gradient = model.backward(features, &output, lr).await?;
                Ok(fallback_gradient)
            }
        }
    }
    
    async fn apply_model_update(&self, update: Array1<f64>) -> Result<(), PredictiveEngineError> {
        let mut optimizer = self.online_optimizer.write().await;
        let param_update = optimizer.update("primary", &update);

        // Apply update to model weights
        let mut model = self.primary_model.write().await;
        let mut weights = model.get_weights();

        if weights.is_empty() {
            warn!("No weights to update; skipping online update");
            return Ok(());
        }

        // Update first layer weights as example (in production, update all layers)
        let flat_update = param_update.as_slice().unwrap();
        let weight_shape = weights[0].clone().shape().to_vec();
        let expected_len = weight_shape[0] * weight_shape[1];

        if flat_update.len() != expected_len {
            warn!(
                "Gradient length {} does not match first-layer params {}; disabling online updates. \
                 Consider implementing parameter-shaped gradients.",
                flat_update.len(), expected_len
            );
            return Ok(());
        }

        let rows = weight_shape[0];
        let cols = weight_shape[1];
        for i in 0..rows {
            for j in 0..cols {
                let delta = flat_update[i * cols + j] * 0.001;
                let cell = &mut weights[0][[i, j]];
                *cell -= delta;
            }
        }
        model.set_weights(weights);

        info!("Applied online learning update to model");
        Ok(())
    }
    
    async fn store_experience(
        &self,
        features: Array1<f64>,
        prediction: MEVPrediction,
        context: &TransactionContext,
    ) -> Result<(), PredictiveEngineError> {
        let mut buffer = self.experience_buffer.lock().await;
        
        if buffer.len() >= EXPERIENCE_BUFFER_SIZE {
            buffer.pop_front();
        }
        
        buffer.push_back(Experience {
            features,
            prediction,
            actual_outcome: None,
            timestamp: context.block_timestamp as i64,
            context_hash: context.tx.hash,
        });
        
        Ok(())
    }
    
    fn should_use_challenger(&self) -> bool {
        if self.challenger_model.is_none() {
            return false;
        }
        
        // A/B testing logic
        // Persist A/B assignment per transaction hash
        false
    }
    
    async fn update_latency_metrics(&self, latency: Duration) {
        let mut metrics = self.performance_metrics.write().await;
        
        // Update P50 and P99 (simplified - in production use proper percentile tracking)
        if metrics.prediction_latency_p50 == Duration::default() {
            metrics.prediction_latency_p50 = latency;
        } else {
            metrics.prediction_latency_p50 = (metrics.prediction_latency_p50 + latency) / 2;
        }
        
        if latency > metrics.prediction_latency_p99 {
            metrics.prediction_latency_p99 = latency;
        }
    }
    
    fn decode_mev_type(&self, probs: &[f64]) -> Result<MEVType, PredictiveEngineError> {
        let max_idx = probs.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .ok_or_else(|| PredictiveEngineError::Inference("Empty probability vector".to_string()))?;
        
        match max_idx {
            0 => Ok(MEVType::NoMEV),
            1 => Ok(MEVType::Backrun),
            2 => Ok(MEVType::Sandwich),
            3 => Ok(MEVType::Frontrun),
            4 => Ok(MEVType::Liquidation),
            _ => Err(PredictiveEngineError::Inference(format!("Invalid MEV type index: {}", max_idx))),
        }
    }
    
    fn decode_competition_level(&self, probs: &[f64]) -> Result<CompetitionLevel, PredictiveEngineError> {
        let max_idx = probs.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .ok_or_else(|| PredictiveEngineError::Inference("Empty probability vector".to_string()))?;
        
        match max_idx {
            0 => Ok(CompetitionLevel::Low),
            1 => Ok(CompetitionLevel::Medium),
            2 => Ok(CompetitionLevel::High),
            3 => Ok(CompetitionLevel::Critical),
            _ => Err(PredictiveEngineError::Inference(format!("Invalid competition level index: {}", max_idx))),
        }
    }
    
    fn generate_prediction_id(&self, context: &TransactionContext, model_version: &str) -> H256 {
        let mut data = Vec::with_capacity(32 + model_version.len() + 8);
        data.extend_from_slice(context.tx.hash.as_bytes());
        data.extend_from_slice(model_version.as_bytes());
        data.extend_from_slice(&context.block_timestamp.to_be_bytes());
        H256::from_slice(&keccak256(&data))
    }
    
    fn estimate_success_rate(
        &self,
        output: &NetworkOutput,
        uncertainty: &UncertaintyEstimate,
        context: &TransactionContext,
    ) -> f64 {
        let base_rate = output.confidence * (1.0 - uncertainty.total_uncertainty);
        let historical_adjustment = if context.route_performance.total_attempts > 0 {
            context.route_performance.successful_attempts as f64
                / context.route_performance.total_attempts as f64
        } else {
            0.0
        };
        let competition_penalty = 1.0
            / (1.0 + context.mempool_context.congestion_score.max(0.0));
        
        (base_rate * 0.5 + historical_adjustment * 0.3 + competition_penalty * 0.2).clamp(0.0, 1.0)
    }
    
    fn create_prediction_features(&self, features: &Array1<f64>) -> PredictionFeatures {
        // Extract key features for the prediction record
        PredictionFeatures {
            gas_price_volatility: features[1],
            pool_liquidity_ratio: features[5] / features[6].max(1.0),
            token_volatility: features[13],
            mempool_congestion: features[19],
            historical_success_rate: features[35],
            market_regime: MarketRegime::Volatile, // Simplified
            time_of_day_factor: features[48],
            weekend_factor: features[50],
        }
    }
    
    fn compute_urgency_score(&self, context: &TransactionContext) -> f64 {
        let competition_factor = context
            .mempool_context
            .congestion_score
            .clamp(0.0, 1.0);

        let (p90, avg) = {
            let prices = &context.mempool_context.gas_price_percentiles;
            if prices.is_empty() {
                (0.0, 0.0)
            } else {
                let max_price = prices.iter().cloned().fold(0.0, f64::max);
                let sum: f64 = prices.iter().sum();
                let avg_price = sum / prices.len() as f64;
                (max_price, avg_price)
            }
        };
        let gas_urgency = if avg > 0.0 { p90 / avg } else { 1.0 };

        let success_rate = if context.route_performance.total_attempts > 0 {
            context.route_performance.successful_attempts as f64
                / context.route_performance.total_attempts as f64
        } else {
            0.0
        };
        let time_factor = success_rate.clamp(0.0, 1.0);
        
        (competition_factor * 0.4 + gas_urgency * 0.3 + time_factor * 0.3).clamp(0.0, 1.0)
    }
    
    fn compute_optimal_gas(&self, context: &TransactionContext, output: &NetworkOutput) -> f64 {
        let base_gas = {
            let prices = &context.mempool_context.gas_price_percentiles;
            if prices.is_empty() {
                0.0
            } else {
                let sum: f64 = prices.iter().sum();
                sum / prices.len() as f64
            }
        };
        let competition_multiplier = 1.0 + context.mempool_context.congestion_score.max(0.0) * 0.5;
        let profit_ratio = output.profit / 1000.0;
        let urgency_boost = 1.0 + self.compute_urgency_score(context) * 0.2;
        
        base_gas * competition_multiplier * profit_ratio.min(2.0) * urgency_boost
    }
    
    fn determine_execution_strategy(
        &self,
        output: &NetworkOutput,
        competition: &CompetitionLevel,
    ) -> ExecutionStrategy {
        match competition {
            CompetitionLevel::Low => ExecutionStrategy::Standard,
            CompetitionLevel::Medium => {
                if output.profit > 500.0 {
                    ExecutionStrategy::FastTrack
                } else {
                    ExecutionStrategy::Standard
                }
            }
            CompetitionLevel::High => ExecutionStrategy::FlashLoan,
            CompetitionLevel::Critical => ExecutionStrategy::PrivateMempool,
        }
    }

    pub async fn get_model_performance(&self) -> Result<ModelPerformance, PredictiveEngineError> {
        let enhanced = self.performance_metrics.read().await;
        Ok(enhanced.base_metrics.clone())
    }
}

// Supporting types
#[derive(Clone, Debug)]
pub struct ExplainablePrediction {
    pub prediction: MEVPrediction,
    pub uncertainty: UncertaintyEstimate,
    pub explanations: FeatureExplanations,
    pub model_version: &'static str,
    pub latency: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct UncertaintyEstimate {
    pub aleatoric_uncertainty: f64,
    pub epistemic_uncertainty: f64,
    pub total_uncertainty: f64,
}

#[derive(Clone, Debug, Default)]
pub struct FeatureExplanations {
    pub shap_values: Array1<f64>,
    pub feature_importance: Array1<f64>,
    pub top_contributing_features: Vec<FeatureContribution>,
    pub decision_path: Vec<String>,
    pub counterfactual: Option<Counterfactual>,
}

#[derive(Clone, Debug)]
pub struct FeatureContribution {
    pub feature_name: String,
    pub contribution: f64,
    pub importance: f64,
}

#[derive(Clone, Debug)]
pub struct Counterfactual {
    pub suggested_changes: Vec<String>,
    pub expected_profit_change: f64,
}


// Extension to MEVPrediction
impl MEVPrediction {
    pub fn no_opportunity(tx_hash: H256) -> Self {
        Self {
            prediction_id: H256::zero(),
            transaction_hash: tx_hash,
            predicted_profit_usd: 0.0,
            confidence_score: 0.0,
            risk_score: 1.0,
            mev_type: MEVType::NoMEV,
            estimated_success_rate: 0.0,
            features: PredictionFeatures::default(),
            competition_level: CompetitionLevel::Low,
        }
    }
}