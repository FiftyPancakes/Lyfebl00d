//! Module exports and type extensions for the Advanced Predictive MEV Engine

// Import from sibling modules through the parent module
use super::predict::MLPredictiveEngine;

// Use existing types from the crate
use crate::types::{TransactionContext, MEVPrediction, ExecutionResult, ModelPerformance};
use crate::errors::PredictiveEngineError;

use async_trait::async_trait;

// Additional types specific to the prediction module that don't exist in crate::types
#[derive(Clone, Debug)]
pub enum ExecutionStrategy {
    Standard,
    FastTrack,
    FlashLoan,
    PrivateMempool,
}

// Re-export the main predictive engine trait
#[async_trait]
pub trait PredictiveEngine: Send + Sync + std::fmt::Debug {
    async fn predict<'a>(
        &'a self,
        tx_context: &'a TransactionContext,
        current_backtest_block_timestamp: Option<u64>,
    ) -> Result<Option<MEVPrediction>, PredictiveEngineError>;
    
    async fn update_model<'a>(
        &'a self,
        result: &'a ExecutionResult,
    ) -> Result<(), PredictiveEngineError>;
    
    async fn get_performance_metrics<'a>(&'a self) -> Result<ModelPerformance, PredictiveEngineError>;
}

// Add Debug derive to MLPredictiveEngine
impl std::fmt::Debug for MLPredictiveEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MLPredictiveEngine")
            .field("config", &"AdvancedEngineConfig")
            .finish()
    }
}

// Implement the trait for the advanced engine
#[async_trait]
impl PredictiveEngine for MLPredictiveEngine {
    async fn predict<'a>(
        &'a self,
        tx_context: &'a TransactionContext,
        current_backtest_block_timestamp: Option<u64>,
    ) -> Result<Option<MEVPrediction>, PredictiveEngineError> {
        // Add lookahead bias assertion before prediction
        if let Some(backtest_timestamp) = current_backtest_block_timestamp {
            assert!(tx_context.block_timestamp <= backtest_timestamp,
                "Lookahead bias detected in predictive engine: TransactionContext block_timestamp ({}) is ahead of current backtest position ({})",
                tx_context.block_timestamp, backtest_timestamp);
        }

        let result = self.predict_with_explanation(tx_context, current_backtest_block_timestamp).await?;
        Ok(Some(result.prediction))
    }
    
    async fn update_model<'a>(
        &'a self,
        result: &'a ExecutionResult,
    ) -> Result<(), PredictiveEngineError> {
        self.learn_from_experience(result).await
    }
    
    async fn get_performance_metrics<'a>(&'a self) -> Result<ModelPerformance, PredictiveEngineError> {
        self.get_model_performance().await
    }
}

/// Factory function to create a predictive engine
pub async fn create_predictive_engine(
    model_path: String,
    enable_online_learning: bool,
    enable_explainability: bool,
) -> Result<Box<dyn PredictiveEngine>, PredictiveEngineError> {
    use super::predict::{AdvancedEngineConfig, UncertaintyMethod};
    
    let config = AdvancedEngineConfig {
        primary_model_path: model_path,
        challenger_model_path: None,
        ensemble_models: vec![],
        confidence_threshold: 0.7,
        online_learning_rate: 0.001,
        exploration_rate: 0.05,
        ab_test_traffic_split: 0.1,
        enable_explainability,
        enable_online_learning,
        uncertainty_method: UncertaintyMethod::Ensemble,
        feature_selection_threshold: 0.01,
    };
    
    let engine = MLPredictiveEngine::new(config).await?;
    Ok(Box::new(engine))
}