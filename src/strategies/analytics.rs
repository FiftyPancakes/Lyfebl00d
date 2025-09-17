use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::price_oracle::PriceOracle;

/// Simple analytics implementation for tracking MEV opportunities and performance
pub struct SimpleAnalytics {
    price_oracle: Arc<dyn PriceOracle + Send + Sync>,
    metrics: Arc<RwLock<HashMap<String, f64>>>,
    opportunities: Arc<RwLock<HashMap<String, HashMap<String, f64>>>>,
    executions: Arc<RwLock<Vec<ExecutionRecord>>>,
}

#[derive(Clone, Debug)]
struct ExecutionRecord {
    execution_id: String,
    success: bool,
    profit_usd: f64,
    timestamp: u64,
}

impl SimpleAnalytics {
    pub fn new(price_oracle: Arc<dyn PriceOracle + Send + Sync>) -> Self {
        Self {
            price_oracle,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            opportunities: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl super::AnalyticsTrait for SimpleAnalytics {
    async fn track_opportunity(&self, opportunity_id: &str, metrics: &HashMap<String, f64>) {
        let mut opportunities = self.opportunities.write().await;
        opportunities.insert(opportunity_id.to_string(), metrics.clone());
    }

    async fn get_performance_metrics(&self) -> HashMap<String, f64> {
        let executions = self.executions.read().await;
        let mut metrics = HashMap::new();
        
        if executions.is_empty() {
            return metrics;
        }
        
        let total_executions = executions.len() as f64;
        let successful_executions = executions.iter().filter(|e| e.success).count() as f64;
        let total_profit = executions.iter().map(|e| e.profit_usd).sum::<f64>();
        let avg_profit = total_profit / total_executions;
        let success_rate = successful_executions / total_executions;
        
        metrics.insert("total_executions".to_string(), total_executions);
        metrics.insert("successful_executions".to_string(), successful_executions);
        metrics.insert("total_profit_usd".to_string(), total_profit);
        metrics.insert("average_profit_usd".to_string(), avg_profit);
        metrics.insert("success_rate".to_string(), success_rate);
        
        // Add custom metrics from the metrics map
        let custom_metrics = self.metrics.read().await;
        for (key, value) in custom_metrics.iter() {
            metrics.insert(key.clone(), *value);
        }
        
        metrics
    }

    async fn record_execution(&self, execution_id: &str, success: bool, profit_usd: f64) {
        let mut executions = self.executions.write().await;
        executions.push(ExecutionRecord {
            execution_id: execution_id.to_string(),
            success,
            profit_usd,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });
        
        // Keep only last 1000 executions to prevent unbounded growth
        if executions.len() > 1000 {
            let drain_count = executions.len() - 1000;
            executions.drain(0..drain_count);
        }
        
        // Update aggregate metrics
        let mut metrics = self.metrics.write().await;
        let total = metrics.get("total_recorded").unwrap_or(&0.0) + 1.0;
        metrics.insert("total_recorded".to_string(), total);
        
        if success {
            let successes = metrics.get("total_successes").unwrap_or(&0.0) + 1.0;
            metrics.insert("total_successes".to_string(), successes);
        }
        
        let cumulative_profit = metrics.get("cumulative_profit_usd").unwrap_or(&0.0) + profit_usd;
        metrics.insert("cumulative_profit_usd".to_string(), cumulative_profit);
    }
}