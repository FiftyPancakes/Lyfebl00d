// src/state_manager.rs

//! # State Persistence Service
//!
//! This module provides a robust and asynchronous service for persisting the bot's
//! operational state, including executed trades and performance metrics, to a
//! PostgreSQL database.
//!
//! ## Core Design
//!
//! - **Asynchronous Writes:** All database operations are performed in background
//!   tasks, ensuring that the critical trading path is never blocked by database latency.
//! - **Connection Pooling:** Utilizes `deadpool-postgres` for efficient and resilient
//!   management of database connections.
//! - **Focused Responsibility:** This manager is solely responsible for writing data.
//!   All complex data analysis, time-series queries, and market regime detection
//!   belong in a separate analytics layer, not in the core state persistence service.

use crate::{
    config::StateDbConfig,
    errors::StateError,
    types::{ExecutionStatus, TradeRecord, PerformanceMetrics},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Config as PgConfig, Pool};
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, instrument, warn};

//================================================================================================//
//                                      CORE DATA STRUCTURES                                      //
//================================================================================================//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub db: Option<StateDbConfig>,
    #[serde(default = "default_true")]
    pub persist_trades: bool,
}
fn default_true() -> bool {
    true
}

/// A simple cancellation token for graceful shutdown, included to match the shell.
/// In this implementation, it's not actively used as there's no long-running worker task.
#[derive(Clone)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    #[allow(dead_code)]
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    #[allow(dead_code)]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

//================================================================================================//
//                                       STATE MANAGER                                            //
//================================================================================================//

pub struct StateManager {
    config: Arc<StateConfig>,
    db_pool: Option<Pool>,
    #[allow(dead_code)]
    cancellation_token: CancellationToken,
}

impl StateManager {
    #[instrument(name = "state_manager::new", skip(config))]
    pub async fn new(config: StateConfig) -> Result<Self, StateError> {
        info!(target: "state_manager", "Initializing StateManager.");
        let db_pool = if let Some(db_config) = &config.db {
            if config.persist_trades {
                match Self::create_db_pool(db_config).await {
                    Ok(pool) => match pool.get().await {
                        Ok(client) => {
                            if let Err(e) = Self::initialize_database(&client).await {
                                error!(target: "state_manager", error = %e, "Failed to initialize database schema. Persistence will be disabled.");
                                None
                            } else {
                                info!(target: "state_manager::db", "Database schema verified/initialized successfully.");
                                Some(pool)
                            }
                        }
                        Err(e) => {
                            error!(target: "state_manager", error = %e, "Failed to get database connection for initialization. Persistence will be disabled.");
                            None
                        }
                    },
                    Err(e) => {
                        error!(target: "state_manager", error = %e, "Failed to create database pool. Persistence will be disabled.");
                        None
                    }
                }
            } else {
                info!(target: "state_manager", "Trade persistence is disabled via configuration.");
                None
            }
        } else {
            info!(target: "state_manager", "No database configuration provided. Persistence is disabled.");
            None
        };

        Ok(Self {
            config: Arc::new(config),
            db_pool,
            cancellation_token: CancellationToken::new(),
        })
    }

    /// Creates and tests a new database connection pool.
    async fn create_db_pool(db_config: &StateDbConfig) -> Result<Pool, StateError> {
        let mut pg_config = PgConfig::new();
        pg_config.host = Some(db_config.host.clone());
        pg_config.port = Some(db_config.port);
        pg_config.user = Some(db_config.user.clone());
        pg_config.password = Some(db_config.password.clone());
        pg_config.dbname = Some(db_config.dbname.clone());
        
        let pool = pg_config.create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
            .map_err(|e| StateError::Config(format!("Database pool creation failed: {}", e)))?;
        
        // Test connection on creation to fail fast
        let conn = pool.get().await
            .map_err(|e| StateError::Pool(format!("Failed to get connection from pool: {}", e)))?;
        drop(conn);
        info!(target: "state_manager::db", "Database connection pool created successfully.");
        Ok(pool)
    }

    /// Initializes the required database schema if it doesn't exist.
    async fn initialize_database(client: &Client) -> Result<(), StateError> {
        client
            .batch_execute(
                r#"
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'trade_status') THEN
                    CREATE TYPE trade_status AS ENUM ('Pending', 'Confirmed', 'Failed', 'Dropped', 'Replaced');
                END IF;
            END
            $$;

            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                tx_hash BYTEA UNIQUE,
                status trade_status NOT NULL,
                profit_usd DOUBLE PRECISION NOT NULL,
                token_in BYTEA NOT NULL,
                token_out BYTEA NOT NULL,
                amount_in NUMERIC(78, 0) NOT NULL,
                amount_out NUMERIC(78, 0) NOT NULL,
                gas_used NUMERIC(78, 0),
                gas_price NUMERIC(78, 0),
                block_number BIGINT,
                details JSONB,
                timestamp TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_profit ON trades (profit_usd DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
            "#,
            )
            .await
            .map_err(|e| StateError::Query(format!("Failed to save state: {}", e)))?;
        Ok(())
    }

    /// Asynchronously records a trade result to the database without blocking the caller.
    #[instrument(skip(self, trade), fields(trade_id = %trade.trade_id, status = ?trade.status, profit_usd = %trade.profit_usd))]
    pub fn record_trade(&self, trade: TradeRecord) {
        if !self.config.persist_trades {
            return;
        }

        let pool = match &self.db_pool {
            Some(p) => p.clone(),
            None => {
                warn!(target: "state_manager", trade_id = %trade.trade_id, "Attempted to record trade, but persistence is disabled (no db pool).");
                return;
            }
        };

        tokio::spawn(async move {
            if let Err(e) = Self::persist_trade_record(&pool, &trade).await {
                error!(target: "state_manager::db", error = %e, trade_id = %trade.trade_id, "Failed to persist trade record.");
            }
        });
    }

    /// The internal function that performs the database write.
    async fn persist_trade_record(pool: &Pool, trade: &TradeRecord) -> Result<(), StateError> {
        let client = pool.get().await
            .map_err(|e| StateError::Pool(format!("Failed to get connection: {}", e)))?;
        let status_str = match trade.status {
            ExecutionStatus::Pending => "pending",
            ExecutionStatus::Confirmed => "confirmed",
            ExecutionStatus::Failed => "failed",
            ExecutionStatus::Dropped => "dropped",
            ExecutionStatus::Replaced => "replaced",
            ExecutionStatus::Submitted => "submitted",
        };

        // Convert BigDecimal values to strings for database storage
        let amount_in_str = trade.amount_in.to_string();
        let amount_out_str = trade.amount_out.to_string();
        let gas_used_str = trade.gas_used.as_ref().map(|g| g.to_string());
        let gas_price_str = trade.gas_price.as_ref().map(|p| p.to_string());

        client.execute(
            "INSERT INTO trades (
                trade_id, tx_hash, status, profit_usd, token_in, token_out, 
                amount_in, amount_out, gas_used, gas_price, block_number, details, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &trade.trade_id,
                &trade.tx_hash.as_bytes(),
                &status_str,
                &trade.profit_usd,
                &trade.token_in.as_bytes(),
                &trade.token_out.as_bytes(),
                &amount_in_str,
                &amount_out_str,
                &gas_used_str,
                &gas_price_str,
                &(trade.block_number.map(|b| b as i64)),
                &trade.details.as_ref().map(|d| serde_json::to_string(d).unwrap_or_default()),
                &std::time::SystemTime::from(trade.timestamp),
            ],
        ).await
            .map_err(|e| StateError::Query(format!("Failed to query state: {}", e)))?;
        debug!(target: "state_manager::db", trade_id = %trade.trade_id, "Trade record persisted.");
        Ok(())
    }

    /// Gets performance metrics for the chatbot operator
    pub async fn get_metrics(&self) -> PerformanceMetrics {
        if let Some(pool) = &self.db_pool {
            match self.fetch_metrics_from_db(pool).await {
                Ok(metrics) => metrics,
                Err(e) => {
                    warn!(target: "state_manager", error = %e, "Failed to fetch metrics from database, returning defaults");
                    PerformanceMetrics::new()
                }
            }
        } else {
            PerformanceMetrics::new()
        }
    }

    /// Fetches recent trade records for the chatbot operator
    pub async fn get_recent_trades(&self, limit: usize) -> Vec<TradeRecord> {
        if let Some(pool) = &self.db_pool {
            match self.fetch_recent_trades_from_db(pool, limit).await {
                Ok(trades) => trades,
                Err(e) => {
                    warn!(target: "state_manager", error = %e, "Failed to fetch recent trades from database");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    }

    /// Internal method to fetch metrics from database
    async fn fetch_metrics_from_db(&self, pool: &Pool) -> Result<PerformanceMetrics, StateError> {
        let client = pool.get().await
            .map_err(|e| StateError::Pool(format!("Failed to get connection: {}", e)))?;
        let row = client.query_one(
            "SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) as successful_trades,
                SUM(CASE WHEN status = 'failed' OR status = 'dropped' THEN 1 ELSE 0 END) as failed_trades,
                COALESCE(SUM(CASE WHEN status = 'confirmed' THEN profit_usd ELSE 0 END), 0) as total_profit,
                COALESCE(AVG(CASE WHEN status = 'confirmed' THEN profit_usd END), 0) as avg_profit_per_trade,
                COALESCE(AVG(CASE WHEN status = 'confirmed' AND gas_used IS NOT NULL AND gas_price IS NOT NULL 
                    THEN (gas_used::numeric * gas_price::numeric) / 1e18 END), 0) as avg_gas_cost_usd
            FROM trades 
            WHERE timestamp >= NOW() - INTERVAL '24 hours'",
            &[],
        ).await
            .map_err(|e| StateError::Query(format!("Failed to query state: {}", e)))?;

        let total_trades: i64 = row.get(0);
        let successful_trades: i64 = row.get(1);
        let failed_trades: i64 = row.get(2);
        let total_profit_str: String = row.get(3);
        let avg_profit_per_trade_str: String = row.get(4);
        let avg_gas_cost_usd_str: String = row.get(5);

        // Parse strings back to f64
        let total_profit: f64 = total_profit_str.parse().unwrap_or(0.0);
        let avg_profit_per_trade: f64 = avg_profit_per_trade_str.parse().unwrap_or(0.0);
        let avg_gas_cost_usd: f64 = avg_gas_cost_usd_str.parse().unwrap_or(0.0);

        let success_rate = if total_trades > 0 {
            successful_trades as f64 / total_trades as f64
        } else {
            0.0
        };

        Ok(PerformanceMetrics {
            opportunities_processed: total_trades as u64,
            opportunities_executed: successful_trades as u64,
            total_profit_usd: total_profit,
            avg_execution_time_ms: 0.0, // Would need additional tracking for this
            cache_hit_rate: 0.0, // Would need additional tracking for this
            circuit_breaker_trips: 0, // Would need additional tracking for this
            last_updated: Utc::now(),
            avg_profit_per_trade,
            avg_gas_cost_usd,
            success_rate,
            successful_trades: successful_trades as u64,
            failed_trades: failed_trades as u64,
        })
    }

    /// Internal method to fetch recent trades from database
    async fn fetch_recent_trades_from_db(&self, pool: &Pool, limit: usize) -> Result<Vec<TradeRecord>, StateError> {
        let client = pool.get().await
            .map_err(|e| StateError::Pool(format!("Failed to get connection: {}", e)))?;
        let rows = client.query(
            "SELECT trade_id, tx_hash, status, profit_usd, token_in, token_out, 
                    amount_in, amount_out, gas_used, gas_price, block_number, details, timestamp
             FROM trades 
             ORDER BY timestamp DESC 
             LIMIT $1",
            &[&(limit as i64)],
        ).await
            .map_err(|e| StateError::Query(format!("Failed to query state: {}", e)))?;

        let mut trades = Vec::new();
        for row in rows {
            let trade_id: String = row.get(0);
            let tx_hash_bytes: Vec<u8> = row.get(1);
            let status_str: String = row.get(2);
            let profit_usd: f64 = row.get(3);
            let token_in_bytes: Vec<u8> = row.get(4);
            let token_out_bytes: Vec<u8> = row.get(5);
            let amount_in_str: String = row.get(6);
            let amount_out_str: String = row.get(7);
            let gas_used_opt: Option<String> = row.get(8);
            let gas_price_opt: Option<String> = row.get(9);
            let block_number_opt: Option<i64> = row.get(10);
            let details_str: Option<String> = row.get(11);
            let timestamp_system: std::time::SystemTime = row.get(12);
            let details = details_str.and_then(|s| serde_json::from_str(&s).ok());
            let timestamp = DateTime::from(timestamp_system);

            let status = match status_str.as_str() {
                "pending" => ExecutionStatus::Pending,
                "confirmed" => ExecutionStatus::Confirmed,
                "failed" => ExecutionStatus::Failed,
                "dropped" => ExecutionStatus::Dropped,
                "replaced" => ExecutionStatus::Replaced,
                _ => ExecutionStatus::Failed,
            };

            // Parse strings back to U256
            let amount_in = U256::from_dec_str(&amount_in_str).unwrap_or_default();
            let amount_out = U256::from_dec_str(&amount_out_str).unwrap_or_default();
            let gas_used = gas_used_opt.and_then(|s| U256::from_dec_str(&s).ok());
            let gas_price = gas_price_opt.and_then(|s| U256::from_dec_str(&s).ok());

            // Convert bytes back to Address
            let tx_hash = if tx_hash_bytes.len() == 32 {
                ethers::types::H256::from_slice(&tx_hash_bytes)
            } else {
                ethers::types::H256::zero()
            };

            let token_in = if token_in_bytes.len() == 20 {
                Address::from_slice(&token_in_bytes)
            } else {
                Address::zero()
            };

            let token_out = if token_out_bytes.len() == 20 {
                Address::from_slice(&token_out_bytes)
            } else {
                Address::zero()
            };

            trades.push(TradeRecord {
                trade_id,
                tx_hash,
                status,
                profit_usd,
                token_in,
                token_out,
                amount_in,
                amount_out,
                gas_used,
                gas_price,
                block_number: block_number_opt.map(|b| b as u64),
                details,
                timestamp,
            });
        }

        Ok(trades)
    }
}

impl std::fmt::Debug for StateManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateManager")
            .field("config", &"Arc<StateConfig>")
            .field("db_pool", &self.db_pool.is_some())
            .field("cancellation_token", &"CancellationToken")
            .finish()
    }
}