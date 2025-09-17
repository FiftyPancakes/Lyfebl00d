//! Shared database connection pool management
//!
//! This module provides singleton database pools that can be shared across the application
//! to prevent connection exhaustion issues. Supports both deadpool_postgres and sqlx pools.

use std::sync::Arc;
use once_cell::sync::Lazy;
use deadpool_postgres::{Pool as DeadpoolPool, Config as PgConfig, Runtime};
use tokio_postgres::NoTls;
use sqlx::PgPool;
use crate::config::Config;
use crate::errors::ArbitrageError;

/// Global database pool instances
static DEADPOOL_POOL: Lazy<Arc<std::sync::Mutex<Option<Arc<DeadpoolPool>>>>> = Lazy::new(|| Arc::new(std::sync::Mutex::new(None)));
static SQLX_POOL: Lazy<Arc<std::sync::Mutex<Option<Arc<PgPool>>>>> = Lazy::new(|| Arc::new(std::sync::Mutex::new(None)));

/// Get or create a shared deadpool_postgres database pool
pub async fn get_shared_database_pool(config: &Config) -> Result<Arc<DeadpoolPool>, ArbitrageError> {
    // Check if we already have a pool
    {
        let pool_guard = DEADPOOL_POOL.lock().unwrap();
        if let Some(ref pool) = *pool_guard {
            return Ok(pool.clone());
        }
    }

    // Create a new pool if none exists
    let pool = create_deadpool_database_pool(config).await?;

    // Store it in the global instance
    {
        let mut pool_guard = DEADPOOL_POOL.lock().unwrap();
        *pool_guard = Some(pool.clone());
    }

    Ok(pool)
}

/// Get or create a shared sqlx database pool
pub async fn get_shared_sqlx_pool(config: &Config) -> Result<Arc<PgPool>, ArbitrageError> {
    // Check if we already have a pool
    {
        let pool_guard = SQLX_POOL.lock().unwrap();
        if let Some(ref pool) = *pool_guard {
            return Ok(pool.clone());
        }
    }

    // Create a new pool if none exists
    let pool = create_sqlx_database_pool(config).await?;

    // Store it in the global instance
    {
        let mut pool_guard = SQLX_POOL.lock().unwrap();
        *pool_guard = Some(pool.clone());
    }

    Ok(pool)
}

/// Create a new deadpool_postgres database pool with proper configuration
async fn create_deadpool_database_pool(config: &Config) -> Result<Arc<DeadpoolPool>, ArbitrageError> {
    use deadpool_postgres::PoolConfig;

    let database_url = config.database_url()
        .ok_or_else(|| ArbitrageError::RiskCheckFailed("DATABASE_URL not configured".to_string()))?;

    let mut pg_config = PgConfig::new();

    // Parse the DATABASE_URL properly
    let url = url::Url::parse(&database_url)
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Invalid DATABASE_URL format: {}", e)))?;

    // Validate scheme
    if url.scheme() != "postgres" && url.scheme() != "postgresql" {
        return Err(ArbitrageError::RiskCheckFailed(
            format!("Invalid database scheme: expected 'postgres' or 'postgresql', got '{}'", url.scheme())
        ));
    }

    // Extract connection parameters
    pg_config.host = Some(url.host_str()
        .ok_or_else(|| ArbitrageError::RiskCheckFailed("Missing host in DATABASE_URL".to_string()))?
        .to_string());

    pg_config.port = Some(url.port().unwrap_or(5432));

    pg_config.user = Some(if !url.username().is_empty() {
        url.username().to_string()
    } else {
        "postgres".to_string()
    });

    pg_config.password = url.password().map(|p| p.to_string());

    pg_config.dbname = Some(url.path()
        .trim_start_matches('/')
        .to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| Some("arbitrage".to_string()));

    // Configure pool limits to prevent connection exhaustion
    pg_config.pool = Some(PoolConfig {
        max_size: 20, // Limit concurrent connections
        timeouts: deadpool_postgres::Timeouts {
            create: Some(std::time::Duration::from_secs(30)),
            wait: Some(std::time::Duration::from_secs(30)),
            recycle: Some(std::time::Duration::from_secs(300)),
        },
    });

    // Parse query parameters for additional options
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "sslmode" => {
                // Handle SSL mode if needed
                match value.as_ref() {
                    "disable" | "prefer" | "require" => {
                        // SSL configuration would go here if supported
                    },
                    _ => {}
                }
            },
            "connect_timeout" => {
                if let Ok(timeout) = value.parse::<u64>() {
                    pg_config.connect_timeout = Some(std::time::Duration::from_secs(timeout));
                }
            },
            "pool_size" => {
                if let Ok(size) = value.parse::<usize>() {
                    pg_config.pool = Some(deadpool_postgres::PoolConfig {
                        max_size: size,
                        timeouts: Default::default(),
                    });
                }
            },
            _ => {
                // Ignore unknown parameters
            }
        }
    }

    let pool = pg_config.create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to create database pool: {}", e)))?;

    // Test the connection
    let conn = pool.get().await
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to get database connection: {}", e)))?;

    // Verify schema
    conn.execute("SELECT 1 FROM swaps LIMIT 1", &[]).await
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to verify swaps table: {}", e)))?;

    tracing::info!("Deadpool database pool created successfully with max_size: {}",
        pg_config.pool.as_ref().map(|p| p.max_size).unwrap_or(20));

    Ok(Arc::new(pool))
}

/// Create a new sqlx database pool with proper configuration
async fn create_sqlx_database_pool(config: &Config) -> Result<Arc<PgPool>, ArbitrageError> {
    let database_url = config.database_url()
        .ok_or_else(|| ArbitrageError::RiskCheckFailed("DATABASE_URL not configured".to_string()))?;

    // Create sqlx pool with connection limits
    let pool = PgPool::connect(&database_url).await
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to create sqlx database pool: {}", e)))?;

    // Test the connection
    sqlx::query("SELECT 1 FROM swaps LIMIT 1")
        .execute(&pool)
        .await
        .map_err(|e| ArbitrageError::RiskCheckFailed(format!("Failed to verify swaps table: {}", e)))?;

    tracing::info!("SQLx database pool created successfully");
    Ok(Arc::new(pool))
}

/// Reset the global database pools (useful for testing)
pub fn reset_shared_database_pools() {
    let mut deadpool_guard = DEADPOOL_POOL.lock().unwrap();
    *deadpool_guard = None;

    let mut sqlx_guard = SQLX_POOL.lock().unwrap();
    *sqlx_guard = None;
}

/// Get deadpool database pool statistics
pub async fn get_deadpool_stats() -> Result<deadpool_postgres::Status, ArbitrageError> {
    let pool_guard = DEADPOOL_POOL.lock().unwrap();
    if let Some(ref pool) = *pool_guard {
        Ok(pool.status())
    } else {
        Err(ArbitrageError::RiskCheckFailed("No deadpool database pool available".to_string()))
    }
}

/// Get sqlx database pool statistics (simplified)
pub async fn get_sqlx_pool_stats() -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, ArbitrageError> {
    let pool_guard = SQLX_POOL.lock().unwrap();
    if let Some(ref pool) = *pool_guard {
        // For sqlx, we can try to acquire a connection to test pool health
        match pool.acquire().await {
            Ok(conn) => Ok(conn),
            Err(e) => Err(ArbitrageError::RiskCheckFailed(format!("Failed to acquire sqlx connection: {}", e)))
        }
    } else {
        Err(ArbitrageError::RiskCheckFailed("No sqlx database pool available".to_string()))
    }
}
