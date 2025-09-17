// src/providers.rs

//! # Centralized Provider Management
//!
//! This module provides centralized provider access with rate limiting, concurrency control,
//! and health management. All RPC calls should go through this manager to ensure
//! proper resource management and rate limiting.

use crate::types::{TypesProviderError, ProviderFactory, WrappedProvider};
use crate::rate_limiter::{ChainRateLimiter, initialize_global_rate_limiter_manager};
use crate::config::{Config, ChainSettings};
use crate::errors::BlockchainError;
use async_trait::async_trait;
use ethers::providers::{Provider, Ws};

use solana_client::nonblocking::pubsub_client::PubsubClient;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};
use tracing::{info, warn};
use governor::DefaultDirectRateLimiter;

/// A production-grade provider factory that creates real websocket connections.
#[derive(Debug, Default, Clone)]
pub struct ProductionProviderFactory;

impl ProductionProviderFactory {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ProviderFactory for ProductionProviderFactory {
    /// Creates a new provider instance by connecting to the given websocket URL.
    ///
    /// It intelligently determines whether to create an EVM or a Solana provider
    /// based on the content of the URL string.
    async fn create_provider(&self, url: &str) -> Result<WrappedProvider, TypesProviderError> {
        if url.contains("solana") {
            let client = PubsubClient::new(url)
                .await
                .map_err(|e| TypesProviderError::Connection(e.to_string()))?;
            Ok(WrappedProvider::Solana(Arc::new(client)))
        } else {
            let provider = Provider::<Ws>::connect(url)
                .await
                .map_err(|e| TypesProviderError::Connection(e.to_string()))?;
            Ok(WrappedProvider::Evm(Arc::new(provider)))
        }
    }
}

/// Centralized provider manager with rate limiting and concurrency control
#[derive(Debug)]
pub struct CentralizedProviderManager {
    /// Global rate limiter shared across all chains
    global_limiter: Arc<DefaultDirectRateLimiter>,
    /// Per-chain rate limiters and providers
    chain_managers: DashMap<String, ChainProviderManager>,
    /// Configuration
    config: Arc<Config>,
    /// Global concurrency limiter for all RPC calls
    global_concurrency_limiter: Arc<Semaphore>,
}

#[derive(Debug)]
struct ChainProviderManager {
    /// Chain-specific rate limiter
    rate_limiter: Arc<ChainRateLimiter>,
    /// EVM providers (with health tracking)
    evm_providers: Vec<ProviderHealthWrapper>,
    /// Current provider index for round-robin
    current_provider_idx: RwLock<usize>,
}

#[derive(Debug)]
struct ProviderHealthWrapper {
    provider: Arc<Provider<Ws>>,
    url: String,
    health_score: RwLock<f64>, // 0.0 to 1.0, higher is better
    last_failure: RwLock<Option<std::time::Instant>>,
    consecutive_failures: RwLock<u32>,
}

impl CentralizedProviderManager {
    /// Create a new centralized provider manager
    pub async fn new(config: Arc<Config>) -> Result<Self, BlockchainError> {
        // Initialize global rate limiter
        let mut chain_settings = ChainSettings::default();
        chain_settings.global_rps_limit = config.chain_config.rate_limiter_settings.global_rps_limit;
        chain_settings.default_chain_rps_limit = config.chain_config.rate_limiter_settings.default_chain_rps_limit;
        let global_rate_limiter_manager = initialize_global_rate_limiter_manager(Arc::new(chain_settings));
        let global_limiter = global_rate_limiter_manager.get_global_limiter();

        // Create global concurrency limiter (limit total concurrent RPC calls across all chains)
        let global_concurrency = 100; // Default value for now
        let global_concurrency_limiter = Arc::new(Semaphore::new(global_concurrency as usize));

        info!(
            global_concurrency = global_concurrency,
            "Initialized centralized provider manager"
        );

        Ok(Self {
            global_limiter,
            chain_managers: DashMap::new(),
            config,
            global_concurrency_limiter,
        })
    }

    /// Initialize providers for a specific chain
    pub async fn initialize_chain(&self, chain_name: &str) -> Result<(), BlockchainError> {
        let chain_config = self.config.chain_config.chains.get(chain_name)
            .ok_or_else(|| BlockchainError::ConfigurationError(format!("Chain {} not found", chain_name)))?;

        let chain_settings = Arc::new(ChainSettings::from(chain_config));

        // Create chain rate limiter
        let rate_limiter = Arc::new(ChainRateLimiter::new(
            chain_name,
            100.into(), // Default RPS limit
            10.into(), // Default max concurrent requests
            self.global_limiter.clone(),
            chain_settings,
        ));

        // Create EVM providers
        let mut evm_providers = Vec::new();
        for endpoint in &chain_config.endpoints {
            // Use the URL field from EndpointConfig for connection
            let ws_url = endpoint.url.clone();
            match Provider::<Ws>::connect(&ws_url).await {
                Ok(provider) => {
                    evm_providers.push(ProviderHealthWrapper {
                        provider: Arc::new(provider),
                        url: ws_url.clone(),
                        health_score: RwLock::new(1.0),
                        last_failure: RwLock::new(None),
                        consecutive_failures: RwLock::new(0),
                    });
                    info!("Connected to {} provider: {:?}", chain_name, ws_url);
                }
                Err(e) => {
                    warn!("Failed to connect to {} provider {:?}: {}", chain_name, ws_url, e);
                }
            }
        }

        if evm_providers.is_empty() {
            return Err(BlockchainError::ConfigurationError(format!(
                "No working providers found for chain {}", chain_name
            )));
        }

        let provider_count = evm_providers.len();
        let chain_manager = ChainProviderManager {
            rate_limiter,
            evm_providers,
            current_provider_idx: RwLock::new(0),
        };

        self.chain_managers.insert(chain_name.to_string(), chain_manager);
        info!("Initialized chain {} with {} providers", chain_name, provider_count);

        Ok(())
    }

    /// Execute an RPC call with centralized rate limiting and concurrency control
    pub async fn execute_rpc_call<F, Fut, T>(
        &self,
        chain_name: &str,
        method_name: &str,
        call_fn: F,
    ) -> Result<T, BlockchainError>
    where
        F: Fn(Arc<Provider<Ws>>) -> Fut,
        Fut: std::future::Future<Output = Result<T, BlockchainError>>,
    {
        let chain_manager = self.chain_managers.get(chain_name)
            .ok_or_else(|| BlockchainError::ConfigurationError(format!("Chain {} not initialized", chain_name)))?;

        // Acquire global concurrency permit
        let _global_permit = self.global_concurrency_limiter.acquire().await
            .map_err(|_| BlockchainError::RateLimitError("Global concurrency limiter closed".to_string()))?;

        // Use chain rate limiter
        chain_manager.rate_limiter.execute_rpc_call(method_name, || async {
            // Get next healthy provider (round-robin with health awareness)
            let provider = self.get_healthy_provider(chain_name).await?;

            // Execute the actual call
            call_fn(provider).await
        }).await
    }

    /// Get a healthy provider for the chain using round-robin with health weighting
    async fn get_healthy_provider(&self, chain_name: &str) -> Result<Arc<Provider<Ws>>, BlockchainError> {
        let chain_manager = self.chain_managers.get(chain_name)
            .ok_or_else(|| BlockchainError::ConfigurationError(format!("Chain {} not found", chain_name)))?;

        let mut current_idx = chain_manager.current_provider_idx.write().await;
        let mut attempts = 0;
        let max_attempts = chain_manager.evm_providers.len();

        while attempts < max_attempts {
            let provider = &chain_manager.evm_providers[*current_idx];
            let health_score = *provider.health_score.read().await;

            // Skip unhealthy providers
            if health_score > 0.3 {
                // Update index for next call (round-robin)
                drop(current_idx);
                let mut idx_write = chain_manager.current_provider_idx.write().await;
                *idx_write = (*idx_write + 1) % chain_manager.evm_providers.len();
                drop(idx_write);

                return Ok(provider.provider.clone());
            }

            // Try next provider
            *current_idx = (*current_idx + 1) % chain_manager.evm_providers.len();
            attempts += 1;
        }

        Err(BlockchainError::NotAvailable(format!("No healthy providers available for chain {}", chain_name)))
    }

    /// Update provider health based on call success/failure
    pub async fn update_provider_health(&self, chain_name: &str, provider_url: &str, success: bool) {
        if let Some(chain_manager) = self.chain_managers.get(chain_name) {
            for provider in &chain_manager.evm_providers {
                if provider.url == provider_url {
                    let mut health_score = provider.health_score.write().await;
                    let mut consecutive_failures = provider.consecutive_failures.write().await;
                    let mut last_failure = provider.last_failure.write().await;

                    if success {
                        // Increase health score on success
                        *health_score = (*health_score + 0.1).min(1.0);
                        *consecutive_failures = 0;
                    } else {
                        // Decrease health score on failure
                        *health_score = (*health_score - 0.2).max(0.0);
                        *consecutive_failures += 1;
                        *last_failure = Some(std::time::Instant::now());
                    }
                    break;
                }
            }
        }
    }
} 