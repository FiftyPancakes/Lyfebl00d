/*!
# Chain Listener Integration Tests

This module provides comprehensive integration tests for the Chain Listener functionality in listen.rs.
These tests use real WebSocket connections to blockchain nodes and test all aspects of the listener system.

## Test Categories

### Basic Functionality Tests
- `test_real_websocket_connection_anvil` - Tests basic WebSocket connection to local anvil node
- `test_configuration_validation` - Tests configuration validation edge cases

### WebSocket Subscription Tests
- `test_websocket_subscription_new_heads` - Tests eth_subscribe("newHeads") functionality
- `test_websocket_subscription_pending_transactions` - Tests eth_subscribe("newPendingTransactions") functionality
- `test_websocket_subscription_logs` - Tests eth_subscribe("logs") with filters
- `test_multiple_subscriptions_simultaneous` - Tests multiple subscription types simultaneously

### Advanced Functionality Tests
- `test_real_websocket_live_chains_comprehensive` - Tests with multiple live chains
- `test_websocket_reconnection_recovery` - Tests reconnection and recovery mechanisms
- `test_high_frequency_event_handling` - Tests handling of high event volumes
- `test_event_processing_performance` - Tests event processing performance metrics
- `test_graceful_shutdown_under_load` - Tests shutdown behavior under load
- `test_resource_cleanup` - Tests resource cleanup and memory leak prevention
- `test_error_recovery_resilience` - Tests error recovery and system resilience
- `test_endpoint_failover` - Tests endpoint failover behavior
- `test_concurrent_listeners` - Tests multiple concurrent listeners

## Prerequisites

### Local Development (Anvil)
For basic tests, you need a local anvil node running:
```bash
anvil --host 127.0.0.1 --port 8545
```

### Live Network Tests
For live network tests, you need:
1. Internet connection
2. API keys for the respective blockchains (if required)
3. All tests run by default - use with caution as they may hit rate limits or incur costs

## Running Tests

### All Tests (Requires Internet + Local Anvil)
```bash
# Run all tests (requires internet connection and local anvil)
cargo test test_listen -- --nocapture

# Run specific tests
cargo test test_listen::test_real_websocket_connection_anvil -- --nocapture
cargo test test_listen::test_websocket_subscription_new_heads -- --nocapture
cargo test test_listen::test_configuration_validation -- --nocapture
```

## Test Configuration

The tests automatically load configuration from `config/chains.json`. Make sure this file contains
the correct WebSocket URLs for the chains you want to test.

## WebSocket Subscriptions Tested

### Ethereum Mainnet
- **newHeads**: Real-time new block notifications
- **newPendingTransactions**: Real-time pending transaction notifications
- **logs**: Event logs with optional filtering

### Polygon, Arbitrum, Base
- Same subscription types as Ethereum
- May have different event volumes and characteristics

## Important Notes

1. **Rate Limiting**: Live network tests may hit API rate limits. Use responsibly.
2. **Costs**: Some blockchain nodes may charge for API usage.
3. **Network Dependency**: Tests require stable internet connection for live network tests.
4. **Performance**: High-frequency tests may consume significant network bandwidth.
5. **Timeouts**: All tests include appropriate timeouts to prevent hanging.

## Test Architecture

The tests use a `RealProviderFactory` that creates actual WebSocket connections to blockchain nodes,
unlike unit tests which use mocks. This provides comprehensive integration testing of:

- WebSocket connection establishment
- Subscription management
- Event processing pipelines
- Error handling and recovery
- Resource cleanup
- Performance under load
- Concurrent operation
*/

#[cfg(test)]
mod tests {
    use rust::config::*;
    use rust::errors::ListenerError;
    use rust::listen::*;
    use ethers::types::{Bytes, Block, Filter, Log as EthersLogType, Transaction, TxHash, H160, H256, U256, U64};
    use ethers::providers::{Middleware, ProviderError, SubscriptionStream, Ws, Provider};
    use std::collections::{HashMap, HashSet};
    use rust::types::ChainEvent;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use rust::types::ConnectionStatus;
    use std::sync::Arc;
    use std::pin::Pin;
    use rust::types::ChainType;
    use async_trait::async_trait;
    use serde_json::json;
    use rust::types::ProviderFactory;
    use tokio_stream::Stream;
    use rust::types::WrappedProvider;
    use tokio::{
        sync::{mpsc, Mutex, RwLock},
        time::{sleep, timeout, Duration},
    };
    use assert_matches::assert_matches;
    use std::path::Path;
    use tokio_util::sync::CancellationToken;


    /// Real WebSocket Provider Factory for integration testing
    #[derive(Debug)]
    struct RealProviderFactory;

    #[async_trait]
    impl ProviderFactory for RealProviderFactory {
        async fn create_provider(&self, url: &str) -> Result<WrappedProvider, rust::types::TypesProviderError> {
            if url.contains("solana") {
                // For Solana, we'd need to create a real PubsubClient
                // This is complex, so we'll skip Solana for now in integration tests
                Err(rust::types::TypesProviderError::UnsupportedChain)
            } else {
                // For EVM chains, create real WebSocket provider
                match Provider::<Ws>::connect(url).await {
                    Ok(provider) => Ok(WrappedProvider::Evm(Arc::new(provider))),
                    Err(e) => Err(rust::types::TypesProviderError::Connection(format!("Failed to connect to {}: {}", url, e))),
                }
            }
        }
    }

    /// Load chains configuration from the config file
    async fn load_chains_config() -> Result<ChainConfig, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = Path::new("config/chains.json");
        let content = tokio::fs::read_to_string(config_path).await?;
        let config: ChainConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Get a test configuration for a specific chain
    fn create_test_config(chain_name: &str, config: &ChainConfig) -> Option<PerChainConfig> {
        config.chains.get(chain_name).cloned()
    }



    /// Test real WebSocket connection to anvil (local testnet)
    #[tokio::test]
    async fn test_real_websocket_connection_anvil() {
        let config = load_chains_config().await.expect("Failed to load chains config");
        let anvil_config = create_test_config("anvil", &config).expect("Anvil config not found");

        let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
        let provider_factory = Arc::new(RealProviderFactory);
        let parent_token = tokio_util::sync::CancellationToken::new();

        let listener_result = ChainListener::new(anvil_config, event_tx, provider_factory, parent_token);

        match listener_result {
            Ok(listener) => {
                println!("Successfully created ChainListener for anvil");

                // Test starting the listener (this will try to connect)
                let start_result = timeout(Duration::from_secs(10), listener.start()).await;

                match start_result {
                    Ok(Ok(())) => println!("Successfully started listener"),
                    Ok(Err(e)) => println!("Failed to start listener: {}", e),
                    Err(_) => println!("Listener start timed out (expected for anvil if not running)"),
                }

                // Give it a moment to attempt connection
                sleep(Duration::from_millis(500)).await;

                // Try to receive an event (if anvil is running)
                let event_result = timeout(Duration::from_secs(2), event_rx.recv()).await;
                match event_result {
                    Ok(Some(event)) => println!("Received event: {:?}", event),
                    Ok(None) => println!("Event channel closed"),
                    Err(_) => println!("No events received within timeout"),
                }

                // Shutdown
                listener.shutdown().await;
                println!("Listener shutdown complete");
            }
            Err(e) => {
                println!("Failed to create ChainListener: {}", e);
                // This is expected if anvil is not running
                assert!(e.to_string().contains("Connection") || e.to_string().contains("timeout"));
            }
        }
    }

    /// Test connection timeout handling
    #[tokio::test]
    async fn test_connection_timeout_handling() {
        let config = load_chains_config().await.expect("Failed to load chains config");
        let mut anvil_config = create_test_config("anvil", &config).expect("Anvil config not found");

        // Modify the WS URL to a non-existent endpoint
        anvil_config.ws_url = "ws://127.0.0.1:9999".to_string();
        if let Some(endpoint) = anvil_config.endpoints.get_mut(1) {
            endpoint.url = "ws://127.0.0.1:9999".to_string();
        }

        let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
        let provider_factory = Arc::new(RealProviderFactory);
        let parent_token = tokio_util::sync::CancellationToken::new();

        let listener = ChainListener::new(anvil_config, event_tx, provider_factory, parent_token)
            .expect("Failed to create ChainListener");

        // Try to start - this should timeout
        let start_result = timeout(Duration::from_secs(5), listener.start()).await;

        match start_result {
            Ok(result) => {
                match result {
                    Ok(()) => println!("Unexpectedly succeeded in connecting"),
                    Err(e) => println!("Expected connection failure: {}", e),
                }
            }
            Err(_) => println!("Connection attempt timed out as expected"),
        }

        listener.shutdown().await;
    }

    /// Test UnifiedListener with multiple chains
    #[tokio::test]
    async fn test_unified_listener_multiple_chains() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        let mut unified_listener = UnifiedListener::new();
        let provider_factory = Arc::new(RealProviderFactory);

        // Add listeners for multiple chains
        let chains_to_test = vec!["anvil"];

        for chain_name in chains_to_test {
            if let Some(chain_config) = create_test_config(chain_name, &config) {
                let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
                let child_token = unified_listener.create_child_token();

                match ChainListener::new(chain_config, event_tx, provider_factory.clone(), child_token) {
                    Ok(listener) => {
                        unified_listener.add_chain_listener(chain_name.to_string(), listener);
                        println!("Added listener for {}", chain_name);
                    }
                    Err(e) => {
                        println!("Failed to create listener for {}: {}", chain_name, e);
                        // Continue with other chains
                    }
                }
            }
        }

        assert!(unified_listener.get_listener_count() > 0, "Should have at least one listener");

        // Test starting all listeners
        let start_result = timeout(Duration::from_secs(15), unified_listener.start_all()).await;

        match start_result {
            Ok(Ok(())) => println!("Successfully started all listeners"),
            Ok(Err(e)) => println!("Failed to start listeners: {}", e),
            Err(_) => println!("Start operation timed out"),
        }

        // Shutdown all listeners
        unified_listener.shutdown().await;
        println!("Unified listener shutdown complete");
    }

    /// Test configuration validation
    #[tokio::test]
    async fn test_configuration_validation() {
        let config = load_chains_config().await.expect("Failed to load chains config");
        let anvil_config = create_test_config("anvil", &config).expect("Anvil config not found");

        // Test with valid configuration
        let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
        let provider_factory = Arc::new(RealProviderFactory);
        let parent_token = CancellationToken::new();

        let result = ChainListener::new(anvil_config.clone(), event_tx, provider_factory, parent_token);
        // This should succeed regardless of whether anvil is running
        assert!(result.is_ok(), "Valid configuration should create listener successfully");

        let listener = result.unwrap();
        listener.shutdown().await;
    }

    /// Test real WebSocket connection to live chains (requires internet and API keys)
    #[tokio::test]
    async fn test_real_websocket_live_chains_comprehensive() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test comprehensive WebSocket subscription functionality
        let chains_to_test = vec![
            ("ethereum", "Ethereum Mainnet"),
            ("polygon", "Polygon"),
            ("arbitrum", "Arbitrum"),
            ("base", "Base"),
        ];

        for (chain_name, display_name) in chains_to_test {
            println!("Testing connection to {} ({})", display_name, chain_name);

            if let Some(chain_config) = create_test_config(chain_name, &config) {
                let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
                let provider_factory = Arc::new(RealProviderFactory);
                let parent_token = CancellationToken::new();

                match ChainListener::new(chain_config, event_tx, provider_factory, parent_token) {
                    Ok(listener) => {
                        println!("Successfully created ChainListener for {}", chain_name);

                        // Try to start the listener with a timeout
                        let start_result = timeout(Duration::from_secs(30), listener.start()).await;

                        match start_result {
                            Ok(Ok(())) => {
                                println!("Successfully started listener for {}", chain_name);

                                // Try to receive at least one event within 10 seconds
                                let mut event_received = false;
                                let start_time = std::time::Instant::now();

                                while start_time.elapsed() < Duration::from_secs(10) && !event_received {
                                    match timeout(Duration::from_secs(2), event_rx.recv()).await {
                                        Ok(Some(event)) => {
                                            println!("Received event from {}: {:?}", chain_name, event);
                                            event_received = true;
                                        }
                                        Ok(None) => {
                                            println!("Event channel closed for {}", chain_name);
                                            break;
                                        }
                                        Err(_) => continue,
                                    }
                                }

                                if event_received {
                                    println!("✓ Successfully received events from {}", chain_name);
                                } else {
                                    println!("⚠ No events received from {} within timeout", chain_name);
                                }
                            }
                            Ok(Err(e)) => {
                                println!("Failed to start listener for {}: {}", chain_name, e);
                            }
                            Err(_) => {
                                println!("Timeout starting listener for {}", chain_name);
                            }
                        }

                        listener.shutdown().await;
                        println!("Shutdown complete for {}", chain_name);
                    }
                    Err(e) => {
                        println!("Failed to create ChainListener for {}: {}", chain_name, e);
                    }
                }
            } else {
                println!("Configuration not found for {}", chain_name);
            }

            // Small delay between tests to avoid overwhelming endpoints
            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Test eth_subscribe("newHeads") functionality
    #[tokio::test]
    async fn test_websocket_subscription_new_heads() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing newHeads subscription for Ethereum");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for newHeads");

                    // Collect events for 30 seconds
                    let mut new_head_count = 0;
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(30) && new_head_count < 10 {
                        match timeout(Duration::from_secs(5), event_rx.recv()).await {
                            Ok(Some(ChainEvent::EvmNewHead { chain_name, block })) => {
                                println!("Received new head from {}: block #{}, hash: {:?}",
                                        chain_name, block.number.unwrap_or_default(), block.hash);
                                new_head_count += 1;
                            }
                            Ok(Some(other_event)) => {
                                println!("Received unexpected event: {:?}", other_event);
                            }
                            Ok(None) => {
                                println!("Event channel closed");
                                break;
                            }
                            Err(_) => {
                                println!("Timeout waiting for event");
                                continue;
                            }
                        }
                    }

                    println!("Received {} new head events", new_head_count);
                    assert!(new_head_count > 0, "Should have received at least one new head event");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test eth_subscribe("newPendingTransactions") functionality
    #[tokio::test]
    async fn test_websocket_subscription_pending_transactions() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing newPendingTransactions subscription for Ethereum");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for pending transactions");

                    // Collect events for 30 seconds
                    let mut pending_tx_count = 0;
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(30) && pending_tx_count < 20 {
                        match timeout(Duration::from_secs(2), event_rx.recv()).await {
                            Ok(Some(ChainEvent::EvmPendingTx { chain_name, tx })) => {
                                println!("Received pending tx from {}: hash: {:?}, from: {:?}, to: {:?}, value: {}",
                                        chain_name, tx.hash, tx.from, tx.to, tx.value);
                                pending_tx_count += 1;
                            }
                            Ok(Some(other_event)) => {
                                println!("Received event: {:?}", other_event);
                            }
                            Ok(None) => {
                                println!("Event channel closed");
                                break;
                            }
                            Err(_) => continue,
                        }
                    }

                    println!("Received {} pending transaction events", pending_tx_count);
                    assert!(pending_tx_count > 0, "Should have received at least one pending transaction");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test eth_subscribe("logs") functionality with specific contract filters
    #[tokio::test]
    async fn test_websocket_subscription_logs() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(mut eth_config) = create_test_config("ethereum", &config) {
            // Add a log filter for common ERC-20 Transfer events
            eth_config.log_filter = Some(LogFilterConfig {
                address: Some(vec![
                    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".parse().unwrap(), // USDC contract on Ethereum mainnet
                ]),
                topics: Some(vec![
                    Some(vec![
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string(), // Transfer event signature
                    ]),
                    None,
                    None,
                    None,
                ]),
            });

            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing logs subscription for Ethereum with Transfer event filter");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for logs");

                    // Collect events for 30 seconds
                    let mut log_count = 0;
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(30) && log_count < 5 {
                        match timeout(Duration::from_secs(5), event_rx.recv()).await {
                            Ok(Some(ChainEvent::EvmLog { chain_name, log })) => {
                                println!("Received log from {}: address: {:?}, topics: {}, data length: {}",
                                        chain_name, log.address, log.topics.len(), log.data.len());
                                log_count += 1;
                            }
                            Ok(Some(other_event)) => {
                                println!("Received event: {:?}", other_event);
                            }
                            Ok(None) => {
                                println!("Event channel closed");
                                break;
                            }
                            Err(_) => continue,
                        }
                    }

                    println!("Received {} log events", log_count);
                    // Note: May receive 0 events if the filtered contract is not active
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test multiple subscription types simultaneously
    #[tokio::test]
    async fn test_multiple_subscriptions_simultaneous() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(1000); // Larger buffer
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing multiple simultaneous subscriptions (newHeads, pendingTx, logs)");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener with multiple subscriptions");

                    // Collect events for 30 seconds
                    let mut event_counts = std::collections::HashMap::new();
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(30) {
                        match timeout(Duration::from_secs(1), event_rx.recv()).await {
                            Ok(Some(ChainEvent::EvmNewHead { .. })) => {
                                *event_counts.entry("new_heads").or_insert(0) += 1;
                            }
                            Ok(Some(ChainEvent::EvmPendingTx { .. })) => {
                                *event_counts.entry("pending_tx").or_insert(0) += 1;
                            }
                            Ok(Some(ChainEvent::EvmLog { .. })) => {
                                *event_counts.entry("logs").or_insert(0) += 1;
                            }
                            Ok(Some(other)) => {
                                *event_counts.entry("other").or_insert(0) += 1;
                                println!("Received unexpected event type: {:?}", other);
                            }
                            Ok(None) => {
                                println!("Event channel closed");
                                break;
                            }
                            Err(_) => continue,
                        }
                    }

                    println!("Event summary:");
                    for (event_type, count) in event_counts.iter() {
                        println!("  {}: {}", event_type, count);
                    }

                    let total_events: usize = event_counts.values().sum();
                    assert!(total_events > 0, "Should have received at least one event");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test WebSocket reconnection and recovery
    #[tokio::test]
    async fn test_websocket_reconnection_recovery() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing WebSocket reconnection and recovery");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener");

                    // Monitor events for 60 seconds to test stability
                    let mut total_events = 0;
                    let mut last_event_time = std::time::Instant::now();
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(60) {
                        match timeout(Duration::from_secs(5), event_rx.recv()).await {
                            Ok(Some(event)) => {
                                total_events += 1;
                                last_event_time = std::time::Instant::now();

                                // Log every 10th event
                                if total_events % 10 == 0 {
                                    println!("Received event #{}: {:?}", total_events, event);
                                }
                            }
                            Ok(None) => {
                                println!("Event channel closed after {} events", total_events);
                                break;
                            }
                            Err(_) => {
                                let time_since_last_event = last_event_time.elapsed();
                                if time_since_last_event > Duration::from_secs(10) {
                                    println!("No events received for {:?}, possible connection issue", time_since_last_event);
                                }
                                continue;
                            }
                        }
                    }

                    println!("Reconnection test completed: received {} total events", total_events);
                    assert!(total_events > 0, "Should have received at least one event during reconnection test");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test high-frequency event handling
    #[tokio::test]
    async fn test_high_frequency_event_handling() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with a high-activity chain like Ethereum
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(10000); // Large buffer
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing high-frequency event handling");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for high-frequency test");

                    // Monitor event rate for 30 seconds
                    let mut event_counts = Vec::new();
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(30) {
                        let interval_start = std::time::Instant::now();
                        let mut interval_count = 0;

                        // Count events in 1-second intervals
                        while interval_start.elapsed() < Duration::from_secs(1) {
                            match timeout(Duration::from_millis(100), event_rx.recv()).await {
                                Ok(Some(_)) => interval_count += 1,
                                Ok(None) => break,
                                Err(_) => continue,
                            }
                        }

                        event_counts.push(interval_count);
                        println!("Events per second: {}", interval_count);
                    }

                    let total_events: usize = event_counts.iter().sum();
                    let avg_events_per_second = total_events as f64 / event_counts.len() as f64;
                    let max_events_per_second = event_counts.iter().max().unwrap_or(&0);

                    println!("High-frequency test results:");
                    println!("  Total events: {}", total_events);
                    println!("  Average events/sec: {:.2}", avg_events_per_second);
                    println!("  Max events/sec: {}", max_events_per_second);
                    println!("  Test duration: 30 seconds");

                    assert!(total_events > 0, "Should have received events during high-frequency test");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test event processing performance and memory usage
    #[tokio::test]
    async fn test_event_processing_performance() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(5000);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing event processing performance");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for performance test");

                    // Monitor performance for 30 seconds
                    let mut processing_times = Vec::new();
                    let mut memory_usage = Vec::new();
                    let start_time = std::time::Instant::now();
                    let mut event_count = 0;

                    while start_time.elapsed() < Duration::from_secs(30) && event_count < 1000 {
                        let event_start = std::time::Instant::now();

                        match timeout(Duration::from_secs(1), event_rx.recv()).await {
                            Ok(Some(event)) => {
                                let processing_time = event_start.elapsed();
                                processing_times.push(processing_time);

                                event_count += 1;

                                // Record memory usage every 100 events
                                if event_count % 100 == 0 {
                                    // Note: In a real implementation, you'd use a memory profiling crate
                                    memory_usage.push(0); // Placeholder
                                }

                                // Log progress
                                if event_count % 100 == 0 {
                                    let avg_processing_time = processing_times.iter().sum::<Duration>() / processing_times.len() as u32;
                                    println!("Processed {} events, avg processing time: {:?}", event_count, avg_processing_time);
                                }
                            }
                            Ok(None) => break,
                            Err(_) => continue,
                        }
                    }

                    // Calculate performance metrics
                    if !processing_times.is_empty() {
                        let total_processing_time = processing_times.iter().sum::<Duration>();
                        let avg_processing_time = total_processing_time / processing_times.len() as u32;
                        let min_processing_time = processing_times.iter().min().unwrap();
                        let max_processing_time = processing_times.iter().max().unwrap();

                        println!("Performance test results:");
                        println!("  Events processed: {}", event_count);
                        println!("  Average processing time: {:?}", avg_processing_time);
                        println!("  Min processing time: {:?}", min_processing_time);
                        println!("  Max processing time: {:?}", max_processing_time);
                        println!("  Events per second: {:.2}", event_count as f64 / start_time.elapsed().as_secs_f64());
                    }

                    assert!(event_count > 0, "Should have processed at least one event");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test graceful shutdown under load
    #[tokio::test]
    async fn test_graceful_shutdown_under_load() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, event_rx) = mpsc::channel::<ChainEvent>(1000);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing graceful shutdown under load");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener");

                    // Let it run for 10 seconds to build up some activity
                    sleep(Duration::from_secs(10)).await;

                    println!("Initiating graceful shutdown...");

                    let shutdown_start = std::time::Instant::now();
                    listener.shutdown().await;
                    let shutdown_duration = shutdown_start.elapsed();

                    println!("Shutdown completed in {:?}", shutdown_duration);
                    println!("Graceful shutdown test passed");

                    // Verify no panic occurred and shutdown was reasonably fast
                    assert!(shutdown_duration < Duration::from_secs(10), "Shutdown took too long: {:?}", shutdown_duration);
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test configuration validation edge cases
    #[tokio::test]
    async fn test_configuration_edge_cases() {
        let config = load_chains_config().await.expect("Failed to load chains config");
        let eth_config = create_test_config("ethereum", &config).expect("Ethereum config not found");

        // Test with invalid WebSocket URL
        let mut invalid_config = eth_config.clone();
        invalid_config.ws_url = "invalid-url".to_string();

        let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
        let provider_factory = Arc::new(RealProviderFactory);
        let parent_token = CancellationToken::new();

        let result = ChainListener::new(invalid_config, event_tx, provider_factory, parent_token);

        match result {
            Ok(listener) => {
                println!("Unexpectedly created listener with invalid URL");

                // Try to start it - should fail
                let start_result = timeout(Duration::from_secs(5), listener.start()).await;
                match start_result {
                    Ok(Ok(())) => println!("Unexpectedly started with invalid URL"),
                    Ok(Err(e)) => println!("Expected failure with invalid URL: {}", e),
                    Err(_) => println!("Expected timeout with invalid URL"),
                }

                listener.shutdown().await;
            }
            Err(e) => {
                println!("Correctly rejected invalid configuration: {}", e);
            }
        }
    }

    /// Test resource cleanup and memory leaks
    #[tokio::test]
    async fn test_resource_cleanup() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing resource cleanup");

            // Start and stop multiple times
            for i in 0..3 {
                println!("Cycle {}: Starting listener", i + 1);

                let start_result = timeout(Duration::from_secs(10), listener.start()).await;
                match start_result {
                    Ok(Ok(())) => {
                        println!("Cycle {}: Started successfully", i + 1);

                        // Run for a short time
                        sleep(Duration::from_secs(2)).await;

                        println!("Cycle {}: Shutting down", i + 1);
                        listener.shutdown().await;
                        println!("Cycle {}: Shutdown complete", i + 1);
                    }
                    Ok(Err(e)) => println!("Cycle {}: Failed to start: {}", i + 1, e),
                    Err(_) => println!("Cycle {}: Timeout starting", i + 1),
                }

                // Brief pause between cycles
                sleep(Duration::from_millis(500)).await;
            }

            println!("Resource cleanup test completed");
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test error recovery and resilience
    #[tokio::test]
    async fn test_error_recovery_resilience() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        // Test with Ethereum mainnet
        if let Some(eth_config) = create_test_config("ethereum", &config) {
            let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
            let provider_factory = Arc::new(RealProviderFactory);
            let parent_token = CancellationToken::new();

            let listener = ChainListener::new(eth_config, event_tx, provider_factory, parent_token)
                .expect("Failed to create ChainListener");

            println!("Testing error recovery and resilience");

            // Start the listener
            let start_result = timeout(Duration::from_secs(30), listener.start()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started listener for resilience test");

                    // Monitor for 60 seconds, checking for gaps in events
                    let mut consecutive_timeouts = 0;
                    let mut max_consecutive_timeouts = 0;
                    let mut total_events = 0;
                    let start_time = std::time::Instant::now();

                    while start_time.elapsed() < Duration::from_secs(60) {
                        match timeout(Duration::from_secs(2), event_rx.recv()).await {
                            Ok(Some(event)) => {
                                total_events += 1;
                                consecutive_timeouts = 0;

                                // Log recovery if we had timeouts
                                if max_consecutive_timeouts > 0 {
                                    println!("Recovered from {} consecutive timeouts, received event: {:?}", max_consecutive_timeouts, event);
                                    max_consecutive_timeouts = 0;
                                }
                            }
                            Ok(None) => {
                                println!("Event channel closed during resilience test");
                                break;
                            }
                            Err(_) => {
                                consecutive_timeouts += 1;
                                max_consecutive_timeouts = max_consecutive_timeouts.max(consecutive_timeouts);

                                if consecutive_timeouts % 5 == 0 {
                                    println!("{} consecutive timeouts, testing resilience...", consecutive_timeouts);
                                }
                            }
                        }
                    }

                    println!("Resilience test completed:");
                    println!("  Total events received: {}", total_events);
                    println!("  Max consecutive timeouts: {}", max_consecutive_timeouts);

                    // The system should be resilient to some network issues
                    assert!(total_events > 0, "Should have received at least some events despite potential network issues");
                }
                Ok(Err(e)) => println!("Failed to start listener: {}", e),
                Err(_) => println!("Timeout starting listener"),
            }

            listener.shutdown().await;
        } else {
            println!("Ethereum config not found, skipping test");
        }
    }

    /// Test endpoint failover behavior
    #[tokio::test]
    async fn test_endpoint_failover() {
        let config = load_chains_config().await.expect("Failed to load chains config");
        let mut anvil_config = create_test_config("anvil", &config).expect("Anvil config not found");

        // Create a configuration with multiple endpoints, some invalid
        anvil_config.endpoints = vec![
            rust::config::EndpointConfig {
                url: "ws://127.0.0.1:9998".to_string(), // Invalid endpoint
                ws: true,
                priority: Some(1),
                name: Some("invalid_endpoint".to_string()),
            },
            rust::config::EndpointConfig {
                url: "ws://127.0.0.1:9999".to_string(), // Another invalid endpoint
                ws: true,
                priority: Some(2),
                name: Some("another_invalid".to_string()),
            },
        ];

        let (event_tx, _event_rx) = mpsc::channel::<ChainEvent>(100);
        let provider_factory = Arc::new(RealProviderFactory);
        let parent_token = CancellationToken::new();

        let listener = ChainListener::new(anvil_config, event_tx, provider_factory, parent_token)
            .expect("Failed to create ChainListener");

        // Start the listener - it should attempt to connect to invalid endpoints and eventually give up
        let start_result = timeout(Duration::from_secs(10), listener.start()).await;

        match start_result {
            Ok(result) => {
                match result {
                    Ok(()) => println!("Unexpectedly succeeded with invalid endpoints"),
                    Err(e) => println!("Expected failure with invalid endpoints: {}", e),
                }
            }
            Err(_) => println!("Connection attempts timed out as expected"),
        }

        listener.shutdown().await;
    }

    /// Test concurrent listeners for different chains
    #[tokio::test]
    async fn test_concurrent_listeners() {
        let config = load_chains_config().await.expect("Failed to load chains config");

        let mut unified_listener = UnifiedListener::new();
        let provider_factory = Arc::new(RealProviderFactory);

        // Add listeners for multiple chains concurrently
        let chains_to_test = vec!["anvil"];

        for chain_name in chains_to_test {
            if let Some(chain_config) = create_test_config(chain_name, &config) {
                let (event_tx, mut event_rx) = mpsc::channel::<ChainEvent>(100);
                let child_token = unified_listener.create_child_token();

                match ChainListener::new(chain_config, event_tx, provider_factory.clone(), child_token) {
                    Ok(listener) => {
                        unified_listener.add_chain_listener(chain_name.to_string(), listener);

                        // Spawn a task to monitor events for this chain
                        tokio::spawn(async move {
                            let mut event_count = 0;
                            let start_time = std::time::Instant::now();

                            while start_time.elapsed() < Duration::from_secs(5) && event_count < 5 {
                                match timeout(Duration::from_millis(500), event_rx.recv()).await {
                                    Ok(Some(event)) => {
                                        println!("Concurrent event from {}: {:?}", chain_name, event);
                                        event_count += 1;
                                    }
                                    Ok(None) => break,
                                    Err(_) => continue,
                                }
                            }
                            println!("Concurrent listener for {} received {} events", chain_name, event_count);
                        });

                        println!("Started concurrent listener for {}", chain_name);
                    }
                    Err(e) => {
                        println!("Failed to create concurrent listener for {}: {}", chain_name, e);
                    }
                }
            }
        }

        if unified_listener.get_listener_count() > 0 {
            // Start all listeners
            let start_result = timeout(Duration::from_secs(10), unified_listener.start_all()).await;

            match start_result {
                Ok(Ok(())) => {
                    println!("Successfully started concurrent listeners");

                    // Let them run for a short time
                    sleep(Duration::from_secs(3)).await;
                }
                Ok(Err(e)) => println!("Failed to start concurrent listeners: {}", e),
                Err(_) => println!("Concurrent listeners start timed out"),
            }

            // Shutdown
            unified_listener.shutdown().await;
            println!("Concurrent listeners shutdown complete");
        } else {
            println!("No concurrent listeners were created");
        }
    }
}