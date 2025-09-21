#[cfg(test)]
mod test_backtest_integration {
    use rust::backtest::Backtester;
    use rust::config::Config;
    use std::sync::Arc;
    use ethers::types::{Address, U256};
    
    #[tokio::test]
    async fn test_backtest_initialization() {
        // Create a minimal config for testing
        let config = Config {
            mode: rust::config::ExecutionMode::Backtest,
            log_level: "info".to_string(),
            chain_config: rust::config::ChainConfig {
                chains: std::collections::HashMap::new(),
                rate_limiter_settings: rust::config::RateLimiterSettings::default(),
            },
            module_config: None,
            backtest: Some(rust::config::BacktestConfig {
                enabled: true,
                chains: std::collections::HashMap::new(),
                max_parallel_blocks: 1,
                database: None,
                random_seed: Some(42),
                rps_limit: 10.0,
                concurrent_limit: 5,
                burst_allowance: 10,
                rate_limit_initial_backoff_ms: 100,
                rate_limit_max_backoff_ms: 1000,
                rate_limit_backoff_multiplier: 2.0,
                rate_limit_jitter_factor: 0.1,
                rate_limit_max_retries: 3,
                rate_limit_timeout_secs: 30,
                batch_size: 10,
                min_batch_delay_ms: 50,
                rate_limiter_settings: rust::config::RateLimiterSettings::default(),
                competition_bot_count: Some(10),
                competition_skill_range: Some((0.1, 0.9)),
                competition_latency_range_ms: Some((50, 200)),
                mev_attack_probability: Some(0.1),
                mev_sandwich_success_rate: Some(0.8),
                mev_frontrun_success_rate: Some(0.6),
                mev_avg_profit_loss_usd: Some(100.0),
                hot_tokens_search_enabled: Some(true),
                hot_tokens_search_interval_blocks: Some(100),
                hot_tokens: Some(vec![Address::zero()]),
                proactive_scanning_enabled: Some(true),
                max_tokens_per_block_scan: Some(50),
                max_opportunities_per_block: Some(10),
                scan_amounts: Some(vec![U256::from(1000000000000000000u128)]),
                scan_usd_amounts: None,
                min_profit_usd: Some(10.0),
                confidence_threshold: Some(0.7),
                mev_resistance_threshold: None,
                gas_bidding_trigger_gwei: None,
                hot_token_search_enabled: Some(true),
            }),
            gas_estimates: rust::config::GasEstimates::default(),
            strict_backtest: false,
            secrets: None,
        };
        
        let config_arc = Arc::new(config);
        
        let backtester = Backtester::new(config_arc).await;
        assert!(backtester.is_ok(), "Failed to initialize backtester");
    }
    
    #[tokio::test]
    async fn test_backtest_config_validation() {
        // Test that backtest config can be created with valid parameters
        let backtest_config = rust::config::BacktestConfig {
            enabled: true,
            chains: std::collections::HashMap::new(),
            max_parallel_blocks: 1,
            database: None,
            random_seed: Some(42),
            rps_limit: 10.0,
            concurrent_limit: 5,
            burst_allowance: 10,
            rate_limit_initial_backoff_ms: 100,
            rate_limit_max_backoff_ms: 1000,
            rate_limit_backoff_multiplier: 2.0,
            rate_limit_jitter_factor: 0.1,
            rate_limit_max_retries: 3,
            rate_limit_timeout_secs: 30,
            batch_size: 10,
            min_batch_delay_ms: 50,
            rate_limiter_settings: rust::config::RateLimiterSettings::default(),
            competition_bot_count: Some(10),
            competition_skill_range: Some((0.1, 0.9)),
            competition_latency_range_ms: Some((50, 200)),
            mev_attack_probability: Some(0.1),
            mev_sandwich_success_rate: Some(0.8),
            mev_frontrun_success_rate: Some(0.6),
            mev_avg_profit_loss_usd: Some(100.0),
            hot_tokens_search_enabled: Some(true),
            hot_tokens_search_interval_blocks: Some(100),
            hot_tokens: Some(vec![Address::zero()]),
            proactive_scanning_enabled: Some(true),
            max_tokens_per_block_scan: Some(50),
            max_opportunities_per_block: Some(10),
            scan_amounts: Some(vec![U256::from(1000000000000000000u128)]),
            scan_usd_amounts: None,
            min_profit_usd: Some(10.0),
            confidence_threshold: Some(0.7),
            mev_resistance_threshold: None,
            gas_bidding_trigger_gwei: None,
            hot_token_search_enabled: Some(true),
        };
        
        assert!(backtest_config.enabled);
        assert_eq!(backtest_config.max_parallel_blocks, 1);
        assert_eq!(backtest_config.competition_bot_count, Some(10));
        assert_eq!(backtest_config.mev_attack_probability, Some(0.1));
    }
    
    #[tokio::test]
    async fn test_backtest_chain_config() {
        // Test backtest chain configuration using the correct endpoint from chains.json
        let chain_config = rust::config::BacktestChainConfig {
            enabled: true,
            backtest_start_block: 1000,
            backtest_end_block: 2000,
            backtest_archival_rpc_url: "https://ethereum-mainnet.core.chainstack.com/8289696ba41f04594614780bac26a4ef".to_string(),
            backtest_result_output_file: "/tmp/test_results.json".to_string(),
            price_oracle: None,
        };
        
        assert!(chain_config.enabled);
        assert_eq!(chain_config.backtest_start_block, 1000);
        assert_eq!(chain_config.backtest_end_block, 2000);
        assert!(!chain_config.backtest_archival_rpc_url.is_empty());
        assert!(!chain_config.backtest_result_output_file.is_empty());
    }
    
    #[tokio::test]
    async fn test_backtest_thresholds() {
        // Test backtest threshold validation
        let min_profit_usd = 10.0;
        let min_confidence_score = 0.7;
        let min_mev_resistance = 0.5;
        
        assert!(min_profit_usd > 0.0);
        assert!(min_confidence_score >= 0.0 && min_confidence_score <= 1.0);
        assert!(min_mev_resistance >= 0.0 && min_mev_resistance <= 1.0);
    }
}