use std::sync::Arc;
use deadpool_postgres::Pool;
use rust::swap_loader::HistoricalSwapLoader;
use rust::config::ChainConfig;
use rust::types::{ChainConfig as ChainCfg};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a basic config for testing
    let mut chain_config = ChainConfig::default();
    chain_config.chains.insert("ethereum".to_string(), ChainCfg {
        rpc_url: "http://localhost:8545".to_string(),
        max_gas_price: ethers::types::U256::from(100000000000u64), // 100 gwei
        reference_stablecoins: Some(vec![
            ethers::types::Address::from_str("0xA0b86a33E6441e88C5F2712C3E9b74F6C5f0eB1A").unwrap(), // USDC
        ]),
        avg_block_time_seconds: Some(12.0),
        price_lookback_blocks: Some(1000),
        backtest_use_external_price_apis: Some(false),
        weth_address: ethers::types::Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
        ..Default::default()
    });

    let chain_config = Arc::new(chain_config);
    
    // Create database pool
    let mut cfg = deadpool_postgres::Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.user = Some("ichevako".to_string());
    cfg.password = Some("maddie".to_string());
    cfg.dbname = Some("lyfeblood".to_string());
    let pool = cfg.create_pool(None, tokio_postgres::NoTls)?;
    
    // Create swap loader
    let swap_loader = Arc::new(HistoricalSwapLoader::new(Arc::new(pool), chain_config).await?);
    
    // Test loading swaps for a small block range
    let start_block = 23026000i64;
    let end_block = 23026005i64;
    
    println!("Testing swap loading for blocks {}-{}", start_block, end_block);
    
    let swaps = swap_loader.load_swaps_for_block_chunk("ethereum", start_block, end_block).await?;
    
    println!("Loaded {} swaps", swaps.len());
    
    // Count by protocol
    let mut protocol_counts = std::collections::HashMap::new();
    for swap in &swaps {
        *protocol_counts.entry(format!("{:?}", swap.protocol)).or_insert(0) += 1;
    }
    
    println!("Protocol distribution:");
    for (protocol, count) in protocol_counts {
        println!("  {}: {}", protocol, count);
    }
    
    // Check if we have any previously filtered protocols
    let has_curve = swaps.iter().any(|s| format!("{:?}", s.protocol).contains("Curve"));
    let has_v2 = swaps.iter().any(|s| format!("{:?}", s.protocol).contains("UniswapV2"));
    let has_v3 = swaps.iter().any(|s| format!("{:?}", s.protocol).contains("UniswapV3"));
    
    println!("\nProtocol presence:");
    println!("  Curve: {}", has_curve);
    println!("  V2: {}", has_v2);
    println!("  V3: {}", has_v3);
    
    Ok(())
}
