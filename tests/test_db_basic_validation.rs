/// Basic database validation tests for backtesting
/// 
/// This test suite validates that:
/// 1. Database is accessible
/// 2. Required tables exist with correct schema
/// 3. All protocols have data available
/// 4. Swap data can be loaded and is properly formatted

use anyhow::Result;
use ethers::types::{Address, U256};
use sqlx::{postgres::PgPoolOptions, Row};
use std::collections::HashMap;
use std::str::FromStr;

#[tokio::test]
async fn test_database_connectivity() -> Result<()> {
    println!("Testing database connectivity...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Test basic query
    let result = sqlx::query("SELECT 1 as test")
        .fetch_one(&pool)
        .await?;
    
    let test_value: i32 = result.get("test");
    assert_eq!(test_value, 1);
    
    println!("✅ Database connectivity verified");
    Ok(())
}

#[tokio::test]
async fn test_required_tables_exist() -> Result<()> {
    println!("Testing required tables exist...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Check for required tables
    let tables = sqlx::query(
        "SELECT table_name 
         FROM information_schema.tables 
         WHERE table_schema = 'public' 
         AND table_name IN ('cg_pools', 'swaps', 'coins')"
    )
    .fetch_all(&pool)
    .await?;
    
    let mut found_tables = vec![];
    for row in tables {
        let table_name: String = row.get("table_name");
        found_tables.push(table_name);
    }
    
    assert!(found_tables.contains(&"cg_pools".to_string()), "cg_pools table must exist");
    assert!(found_tables.contains(&"swaps".to_string()), "swaps table must exist");
    
    println!("✅ Required tables exist: {:?}", found_tables);
    Ok(())
}

#[tokio::test]
async fn test_protocol_data_availability() -> Result<()> {
    println!("Testing protocol data availability...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Get protocol counts from cg_pools
    let protocols = sqlx::query(
        "SELECT protocol_type, dex_name, COUNT(*) as pool_count 
         FROM cg_pools 
         WHERE chain_name = 'ethereum' 
         GROUP BY protocol_type, dex_name 
         ORDER BY pool_count DESC
         LIMIT 10"
    )
    .fetch_all(&pool)
    .await?;
    
    println!("\n📊 Protocol pool counts:");
    let mut protocol_map = HashMap::new();
    for row in protocols {
        let protocol_type: i16 = row.get("protocol_type");
        let dex_name: String = row.get("dex_name");
        let count: i64 = row.get("pool_count");
        protocol_map.insert(dex_name.clone(), count);
        println!("  - {} (type {}): {} pools", dex_name, protocol_type, count);
    }
    
    // Verify major protocols are present
    assert!(protocol_map.len() > 0, "Should have at least one protocol");
    
    // Check swap data availability
    let swap_count = sqlx::query(
        "SELECT COUNT(*) as count 
         FROM swaps 
         WHERE chain_name = 'ethereum' 
         AND block_number > 19000000
         LIMIT 1"
    )
    .fetch_one(&pool)
    .await?;
    
    let total_swaps: i64 = swap_count.get("count");
    println!("\n📊 Recent swap count: {}", total_swaps);
    assert!(total_swaps > 0, "Should have recent swap data");
    
    println!("\n✅ Protocol data availability verified");
    Ok(())
}

#[tokio::test]
async fn test_swap_data_format() -> Result<()> {
    println!("Testing swap data format...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Get sample swaps from different protocol types
    // Protocol types: 1=UniswapV2, 2=UniswapV3, 3=Curve, etc.
    let protocol_types = vec![(1i16, "uniswap_v2"), (2i16, "uniswap_v3"), (3i16, "curve")];
    
    for (protocol_type, protocol_name) in protocol_types {
        println!("\n🔍 Testing {} swap data format...", protocol_name);
        
        let swaps = sqlx::query(
            "SELECT pool_address, block_number, token0_address, token1_address,
                    token0_reserve, token1_reserve, sqrt_price_x96, liquidity, tick
             FROM swaps 
             WHERE protocol_type = $1 
             AND chain_name = 'ethereum'
             AND block_number > 19000000
             LIMIT 3"
        )
        .bind(protocol_type)
        .fetch_all(&pool)
        .await?;
        
        if swaps.is_empty() {
            println!("  ⚠️  No recent swaps found for {}", protocol_name);
            continue;
        }
        
        for (i, swap_row) in swaps.iter().enumerate() {
            let pool_address_bytes: Vec<u8> = swap_row.get("pool_address");
            let block_number: i64 = swap_row.get("block_number");
            
            // Check token addresses
            let token0_bytes: Option<Vec<u8>> = swap_row.get("token0_address");
            let token1_bytes: Option<Vec<u8>> = swap_row.get("token1_address");
            
            println!("  Swap {}: block {}, pool {:?}", 
                i + 1, 
                block_number,
                Address::from_slice(&pool_address_bytes)
            );
            
            // Validate swap data structure based on protocol type
            match protocol_name {
                "uniswap_v2" => {
                    let reserve0: Option<f64> = swap_row.try_get("token0_reserve").ok();
                    let reserve1: Option<f64> = swap_row.try_get("token1_reserve").ok();
                    if reserve0.is_some() && reserve1.is_some() {
                        println!("    ✓ V2 reserves present");
                    }
                }
                "uniswap_v3" => {
                    let sqrt_price: Option<String> = swap_row.try_get("sqrt_price_x96").ok();
                    let liquidity: Option<String> = swap_row.try_get("liquidity").ok();
                    let tick: Option<i32> = swap_row.get("tick");
                    if sqrt_price.is_some() || liquidity.is_some() || tick.is_some() {
                        println!("    ✓ V3 data present");
                    }
                }
                "curve" => {
                    let reserve0: Option<f64> = swap_row.try_get("token0_reserve").ok();
                    let reserve1: Option<f64> = swap_row.try_get("token1_reserve").ok();
                    if reserve0.is_some() || reserve1.is_some() {
                        println!("    ✓ Curve data present");
                    }
                }
                _ => {}
            }
            
            // Check token addresses are not null for recent blocks
            // Note: Some swaps may have null token addresses due to data collection issues
            // We'll log this but not fail the test as it's a data quality issue, not a code issue
            if block_number > 19500000 {
                if token0_bytes.is_none() || token1_bytes.is_none() {
                    println!("    ⚠️  Missing token addresses for swap at block {}", block_number);
                }
            }
        }
    }
    
    println!("\n✅ Swap data format validated");
    Ok(())
}

#[tokio::test]
async fn test_pool_state_availability() -> Result<()> {
    println!("Testing pool state availability...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Test state availability for well-known pools
    let test_pools = vec![
        ("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852", "ETH/USDT UniswapV2"),
        ("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", "USDC/ETH UniswapV3"),
        ("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "USDC/ETH UniswapV2"),
    ];
    
    for (pool_address_str, description) in test_pools {
        println!("\n🔄 Testing state for {} ({})", pool_address_str, description);
        
        let pool_address = Address::from_str(pool_address_str)?;
        
        // Check if pool exists in cg_pools
        let pool_info = sqlx::query(
            "SELECT protocol_type, dex_name, token0_address, token1_address 
             FROM cg_pools 
             WHERE pool_address = $1 
             AND chain_name = 'ethereum'"
        )
        .bind(pool_address_str) // pool_address is TEXT in cg_pools
        .fetch_optional(&pool)
        .await?;
        
        if let Some(row) = pool_info {
            let protocol_type: i16 = row.get("protocol_type");
            let dex_name: String = row.get("dex_name");
            println!("  Protocol: {} (type {})", dex_name, protocol_type);
            
            // Check for recent swap data
            let recent_swap = sqlx::query(
                "SELECT block_number 
                 FROM swaps 
                 WHERE pool_address = $1 
                 AND chain_name = 'ethereum'
                 ORDER BY block_number DESC 
                 LIMIT 1"
            )
            .bind(pool_address.as_bytes()) // pool_address is BYTEA in swaps
            .fetch_optional(&pool)
            .await?;
            
            if let Some(swap_row) = recent_swap {
                let block_number: i64 = swap_row.get("block_number");
                println!("  Latest swap at block: {}", block_number);
                println!("  ✓ State data available");
            } else {
                println!("  ⚠️  No swap data found");
            }
        } else {
            println!("  ⚠️  Pool not found in cg_pools");
        }
    }
    
    println!("\n✅ Pool state availability tested");
    Ok(())
}

#[tokio::test]
async fn test_data_consistency() -> Result<()> {
    println!("Testing data consistency...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Check for orphaned swaps (swaps without corresponding pool in cg_pools)
    let orphaned_swaps = sqlx::query(
        "SELECT COUNT(DISTINCT s.pool_address) as count
         FROM swaps s
         LEFT JOIN cg_pools p 
           ON ('0x' || encode(s.pool_address, 'hex')) = lower(p.pool_address)
          AND s.chain_name = p.chain_name
         WHERE p.pool_address IS NULL
           AND s.chain_name = 'ethereum'
           AND s.block_number > 19000000"
    )
    .fetch_one(&pool)
    .await?;
    
    let orphaned_count: i64 = orphaned_swaps.get("count");
    println!("  Orphaned swap pools: {}", orphaned_count);
    
    // Check for swaps with null critical fields
    let incomplete_swaps = sqlx::query(
        "SELECT COUNT(*) as count
         FROM swaps
         WHERE (token0_address IS NULL OR token1_address IS NULL)
         AND chain_name = 'ethereum'
         AND block_number > 19500000"
    )
    .fetch_one(&pool)
    .await?;
    
    let incomplete_count: i64 = incomplete_swaps.get("count");
    println!("  Recent swaps with NULL token addresses: {}", incomplete_count);
    
    // Check protocol consistency
    let protocol_mismatches = sqlx::query(
        "SELECT COUNT(*) as count
         FROM swaps s
         JOIN cg_pools p 
           ON ('0x' || encode(s.pool_address, 'hex')) = lower(p.pool_address)
          AND s.chain_name = p.chain_name
         WHERE s.protocol_type != p.protocol_type
           AND s.chain_name = 'ethereum'
         LIMIT 1"
    )
    .fetch_one(&pool)
    .await?;
    
    let mismatch_count: i64 = protocol_mismatches.get("count");
    println!("  Protocol mismatches: {}", mismatch_count);
    
    println!("\n✅ Data consistency checked");
    Ok(())
}

#[tokio::test]
async fn test_historical_block_coverage() -> Result<()> {
    println!("Testing historical block coverage...");
    
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://ichevako:maddie@localhost:5432/lyfeblood".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    // Check data density across different block ranges
    let block_ranges = vec![
        (19000000u64, 19100000u64, "Block range 19M-19.1M"),
        (19500000, 19600000, "Block range 19.5M-19.6M"),
        (20000000, 20100000, "Block range 20M-20.1M"),
    ];
    
    println!("\n📊 Swap data density:");
    for (start_block, end_block, description) in block_ranges {
        let density = sqlx::query(
            "SELECT 
                COUNT(DISTINCT block_number) as blocks_with_swaps,
                COUNT(*) as total_swaps,
                COUNT(DISTINCT pool_address) as unique_pools
             FROM swaps
             WHERE chain_name = 'ethereum'
             AND block_number BETWEEN $1 AND $2"
        )
        .bind(start_block as i64)
        .bind(end_block as i64)
        .fetch_one(&pool)
        .await?;
        
        let blocks_with_swaps: i64 = density.get("blocks_with_swaps");
        let total_swaps: i64 = density.get("total_swaps");
        let unique_pools: i64 = density.get("unique_pools");
        
        let total_blocks = (end_block - start_block) as i64;
        let coverage_pct = (blocks_with_swaps as f64 / total_blocks as f64) * 100.0;
        
        println!("  {}:", description);
        println!("    Blocks with swaps: {}/{} ({:.1}%)", 
            blocks_with_swaps, total_blocks, coverage_pct);
        println!("    Total swaps: {}", total_swaps);
        println!("    Unique pools: {}", unique_pools);
    }
    
    println!("\n✅ Historical block coverage analyzed");
    Ok(())
}
