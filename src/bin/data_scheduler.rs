use anyhow::Result;
use clap::Parser;
use rust::config::Config;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "data_scheduler")]
#[command(about = "Data scheduler for arbitrage bot")]
struct Args {
    #[arg(long, default_value = "config")]
    config_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    info!("Starting data scheduler...");
    
    let config = Arc::new(Config::load_from_directory(&args.config_dir).await.map_err(|e| anyhow::anyhow!("{}", e))?);
    
    info!("Configuration loaded successfully");
    
    // Placeholder for data scheduling logic
    warn!("Data scheduler is not yet implemented");
    
    Ok(())
} 