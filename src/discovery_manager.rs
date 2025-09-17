use std::sync::Arc;
use tokio::sync::{mpsc, broadcast};
use tracing::{info, warn, error};

use crate::{
    config::Config,
    errors::ArbitrageError,
    mempool::MEVOpportunity,
    strategies::discovery::{
        CyclicArbitrageScanner,
        StatisticalArbitrageScanner,
        CrossChainArbitrageScanner,
        MarketMakingScanner,
        DiscoveryConfig,
        ScannerContext,
    },
    types::{ChainInfra, MarketRegime},
};

pub struct DiscoveryManager {
    config: Arc<Config>,
    chain_infras: Arc<tokio::sync::RwLock<std::collections::HashMap<String, ChainInfra>>>,
    opportunity_sender: broadcast::Sender<MEVOpportunity>,
    event_broadcaster: broadcast::Sender<crate::types::ChainEvent>,
    scanners: Vec<Arc<dyn crate::strategies::discovery::DiscoveryScanner>>,
    running: Arc<tokio::sync::RwLock<bool>>,
}

impl DiscoveryManager {
    pub fn new(
        config: Arc<Config>,
        chain_infras: Arc<tokio::sync::RwLock<std::collections::HashMap<String, ChainInfra>>>,
        opportunity_sender: broadcast::Sender<MEVOpportunity>,
        event_broadcaster: broadcast::Sender<crate::types::ChainEvent>,
    ) -> Self {
        Self {
            config,
            chain_infras,
            opportunity_sender,
            event_broadcaster,
            scanners: Vec::new(),
            running: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    pub async fn start(&self) -> Result<(), ArbitrageError> {
        info!("Starting DiscoveryManager");
        *self.running.write().await = true;

        // Initialize scanners for each chain
        let chain_infras = self.chain_infras.read().await;
        for (chain_name, chain_infra) in chain_infras.iter() {
            self.initialize_scanners_for_chain(chain_name, chain_infra).await?;
        }

        // Start the main discovery loop
        self.run_discovery_loop().await?;

        Ok(())
    }

    pub async fn stop(&self) {
        info!("Stopping DiscoveryManager");
        *self.running.write().await = false;
    }

    async fn initialize_scanners_for_chain(
        &self,
        chain_name: &str,
        chain_infra: &ChainInfra,
    ) -> Result<(), ArbitrageError> {
        let context = ScannerContext::new(Arc::new(chain_infra.clone()));
        let discovery_config = DiscoveryConfig::default();

        // Create and start CyclicArbitrageScanner
        let cyclic_scanner = Arc::new(CyclicArbitrageScanner::new(
            context.clone(),
            self.config.alpha_engine_settings.min_scan_amount_wei,
            self.config.alpha_engine_settings.min_profit_threshold_usd,
        ));
        cyclic_scanner.start(self.opportunity_sender.clone(), discovery_config.clone()).await?;
        self.scanners.push(cyclic_scanner);

        // Create and start StatisticalArbitrageScanner
        let statistical_pair_stats = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let statistical_scanner = Arc::new(StatisticalArbitrageScanner::new(
            context.clone(),
            self.config.alpha_engine_settings.min_scan_amount_wei,
            self.config.alpha_engine_settings.min_profit_threshold_usd,
            self.config.alpha_engine_settings.statistical_arb_z_score_threshold,
            statistical_pair_stats,
        ));
        statistical_scanner.start(self.opportunity_sender.clone(), discovery_config.clone()).await?;
        self.scanners.push(statistical_scanner);

        // Create and start MarketMakingScanner
        let market_making_scanner = Arc::new(MarketMakingScanner::new(
            context.clone(),
            self.config.alpha_engine_settings.min_scan_amount_wei,
            self.config.alpha_engine_settings.min_profit_threshold_usd,
        ));
        market_making_scanner.start(self.opportunity_sender.clone(), discovery_config.clone()).await?;
        self.scanners.push(market_making_scanner);

        // Create and start CrossChainArbitrageScanner (if cross-chain is enabled)
        if let Some(cross_chain_pathfinder) = &self.config.cross_chain_pathfinder {
            let opportunity_cache = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
            let cross_chain_scanner = Arc::new(CrossChainArbitrageScanner::new(
                context,
                self.config.alpha_engine_settings.min_scan_amount_wei,
                self.config.alpha_engine_settings.min_profit_threshold_usd,
                cross_chain_pathfinder.clone(),
                opportunity_cache,
            ));
            cross_chain_scanner.start(self.opportunity_sender.clone(), discovery_config).await?;
            self.scanners.push(cross_chain_scanner);
        }

        info!("Initialized {} scanners for chain {}", self.scanners.len(), chain_name);
        Ok(())
    }

    async fn run_discovery_loop(&self) -> Result<(), ArbitrageError> {
        let mut block_rx = self.event_broadcaster.subscribe();
        
        while *self.running.read().await {
            match block_rx.recv().await {
                Ok(event) => {
                    match event {
                        crate::types::ChainEvent::EvmNewHead { block, .. } => {
                            if let Some(block_number) = block.number {
                                self.on_new_block(block_number.as_u64()).await?;
                            }
                        }
                        crate::types::ChainEvent::NewMarketRegime(regime) => {
                            self.on_market_regime_change(regime).await;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    warn!("Error receiving event: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn on_new_block(&self, block_number: u64) -> Result<(), ArbitrageError> {
        // Run all scanners for the new block
        for scanner in &self.scanners {
            if scanner.is_running() {
                // For now, we'll use a simplified approach
                // In a real implementation, you'd call the scanner's run_scan method
                // and send the opportunities to the MEVEngine
                match scanner.name() {
                    "CyclicArbitrageScanner" => {
                        // Run cyclic arbitrage scan
                        if let Some(cyclic_scanner) = self.get_scanner_as::<CyclicArbitrageScanner>(scanner) {
                            if let Ok(opportunities) = cyclic_scanner.run_scan(Some(block_number)).await {
                                for opportunity in opportunities {
                                    if let Err(e) = self.opportunity_sender.send(opportunity).await {
                                        error!("Failed to send cyclic opportunity: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    "StatisticalArbitrageScanner" => {
                        // Run statistical arbitrage scan
                        if let Some(statistical_scanner) = self.get_scanner_as::<StatisticalArbitrageScanner>(scanner) {
                            if let Ok(opportunities) = statistical_scanner.run_scan(Some(block_number)).await {
                                for opportunity in opportunities {
                                    if let Err(e) = self.opportunity_sender.send(opportunity).await {
                                        error!("Failed to send statistical opportunity: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    "MarketMakingScanner" => {
                        // Run market making scan
                        if let Some(market_making_scanner) = self.get_scanner_as::<MarketMakingScanner>(scanner) {
                            if let Ok(opportunities) = market_making_scanner.run_scan(Some(block_number)).await {
                                for opportunity in opportunities {
                                    if let Err(e) = self.opportunity_sender.send(opportunity).await {
                                        error!("Failed to send market making opportunity: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    "CrossChainArbitrageScanner" => {
                        // Run cross-chain arbitrage scan
                        if let Some(cross_chain_scanner) = self.get_scanner_as::<CrossChainArbitrageScanner>(scanner) {
                            if let Ok(opportunities) = cross_chain_scanner.run_scan(Some(block_number)).await {
                                for opportunity in opportunities {
                                    if let Err(e) = self.opportunity_sender.send(opportunity).await {
                                        error!("Failed to send cross-chain opportunity: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    async fn on_market_regime_change(&self, new_regime: MarketRegime) {
        info!("Market regime changed to: {:?}", new_regime);
        // Adjust scanner parameters based on market regime
        // This could include changing profit thresholds, scan frequencies, etc.
    }

    fn get_scanner_as<T>(&self, scanner: &Arc<dyn crate::strategies::discovery::DiscoveryScanner>) -> Option<&T> {
        // This is a simplified approach - in practice you'd need proper downcasting
        // or a different architecture to handle this
        None
    }
} 