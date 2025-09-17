use ethers::types::{Address, U256};
use crate::{
    quote_source::AggregatorQuoteSource,
    errors::PriceError,
};
use std::sync::Arc;
use super::types::{DecimalResolver, safe_u256_to_f64};

/// Wrapper to adapt AggregatorQuoteSource for price oracle use
pub struct AggregatorPriceAdapter {
    quote_source: Arc<dyn AggregatorQuoteSource>,
    decimal_resolver: Arc<DecimalResolver>,
    // Maximum allowed deviation from median when aggregating stablecoin quotes
    max_deviation_fraction: f64,
}

impl AggregatorPriceAdapter {
    pub fn new(quote_source: Arc<dyn AggregatorQuoteSource>, decimal_resolver: Arc<DecimalResolver>) -> Self {
        Self { 
            quote_source,
            decimal_resolver,
            max_deviation_fraction: 0.1,
        }
    }
    
    /// Estimate USD price by comparing against stablecoins
    pub async fn estimate_token_price_usd(
        &self,
        chain_id: u64,
        token: Address,
        stablecoin_addresses: &[Address],
    ) -> Result<f64, PriceError> {
        // Use a small notional to minimize price impact: 0.05 token assuming 18 decimals
        let test_amount = U256::from(5u64).saturating_mul(U256::from(10u64).pow(U256::from(16))); // 5e16

        let mut quotes: Vec<f64> = Vec::new();

        for stablecoin in stablecoin_addresses {
            // Resolve stablecoin decimals with fallback
            let stable_decimals = self.decimal_resolver.get_decimals_with_fallback(*stablecoin, || async {
                Err(PriceError::DataSource("No on-chain decimals resolver in adapter".to_string()))
            }).await;

            match self.quote_source.get_rate_estimate(
                chain_id,
                token,
                *stablecoin,
                test_amount,
            ).await {
                Ok(rate) => {
                    // Convert amount_out to USD using stablecoin decimals
                    let amount_out_f64 = safe_u256_to_f64(rate.amount_out, stable_decimals)?;
                    let amount_in_f64 = safe_u256_to_f64(test_amount, 18)?;
                    if amount_in_f64 > 0.0 {
                        let price = amount_out_f64 / amount_in_f64;
                        if price.is_finite() && price > 0.0 {
                            quotes.push(price);
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        if quotes.is_empty() {
            return Err(PriceError::NotAvailable(format!(
                "No price available for token {:?} via aggregator",
                token
            )));
        }

        // Aggregate quotes: median with outlier rejection
        quotes.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = if quotes.len() % 2 == 1 {
            quotes[quotes.len() / 2]
        } else {
            let mid = quotes.len() / 2;
            (quotes[mid - 1] + quotes[mid]) / 2.0
        };

        let filtered: Vec<f64> = quotes
            .into_iter()
            .filter(|q| {
                let dev = (q - median).abs() / median.max(1e-12);
                dev <= self.max_deviation_fraction
            })
            .collect();

        if filtered.is_empty() {
            return Ok(median);
        }

        let mut filtered_sorted = filtered;
        filtered_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let result = if filtered_sorted.len() % 2 == 1 {
            filtered_sorted[filtered_sorted.len() / 2]
        } else {
            let mid = filtered_sorted.len() / 2;
            (filtered_sorted[mid - 1] + filtered_sorted[mid]) / 2.0
        };

        Ok(result)
    }
}