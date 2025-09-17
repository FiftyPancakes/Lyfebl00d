use std::collections::VecDeque;
use std::time::Instant;
use ethers::types::{Address, U256};

/// Statistical metrics used for quantitative analysis of trading opportunities
#[derive(Debug, Clone)]
pub struct StatisticalMetrics {
    pub z_score: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub avg_profit: f64,
    pub volatility: f64,
    pub correlation: f64,
    pub information_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
}

impl StatisticalMetrics {
    /// Create new statistical metrics with default values
    pub fn new() -> Self {
        Self {
            z_score: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
            win_rate: 0.0,
            avg_profit: 0.0,
            volatility: 0.0,
            correlation: 0.0,
            information_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
        }
    }

    /// Calculate risk-adjusted return score
    pub fn risk_adjusted_score(&self) -> f64 {
        let sharpe_weight = 0.4;
        let sortino_weight = 0.3;
        let calmar_weight = 0.2;
        let win_rate_weight = 0.1;

        self.sharpe_ratio * sharpe_weight
            + self.sortino_ratio * sortino_weight
            + self.calmar_ratio * calmar_weight
            + self.win_rate * win_rate_weight
    }

    /// Check if metrics indicate a viable trading strategy
    pub fn is_viable_strategy(&self) -> bool {
        self.sharpe_ratio > 1.0 
            && self.win_rate > 0.55 
            && self.max_drawdown.abs() < 0.15
            && self.volatility < 0.25
    }
}

impl Default for StatisticalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Historical price and volume statistics for a token pair
#[derive(Debug, Clone)]
pub struct TokenPairStats {
    pub price_history: VecDeque<(f64, u64)>, // (price, block_number)
    pub volume_history: VecDeque<(f64, u64)>, // (volume_usd, block_number)
    pub volatility: f64,
    pub mean_price: f64,
    pub std_dev: f64,
    pub cointegration_score: f64,
    pub correlation_coefficient: f64,
    pub last_update: Instant,
    pub sample_size: usize,
    pub price_momentum: f64,
    pub volume_weighted_average_price: f64,
}

impl TokenPairStats {
    /// Create new token pair statistics
    pub fn new() -> Self {
        Self {
            price_history: VecDeque::with_capacity(1000),
            volume_history: VecDeque::with_capacity(1000),
            volatility: 0.0,
            mean_price: 0.0,
            std_dev: 0.0,
            cointegration_score: 0.0,
            correlation_coefficient: 0.0,
            last_update: Instant::now(),
            sample_size: 0,
            price_momentum: 0.0,
            volume_weighted_average_price: 0.0,
        }
    }

    /// Add a new price observation with validation
    pub fn add_price_observation(&mut self, price: f64, block_number: u64) {
        // Validate price
        if !price.is_finite() || price <= 0.0 {
            tracing::warn!("Invalid price observation: {}", price);
            return;
        }
        
        // Check for duplicate or out-of-order block numbers
        if let Some(&(_, last_block)) = self.price_history.back() {
            if last_block >= block_number {
                tracing::debug!("Skipping out-of-order block: {} <= {}", block_number, last_block);
                return;
            }
        }
        
        self.price_history.push_back((price, block_number));
        if self.price_history.len() > 1000 {
            self.price_history.pop_front();
        }
        self.sample_size = self.price_history.len();
        self.last_update = Instant::now();
        self.recalculate_statistics();
    }

    /// Add a new volume observation
    pub fn add_volume_observation(&mut self, volume_usd: f64, block_number: u64) {
        self.volume_history.push_back((volume_usd, block_number));
        if self.volume_history.len() > 1000 {
            self.volume_history.pop_front();
        }
        self.recalculate_vwap();
    }

    /// Recalculate all statistical measures
    fn recalculate_statistics(&mut self) {
        if self.price_history.len() < 2 {
            return;
        }

        let prices: Vec<f64> = self.price_history.iter().map(|(p, _)| *p).collect();
        
        self.mean_price = prices.iter().sum::<f64>() / prices.len() as f64;
        
        let variance = prices.iter()
            .map(|p| (p - self.mean_price).powi(2))
            .sum::<f64>() / (prices.len() - 1) as f64;
        self.std_dev = variance.sqrt();
        
        self.volatility = self.calculate_garch_volatility(&prices);
        self.price_momentum = self.calculate_price_momentum(&prices);
    }

    /// Recalculate volume-weighted average price
    fn recalculate_vwap(&mut self) {
        if self.price_history.is_empty() || self.volume_history.is_empty() {
            return;
        }

        let mut total_volume = 0.0;
        let mut weighted_sum = 0.0;

        // Match prices with volumes by block number
        for (price, price_block) in &self.price_history {
            if let Some((volume, _)) = self.volume_history
                .iter()
                .find(|(_, vol_block)| vol_block == price_block)
            {
                weighted_sum += price * volume;
                total_volume += volume;
            }
        }

        if total_volume > 0.0 {
            self.volume_weighted_average_price = weighted_sum / total_volume;
        }
    }

    /// Calculate GARCH-style volatility estimate
    fn calculate_garch_volatility(&self, prices: &[f64]) -> f64 {
        if prices.len() < 10 {
            return 0.0;
        }

        let returns: Vec<f64> = prices.windows(2)
            .map(|window| (window[1] / window[0]).ln())
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt() * 252.0_f64.sqrt() // Annualized volatility
    }

    /// Calculate price momentum (rate of change)
    fn calculate_price_momentum(&self, prices: &[f64]) -> f64 {
        if prices.len() < 20 {
            return 0.0;
        }

        let recent_window = 10;
        let older_window = 10;

        let recent_avg = prices.iter()
            .rev()
            .take(recent_window)
            .sum::<f64>() / recent_window as f64;

        let older_avg = prices.iter()
            .rev()
            .skip(recent_window)
            .take(older_window)
            .sum::<f64>() / older_window as f64;

        if older_avg != 0.0 {
            (recent_avg - older_avg) / older_avg
        } else {
            0.0
        }
    }

    /// Check if there's sufficient data for statistical analysis
    pub fn has_sufficient_data(&self) -> bool {
        self.price_history.len() >= 50 && self.volume_history.len() >= 30
    }

    /// Get the latest price
    pub fn latest_price(&self) -> Option<f64> {
        self.price_history.back().map(|(price, _)| *price)
    }

    /// Calculate z-score for current price relative to historical mean
    pub fn current_price_z_score(&self) -> Option<f64> {
        if self.std_dev == 0.0 || self.price_history.is_empty() {
            return None;
        }

        self.latest_price().map(|current_price| {
            (current_price - self.mean_price) / self.std_dev
        })
    }
}

impl Default for TokenPairStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Candidate for liquidation on lending protocols
#[derive(Debug, Clone)]
pub struct LiquidationCandidate {
    pub borrower: Address,
    pub collateral_token: Address,
    pub debt_token: Address,
    pub health_factor: f64,
    pub collateral_value_usd: f64,
    pub debt_value_usd: f64,
    pub liquidation_bonus_bps: u32,
    pub protocol: String,
    pub liquidation_threshold: f64,
    pub max_liquidation_amount: U256,
    pub estimated_gas_cost: U256,
    pub priority_score: f64,
    pub discovery_time: Instant,
}

impl LiquidationCandidate {
    /// Calculate the expected profit from liquidating this position
    pub fn expected_profit_usd(&self) -> f64 {
        let liquidation_bonus = self.liquidation_bonus_bps as f64 / 10000.0;
        let max_liquidation_usd = self.debt_value_usd.min(self.collateral_value_usd * 0.5);
        max_liquidation_usd * liquidation_bonus
    }

    /// Check if this candidate is still viable for liquidation
    pub fn is_liquidatable(&self) -> bool {
        self.health_factor < 1.0 && self.collateral_value_usd > 0.0
    }

    /// Calculate priority score based on profitability and urgency
    pub fn calculate_priority_score(&mut self) {
        let profit_score = self.expected_profit_usd() / 1000.0; // Normalize to reasonable range
        let urgency_score = (1.0 - self.health_factor).max(0.0) * 2.0; // More urgent as health factor approaches 0
        let size_score = (self.collateral_value_usd / 10000.0).min(1.0); // Prefer larger positions
        
        self.priority_score = profit_score + urgency_score + size_score;
    }
}

/// Yield farming strategy definition
#[derive(Debug, Clone)]
pub struct YieldStrategy {
    pub protocol: String,
    pub vault_address: Address,
    pub apy: f64,
    pub tvl_usd: f64,
    pub risk_score: f64,
    pub gas_cost_per_rebalance: U256,
    pub min_deposit_amount: U256,
    pub max_deposit_amount: U256,
    pub lock_period_seconds: u64,
    pub fee_bps: u32,
    pub strategy_type: YieldStrategyType,
    pub underlying_tokens: Vec<Address>,
    pub last_rebalance_time: Option<Instant>,
}

/// Types of yield strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YieldStrategyType {
    LiquidityProviding,
    Lending,
    Staking,
    LeveragedFarming,
    ArbitrageYield,
    DeltaNeutral,
}

impl YieldStrategy {
    /// Calculate risk-adjusted yield
    pub fn risk_adjusted_yield(&self) -> f64 {
        self.apy * (1.0 - self.risk_score)
    }

    /// Check if strategy is currently profitable
    pub fn is_profitable(&self, gas_price_gwei: f64) -> bool {
        let annual_gas_cost = (self.gas_cost_per_rebalance.as_u128() as f64 / 1e18) 
            * gas_price_gwei * 1e9 * 365.0 / 30.0; // Assuming monthly rebalancing
        
        self.risk_adjusted_yield() > annual_gas_cost / 1000.0 // Rough profitability check
    }
}

/// Options position for derivatives arbitrage
#[derive(Debug, Clone)]
pub struct OptionsPosition {
    pub strike_price: f64,
    pub expiry: u64,
    pub option_type: OptionType,
    pub underlying_asset: Address,
    pub premium: f64,
    pub implied_volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub rho: f64,
    pub contract_size: U256,
    pub time_to_expiry_days: f64,
    pub moneyness: f64, // Spot/Strike ratio
}

/// Type of option contract
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionType {
    Call,
    Put,
}

impl OptionsPosition {
    /// Calculate theoretical option value using Black-Scholes
    pub fn theoretical_value(&self, spot_price: f64, risk_free_rate: f64) -> f64 {
        black_scholes_price(
            spot_price,
            self.strike_price,
            self.time_to_expiry_days / 365.0,
            risk_free_rate,
            self.implied_volatility,
            self.option_type,
        )
    }

    /// Calculate arbitrage profit potential
    pub fn arbitrage_profit(&self, market_price: f64, spot_price: f64, risk_free_rate: f64) -> f64 {
        let theoretical = self.theoretical_value(spot_price, risk_free_rate);
        (theoretical - market_price).abs()
    }

    /// Check if option is in-the-money
    pub fn is_in_the_money(&self, spot_price: f64) -> bool {
        match self.option_type {
            OptionType::Call => spot_price > self.strike_price,
            OptionType::Put => spot_price < self.strike_price,
        }
    }

    /// Update Greeks based on current market conditions
    pub fn update_greeks(&mut self, spot_price: f64, risk_free_rate: f64) {
        self.delta = calculate_delta(spot_price, self.strike_price, self.time_to_expiry_days / 365.0, 
                                    risk_free_rate, self.implied_volatility, self.option_type);
        self.gamma = calculate_gamma(spot_price, self.strike_price, self.time_to_expiry_days / 365.0, 
                                    risk_free_rate, self.implied_volatility);
        self.theta = calculate_theta(spot_price, self.strike_price, self.time_to_expiry_days / 365.0, 
                                    risk_free_rate, self.implied_volatility, self.option_type);
        self.vega = calculate_vega(spot_price, self.strike_price, self.time_to_expiry_days / 365.0, 
                                  risk_free_rate, self.implied_volatility);
        self.moneyness = spot_price / self.strike_price;
    }
}

/// Black-Scholes option pricing formula
fn black_scholes_price(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    risk_free_rate: f64,
    volatility: f64,
    option_type: OptionType,
) -> f64 {
    if time_to_expiry <= 0.0 || volatility <= 0.0 || spot <= 0.0 || strike <= 0.0 {
        return 0.0;
    }

    let d1 = ((spot / strike).ln() + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry) 
        / (volatility * time_to_expiry.sqrt());
    let d2 = d1 - volatility * time_to_expiry.sqrt();

    match option_type {
        OptionType::Call => {
            spot * normal_cdf(d1) - strike * (-risk_free_rate * time_to_expiry).exp() * normal_cdf(d2)
        }
        OptionType::Put => {
            strike * (-risk_free_rate * time_to_expiry).exp() * normal_cdf(-d2) - spot * normal_cdf(-d1)
        }
    }
}

/// Calculate option delta (price sensitivity to underlying price changes)
fn calculate_delta(spot: f64, strike: f64, time_to_expiry: f64, risk_free_rate: f64, volatility: f64, option_type: OptionType) -> f64 {
    if time_to_expiry <= 0.0 || volatility <= 0.0 {
        return 0.0;
    }

    let d1 = ((spot / strike).ln() + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry) 
        / (volatility * time_to_expiry.sqrt());

    match option_type {
        OptionType::Call => normal_cdf(d1),
        OptionType::Put => normal_cdf(d1) - 1.0,
    }
}

/// Calculate option gamma (delta sensitivity to underlying price changes)
fn calculate_gamma(spot: f64, strike: f64, time_to_expiry: f64, risk_free_rate: f64, volatility: f64) -> f64 {
    if time_to_expiry <= 0.0 || volatility <= 0.0 || spot <= 0.0 {
        return 0.0;
    }

    let d1 = ((spot / strike).ln() + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry) 
        / (volatility * time_to_expiry.sqrt());

    normal_pdf(d1) / (spot * volatility * time_to_expiry.sqrt())
}

/// Calculate option theta (time decay)
fn calculate_theta(spot: f64, strike: f64, time_to_expiry: f64, risk_free_rate: f64, volatility: f64, option_type: OptionType) -> f64 {
    if time_to_expiry <= 0.0 || volatility <= 0.0 {
        return 0.0;
    }

    let d1 = ((spot / strike).ln() + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry) 
        / (volatility * time_to_expiry.sqrt());
    let d2 = d1 - volatility * time_to_expiry.sqrt();

    let common_term = -(spot * normal_pdf(d1) * volatility) / (2.0 * time_to_expiry.sqrt());

    match option_type {
        OptionType::Call => {
            common_term - risk_free_rate * strike * (-risk_free_rate * time_to_expiry).exp() * normal_cdf(d2)
        }
        OptionType::Put => {
            common_term + risk_free_rate * strike * (-risk_free_rate * time_to_expiry).exp() * normal_cdf(-d2)
        }
    }
}

/// Calculate option vega (volatility sensitivity)
fn calculate_vega(spot: f64, strike: f64, time_to_expiry: f64, risk_free_rate: f64, volatility: f64) -> f64 {
    if time_to_expiry <= 0.0 || volatility <= 0.0 || spot <= 0.0 {
        return 0.0;
    }

    let d1 = ((spot / strike).ln() + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry) 
        / (volatility * time_to_expiry.sqrt());

    spot * normal_pdf(d1) * time_to_expiry.sqrt()
}

/// Standard normal cumulative distribution function
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / 2.0_f64.sqrt()))
}

/// Standard normal probability density function
fn normal_pdf(x: f64) -> f64 {
    (-0.5 * x.powi(2)).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// Error function approximation
fn erf(x: f64) -> f64 {
    // Abramowitz and Stegun approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

/// Various opportunity types supported by the alpha engine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpportunityType {
    SingleChainCyclic,
    MultiDexTriangular,
    CrossChainArbitrage,
    StatisticalArbitrage,
    MarketMaking,
    Liquidation,
    FlashLoanArbitrage,
    YieldAggregation,
    OptionsArbitrage,
    PredictiveArbitrage,
    LatencyArbitrage,
    AtomicArbitrage,
    MeanReversion,
    MomentumTrading,
    PairTrading,
    VolatilityArbitrage,
}

impl OpportunityType {
    /// Get the risk profile for this opportunity type
    pub fn base_risk_score(&self) -> f64 {
        match self {
            OpportunityType::SingleChainCyclic => 0.2,
            OpportunityType::MultiDexTriangular => 0.3,
            OpportunityType::CrossChainArbitrage => 0.7,
            OpportunityType::StatisticalArbitrage => 0.4,
            OpportunityType::MarketMaking => 0.5,
            OpportunityType::Liquidation => 0.6,
            OpportunityType::FlashLoanArbitrage => 0.3,
            OpportunityType::YieldAggregation => 0.4,
            OpportunityType::OptionsArbitrage => 0.8,
            OpportunityType::PredictiveArbitrage => 0.9,
            OpportunityType::LatencyArbitrage => 0.2,
            OpportunityType::AtomicArbitrage => 0.1,
            OpportunityType::MeanReversion => 0.5,
            OpportunityType::MomentumTrading => 0.6,
            OpportunityType::PairTrading => 0.4,
            OpportunityType::VolatilityArbitrage => 0.7,
        }
    }

    /// Get expected holding time in seconds
    pub fn expected_holding_time_seconds(&self) -> u64 {
        match self {
            OpportunityType::SingleChainCyclic => 30,
            OpportunityType::MultiDexTriangular => 45,
            OpportunityType::CrossChainArbitrage => 1800, // 30 minutes
            OpportunityType::StatisticalArbitrage => 3600, // 1 hour
            OpportunityType::MarketMaking => 300, // 5 minutes
            OpportunityType::Liquidation => 60,
            OpportunityType::FlashLoanArbitrage => 15,
            OpportunityType::YieldAggregation => 86400, // 1 day
            OpportunityType::OptionsArbitrage => 7200, // 2 hours
            OpportunityType::PredictiveArbitrage => 1800,
            OpportunityType::LatencyArbitrage => 5,
            OpportunityType::AtomicArbitrage => 15,
            OpportunityType::MeanReversion => 3600,
            OpportunityType::MomentumTrading => 1800,
            OpportunityType::PairTrading => 7200,
            OpportunityType::VolatilityArbitrage => 3600,
        }
    }
}