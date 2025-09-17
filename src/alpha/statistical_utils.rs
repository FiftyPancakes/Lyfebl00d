use std::collections::VecDeque;

/// Calculate the arithmetic mean of a collection of values
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Calculate the standard deviation of a collection of values
/// 
/// # Arguments
/// * `values` - The data points
/// * `mean_value` - Optional pre-calculated mean to avoid recalculation
pub fn standard_deviation(values: &[f64], mean_value: Option<f64>) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    
    let mean = mean_value.unwrap_or_else(|| mean(values));
    let variance = values.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>() / (values.len() - 1) as f64;
    
    variance.sqrt()
}

/// Calculate GARCH-style volatility estimate using log returns
pub fn calculate_garch_volatility(prices: &[f64]) -> f64 {
    if prices.len() < 10 {
        return 0.0;
    }

    let returns: Vec<f64> = prices.windows(2)
        .map(|window| {
            if window[0] > 0.0 && window[1] > 0.0 {
                (window[1] / window[0]).ln()
            } else {
                0.0
            }
        })
        .collect();

    if returns.is_empty() {
        return 0.0;
    }

    let mean_return = mean(&returns);
    let variance = returns.iter()
        .map(|r| (r - mean_return).powi(2))
        .sum::<f64>() / returns.len() as f64;

    // Annualized volatility (assuming 252 trading days)
    variance.sqrt() * 252.0_f64.sqrt()
}

/// Calculate Sharpe ratio given returns and risk-free rate
/// 
/// # Arguments
/// * `returns` - Portfolio returns
/// * `risk_free_rate` - Risk-free rate (annual)
/// * `trading_days_per_year` - Number of trading days per year (default 252)
pub fn calculate_sharpe_ratio(returns: &[f64], risk_free_rate: f64, trading_days_per_year: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let mean_return = mean(returns);
    let return_std = standard_deviation(returns, Some(mean_return));
    
    if return_std == 0.0 {
        return 0.0;
    }

    let annualized_return = mean_return * trading_days_per_year;
    let annualized_std = return_std * trading_days_per_year.sqrt();
    
    (annualized_return - risk_free_rate) / annualized_std
}

/// Calculate Sortino ratio (downside deviation instead of total volatility)
/// 
/// # Arguments
/// * `returns` - Portfolio returns
/// * `target_return` - Target return rate (often risk-free rate)
/// * `trading_days_per_year` - Number of trading days per year
pub fn calculate_sortino_ratio(returns: &[f64], target_return: f64, trading_days_per_year: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let mean_return = mean(returns);
    let daily_target = target_return / trading_days_per_year;
    
    // Calculate downside deviation (only negative deviations from target)
    let downside_returns: Vec<f64> = returns.iter()
        .map(|&r| if r < daily_target { r - daily_target } else { 0.0 })
        .collect();
    
    let downside_variance = downside_returns.iter()
        .map(|&r| r.powi(2))
        .sum::<f64>() / returns.len() as f64;
    
    let downside_deviation = downside_variance.sqrt();
    
    if downside_deviation == 0.0 {
        return 0.0;
    }

    let annualized_return = mean_return * trading_days_per_year;
    let annualized_downside_dev = downside_deviation * trading_days_per_year.sqrt();
    
    (annualized_return - target_return) / annualized_downside_dev
}

/// Calculate maximum drawdown from a series of portfolio values
pub fn calculate_max_drawdown(portfolio_values: &[f64]) -> f64 {
    if portfolio_values.len() < 2 {
        return 0.0;
    }

    let mut max_drawdown = 0.0;
    let mut peak = portfolio_values[0];

    for &value in portfolio_values.iter().skip(1) {
        if value > peak {
            peak = value;
        } else {
            let drawdown = (peak - value) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }

    max_drawdown
}

/// Calculate correlation coefficient between two data series
pub fn calculate_correlation(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }

    let mean_x = mean(x);
    let mean_y = mean(y);
    
    let numerator: f64 = x.iter().zip(y.iter())
        .map(|(&xi, &yi)| (xi - mean_x) * (yi - mean_y))
        .sum();
    
    let sum_sq_x: f64 = x.iter().map(|&xi| (xi - mean_x).powi(2)).sum();
    let sum_sq_y: f64 = y.iter().map(|&yi| (yi - mean_y).powi(2)).sum();
    
    let denominator = (sum_sq_x * sum_sq_y).sqrt();
    
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

/// Simple Moving Average calculator
#[derive(Debug, Clone)]
pub struct MovingAverage {
    window_size: usize,
    values: VecDeque<f64>,
    sum: f64,
}

impl MovingAverage {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            values: VecDeque::with_capacity(window_size),
            sum: 0.0,
        }
    }

    pub fn add(&mut self, value: f64) -> Option<f64> {
        self.values.push_back(value);
        self.sum += value;

        if self.values.len() > self.window_size {
            if let Some(old_value) = self.values.pop_front() {
                self.sum -= old_value;
            }
        }

        if self.values.len() == self.window_size {
            Some(self.sum / self.window_size as f64)
        } else {
            None
        }
    }

    pub fn current(&self) -> Option<f64> {
        if self.values.len() == self.window_size {
            Some(self.sum / self.window_size as f64)
        } else {
            None
        }
    }

    pub fn is_ready(&self) -> bool {
        self.values.len() == self.window_size
    }
}

/// Exponential Moving Average calculator
#[derive(Debug, Clone)]
pub struct ExponentialMovingAverage {
    alpha: f64,
    current_value: Option<f64>,
}

impl ExponentialMovingAverage {
    pub fn new(period: usize) -> Self {
        let alpha = 2.0 / (period as f64 + 1.0);
        Self {
            alpha,
            current_value: None,
        }
    }

    pub fn add(&mut self, value: f64) -> f64 {
        match self.current_value {
            Some(current) => {
                let new_value = self.alpha * value + (1.0 - self.alpha) * current;
                self.current_value = Some(new_value);
                new_value
            }
            None => {
                self.current_value = Some(value);
                value
            }
        }
    }

    pub fn current(&self) -> Option<f64> {
        self.current_value
    }
}

/// Bollinger Bands calculator
#[derive(Debug, Clone)]
pub struct BollingerBands {
    moving_average: MovingAverage,
    values: VecDeque<f64>,
    window_size: usize,
    std_dev_multiplier: f64,
}

impl BollingerBands {
    pub fn new(window_size: usize, std_dev_multiplier: f64) -> Self {
        Self {
            moving_average: MovingAverage::new(window_size),
            values: VecDeque::with_capacity(window_size),
            window_size,
            std_dev_multiplier,
        }
    }

    pub fn add(&mut self, value: f64) -> Option<BollingerBandsValue> {
        self.values.push_back(value);
        if self.values.len() > self.window_size {
            self.values.pop_front();
        }

        if let Some(middle) = self.moving_average.add(value) {
            if self.values.len() == self.window_size {
                let values_vec: Vec<f64> = self.values.iter().copied().collect();
                let std_dev = standard_deviation(&values_vec, Some(middle));
                let band_width = self.std_dev_multiplier * std_dev;
                
                return Some(BollingerBandsValue {
                    upper: middle + band_width,
                    middle,
                    lower: middle - band_width,
                    bandwidth: band_width * 2.0,
                });
            }
        }

        None
    }

    pub fn is_ready(&self) -> bool {
        self.values.len() == self.window_size
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BollingerBandsValue {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub bandwidth: f64,
}

impl BollingerBandsValue {
    /// Calculate position within the bands (0.0 = lower band, 1.0 = upper band)
    pub fn position(&self, price: f64) -> f64 {
        if self.bandwidth == 0.0 {
            0.5
        } else {
            (price - self.lower) / (self.upper - self.lower)
        }
    }

    /// Check if price is outside the bands
    pub fn is_outside_bands(&self, price: f64) -> bool {
        price < self.lower || price > self.upper
    }
}

/// Relative Strength Index (RSI) calculator
#[derive(Debug, Clone)]
pub struct RSI {
    period: usize,
    gains: MovingAverage,
    losses: MovingAverage,
    previous_close: Option<f64>,
}

impl RSI {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            gains: MovingAverage::new(period),
            losses: MovingAverage::new(period),
            previous_close: None,
        }
    }

    pub fn add(&mut self, price: f64) -> Option<f64> {
        if let Some(prev_close) = self.previous_close {
            let change = price - prev_close;
            let gain = if change > 0.0 { change } else { 0.0 };
            let loss = if change < 0.0 { -change } else { 0.0 };

            if let (Some(avg_gain), Some(avg_loss)) = (self.gains.add(gain), self.losses.add(loss)) {
                if avg_loss == 0.0 {
                    return Some(100.0);
                }
                let rs = avg_gain / avg_loss;
                let rsi = 100.0 - (100.0 / (1.0 + rs));
                self.previous_close = Some(price);
                return Some(rsi);
            }
        }
        
        self.previous_close = Some(price);
        None
    }

    pub fn is_ready(&self) -> bool {
        self.gains.is_ready() && self.losses.is_ready()
    }
}

/// MACD (Moving Average Convergence Divergence) calculator
#[derive(Debug, Clone)]
pub struct MACD {
    fast_ema: ExponentialMovingAverage,
    slow_ema: ExponentialMovingAverage,
    signal_ema: ExponentialMovingAverage,
    macd_history: Vec<f64>,
}

impl MACD {
    pub fn new(fast_period: usize, slow_period: usize, signal_period: usize) -> Self {
        Self {
            fast_ema: ExponentialMovingAverage::new(fast_period),
            slow_ema: ExponentialMovingAverage::new(slow_period),
            signal_ema: ExponentialMovingAverage::new(signal_period),
            macd_history: Vec::new(),
        }
    }

    pub fn add(&mut self, price: f64) -> Option<MACDValue> {
        let fast = self.fast_ema.add(price);
        let slow = self.slow_ema.add(price);

        // Use the updated EMA values directly for MACD calculation to avoid stale reads
        let macd = fast - slow;
        self.macd_history.push(macd);

        // Update the signal line with the latest MACD and prefer the returned value
        let signal = self.signal_ema.add(macd);

        // Only emit a value once the signal EMA has been sufficiently initialized
        if self.signal_ema.current().is_some() {
            let histogram = macd - signal;
            return Some(MACDValue {
                macd,
                signal,
                histogram,
            });
        }

        None
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MACDValue {
    pub macd: f64,
    pub signal: f64,
    pub histogram: f64,
}

impl MACDValue {
    /// Check if MACD is above signal line (bullish)
    pub fn is_bullish(&self) -> bool {
        self.macd > self.signal
    }

    /// Check if MACD is below signal line (bearish)
    pub fn is_bearish(&self) -> bool {
        self.macd < self.signal
    }

    /// Get the divergence strength between MACD and signal
    pub fn divergence_strength(&self) -> f64 {
        (self.macd - self.signal).abs()
    }
}

/// Calculate cointegration score using enhanced Engle-Granger test
/// 
/// Returns a score between 0 and 1, where higher values indicate stronger cointegration
pub fn calculate_cointegration(x: &[f64], y: &[f64]) -> f64 {
    const MIN_OBSERVATIONS: usize = 30;
    
    if x.len() != y.len() || x.len() < MIN_OBSERVATIONS {
        return 0.0;
    }

    // Simple linear regression to find cointegrating relationship
    let correlation = calculate_correlation(x, y);
    
    // Calculate residuals from the linear relationship
    let mean_x = mean(x);
    let mean_y = mean(y);
    
    let numerator: f64 = x.iter().zip(y.iter())
        .map(|(&xi, &yi)| (xi - mean_x) * (yi - mean_y))
        .sum();
    
    let denominator: f64 = x.iter()
        .map(|&xi| (xi - mean_x).powi(2))
        .sum();
    
    if denominator.abs() < 1e-10 {
        return 0.0;
    }
    
    let beta = numerator / denominator;
    let alpha = mean_y - beta * mean_x;
    
    // Calculate residuals and test for stationarity
    let residuals: Vec<f64> = x.iter().zip(y.iter())
        .map(|(&xi, &yi)| yi - (alpha + beta * xi))
        .collect();
    
    // Enhanced stationarity testing
    let residual_volatility = standard_deviation(&residuals, None);
    let residual_mean = mean(&residuals);
    
    // Check for mean reversion in residuals
    let mut mean_reversion_score = 0.0;
    if residuals.len() > 1 {
        let mut positive_to_negative = 0;
        let mut negative_to_positive = 0;
        
        for i in 1..residuals.len() {
            if residuals[i-1] > residual_mean && residuals[i] < residual_mean {
                positive_to_negative += 1;
            } else if residuals[i-1] < residual_mean && residuals[i] > residual_mean {
                negative_to_positive += 1;
            }
        }
        
        let total_crossings = (positive_to_negative + negative_to_positive) as f64;
        let expected_crossings = (residuals.len() as f64) / 3.0; // Rough expectation for mean-reverting series
        mean_reversion_score = (total_crossings / expected_crossings).min(1.0);
    }
    
    // Combine scores: correlation strength, low residual volatility, and mean reversion
    let volatility_score = if residual_volatility > 0.0 {
        (1.0 / (1.0 + residual_volatility)).min(1.0)
    } else {
        1.0
    };
    
    // Weighted combination of scores
    let cointegration_score = 0.4 * correlation.abs() + 
                              0.3 * volatility_score + 
                              0.3 * mean_reversion_score;
    
    cointegration_score.min(1.0).max(0.0)
}

/// Calculate Value at Risk (VaR) at a given confidence level
pub fn calculate_var(returns: &[f64], confidence_level: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let mut sorted_returns = returns.to_vec();
    sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let index = ((1.0 - confidence_level) * sorted_returns.len() as f64) as usize;
    let index = index.min(sorted_returns.len() - 1);
    
    -sorted_returns[index] // VaR is typically expressed as a positive number
}

/// Calculate Conditional Value at Risk (CVaR/Expected Shortfall)
pub fn calculate_cvar(returns: &[f64], confidence_level: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let var = calculate_var(returns, confidence_level);
    let var_threshold = -var;
    
    let tail_returns: Vec<f64> = returns.iter()
        .filter(|&&r| r <= var_threshold)
        .copied()
        .collect();
    
    if tail_returns.is_empty() {
        return var;
    }
    
    -mean(&tail_returns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1.0, 2.0, 3.0, 4.0, 5.0]), 3.0);
        assert_eq!(mean(&[]), 0.0);
    }

    #[test]
    fn test_standard_deviation() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let std_dev = standard_deviation(&data, None);
        assert!((std_dev - 1.5811388300841898).abs() < 1e-10);
    }

    #[test]
    fn test_moving_average() {
        let mut ma = MovingAverage::new(3);
        assert_eq!(ma.add(1.0), None);
        assert_eq!(ma.add(2.0), None);
        assert_eq!(ma.add(3.0), Some(2.0));
        assert_eq!(ma.add(4.0), Some(3.0));
    }

    #[test]
    fn test_correlation() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [2.0, 4.0, 6.0, 8.0, 10.0];
        let corr = calculate_correlation(&x, &y);
        assert!((corr - 1.0).abs() < 1e-10); // Perfect positive correlation
    }
}