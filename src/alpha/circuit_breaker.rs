use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn, info};
use rand::{thread_rng, Rng};

/// Internal state of the circuit breaker
#[derive(Debug)]
struct BreakerState {
    is_tripped: bool,
    consecutive_failures: u64,
    last_failure_time: Option<Instant>,
    trips: u64,
    last_success_time: Option<Instant>,
    half_open_tests: u64,
    half_open_successes: u64,
    total_failures: u64,
    total_successes: u64,
    current_backoff_multiplier: u64,
    last_reset_time: Instant,
}

/// Circuit breaker for protecting against cascading failures in trading operations
/// 
/// The circuit breaker monitors failure rates and automatically "trips" (disables operations)
/// when failures exceed a configured threshold. After a timeout period with exponential backoff
/// and jitter, it enters a half-open state allowing limited testing. If tests succeed, it
/// recovers; if they fail, it resets with increased backoff.
pub struct CircuitBreaker {
    state: Arc<Mutex<BreakerState>>,
    threshold: u64,
    timeout: Duration,
    half_open_test_count: u64,
    half_open_success_threshold: u64,
    max_backoff_multiplier: u64,
    jitter_factor: f64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the specified failure threshold and recovery timeout
    /// 
    /// # Arguments
    /// * `threshold` - Number of consecutive failures before tripping
    /// * `timeout` - Base duration to wait before attempting recovery
    pub fn new(threshold: u64, timeout: Duration) -> Self {
        Self::with_advanced_config(
            threshold,
            timeout,
            3,  // half_open_test_count
            2,  // half_open_success_threshold
            8,  // max_backoff_multiplier
            0.2 // jitter_factor
        )
    }

    /// Create a circuit breaker with advanced configuration
    pub fn with_advanced_config(
        threshold: u64,
        timeout: Duration,
        half_open_test_count: u64,
        half_open_success_threshold: u64,
        max_backoff_multiplier: u64,
        jitter_factor: f64,
    ) -> Self {
        let now = Instant::now();
        Self {
            state: Arc::new(Mutex::new(BreakerState {
                is_tripped: false,
                consecutive_failures: 0,
                last_failure_time: None,
                trips: 0,
                last_success_time: Some(now),
                half_open_tests: 0,
                half_open_successes: 0,
                total_failures: 0,
                total_successes: 0,
                current_backoff_multiplier: 1,
                last_reset_time: now,
            })),
            threshold,
            timeout,
            half_open_test_count,
            half_open_success_threshold,
            max_backoff_multiplier,
            jitter_factor: jitter_factor.min(0.5).max(0.0), // Clamp between 0 and 0.5
        }
    }

    /// Check if the circuit breaker is currently tripped
    /// 
    /// Returns false if enough time has passed since the last failure to attempt recovery
    /// Implements exponential backoff with jitter for recovery attempts
    pub async fn is_tripped(&self) -> bool {
        let mut state = self.state.lock().await;
        
        if state.is_tripped {
            if let Some(last_failure) = state.last_failure_time {
                // Calculate timeout with exponential backoff and jitter
                let base_timeout = self.timeout.as_secs_f64() * state.current_backoff_multiplier as f64;
                let jitter = if self.jitter_factor > 0.0 {
                    let mut rng = thread_rng();
                    let jitter_range = base_timeout * self.jitter_factor;
                    rng.gen_range(-jitter_range..=jitter_range)
                } else {
                    0.0
                };
                let effective_timeout = Duration::from_secs_f64((base_timeout + jitter).max(1.0));
                
                if last_failure.elapsed() > effective_timeout {
                    // Enter half-open state - allow limited testing
                    if state.half_open_tests == 0 {
                        info!(
                            "Circuit breaker entering half-open state (backoff: {}x, timeout: {:.2}s)",
                            state.current_backoff_multiplier,
                            effective_timeout.as_secs_f64()
                        );
                    }
                    
                                        // Check if we've exhausted half-open tests
                    if state.half_open_tests >= self.half_open_test_count {
                        // Evaluate half-open test results
                        if state.half_open_successes >= self.half_open_success_threshold {
                            // Recovery successful
                            info!(
                                "Circuit breaker recovered: {}/{} successful tests",
                                state.half_open_successes, state.half_open_tests
                            );
                            state.is_tripped = false;
                            state.consecutive_failures = 0;
                            state.half_open_tests = 0;
                            state.half_open_successes = 0;
                            state.current_backoff_multiplier = 1;
                            return false;
                        } else {
                            // Recovery failed, increase backoff
                            warn!(
                                "Circuit breaker recovery failed: only {}/{} successful tests",
                                state.half_open_successes, state.half_open_tests
                            );
                            state.current_backoff_multiplier =
                                (state.current_backoff_multiplier * 2).min(self.max_backoff_multiplier);
                            state.half_open_tests = 0;
                            state.half_open_successes = 0;
                            state.last_failure_time = Some(Instant::now());
                            return true;
                        }
                    }

                    // Allow half-open test
                    state.half_open_tests = state.half_open_tests.saturating_add(1);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// Record a failure event
    /// 
    /// If consecutive failures exceed the threshold, the circuit breaker will trip
    pub async fn record_failure(&self) {
        let mut state = self.state.lock().await;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.total_failures = state.total_failures.saturating_add(1);
        state.last_failure_time = Some(Instant::now());
        
        // If we're in half-open state, this counts as a test failure
        if state.half_open_tests > 0 && state.half_open_tests <= self.half_open_test_count {
            debug!("Half-open test failed ({}/{})", state.half_open_tests, self.half_open_test_count);
        }
        
        if state.consecutive_failures >= self.threshold {
            if !state.is_tripped {
                warn!(
                    "Circuit breaker tripped after {} consecutive failures (threshold: {}, total trips: {})",
                    state.consecutive_failures, self.threshold, state.trips + 1
                );
                state.trips = state.trips.saturating_add(1);
                
                // Reset half-open state if we trip during testing
                if state.half_open_tests > 0 {
                    state.current_backoff_multiplier = 
                        (state.current_backoff_multiplier * 2).min(self.max_backoff_multiplier);
                    state.half_open_tests = 0;
                    state.half_open_successes = 0;
                }
            }
            state.is_tripped = true;
        }
        
        debug!(
            "Circuit breaker recorded failure #{} (threshold: {}, total: {})", 
            state.consecutive_failures, self.threshold, state.total_failures
        );
    }

    /// Record a successful operation
    /// 
    /// Resets the failure counter and may clear the tripped state based on half-open testing
    pub async fn record_success(&self) {
        let mut state = self.state.lock().await;
        let was_tripped = state.is_tripped;
        let was_half_open = state.half_open_tests > 0 && state.half_open_tests <= self.half_open_test_count;
        
        state.total_successes = state.total_successes.saturating_add(1);
        state.last_success_time = Some(Instant::now());
        
        // If we're in half-open state, track the success
        if was_half_open {
            state.half_open_successes = state.half_open_successes.saturating_add(1);
            debug!(
                "Half-open test succeeded ({}/{} successes, {}/{} tests)",
                state.half_open_successes, self.half_open_success_threshold,
                state.half_open_tests, self.half_open_test_count
            );
            
            // Check if we've met the success threshold
            if state.half_open_successes >= self.half_open_success_threshold {
                info!("Circuit breaker fully recovered after {} successful half-open tests", state.half_open_successes);
                state.consecutive_failures = 0;
                state.is_tripped = false;
                state.half_open_tests = 0;
                state.half_open_successes = 0;
                state.current_backoff_multiplier = 1;
            }
        } else if !was_tripped {
            // Normal success, reset consecutive failures
            state.consecutive_failures = 0;
        } else if was_tripped && state.half_open_tests == 0 {
            // Direct recovery without half-open testing (legacy behavior)
            state.consecutive_failures = 0;
            state.is_tripped = false;
            state.current_backoff_multiplier = 1;
            debug!("Circuit breaker recovered after successful operation");
        }
    }

    /// Get the total number of times this circuit breaker has tripped
    pub async fn trips(&self) -> u64 {
        let state = self.state.lock().await;
        state.trips
    }

    /// Get current failure statistics
    pub async fn get_stats(&self) -> CircuitBreakerStats {
        let state = self.state.lock().await;
        CircuitBreakerStats {
            is_tripped: state.is_tripped,
            consecutive_failures: state.consecutive_failures,
            total_trips: state.trips,
            last_failure_time: state.last_failure_time,
            last_success_time: state.last_success_time,
            threshold: self.threshold,
            timeout_seconds: self.timeout.as_secs(),
            total_failures: state.total_failures,
            total_successes: state.total_successes,
            current_backoff_multiplier: state.current_backoff_multiplier,
            half_open_tests: state.half_open_tests,
            half_open_successes: state.half_open_successes,
            uptime_seconds: state.last_reset_time.elapsed().as_secs(),
        }
    }

    /// Force reset the circuit breaker (for administrative override)
    pub async fn force_reset(&self) {
        let mut state = self.state.lock().await;
        warn!("Circuit breaker manually reset by administrator");
        let now = Instant::now();
        state.is_tripped = false;
        state.consecutive_failures = 0;
        state.last_success_time = Some(now);
        state.half_open_tests = 0;
        state.half_open_successes = 0;
        state.current_backoff_multiplier = 1;
        state.last_reset_time = now;
    }

    /// Check if the circuit breaker is in a healthy state
    /// 
    /// Returns true if no recent failures and not tripped
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.lock().await;
        !state.is_tripped && state.consecutive_failures == 0
    }
}

/// Statistics about circuit breaker state
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub is_tripped: bool,
    pub consecutive_failures: u64,
    pub total_trips: u64,
    pub last_failure_time: Option<Instant>,
    pub last_success_time: Option<Instant>,
    pub threshold: u64,
    pub timeout_seconds: u64,
    pub total_failures: u64,
    pub total_successes: u64,
    pub current_backoff_multiplier: u64,
    pub half_open_tests: u64,
    pub half_open_successes: u64,
    pub uptime_seconds: u64,
}

impl CircuitBreakerStats {
    /// Calculate the time since last failure in seconds
    pub fn seconds_since_last_failure(&self) -> Option<u64> {
        self.last_failure_time.map(|t| t.elapsed().as_secs())
    }
    
    /// Calculate the time since last success in seconds
    pub fn seconds_since_last_success(&self) -> Option<u64> {
        self.last_success_time.map(|t| t.elapsed().as_secs())
    }
    
    /// Calculate failure rate as a percentage of threshold
    pub fn failure_rate_percentage(&self) -> f64 {
        (self.consecutive_failures as f64 / self.threshold as f64) * 100.0
    }
    
    /// Calculate overall success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.total_failures + self.total_successes;
        if total == 0 {
            100.0
        } else {
            (self.total_successes as f64 / total as f64) * 100.0
        }
    }
    
    /// Get effective timeout with current backoff
    pub fn effective_timeout_seconds(&self) -> u64 {
        self.timeout_seconds.saturating_mul(self.current_backoff_multiplier)
    }
    
    /// Check if circuit breaker is in half-open state
    pub fn is_half_open(&self) -> bool {
        self.half_open_tests > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_circuit_breaker_basic_operation() {
        let breaker = CircuitBreaker::new(3, Duration::from_millis(100));
        
        // Initially not tripped
        assert!(!breaker.is_tripped().await);
        
        // Record failures
        breaker.record_failure().await;
        assert!(!breaker.is_tripped().await);
        
        breaker.record_failure().await;
        assert!(!breaker.is_tripped().await);
        
        breaker.record_failure().await;
        assert!(breaker.is_tripped().await);
        
        // Should stay tripped
        assert!(breaker.is_tripped().await);
        
        // Wait for timeout
        sleep(Duration::from_millis(150)).await;
        
        // Should enter half-open testing phase and allow an attempt
        let is_tripped = breaker.is_tripped().await;
        // Validate that a test attempt is permitted by checking stats
        let stats = breaker.get_stats().await;
        assert!(stats.half_open_tests > 0 || !is_tripped);
        
        // Success should fully reset
        breaker.record_success().await;
        assert!(!breaker.is_tripped().await);
        assert_eq!(breaker.trips().await, 1);
    }

    #[tokio::test]
    async fn test_circuit_breaker_stats() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(60));
        
        breaker.record_failure().await;
        let stats = breaker.get_stats().await;
        
        assert_eq!(stats.consecutive_failures, 1);
        assert!(!stats.is_tripped);
        assert_eq!(stats.threshold, 2);
        assert!(stats.last_failure_time.is_some());
    }
}