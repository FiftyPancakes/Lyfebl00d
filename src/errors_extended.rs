use thiserror::Error;

/// Extended error types for the enhanced transaction optimizer
#[derive(Error, Debug)]
pub enum OptimizerError {
    #[error("Blockchain error: {0}")]
    Blockchain(String),
    
    #[error("Provider error: {0}")]
    Provider(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Gas estimation error: {0}")]
    GasEstimation(String),
    
    #[error("Submission final error: {0}")]
    SubmissionFinal(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Security error: {0}")]
    Security(String),
    
    #[error("Signing error: {0}")]
    Signing(String),
    
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    #[error("File I/O error: {0}")]
    FileIO(String),
    
    #[error("Simulation error: {0}")]
    Simulation(String),
    
    #[error("Concurrency error: {0}")]
    Concurrency(String),
    
    #[error("Circuit breaker is open")]
    CircuitBreakerOpen,
    
    #[error("Maximum retries exceeded")]
    MaxRetriesExceeded,
    
    #[error("Daily limit exceeded")]
    DailyLimitExceeded,
    
    #[error("Invalid transaction")]
    InvalidTransaction,
    
    #[error("Cache error: {0}")]
    Cache(String),
}