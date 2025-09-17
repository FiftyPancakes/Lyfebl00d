//! # Centralized Error Handling
//!
//! This module defines a comprehensive, hierarchical error enum for the entire
//! application. Using a centralized, typed error system provides robust error
//! handling, clear debugging context, and prevents the propagation of ambiguous
//! string-based errors.

use deadpool_postgres::PoolError;
use ethers::types::{Address, H256, U256};
use thiserror::Error;
use crate::types::LegExecutionResult;

/// The top-level error type, encapsulating all possible failures within the bot.
#[derive(Error, Debug)]
pub enum BotError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Infrastructure setup error: {0}")]
    Infrastructure(String),
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Arbitrage execution error: {0}")]
    Arbitrage(#[from] ArbitrageError),
    #[error("Listener error: {0}")]
    Listener(#[from] ListenerError),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("DEX error: {0}")]
    Dex(#[from] DexError),
    #[error("Risk error: {0}")]
    Risk(#[from] RiskError),
    #[error("Gas oracle error: {0}")]
    Gas(#[from] GasError),
    #[error("Price oracle error: {0}")]
    Price(#[from] PriceError),
    #[error("Data science error: {0}")]
    DataScience(#[from] DataScienceError),
    #[error("Backtest error: {0}")]
    Backtest(#[from] BacktestError),
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Response parsing error: {0}")]
    ResponseParse(String),
    #[error("Configuration update error: {0}")]
    ConfigUpdate(String),
    #[error("Channel error: {0}")]
    Channel(String),
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("Math error: {0}")]
    Math(String),
    #[error("Other error: {0}")]
    Other(String),
    #[error("System shut down")]
    Shutdown,
}

/// Errors related to the `ArbitrageEngine` and its execution pipeline.
#[derive(Error, Debug)]
pub enum ArbitrageError {
    #[error("Pathfinding failed: {0}")]
    PathFinding(#[from] PathError),
    #[error("Risk check failed: {0}")]
    RiskCheckFailed(String),
    #[error("Trade simulation failed: {0}")]
    SimulationFailed(String),
    #[error("Transaction optimization failed: {0}")]
    OptimizationFailed(#[from] OptimizerError),
    #[error("Opportunity is no longer profitable or viable")]
    OpportunityNotViable,
    #[error("Opportunity is stale for block {0}")]
    OpportunityStale(u64),
    #[error("Risk assessment failed: {0}")]
    RiskAssessmentFailed(String),
    #[error("Risk threshold exceeded: {0}")]
    RiskThresholdExceeded(f64),
    #[error("Blockchain query failed: {0}")]
    BlockchainQueryFailed(String),
    #[error("Partial execution failure: completed {completed_legs} legs, failed at leg {failed_leg}: {error}")]
    PartialExecutionFailure {
        completed_legs: usize,
        failed_leg: usize,
        error: String,
        partial_results: Vec<LegExecutionResult>,
    },
    #[error("Swap execution failed: {0}")]
    SwapExecutionFailed(String),
    #[error("Profit threshold not met: actual {0}, required {1}")]
    ProfitThresholdNotMet(f64, f64),
    #[error("Excessive gas usage: used {0}, max expected {1}")]
    ExcessiveGasUsage(U256, U256),
    #[error("Unrecoverable failure: {0}")]
    UnrecoverableFailure(String),
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),
    #[error("Gas estimation failed: {0}")]
    GasEstimationFailed(String),
    #[error("Slippage exceeded: expected {0}, actual {1}")]
    SlippageExceeded(U256, U256),
    #[error("Submission failed: {0}")]
    Submission(String),
    #[error("Simulator error: {0}")]
    Simulator(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Price oracle error: {0}")]
    PriceOracle(String),
    #[error("Token registry error: {0}")]
    TokenRegistry(String),
    #[error("Cross-chain error: {0}")]
    CrossChain(#[from] CrossChainError),
    #[error("DEX error: {0}")]
    Dex(String),
    #[error("Other arbitrage error: {0}")]
    Other(String),
}



impl From<GasError> for ArbitrageError {
    fn from(e: GasError) -> Self {
        ArbitrageError::GasEstimationFailed(format!("gas oracle error: {}", e))
    }
}

impl From<DexError> for ArbitrageError {
    fn from(e: DexError) -> Self {
        ArbitrageError::Dex(format!("dex operation failed: {}", e))
    }
}

/// Errors related to the Backtesting engine and simulation.
#[derive(Error, Debug)]
pub enum BacktestError {
    #[error("Failed to initialize backtest infrastructure: {0}")]
    InfraInit(String),
    #[error("Failed to load historical swaps: {0}")]
    SwapLoad(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Data loading error: {0}")]
    DataLoad(String),
    #[error("Failed to process block {0}: {1}")]
    BlockProcessing(u64, String),
    #[error("Failed to save backtest results: {0}")]
    ResultSave(String),
    #[error("Competition simulation error: {0}")]
    Competition(String),
    #[error("MEV attack simulation error: {0}")]
    MevAttack(String),
    #[error("Pool discovery error: {0}")]
    PoolDiscovery(String),
    #[error("Invalid field format for {0}: {1}")]
    InvalidFieldFormat(String, String),
    #[error("Missing field {0}: {1}")]
    MissingField(String, String),
    #[error("Null field {0}")]
    NullField(String),
    #[error("Data source error: {0}")]
    DataSourceError(String),
    #[error("Data unavailable: {0}")]
    DataUnavailable(String),
    #[error("No opportunity: {0}")]
    NoOpportunity(String),
    #[error("Path validation error: {0}")]
    PathValidation(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Infrastructure error: {0}")]
    Infrastructure(String),
    #[error("IO error: {0}")]
    IO(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Numeric safety error: {0}")]
    NumericSafety(#[from] NumericSafetyError),
    #[error("Other backtest error: {0}")]
    Other(String),
}

/// Errors related to numeric safety and precision operations
#[derive(Error, Debug, Clone)]
pub enum NumericSafetyError {
    #[error("Token decimals cannot exceed 18, got {0}")]
    InvalidDecimals(u8),
    #[error("Overflow in {operation}: {details}")]
    Overflow { operation: String, details: String },
    #[error("Division by zero in {operation}")]
    DivisionByZero { operation: String },
    #[error("Invalid percentage {0}: must be between 0 and 100")]
    InvalidPercentage(f64),
    #[error("Invalid BPS value {0}: must be between 0 and 10000")]
    InvalidBps(u16),
    #[error("Gas limit out of reasonable bounds: {0}")]
    InvalidGasLimit(u128),
    #[error("Calldata too large: {0} bytes (max 64KB)")]
    CalldataTooLarge(usize),
    #[error("Deadline has expired: {0}")]
    ExpiredDeadline(u64),
    #[error("Conversion error from {from} to {to}")]
    Conversion { from: String, to: String },
    #[error("Zero recipient address not allowed")]
    ZeroRecipient,
    #[error("Minimum return cannot be zero")]
    ZeroMinReturn,
}

/// Errors related to the `PathFinder`.
#[derive(Error, Debug, Clone)]
pub enum PathError {
    #[error("No profitable route found")]
    NoRouteFound,
    #[error("Graph is not ready or is missing data")]
    GraphNotReady,
    #[error("Token not found in graph: {0:?}")]
    TokenNotFound(Address),
    #[error("Timeout during pathfinding search")]
    Timeout,
    #[error("Graph update failed: {0}")]
    GraphUpdateFailed(String),
    #[error("DEX error: {0}")]
    Dex(#[from] DexError),
    #[error("Generic pathfinding error: {0}")]
    Generic(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Calculation error: {0}")]
    Calculation(String),
    #[error("Unexpected error: {0}")]
    Unexpected(String),
    #[error("Market data error: {0}")]
    MarketData(String),
    #[error("Gas estimation error: {0}")]
    GasEstimation(String),
    #[error("Pool manager error: {0}")]
    PoolManager(String),
}

impl From<DexMathError> for PathError {
    fn from(err: DexMathError) -> Self {
        PathError::Calculation(format!("DEX math error: {}", err))
    }
}

impl From<GasError> for PathError {
    fn from(err: GasError) -> Self {
        PathError::GasEstimation(format!("Gas oracle error: {}", err))
    }
}

/// Errors related to the `TransactionOptimizer`.
#[derive(Error, Debug, Clone)]
pub enum OptimizerError {
    #[error("Transaction submission failed after all retries: {0}")]
    SubmissionFinal(String),
    #[error("Simulation failed: {0}")]
    Simulation(String),
    #[error("Gas estimation failed: {0}")]
    GasEstimation(String),
    #[error("Signing transaction failed: {0}")]
    Signing(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Provider error: {0}")]
    Provider(String),
    #[error("Operation timed out: {0}")]
    Timeout(String),
    #[error("Transaction encoding failed: {0}")]
    Encoding(String),
    #[error("Transaction reverted with hash: {tx_hash:?}, reason: {reason}")]
    Reverted { tx_hash: H256, reason: String },
    #[error("Transaction dropped from mempool with hash: {0:?}")]
    Dropped(H256),
    #[error("Concurrency error: {0}")]
    Concurrency(String),
    #[error("Circuit breaker is open: {0}")]
    CircuitBreakerOpen(String),
    #[error("Maximum retries exceeded: {0}")]
    MaxRetriesExceeded(String),
    #[error("File I/O error: {0}")]
    FileIO(String),
}

/// Errors related to real-time event listeners.
#[derive(Error, Debug)]
pub enum ListenerError {
    #[error("Provider connection failed: {0}")]
    Connection(String),
    #[error("Subscription failed: {0}")]
    Subscription(String),
    #[error("Internal channel error: {0}")]
    Channel(String),
    #[error("Failed to decode event log: {0}")]
    EventDecoding(String),
    #[error("Operation timed out: {0}")]
    Timeout(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Failed to decode swap events: {0}")]
    DecodeError(String),
}

/// Errors related to blockchain interaction and RPC calls.
#[derive(Error, Debug, Clone)]
pub enum BlockchainError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("RPC provider error: {0}")]
    Provider(String),
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Wallet error: {0}")]
    WalletError(String),
    #[error("Attempted operation is not available: {0}")]
    NotAvailable(String),
    #[error("Invalid operation for current execution mode (Live/Backtest): {0}")]
    ModeError(String),
    #[error("Failed to send transaction: {0}")]
    SendTransaction(String),
    #[error("Gas price {0} is higher than configured max {1}")]
    GasPriceTooHigh(U256, U256),
    #[error("Gas estimation failed: {0}")]
    GasEstimation(String),
    #[error("Data encoding/decoding error: {0}")]
    DataEncoding(String),
    #[error("Block not found")]
    BlockNotFound,
    #[error("Rate limit error: {0}")]
    RateLimitError(String),
    #[error("HTTP request error: {0}")]
    Reqwest(String),
    #[error("Serialization/Deserialization error: {0}")]
    Serde(String),
    #[error("An internal or unexpected error occurred: {0}")]
    Internal(String),
    #[error("Error downcasting to a specific error type")]
    DowncastError,
    #[error("Other blockchain error: {0}")]
    Other(String),
}

/// Errors related to state persistence and database interaction.
#[derive(Error, Debug, Clone)]
pub enum StateError {
    #[error("Database not available")]
    DbNotAvailable,
    #[error("Database connection pool error: {0}")]
    Pool(String),
    #[error("Database query or execution error: {0}")]
    Query(String),
    #[error("Data serialization or deserialization error: {0}")]
    Serialization(String),
    #[error("Data parsing error: {0}")]
    Parse(String),
    #[error("Transaction error during database operation: {0}")]
    Transaction(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Errors related to DEX-specific mathematical calculations.
#[derive(Error, Debug, Clone)]
pub enum DexMathError {
    #[error("Invalid token for this pool")]
    InvalidToken,
    #[error("Insufficient liquidity to perform swap")]
    InsufficientLiquidity,
    #[error("Calculation resulted in overflow")]
    Overflow,
    #[error("Division by zero")]
    DivisionByZero,
    #[error("Invalid V3 state: {0}")]
    InvalidV3State(String),
    #[error("Invalid V2 state: insufficient reserves")]
    InsufficientReserves,
    #[error("Zero liquidity in pool")]
    ZeroLiquidity,
    #[error("Invalid sqrt price")]
    InvalidSqrtPrice,
    #[error("Tick not found")]
    TickNotFound,
}

/// Errors related to DEX execution and swap management.
#[derive(Error, Debug, Clone)]
pub enum DexError {
    #[error("Route not found: {0}")]
    RouteNotFound(String),
    #[error("Swap simulation failed: {0}")]
    SwapSimulation(String),
    #[error("Token approval failed: {0}")]
    TokenApproval(String),
    #[error("Encoding error: {0}")]
    Encoding(String),
    #[error("DEX math error: {0}")]
    Math(DexMathError),
    #[error("Blockchain error: {0}")]
    Blockchain(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("No rate found")]
    NoRateFound,
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    #[error("Pool state error: {0}")]
    PoolState(String),
    #[error("Pool discovery error: {0}")]
    PoolDiscovery(String),
    #[error("Pool info error: {0}")]
    PoolInfoError(String),
    #[error("Pool not found: {0}")]
    PoolNotFound(String),
    #[error("Pool manager error: {0}")]
    PoolManagerError(String),
    #[error("Pool state not found for pool {0} at block {1}")]
    PoolStateNotFound(Address, u64),
    #[error("Other DEX error: {0}")]
    Other(String),
}

/// Errors related to predictive engine operations, model inference, and prediction lifecycle.
#[derive(thiserror::Error, Debug)]
pub enum PredictiveEngineError {
    #[error("Prediction failed: {0}")]
    Prediction(String),
    #[error("Model update failed: {0}")]
    ModelUpdate(String),
    #[error("Model load failed: {0}")]
    ModelLoad(String),
    #[error("Model not ready")]
    NotReady,
    #[error("Inference error: {0}")]
    Inference(String),
    #[error("Feature extraction error: {0}")]
    FeatureExtraction(String),
    #[error("Performance metrics unavailable: {0}")]
    MetricsUnavailable(String),
    #[error("Historical data unavailable for block {block_number} on chain {chain_name}")]
    HistoricalDataUnavailable { chain_name: String, block_number: u64 },
    #[error("Market data error: {0}")]
    MarketData(#[from] MarketDataError),
    #[error("Gas oracle error: {0}")]
    GasOracle(#[from] GasError),
    #[error("Price oracle error: {0}")]
    PriceOracle(#[from] PriceError),
    #[error("DEX error: {0}")]
    Dex(#[from] DexError),
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("Other predictive engine error: {0}")]
    Other(String),
}


/// Errors related to gas price estimation, gas oracles, and gas cost calculations.
#[derive(Error, Debug)]
pub enum GasError {
    #[error("Gas price lookup failed: {0}")]
    GasPriceLookup(String),
    #[error("Gas estimation failed: {0}")]
    GasEstimation(String),
    #[error("Gas oracle misconfiguration: {0}")]
    Config(String),
    #[error("Historical gas data unavailable for block {block_number} on chain {chain_name}")]
    HistoricalDataUnavailable { chain_name: String, block_number: u64 },
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("State error: {0}")]
    StateError(String),
    #[error("Data source error: {0}")]
    DataSource(String),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Other gas oracle error: {0}")]
    Other(String),
}

/// Errors related to MEV engine operations and strategy execution.
#[derive(Error, Debug)]
pub enum MEVEngineError {
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("Strategy execution failed: {0}")]
    StrategyError(String),
    #[error("Simulation failed: {0}")]
    SimulationError(String),
    #[error("Risk assessment failed: {0}")]
    RiskAssessment(String),
    #[error("Transaction optimization failed: {0}")]
    TransactionOptimization(String),
    #[error("Bundle submission failed: {0}")]
    BundleSubmission(String),
    #[error("Opportunity processing failed: {0}")]
    OpportunityProcessing(String),
    #[error("MEV play management failed: {0}")]
    PlayManagement(String),
    #[error("Gas estimation failed: {0}")]
    GasEstimation(String),
    #[error("Price oracle error: {0}")]
    PriceOracle(String),
    #[error("Blockchain error: {0}")]
    Blockchain(String),
    #[error("Pool manager error: {0}")]
    PoolManager(String),
    #[error("Path finding error: {0}")]
    PathFinding(String),
    #[error("Risk error: {0}")]
    RiskError(String),
    #[error("Submission error: {0}")]
    SubmissionError(String),
    #[error("Pricing error: {0}")]
    PricingError(String),
    #[error("Calldata error: {0}")]
    CalldataError(String),
    #[error("Unsupported protocol: {0}")]
    UnsupportedProtocol(String),
    #[error("Unsupported strategy: {0}")]
    UnsupportedStrategy(String),
    #[error("Resource error: {0}")]
    ResourceError(String),
    #[error("Monitor error: {0}")]
    MonitorError(String),
    #[error("Other MEV engine error: {0}")]
    Other(String),
    #[error("Profit too low: {0}")]
    ProfitTooLow(String),
    #[error("Monitoring error: {0}")]
    MonitoringError(String),
    #[error("Config error: {0}")]
    ConfigError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Arithmetic error: {0}")]
    ArithmeticError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("Insufficient balance: {0}")]
    InsufficientBalance(String),
}

impl From<OptimizerError> for MEVEngineError {
    fn from(err: OptimizerError) -> Self {
        MEVEngineError::TransactionOptimization(format!("Transaction optimizer error: {}", err))
    }
}

impl From<GasError> for MEVEngineError {
    fn from(err: GasError) -> Self {
        MEVEngineError::GasEstimation(format!("Gas oracle error: {}", err))
    }
}

impl From<DexError> for MEVEngineError {
    fn from(err: DexError) -> Self {
        MEVEngineError::PoolManager(format!("DEX operation failed: {}", err))
    }
}

impl From<&str> for MEVEngineError {
    fn from(err: &str) -> Self {
        MEVEngineError::Other(err.to_string())
    }
}

/// Errors related to market data retrieval, normalization, and validation.
#[derive(thiserror::Error, Debug)]
pub enum MarketDataError {
    #[error("Market data fetch failed: {0}")]
    Fetch(String),
    #[error("Market data is stale or unavailable")]
    StaleOrUnavailable,
    #[error("Market data normalization error: {0}")]
    Normalization(String),
    #[error("Market data validation error: {0}")]
    Validation(String),
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("Other market data error: {0}")]
    Other(String),
}


/// Errors related to price oracle, price fetching, and price calculations.
#[derive(Error, Debug, Clone)]
pub enum PriceError {
    #[error("Price lookup failed: {0}")]
    PriceLookup(String),
    #[error("Price data is stale or unavailable")]
    StaleOrUnavailable,
    #[error("Price deviation too high: {0}")]
    Deviation(String),
    #[error("Price normalization error: {0}")]
    Normalization(String),
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("State error: {0}")]
    StateError(String),
    #[error("Data source error: {0}")]
    DataSource(String),
    #[error("Not available: {0}")]
    NotAvailable(String),
    #[error("Market data error: {0}")]
    MarketData(String),
    #[error("Mode error: {0}")]
    ModeError(String),
    #[error("Calculation error: {0}")]
    Calculation(String),
    #[error("Input error: {0}")]
    InputError(String),
    #[error("Other price oracle error: {0}")]
    Other(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),
}

/// Errors related to risk management and assessment.
#[derive(Error, Debug, Clone)]
pub enum RiskError {
    #[error("Price oracle error: {0}")]
    PriceOracle(String),
    #[error("Gas oracle error: {0}")]
    GasOracle(String),
    #[error("Pool manager error: {0}")]
    PoolManager(String),
    #[error("Risk calculation error: {0}")]
    Calculation(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("No pools available")]
    NoPoolsAvailable,
    #[error("DEX math error: {0}")]
    DexMath(#[from] DexMathError),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Other risk error: {0}")]
    Other(String),
}

/// Errors related to data science, analytics, and market analysis.
#[derive(Error, Debug)]
pub enum DataScienceError {
    #[error("Database error: {0}")]
    Database(#[from] StateError),
    #[error("Database pool error: {0}")]
    Pool(#[from] PoolError),
    #[error("Database query error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Analysis failed: {0}")]
    Analysis(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Time series error: {0}")]
    TimeSeries(#[from] TimeSeriesError),
    #[error("Other data science error: {0}")]
    Other(String),
}

impl From<eyre::Report> for DataScienceError {
    fn from(e: eyre::Report) -> Self {
        DataScienceError::Analysis(e.to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MempoolError {
    #[error("Mempool configuration error: {0}")]
    Config(String),
    #[error("Provider error: {0}")]
    Provider(String),
    #[error("Subscription error: {0}")]
    Subscription(String),
    #[error("Internal channel error: {0}")]
    Channel(String),
    #[error("Failed to decode transaction: {0}")]
    Decoding(String),
    #[error("Error decoding transaction: {0}")]
    DecoderError(#[from] DecoderError),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Monitor error: {0}")]
    MonitorError(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

#[derive(Error, Debug)]
pub enum DecoderError {
    #[error("Failed to decode ABI data: {0}")]
    AbiDecodeFailed(String),
    #[error("Failed to extract parameter: {0}")]
    ParamExtractionFailed(String),
    #[error("Unsupported function selector: {0}")]
    UnsupportedSelector(String),
}

#[derive(Error, Debug)]
pub enum EnricherError {
    #[error("Failed to get data from provider: {0}")]
    ProviderError(String),
    #[error("Data not available for enrichment: {0}")]
    DataUnavailable(String),
    #[error("An internal error occurred: {0}")]
    InternalError(String),
}

#[derive(Error, Debug, Clone)]
pub enum TimeSeriesError {
    #[error("Data source returned an error: {0}")]
    ProviderError(String),
    #[error("Not enough data to perform analysis for period")]
    DataUnavailable,
    #[error("An internal error occurred in time series analysis: {0}")]
    InternalError(String),
    #[error("Dependency error: {0}")]
    Dependency(String),
    #[error("Insufficient data for analysis: {0}")]
    InsufficientData(String),
    #[error("Computation error: {0}")]
    ComputationError(String),
}

/// Errors related to cross-chain operations and bridge execution.
#[derive(thiserror::Error, Debug)]
pub enum CrossChainError {
    #[error("Cross-chain configuration error: {0}")]
    Configuration(String),
    #[error("Cross-chain timeout: {0}")]
    Timeout(String),
    #[error("Insufficient profit: {0}")]
    InsufficientProfit(String),
    #[error("Chain not found: {0}")]
    ChainNotFound(u64),
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),
    #[error("External API error: {0}")]
    ExternalAPI(String),
    #[error("Dependency error: {0}")]
    DependencyError(String),
    #[error("Calculation error: {0}")]
    Calculation(String),
    #[error("Other cross-chain error: {0}")]
    Other(String),
}

/// Errors related to the PoolManager and pool discovery/indexing.
#[derive(thiserror::Error, Debug, Clone)]
pub enum PoolManagerError {
    #[error("PoolManager configuration error: {0}")]
    Config(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Blockchain error: {0}")]
    Blockchain(String),
    #[error("Pool discovery failed: {0}")]
    PoolDiscovery(String),
    #[error("Pool update failed: {0}")]
    PoolUpdateFailed(String),
    #[error("No blockchain manager for chain {0}")]
    NoBlockchainManager(String),
    #[error("DEX error: {0}")]
    Dex(#[from] DexError),
    #[error("Other pool manager error: {0}")]
    Other(String),
}

impl From<PoolManagerError> for DexError {
    fn from(e: PoolManagerError) -> Self {
        match e {
            PoolManagerError::Config(msg) => DexError::PoolManagerError(format!("PoolManager config: {msg}")),
            PoolManagerError::Blockchain(msg) => DexError::Blockchain(msg),
            PoolManagerError::PoolDiscovery(msg) => DexError::PoolDiscovery(msg),
            PoolManagerError::PoolUpdateFailed(msg) => DexError::PoolState(msg),
            PoolManagerError::NoBlockchainManager(msg) => DexError::PoolManagerError(format!("No blockchain manager: {msg}")),
            PoolManagerError::Dex(err) => err,
            PoolManagerError::Other(msg) => DexError::PoolManagerError(msg),
            PoolManagerError::Database(msg) => DexError::PoolManagerError(format!("Database error: {msg}")),
        }
    }
}





impl From<anyhow::Error> for crate::pool_states::Error {
    fn from(err: anyhow::Error) -> Self {
        crate::pool_states::Error::Internal(err.to_string())
    }
}

// Boilerplate `From` implementations for convenience
impl From<tokio::task::JoinError> for BotError {
    fn from(e: tokio::task::JoinError) -> Self {
        BotError::Infrastructure(e.to_string())
    }
}
impl From<eyre::Report> for BotError {
    fn from(e: eyre::Report) -> Self {
        BotError::Infrastructure(e.to_string())
    }
}

impl From<ethers::providers::ProviderError> for OptimizerError {
    fn from(e: ethers::providers::ProviderError) -> Self {
        OptimizerError::Provider(e.to_string())
    }
}

impl From<ethers::providers::ProviderError> for BlockchainError {
    fn from(e: ethers::providers::ProviderError) -> Self {
        BlockchainError::Provider(e.to_string())
    }
}

// Convert specific provider errors into our custom error types for better context.
impl From<ethers::providers::ProviderError> for ListenerError {
    fn from(e: ethers::providers::ProviderError) -> Self {
        ListenerError::Connection(e.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ListenerError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ListenerError::Channel(format!("Failed to send on internal channel: {}", e))
    }
}

impl From<ethers::abi::Error> for ListenerError {
    fn from(e: ethers::abi::Error) -> Self {
        ListenerError::EventDecoding(e.to_string())
    }
}

impl From<String> for ListenerError {
    fn from(e: String) -> Self {
        ListenerError::Internal(e)
    }
}

// Add missing From implementations for CrossChainError
impl From<GasError> for CrossChainError {
    fn from(e: GasError) -> Self {
        CrossChainError::DependencyError(format!("Gas oracle error: {}", e))
    }
}

impl From<TimeSeriesError> for CrossChainError {
    fn from(e: TimeSeriesError) -> Self {
        CrossChainError::DependencyError(format!("Time series error: {}", e))
    }
}

impl From<PriceError> for CrossChainError {
    fn from(e: PriceError) -> Self {
        CrossChainError::DependencyError(format!("Price oracle error: {}", e))
    }
}

impl From<MempoolError> for CrossChainError {
    fn from(e: MempoolError) -> Self {
        CrossChainError::DependencyError(format!("Mempool error: {}", e))
    }
}

impl From<DexError> for RiskError {
    fn from(e: DexError) -> Self {
        RiskError::PoolManager(format!("DEX error: {}", e))
    }
}

impl From<PriceError> for RiskError {
    fn from(e: PriceError) -> Self {
        RiskError::PriceOracle(format!("Price oracle error: {}", e))
    }
}

impl From<BlockchainError> for RiskError {
    fn from(e: BlockchainError) -> Self {
        RiskError::Other(format!("Blockchain error: {}", e))
    }
}

impl From<GasError> for RiskError {
    fn from(e: GasError) -> Self {
        RiskError::GasOracle(format!("Gas oracle error: {}", e))
    }
}

impl From<PriceError> for BlockchainError {
    fn from(e: PriceError) -> Self {
        BlockchainError::Other(format!("Price oracle error: {}", e))
    }
}

// Add From implementations for chatbot operator error types
impl From<reqwest::Error> for BotError {
    fn from(e: reqwest::Error) -> Self {
        BotError::ApiError(format!("HTTP request failed: {}", e))
    }
}

impl From<serde_json::Error> for BotError {
    fn from(e: serde_json::Error) -> Self {
        BotError::ResponseParse(format!("JSON serialization/deserialization failed: {}", e))
    }
}

// Add missing From implementations for engine.rs compilation errors
impl From<OptimizerError> for BotError {
    fn from(e: OptimizerError) -> Self {
        BotError::Arbitrage(ArbitrageError::OptimizationFailed(e))
    }
}

impl From<crate::pool_states::Error> for BotError {
    fn from(e: crate::pool_states::Error) -> Self {
        BotError::Dex(DexError::PoolState(e.to_string()))
    }
}

// Add From implementation for MEVEngineError from mev_engine::types
impl From<crate::mev_engine::types::MEVEngineError> for BotError {
    fn from(e: crate::mev_engine::types::MEVEngineError) -> Self {
        match e {
            crate::mev_engine::types::MEVEngineError::StrategyError(msg) => BotError::Other(format!("MEV strategy error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::SimulationError(msg) => BotError::Other(format!("MEV simulation error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ValidationError(msg) => BotError::Other(format!("MEV validation error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::PricingError(msg) => BotError::Other(format!("MEV pricing error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ArithmeticError(msg) => BotError::Math(format!("MEV arithmetic error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::Timeout(msg) => BotError::Other(format!("MEV timeout: {}", msg)),
            crate::mev_engine::types::MEVEngineError::CircuitBreakerOpen(msg) => BotError::Other(format!("MEV circuit breaker: {}", msg)),
            crate::mev_engine::types::MEVEngineError::InsufficientBalance(msg) => BotError::Other(format!("MEV insufficient balance: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ResourceExhausted(msg) => BotError::Other(format!("MEV resource exhausted: {}", msg)),
            crate::mev_engine::types::MEVEngineError::PlayNotFound(msg) => BotError::Other(format!("MEV play not found: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ConfigError(msg) => BotError::Configuration(format!("MEV config error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::NetworkError(msg) => BotError::Blockchain(BlockchainError::Network(msg)),
            crate::mev_engine::types::MEVEngineError::MonitoringError(msg) => BotError::Other(format!("MEV monitoring error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ProfitTooLow(msg) => BotError::Other(format!("MEV profit too low: {}", msg)),
            crate::mev_engine::types::MEVEngineError::ExecutionError(msg) => BotError::Other(format!("MEV execution error: {}", msg)),
            crate::mev_engine::types::MEVEngineError::UnknownError(msg) => BotError::Other(format!("MEV unknown error: {}", msg)),
        }
    }
}