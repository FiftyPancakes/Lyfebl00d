# Multi-Chain MEV & Arbitrage Trading System

A sophisticated automated trading system built in Rust for capturing arbitrage opportunities and MEV (Maximal Extractable Value) across multiple blockchain networks. The system features both live trading and historical backtesting capabilities.

## System Architecture

### Core Components

The system is organized into several key modules that work together to discover and execute profitable trading opportunities:

#### 1. MEV Engine (`src/mev_engine/`)
- Orchestrates MEV strategy execution
- Manages active MEV plays and opportunities
- Coordinates with multiple strategies including sandwich, liquidation, and JIT liquidity
- Handles transaction status monitoring and bundle management

#### 2. Alpha Engine (`src/alpha/`)
- Advanced opportunity discovery and strategy coordination
- Market regime detection (Bull/Bear/Sideways/High Volatility)
- Circuit breaker system for risk management
- Statistical analysis utilities (GARCH volatility, Sharpe ratio, correlation analysis)
- Performance metrics tracking

#### 3. Discovery Scanners (`src/strategies/discovery/`)
- **Cyclic Scanner**: Finds circular arbitrage paths using graph analysis
- **Statistical Scanner**: Identifies statistical arbitrage using z-scores and cointegration
- **Cross-Chain Scanner**: Discovers cross-chain arbitrage opportunities via bridges
- **Market Making Scanner**: Identifies market making opportunities

#### 4. Trading Strategies (`src/strategies/`)
- **Arbitrage**: Standard DEX arbitrage execution
- **Sandwich**: Front-run and back-run detection and execution
- **Liquidation**: Monitors and executes liquidation opportunities
- **JIT Liquidity**: Just-in-time liquidity provision

#### 5. Blockchain Infrastructure (`src/blockchain.rs`)
- Multi-chain blockchain interaction layer
- Manages connections to Ethereum, Polygon, Arbitrum, Optimism, and Solana
- Handles RPC endpoints, WebSocket connections, and failover

#### 6. Path Finding (`src/path/`)
- A* pathfinding algorithm for optimal trading routes
- Graph-based pool network analysis
- Multi-hop route optimization
- Circular arbitrage detection

#### 7. Universal Arbitrage Router Integration (`src/uar_connector.rs`)
- Interfaces with the UAR smart contract (`contracts/uar.sol`)
- Enables atomic multi-step arbitrage execution
- Flash loan integration for capital efficiency
- Supports Aave and Balancer flash loans

#### 8. Risk Management (`src/risk.rs`)
- Multi-factor risk assessment system
- Evaluates volatility, liquidity, slippage, MEV competition, and gas costs
- Dynamic risk score calculation
- Position and exposure limits

#### 9. Gas Oracle (`src/gas_oracle/`)
- Live gas price provider for real-time trading
- Historical gas provider for backtesting
- Protocol-specific gas estimation

#### 10. Price Oracle (`src/price_oracle/`)
- Token price aggregation from multiple sources
- Historical price data for backtesting
- Liquidity-weighted price calculations

#### 11. Pool Management (`src/pool_manager.rs`)
- Pool state tracking and caching
- Multi-protocol support (Uniswap V2/V3, Sushiswap, Curve, Balancer)
- Real-time pool updates via event listeners

#### 12. Transaction Optimization (`src/transaction_optimizer.rs`)
- MEV protection through Flashbots integration
- Gas optimization strategies
- Transaction bundling and submission

#### 13. Mempool Monitoring (`src/mempool.rs`)
- Real-time pending transaction analysis
- MEV opportunity detection from mempool
- Transaction classification and filtering

#### 14. Backtesting Framework (`src/backtest.rs`)
- Historical simulation engine
- Competition simulation with multiple bot profiles
- MEV attack simulation
- Multiple discovery strategies (proactive, reactive, cross-DEX, hot token)

### Data Pipeline

#### Data Collection Tools (`src/bin/`)
- `swap.rs`: Historical swap event collection
- `prices.rs`: Price data aggregation  
- `cg_pools.rs`: Pool data from CoinGecko
- `coins.rs`: Token metadata management
- `dexes.rs`: DEX protocol data
- `trades.rs`: Trade data collection

#### State Management (`src/state.rs`)
- Persistent state storage
- Database integration for historical data
- Cache management

### Smart Contracts

#### Universal Arbitrage Router (`contracts/uar.sol`)
The UAR contract provides:
- Atomic multi-step execution
- Flash loan callbacks (Aave V3, Balancer)
- Trusted target management
- Owner-controlled access
- Action types: SWAP, WRAP_NATIVE, UNWRAP_NATIVE, TRANSFER_PROFIT

### Multi-Chain Support
- **Ethereum Mainnet**
- **Layer 2s**: Polygon, Arbitrum, Optimism
- **Solana**
- **Cross-chain**: Bridge protocol integrations

### Configuration System (`src/config.rs`)

Modular configuration supporting:
- Multi-chain settings with RPC endpoints
- Strategy-specific parameters
- Risk management thresholds
- Gas estimation settings
- Backtesting parameters
- MEV engine configuration
- Cross-chain bridge settings

### Execution Modes

1. **Live Trading**: Real-time opportunity discovery and execution
2. **Backtesting**: Historical simulation with configurable parameters
3. **Paper Trading**: Live monitoring without actual execution

### Supported Protocols

- **DEXs**: Uniswap V2/V3/V4, Sushiswap, PancakeSwap, Curve, Balancer
- **Lending**: Aave, Compound (for liquidations)
- **Bridges**: Cross-chain bridge integrations
- **Flash Loans**: Aave V3, Balancer



## Technical Features

- **Language**: Rust for high performance and safety
- **Async/Await**: Tokio-based asynchronous runtime
- **Parallelization**: Multi-threaded opportunity discovery
- **Caching**: Moka cache for performance optimization
- **Error Handling**: Comprehensive error types and recovery
- **Testing**: Unit tests and integration tests throughout
- **Circuit Breaker**: Automatic shutdown on consecutive failures with exponential backoff
- **Risk Scoring**: Multi-factor assessment (volatility, liquidity, slippage, MEV, gas)
- **MEV Protection**: Flashbots integration for private transaction submission

## Project Structure

```
rust/
├── src/
│   ├── alpha/              # Alpha engine with circuit breaker
│   ├── strategies/         # MEV strategies and discovery scanners
│   ├── mev_engine/         # MEV orchestration
│   ├── path/               # Pathfinding algorithms
│   ├── gas_oracle/         # Gas price management
│   ├── price_oracle/       # Price data providers
│   ├── execution/          # Transaction building
│   ├── bin/                # Data collection tools
│   └── ...                 # Additional modules
├── contracts/              # Solidity smart contracts
├── config/                 # Configuration files
└── tests/                  # Test suites
```

## 🔌 Integration Points

### External APIs
- **1inch API**: Quote aggregation
- **Chainlink**: Price feeds
- **Bridge Protocols**: Cross-chain transfers
- **MEV Protection Services**: Flashbots, private relays

### Database Integration
- **PostgreSQL**: Primary data storage
- **Redis**: Caching layer
- **Time-series Data**: Historical analysis storage

## 🚀 Getting Started

### Prerequisites
- Rust 1.70+
- PostgreSQL 13+
- Node.js 16+ (for some tooling)
- Access to blockchain RPC endpoints

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd rust

# Install dependencies
cargo build --release

# Set up configuration
cp -r config/example config/
# Edit config files with your settings

# Set environment variables
export ARBITRAGE_CONFIG_DIR=./config
export ONEINCH_API_KEY=your_api_key
export DATABASE_URL=postgresql://user:pass@localhost/arbitrage

# Run live trading
cargo run --release -- --config ./config

# Run backtesting
# Edit config/main.json to set mode: "Backtest"
cargo run --release -- --config ./config
```

### Data Ingestion Setup
```bash
# Set up database schema
psql $DATABASE_URL < sql/database_schema.sql

# Run data ingestion tools
cargo run --bin swap -- --config ./config
cargo run --bin cg_pools -- --config ./config
cargo run --bin prices -- --config ./config
```

## 📚 Module Documentation

### Core Modules
- **`mev_engine.rs`**: Main MEV orchestration and strategy execution
- **`alpha/engine.rs`**: Alpha engine for strategy coordination and discovery
- **`listen.rs`**: Real-time blockchain event listeners
- **`path.rs`**: A* pathfinding for optimal routes
- **`pool_manager.rs`**: Pool state management and discovery
- **`uar_connector.rs`**: Integration with Universal Arbitrage Router contract

### Discovery Modules
- **`strategies/discovery/cyclic_scanner.rs`**: Cyclic arbitrage opportunity detection
- **`strategies/discovery/statistical_scanner.rs`**: Statistical arbitrage based on z-scores
- **`strategies/discovery/cross_chain_scanner.rs`**: Cross-chain arbitrage via bridges
- **`strategies/discovery/market_making_scanner.rs`**: Market making opportunities

### MEV Strategy Modules
- **`strategies/arbitrage.rs`**: Standard arbitrage execution
- **`strategies/sandwich.rs`**: Sandwich attack implementation
- **`strategies/liquidation.rs`**: Liquidation opportunity execution
- **`strategies/jit_liquidity.rs`**: JIT liquidity provision

### Infrastructure Modules
- **`blockchain.rs`**: Multi-chain blockchain interaction
- **`gas_oracle/`**: Gas price estimation and optimization
- **`price_oracle/`**: Token price aggregation
- **`transaction_optimizer.rs`**: Transaction optimization and MEV protection
- **`risk.rs`**: Risk assessment and management

### Data Modules
- **`swap_loader.rs`**: Historical swap event loading
- **`state.rs`**: State management and persistence
- **`bin/`**: Data collection and management tools

## 🔒 Security Features

- **Private Key Management**: Secure wallet management
- **Rate Limiting**: RPC endpoint protection
- **Error Handling**: Comprehensive error recovery
- **Circuit Breakers**: Automatic failure protection
- **Audit Logging**: Complete transaction audit trail

## Dependencies

The system uses various Rust crates including:
- `ethers`: Ethereum interaction
- `tokio`: Async runtime
- `axum`: HTTP server framework
- `sqlx`: Database operations
- `moka`: Caching
- Various utility and cryptographic libraries

## Configuration

Configuration is managed through JSON files in the `config/` directory:
- Chain configurations with RPC endpoints
- Strategy parameters
- Risk thresholds
- Gas estimates
- Backtesting settings

## Testing

The system includes comprehensive testing:
- Unit tests for individual components
- Integration tests for system workflows
- Backtesting framework for strategy validation
- Fuzz testing for edge cases

---

This is a complex, production-grade trading system designed for sophisticated users who understand the risks involved in automated trading and MEV extraction. 