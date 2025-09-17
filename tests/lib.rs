// Integration tests for the rust crate
// This file makes all the test modules in the tests/ directory discoverable by Cargo

// Re-export the main crate for testing
pub use rust;

// Common test utilities
pub mod common;

// Test modules
pub mod test_arbitrage_fix_complete;
pub mod test_backtest_integration;
pub mod test_backtest_modules;
pub mod test_db_basic_validation;
pub mod test_db_validation;
pub mod test_fuzz;
pub mod test_gas_configuration;
pub mod test_integration;
pub mod test_mev_strategies;
pub mod test_path_block_building;
pub mod test_protocol_coverage;
pub mod test_security_improvements;
pub mod test_swap_data_flow;

// Re-export common types for easier access in test files
pub use common::{TestHarness, TestMode, DeployedToken, TokenType, ContractWrapper};
