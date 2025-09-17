//! Execution module for building and encoding transaction calldata

pub mod builder;

pub use builder::ExecutionBuilder;
pub use crate::transaction_optimizer::TransactionOptimizerTrait; 