//! Compile-time contract bindings for MEV Engine
//!
//! This module contains type-safe contract bindings generated at compile time
//! to replace runtime HumanReadableParser usage.

use ethers::contract::abigen;

// Oracle contract bindings
abigen!(
    OracleContract,
    "/Users/ichevako/Desktop/rust/src/contracts/oracle_abi.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

// Flash loan contract bindings
abigen!(
    FlashLoanContract,
    "/Users/ichevako/Desktop/rust/src/contracts/flashloan_abi.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

// Type-safe oracle contract interface
pub mod oracle {
    use ethers::types::Address;

    pub struct Oracle {
        pub address: Address,
    }

    impl Oracle {
        pub fn new(address: Address) -> Self {
            Self { address }
        }

        /// Get the latest answer from the oracle (Chainlink-style)
        pub fn latest_answer(&self) -> (Address, String) {
            (self.address, "latestAnswer()".to_string())
        }

        /// Update the price on the oracle
        pub fn update_price(&self) -> (Address, String) {
            (self.address, "updatePrice(uint256)".to_string())
        }
    }
}

/// Type-safe flash loan contract interface
pub mod flashloan {
    use ethers::types::Address;

    pub struct FlashLoan {
        pub address: Address,
    }

    impl FlashLoan {
        pub fn new(address: Address) -> Self {
            Self { address }
        }

        /// Execute a flash loan
        pub fn flash_loan(&self) -> (Address, String) {
            (self.address, "flashLoan(address,address,uint256,bytes)".to_string())
        }
    }
}
