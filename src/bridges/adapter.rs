//! # Bridge Adapter Interface
//!
//! Defines the universal `BridgeAdapter` trait for interacting with various cross-chain bridges.
//! This ensures that the `CrossChainPathfinder` and `ExecutionEngine` can operate on a standard
//! interface without needing to know the specific implementation details of each bridge protocol.

use crate::{
    errors::CrossChainError,
    types::{BridgeQuote, CrossChainExecutionPlan},
};
use async_trait::async_trait;
use ethers::{
    prelude::*,
    types::{Address, U256, H256},
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Represents a standardized quote from a bridge protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeTxRequest {
    /// The address of the router or contract to call.
    pub to: Address,
    /// The encoded transaction data.
    pub data: Bytes,
    /// The native token value to send with the transaction.
    pub value: U256,
}

/// The `BridgeAdapter` trait provides a standardized interface for interacting with different
/// bridge protocols. Each implementation of this trait will handle the protocol-specific
/// logic for fetching quotes, building transactions, and identifying bridge messages.
#[async_trait]
pub trait BridgeAdapter: Send + Sync + Debug {
    /// Returns the name of the bridge protocol (e.g., "Stargate", "Hop").
    fn name(&self) -> String;

    /// Fetches a quote for a cross-chain transfer from the bridge's API or contracts.
    ///
    /// # Arguments
    /// * `source_chain_id` - The ID of the source chain.
    /// * `dest_chain_id` - The ID of the destination chain.
    /// * `from_token` - The address of the token to be sent.
    /// * `to_token` - The address of the token to be received.
    /// * `amount_in` - The amount of `from_token` to be sent.
    ///
    /// # Returns
    /// A `Result` containing a `BridgeQuote` on success, or a `CrossChainError` on failure.
    async fn fetch_quote(
        &self,
        source_chain_id: u64,
        dest_chain_id: u64,
        from_token: Address,
        to_token: Address,
        amount_in: U256,
    ) -> Result<BridgeQuote, CrossChainError>;

    /// Constructs the raw transaction request for executing the bridge transfer.
    ///
    /// # Arguments
    /// * `plan` - The `CrossChainExecutionPlan` containing all necessary details for the transfer.
    /// * `source_amount` - The final calculated amount to be bridged.
    ///
    /// # Returns
    /// A `Result` containing the encoded `Bytes` for the transaction data, or a `CrossChainError`.
    async fn build_bridge_tx(
        &self,
        plan: &CrossChainExecutionPlan,
        source_amount: U256,
    ) -> Result<BridgeTxRequest, CrossChainError>;

    /// Extracts a unique identifier for the bridge message from the source transaction receipt.
    /// This identifier is crucial for tracking the message's progress and confirming its
    /// arrival on the destination chain.
    ///
    /// # Arguments
    /// * `receipt` - The `TransactionReceipt` from the source chain bridge transaction.
    ///
    /// # Returns
    /// A `Result` containing a unique `H256` identifier, or a `CrossChainError`.
    fn get_message_identifier(&self, receipt: &TransactionReceipt) -> Result<H256, CrossChainError>;

    /// Constructs an event filter for the destination chain to listen for the confirmation
    /// of a specific bridge message. This filter is used by the `listen.rs` module.
    ///
    /// # Arguments
    /// * `plan` - The `CrossChainExecutionPlan` for context about the destination contract.
    /// * `message_identifier` - The unique identifier of the message to listen for.
    ///
    /// # Returns
    /// A `Result` containing an `ethers::Filter`, or a `CrossChainError`.
    fn get_destination_event_filter(
        &self,
        plan: &CrossChainExecutionPlan,
        message_identifier: &H256,
    ) -> Result<Filter, CrossChainError>;

    /// Parses a log from the destination chain to confirm it corresponds to the expected
    /// bridge message completion event.
    ///
    /// # Arguments
    /// * `log` - The `Log` object received from the event stream.
    ///
    /// # Returns
    /// A `Result` containing `()` on successful parse and confirmation, or a `CrossChainError`.
    async fn parse_destination_event(&self, log: &Log) -> Result<(), CrossChainError>;
}