// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @title IWETH Interface
 * @notice Interface for Wrapped Ether (WETH) contract
 * @dev Standard interface for interacting with WETH in DeFi protocols
 */
interface IWETH {
    /**
     * @notice Deposit ETH and receive WETH
     */
    function deposit() external payable;
    
    /**
     * @notice Withdraw ETH by burning WETH
     * @param wad Amount of WETH to withdraw as ETH
     */
    function withdraw(uint wad) external;
    
    /**
     * @notice Transfer WETH to another address
     * @param to Recipient address
     * @param value Amount to transfer
     * @return Success boolean
     */
    function transfer(address to, uint value) external returns (bool);
    
    /**
     * @notice Approve another address to spend WETH
     * @param guy Address to approve
     * @param wad Amount to approve
     * @return Success boolean
     */
    function approve(address guy, uint wad) external returns (bool);
    
    /**
     * @notice Transfer WETH from one address to another
     * @param src Source address
     * @param dst Destination address
     * @param wad Amount to transfer
     * @return Success boolean
     */
    function transferFrom(address src, address dst, uint wad) external returns (bool);
}