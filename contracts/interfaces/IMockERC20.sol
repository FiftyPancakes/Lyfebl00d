// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @title IMockERC20 Interface
 * @notice Basic interface for ERC20 tokens used in mock contracts
 */
interface IMockERC20 {
    /**
     * @notice Approve spender to transfer a specific amount of tokens
     * @param spender Address to approve
     * @param amount Amount to approve
     * @return Success boolean
     */
    function approve(address spender, uint256 amount) external returns (bool);
    
    /**
     * @notice Transfer tokens to a recipient
     * @param recipient Address to receive tokens
     * @param amount Amount to transfer
     * @return Success boolean
     */
    function transfer(address recipient, uint256 amount) external returns (bool);
    
    /**
     * @notice Transfer tokens from one address to another
     * @param sender Source address
     * @param recipient Destination address
     * @param amount Amount to transfer
     * @return Success boolean
     */
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
    
    /**
     * @notice Get balance of an account
     * @param account Address to check balance for
     * @return Balance amount
     */
    function balanceOf(address account) external view returns (uint256);
} 