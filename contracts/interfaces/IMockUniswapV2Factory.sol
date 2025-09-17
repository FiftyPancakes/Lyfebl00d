// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/// @title IMockUniswapV2Factory
/// @notice Minimal interface for the mock Uniswap V2 Factory used in tests.
interface IMockUniswapV2Factory {
    /// @notice Returns the pair address for two tokens, or address(0) if not created.
    /// @param tokenA The first token address.
    /// @param tokenB The second token address.
    /// @return pair The address of the pair contract.
    function getPair(address tokenA, address tokenB) external view returns (address pair);

    /// @notice Creates a new pair for two tokens if it does not exist.
    /// @param tokenA The first token address.
    /// @param tokenB The second token address.
    /// @return pair The address of the newly created pair contract.
    function createPair(address tokenA, address tokenB) external returns (address pair);
} 