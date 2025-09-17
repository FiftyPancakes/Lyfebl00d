// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] calldata tokens,
        uint256[] calldata amounts,
        bytes calldata userData
    ) external;
    
    function getPoolTokens(bytes32 poolId) external view returns (
        address[] memory tokens,
        uint256[] memory balances,
        uint256 lastChangeBlock
    );
} 