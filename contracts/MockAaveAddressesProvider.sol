// SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;

contract MockAavePool {
    // Add any mock logic if needed
}

contract MockAaveAddressesProvider {
    address public pool;

    constructor() {
        pool = address(new MockAavePool());
    }

    function getPool() external view returns (address) {
        return pool;
    }
}