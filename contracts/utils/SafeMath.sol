// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @title SafeMath
 * @dev Math operations with safety checks that revert on error
 * @notice This is a simplified version of OpenZeppelin's SafeMath for Solidity 0.8.0+,
 * which already includes built-in overflow checking. This library is included for
 * compatibility with existing code that expects SafeMath to be available.
 */
library SafeMath {
    /**
     * @dev Returns the addition of two unsigned integers.
     */
    function add(uint256 a, uint256 b) internal pure returns (uint256) {
        return a + b;
    }

    /**
     * @dev Returns the subtraction of two unsigned integers.
     */
    function sub(uint256 a, uint256 b) internal pure returns (uint256) {
        return a - b;
    }

    /**
     * @dev Returns the multiplication of two unsigned integers.
     */
    function mul(uint256 a, uint256 b) internal pure returns (uint256) {
        return a * b;
    }

    /**
     * @dev Returns the division of two unsigned integers.
     */
    function div(uint256 a, uint256 b) internal pure returns (uint256) {
        return a / b;
    }

    /**
     * @dev Returns the remainder of dividing two unsigned integers.
     */
    function mod(uint256 a, uint256 b) internal pure returns (uint256) {
        return a % b;
    }
} 