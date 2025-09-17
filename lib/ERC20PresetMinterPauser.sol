// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "lib/openzeppelin-contracts/contracts/token/ERC20/ERC20.sol";
import "lib/openzeppelin-contracts/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import "lib/openzeppelin-contracts/contracts/token/ERC20/extensions/ERC20Pausable.sol";
import "lib/openzeppelin-contracts/contracts/access/AccessControl.sol";

contract ERC20PresetMinterPauser is ERC20, ERC20Burnable, ERC20Pausable, AccessControl {
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    constructor(string memory name, string memory symbol) 
        ERC20(name, symbol) 
    {
        // Grant the default admin role to the deployer
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        // Grant the minter role to the deployer
        _grantRole(MINTER_ROLE, msg.sender);
        // Grant the pauser role to the deployer
        _grantRole(PAUSER_ROLE, msg.sender);
    }

    // Mint new tokens
    function mint(address to, uint256 amount) 
        public 
        onlyRole(MINTER_ROLE) 
    {
        _mint(to, amount);
    }

    // Pause token transfers
    function pause() 
        public 
        onlyRole(PAUSER_ROLE) 
    {
        _pause();
    }

    // Unpause token transfers
    function unpause() 
        public 
        onlyRole(PAUSER_ROLE) 
    {
        _unpause();
    }

    // Override _update to combine ERC20, ERC20Burnable, and ERC20Pausable logic
    function _update(
        address from,
        address to,
        uint256 amount
    ) internal virtual override(ERC20, ERC20Pausable) {
        super._update(from, to, amount);
    }
}