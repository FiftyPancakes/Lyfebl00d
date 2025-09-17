// SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;

/// @custom:solidity-via-ir true

/* ──────────────────────────────────────────── External Deps ─────────────────────────────────────────── */
import { IERC20 } from "lib/openzeppelin-contracts/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "lib/openzeppelin-contracts/contracts/token/ERC20/utils/SafeERC20.sol";
import { Address } from "lib/openzeppelin-contracts/contracts/utils/Address.sol";
import { IWETH } from "./interfaces/IWETH.sol";

import { IFlashLoanReceiver } from "./interfaces/IFlashLoanReceiver.sol";
import { IPool } from "./interfaces/IPool.sol";

import { IBalancerVault } from "./interfaces/IBalancerVault.sol";

import { ReentrancyGuard } from "lib/openzeppelin-contracts/contracts/utils/ReentrancyGuard.sol";

/* ───────────────────────────────────────────── Contract ────────────────────────────────────────────── */
/**
 * @title  UniversalArbitrageExecutor
 * @notice Stateless executor for atomic, multi‑step arbitrage strategies driven by an off‑chain engine.
 * @dev    Holds no funds between transactions. Owner controls whitelisted targets and may rescue dust.
 */
contract UniversalArbitrageExecutor is IFlashLoanReceiver, ReentrancyGuard {
    using SafeERC20 for IERC20;
    using Address for address;

    uint256 private constant MAX_UINT = type(uint256).max;

    /* ───────────────────────────────────────────── Events ──────────────────────────────────────────── */
    event TrustedTargetUpdated(address indexed target, bool trusted);
    event Executed(uint256 actions, address indexed profitToken);
    event ProfitTransferred(address indexed token, uint256 amount);
    event OwnershipTransferInitiated(address indexed previousOwner, address indexed newOwner);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    /* ───────────────────────────────────────────── Errors ──────────────────────────────────────────── */
    error OnlyOwner();
    error OnlyPendingOwner();
    error UntrustedTarget();
    error ExternalCallFailed();
    error FlashloanRepaymentFailed();
    error UnsupportedCallback(address sender);
    error ProfitTransferFailed();
    error CallbackLocked();
    error InvalidConfig();
    error ETHTransferFailed();

    /* ────────────────────────────────────────── State Variables ───────────────────────────────────── */
    address public owner;
    address public pendingOwner;

    IPool           public immutable AAVE_POOL;
    IBalancerVault  public immutable BALANCER_VAULT;
    address         public immutable WETH;

    mapping(address => bool) private _trustedTarget;
    bool private _inCallback;

    /* ───────────────────────────────────────────── Types ───────────────────────────────────────────── */
    enum ActionType {
        SWAP,
        WRAP_NATIVE,
        UNWRAP_NATIVE,
        TRANSFER_PROFIT
    }

    struct Action {
        ActionType actionType;
        address    target;     // Router / pool to call
        address    tokenIn;    // Address(0) represents native ETH
        uint256    amountIn;   // If == type(uint256).max use full balance
        bytes      callData;   // Pre‑encoded calldata for the target
    }

    /* ─────────────────────────────────────────── Modifiers ────────────────────────────────────────── */
    modifier onlyOwner() {
        if (msg.sender != owner) revert OnlyOwner();
        _;
    }

    modifier noCallback() {
        if (_inCallback) revert CallbackLocked();
        _;
    }

    /* ───────────────────────────────────────── Constructor ────────────────────────────────────────── */
    constructor(address aavePool, address balancerVault, address weth) {
        if (aavePool == address(0) || balancerVault == address(0) || weth == address(0))
            revert InvalidConfig();
        
        owner           = msg.sender;
        AAVE_POOL       = IPool(aavePool);
        BALANCER_VAULT  = IBalancerVault(balancerVault);
        WETH            = weth;
    }

    /* ─────────────────────────────────────── Trusted Target Admin ─────────────────────────────────── */
    function addTrustedTarget(address target) external onlyOwner noCallback {
        if (target.code.length == 0) revert InvalidConfig();
        _trustedTarget[target] = true;
        emit TrustedTargetUpdated(target, true);
    }

    function removeTrustedTarget(address target) external onlyOwner noCallback {
        _trustedTarget[target] = false;
        emit TrustedTargetUpdated(target, false);
    }

    function isTrustedTarget(address target) external view returns (bool) {
        return _trustedTarget[target];
    }

    /* ───────────────────────────────────────── External Execute ───────────────────────────────────── */
    function execute(Action[] calldata actions, address profitToken) external onlyOwner nonReentrant {
        _processActions(actions, profitToken);
        emit Executed(actions.length, profitToken);
    }

    /* ──────────────────────────────── Aave V3 Flash‑loan Callback ─────────────────────────────────── */
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address   initiator,
        bytes calldata params
    )
        external
        override
        nonReentrant
        returns (bool)
    {
        if (msg.sender != address(AAVE_POOL) || initiator != address(this))
            revert UnsupportedCallback(msg.sender);

        (Action[] memory actions, address profitToken) =
            abi.decode(params, (Action[], address));

        _inCallback = true;
        _processActions(actions, profitToken);

        // Approve Aave Pool to pull repayments after callback returns
        unchecked {
            for (uint256 i; i < assets.length; ++i) {
                uint256 repay = amounts[i] + premiums[i];
                IERC20(assets[i]).forceApprove(address(AAVE_POOL), repay);
            }
        }

        // Optional stateless behavior: transfer profit while ensuring Aave can still pull repayments
        // If profitToken matches a borrowed asset, retain exactly the repay amount as reserve
        uint256 reserve = 0;
        unchecked {
            for (uint256 i; i < assets.length; ++i) {
                if (assets[i] == profitToken) {
                    reserve += amounts[i] + premiums[i];
                }
            }
        }
        _transferProfitWithReserve(profitToken, reserve);

        _inCallback = false;

        return true;
    }

    /* ─────────────────────────────── Balancer Flash‑loan Callback ─────────────────────────────────── */
    function receiveFlashLoan(
        IERC20[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    )
        external
        nonReentrant
    {
        if (msg.sender != address(BALANCER_VAULT))
            revert UnsupportedCallback(msg.sender);

        (Action[] memory actions, address profitToken) =
            abi.decode(userData, (Action[], address));

        _inCallback = true;
        _processActions(actions, profitToken);

        unchecked {
            for (uint256 i; i < tokens.length; ++i) {
                uint256 repay = amounts[i] + feeAmounts[i];
                tokens[i].safeTransfer(address(BALANCER_VAULT), repay);
            }
        }

        // After explicit repayment to Balancer, transfer remaining profit (stateless)
        _transferProfit(profitToken);

        _inCallback = false;
    }

    /* ───────────────────────────────────── Internal Action Router ─────────────────────────────────── */
    function _processActions(Action[] memory actions, address profitToken) internal {
        uint256 len = actions.length;
        unchecked {
            for (uint256 i; i < len; ++i) {
                Action memory a = actions[i];
                if (a.actionType == ActionType.SWAP) {
                    _swap(a);
                } else if (a.actionType == ActionType.WRAP_NATIVE) {
                    _wrapNative(a.amountIn);
                } else if (a.actionType == ActionType.UNWRAP_NATIVE) {
                    _unwrapNative(a.amountIn);
                } else if (a.actionType == ActionType.TRANSFER_PROFIT) {
                    if (_inCallback) revert CallbackLocked();
                    _transferProfit(profitToken);
                } else {
                    revert ExternalCallFailed(); // unreachable
                }
            }
        }
    }

    /* ─────────────────────────────────────── Action Handlers ──────────────────────────────────────── */
    function _swap(Action memory a) private {
        if (!_trustedTarget[a.target]) revert UntrustedTarget();

        uint256 amountToUse = a.amountIn == MAX_UINT
            ? _balanceOf(a.tokenIn)
            : a.amountIn;

        if (a.tokenIn == address(0)) {
            (bool ok, bytes memory returndata) = a.target.call{ value: amountToUse }(a.callData);
            if (!ok) {
                assembly {
                    revert(add(returndata, 32), mload(returndata))
                }
            }
            return;
        }

        IERC20 token = IERC20(a.tokenIn);
        // Approve exact spend and revoke after call to avoid dangling allowances
        token.forceApprove(a.target, amountToUse);

        (bool success, bytes memory result) = a.target.call(a.callData);
        if (!success) {
            assembly {
                revert(add(result, 32), mload(result))
            }
        }

        token.forceApprove(a.target, 0);
    }

    function _wrapNative(uint256 amount) private {
        uint256 amt = amount == type(uint256).max ? address(this).balance : amount;
        IWETH(WETH).deposit{ value: amt }();
    }

    function _unwrapNative(uint256 amount) private {
        uint256 amt = amount == type(uint256).max ? IERC20(WETH).balanceOf(address(this)) : amount;
        IWETH(WETH).withdraw(amt);
    }

    function _transferProfit(address profitToken) private {
        if (profitToken == address(0)) {
            uint256 ethBal = address(this).balance;
            if (ethBal == 0) return;
            (bool ok, ) = owner.call{ value: ethBal }("");
            if (!ok) revert ETHTransferFailed();
            emit ProfitTransferred(address(0), ethBal);
            return;
        }
        uint256 bal = IERC20(profitToken).balanceOf(address(this));
        if (bal == 0) return;
        IERC20(profitToken).safeTransfer(owner, bal);
        emit ProfitTransferred(profitToken, bal);
    }

    function _transferProfitWithReserve(address profitToken, uint256 reserve) private {
        // Transfers entire balance of profitToken minus reserve (if any)
        if (profitToken == address(0)) {
            uint256 ethBal = address(this).balance;
            if (ethBal <= reserve) return;
            uint256 amount = ethBal - reserve;
            (bool ok, ) = owner.call{ value: amount }("");
            if (!ok) revert ETHTransferFailed();
            emit ProfitTransferred(address(0), amount);
            return;
        }

        uint256 bal = IERC20(profitToken).balanceOf(address(this));
        if (bal <= reserve) return;
        uint256 sendAmount = bal - reserve;
        IERC20(profitToken).safeTransfer(owner, sendAmount);
        emit ProfitTransferred(profitToken, sendAmount);
    }

    /* ───────────────────────────────────────── Utility Helpers ───────────────────────────────────── */
    function _balanceOf(address token) private view returns (uint256) {
        if (token == address(0)) return address(this).balance;
        return IERC20(token).balanceOf(address(this));
    }

    /* ─────────────────────────────────── Owner Emergency Functions ───────────────────────────────── */
    function withdrawStuckTokens(address token) external onlyOwner noCallback nonReentrant {
        uint256 bal = IERC20(token).balanceOf(address(this));
        IERC20(token).safeTransfer(owner, bal);
    }

    function withdrawETH(uint256 amount) external onlyOwner noCallback nonReentrant {
        (bool ok, ) = owner.call{ value: amount }("");
        if (!ok) revert ETHTransferFailed();
    }

    /* ───────────────────────────────────── Ownership Management ───────────────────────────────────── */
    function transferOwnership(address newOwner) external onlyOwner {
        pendingOwner = newOwner;
        emit OwnershipTransferInitiated(owner, newOwner);
    }

    function acceptOwnership() external {
        if (msg.sender != pendingOwner) revert OnlyPendingOwner();
        address previousOwner = owner;
        owner = pendingOwner;
        pendingOwner = address(0);
        emit OwnershipTransferred(previousOwner, owner);
    }

    function renounceOwnership() external onlyOwner {
        address previousOwner = owner;
        owner = address(0);
        pendingOwner = address(0);
        emit OwnershipTransferred(previousOwner, address(0));
    }

    receive() external payable {}
}
