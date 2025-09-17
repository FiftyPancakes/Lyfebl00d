// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

// Import interfaces for clarity, even if using mock implementations
// Use specific import paths if mocks are in subdirectories
import {IWETH} from "./interfaces/IWETH.sol";
import {IMockUniswapV2Factory} from "../contracts/interfaces/IMockUniswapV2Factory.sol";
import {IMockERC20} from "./interfaces/IMockERC20.sol";

// --- Minimal MockUniswapV2Pair contract for factory deployment ---
contract MockUniswapV2Pair {
    address public factory;
    address public token0;
    address public token1;
    uint112 private _reserve0;
    uint112 private _reserve1;
    uint32 private _blockTimestampLast;
    bool public initialized;

    event DebugMint(address indexed sender, uint balance0, uint balance1, uint112 reserve0, uint112 reserve1);
    event DebugReserves(uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);

    constructor() {}

    function initialize(address _factory, address _token0, address _token1) external {
        require(!initialized, "Already initialized");
        factory = _factory;
        token0 = _token0;
        token1 = _token1;
        initialized = true;
    }

    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast) {
        return (_reserve0, _reserve1, _blockTimestampLast);
    }

    function mint(address to) external returns (uint liquidity) {
        // Get actual balances of token0 and token1
        uint balance0 = IMockERC20(token0).balanceOf(address(this));
        uint balance1 = IMockERC20(token1).balanceOf(address(this));
        require(balance0 > 0 && balance1 > 0, "No liquidity provided");

        _reserve0 = uint112(balance0);
        _reserve1 = uint112(balance1);
        _blockTimestampLast = uint32(block.timestamp);
        liquidity = balance0 < balance1 ? balance0 : balance1; // Simplified

        emit DebugMint(msg.sender, balance0, balance1, _reserve0, _reserve1);
        emit DebugReserves(_reserve0, _reserve1, _blockTimestampLast);
    }

    // A private view function for internal updates
    function _update(uint balance0, uint balance1, uint112 reserve0, uint112 reserve1) private {
        // This check is important to prevent overflow when casting to uint112
        require(balance0 <= type(uint112).max && balance1 <= type(uint112).max, 'MockPair: OVERFLOW');
        _reserve0 = uint112(balance0);
        _reserve1 = uint112(balance1);
        _blockTimestampLast = uint32(block.timestamp);
    }

    function swap(uint amount0Out, uint amount1Out, address to, bytes calldata data) external {
        require(amount0Out > 0 || amount1Out > 0, 'MockPair: INSUFFICIENT_OUTPUT_AMOUNT');
        // CORRECTED REQUIRE: Check each reserve individually.
        require(amount0Out <= _reserve0 && amount1Out <= _reserve1, 'MockPair: INSUFFICIENT_LIQUIDITY');

        // Transfer tokens OUT
        if (amount0Out > 0) IMockERC20(token0).transfer(to, amount0Out);
        if (amount1Out > 0) IMockERC20(token1).transfer(to, amount1Out);

        // After transfers, update reserves based on the new balances. The router is responsible
        // for transferring the input tokens to this contract BEFORE calling swap.
        uint balance0 = IMockERC20(token0).balanceOf(address(this));
        uint balance1 = IMockERC20(token1).balanceOf(address(this));

        _update(balance0, balance1, _reserve0, _reserve1);
    }
}

/**
 * @title Mock Uniswap V2 Router 02 (Comprehensive Update)
 * @notice A revised mock implementation of a Uniswap V2 Router, suitable for testing environments.
 * @dev This version corrects issues found in the previous mock, particularly in liquidity provision
 *      and swap execution logic. It aims to mimic the core external functions and interactions
 *      of a real V2 router but relies on simplified internal logic and assumes the provided
 *      Mock Uniswap V2 Pair contract has a standard interface (`getReserves`, `mint`, `swap`).
 *      It does NOT implement safety features like reentrancy guards or advanced pathfinding.
 *      Intended solely for integration testing in controlled environments like Anvil/Hardhat.
 */
contract MockUniswapV2Router02 {
    address public immutable factory;
    address public immutable WETH; // Use immutable for addresses set only in constructor

    /**
     * @notice Emitted when liquidity is added to a pair.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @param liquidityMinted Amount of LP tokens minted.
     * @param amountA Amount of tokenA deposited.
     * @param amountB Amount of tokenB deposited.
     */
    event LiquidityAdded(
        address indexed tokenA,
        address indexed tokenB,
        uint256 liquidityMinted,
        uint256 amountA,
        uint256 amountB
    );

    /**
     * @notice Emitted when tokens are swapped.
     * @param sender Address initiating the swap (original msg.sender).
     * @param path The sequence of token addresses in the swap path.
     * @param amounts The amounts corresponding to each token in the path (input amount first, then outputs).
     * @param to The final recipient of the output tokens.
     */
    event TokensSwapped(
        address indexed sender,
        address[] path,
        uint[] amounts,
        address indexed to
    );


    /**
     * @notice Sets the factory and WETH addresses upon deployment.
     * @param _factory The address of the Uniswap V2 Factory contract.
     * @param _WETH The address of the Wrapped Ether (WETH) contract.
     */
    constructor(address _factory, address _WETH) {
        require(_factory != address(0), "MockRouter: ZERO_FACTORY_ADDRESS");
        require(_WETH != address(0), "MockRouter: ZERO_WETH_ADDRESS");
        factory = _factory;
        WETH = _WETH;
    }

    /**
     * @notice Modifier to ensure the transaction deadline has not passed.
     * @param deadline The Unix timestamp after which the transaction should revert.
     */
    modifier ensure(uint deadline) {
        require(deadline >= block.timestamp, "MockRouter: EXPIRED");
        _;
    }

    // ==========================================================================================
    //                                Liquidity Functions
    // ==========================================================================================

    /**
     * @notice Adds liquidity to an ERC20-ERC20 pair.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @param amountADesired The desired amount of tokenA to add.
     * @param amountBDesired The desired amount of tokenB to add.
     * @param amountAMin The minimum acceptable amount of tokenA to add.
     * @param amountBMin The minimum acceptable amount of tokenB to add.
     * @param to The address that will receive the liquidity pool (LP) tokens.
     * @param deadline Timestamp after which the transaction will revert.
     * @return amountA The actual amount of tokenA added.
     * @return amountB The actual amount of tokenB added.
     * @return liquidity The amount of LP tokens minted.
     */
    function addLiquidity(
        address tokenA,
        address tokenB,
        uint amountADesired,
        uint amountBDesired,
        uint amountAMin,
        uint amountBMin,
        address to,
        uint deadline
    ) external ensure(deadline) returns (uint amountA, uint amountB, uint liquidity) {
        // Internal function handles pair creation, amount calculation (simplified), transfers, and minting.
        (amountA, amountB) = _addLiquidity(tokenA, tokenB, amountADesired, amountBDesired, amountAMin, amountBMin);

        address pair = pairFor(factory, tokenA, tokenB);
        require(pair != address(0), "MockRouter: PAIR_NOT_FOUND_ADD_LIQ"); // Ensure pair exists

        // Transfer tokens from the sender to the pair address.
        // Note: Requires sender to have approved this router contract beforehand.
        IMockERC20(tokenA).transferFrom(msg.sender, pair, amountA);
        IMockERC20(tokenB).transferFrom(msg.sender, pair, amountB);

        // Call the pair's mint function. The mock pair needs to implement this.
        // We assume the pair contract calculates and returns the liquidity amount.
        liquidity = IMockUniswapV2Pair(pair).mint(to);
        require(liquidity > 0, "MockRouter: ZERO_LIQUIDITY_MINTED"); // Basic check

        emit LiquidityAdded(tokenA, tokenB, liquidity, amountA, amountB);
    }

     /**
     * @notice Adds liquidity to an ERC20-ETH pair. ETH is provided via msg.value.
     * @param token Address of the ERC20 token.
     * @param amountTokenDesired The desired amount of the ERC20 token to add.
     * @param amountTokenMin The minimum acceptable amount of the ERC20 token to add.
     * @param amountETHMin The minimum acceptable amount of ETH to add.
     * @param to The address that will receive the LP tokens.
     * @param deadline Timestamp after which the transaction will revert.
     * @return amountToken The actual amount of the ERC20 token added.
     * @return amountETH The actual amount of ETH added.
     * @return liquidity The amount of LP tokens minted.
     */
    function addLiquidityETH(
        address token,
        uint amountTokenDesired,
        uint amountTokenMin,
        uint amountETHMin,
        address to,
        uint deadline
    ) external payable ensure(deadline) returns (uint amountToken, uint amountETH, uint liquidity) {
        // Internal function handles pair creation, amount calculation, transfers, and minting.
        (amountToken, amountETH) = _addLiquidity(token, WETH, amountTokenDesired, msg.value, amountTokenMin, amountETHMin);

        address pair = pairFor(factory, token, WETH);
        require(pair != address(0), "MockRouter: PAIR_NOT_FOUND_ADD_LIQ_ETH"); // Ensure pair exists

        // Transfer ERC20 token from sender to pair.
        IMockERC20(token).transferFrom(msg.sender, pair, amountToken);

        // Wrap the required amount of ETH and transfer WETH to the pair.
        if (amountETH > 0) { // Only deposit if needed
            IWETH(WETH).deposit{value: amountETH}();
            // Should always succeed if deposit succeeded and router has WETH
            require(IWETH(WETH).transfer(pair, amountETH), "MockRouter: WETH_TRANSFER_FAILED");
        }

        // Call the pair's mint function.
        liquidity = IMockUniswapV2Pair(pair).mint(to);
        require(liquidity > 0, "MockRouter: ZERO_LIQUIDITY_MINTED");

        emit LiquidityAdded(token, WETH, liquidity, amountToken, amountETH);

        // Refund any excess ETH sent if msg.value was more than the required amountETH
        if (msg.value > amountETH) {
            payable(msg.sender).transfer(msg.value - amountETH);
        }
    }

    /**
     * @dev Internal logic for adding liquidity. Calculates amounts and gets pair address.
     *      This version uses the standard Uniswap V2 formula for determining optimal amounts.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @param amountADesired Desired amount of tokenA.
     * @param amountBDesired Desired amount of tokenB.
     * @param amountAMin Minimum acceptable amount of tokenA.
     * @param amountBMin Minimum acceptable amount of tokenB.
     * @return amountA Actual amount of tokenA to add.
     * @return amountB Actual amount of tokenB to add.
     */
    function _addLiquidity(
        address tokenA,
        address tokenB,
        uint amountADesired,
        uint amountBDesired,
        uint amountAMin,
        uint amountBMin
    ) internal returns (uint amountA, uint amountB) {
        // Create pair if it doesn't exist
        address pair = IMockUniswapV2Factory(factory).getPair(tokenA, tokenB);
        if (pair == address(0)) {
            pair = IMockUniswapV2Factory(factory).createPair(tokenA, tokenB);
        }

        (uint reserveA, uint reserveB) = getReserves(factory, tokenA, tokenB);

        if (reserveA == 0 && reserveB == 0) {
            // First liquidity provider sets the ratio
            amountA = amountADesired;
            amountB = amountBDesired;
        } else {
            // Calculate optimal amount B for desired amount A based on current reserves
            uint amountBOptimal = quote(amountADesired, reserveA, reserveB);
            if (amountBOptimal <= amountBDesired) {
                // Use amountADesired and calculated amountBOptimal
                require(amountBOptimal >= amountBMin, "MockRouter: INSUFFICIENT_B_AMOUNT");
                amountA = amountADesired;
                amountB = amountBOptimal;
            } else {
                // Calculate optimal amount A for desired amount B
                uint amountAOptimal = quote(amountBDesired, reserveB, reserveA);
                // Check if calculated optimal A is within desired bounds
                // Note: amountAOptimal <= amountADesired should always be true if quote is monotonic,
                // but checking amountAOptimal >= amountAMin is crucial.
                require(amountAOptimal >= amountAMin, "MockRouter: INSUFFICIENT_A_AMOUNT");
                amountA = amountAOptimal;
                amountB = amountBDesired;
            }
        }
        // Double check against minimums again after calculation, although requires should suffice
        require(amountA >= amountAMin, "MockRouter: FINAL_A_LESS_THAN_MIN");
        require(amountB >= amountBMin, "MockRouter: FINAL_B_LESS_THAN_MIN");
    }

    // ==========================================================================================
    //                                     Swap Functions
    // ==========================================================================================

    /**
     * @notice Swaps an exact amount of input tokens for as many output tokens as possible.
     * @param amountIn The exact amount of input tokens to send.
     * @param amountOutMin The minimum acceptable amount of output tokens.
     * @param path An array of token addresses, `path[0]` is input, `path[length-1]` is output.
     * @param to The address to receive the output tokens.
     * @param deadline Timestamp after which the transaction will revert.
     * @return amounts An array containing the input amount and all subsequent output amounts.
     */
    function swapExactTokensForTokens(
        uint amountIn,
        uint amountOutMin,
        address[] calldata path,
        address to,
        uint deadline
    ) external ensure(deadline) returns (uint[] memory amounts) {
        require(path.length >= 2, "MockRouter: INVALID_PATH");
        // Use internal _swap function, initial transfer comes from msg.sender
        amounts = _swap(amountIn, path, msg.sender, to);
        require(amounts[amounts.length - 1] >= amountOutMin, "MockRouter: INSUFFICIENT_OUTPUT_AMOUNT");
        emit TokensSwapped(msg.sender, path, amounts, to);
    }

    /**
     * @notice Swaps an exact amount of ETH for as many output tokens as possible.
     * @param amountOutMin The minimum acceptable amount of output tokens.
     * @param path An array of token addresses, `path[0]` must be WETH.
     * @param to The address to receive the output tokens.
     * @param deadline Timestamp after which the transaction will revert.
     * @return amounts An array containing the input ETH amount (as WETH) and all output amounts.
     */
    function swapExactETHForTokens(
        uint amountOutMin,
        address[] calldata path,
        address to,
        uint deadline
    ) external payable ensure(deadline) returns (uint[] memory amounts) {
        require(path[0] == WETH, "MockRouter: INVALID_PATH_WETH_IN");
        uint amountIn = msg.value; // Amount of ETH sent
        require(amountIn > 0, "MockRouter: ZERO_ETH_INPUT");
        IWETH(WETH).deposit{value: amountIn}();
        // Transfer the deposited WETH to the first pair *before* swapping
        address firstPair = pairFor(factory, path[0], path[1]);
        require(firstPair != address(0), "MockRouter: FIRST_PAIR_NOT_FOUND_ETH_IN");
        require(IWETH(WETH).transfer(firstPair, amountIn), "MockRouter: WETH_TRANSFER_FAILED");
        // Use internal _swap, starting amount is ETH value (as WETH), initial tokens come from firstPair
        amounts = _swap(amountIn, path, firstPair, to);
        require(amounts[amounts.length - 1] >= amountOutMin, "MockRouter: INSUFFICIENT_OUTPUT_AMOUNT");
        emit TokensSwapped(msg.sender, path, amounts, to); // Log original sender
    }

     /**
     * @notice Swaps an exact amount of input tokens for as much ETH as possible.
     * @param amountIn The exact amount of input tokens to send.
     * @param amountOutMin The minimum acceptable amount of ETH (as WETH).
     * @param path An array of token addresses, `path[length-1]` must be WETH.
     * @param to The address to receive the output ETH.
     * @param deadline Timestamp after which the transaction will revert.
     * @return amounts An array containing the input amount and all subsequent output amounts (incl. WETH).
     */
    function swapExactTokensForETH(
        uint amountIn,
        uint amountOutMin,
        address[] calldata path,
        address to,
        uint deadline
    ) external ensure(deadline) returns (uint[] memory amounts) {
        require(path[path.length - 1] == WETH, "MockRouter: INVALID_PATH_WETH_OUT");
        // Use internal _swap function, receive WETH to this router contract
        amounts = _swap(amountIn, path, msg.sender, address(this));
        uint amountWETH = amounts[amounts.length - 1]; // Get the final WETH amount received by router
        require(amountWETH >= amountOutMin, "MockRouter: INSUFFICIENT_OUTPUT_AMOUNT");
        IWETH(WETH).withdraw(amountWETH); // Withdraw WETH to ETH
        payable(to).transfer(amountWETH); // Transfer ETH to final recipient
        emit TokensSwapped(msg.sender, path, amounts, to);
    }

    /**
     * @dev Internal swap logic. Handles transfers between pairs and calls pair swap.
     * @param amountInInitial The initial amount of the first token in the path.
     * @param path The swap path.
     * @param initialSender The address providing the initial input tokens (msg.sender or first pair for ETH swaps).
     * @param finalRecipient The final address receiving the output tokens.
     * @return amounts Array of amounts for each token in the path.
     */
    function _swap(
        uint amountInInitial,
        address[] calldata path,
        address initialSender, // Could be msg.sender or the first pair (for ETH swaps)
        address finalRecipient
    ) internal returns (uint[] memory amounts) {
        amounts = new uint[](path.length);
        amounts[0] = amountInInitial;

        // --- CRITICAL FIX START ---
        // The initial transfer from the user happens only ONCE, before the loop.
        IMockERC20(path[0]).transferFrom(initialSender, pairFor(factory, path[0], path[1]), amounts[0]);
        // --- CRITICAL FIX END ---

        for (uint i = 0; i < path.length - 1; i++) {
            (address tokenIn, address tokenOut) = (path[i], path[i+1]);
            address pairAddress = pairFor(factory, tokenIn, tokenOut);
            
            // Calculate output for this hop
            (uint reserveIn, uint reserveOut) = getReserves(factory, tokenIn, tokenOut);
            // The input for this hop is the output from the previous one (or the initial amount for the first hop)
            uint amountInputForHop = amounts[i];
            uint amountOutputForHop = getAmountOut(amountInputForHop, reserveIn, reserveOut);
            amounts[i+1] = amountOutputForHop;

            // Determine the recipient for this hop's output
            address recipientForHop = (i < path.length - 2) ? pairFor(factory, tokenOut, path[i+2]) : finalRecipient;
            
            // Prepare amounts for the pair's swap function
            (address token0,) = sortTokens(tokenIn, tokenOut);
            (uint amount0Out, uint amount1Out) = (tokenIn == token0) ? (uint(0), amountOutputForHop) : (amountOutputForHop, uint(0));

            // Call swap. The pair will transfer `amountOutputForHop` directly to `recipientForHop`.
            // The input tokens are already in `pairAddress`.
            IMockUniswapV2Pair(pairAddress).swap(amount0Out, amount1Out, recipientForHop, bytes(""));
        }
    }

    // ==========================================================================================
    //                                     Helper Functions
    // ==========================================================================================

    /**
     * @notice Sorts two token addresses. Reverts if identical or zero.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @return token0 The token with the lower address.
     * @return token1 The token with the higher address.
     */
    function sortTokens(address tokenA, address tokenB) internal pure returns (address token0, address token1) {
        require(tokenA != tokenB, "MockRouter: IDENTICAL_ADDRESSES");
        (token0, token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        require(token0 != address(0), "MockRouter: ZERO_ADDRESS");
    }

    /**
     * @notice Calculates the pair address for two tokens using the factory.
     * @param _factory Address of the factory contract.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @return pair The address of the liquidity pair contract. Returns address(0) if pair doesn't exist.
     */
    function pairFor(address _factory, address tokenA, address tokenB) internal view returns (address pair) {
        pair = IMockUniswapV2Factory(_factory).getPair(tokenA, tokenB);
    }

    /**
     * @notice Retrieves the current reserves for a given pair, ordered according to standard V2 convention.
     * @param _factory Address of the factory contract.
     * @param tokenA Address of the first token.
     * @param tokenB Address of the second token.
     * @return reserveA Reserve amount corresponding to the token with the lower address (token0).
     * @return reserveB Reserve amount corresponding to the token with the higher address (token1).
     */
    function getReserves(address _factory, address tokenA, address tokenB) internal view returns (uint reserveA, uint reserveB) {
        (address token0,) = sortTokens(tokenA, tokenB); // Ensure order
        address pair = pairFor(_factory, tokenA, tokenB);
        if (pair == address(0)) return (0, 0); // Pair doesn't exist
        (uint112 reserve0_112, uint112 reserve1_112,) = IMockUniswapV2Pair(pair).getReserves();
        (reserveA, reserveB) = tokenA == token0 ? (reserve0_112, reserve1_112) : (reserve1_112, reserve0_112);
    }

    /**
     * @notice Calculates the required amount of one token given an amount of the other and pair reserves.
     *         Used for calculating optimal liquidity addition amounts.
     * @param amountA Amount of tokenA.
     * @param reserveA Reserve of tokenA in the pair.
     * @param reserveB Reserve of tokenB in the pair.
     * @return amountB Required amount of tokenB.
     */
    function quote(uint amountA, uint reserveA, uint reserveB) internal pure returns (uint amountB) {
        require(amountA > 0, "MockRouter: INSUFFICIENT_AMOUNT_QUOTE");
        require(reserveA > 0 && reserveB > 0, "MockRouter: INSUFFICIENT_LIQUIDITY_QUOTE");
        amountB = amountA * reserveB / reserveA;
    }

    /**
     * @notice Calculates the output amount for a given input amount and pair reserves. Applies 0.3% fee.
     * @param amountIn Input amount of the trade.
     * @param reserveIn Reserve of the input token.
     * @param reserveOut Reserve of the output token.
     * @return amountOut Output amount after the swap.
     */
    function getAmountOut(uint amountIn, uint reserveIn, uint reserveOut) internal pure returns (uint amountOut) {
        require(amountIn > 0, "MockRouter: INSUFFICIENT_INPUT_AMOUNT");
        require(reserveIn > 0 && reserveOut > 0, "MockRouter: INSUFFICIENT_LIQUIDITY");
        uint amountInWithFee = amountIn * 997; // Apply 0.3% fee (1000 - 3)
        uint numerator = amountInWithFee * reserveOut;
        uint denominator = reserveIn * 1000 + amountInWithFee;
        require(denominator > 0, "MockRouter: ZERO_DENOMINATOR_OUT"); // Should not happen if reserves > 0
        amountOut = numerator / denominator;
    }

    /**
     * @notice Calculates the input amount needed for a desired output amount and pair reserves. Applies 0.3% fee.
     * @param amountOut Desired output amount.
     * @param reserveIn Reserve of the input token.
     * @param reserveOut Reserve of the output token.
     * @return amountIn Required input amount.
     */
    function getAmountIn(uint amountOut, uint reserveIn, uint reserveOut) internal pure returns (uint amountIn) {
        require(amountOut > 0, "MockRouter: INSUFFICIENT_OUTPUT_AMOUNT");
        require(reserveIn > 0 && reserveOut > 0, "MockRouter: INSUFFICIENT_LIQUIDITY");
        require(reserveOut > amountOut, "MockRouter: OUTPUT_EXCEEDS_RESERVES"); // Cannot get more than exists
        uint numerator = reserveIn * amountOut * 1000;
        uint denominator = (reserveOut - amountOut) * 997; // Apply 0.3% fee adjustment
        require(denominator > 0, "MockRouter: ZERO_DENOMINATOR_IN");
        amountIn = (numerator / denominator) + 1; // Add 1 for rounding up
    }

    /**
     * @notice Calculates the integer square root of a number using Babylonian method.
     * @param y The number to find the square root of.
     * @return z The integer square root.
     */
    function sqrt(uint y) internal pure returns (uint z) {
        if (y > 3) {
            z = y;
            uint x = y / 2 + 1;
            while (x < z) {
                z = x;
                x = (y / x + x) / 2;
            }
        } else if (y != 0) {
            z = 1;
        }
        // else z = 0 (default)
    }

    // --- Receive Function ---
    // Receive ETH when swapping tokens *for* ETH via this router
    receive() external payable {}
}

/// @title IMockUniswapV2Pair
/// @notice Minimal interface for the mock Uniswap V2 Pair used in tests.
interface IMockUniswapV2Pair {
    function factory() external view returns (address);
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function mint(address to) external returns (uint liquidity);
    function swap(uint amount0Out, uint amount1Out, address to, bytes calldata data) external;
    function token0() external view returns (address);
    function token1() external view returns (address);
}

// --- Interfaces (Make sure these exist or are imported) ---