// SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;

import "lib/forge-std/src/Test.sol";
import "lib/forge-std/src/console.sol";
import "../contracts/uar.sol";
import "lib/openzeppelin-contracts/contracts/token/ERC20/IERC20.sol";
import "lib/openzeppelin-contracts/contracts/token/ERC20/ERC20.sol";
import "../contracts/interfaces/IPool.sol";
import "../contracts/interfaces/IFlashLoanReceiver.sol";
import "../contracts/interfaces/IBalancerVault.sol";
import "lib/aave-v3-core/contracts/interfaces/IPoolAddressesProvider.sol";
import "lib/aave-v3-core/contracts/protocol/libraries/types/DataTypes.sol";

/**
 * @title MockERC20
 * @notice Full-featured ERC20 mock for testing
 */
contract MockERC20 is ERC20 {
    uint8 private _decimals;
    mapping(address => mapping(address => uint256)) private _allowances;
    
    constructor(string memory name, string memory symbol, uint8 decimals_) ERC20(name, symbol) {
        _decimals = decimals_;
    }
    
    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
    
    function burn(address from, uint256 amount) external {
        _burn(from, amount);
    }
    
    function decimals() public view override returns (uint8) {
        return _decimals;
    }
    
    function forceApprove(address spender, uint256 amount) external returns (bool) {
        _allowances[msg.sender][spender] = amount;
        _approve(msg.sender, spender, amount);
        emit Approval(msg.sender, spender, amount);
        return true;
    }
}

/**
 * @title MockWETH
 * @notice Complete WETH implementation for testing
 */
contract MockWETH is MockERC20 {
    event Deposit(address indexed dst, uint256 wad);
    event Withdrawal(address indexed src, uint256 wad);
    
    constructor() MockERC20("Wrapped Ether", "WETH", 18) {}
    
    function deposit() public payable {
        _mint(msg.sender, msg.value);
        emit Deposit(msg.sender, msg.value);
    }
    
    function withdraw(uint256 wad) public {
        require(balanceOf(msg.sender) >= wad, "WETH: insufficient balance");
        _burn(msg.sender, wad);
        (bool success,) = msg.sender.call{value: wad}("");
        require(success, "WETH: ETH transfer failed");
        emit Withdrawal(msg.sender, wad);
    }
    
    receive() external payable {
        deposit();
    }
    
    fallback() external payable {
        deposit();
    }
}

/**
 * @title MockAavePool
 * @notice Production-grade mock of Aave V3 Pool
 */
contract MockAavePool {
    mapping(address => uint256) public flashLoanPremiums;
    uint128 private constant _FLASHLOAN_PREMIUM_TOTAL = 9; // 0.09%
    uint128 private constant _FLASHLOAN_PREMIUM_TO_PROTOCOL = 0;
    
    bool public simulateFlashLoanFailure;
    bool public simulateInsufficientRepayment;
    
    event FlashLoanExecuted(
        address indexed receiver,
        address[] assets,
        uint256[] amounts,
        uint256[] premiums
    );
    
    function setSimulateFlashLoanFailure(bool _fail) external {
        simulateFlashLoanFailure = _fail;
    }
    
    function setSimulateInsufficientRepayment(bool _fail) external {
        simulateInsufficientRepayment = _fail;
    }
    
    function flashLoan(
        address receiverAddress,
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata interestRateModes,
        address onBehalfOf,
        bytes calldata params,
        uint16 referralCode
    ) external {
        require(receiverAddress != address(0), "Invalid receiver");
        require(assets.length == amounts.length, "Array length mismatch");
        require(assets.length == interestRateModes.length, "Array length mismatch");
        
        if (simulateFlashLoanFailure) {
            revert("Flash loan execution failed");
        }
        
        uint256[] memory premiums = new uint256[](assets.length);
        
        for (uint256 i = 0; i < assets.length; i++) {
            premiums[i] = (amounts[i] * _FLASHLOAN_PREMIUM_TOTAL) / 10000;
            MockERC20(assets[i]).mint(receiverAddress, amounts[i]);
        }
        
        bool success = IFlashLoanReceiver(receiverAddress).executeOperation(
            assets,
            amounts,
            premiums,
            msg.sender,
            params
        );
        
        require(success, "Flash loan execution failed");
        
        for (uint256 i = 0; i < assets.length; i++) {
            uint256 amountOwing = amounts[i] + premiums[i];
            
            if (simulateInsufficientRepayment) {
                amountOwing = amounts[i] + premiums[i] - 1;
            }
            
            uint256 balance = IERC20(assets[i]).balanceOf(address(this));
            require(balance >= amountOwing, "Insufficient repayment");
            
            MockERC20(assets[i]).burn(address(this), amountOwing);
        }
        
        emit FlashLoanExecuted(receiverAddress, assets, amounts, premiums);
    }
    
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external {
        address[] memory assets = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory modes = new uint256[](1);
        
        assets[0] = asset;
        amounts[0] = amount;
        modes[0] = 0;
        
        this.flashLoan(receiverAddress, assets, amounts, modes, receiverAddress, params, referralCode);
    }
    
    function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external {}
    function withdraw(address asset, uint256 amount, address to) external returns (uint256) { return amount; }
    function borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf) external {}
    function repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf) external returns (uint256) { return amount; }
    function setUserUseReserveAsCollateral(address asset, bool useAsCollateral) external {}
    function liquidationCall(address collateralAsset, address debtAsset, address user, uint256 debtToCover, bool receiveAToken) external {}
    function getUserAccountData(address user) external view returns (uint256, uint256, uint256, uint256, uint256, uint256) {
        return (0, 0, 0, 0, 0, 1e18);
    }
    function getConfiguration(address asset) external view returns (DataTypes.ReserveConfigurationMap memory) {
        return DataTypes.ReserveConfigurationMap(0);
    }
    function getUserConfiguration(address user) external view returns (DataTypes.UserConfigurationMap memory) {
        return DataTypes.UserConfigurationMap(0);
    }
    function getReserveNormalizedIncome(address asset) external view returns (uint256) { return 1e27; }
    function getReserveNormalizedVariableDebt(address asset) external view returns (uint256) { return 1e27; }
    function getReserveData(address asset) external view returns (DataTypes.ReserveData memory) {
        DataTypes.ReserveData memory data;
        return data;
    }
    function getReservesList() external view returns (address[] memory) {
        return new address[](0);
    }
    function getReserveAddressById(uint16 id) external view returns (address) { return address(0); }
    function ADDRESSES_PROVIDER() external view returns (IPoolAddressesProvider) {
        return IPoolAddressesProvider(address(0));
    }
    function supplyWithPermit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode, uint256 deadline, uint8 permitV, bytes32 permitR, bytes32 permitS) external {}
    function repayWithPermit(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf, uint256 deadline, uint8 permitV, bytes32 permitR, bytes32 permitS) external returns (uint256) { return amount; }
    function repayWithATokens(address asset, uint256 amount, uint256 interestRateMode) external returns (uint256) { return amount; }
    function swapBorrowRateMode(address asset, uint256 interestRateMode) external {}
    function rebalanceStableBorrowRate(address asset, address user) external {}
    function setUserEMode(uint8 categoryId) external {}
    function getUserEMode(address user) external view returns (uint256) { return 0; }
    function getEModeCategoryData(uint8 id) external view returns (DataTypes.EModeCategory memory) {
        DataTypes.EModeCategory memory category;
        return category;
    }
    function mintToTreasury(address[] calldata assets) external {}
    function mintUnbacked(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external {}
    function backUnbacked(address asset, uint256 amount, uint256 fee) external returns (uint256) { return amount; }
    function rescueTokens(address token, address to, uint256 amount) external {}
    function deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external {}
    function MAX_STABLE_RATE_BORROW_SIZE_PERCENT() external view returns (uint256) { return 2500; }
    function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128) { return _FLASHLOAN_PREMIUM_TOTAL; }
    function BRIDGE_PROTOCOL_FEE() external view returns (uint256) { return 0; }
    function FLASHLOAN_PREMIUM_TO_PROTOCOL() external view returns (uint128) { return _FLASHLOAN_PREMIUM_TO_PROTOCOL; }
    function MAX_NUMBER_RESERVES() external view returns (uint16) { return 128; }
    function finalizeTransfer(address asset, address from, address to, uint256 amount, uint256 balanceFromBefore, uint256 balanceToBefore) external {}
    function initReserve(address asset, address aTokenAddress, address stableDebtAddress, address variableDebtAddress, address interestRateStrategyAddress) external {}
    function dropReserve(address asset) external {}
    function setReserveInterestRateStrategyAddress(address asset, address rateStrategyAddress) external {}
    function setConfiguration(address asset, DataTypes.ReserveConfigurationMap calldata configuration) external {}
    function updateBridgeProtocolFee(uint256 protocolFee) external {}
    function updateFlashloanPremiums(uint128 flashLoanPremiumTotal, uint128 flashLoanPremiumToProtocol) external {}
    function configureEModeCategory(uint8 id, DataTypes.EModeCategory calldata category) external {}
    function resetIsolationModeTotalDebt(address asset) external {}
}

/**
 * @title MockBalancerVault
 * @notice Production-grade mock of Balancer Vault
 */
contract MockBalancerVault {
    bool public simulateFlashLoanFailure;
    uint256 public constant FLASH_LOAN_FEE_PERCENTAGE = 0; // 0% for testing
    
    event FlashLoanExecuted(
        address indexed recipient,
        IERC20[] tokens,
        uint256[] amounts,
        uint256[] feeAmounts
    );
    
    function setSimulateFlashLoanFailure(bool _fail) external {
        simulateFlashLoanFailure = _fail;
    }
    
    function flashLoan(
        address recipient,
        IERC20[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external {
        require(address(recipient) != address(0), "Invalid recipient");
        require(tokens.length == amounts.length, "Array length mismatch");
        
        if (simulateFlashLoanFailure) {
            revert("Flash loan execution failed");
        }
        
        uint256[] memory feeAmounts = new uint256[](tokens.length);
        
        for (uint256 i = 0; i < tokens.length; i++) {
            feeAmounts[i] = (amounts[i] * FLASH_LOAN_FEE_PERCENTAGE) / 10000;
            MockERC20(address(tokens[i])).mint(address(recipient), amounts[i]);
        }
        
        UniversalArbitrageExecutor(payable(recipient)).receiveFlashLoan(tokens, amounts, feeAmounts, userData);
        
        for (uint256 i = 0; i < tokens.length; i++) {
            uint256 amountOwing = amounts[i] + feeAmounts[i];
            uint256 balance = tokens[i].balanceOf(address(this));
            require(balance >= amountOwing, "Insufficient repayment");
            MockERC20(address(tokens[i])).burn(address(this), amountOwing);
        }
        
        emit FlashLoanExecuted(address(recipient), tokens, amounts, feeAmounts);
    }
    
    function getPoolTokens(bytes32 poolId) external view returns (
        IERC20[] memory tokens,
        uint256[] memory balances,
        uint256 lastChangeBlock
    ) {
        tokens = new IERC20[](0);
        balances = new uint256[](0);
        lastChangeBlock = block.number;
    }
}

/**
 * @title MockDEXRouter
 * @notice Simulates DEX router behavior for swap testing
 */
contract MockDEXRouter {
    uint256 public constant SWAP_RATE = 100; // 100% of input (no slippage by default)
    bool public simulateSwapFailure;
    bool public simulateSlippage;
    uint256 public slippagePercent = 5; // 5% slippage when enabled
    
    event SwapExecuted(
        address indexed tokenIn,
        address indexed tokenOut,
        uint256 amountIn,
        uint256 amountOut
    );
    
    function setSimulateSwapFailure(bool _fail) external {
        simulateSwapFailure = _fail;
    }
    
    function setSimulateSlippage(bool _slippage) external {
        simulateSlippage = _slippage;
    }
    
    function setSlippagePercent(uint256 _percent) external {
        require(_percent <= 100, "Invalid slippage");
        slippagePercent = _percent;
    }
    
    function swap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 minAmountOut,
        address recipient
    ) external payable returns (uint256 amountOut) {
        if (simulateSwapFailure) {
            revert("Swap failed");
        }
        
        if (tokenIn == address(0)) {
            require(msg.value == amountIn, "Incorrect ETH amount");
        } else {
            require(IERC20(tokenIn).transferFrom(msg.sender, address(this), amountIn), "Transfer failed");
        }
        
        if (simulateSlippage) {
            amountOut = (amountIn * (100 - slippagePercent)) / 100;
        } else {
            amountOut = amountIn;
        }
        
        require(amountOut >= minAmountOut, "Slippage exceeded");
        
        if (tokenOut == address(0)) {
            (bool success,) = recipient.call{value: amountOut}("");
            require(success, "ETH transfer failed");
        } else {
            MockERC20(tokenOut).mint(recipient, amountOut);
        }
        
        emit SwapExecuted(tokenIn, tokenOut, amountIn, amountOut);
        
        return amountOut;
    }
    
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts) {
        require(block.timestamp <= deadline, "Expired");
        require(path.length >= 2, "Invalid path");
        
        amounts = new uint256[](path.length);
        amounts[0] = amountIn;
        
        for (uint256 i = 0; i < path.length - 1; i++) {
            amounts[i + 1] = this.swap(path[i], path[i + 1], amounts[i], 0, (i == path.length - 2) ? to : address(this));
        }
        
        require(amounts[amounts.length - 1] >= amountOutMin, "Insufficient output");
        
        return amounts;
    }
    
    function swapExactETHForTokens(
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external payable returns (uint256[] memory amounts) {
        require(block.timestamp <= deadline, "Expired");
        require(path.length >= 2, "Invalid path");
        require(path[0] == address(0), "First token must be ETH");
        
        amounts = new uint256[](path.length);
        amounts[0] = msg.value;
        
        for (uint256 i = 0; i < path.length - 1; i++) {
            amounts[i + 1] = this.swap(path[i], path[i + 1], amounts[i], 0, (i == path.length - 2) ? to : address(this));
        }
        
        require(amounts[amounts.length - 1] >= amountOutMin, "Insufficient output");
        
        return amounts;
    }
    
    receive() external payable {}
}

/**
 * @title UniversalArbitrageExecutorTest
 * @notice Comprehensive test suite for UniversalArbitrageExecutor
 */
contract UniversalArbitrageExecutorTest is Test {
    UniversalArbitrageExecutor public uar;
    
    MockAavePool public mockAavePool;
    MockBalancerVault public mockBalancerVault;
    MockWETH public mockWETH;
    MockERC20 public mockUSDC;
    MockERC20 public mockDAI;
    MockDEXRouter public mockRouter1;
    MockDEXRouter public mockRouter2;
    
    address public owner = address(0x1);
    address public pendingOwner = address(0x2);
    address public attacker = address(0x666);
    address public user = address(0x777);
    
    event TrustedTargetUpdated(address indexed target, bool trusted);
    event Executed(uint256 actions, address indexed profitToken);
    event ProfitTransferred(address indexed token, uint256 amount);
    event OwnershipTransferInitiated(address indexed previousOwner, address indexed newOwner);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    
    function setUp() public {
        vm.startPrank(owner);
        
        mockAavePool = new MockAavePool();
        mockBalancerVault = new MockBalancerVault();
        mockWETH = new MockWETH();
        mockUSDC = new MockERC20("USD Coin", "USDC", 6);
        mockDAI = new MockERC20("Dai Stablecoin", "DAI", 18);
        mockRouter1 = new MockDEXRouter();
        mockRouter2 = new MockDEXRouter();
        
        uar = new UniversalArbitrageExecutor(
            address(mockAavePool),
            address(mockBalancerVault),
            address(mockWETH)
        );
        
        uar.addTrustedTarget(address(mockRouter1));
        uar.addTrustedTarget(address(mockRouter2));
        
        mockUSDC.mint(address(mockAavePool), 1000000 * 10**6);
        mockDAI.mint(address(mockAavePool), 1000000 * 10**18);
        mockUSDC.mint(address(mockBalancerVault), 1000000 * 10**6);
        mockDAI.mint(address(mockBalancerVault), 1000000 * 10**18);
        
        vm.deal(owner, 100 ether);
        vm.deal(address(uar), 10 ether);
        vm.deal(address(mockRouter1), 100 ether);
        vm.deal(address(mockRouter2), 100 ether);
        
        vm.stopPrank();
    }
    
    function test_Constructor() public {
        assertEq(uar.owner(), owner);
        assertEq(address(uar.AAVE_POOL()), address(mockAavePool));
        assertEq(address(uar.BALANCER_VAULT()), address(mockBalancerVault));
        assertEq(uar.WETH(), address(mockWETH));
    }
    
    function test_Constructor_InvalidConfig() public {
        vm.expectRevert(UniversalArbitrageExecutor.InvalidConfig.selector);
        new UniversalArbitrageExecutor(address(0), address(mockBalancerVault), address(mockWETH));
        
        vm.expectRevert(UniversalArbitrageExecutor.InvalidConfig.selector);
        new UniversalArbitrageExecutor(address(mockAavePool), address(0), address(mockWETH));
        
        vm.expectRevert(UniversalArbitrageExecutor.InvalidConfig.selector);
        new UniversalArbitrageExecutor(address(mockAavePool), address(mockBalancerVault), address(0));
    }
    
    function test_AddTrustedTarget() public {
        address newTarget = address(new MockDEXRouter());
        
        vm.startPrank(owner);
        
        vm.expectEmit(true, false, false, true);
        emit TrustedTargetUpdated(newTarget, true);
        
        uar.addTrustedTarget(newTarget);
        assertTrue(uar.isTrustedTarget(newTarget));
        
        vm.stopPrank();
    }
    
    function test_AddTrustedTarget_OnlyOwner() public {
        vm.startPrank(attacker);
        vm.expectRevert(UniversalArbitrageExecutor.OnlyOwner.selector);
        uar.addTrustedTarget(address(0x123));
        vm.stopPrank();
    }
    
    function test_RemoveTrustedTarget() public {
        vm.startPrank(owner);
        
        vm.expectEmit(true, false, false, true);
        emit TrustedTargetUpdated(address(mockRouter1), false);
        
        uar.removeTrustedTarget(address(mockRouter1));
        assertFalse(uar.isTrustedTarget(address(mockRouter1)));
        
        vm.stopPrank();
    }
    
    function test_Execute_SimpleSwap() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        bytes memory swapData = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            1000 * 10**6,
            1000 * 10**6,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockUSDC),
            amountIn: 1000 * 10**6,
            callData: swapData
        });
        
        mockUSDC.mint(address(uar), 1000 * 10**6);
        
        vm.startPrank(owner);
        
        vm.expectEmit(false, true, false, true);
        emit Executed(1, address(mockDAI));
        
        uar.execute(actions, address(mockDAI));
        
        assertGt(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_Execute_ETHSwap() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        bytes memory swapData = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(0),
            address(mockDAI),
            1 ether,
            1 ether,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(0),
            amountIn: 1 ether,
            callData: swapData
        });
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockDAI));
        
        assertGt(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_Execute_MaxAmountSwap() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        uint256 initialBalance = 5000 * 10**6;
        mockUSDC.mint(address(uar), initialBalance);
        
        bytes memory swapData = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            initialBalance,
            0,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockUSDC),
            amountIn: type(uint256).max,
            callData: swapData
        });
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockDAI));
        
        assertEq(mockUSDC.balanceOf(address(uar)), 0);
        assertGt(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_Execute_WrapNative() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.WRAP_NATIVE,
            target: address(0),
            tokenIn: address(0),
            amountIn: 1 ether,
            callData: ""
        });
        
        uint256 initialETH = address(uar).balance;
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockWETH));
        
        assertEq(address(uar).balance, initialETH - 1 ether);
        assertEq(mockWETH.balanceOf(address(uar)), 1 ether);
        
        vm.stopPrank();
    }
    
    function test_Execute_UnwrapNative() public {
        mockWETH.mint(address(uar), 2 ether);
        vm.deal(address(mockWETH), 10 ether);
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.UNWRAP_NATIVE,
            target: address(0),
            tokenIn: address(0),
            amountIn: 1 ether,
            callData: ""
        });
        
        uint256 initialETH = address(uar).balance;
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(0));
        
        assertEq(address(uar).balance, initialETH + 1 ether);
        assertEq(mockWETH.balanceOf(address(uar)), 1 ether);
        
        vm.stopPrank();
    }
    
    function test_Execute_TransferProfit() public {
        mockDAI.mint(address(uar), 1000 * 10**18);
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.TRANSFER_PROFIT,
            target: address(0),
            tokenIn: address(0),
            amountIn: 0,
            callData: ""
        });
        
        vm.startPrank(owner);
        
        vm.expectEmit(true, false, false, true);
        emit ProfitTransferred(address(mockDAI), 1000 * 10**18);
        
        uar.execute(actions, address(mockDAI));
        
        assertEq(mockDAI.balanceOf(owner), 1000 * 10**18);
        assertEq(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_Execute_MultipleActions() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](4);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.WRAP_NATIVE,
            target: address(0),
            tokenIn: address(0),
            amountIn: 2 ether,
            callData: ""
        });
        
        bytes memory swapData1 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockWETH),
            address(mockUSDC),
            1 ether,
            1000000,
            address(uar)
        );
        
        actions[1] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockWETH),
            amountIn: 1 ether,
            callData: swapData1
        });
        
        bytes memory swapData2 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            1_000_000_000_000_000_000,
            0,
            address(uar)
        );
        
        actions[2] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter2),
            tokenIn: address(mockUSDC),
            amountIn: type(uint256).max,
            callData: swapData2
        });
        
        actions[3] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.TRANSFER_PROFIT,
            target: address(0),
            tokenIn: address(0),
            amountIn: 0,
            callData: ""
        });
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockDAI));
        
        assertGt(mockDAI.balanceOf(owner), 0);
        assertEq(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_Execute_UntrustedTarget() public {
        address untrustedRouter = address(new MockDEXRouter());
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: untrustedRouter,
            tokenIn: address(mockUSDC),
            amountIn: 1000 * 10**6,
            callData: ""
        });
        
        vm.startPrank(owner);
        
        vm.expectRevert(UniversalArbitrageExecutor.UntrustedTarget.selector);
        uar.execute(actions, address(mockDAI));
        
        vm.stopPrank();
    }
    
    function test_AaveFlashLoan() public {
        address[] memory assets = new address[](2);
        uint256[] memory amounts = new uint256[](2);
        uint256[] memory modes = new uint256[](2);
        
        assets[0] = address(mockUSDC);
        assets[1] = address(mockDAI);
        amounts[0] = 10000 * 10**6;
        amounts[1] = 10000 * 10**18;
        modes[0] = 0;
        modes[1] = 0;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](2);
        
        bytes memory swapData1 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            10000 * 10**6,
            10000 * 10**6,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockUSDC),
            amountIn: 10000 * 10**6,
            callData: swapData1
        });
        
        bytes memory swapData2 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockDAI),
            address(mockUSDC),
            10000 * 10**6,
            10000 * 10**6,
            address(uar)
        );
        
        actions[1] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter2),
            tokenIn: address(mockDAI),
            amountIn: 10000 * 10**6,
            callData: swapData2
        });
        
        bytes memory params = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(address(uar));
        
        mockAavePool.flashLoan(
            address(uar),
            assets,
            amounts,
            modes,
            address(uar),
            params,
            0
        );
        
        assertGt(mockUSDC.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_AaveFlashLoan_InvalidSender() public {
        address[] memory assets = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory premiums = new uint256[](1);
        
        assets[0] = address(mockUSDC);
        amounts[0] = 10000 * 10**6;
        premiums[0] = 9;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](0);
        bytes memory params = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(attacker);
        
        vm.expectRevert(abi.encodeWithSelector(UniversalArbitrageExecutor.UnsupportedCallback.selector, attacker));
        uar.executeOperation(assets, amounts, premiums, address(uar), params);
        
        vm.stopPrank();
    }
    
    function test_AaveFlashLoan_InvalidInitiator() public {
        address[] memory assets = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory premiums = new uint256[](1);
        
        assets[0] = address(mockUSDC);
        amounts[0] = 10000 * 10**6;
        premiums[0] = 9;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](0);
        bytes memory params = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(address(mockAavePool));
        
        vm.expectRevert(abi.encodeWithSelector(UniversalArbitrageExecutor.UnsupportedCallback.selector, address(mockAavePool)));
        uar.executeOperation(assets, amounts, premiums, attacker, params);
        
        vm.stopPrank();
    }
    
    function test_BalancerFlashLoan() public {
        IERC20[] memory tokens = new IERC20[](2);
        uint256[] memory amounts = new uint256[](2);
        
        tokens[0] = IERC20(address(mockUSDC));
        tokens[1] = IERC20(address(mockDAI));
        amounts[0] = 10000 * 10**6;
        amounts[1] = 10000 * 10**18;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](2);
        
        bytes memory swapData1 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            10000 * 10**6,
            10000 * 10**6,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockUSDC),
            amountIn: 10000 * 10**6,
            callData: swapData1
        });
        
        bytes memory swapData2 = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockDAI),
            address(mockUSDC),
            10000 * 10**6,
            10000 * 10**6,
            address(uar)
        );
        
        actions[1] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter2),
            tokenIn: address(mockDAI),
            amountIn: 10000 * 10**6,
            callData: swapData2
        });
        
        bytes memory userData = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(address(uar));
        
        mockBalancerVault.flashLoan(
            address(uar),
            tokens,
            amounts,
            userData
        );
        
        assertEq(mockUSDC.balanceOf(address(uar)), 0);
        assertEq(mockDAI.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_BalancerFlashLoan_InvalidSender() public {
        IERC20[] memory tokens = new IERC20[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory feeAmounts = new uint256[](1);
        
        tokens[0] = IERC20(address(mockUSDC));
        amounts[0] = 10000 * 10**6;
        feeAmounts[0] = 0;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](0);
        bytes memory userData = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(attacker);
        
        vm.expectRevert(abi.encodeWithSelector(UniversalArbitrageExecutor.UnsupportedCallback.selector, attacker));
        uar.receiveFlashLoan(tokens, amounts, feeAmounts, userData);
        
        vm.stopPrank();
    }
    
    function test_TransferOwnership() public {
        vm.startPrank(owner);
        
        vm.expectEmit(true, true, false, false);
        emit OwnershipTransferInitiated(owner, pendingOwner);
        
        uar.transferOwnership(pendingOwner);
        
        assertEq(uar.pendingOwner(), pendingOwner);
        assertEq(uar.owner(), owner);
        
        vm.stopPrank();
        
        vm.startPrank(pendingOwner);
        
        vm.expectEmit(true, true, false, false);
        emit OwnershipTransferred(owner, pendingOwner);
        
        uar.acceptOwnership();
        
        assertEq(uar.owner(), pendingOwner);
        assertEq(uar.pendingOwner(), address(0));
        
        vm.stopPrank();
    }
    
    function test_AcceptOwnership_OnlyPendingOwner() public {
        vm.startPrank(owner);
        uar.transferOwnership(pendingOwner);
        vm.stopPrank();
        
        vm.startPrank(attacker);
        vm.expectRevert(UniversalArbitrageExecutor.OnlyPendingOwner.selector);
        uar.acceptOwnership();
        vm.stopPrank();
    }
    
    function test_RenounceOwnership() public {
        vm.startPrank(owner);
        
        vm.expectEmit(true, true, false, false);
        emit OwnershipTransferred(owner, address(0));
        
        uar.renounceOwnership();
        
        assertEq(uar.owner(), address(0));
        assertEq(uar.pendingOwner(), address(0));
        
        vm.stopPrank();
    }
    
    function test_WithdrawStuckTokens() public {
        uint256 stuckAmount = 1000 * 10**6;
        mockUSDC.mint(address(uar), stuckAmount);
        
        vm.startPrank(owner);
        
        uar.withdrawStuckTokens(address(mockUSDC));
        
        assertEq(mockUSDC.balanceOf(owner), stuckAmount);
        assertEq(mockUSDC.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function test_WithdrawETH() public {
        uint256 withdrawAmount = 1 ether;
        uint256 initialOwnerBalance = owner.balance;
        
        vm.startPrank(owner);
        
        uar.withdrawETH(withdrawAmount);
        
        assertEq(owner.balance, initialOwnerBalance + withdrawAmount);
        
        vm.stopPrank();
    }
    
    function test_WithdrawETH_Failed() public {
        uint256 withdrawAmount = 100 ether;
        
        vm.startPrank(owner);
        
        vm.expectRevert(UniversalArbitrageExecutor.ETHTransferFailed.selector);
        uar.withdrawETH(withdrawAmount);
        
        vm.stopPrank();
    }
    
    function test_Reentrancy_Protection() public {
        ReentrancyAttacker reentrancyAttacker = new ReentrancyAttacker(address(uar));
        
        vm.startPrank(owner);
        uar.addTrustedTarget(address(reentrancyAttacker));
        uar.transferOwnership(address(reentrancyAttacker));
        vm.stopPrank();
        
        vm.startPrank(address(reentrancyAttacker));
        reentrancyAttacker.acceptOwnership();
        vm.stopPrank();
        
        mockUSDC.mint(address(uar), 1000 * 10**6);
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(reentrancyAttacker),
            tokenIn: address(mockUSDC),
            amountIn: 1000 * 10**6,
            callData: abi.encodeWithSignature("attack()")
        });
        
        vm.startPrank(address(reentrancyAttacker));
        
        vm.expectRevert();
        uar.execute(actions, address(mockUSDC));
        
        vm.stopPrank();
    }
    
    function test_CallbackLock_InFlashLoan() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.TRANSFER_PROFIT,
            target: address(0),
            tokenIn: address(0),
            amountIn: 0,
            callData: ""
        });
        
        address[] memory assets = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory modes = new uint256[](1);
        
        assets[0] = address(mockUSDC);
        amounts[0] = 10000 * 10**6;
        modes[0] = 0;
        
        bytes memory params = abi.encode(actions, address(mockUSDC));
        
        vm.startPrank(address(uar));
        
        vm.expectRevert(UniversalArbitrageExecutor.CallbackLocked.selector);
        mockAavePool.flashLoan(
            address(uar),
            assets,
            amounts,
            modes,
            address(uar),
            params,
            0
        );
        
        vm.stopPrank();
    }
    
    function test_CallbackLock_AdminFunctions() public {
        CallbackLockTester tester = new CallbackLockTester(address(uar), address(mockAavePool));
        
        vm.startPrank(owner);
        uar.addTrustedTarget(address(tester));
        uar.transferOwnership(address(tester));
        vm.stopPrank();
        
        vm.startPrank(address(tester));
        tester.acceptOwnership();
        
        address[] memory assets = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        uint256[] memory modes = new uint256[](1);
        
        assets[0] = address(mockUSDC);
        amounts[0] = 10000 * 10**6;
        modes[0] = 0;
        
        mockUSDC.mint(address(tester), amounts[0] + 10);
        
        bytes memory params = abi.encode(new UniversalArbitrageExecutor.Action[](0), address(mockUSDC));
        
        vm.expectRevert(UniversalArbitrageExecutor.InvalidConfig.selector);
        mockAavePool.flashLoan(
            address(tester),
            assets,
            amounts,
            modes,
            address(tester),
            params,
            0
        );
        
        vm.stopPrank();
    }
    
    function testFuzz_Execute_SwapAmounts(uint256 amount) public {
        amount = bound(amount, 101, 100000 * 10**6);
        
        mockUSDC.mint(address(uar), amount);
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        bytes memory swapData = abi.encodeWithSelector(
            MockDEXRouter.swap.selector,
            address(mockUSDC),
            address(mockDAI),
            amount,
            0,
            address(uar)
        );
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(mockRouter1),
            tokenIn: address(mockUSDC),
            amountIn: amount,
            callData: swapData
        });
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockDAI));
        
        assertGt(mockDAI.balanceOf(address(uar)), 0);
        assertEq(mockUSDC.balanceOf(address(uar)), 0);
        
        vm.stopPrank();
    }
    
    function testFuzz_FlashLoan_Amounts(uint256 amount1, uint256 amount2) public {
        amount1 = bound(amount1, 1000, 100000 * 10**6);
        amount2 = bound(amount2, 1000, 100000 * 10**18);
        
        address[] memory assets = new address[](2);
        uint256[] memory amounts = new uint256[](2);
        uint256[] memory modes = new uint256[](2);
        
        assets[0] = address(mockUSDC);
        assets[1] = address(mockDAI);
        amounts[0] = amount1;
        amounts[1] = amount2;
        modes[0] = 0;
        modes[1] = 0;
        
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](0);
        bytes memory params = abi.encode(actions, address(mockUSDC));
        
        uint256 premium1 = (amount1 * 9) / 10000;
        uint256 premium2 = (amount2 * 9) / 10000;
        
        mockUSDC.mint(address(uar), premium1);
        mockDAI.mint(address(uar), premium2);
        
        vm.startPrank(address(uar));
        
        mockAavePool.flashLoan(
            address(uar),
            assets,
            amounts,
            modes,
            address(uar),
            params,
            0
        );
        
        vm.stopPrank();
    }
    
    function test_GasOptimization_BatchedActions() public {
        uint256 numActions = 10;
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](numActions);
        
        for (uint256 i = 0; i < numActions; i++) {
            if (i % 2 == 0) {
                actions[i] = UniversalArbitrageExecutor.Action({
                    actionType: UniversalArbitrageExecutor.ActionType.WRAP_NATIVE,
                    target: address(0),
                    tokenIn: address(0),
                    amountIn: 0.1 ether,
                    callData: ""
                });
            } else {
                actions[i] = UniversalArbitrageExecutor.Action({
                    actionType: UniversalArbitrageExecutor.ActionType.UNWRAP_NATIVE,
                    target: address(0),
                    tokenIn: address(0),
                    amountIn: 0.1 ether,
                    callData: ""
                });
            }
        }
        
        vm.startPrank(owner);
        
        uint256 gasStart = gasleft();
        uar.execute(actions, address(0));
        uint256 gasUsed = gasStart - gasleft();
        
        assertLt(gasUsed, 500000);
        
        vm.stopPrank();
    }
    
    function test_EdgeCase_ZeroAmountTransfer() public {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.TRANSFER_PROFIT,
            target: address(0),
            tokenIn: address(0),
            amountIn: 0,
            callData: ""
        });
        
        vm.startPrank(owner);
        
        uar.execute(actions, address(mockDAI));
        
        vm.stopPrank();
    }
    
    function test_Receive_ETH() public {
        uint256 sendAmount = 1 ether;
        uint256 initialBalance = address(uar).balance;
        
        vm.deal(user, sendAmount);
        vm.prank(user);
        (bool success,) = address(uar).call{value: sendAmount}("");
        
        assertTrue(success);
        assertEq(address(uar).balance, initialBalance + sendAmount);
    }
}

/**
 * @title ReentrancyAttacker
 * @notice Contract to test reentrancy protection
 */
contract ReentrancyAttacker {
    UniversalArbitrageExecutor public target;
    
    constructor(address _target) {
        target = UniversalArbitrageExecutor(payable(_target));
    }
    
    function acceptOwnership() external {
        target.acceptOwnership();
    }
    
    function attack() external {
        UniversalArbitrageExecutor.Action[] memory actions = new UniversalArbitrageExecutor.Action[](1);
        
        actions[0] = UniversalArbitrageExecutor.Action({
            actionType: UniversalArbitrageExecutor.ActionType.SWAP,
            target: address(this),
            tokenIn: address(0),
            amountIn: 0,
            callData: ""
        });
        
        target.execute(actions, address(0));
    }
    
    receive() external payable {}
}

/**
 * @title CallbackLockTester
 * @notice Contract to test callback lock mechanism
 */
contract CallbackLockTester is IFlashLoanReceiver {
    UniversalArbitrageExecutor public uar;
    IPool public aavePool;
    
    constructor(address _uar, address _aavePool) {
        uar = UniversalArbitrageExecutor(payable(_uar));
        aavePool = IPool(_aavePool);
    }
    
    function acceptOwnership() external {
        uar.acceptOwnership();
    }
    
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        uar.addTrustedTarget(address(0x999));
        
        for (uint256 i = 0; i < assets.length; i++) {
            IERC20(assets[i]).transfer(msg.sender, amounts[i] + premiums[i]);
        }
        
        return true;
    }
}
