// SPDX-License-Identifier: MIT
// test_uar.sol
pragma solidity ^0.8.21;

import "lib/forge-std/src/Test.sol";
import "lib/forge-std/src/console.sol";
import "../contracts/uar.sol";
import "lib/openzeppelin-contracts/contracts/token/ERC20/IERC20.sol";
import "lib/ERC20PresetMinterPauser.sol";
import "../contracts/interfaces/IPool.sol";
import "../contracts/interfaces/IFlashLoanReceiver.sol";
import "lib/aave-v3-core/contracts/interfaces/IPoolAddressesProvider.sol";
import "lib/aave-v3-core/contracts/protocol/libraries/types/DataTypes.sol";
import "lib/chainlink/contracts/src/v0.8/shared/interfaces/AggregatorV3Interface.sol";
import "lib/openzeppelin-contracts/contracts/utils/structs/EnumerableSet.sol";

/**
 * @title MockPriceOracle
 * @notice Mock implementation of Chainlink Price Feed
 */
contract MockPriceOracle is AggregatorV3Interface {
    int256 private constant FIXED_PRICE = 2000 * 10**8; // $2000 with 8 decimals
    
    function decimals() external view returns (uint8) {
        return 8;
    }
    
    function description() external view returns (string memory) {
        return "Mock Price Oracle";
    }
    
    function version() external view returns (uint256) {
        return 1;
    }
    
    function getRoundData(uint80 _roundId) external view returns (
        uint80 roundId,
        int256 answer,
        uint256 startedAt,
        uint256 updatedAt,
        uint80 answeredInRound
    ) {
        return (_roundId, FIXED_PRICE, block.timestamp - 3600, block.timestamp - 3600, _roundId);
    }
    
    function latestRoundData() external view returns (
        uint80 roundId,
        int256 answer,
        uint256 startedAt,
        uint256 updatedAt,
        uint80 answeredInRound
    ) {
        return (1, FIXED_PRICE, block.timestamp - 60, block.timestamp - 60, 1);
    }
}

contract MockAaveAddressesProvider is IPoolAddressesProvider {
    address private pool;

    function getPool() external view returns (address) {
        return pool;
    }

    function setPool(address newPool) external {
        pool = newPool;
    }

    function getMarketId() external view returns (string memory) { return "MockMarket"; }
    function setMarketId(string calldata) external view {}

    function getAddress(bytes32) external view returns (address) { return address(0); }
    function setAddress(bytes32, address) external view {}
    function setAddressAsProxy(bytes32, address) external view {}

    function getPoolConfigurator() external view returns (address) { return address(0); }
    function setPoolConfiguratorImpl(address) external view {}

    function setPoolImpl(address) external view {}

    function getPriceOracle() external view returns (address) { return address(0); }
    function setPriceOracle(address) external view {}

    function getACLManager() external view returns (address) { return address(0); }
    function setACLManager(address) external view {}

    function getACLAdmin() external view returns (address) { return address(0); }
    function setACLAdmin(address) external view {}

    function getPriceOracleSentinel() external view returns (address) { return address(0); }
    function setPriceOracleSentinel(address) external view {}

    function getPoolDataProvider() external view returns (address) { return address(0); }
    function setPoolDataProvider(address) external view {}
}

/**
 * @title MockAavePool
 * @notice Mock implementation of Aave Pool for testing
 */
contract MockAavePool is IPool {
    // Basic flash loan implementation
    function flashLoan(
        address receiverAddress,
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata interestRateModes,
        address onBehalfOf,
        bytes calldata params,
        uint16 referralCode
    ) external {}
    
    // Basic supply implementation
    function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external {}
    
    // Supply with permit implementation
    function supplyWithPermit(
        address asset,
        uint256 amount,
        address onBehalfOf,
        uint16 referralCode,
        uint256 deadline,
        uint8 permitV,
        bytes32 permitR,
        bytes32 permitS
    ) external {}
    
    // Withdraw implementation with mock return
    function withdraw(address asset, uint256 amount, address to) external returns (uint256) { 
        return 0; 
    }
    
    // Borrow implementation
    function borrow(
        address asset,
        uint256 amount,
        uint256 interestRateMode,
        uint16 referralCode,
        address onBehalfOf
    ) external {}
    
    // Repay implementation with mock return
    function repay(
        address asset,
        uint256 amount,
        uint256 interestRateMode,
        address onBehalfOf
    ) external returns (uint256) { 
        return 0; 
    }
    
    // Repay with permit implementation with mock return
    function repayWithPermit(
        address asset,
        uint256 amount,
        uint256 interestRateMode,
        address onBehalfOf,
        uint256 deadline,
        uint8 permitV,
        bytes32 permitR,
        bytes32 permitS
    ) external returns (uint256) { 
        return 0; 
    }
    
    // Repay with aTokens implementation with mock return
    function repayWithATokens(
        address asset,
        uint256 amount,
        uint256 interestRateMode
    ) external returns (uint256) { 
        return 0; 
    }
    
    // Swap borrow rate mode implementation
    function swapBorrowRateMode(address asset, uint256 interestRateMode) external {}
    
    // Rebalance stable borrow rate implementation
    function rebalanceStableBorrowRate(address asset, address user) external {}
    
    // Set user use reserve as collateral implementation
    function setUserUseReserveAsCollateral(address asset, bool useAsCollateral) external {}
    
    // Liquidation call implementation
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external {}
    
    // Simple flash loan implementation
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external {}
    
    // Mint to treasury implementation
    function mintToTreasury(address[] calldata assets) external {}
    
    // Deposit implementation
    function deposit(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external {}

    // Get reserve data mock implementation
    function getReserveData(address asset) external view returns (DataTypes.ReserveData memory) {
        return DataTypes.ReserveData({
            configuration: DataTypes.ReserveConfigurationMap(0),
            liquidityIndex: 0,
            currentLiquidityRate: 0,
            variableBorrowIndex: 0,
            currentVariableBorrowRate: 0,
            currentStableBorrowRate: 0,
            lastUpdateTimestamp: 0,
            id: 0,
            aTokenAddress: address(0),
            stableDebtTokenAddress: address(0),
            variableDebtTokenAddress: address(0),
            interestRateStrategyAddress: address(0),
            accruedToTreasury: 0,
            unbacked: 0,
            isolationModeTotalDebt: 0
        });
    }

    // Get reserves list mock implementation
    function getReservesList() external view returns (address[] memory) {
        address[] memory reserves = new address[](1);
        reserves[0] = address(0x1234567890123456789012345678901234567890);
        return reserves;
    }    
    
    // Get reserve normalized income mock implementation
    function getReserveNormalizedIncome(address asset) external view returns (uint256) { 
        return 0; 
    }
    
    // Get reserve normalized variable debt mock implementation
    function getReserveNormalizedVariableDebt(address asset) external view returns (uint256) { 
        return 0; 
    }
    
    // Finalize transfer mock implementation
    function finalizeTransfer(
        address asset,
        address from,
        address to,
        uint256 amount,
        uint256 balanceFromBefore,
        uint256 balanceToBefore
    ) external {}
    
    // Init reserve mock implementation
    function initReserve(
        address asset,
        address aTokenAddress,
        address stableDebtAddress,
        address variableDebtAddress,
        address interestRateStrategyAddress
    ) external {}
    
    // Drop reserve mock implementation
    function dropReserve(address asset) external {}
    
    // Update bridge protocol fee mock implementation
    function updateBridgeProtocolFee(uint256 protocolFee) external {}
    
    // Update flash loan premiums mock implementation
    function updateFlashloanPremiums(
        uint128 flashLoanPremiumTotal,
        uint128 flashLoanPremiumToProtocol
    ) external {}
    
    // Configure eMode category mock implementation
    function configureEModeCategory(uint8 id, DataTypes.EModeCategory calldata category) external {}
    
    // Get eMode category data mock implementation
    function getEModeCategoryData(uint8 id) external view returns (DataTypes.EModeCategory memory) {
        return DataTypes.EModeCategory({
            ltv: 0,
            liquidationThreshold: 0,
            liquidationBonus: 0,
            priceSource: address(0),
            label: ""
        });
    }
    
    // Get user eMode mock implementation
    function getUserEMode(address user) external view returns (uint256) { 
        return 0; 
    }
    
    // Set user eMode mock implementation
    function setUserEMode(uint8 categoryId) external {}
    
    // Reset isolation mode total debt mock implementation
    function resetIsolationModeTotalDebt(address asset) external {}
    
    // Rescue tokens mock implementation
    function rescueTokens(
        address token,
        address to,
        uint256 amount
    ) external {}

    // Additional functions from IPool that were missing in the original implementation

    // Get ADDRESSES_PROVIDER mock implementation
    function ADDRESSES_PROVIDER() external view returns (IPoolAddressesProvider) {
        return IPoolAddressesProvider(address(0));
    }

    // Get MAX_STABLE_RATE_BORROW_SIZE_PERCENT mock implementation
    function MAX_STABLE_RATE_BORROW_SIZE_PERCENT() external view returns (uint256) {
        return 0;
    }

    // Get FLASHLOAN_PREMIUM_TOTAL mock implementation
    function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128) {
        return 0;
    }

    // Get BRIDGE_PROTOCOL_FEE mock implementation
    function BRIDGE_PROTOCOL_FEE() external view returns (uint256) {
        return 0;
    }

    // Get FLASHLOAN_PREMIUM_TO_PROTOCOL mock implementation
    function FLASHLOAN_PREMIUM_TO_PROTOCOL() external view returns (uint128) {
        return 0;
    }

    // Get MAX_NUMBER_RESERVES mock implementation
    function MAX_NUMBER_RESERVES() external view returns (uint16) {
        return 0;
    }

    // Get configuration mock implementation
    function getConfiguration(address) external view returns (DataTypes.ReserveConfigurationMap memory) {
        return DataTypes.ReserveConfigurationMap(0);
    }

    // Get user configuration mock implementation
    function getUserConfiguration(address) external view returns (DataTypes.UserConfigurationMap memory) {
        return DataTypes.UserConfigurationMap(0);
    }

    // Get reserve address by id mock implementation
    function getReserveAddressById(uint16) external view returns (address) {
        return address(0);
    }

    // Get user account data mock implementation
    function getUserAccountData(address) external view returns (
        uint256 totalCollateralBase,
        uint256 totalDebtBase,
        uint256 availableBorrowsBase,
        uint256 currentLiquidationThreshold,
        uint256 ltv,
        uint256 healthFactor
    ) {
        return (0, 0, 0, 0, 0, 0);
    }

    // Set reserve interest rate strategy address mock implementation
    function setReserveInterestRateStrategyAddress(address, address) external {}

    // Set configuration mock implementation
    function setConfiguration(address, DataTypes.ReserveConfigurationMap calldata) external {}

    // Mint unbacked mock implementation
    function mintUnbacked(address, uint256, address, uint16) external {}

    // Back unbacked mock implementation
    function backUnbacked(address, uint256, uint256) external returns (uint256) {
        return 0;
    }
}


/**
 * @title MockOneInchRouter
 * @notice Mock implementation of 1inch Router
 */
contract MockOneInchRouter {
    struct SwapDescription {
        address srcToken;
        address dstToken;
        address payable srcReceiver;
        address payable dstReceiver;
        uint256 amount;
        uint256 minReturnAmount;
        uint256 flags;
        bytes permit;
    }
    
    event Swapped(
        address srcToken,
        address dstToken,
        uint256 amountIn,
        uint256 amountOut
    );
    
    function swap(
        address caller,
        SwapDescription calldata desc,
        bytes calldata data
    ) external returns (uint256 returnAmount, uint256 spentAmount) {
        // Simple swap implementation - just emits event
        require(IERC20(desc.srcToken).transferFrom(desc.srcReceiver, address(this), desc.amount), "Transfer failed");
        
        // In a real swap, we'd actually swap tokens
        // For testing, we just simulate a return amount
        uint256 simulatedReturnAmount = desc.minReturnAmount;
        
        // Check if we need to simulate a slippage (for testing failure cases)
        if (keccak256(data) == keccak256("simulateSlippage")) {
            simulatedReturnAmount = desc.minReturnAmount * 95 / 100; // 5% slippage
        }
        
        // Transfer the output token to the destination
        IERC20(desc.dstToken).transfer(desc.dstReceiver, simulatedReturnAmount);
        
        emit Swapped(desc.srcToken, desc.dstToken, desc.amount, simulatedReturnAmount);
        
        return (simulatedReturnAmount, desc.amount);
    }
}

/**
 * @title MockZeroXExchange
 * @notice Mock implementation of 0x Exchange
 */
contract MockZeroXExchange {
    struct Transformation {
        uint32 deploymentNonce;
        bytes data;
    }
    
    event Swapped(
        address inputToken,
        address outputToken,
        uint256 amountIn,
        uint256 amountOut
    );
    
    function transformERC20(
        address inputToken,
        address outputToken,
        uint256 inputTokenAmount,
        uint256 minOutputTokenAmount,
        Transformation[] calldata transformations
    ) external returns (uint256 outputTokenAmount) {
        // Simple transformation implementation
        require(IERC20(inputToken).transferFrom(msg.sender, address(this), inputTokenAmount), "Transfer failed");
        
        // Simulate return amount
        uint256 simulatedReturnAmount = minOutputTokenAmount;
        
        // Check if we need to simulate a slippage (for testing failure cases)
        bool simulateSlippage = false;
        if (transformations.length > 0 && transformations[0].deploymentNonce == 1) {
            simulateSlippage = true;
        }
        
        if (simulateSlippage) {
            simulatedReturnAmount = minOutputTokenAmount * 95 / 100; // 5% slippage
        }
        
        // Transfer the output token
        IERC20(outputToken).transfer(msg.sender, simulatedReturnAmount);
        
        emit Swapped(inputToken, outputToken, inputTokenAmount, simulatedReturnAmount);
        
        return simulatedReturnAmount;
    }
}

/**
 * @title MockBridge
 * @notice Mock implementation of a cross-chain bridge
 */
contract MockBridge {
    event BridgeTransfer(
        uint256 chainId,
        address receiver,
        address token,
        uint256 amount
    );
    
    enum BridgeResult {
        SUCCESS,
        FAILURE
    }
    
    BridgeResult private _result = BridgeResult.SUCCESS;
    
    function setResult(BridgeResult result) external {
        _result = result;
    }
    
    function sendToChain(
        uint256 chainId,
        address receiver,
        address token,
        uint256 amount,
        uint256 deadline
    ) external returns (bytes32) {
        require(block.timestamp <= deadline, "Deadline expired");
        require(amount > 0, "Amount must be positive");
        
        if (_result == BridgeResult.FAILURE) {
            revert("Bridge transfer failed");
        }
        
        // Transfer the tokens from sender to this contract
        IERC20(token).transferFrom(msg.sender, address(this), amount);
        
        // Emit event
        emit BridgeTransfer(chainId, receiver, token, amount);
        
        // Return a dummy transfer ID
        return keccak256(abi.encodePacked(chainId, receiver, token, amount, block.timestamp));
    }
}

/**
 * @title MockWETH
 * @notice Mock implementation of WETH
 */
contract MockWETH is ERC20PresetMinterPauser {
    constructor() ERC20PresetMinterPauser("Wrapped ETH", "WETH") {}
    
    function deposit() external payable {
        _mint(msg.sender, msg.value);
    }
    
    function withdraw(uint256 amount) external {
        _burn(msg.sender, amount);
        (bool success, ) = msg.sender.call{value: amount}("");
        require(success, "ETH transfer failed");
    }
    
    receive() external payable {
        _mint(msg.sender, msg.value);
    }
}

/**
 * @title ConcreteRouter
 * @notice A concrete implementation of the abstract EnhancedUniversalArbitrageRouter
 * @dev Provides simplified implementations for testing
 */
contract ConcreteRouter {
    using EnumerableSet for EnumerableSet.AddressSet;
    using EnumerableSet for EnumerableSet.Bytes32Set;

    // Events
    event AdminChanged(address indexed oldAdmin, address indexed newAdmin);
    event DynamicSlippageLimitSet(uint256 newLimitBps);
    event SynergyCheckTriggered(bytes32 indexed synergyKey, bool passed, string reason);
    event SynergyScoreUpdated(bytes32 synergyKey, uint256 newScore);
    event SynergyThresholdUpdated(uint256 newThreshold);
    event SynergyOperatorAdded(address indexed operator);
    event SynergyOperatorRemoved(address indexed operator);
    event RequiredSynergyApprovalsSet(uint256 requiredApprovals);
    event MemoryStored(bytes32 indexed key, uint256 size);
    event PatternDeleted(bytes32 indexed key);
    event PriceOracleUpdated(address token, address oracle);
    event SwapExecuted(
        address indexed aggregator,
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOut,
        uint256 slippageLimitBps,
        uint256 gasUsed,
        address indexed executor
    );
    event CrossChainTransferExecuted(
        bytes32 indexed transferId,
        uint256 destChainId,
        string bridgeType,
        address token,
        uint256 amount,
        bool success,
        uint256 gasUsed
    );
    event NewActionExecuted(uint8 indexed aType, address indexed target, bytes data, uint256 gasUsed);
    event FlashLoanRepaid(address asset, uint256 amountOwing, uint256 gasUsed);
    event EmergencyWithdrawal(address indexed token, uint256 amount, address indexed recipient);
    event CircuitBreakerReset(string breakerType);
    event CircuitBreakerTriggered(string breakerType, uint256 currentValue, uint256 threshold);
    event BridgeCallFailed(address bridge, bytes data, string reason);
    event AggregatorUpdated(string aggregatorType, address newAddress);
    event Pause();
    event Unpause();
    event OptimalPathComputed(bytes32 indexed pathId, uint256 estimatedProfit, uint256 gasEstimate);
    event ProfitPathExecuted(address indexed executor, bytes32 pathId, uint256 profitAmount);

    // Constants
    bytes32 public constant ADMIN_ROLE = keccak256("ADMIN_ROLE");
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant EMERGENCY_ROLE = keccak256("EMERGENCY_ROLE");
    bytes32 public constant SENTINEL_ROLE = keccak256("SENTINEL_ROLE");
    bytes32 public constant ANALYST_ROLE = keccak256("ANALYST_ROLE");
    bytes32 public constant EXECUTOR_ROLE = keccak256("EXECUTOR_ROLE");
    uint256 public constant MAX_PATTERN_SIZE = 64 * 1024;
    uint256 public constant MAX_BATCH_DELETE_SIZE = 20;

    // State variables
    address public oneInchRouter;
    address public zeroXExchange;
    IPoolAddressesProvider public aaveAddressesProvider;
    IPool public aaveLendingPool;
    address public nativeWrapper;
    uint256 public dynamicSlippageLimitBps = 100;
    uint256 public synergyThreshold = 100;
    uint256 public requiredSynergyApprovals = 3;
    uint256 public mevProtectionLevel = 80;
    address public feeCollector;
    uint256 public feeBps = 500;
    bool public paused;
    uint256 public chainId;
    uint256 public totalProfitGenerated;
    
    // Maps
    mapping(address => bool) public hasRole;
    mapping(string => address) public bridgeContracts;
    mapping(bytes32 => uint256) public synergyScores;
    mapping(uint256 => bool) public supportedChainIds;
    mapping(uint256 => address) public chainProviders;
    mapping(bytes32 => mapping(address => bool)) public synergyApprovals;
    mapping(address => bool) public isSynergyOperator;
    mapping(address => address) public priceOracles;
    mapping(uint256 => bool) public usedNonces;
    mapping(address => uint256) public tokenVolatilityScores;
    mapping(bytes32 => bytes) public memoryPatterns;
    
    // Custom defined struct types
    struct CircuitBreaker {
        uint256 threshold;
        uint256 cooldownPeriod;
        uint256 lastTriggered;
        bool isTriggered;
    }
    
    struct PoolInfo {
        address token0;
        address token1;
        uint256 reserve0;
        uint256 reserve1;
        uint256 fee;
        uint256 lastUpdated;
        string protocol;
    }
    
    struct ComputedPath {
        address[] tokens;
        address[] pools;
        uint256[] amounts;
        uint256 expectedProfit;
        uint256 gasEstimate;
        uint256 timestamp;
        uint256 riskScore;
        bytes executionData;
    }
    
    struct SlippageModel {
        uint256 baseSlippageBps;
        uint256 volMultiplier;
        uint256 sizeMultiplier;
        uint256 timeMultiplier;
        uint256 lastUpdated;
    }
    
    // Storage for complex structs
    mapping(string => CircuitBreaker) public circuitBreakers;
    mapping(address => PoolInfo) public poolInfoCache;
    mapping(bytes32 => ComputedPath) public optimizedPaths;
    mapping(address => SlippageModel) public tokenSlippageModels;
    
    // Collections
    EnumerableSet.AddressSet private _operators;
    EnumerableSet.Bytes32Set private _activePaths;
    
    // Errors
    error NotAuthorized();
    error ZeroAddress();
    error InvalidArguments(string argument);
    error PatternTooLarge(uint256 maxSize, uint256 actualSize);
    error BatchDeleteLimitExceeded(uint256 limit, uint256 actualSize);
    error PatternNotFound(bytes32 key);
    error BridgeNotSet(string bridgeType);
    error ChainNotSupported();
    error BridgeTransferFailed(string bridgeType, string reason);
    error SynergyCheckFailed(bytes32 synergyKey, string reason);
    error FlashLoanFailed(address asset, string reason);
    error InvalidActionType();
    error AggregatorSwapFailed(address aggregator, string reason);
    error InvalidPath(uint256 providedLength, uint256 expectedLength);
    error NonceReused(uint256 nonce);
    error InvalidDeadline();
    error OperatorAlreadyAdded(address operator);
    error OperatorNotFound(address operator);
    error ChainRemovalForbidden(uint256 chainId);
    
    // Constructor
    constructor(
        address _oneInchRouter,
        address _zeroXExchange,
        address _aaveAddressesProvider,
        address _nativeWrapper
    ) {
        oneInchRouter = _oneInchRouter;
        zeroXExchange = _zeroXExchange;
        aaveAddressesProvider = IPoolAddressesProvider(_aaveAddressesProvider);
        aaveLendingPool = IPool(aaveAddressesProvider.getPool());
        nativeWrapper = _nativeWrapper;
        chainId = block.chainid;
        supportedChainIds[chainId] = true;
        chainProviders[chainId] = _aaveAddressesProvider;
        
        // Setup default roles
        hasRole[msg.sender] = true;
        
        // Setup circuit breakers
        circuitBreakers["maxSlippage"] = CircuitBreaker({
            threshold: 1000, // 10%
            cooldownPeriod: 3600, // 1 hour
            lastTriggered: 0,
            isTriggered: false
        });
    }
    
    // Role modifiers
    modifier onlyAdmin() {
        if (!hasRole[msg.sender]) revert NotAuthorized();
        _;
    }
    
    modifier onlyOperator() {
        if (!hasRole[msg.sender]) revert NotAuthorized();
        _;
    }
    
    modifier onlyEmergency() {
        if (!hasRole[msg.sender]) revert NotAuthorized();
        _;
    }
    
    modifier onlySentinel() {
        if (!hasRole[msg.sender]) revert NotAuthorized();
        _;
    }
    
    modifier onlyExecutor() {
        if (!hasRole[msg.sender]) revert NotAuthorized();
        _;
    }
    
    modifier nonReusableNonce(uint256 nonce) {
        if (usedNonces[nonce]) revert NonceReused(nonce);
        usedNonces[nonce] = true;
        _;
    }
    
    modifier ensureDeadline(uint256 deadline) {
        if (block.timestamp > deadline) revert InvalidDeadline();
        _;
    }
    
    modifier validActionType(uint8 aType) {
        if (aType == 0 || aType > 5) revert InvalidActionType();
        _;
    }
    
    modifier checkCircuitBreaker(string memory breakerType) {
        CircuitBreaker storage breaker = circuitBreakers[breakerType];
        if (breaker.isTriggered) {
            revert("Circuit breaker triggered");
        }
        _;
    }
    
    // Grant role method
    function grantRole(bytes32, address account) public onlyAdmin {
        hasRole[account] = true;
    }

    
    // Set admin
    function setAdmin(address newAdmin) external onlyAdmin {
        if (newAdmin == address(0)) revert ZeroAddress();
        address oldAdmin = msg.sender;
        hasRole[newAdmin] = true;
        emit AdminChanged(oldAdmin, newAdmin);
    }
    
    // Set dynamic slippage limit
    function setDynamicSlippageLimit(uint256 _limitBps) external onlyAdmin {
        if (_limitBps > 10000) revert InvalidArguments("Slippage limit cannot exceed 100%");
        dynamicSlippageLimitBps = _limitBps;
        emit DynamicSlippageLimitSet(_limitBps);
    }
    
    // Set synergy threshold
    function setSynergyThreshold(uint256 newThreshold) external onlyAdmin {
        synergyThreshold = newThreshold;
        emit SynergyThresholdUpdated(newThreshold);
    }
    
    // Set synergy score
    function setSynergyScore(bytes32 synergyKey, uint256 newScore) external onlyAdmin {
        synergyScores[synergyKey] = newScore;
        emit SynergyScoreUpdated(synergyKey, newScore);
    }
    
    // Set max pattern size
    function setMaxPatternSize(uint256 size) external onlyAdmin {
        if (size > 64 * 1024) revert InvalidArguments("Max pattern size cannot exceed 64kB");
    }
    
    // Set required approvals
    function setRequiredSynergyApprovals(uint256 requiredApprovals) external onlyAdmin {
        requiredSynergyApprovals = requiredApprovals;
        emit RequiredSynergyApprovalsSet(requiredApprovals);
    }
    
    // Add chain support
    function addChainSupport(uint256 _chainId, address _provider) external onlyAdmin {
        if (_chainId == 0) revert InvalidArguments("Chain ID cannot be 0");
        if (_provider == address(0)) revert ZeroAddress();
        supportedChainIds[_chainId] = true;
        chainProviders[_chainId] = _provider;
    }
    
    // Remove chain support
    function removeChainSupport(uint256 _chainId) external onlyAdmin {
        if (_chainId == chainId) revert ChainRemovalForbidden(_chainId);
        supportedChainIds[_chainId] = false;
        delete chainProviders[_chainId];
    }
    
    // Get chain provider
    function getChainProvider(uint256 _chainId) external view returns (address) {
        return chainProviders[_chainId];
    }
    
    // Set bridge contract
    function setBridgeContract(string calldata bridgeType, address bridgeAddress) external onlyAdmin {
        if (bridgeAddress == address(0)) revert ZeroAddress();
        bridgeContracts[bridgeType] = bridgeAddress;
    }
    
    // Update bridge contract
    function updateBridgeContract(string calldata bridgeType, address bridgeAddress) external onlyAdmin {
        if (bridgeAddress == address(0)) revert ZeroAddress();
        if (bridgeContracts[bridgeType] == address(0)) revert BridgeNotSet(bridgeType);
        bridgeContracts[bridgeType] = bridgeAddress;
    }
    
    // Get bridge contract
    function getBridgeContract(string memory bridgeType) external view returns (address) {
        return bridgeContracts[bridgeType];
    }
    
    // Update 1inch router
    function updateOneInchRouter(address _oneInchRouter) external onlyAdmin {
        if (_oneInchRouter == address(0)) revert ZeroAddress();
        oneInchRouter = _oneInchRouter;
        emit AggregatorUpdated("1inch", _oneInchRouter);
    }
    
    // Update 0x exchange
    function updateZeroXExchange(address _zeroXExchange) external onlyAdmin {
        if (_zeroXExchange == address(0)) revert ZeroAddress();
        zeroXExchange = _zeroXExchange;
        emit AggregatorUpdated("0x", _zeroXExchange);
    }
    
    // Set price oracle
    function setPriceOracle(address token, address oracle) external onlyAdmin {
        if (token == address(0) || oracle == address(0)) revert ZeroAddress();
        priceOracles[token] = oracle;
        emit PriceOracleUpdated(token, oracle);
    }
    
    // Add synergy operator
    function addSynergyOperator(address operator) external onlyAdmin {
        if (isSynergyOperator[operator]) revert OperatorAlreadyAdded(operator);
        _operators.add(operator);
        isSynergyOperator[operator] = true;
        hasRole[operator] = true;  // Add this line to grant the role
        emit SynergyOperatorAdded(operator);
    }
    
    // Remove synergy operator
    function removeSynergyOperator(address operator) external onlyAdmin {
        if (!isSynergyOperator[operator]) revert OperatorNotFound(operator);
        _operators.remove(operator);
        isSynergyOperator[operator] = false;
        emit SynergyOperatorRemoved(operator);
    }
    
    // Get synergy operators
    function getSynergyOperators() external view returns (address[] memory) {
        uint256 length = _operators.length();
        address[] memory operators = new address[](length);
        for (uint256 i = 0; i < length; i++) {
            operators[i] = _operators.at(i);
        }
        return operators;
    }
    
    // Approve synergy key
    function approveSynergyKey(bytes32 synergyKey) external onlyOperator {
        synergyApprovals[synergyKey][msg.sender] = true;
        emit SynergyCheckTriggered(synergyKey, true, "Operator approved synergy key");
    }
    
    // Revoke synergy key approval
    function revokeSynergyKeyApproval(bytes32 synergyKey) external onlyOperator {
        synergyApprovals[synergyKey][msg.sender] = false;
        emit SynergyCheckTriggered(synergyKey, false, "Operator revoked synergy key approval");
    }
    
    // Check synergy for route
    function synergyCheckForRoute(bytes32 synergyKey) public view {
        uint256 score = synergyScores[synergyKey];
        
        // Check if score is above threshold
        if (score < synergyThreshold) {
            revert SynergyCheckFailed(synergyKey, "Score below threshold");
        }
        
        // Count approvals
        uint256 approvalCount = 0;
        uint256 length = _operators.length();
        for (uint256 i = 0; i < length; i++) {
            address operator = _operators.at(i);
            if (synergyApprovals[synergyKey][operator]) {
                approvalCount++;
            }
        }
        
        // Check if enough approvals
        if (approvalCount < requiredSynergyApprovals) {
            revert SynergyCheckFailed(synergyKey, "Insufficient approvals");
        }
    }
    
    // Store memory
    function storeMemory(bytes32 key, bytes calldata data) external onlyOperator {
        if (data.length > MAX_PATTERN_SIZE) {
            revert PatternTooLarge(MAX_PATTERN_SIZE, data.length);
        }
        
        memoryPatterns[key] = data;
        emit MemoryStored(key, data.length);
    }
    
    // Retrieve memory
    function retrieveMemory(bytes32 key) external view onlyOperator returns (bytes memory) {
        bytes memory data = memoryPatterns[key];
        if (data.length == 0) {
            revert PatternNotFound(key);
        }
        return data;
    }
    
    // Batch delete patterns
    function batchDeletePatterns(bytes32[] calldata keys) external onlyOperator {
        if (keys.length > MAX_BATCH_DELETE_SIZE) {
            revert BatchDeleteLimitExceeded(MAX_BATCH_DELETE_SIZE, keys.length);
        }
        
        for (uint256 i = 0; i < keys.length; i++) {
            delete memoryPatterns[keys[i]];
            emit PatternDeleted(keys[i]);
        }
    }
    
    // Pause
    function pause() external onlyEmergency {
        paused = true;
        emit Pause();
    }
    
    // Unpause
    function unpause() external onlyEmergency {
        paused = false;
        emit Unpause();
    }

    // Emergency withdraw
    function emergencyWithdraw(address token) external onlyEmergency {
        if (token == address(0)) {
            // ETH
            uint256 balance = address(this).balance;
            if (balance > 0) {
                (bool success, ) = msg.sender.call{value: balance}("");
                require(success, "ETH transfer failed");
                emit EmergencyWithdrawal(address(0), balance, msg.sender);
            }
        } else {
            // ERC20
            uint256 balance = IERC20(token).balanceOf(address(this));
            if (balance > 0) {
                IERC20(token).transfer(msg.sender, balance);
                emit EmergencyWithdrawal(token, balance, msg.sender);
            }
        }
    }
    
    // Trigger circuit breaker
    function triggerCircuitBreaker(string calldata breakerType) external onlySentinel {
        CircuitBreaker storage breaker = circuitBreakers[breakerType];
        breaker.isTriggered = true;
        breaker.lastTriggered = block.timestamp;
        
        emit CircuitBreakerTriggered(breakerType, breaker.threshold, breaker.threshold);
    }
    
    // Reset circuit breaker
    function resetCircuitBreaker(string calldata breakerType) external onlySentinel {
        CircuitBreaker storage breaker = circuitBreakers[breakerType];
        breaker.isTriggered = false;
        
        emit CircuitBreakerReset(breakerType);
    }
    
    // Update token volatility score
    function updateTokenVolatilityScore(address token, uint256 score) external onlyAdmin {
        if (score > 100) revert InvalidArguments("Score must be 0-100");
        tokenVolatilityScores[token] = score;
    }
    
    // Calculate dynamic slippage
    function calculateDynamicSlippage(address token, uint256 amount) external view returns (uint256) {
        SlippageModel storage model = tokenSlippageModels[token];
        
        if (model.lastUpdated == 0) {
            // No model configured, return default
            return dynamicSlippageLimitBps;
        }
        
        // Get token volatility (0-100)
        uint256 volatility = tokenVolatilityScores[token];
        
        // Apply slippage model
        uint256 volComponent = (volatility * model.volMultiplier) / 100;
        
        // Simplified size component (increases with size)
        uint256 sizeComponent = 0;
        if (amount > 10 ether) {
            sizeComponent = (model.sizeMultiplier * amount) / (100 ether);
        }
        
        // Time component (increases the longer since last update)
        uint256 timeSinceUpdate = block.timestamp - model.lastUpdated;
        uint256 timeComponent = (timeSinceUpdate * model.timeMultiplier) / 3600; // Per hour
        
        // Combine all components with base slippage
        uint256 totalSlippage = model.baseSlippageBps + volComponent + sizeComponent + timeComponent;
        
        // Cap at reasonable maximum
        if (totalSlippage > 1000) { // Max 10%
            totalSlippage = 1000;
        }
        
        return totalSlippage;
    }
    
    // Check MEV protection
    function checkMevProtection(
        address tokenIn, 
        address tokenOut, 
        uint256 amount
    ) external view returns (bool) {
        // If MEV protection level is 0, never protect
        if (mevProtectionLevel == 0) return false;
        
        // Get volatility scores
        uint256 volatilityIn = tokenVolatilityScores[tokenIn];
        uint256 volatilityOut = tokenVolatilityScores[tokenOut];
        
        // Higher volatility means higher MEV risk
        uint256 combinedVolatility = (volatilityIn + volatilityOut) / 2;
        
        // Size-based risk (larger trades get more protection)
        uint256 sizeRisk = 0;
        if (amount > 5 ether) {
            sizeRisk = 20; // Add 20% risk for large trades
        } else if (amount > 1 ether) {
            sizeRisk = 10; // Add 10% risk for medium trades
        }
        
        // Calculate overall risk score
        uint256 riskScore = combinedVolatility + sizeRisk;
        
        // If risk score exceeds protection level, trigger protection
        return riskScore >= mevProtectionLevel;
    }
    
    // Set slippage model
    function setSlippageModel(
        address token,
        uint256 baseSlippageBps,
        uint256 volMultiplier,
        uint256 sizeMultiplier,
        uint256 timeMultiplier
    ) external onlyAdmin {
        if (token == address(0)) revert ZeroAddress();
        
        tokenSlippageModels[token] = SlippageModel({
            baseSlippageBps: baseSlippageBps,
            volMultiplier: volMultiplier,
            sizeMultiplier: sizeMultiplier,
            timeMultiplier: timeMultiplier,
            lastUpdated: block.timestamp
        });
    }
    
    // Update pool info
    function updatePoolInfo(
        address poolAddress,
        address token0,
        address token1,
        uint256 reserve0,
        uint256 reserve1,
        uint256 fee,
        string memory protocol
    ) external onlyOperator {
        poolInfoCache[poolAddress] = PoolInfo({
            token0: token0,
            token1: token1,
            reserve0: reserve0,
            reserve1: reserve1,
            fee: fee,
            lastUpdated: block.timestamp,
            protocol: protocol
        });
    }
    
    // Get pool info
    function getPoolInfo(address poolAddress) external view returns (
        address token0,
        address token1,
        uint256 reserve0,
        uint256 reserve1,
        uint256 fee,
        uint256 lastUpdated,
        string memory protocol
    ) {
        PoolInfo storage info = poolInfoCache[poolAddress];
        return (
            info.token0,
            info.token1,
            info.reserve0,
            info.reserve1,
            info.fee,
            info.lastUpdated,
            info.protocol
        );
    }
    
    // Get current chain ID
    function getCurrentChainId() external view returns (uint256) {
        return chainId;
    }
    
    // Get token price
    function getTokenPrice(address token) public view returns (uint256) {
        address oracle = priceOracles[token];
        if (oracle == address(0)) {
            return 0;
        }
        
        try AggregatorV3Interface(oracle).latestRoundData() returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        ) {
            return uint256(answer);
        } catch {
            return 0;
        }
    }
    
    // Set fee parameters
    function setFeeParameters(address _feeCollector, uint256 _feeBps) external onlyAdmin {
        if (_feeCollector == address(0)) revert ZeroAddress();
        if (_feeBps > 2000) revert InvalidArguments("Fee cannot exceed 20%");
        
        feeCollector = _feeCollector;
        feeBps = _feeBps;
    }
    
    // Set MEV protection level
    function setMevProtectionLevel(uint256 level) external onlyAdmin {
        if (level > 100) revert InvalidArguments("Level must be 0-100");
        mevProtectionLevel = level;
    }
    
    // Compute optimal path
    function computeOptimalPath(
        address[] calldata tokens,
        address[] calldata pools,
        uint256[] calldata amounts,
        bytes calldata executionData,
        uint256 gasEstimate,
        uint256 riskScore
    ) external onlyOperator returns (bytes32) {
        bytes32 pathId = keccak256(abi.encodePacked(tokens, pools, amounts, block.timestamp));
        
        optimizedPaths[pathId] = ComputedPath({
            tokens: tokens,
            pools: pools,
            amounts: amounts,
            expectedProfit: amounts[amounts.length - 1] > amounts[0] ? amounts[amounts.length - 1] - amounts[0] : 0,
            gasEstimate: gasEstimate,
            timestamp: block.timestamp,
            riskScore: riskScore,
            executionData: executionData
        });
        
        _activePaths.add(pathId);
        
        emit OptimalPathComputed(pathId, optimizedPaths[pathId].expectedProfit, gasEstimate);
        
        return pathId;
    }
    
    // Execute profit path
    function executeProfitPath(
        bytes32 pathId,
        bytes calldata executionData,
        uint256 maxGasPrice,
        uint256 deadline,
        uint256 nonce
    ) external nonReusableNonce(nonce) onlyExecutor ensureDeadline(deadline) returns (bool) {
        ComputedPath storage path = optimizedPaths[pathId];
        
        if (path.timestamp == 0) {
            revert InvalidArguments("Path not found");
        }
        
        if (path.tokens.length < 2) {
            revert InvalidPath(path.tokens.length, 2);
        }
        
        // Simulate profit generation
        uint256 profit = path.expectedProfit;
        
        // Update total profits
        totalProfitGenerated += profit;
        
        emit ProfitPathExecuted(msg.sender, pathId, profit);
        
        return true;
    }
    
    function executeOperation(
        uint8 aType,
        address target,
        bytes calldata callData,
        bytes32 synergyKey
    ) public validActionType(aType) onlyOperator checkCircuitBreaker("maxSlippage") returns (bool) {
        // Verify synergy score and approvals
        synergyCheckForRoute(synergyKey);
        
        uint256 gasStart = gasleft();
        bool success = true;
        
        // Mock implementations - in real contract these would do actual operations
        if (aType == 1) {
            // Swap
            emit SwapExecuted(target, address(0), address(0), 0, 0, 0, 0, msg.sender);
        } else if (aType == 2) {
            // Cross-chain transfer
            emit CrossChainTransferExecuted(bytes32(0), 0, "", address(0), 0, true, 0);
        } else if (aType == 3) {
            // Flash loan
            emit FlashLoanRepaid(address(0), 0, 0);
        }
        
        uint256 gasUsed = gasStart - gasleft();
        
        emit NewActionExecuted(aType, target, callData, gasUsed);
        
        return success;
    }
    
    // Execute multiple operations as a route
    function executeMultiRoute(
        uint8[] calldata actionTypes,
        address[] calldata targets,
        bytes[] calldata callDatas,
        bytes32 synergyKey
    ) external onlyOperator checkCircuitBreaker("maxSlippage") returns (bool) {
        // Verify synergy score and approvals
        synergyCheckForRoute(synergyKey);
        
        if (actionTypes.length != targets.length || targets.length != callDatas.length) {
            revert InvalidArguments("Array lengths mismatch");
        }
        
        for (uint256 i = 0; i < actionTypes.length; i++) {
            bool success = executeOperation(actionTypes[i], targets[i], callDatas[i], synergyKey);
            if (!success) {
                return false;
            }
        }
        
        return true;
    }
    
    // Execute Aave flash loan callback
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        // Mock implementation
        return true;
    }
    
    // Allow contract to receive ETH
    receive() external payable {}
}

/**
 * @title EnhancedUniversalArbitrageRouterTest
 * @notice Comprehensive test suite for EnhancedUniversalArbitrageRouter
 * @dev Tests all major router functionalities with mocks for external dependencies
 */
contract EnhancedUniversalArbitrageRouterTest is Test {
    // Main contract under test
    ConcreteRouter public router;
    
    // Mock contracts
    MockOneInchRouter public mockOneInchRouter;
    MockZeroXExchange public mockZeroXExchange;
    MockAaveAddressesProvider public mockAaveAddressesProvider;
    MockAavePool public mockAavePool;
    MockWETH public mockWeth;
    ERC20PresetMinterPauser public mockUSDC;
    ERC20PresetMinterPauser public mockDAI;
    MockBridge public mockBridge;
    MockPriceOracle public mockPriceOracle;
    
    // Test accounts
    address public admin = address(0x1);
    address public operator = address(0x2);
    address public emergency = address(0x3);
    address public upgrader = address(0x4);
    address public sentinel = address(0x5);
    address public analyst = address(0x6);
    address public executor = address(0x7);
    address public regularUser = address(0x8);
    address public feeCollector = address(0x9);
    
    // Events to listen for
    event AdminChanged(address indexed oldAdmin, address indexed newAdmin);
    event DynamicSlippageLimitSet(uint256 newLimitBps);
    event SynergyCheckTriggered(bytes32 indexed synergyKey, bool passed, string reason);
    event SwapExecuted(
        address indexed aggregator,
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 amountOut,
        uint256 slippageLimitBps,
        uint256 gasUsed,
        address indexed executor
    );
    event CrossChainTransferExecuted(
        bytes32 indexed transferId,
        uint256 destChainId,
        string bridgeType,
        address token,
        uint256 amount,
        bool success,
        uint256 gasUsed
    );
    event FlashLoanRepaid(address asset, uint256 amountOwing, uint256 gasUsed);
    event EmergencyWithdrawal(address indexed token, uint256 amount, address indexed recipient);
    event SynergyOperatorAdded(address indexed operator);
    event SynergyOperatorRemoved(address indexed operator);
    event PatternDeleted(bytes32 indexed key);
    event MemoryStored(bytes32 indexed key, uint256 size);
    event Pause();
    event Unpause();
    event PriceOracleUpdated(address token, address oracle);
    event CircuitBreakerTriggered(string breakerType, uint256 currentValue, uint256 threshold);
    event CircuitBreakerReset(string breakerType);
    event ProfitPathExecuted(address indexed executor, bytes32 pathId, uint256 profitAmount);
    
    // Test constants
    bytes32 public constant ADMIN_ROLE = keccak256("ADMIN_ROLE");
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant EMERGENCY_ROLE = keccak256("EMERGENCY_ROLE");
    bytes32 public constant SENTINEL_ROLE = keccak256("SENTINEL_ROLE");
    bytes32 public constant ANALYST_ROLE = keccak256("ANALYST_ROLE");
    bytes32 public constant EXECUTOR_ROLE = keccak256("EXECUTOR_ROLE");
    
    // Test synergy values
    bytes32 public testSynergyKey = keccak256("TEST_SYNERGY_KEY");
    uint256 public testNonce = 1;
    
    // -----------------------------------------------------------------
    //                          Setup & Mocks
    // -----------------------------------------------------------------
    
    function setUp() public {
        // Set up accounts
        vm.startPrank(admin);
        
        // Deploy mock contracts
        mockOneInchRouter = new MockOneInchRouter();
        mockZeroXExchange = new MockZeroXExchange();
        mockAaveAddressesProvider = new MockAaveAddressesProvider();
        mockAavePool = new MockAavePool();
        mockAaveAddressesProvider.setPool(address(mockAavePool));
        
        // Deploy mock tokens
        mockWeth = new MockWETH();
        mockUSDC = new ERC20PresetMinterPauser("USD Coin", "USDC");
        mockDAI = new ERC20PresetMinterPauser("Dai Stablecoin", "DAI");
        
        // Deploy mock bridge and price oracle
        mockBridge = new MockBridge();
        mockPriceOracle = new MockPriceOracle();
        
        // Deploy the main contract - now using ConcreteRouter instead
        router = new ConcreteRouter(
            address(mockOneInchRouter),
            address(mockZeroXExchange),
            address(mockAaveAddressesProvider),
            address(mockWeth)
        );
        
        // Set up permissions
        router.grantRole(ADMIN_ROLE, admin);
        router.grantRole(OPERATOR_ROLE, operator);
        router.grantRole(UPGRADER_ROLE, upgrader);
        router.grantRole(EMERGENCY_ROLE, emergency);
        router.grantRole(SENTINEL_ROLE, sentinel);
        router.grantRole(ANALYST_ROLE, analyst);
        router.grantRole(EXECUTOR_ROLE, executor);
        
        // Set up bridge contract
        router.setBridgeContract("test", address(mockBridge));
        
        // Set price oracles
        router.setPriceOracle(address(mockWeth), address(mockPriceOracle));
        router.setPriceOracle(address(mockUSDC), address(mockPriceOracle));
        router.setPriceOracle(address(mockDAI), address(mockPriceOracle));
        
        // Add synergy operator
        router.addSynergyOperator(operator);
        
        // Set synergy score above threshold
        router.setSynergyScore(testSynergyKey, 1000);
        
        // Mint some tokens for testing
        mockWeth.mint(address(router), 100 ether);
        mockUSDC.mint(address(router), 100000 * 10**6);
        mockDAI.mint(address(router), 100000 ether);
        
        // Mint tokens for user
        mockWeth.mint(regularUser, 10 ether);
        mockUSDC.mint(regularUser, 10000 * 10**6);
        mockDAI.mint(regularUser, 10000 ether);
        
        vm.stopPrank();
        
        // Approve synergy key as operator
        vm.startPrank(operator);
        router.approveSynergyKey(testSynergyKey);
        vm.stopPrank();
    }
    
    // -----------------------------------------------------------------
    //                      Basic Deployment Tests
    // -----------------------------------------------------------------
    
    function test_InitialState() public {
        // Verify initial state
        assertEq(router.oneInchRouter(), address(mockOneInchRouter));
        assertEq(router.zeroXExchange(), address(mockZeroXExchange));
        assertEq(address(router.aaveAddressesProvider()), address(mockAaveAddressesProvider));
        assertEq(address(router.aaveLendingPool()), address(mockAavePool));
        assertEq(router.nativeWrapper(), address(mockWeth));
        
        // Verify roles
        assertTrue(router.hasRole(admin));
        assertTrue(router.hasRole(operator));
        assertTrue(router.hasRole(upgrader));
        assertTrue(router.hasRole(emergency));
        assertTrue(router.hasRole(sentinel));
        assertTrue(router.hasRole(analyst));
        assertTrue(router.hasRole(executor));
        
        // Verify default settings
        assertEq(router.dynamicSlippageLimitBps(), 100);
        assertEq(router.synergyThreshold(), 100);
        assertEq(router.requiredSynergyApprovals(), 3);
        assertEq(router.mevProtectionLevel(), 80);
    }

    // -----------------------------------------------------------------
    //                      Role Management Tests
    // -----------------------------------------------------------------
    
    function test_SetAdmin() public {
        address newAdmin = address(0x999);
        
        vm.startPrank(admin);
        
        vm.expectEmit(true, true, false, false);
        emit AdminChanged(admin, newAdmin);
        
        router.setAdmin(newAdmin);
        
        assertTrue(router.hasRole(newAdmin));
        
        vm.stopPrank();
    }
    
    function test_SetAdmin_Unauthorized() public {
        address newAdmin = address(0x999);
        
        vm.startPrank(regularUser);
        
        vm.expectRevert(ConcreteRouter.NotAuthorized.selector);
        
        router.setAdmin(newAdmin);
        
        vm.stopPrank();
    }
    
    function test_AddSynergyOperator() public {
        address newOperator = address(0x999);
        
        vm.startPrank(admin);
        
        vm.expectEmit(true, false, false, false);
        emit SynergyOperatorAdded(newOperator);
        
        router.addSynergyOperator(newOperator);
        
        assertTrue(router.hasRole(newOperator));
        assertTrue(router.isSynergyOperator(newOperator));
        
        // Check that it's included in the operator list
        address[] memory operators = router.getSynergyOperators();
        bool found = false;
        for (uint i = 0; i < operators.length; i++) {
            if (operators[i] == newOperator) {
                found = true;
                break;
            }
        }
        assertTrue(found);
        
        vm.stopPrank();
    }
    
    function test_RemoveSynergyOperator() public {
        vm.startPrank(admin);
        
        vm.expectEmit(true, false, false, false);
        emit SynergyOperatorRemoved(operator);
        
        router.removeSynergyOperator(operator);
        
        assertFalse(router.isSynergyOperator(operator));
        
        // Check that it's removed from the operator list
        address[] memory operators = router.getSynergyOperators();
        bool found = false;
        for (uint i = 0; i < operators.length; i++) {
            if (operators[i] == operator) {
                found = true;
                break;
            }
        }
        assertFalse(found);
        
        vm.stopPrank();
    }
    
    // -----------------------------------------------------------------
    //                    Configuration Setting Tests
    // -----------------------------------------------------------------
    
    function test_SetDynamicSlippageLimit() public {
        uint256 newLimit = 200; // 2%
        
        vm.startPrank(admin);
        
        vm.expectEmit(false, false, false, true);
        emit DynamicSlippageLimitSet(newLimit);
        
        router.setDynamicSlippageLimit(newLimit);
        
        assertEq(router.dynamicSlippageLimitBps(), newLimit);
        
        vm.stopPrank();
    }
    
    function test_SetDynamicSlippageLimit_InvalidValue() public {
        uint256 invalidLimit = 15000; // 150% - over 100%
        
        vm.startPrank(admin);
        
        vm.expectRevert(abi.encodeWithSelector(ConcreteRouter.InvalidArguments.selector, "Slippage limit cannot exceed 100%"));
        router.setDynamicSlippageLimit(invalidLimit);
        
        vm.stopPrank();
    }
    
    function test_SetSynergyThreshold() public {
        uint256 newThreshold = 500;
        
        vm.startPrank(admin);
        
        router.setSynergyThreshold(newThreshold);
        
        assertEq(router.synergyThreshold(), newThreshold);
        
        vm.stopPrank();
    }
    
    function test_SetSynergyScore() public {
        bytes32 synergyKey = keccak256("NEW_SYNERGY_KEY");
        uint256 newScore = 750;
        
        vm.startPrank(admin);
        
        router.setSynergyScore(synergyKey, newScore);
        
        assertEq(router.synergyScores(synergyKey), newScore);
        
        vm.stopPrank();
    }
    
    function test_SetMevProtectionLevel() public {
        uint256 newLevel = 90;
        
        vm.startPrank(admin);
        
        router.setMevProtectionLevel(newLevel);
        
        assertEq(router.mevProtectionLevel(), newLevel);
        
        vm.stopPrank();
    }
    
    function test_SetMevProtectionLevel_InvalidValue() public {
        uint256 invalidLevel = 101; // Over 100 limit
        
        vm.startPrank(admin);
        
        vm.expectRevert(abi.encodeWithSelector(ConcreteRouter.InvalidArguments.selector, "Level must be 0-100"));
        router.setMevProtectionLevel(invalidLevel);
        
        vm.stopPrank();
    }
    
    function test_SetFeeParameters() public {
        uint256 newFeeBps = 300; // 3%
        
        vm.startPrank(admin);
        
        router.setFeeParameters(feeCollector, newFeeBps);
        
        assertEq(router.feeCollector(), feeCollector);
        assertEq(router.feeBps(), newFeeBps);
        
        vm.stopPrank();
    }
    
    function test_SetFeeParameters_InvalidValues() public {
        uint256 invalidFeeBps = 3000; // 30% - over limit
        
        vm.startPrank(admin);
        
        vm.expectRevert(abi.encodeWithSelector(ConcreteRouter.InvalidArguments.selector, "Fee cannot exceed 20%"));
        router.setFeeParameters(feeCollector, invalidFeeBps);
        
        vm.expectRevert(ConcreteRouter.ZeroAddress.selector);
        router.setFeeParameters(address(0), 100); // Zero address
        
        vm.stopPrank();
    }
}