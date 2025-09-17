// SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;

import "contracts/uar.sol";
import "lib/openzeppelin-contracts/contracts/proxy/transparent/TransparentUpgradeableProxy.sol";
import "lib/openzeppelin-contracts/contracts/proxy/transparent/ProxyAdmin.sol";
import {MockAaveAddressesProvider} from "../contracts/MockAaveAddressesProvider.sol";
import {MockERC20} from "../contracts/MockERC20.sol";
import {IERC20} from "lib/openzeppelin-contracts/contracts/token/ERC20/IERC20.sol";
import "forge-std/console2.sol";

/**
 * @title UniversalArbitrageDeployer
 * @notice Deploys the Enhanced Universal Arbitrage Router with proper configuration for different networks
 * @dev Supports deployment on Ethereum mainnet, Polygon, Arbitrum, Optimism, and BSC
 */
contract UniversalArbitrageDeployer {
    struct NetworkConfig {
        address oneInchRouter;
        address zeroXExchange;
        address aaveAddressesProvider;
        address aavePool;
        address balancerVault;
        address nativeWrapper;
    }

    mapping(uint256 => NetworkConfig) public networkConfigs;
    ProxyAdmin public immutable proxyAdmin;

    event ProxyDeployed(address indexed implementation, address indexed proxy, address indexed admin, uint256 chainId);
    event RouterDeployed(address indexed router, uint256 chainId);
    event NetworkSupportAdded(uint256 indexed chainId, address oneInchRouter, address zeroXExchange, address aaveAddressesProvider, address aavePool, address balancerVault, address nativeWrapper);
    event ProxyAdminOwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    modifier onlyProxyAdminOwner() {
        require(msg.sender == proxyAdmin.owner(), "UniversalArbitrageDeployer: Not authorized");
        _;
    }

    constructor() {
        proxyAdmin = new ProxyAdmin(msg.sender);

        // Ethereum Mainnet (ChainID: 1)
        networkConfigs[1] = NetworkConfig({
            oneInchRouter: 0x1111111254EEB25477B68fb85Ed929f73A960582,
            zeroXExchange: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF,
            aaveAddressesProvider: 0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e,
            aavePool: 0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9,
            balancerVault: 0xBA12222222228d8Ba445958a75a0704d566BF2C8,
            nativeWrapper: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
        });

        // Polygon (ChainID: 137)
        networkConfigs[137] = NetworkConfig({
            oneInchRouter: 0x1111111254EEB25477B68fb85Ed929f73A960582,
            zeroXExchange: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF,
            aaveAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb,
            aavePool: 0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf,
            balancerVault: 0xBA12222222228d8Ba445958a75a0704d566BF2C8,
            nativeWrapper: 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270
        });

        // Arbitrum (ChainID: 42161)
        networkConfigs[42161] = NetworkConfig({
            oneInchRouter: 0x1111111254EEB25477B68fb85Ed929f73A960582,
            zeroXExchange: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF,
            aaveAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb,
            aavePool: 0x794a61358D6845594F94dc1DB02A252b5b4814aD,
            balancerVault: 0xBA12222222228d8Ba445958a75a0704d566BF2C8,
            nativeWrapper: 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1
        });

        // Optimism (ChainID: 10)
        networkConfigs[10] = NetworkConfig({
            oneInchRouter: 0x1111111254EEB25477B68fb85Ed929f73A960582,
            zeroXExchange: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF,
            aaveAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb,
            aavePool: 0x794a61358D6845594F94dc1DB02A252b5b4814aD,
            balancerVault: 0xBA12222222228d8Ba445958a75a0704d566BF2C8,
            nativeWrapper: 0x4200000000000000000000000000000000000006
        });

        // BSC (ChainID: 56)
        networkConfigs[56] = NetworkConfig({
            oneInchRouter: 0x1111111254EEB25477B68fb85Ed929f73A960582,
            zeroXExchange: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF,
            aaveAddressesProvider: 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5,
            aavePool: 0x0000000000000000000000000000000000000000, // Aave not deployed on BSC
            balancerVault: 0x0000000000000000000000000000000000000000, // Balancer not deployed on BSC
            nativeWrapper: 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c
        });

        // Anvil (ChainID: 31337)
        networkConfigs[31337] = NetworkConfig({
            oneInchRouter: 0x1000000000000000000000000000000000000001,
            zeroXExchange: 0x1000000000000000000000000000000000000002,
            aaveAddressesProvider: 0x1000000000000000000000000000000000000003,
            aavePool: 0x1000000000000000000000000000000000000005,
            balancerVault: 0x1000000000000000000000000000000000000006,
            nativeWrapper: 0x1000000000000000000000000000000000000004
        });
    }

    /**
     * @notice Deploys the router with a proxy for upgradeability
     * @return router The deployed router implementation
     * @return proxy The deployed proxy
     */
    function deployWithProxy() external returns (UniversalArbitrageExecutor router, TransparentUpgradeableProxy proxy) {
        NetworkConfig memory config = networkConfigs[block.chainid];
        require(config.aavePool != address(0), "UniversalArbitrageDeployer: Network not supported");

        router = new UniversalArbitrageExecutor(config.aavePool, config.balancerVault, config.nativeWrapper);

        proxy = new TransparentUpgradeableProxy(
            address(router),
            address(proxyAdmin),
            ""
        );

        emit ProxyDeployed(address(router), address(proxy), proxyAdmin.owner(), block.chainid);
        return (router, proxy);
    }

    /**
     * @notice Deploys the router without a proxy (direct implementation)
     * @return router The deployed router
     */
    function deployWithoutProxy() public returns (UniversalArbitrageExecutor router) {
        NetworkConfig memory config = networkConfigs[block.chainid];
        require(config.aavePool != address(0), "UniversalArbitrageDeployer: Network not supported");

        router = new UniversalArbitrageExecutor(config.aavePool, config.balancerVault, config.nativeWrapper);

        console2.log("Router deployed at:", address(router));
        emit RouterDeployed(address(router), block.chainid);
        return router;
    }

    /**
     * @notice Adds support for a new network
     * @param chainId Chain ID of the network
     * @param config Network configuration
     */
    function addNetworkSupport(uint256 chainId, NetworkConfig calldata config) external onlyProxyAdminOwner {
        require(config.oneInchRouter != address(0), "UniversalArbitrageDeployer: Invalid 1inch router");
        require(config.zeroXExchange != address(0), "UniversalArbitrageDeployer: Invalid 0x exchange");
        require(config.aaveAddressesProvider != address(0), "UniversalArbitrageDeployer: Invalid Aave provider");
        require(config.aavePool != address(0), "UniversalArbitrageDeployer: Invalid Aave pool");
        require(config.balancerVault != address(0), "UniversalArbitrageDeployer: Invalid Balancer vault");
        require(config.nativeWrapper != address(0), "UniversalArbitrageDeployer: Invalid native wrapper");

        networkConfigs[chainId] = config;
        emit NetworkSupportAdded(chainId, config.oneInchRouter, config.zeroXExchange, config.aaveAddressesProvider, config.aavePool, config.balancerVault, config.nativeWrapper);
    }

    /**
     * @notice Transfers ownership of the proxy admin
     * @param newOwner New owner address
     */
    function transferProxyAdminOwnership(address newOwner) external onlyProxyAdminOwner {
        require(newOwner != address(0), "UniversalArbitrageDeployer: New owner is zero address");
        address previousOwner = proxyAdmin.owner();
        proxyAdmin.transferOwnership(newOwner);
        emit ProxyAdminOwnershipTransferred(previousOwner, newOwner);
    }

    /// @notice Foundry entrypoint for automated deployment. Deploys the mock Aave provider and router for local/CI use.
    function run() external {
        // 1. Deploy the mock Aave pool provider
        MockAaveAddressesProvider provider = new MockAaveAddressesProvider();
        
        // 2. Deploy a mock WETH (ERC20)
        MockERC20 mockWETH = new MockERC20("Mock WETH", "WETH", 18);
        
        // 3. Deploy mock Aave Pool and Balancer Vault for testing
        MockAavePool mockAavePool = new MockAavePool();
        MockBalancerVault mockBalancerVault = new MockBalancerVault();
        
        // 4. Update the config for Anvil to use the deployed mocks
        networkConfigs[31337].aaveAddressesProvider = address(provider);
        networkConfigs[31337].aavePool = address(mockAavePool);
        networkConfigs[31337].balancerVault = address(mockBalancerVault);
        networkConfigs[31337].nativeWrapper = address(mockWETH);
        
        // 5. Deploy the router using the correct parameters
        UniversalArbitrageExecutor router = deployWithoutProxy();
        
        console2.log("Mock Aave Pool deployed at:", address(mockAavePool));
        console2.log("Mock Balancer Vault deployed at:", address(mockBalancerVault));
        console2.log("Mock WETH deployed at:", address(mockWETH));
        console2.log("Router deployed at:", address(router));
        
        emit RouterDeployed(address(router), block.chainid);
    }
}

/**
 * @title MockAavePool
 * @notice Mock implementation of Aave Pool for testing
 */
contract MockAavePool {
    function flashLoan(
        address receiverAddress,
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata interestRateModes,
        address onBehalfOf,
        bytes calldata params,
        uint16 referralCode
    ) external {
        // Mock implementation - just call the receiver
        IFlashLoanReceiver(receiverAddress).executeOperation(
            assets,
            amounts,
            new uint256[](assets.length), // premiums
            address(this),
            params
        );
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
        uint256[] memory interestRateModes = new uint256[](1);
        
        assets[0] = asset;
        amounts[0] = amount;
        interestRateModes[0] = 0;
        
        // Call executeOperation directly to avoid recursion
        IFlashLoanReceiver(receiverAddress).executeOperation(
            assets,
            amounts,
            new uint256[](1), // premiums
            address(this),
            params
        );
    }
}

/**
 * @title MockBalancerVault
 * @notice Mock implementation of Balancer Vault for testing
 */
contract MockBalancerVault {
    function flashLoan(
        address recipient,
        address[] calldata tokens,
        uint256[] calldata amounts,
        bytes calldata userData
    ) external {
        // Mock implementation - just call the recipient
        // For Balancer, we need to call receiveFlashLoan directly since it's not in the interface
        UniversalArbitrageExecutor(payable(recipient)).receiveFlashLoan(
            _toIERC20Array(tokens),
            amounts,
            new uint256[](tokens.length), // feeAmounts
            userData
        );
    }
    
    function getPoolTokens(bytes32 poolId) external pure returns (
        address[] memory tokens,
        uint256[] memory balances,
        uint256 lastChangeBlock
    ) {
        // Mock implementation
        tokens = new address[](0);
        balances = new uint256[](0);
        lastChangeBlock = 0;
    }
    
    function _toIERC20Array(address[] calldata addresses) private pure returns (IERC20[] memory) {
        IERC20[] memory tokens = new IERC20[](addresses.length);
        for (uint256 i = 0; i < addresses.length; i++) {
            tokens[i] = IERC20(addresses[i]);
        }
        return tokens;
    }
}