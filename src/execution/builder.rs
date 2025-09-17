//! ExecutionBuilder - protocol-aware calldata and transaction builder for all supported DEX and MEV strategies

use crate::{
    errors::MEVEngineError,
    mempool::{FlashLoanOpportunity, SwapParams, PotentialLiquidationOpportunity},
    types::{MintParams, ProtocolType, UARActionType, PoolStateFromSwaps},
    pool_manager::PoolManagerTrait,
};
use ethers::{
    abi::{Function, HumanReadableParser, Token},
    core::types::{Address, Bytes, U256, NameOrAddress},
    types::transaction::eip2718::TypedTransaction,
};
use dashmap::DashMap;
use std::sync::Arc;
use std::str::FromStr;

/// Parameters for a swap operation
#[derive(Debug, Clone)]
struct SwapParameters {
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    amount_out_min: U256,
    recipient: Address,
    deadline: U256,
    path: Vec<Address>,
}

/// Execution step for complex arbitrage
#[derive(Debug, Clone)]
struct ExecutionStep {
    target: Address,
    value: U256,
    data: Vec<u8>,
}

/// Centralized builder for encoding transaction calldata for all supported protocols and strategies.
/// This is the only place in the system that should ever encode calldata for execution.
pub struct ExecutionBuilder {
    pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>,
    function_abi_cache: Arc<DashMap<String, Arc<Function>>>,
}

impl ExecutionBuilder {
    pub fn new(pool_manager: Arc<dyn PoolManagerTrait + Send + Sync>) -> Self {
        let function_abi_cache = Arc::new(DashMap::new());
        Self {
            pool_manager,
            function_abi_cache,
        }
    }

    /// Build calldata for a router swap, dispatching to the correct protocol-specific builder.
    pub async fn build_router_swap_calldata(
        &self,
        chain_name: &str,
        router_address: Address,
        path: &[Address],
        amount_in: U256,
        amount_out_min: U256,
        recipient: Address,
        is_native_in: bool,
        is_native_out: bool,
        block_number: Option<u64>,
    ) -> Result<Bytes, MEVEngineError> {
        if path.len() < 2 {
            return Err(MEVEngineError::CalldataError("Swap path must have at least 2 tokens".to_string()));
        }

        let protocol_type = self.detect_router_protocol(router_address).await?;

        match protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                self.build_v2_swap_calldata(
                    path,
                    amount_in,
                    amount_out_min,
                    recipient,
                    is_native_in,
                    is_native_out,
                )
            }
            ProtocolType::UniswapV3 => {
                self.encode_v3_swap_calldata(
                    chain_name,
                    path,
                    amount_in,
                    amount_out_min,
                    recipient,
                    block_number,
                ).await
            }
            _ => Err(MEVEngineError::CalldataError(format!("Unsupported protocol type: {:?}", protocol_type))),
        }
    }

    /// Build calldata for Uniswap V2/SushiSwap style routers.
    fn build_v2_swap_calldata(
        &self,
        path: &[Address],
        amount_in: U256,
        amount_out_min: U256,
        recipient: Address,
        is_native_in: bool,
        is_native_out: bool,
    ) -> Result<Bytes, MEVEngineError> {
        let function_name = if is_native_in {
            "swapExactETHForTokens"
        } else if is_native_out {
            "swapExactTokensForETH"
        } else {
            "swapExactTokensForTokens"
        };

        let tokens = if is_native_in {
            vec![
                Token::Uint(amount_out_min),
                Token::Array(path.iter().map(|&a| Token::Address(a)).collect()),
                Token::Address(recipient),
                Token::Uint(U256::MAX), // deadline
            ]
        } else {
            vec![
                Token::Uint(amount_in),
                Token::Uint(amount_out_min),
                Token::Array(path.iter().map(|&a| Token::Address(a)).collect()),
                Token::Address(recipient),
                Token::Uint(U256::MAX), // deadline
            ]
        };

        self.encode_function_call(function_name, &tokens)
    }

    /// Build calldata for Uniswap V3 routers, handling both single-hop and multi-hop.
    pub async fn encode_v3_swap_calldata(
        &self,
        chain_name: &str,
        path: &[Address],
        amount_in: U256,
        amount_out_min: U256,
        recipient: Address,
        block_number: Option<u64>,
    ) -> Result<Bytes, MEVEngineError> {
        if path.len() == 2 {
            // Single-hop: exactInputSingle
            let pool_addresses = self.pool_manager
                .get_pools_for_pair(chain_name, path[0], path[1])
                .await
                .map_err(|e| MEVEngineError::SimulationError(format!("Failed to get pool info: {}", e)))?;
            let pool_address = pool_addresses.into_iter().next()
                .ok_or_else(|| MEVEngineError::SimulationError("No pool found for token pair".to_string()))?;

            let pool_state = self.pool_manager
                .get_pool_info(chain_name, pool_address, block_number)
                .await
                .map_err(|e| MEVEngineError::SimulationError(format!("Failed to get pool state: {}", e)))?;

            let fee = pool_state.fee_bps;

            let params = Token::Tuple(vec![
                Token::Address(path[0]), // tokenIn
                Token::Address(path[1]), // tokenOut
                Token::Uint(U256::from(fee)), // fee
                Token::Address(recipient), // recipient
                Token::Uint(U256::MAX), // deadline
                Token::Uint(amount_in), // amountIn
                Token::Uint(amount_out_min), // amountOutMinimum
                Token::Uint(U256::zero()), // sqrtPriceLimitX96 (0 for no limit)
            ]);

            self.encode_function_call("exactInputSingle", &[params])
        } else {
            // Multi-hop: exactInput
            let encoded_path = self.encode_v3_path(chain_name, path, block_number).await?;

            let params = Token::Tuple(vec![
                Token::Bytes(encoded_path),
                Token::Address(recipient),
                Token::Uint(U256::MAX), // deadline
                Token::Uint(amount_in),
                Token::Uint(amount_out_min),
            ]);

            self.encode_function_call("exactInput", &[params])
        }
    }

    /// Encode the packed path for Uniswap V3 multi-hop swaps.
    pub async fn encode_v3_path(
        &self,
        chain_name: &str,
        path: &[Address],
        block_number: Option<u64>,
    ) -> Result<Vec<u8>, MEVEngineError> {
        if path.len() < 2 {
            return Err(MEVEngineError::CalldataError("Path must have at least 2 tokens".to_string()));
        }

        let mut encoded_path = Vec::with_capacity(path.len() * 20 + (path.len() - 1) * 3);
    
        for i in 0..path.len() - 1 {
            encoded_path.extend_from_slice(path[i].as_bytes());
    
            let pools = self.pool_manager
                .get_pools_for_pair(chain_name, path[i], path[i+1])
                .await
                .map_err(|e| MEVEngineError::SimulationError(format!("Failed to get pools: {}", e)))?;
    
            let pool_address = pools.into_iter().next()
                .ok_or_else(|| MEVEngineError::SimulationError(format!("No pool found for pair {} - {}", path[i], path[i+1])))?;
    
            let pool_info = self.pool_manager
                .get_pool_info(chain_name, pool_address, block_number)
                .await
                .map_err(|e| MEVEngineError::SimulationError(format!("Failed to get pool info for {}: {}", pool_address, e)))?;
    
            // Fee is u24, so we take 3 bytes
            let fee_bytes = pool_info.fee_bps.to_be_bytes();
            encoded_path.extend_from_slice(&fee_bytes[1..]);
        }
    
        encoded_path.extend_from_slice(path[path.len() - 1].as_bytes());
    
        Ok(encoded_path)
    }

    /// Build calldata for a direct liquidation. Handles protocol-specific logic for supported liquidation strategies.
    pub async fn build_direct_liquidation_calldata(
        &self,
        opp: &PotentialLiquidationOpportunity,
        sender_address: Address,
    ) -> Result<(Address, Bytes, U256), MEVEngineError> {
        if sender_address == Address::zero() {
            return Err(MEVEngineError::ValidationError("Sender address cannot be zero".to_string()));
        }
        let protocol_type = match opp.protocol_name.as_str() {
            "Aave" | "AaveV2" => ProtocolType::Other("AaveV2".to_string()),
            "Compound" => ProtocolType::Other("Compound".to_string()),
            _ => ProtocolType::Other(opp.protocol_name.clone()),
        };
        match protocol_type {
            ProtocolType::Other(ref name) if name == "AaveV2" => {
                let liquidation_contract = opp.lending_protocol_address;
                let borrower = opp.borrower;
                let collateral_asset = opp.collateral_token;
                let debt_asset = opp.debt_token;
                let repay_amount = opp.debt_to_cover;
                let receive_a_token = false; // Default to false as field doesn't exist in struct

                let tokens = vec![
                    Token::Address(collateral_asset),
                    Token::Address(debt_asset),
                    Token::Address(borrower),
                    Token::Uint(repay_amount),
                    Token::Bool(receive_a_token),
                ];

                let calldata = self.encode_function_call("liquidationCall", &tokens)?;
                Ok((liquidation_contract, calldata, U256::zero()))
            }
            ProtocolType::Other(ref name) if name == "Compound" => {
                let ctoken_collateral = opp.collateral_token;
                let ctoken_borrowed = opp.debt_token;
                let borrower = opp.borrower;
                let repay_amount = opp.debt_to_cover;

                let tokens = vec![
                    Token::Address(borrower),
                    Token::Uint(repay_amount),
                    Token::Address(ctoken_collateral),
                ];

                let calldata = self.encode_function_call("liquidateBorrow", &tokens)?;
                Ok((ctoken_borrowed, calldata, U256::zero()))
            }
            _ => Err(MEVEngineError::CalldataError(format!(
                "Direct liquidation not supported for protocol: {:?}",
                protocol_type
            ))),
        }
    }

    /// Build calldata for a UAR liquidation using the UniversalArbitrageExecutor contract.
    pub async fn build_uar_liquidation_calldata(
        &self,
        opp: &PotentialLiquidationOpportunity,
        sender_address: Address,
    ) -> Result<(Address, Bytes, U256), MEVEngineError> {
        if sender_address == Address::zero() {
            return Err(MEVEngineError::ValidationError("Sender address cannot be zero".to_string()));
        }
        let uar_executor = opp
            .uar_connector
            .ok_or_else(|| MEVEngineError::CalldataError("Missing UAR connector for UAR liquidation".to_string()))?;

        let actions = &opp.uar_actions;

        let mut action_tokens = Vec::with_capacity(actions.len().saturating_add(1));
        let mut has_transfer_profit_action = false;
        for action in actions {
            let action_type_value = match action.action_type {
                UARActionType::Swap => U256::from(0u8),
                UARActionType::WrapNative => U256::from(1u8),
                UARActionType::UnwrapNative => U256::from(2u8),
                UARActionType::TransferProfit => U256::from(3u8),
            };
            if matches!(action.action_type, UARActionType::TransferProfit) {
                has_transfer_profit_action = true;
            }
            let target = action.target.unwrap_or_else(Address::zero);
            let token_in = action.token_in.unwrap_or_else(Address::zero);
            let amount_in = action.amount_in.unwrap_or(U256::zero());
            let call_data = action.call_data.clone().unwrap_or_else(Bytes::new);

            let tokens = Token::Tuple(vec![
                Token::Uint(action_type_value),
                Token::Address(target),
                Token::Address(token_in),
                Token::Uint(amount_in),
                Token::Bytes(call_data.to_vec()),
            ]);
            action_tokens.push(tokens);
        }

        if !has_transfer_profit_action {
            let transfer_profit = Token::Tuple(vec![
                Token::Uint(U256::from(3u8)),
                Token::Address(sender_address),
                Token::Address(opp.profit_token),
                Token::Uint(U256::zero()),
                Token::Bytes(Vec::new()),
            ]);
            action_tokens.push(transfer_profit);
        }

        let actions_token = Token::Array(action_tokens);
        let profit_token_token = Token::Address(opp.profit_token);

        let params = ethers::abi::encode(&[actions_token, profit_token_token]);
        let calldata = self.encode_function_call("execute", &[Token::Bytes(params)])?;

        Ok((uar_executor, calldata, U256::zero()))
    }

    /// Build calldata for a JIT helper mint.
    pub async fn build_jit_helper_mint_calldata(
        &self,
        helper_address: Address,
        mint_params: MintParams,
    ) -> Result<Bytes, MEVEngineError> {
        if helper_address == Address::zero() {
            return Err(MEVEngineError::ValidationError("JIT helper address cannot be zero".to_string()));
        }
        let tokens = vec![
            Token::Address(mint_params.token0),
            Token::Address(mint_params.token1),
            Token::Uint(U256::from(mint_params.fee)),
            Token::Int(mint_params.tick_lower.into()),
            Token::Int(mint_params.tick_upper.into()),
            Token::Uint(mint_params.amount0_desired),
            Token::Uint(mint_params.amount1_desired),
            Token::Uint(mint_params.amount0_min),
            Token::Uint(mint_params.amount1_min),
            Token::Address(mint_params.recipient),
            Token::Uint(mint_params.deadline.into()),
        ];

        self.encode_function_call("mint", &tokens)
    }

    /// Build calldata for a JIT helper collect.
    pub async fn build_jit_helper_collect_calldata(
        &self,
        helper_address: Address,
        recipient: Address,
    ) -> Result<Bytes, MEVEngineError> {
        if helper_address == Address::zero() {
            return Err(MEVEngineError::ValidationError("JIT helper address cannot be zero".to_string()));
        }
        let tokens = vec![
            Token::Uint(U256::zero()), // tokenId (0 for all)
            Token::Address(recipient),
            Token::Uint(U256::max_value()), // amount0Max
            Token::Uint(U256::max_value()), // amount1Max
        ];

        self.encode_function_call("collect", &tokens)
    }

    /// Build execution calldata for custom ArbitrageExecutor contract
    pub async fn build_execution_calldata(&self, tx: &TypedTransaction) -> Result<Bytes, MEVEngineError> {
        // Extract essential transaction parameters
        let to = tx.to().ok_or_else(|| MEVEngineError::CalldataError(
            "Transaction must have a 'to' address".to_string()
        ))?;
        
        let value = tx.value().copied().unwrap_or(U256::zero());
        let data = tx.data().cloned().unwrap_or_else(|| Bytes::from(vec![]));
        
        // Build ArbitrageExecutor calldata based on transaction type
        // The executor contract handles different arbitrage strategies
        match tx {
            TypedTransaction::Eip1559(_) => {
                // EIP-1559 transaction - likely a complex arbitrage
                let to_address = match to {
                    NameOrAddress::Address(addr) => addr,
                    NameOrAddress::Name(_) => {
                        return Err(MEVEngineError::CalldataError(
                            "Named addresses not supported".to_string()
                        ));
                    }
                };
                self.build_complex_arbitrage_calldata(to_address, value, data).await
            },
            _ => {
                // Legacy or type-1 transaction - simple arbitrage
                let to_address = match to {
                    NameOrAddress::Address(addr) => addr,
                    NameOrAddress::Name(_) => {
                        return Err(MEVEngineError::CalldataError(
                            "Named addresses not supported".to_string()
                        ));
                    }
                };
                self.build_simple_arbitrage_calldata(to_address, value, data).await
            }
        }
    }
    
    /// Build calldata for simple arbitrage execution
    async fn build_simple_arbitrage_calldata(
        &self,
        target: &Address,
        value: U256,
        data: Bytes,
    ) -> Result<Bytes, MEVEngineError> {
        // Encode the executeArbitrage function call
        // function executeArbitrage(address target, uint256 value, bytes calldata data)
        let function_selector = ethers::utils::keccak256(b"executeArbitrage(address,uint256,bytes)")
            [..4].to_vec();
        
        let params = ethers::abi::encode(&[
            Token::Address(*target),
            Token::Uint(value),
            Token::Bytes(data.to_vec()),
        ]);
        
        let mut calldata = function_selector;
        calldata.extend_from_slice(&params);
        
        Ok(Bytes::from(calldata))
    }
    
    /// Build calldata for complex arbitrage with multiple hops
    async fn build_complex_arbitrage_calldata(
        &self,
        target: &Address,
        value: U256,
        data: Bytes,
    ) -> Result<Bytes, MEVEngineError> {
        // Parse the original calldata to extract swap parameters
        let parsed_data = self.parse_swap_calldata(&data)?;
        
        // Build optimized execution path
        let mut execution_steps = self.optimize_execution_path(parsed_data)?;
        
        // If target and value are provided, prepend them as the first step
        if *target != Address::zero() || value > U256::zero() {
            execution_steps.insert(0, ExecutionStep {
                target: *target,
                value,
                data: data.to_vec(),
            });
        }
        
        // Encode the executeComplexArbitrage function call
        // function executeComplexArbitrage(ExecutionStep[] calldata steps)
        let function_selector = ethers::utils::keccak256(b"executeComplexArbitrage((address,uint256,bytes)[])")
            [..4].to_vec();
        
        let steps_tokens: Vec<Token> = execution_steps.into_iter()
            .map(|step| Token::Tuple(vec![
                Token::Address(step.target),
                Token::Uint(step.value),
                Token::Bytes(step.data),
            ]))
            .collect();
        
        let params = ethers::abi::encode(&[Token::Array(steps_tokens)]);
        
        let mut calldata = function_selector;
        calldata.extend_from_slice(&params);
        
        Ok(Bytes::from(calldata))
    }
    
    /// Parse swap calldata to extract parameters
    fn parse_swap_calldata(&self, data: &Bytes) -> Result<SwapParameters, MEVEngineError> {
        if data.len() < 4 {
            return Err(MEVEngineError::CalldataError(
                "Calldata too short to contain function selector".to_string()
            ));
        }
        
        let selector = &data[..4];
        let params = &data[4..];
        
        // Decode based on common swap function selectors
        let swap_params = match selector {
            // swapExactTokensForTokens selector
            [0x38, 0xed, 0x17, 0x39] => self.decode_uniswap_v2_swap(params)?,
            // multicall selector (Uniswap V3)
            [0xac, 0x9d, 0x03, 0xcd] => self.decode_uniswap_v3_multicall(params)?,
            // swap selector (Curve)
            [0x3d, 0xf0, 0x21, 0x24] => self.decode_curve_swap(params)?,
            _ => {
                // Generic swap parameters
                SwapParameters {
                    token_in: Address::zero(),
                    token_out: Address::zero(),
                    amount_in: U256::zero(),
                    amount_out_min: U256::zero(),
                    recipient: Address::zero(),
                    deadline: U256::zero(),
                    path: vec![],
                }
            }
        };
        
        Ok(swap_params)
    }
    
    /// Optimize execution path for complex arbitrage
    fn optimize_execution_path(&self, params: SwapParameters) -> Result<Vec<ExecutionStep>, MEVEngineError> {
        let mut steps = Vec::new();

        // Derive hop path from provided params ensuring token_in and token_out are respected
        let hop_path: Vec<Address> = if params.path.len() >= 2 {
            if params.token_in != Address::zero() && params.token_in != params.path[0] {
                return Err(MEVEngineError::CalldataError("token_in does not match path start".to_string()));
            }
            if params.token_out != Address::zero() && params.token_out != *params.path.last().unwrap() {
                return Err(MEVEngineError::CalldataError("token_out does not match path end".to_string()));
            }
            params.path.clone()
        } else if params.token_in != Address::zero() && params.token_out != Address::zero() {
            vec![params.token_in, params.token_out]
        } else {
            return Err(MEVEngineError::CalldataError("Insufficient swap path information".to_string()));
        };

        let last_hop_index = hop_path.len().saturating_sub(2);
        let enforce_min_output = !params.amount_out_min.is_zero();
        let has_recipient = params.recipient != Address::zero();
        let has_deadline = !params.deadline.is_zero();
        let min_output_for_last_hop = if enforce_min_output { Some(params.amount_out_min) } else { None };
        let recipient_for_last_hop = if has_recipient { Some(params.recipient) } else { None };
        let deadline_for_last_hop = if has_deadline { Some(params.deadline) } else { None };

        // Build execution steps based on the swap path
        for i in 0..hop_path.len().saturating_sub(1) {
            let step = ExecutionStep {
                target: hop_path[i],
                value: if i == 0 { params.amount_in } else { U256::zero() },
                data: self.encode_single_swap(
                    hop_path[i],
                    hop_path[i + 1],
                    if i == 0 { params.amount_in } else { U256::zero() },
                    if i == last_hop_index { min_output_for_last_hop } else { None },
                    if i == last_hop_index { recipient_for_last_hop } else { None },
                    if i == last_hop_index { deadline_for_last_hop } else { None },
                )?,
            };
            steps.push(step);
        }
        
        Ok(steps)
    }
    
    /// Encode a single swap operation
    fn encode_single_swap(
        &self,
        token_in: Address,
        token_out: Address,
        amount: U256,
    min_amount_out: Option<U256>,
    recipient: Option<Address>,
    deadline: Option<U256>,
    ) -> Result<Vec<u8>, MEVEngineError> {
        let params = ethers::abi::encode(&[
            Token::Address(token_in),
            Token::Address(token_out),
            Token::Uint(amount),
            Token::Uint(min_amount_out.unwrap_or_else(U256::zero)),
            Token::Address(recipient.unwrap_or_else(Address::zero)),
            Token::Uint(deadline.unwrap_or_else(U256::zero)),
        ]);
        
        Ok(params)
    }
    
    /// Decode Uniswap V2 swap parameters
    fn decode_uniswap_v2_swap(&self, data: &[u8]) -> Result<SwapParameters, MEVEngineError> {
        if data.len() < 160 {
            return Err(MEVEngineError::CalldataError(
                "Insufficient data for Uniswap V2 swap".to_string()
            ));
        }
        
        let tokens = ethers::abi::decode(
            &[
                ethers::abi::ParamType::Uint(256),
                ethers::abi::ParamType::Uint(256),
                ethers::abi::ParamType::Array(Box::new(ethers::abi::ParamType::Address)),
                ethers::abi::ParamType::Address,
                ethers::abi::ParamType::Uint(256),
            ],
            data,
        ).map_err(|e| MEVEngineError::CalldataError(format!("Failed to decode: {}", e)))?;
        
        let amount_in = tokens[0].clone().into_uint()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid amount_in".to_string()))?;
        let amount_out_min = tokens[1].clone().into_uint()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid amount_out_min".to_string()))?;
        let path = tokens[2].clone().into_array()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid path".to_string()))?
            .into_iter()
            .map(|t| t.into_address())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid path addresses".to_string()))?;
        let recipient = tokens[3].clone().into_address()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid recipient".to_string()))?;
        let deadline = tokens[4].clone().into_uint()
            .ok_or_else(|| MEVEngineError::CalldataError("Invalid deadline".to_string()))?;
        
        Ok(SwapParameters {
            token_in: *path.first().unwrap_or(&Address::zero()),
            token_out: *path.last().unwrap_or(&Address::zero()),
            amount_in,
            amount_out_min,
            recipient,
            deadline,
            path,
        })
    }
    
    /// Decode Uniswap V3 multicall parameters
    fn decode_uniswap_v3_multicall(&self, data: &[u8]) -> Result<SwapParameters, MEVEngineError> {
        // V3 multicall is complex - attempt basic decoding
        if data.len() < 68 {
            return Err(MEVEngineError::CalldataError(
                "Insufficient data for V3 multicall".to_string()
            ));
        }
        
        // Extract basic parameters from multicall data
        // This is a simplified version - production would need full multicall parsing
        let token_in = Address::from_slice(&data[12..32]);
        let token_out = Address::from_slice(&data[32..52]);
        let amount_bytes = &data[52..68];
        let amount_in = U256::from_big_endian(amount_bytes);
        
        Ok(SwapParameters {
            token_in,
            token_out,
            amount_in,
            amount_out_min: U256::zero(),
            recipient: Address::zero(),
            deadline: U256::from(u64::MAX),
            path: vec![token_in, token_out],
        })
    }
    
    /// Decode Curve swap parameters
    fn decode_curve_swap(&self, data: &[u8]) -> Result<SwapParameters, MEVEngineError> {
        // Curve swap decoding - simplified version
        if data.len() < 128 {
            return Err(MEVEngineError::CalldataError(
                "Insufficient data for Curve swap".to_string()
            ));
        }
        
        // Basic Curve exchange parameters extraction
        // exchange(int128 i, int128 j, uint256 dx, uint256 min_dy)
        let amount_in = U256::from_big_endian(&data[64..96]);
        let amount_out_min = U256::from_big_endian(&data[96..128]);
        
        Ok(SwapParameters {
            token_in: Address::zero(),  // Would need pool context to determine
            token_out: Address::zero(), // Would need pool context to determine
            amount_in,
            amount_out_min,
            recipient: Address::zero(),
            deadline: U256::from(u64::MAX),
            path: vec![],
        })
    }

    /// Build flashloan execution calldata for the UniversalArbitrageExecutor.
    pub async fn build_flashloan_execution_calldata(
        &self,
        chain_name: &str,
        flashloan_opp: &FlashLoanOpportunity,
        sender_address: Address,
    ) -> Result<(Address, Bytes, U256), MEVEngineError> {
        // Get UAR address from the flashloan opportunity path
        let uar_executor = flashloan_opp.path.uar_contract;
        
        // Build action tokens for each swap in the path
        let mut action_tokens = Vec::new();
        
        // Process each DEX swap in the path
        let path_tokens = &flashloan_opp.path.token_path;
        if path_tokens.len() < 2 {
            return Err(MEVEngineError::CalldataError(
                "Flash loan path must have at least 2 tokens".to_string()
            ));
        }
        
        // Calculate the amount for each swap based on flash loan amount
        let mut current_amount = flashloan_opp.amount_to_flash;
        
        for (i, dex) in flashloan_opp.path.dexes.iter().enumerate() {
            if i >= path_tokens.len() - 1 {
                break;
            }
            
            let token_in = path_tokens[i];
            let token_out = path_tokens[i + 1];
            
            // Build the swap path segment for this DEX
            let swap_path = if i == 0 {
                // First swap uses full path if multi-hop on same DEX
                let mut path_segment = vec![token_in];
                let mut j = i + 1;
                while j < path_tokens.len() && j < flashloan_opp.path.dexes.len() 
                    && flashloan_opp.path.dexes[j].router_address == dex.router_address {
                    path_segment.push(path_tokens[j]);
                    j += 1;
                }
                path_segment
            } else {
                vec![token_in, token_out]
            };
            
            // Build swap calldata for this DEX
            let swap_calldata = self.build_router_swap_calldata(
                chain_name,
                dex.router_address,
                &swap_path,
                current_amount,
                U256::zero(), // No minimum output for arbitrage swaps
                uar_executor, // The recipient is the UAR itself
                false, // is_native_in
                false, // is_native_out
                Some(flashloan_opp.block_number),
            ).await?;

            let action = Token::Tuple(vec![
                Token::Uint(U256::from(0u8)), // UARActionType::Swap
                Token::Address(dex.router_address),
                Token::Address(token_in),
                Token::Uint(current_amount),
                Token::Bytes(swap_calldata.to_vec()),
            ]);
            action_tokens.push(action);
            
            // Calculate expected output for next swap using actual quotes
            current_amount = if let Some(pool_states) = &flashloan_opp.pool_states {
                // Use actual pool states to calculate precise output
                self.calculate_swap_output(
                    &pool_states[i],
                    token_in,
                    token_out,
                    current_amount,
                    dex.protocol_type.clone(),
                ).await.unwrap_or_else(|_| {
                    // Fallback to conservative estimate if calculation fails
                    current_amount
                        .saturating_mul(U256::from(9950)) // 0.5% slippage
                        .checked_div(U256::from(10000))
                        .unwrap_or(current_amount)
                })
            } else {
                // Use quote from opportunity if available
                if let Some(ref quote_amounts) = flashloan_opp.quote_amounts {
                    if i < quote_amounts.len() - 1 {
                        quote_amounts[i + 1]
                    } else {
                        // Conservative estimate with slippage
                        current_amount
                            .saturating_mul(U256::from(9950))
                            .checked_div(U256::from(10000))
                            .unwrap_or(current_amount)
                    }
                } else {
                    // Conservative estimate with slippage
                    current_amount
                        .saturating_mul(U256::from(9950))
                        .checked_div(U256::from(10000))
                        .unwrap_or(current_amount)
                }
            };
        }

        // After all swaps, add an action to transfer the profit to the sender.
        // The profit token is the token we started with.
        let profit_action = Token::Tuple(vec![
            Token::Uint(U256::from(3u8)), // UARActionType::TransferProfit
            Token::Address(sender_address), // The ultimate recipient of the profit
            Token::Address(flashloan_opp.token_to_flash),
            Token::Uint(U256::zero()), // Amount is ignored; UAR transfers the full balance of the profit token
            Token::Bytes(vec![]),      // No calldata needed for profit transfer
        ]);
        action_tokens.push(profit_action);

        let actions = Token::Array(action_tokens);

        // The 'execute' function of the UAR expects a bytes payload containing the encoded actions and flashloan details.
        let params = ethers::abi::encode(&[
            Token::Address(flashloan_opp.token_to_flash),
            Token::Uint(flashloan_opp.amount_to_flash),
            actions
        ]);
        
        let calldata = self.encode_function_call("execute", &[Token::Bytes(params)])?;
        
        Ok((uar_executor, calldata, U256::zero()))
    }


    /// Parse a victim's swap transaction into SwapParams.
    pub fn parse_victim_swap_tx(&self, victim_tx: &ethers::types::Transaction) -> Result<SwapParams, MEVEngineError> {
        crate::mempool::parse_swap_params(victim_tx)
            .map_err(|e| MEVEngineError::Other(format!("Mempool parser failed to parse victim transaction: {}", e)))
    }

    /// Detect the protocol type of a router address.
    pub async fn detect_router_protocol(&self, router_address: Address) -> Result<ProtocolType, MEVEngineError> {
        // TODO: Move to a registry or config for extensibility.
        let known_routers = [
            // Uniswap V2
            ("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", ProtocolType::UniswapV2),
            // SushiSwap
            ("0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F", ProtocolType::SushiSwap),
            // Uniswap V3
            ("0xE592427A0AEce92De3Edee1F18E0157C05861564", ProtocolType::UniswapV3),
        ];

        for (addr_str, protocol) in known_routers.iter() {
            if Address::from_str(addr_str).map_err(|_| MEVEngineError::CalldataError("Invalid router address in known_routers".to_string()))? == router_address {
                return Ok(protocol.clone());
            }
        }

        // Defaulting to UniswapV2 might be risky. Consider returning an error for unknown routers.
        Err(MEVEngineError::CalldataError(format!("Unknown router protocol for address: {}", router_address)))
    }

    /// Get liquidation parameters for a specific lending protocol.
    pub async fn get_liquidation_params(&self, protocol_address: Address) -> Result<LiquidationParams, MEVEngineError> {
        let protocol_name = self.detect_lending_protocol(protocol_address).await?;

        match protocol_name.as_str() {
            "Aave" => Ok(LiquidationParams {
                liquidation_bonus_bps: 500,
                flashloan_fee_bps: 9,
                min_liquidation_amount: U256::from(10u64).pow(U256::from(16u64)),
            }),
            "Compound" => Ok(LiquidationParams {
                liquidation_bonus_bps: 500,
                flashloan_fee_bps: 0,
                min_liquidation_amount: U256::from(10u64).pow(U256::from(16u64)),
            }),
            "Maker" => Ok(LiquidationParams {
                liquidation_bonus_bps: 300,
                flashloan_fee_bps: 0,
                min_liquidation_amount: U256::from(10u64).pow(U256::from(17u64)),
            }),
            _ => Err(MEVEngineError::Other(format!("Unsupported lending protocol: {}", protocol_name)))
        }
    }

    /// Detect the lending protocol from a contract address.
    pub async fn detect_lending_protocol(&self, protocol_address: Address) -> Result<String, MEVEngineError> {
        // TODO: This should be sourced from a configuration file or a registry contract
        let known_protocols = [
            // Aave V3 Pool
            ("0x87870Bca3F3fD6036383eADf83275c02A56A9A15", "Aave"), // Mainnet V3
            ("0x794a61358D6845594F94dc1DB02A252b5b4814aD", "Aave"), // Polygon V3
            // Compound V3 Comet
            ("0xc3d688B66703497DAA19211EEd5eC8C9bE4A2E6E", "Compound"), // Mainnet USDC
            // Maker
            ("0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B", "Maker"),
        ];

        for (addr_str, protocol) in known_protocols.iter() {
            if Address::from_str(addr_str).map_err(|_| MEVEngineError::CalldataError("Invalid protocol address in known_protocols".to_string()))? == protocol_address {
                return Ok(protocol.to_string());
            }
        }

        Err(MEVEngineError::Other(format!("Unknown lending protocol address: {}", protocol_address)))
    }

    /// Encode a function call with the given parameters, using ABI cache for performance.
    fn encode_function_call(&self, function_name: &str, tokens: &[Token]) -> Result<Bytes, MEVEngineError> {
        let function_abi = self.get_or_create_function_abi(function_name)?;

        function_abi.encode_input(tokens)
            .map(Bytes::from)
            .map_err(|e| MEVEngineError::CalldataError(format!("Failed to encode {}: {}", function_name, e)))
    }

    /// Get or create a function ABI from cache, parsing if necessary.
    fn get_or_create_function_abi(&self, function_name: &str) -> Result<Arc<Function>, MEVEngineError> {
        if let Some(abi) = self.function_abi_cache.get(function_name) {
            return Ok(abi.value().clone());
        }

        // This is a critical section that defines the interfaces this builder can interact with.
        // It should be comprehensive and match the target contracts' ABIs exactly.
        let abi_str = match function_name {
            "swapExactETHForTokens" => "function swapExactETHForTokens(uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external payable returns (uint256[] memory amounts)",
            "swapExactTokensForETH" => "function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory amounts)",
            "swapExactTokensForTokens" => "function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory amounts)",
            "exactInputSingle" => "function exactInputSingle(tuple(address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96)) external payable returns (uint256 amountOut)",
            "exactInput" => "function exactInput(tuple(bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum)) external payable returns (uint256 amountOut)",
            "mint" => "function mint(tuple(address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint256 amount0Desired, uint256 amount1Desired, uint256 amount0Min, uint256 amount1Min, address recipient, uint256 deadline)) external payable returns (uint256 tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
            "collect" => "function collect(tuple(uint256 tokenId, address recipient, uint128 amount0Max, uint128 amount1Max)) external payable returns (uint256 amount0, uint256 amount1)",
            "liquidationCall" => "function liquidationCall(address collateralAsset, address debtAsset, address user, uint256 debtToCover, bool receiveAToken) external",
            "liquidateBorrow" => "function liquidateBorrow(address borrower, uint repayAmount, address cTokenCollateral) external returns (uint)",
            "execute" => "function execute(bytes calldata data) external payable",
            _ => return Err(MEVEngineError::CalldataError(format!("Unknown function signature for: {}", function_name))),
        };

        let function = HumanReadableParser::parse_function(abi_str)
            .map_err(|e| MEVEngineError::CalldataError(format!("Failed to parse ABI for {}: {}", function_name, e)))?;

        let arc_function = Arc::new(function);
        self.function_abi_cache.insert(function_name.to_string(), arc_function.clone());

        Ok(arc_function)
    }
    
    /// Calculate swap output based on pool state and protocol
    async fn calculate_swap_output(
        &self,
        pool_state: &PoolStateFromSwaps,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        protocol_type: ProtocolType,
    ) -> Result<U256, MEVEngineError> {
        // Validate that token_in and token_out match the pool tokens
        let expected_out = if pool_state.token0 == token_in {
            pool_state.token1
        } else if pool_state.token1 == token_in {
            pool_state.token0
        } else {
            return Err(MEVEngineError::CalldataError("Token pair does not match pool".to_string()));
        };
        if expected_out != token_out {
            return Err(MEVEngineError::CalldataError("token_out does not match pool for given token_in".to_string()));
        }
        match protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::SushiSwap => {
                // Constant product formula: y_out = (y * x * amount_in) / (x + amount_in)
                let (reserve_in, reserve_out) = if pool_state.token0 == token_in {
                    (pool_state.reserve0, pool_state.reserve1)
                } else {
                    (pool_state.reserve1, pool_state.reserve0)
                };
                
                if reserve_in.is_zero() || reserve_out.is_zero() {
                    return Err(MEVEngineError::CalldataError(
                        "Invalid pool reserves".to_string()
                    ));
                }
                
                // Apply fee (0.3% for Uniswap V2)
                let amount_in_with_fee = amount_in
                    .saturating_mul(U256::from(997));
                let numerator = amount_in_with_fee
                    .saturating_mul(reserve_out);
                let denominator = reserve_in
                    .saturating_mul(U256::from(1000))
                    .saturating_add(amount_in_with_fee);
                
                if denominator.is_zero() {
                    return Err(MEVEngineError::CalldataError(
                        "Division by zero in swap calculation".to_string()
                    ));
                }
                
                Ok(numerator.checked_div(denominator).unwrap_or(U256::zero()))
            },
            ProtocolType::UniswapV3 => {
                // Simplified V3 calculation using sqrt price
                // In production, this would use tick math and liquidity calculations
                let sqrt_price = pool_state.sqrt_price_x96.unwrap_or(U256::zero());
                if sqrt_price.is_zero() {
                    return Err(MEVEngineError::CalldataError(
                        "Invalid sqrt price".to_string()
                    ));
                }
                
                // Basic approximation for small swaps
                let price = sqrt_price
                    .saturating_mul(sqrt_price)
                    .checked_div(U256::from(2).pow(U256::from(192)))
                    .unwrap_or(U256::zero());
                
                Ok(if pool_state.token0 == token_in {
                    // Selling token0 for token1
                    amount_in
                        .saturating_mul(price)
                        .checked_div(U256::from(10).pow(U256::from(18)))
                        .unwrap_or(U256::zero())
                } else {
                    // Selling token1 for token0
                    if price.is_zero() {
                        U256::zero()
                    } else {
                        amount_in
                            .saturating_mul(U256::from(10).pow(U256::from(18)))
                            .checked_div(price)
                            .unwrap_or(U256::zero())
                    }
                })
            },
            ProtocolType::Curve => {
                // Curve uses stable swap invariant or crypto pool formulas
                // For now, use a conservative 1:1 ratio with small slippage
                // Production would need actual Curve math implementation
                Ok(amount_in
                    .saturating_mul(U256::from(997))
                    .checked_div(U256::from(1000))
                    .unwrap_or(U256::zero()))
            },
            ProtocolType::Balancer => {
                // Balancer uses weighted math
                // Simplified calculation using standard AMM formula
                let (reserve_in, reserve_out) = if pool_state.token0 == token_in {
                    (pool_state.reserve0, pool_state.reserve1)
                } else {
                    (pool_state.reserve1, pool_state.reserve0)
                };
                
                if reserve_in.is_zero() || reserve_out.is_zero() {
                    return Err(MEVEngineError::CalldataError(
                        "Invalid pool reserves".to_string()
                    ));
                }
                
                // Simplified Balancer calculation with default 0.3% fee
                let amount_in_with_fee = amount_in
                    .saturating_mul(U256::from(997));
                let numerator = amount_in_with_fee
                    .saturating_mul(reserve_out);
                let denominator = reserve_in
                    .saturating_mul(U256::from(1000))
                    .saturating_add(amount_in_with_fee);
                
                if denominator.is_zero() {
                    return Err(MEVEngineError::CalldataError(
                        "Division by zero in Balancer calculation".to_string()
                    ));
                }
                
                Ok(numerator.checked_div(denominator).unwrap_or(U256::zero()))
            },
            _ => {
                // Unknown protocol, use conservative estimate
                Ok(amount_in
                    .saturating_mul(U256::from(995))
                    .checked_div(U256::from(1000))
                    .unwrap_or(U256::zero()))
            }
        }
    }
}

/// Liquidation parameters for different protocols.
#[derive(Debug, Clone)]
pub struct LiquidationParams {
    pub liquidation_bonus_bps: u32,
    pub flashloan_fee_bps: u32,
    pub min_liquidation_amount: U256,
}
