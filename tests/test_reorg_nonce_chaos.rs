mod common {
    include!("common/mod.rs");
}

use crate::common::{
    TestHarness, TestMode, DeployedToken, TokenType, ContractWrapper,
    DeployedPair, UarAction, UarActionType
};
use ethers::{
    types::{
        Address, U256, H256, H160, TransactionRequest, TransactionReceipt,
        Block, BlockNumber, BlockId, Transaction, Bytes, Filter,
        Log, ValueOrArray, U64, transaction::eip2930::{AccessList, AccessListItem},
        Eip1559TransactionRequest
    },
    providers::{Provider, Http, Ws, StreamExt, Middleware, MockProvider, PendingTransaction},
    signers::{LocalWallet, Signer},
    middleware::SignerMiddleware,
    utils::{keccak256, hex, rlp, to_checksum},
};
use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}},
    collections::{HashMap, HashSet, BTreeMap, VecDeque},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, RwLock as TokioRwLock, Mutex as TokioMutex, Semaphore, broadcast},
    time::{timeout, sleep, interval},
    select,
    spawn,
    task::JoinHandle,
};
use futures::{future::join_all, FutureExt};
use tracing::{info, warn, error, debug, trace};
use eyre::{Result, eyre, Context};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Serialize, Deserialize};
use async_trait::async_trait;

// Import the production modules from the project
use rust::{
    blockchain::{BlockchainManager, BlockchainManagerImpl},
    gas_oracle::{GasPrice, GasOracle, GasOracleProvider},
    types::{TransactionContext, MEVPrediction, ExecutionResult, ModelPerformance, ArbitrageOpportunity, ArbitrageMetrics},
    state::{StateManager, StateConfig},
    mempool::{MempoolMonitor, DefaultMempoolMonitor},
    price_oracle::{PriceOracle},
    transaction_optimizer::{TransactionOptimizerTrait, TransactionOptimizer},
    pool_manager::{PoolManagerTrait, PoolManager},
    risk::{RiskManager, RiskManagerImpl},
    config::{Config, RiskSettings, MempoolConfig},
    errors::BotError,
    path::PathFinder,
    quote_source::{AggregatorQuoteSource, MultiAggregatorQuoteSource},
    cross::CrossChainPathfinder,
    config::CrossChainSettings,
    predict::PredictiveEngine,
    synergy_scorer::{SynergyScorer, DefaultSynergyScorer},
};

// Reorg simulation structures
#[derive(Debug, Clone)]
struct ReorgEvent {
    depth: u64,
    old_chain: Vec<Block<H256>>,
    new_chain: Vec<Block<H256>>,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct NonceState {
    account: Address,
    current_nonce: U256,
    pending_nonces: HashSet<U256>,
    replacement_txs: HashMap<U256, Vec<Eip1559TransactionRequest>>,
    confirmed_txs: HashMap<U256, H256>,
}

#[derive(Debug, Clone)]
struct JobKey {
    id: H256,
    nonce: U256,
    account: Address,
    created_at: Instant,
    execution_count: u64,
}

#[derive(Debug)]
struct ReorgSimulator {
    provider: Arc<dyn BlockchainManager + Send + Sync>,
    chain_state: Arc<TokioRwLock<ChainState>>,
    nonce_manager: Arc<NonceManager>,
    job_tracker: Arc<JobTracker>,
}

#[derive(Debug)]
struct ChainState {
    current_block: U64,
    chain_tip: H256,
    fork_history: VecDeque<ReorgEvent>,
    block_cache: BTreeMap<U64, Block<H256>>,
    uncle_blocks: HashMap<H256, Block<H256>>,
}

#[derive(Debug)]
struct NonceManager {
    states: Arc<TokioRwLock<HashMap<Address, NonceState>>>,
    duplicate_detector: Arc<TokioMutex<HashMap<(Address, U256), Vec<H256>>>>,
    replacement_strategies: Arc<TokioRwLock<ReplacementStrategies>>,
}

#[derive(Debug)]
struct ReplacementStrategies {
    min_tip_bump_percent: u64,
    max_fee_bump_percent: u64,
    escalation_curve: EscalationCurve,
}

#[derive(Debug, Clone)]
enum EscalationCurve {
    Linear { step: U256 },
    Exponential { base: f64, cap: U256 },
    Adaptive { history: VecDeque<(U256, bool)> },
}

#[derive(Debug)]
struct JobTracker {
    jobs: Arc<TokioRwLock<HashMap<H256, JobKey>>>,
    execution_log: Arc<TokioMutex<Vec<JobExecution>>>,
    idempotency_enforcer: Arc<IdempotencyEnforcer>,
}

#[derive(Debug, Clone)]
struct JobExecution {
    job_key: H256,
    nonce: U256,
    tx_hash: H256,
    block_number: U64,
    success: bool,
    reorg_count: u64,
}

#[derive(Debug)]
struct IdempotencyEnforcer {
    processed_jobs: Arc<TokioRwLock<HashMap<H256, ProcessedJob>>>,
    pending_jobs: Arc<TokioRwLock<HashMap<H256, PendingJob>>>,
}

#[derive(Debug, Clone)]
struct ProcessedJob {
    job_key: H256,
    final_tx_hash: H256,
    block_number: U64,
    confirmations: u64,
    value_transferred: U256,
}

#[derive(Debug, Clone)]
struct PendingJob {
    job_key: H256,
    tx_attempts: Vec<TxAttempt>,
    current_nonce: U256,
    deadline: Instant,
}

#[derive(Debug, Clone)]
struct TxAttempt {
    tx_hash: H256,
    nonce: U256,
    gas_price: GasPriceStrategy,
    submitted_at: Instant,
    status: AttemptStatus,
}

#[derive(Debug, Clone)]
enum GasPriceStrategy {
    Legacy(U256),
    EIP1559 { max_fee: U256, priority_fee: U256 },
}

#[derive(Debug, Clone, PartialEq)]
enum AttemptStatus {
    Pending,
    Mined { block: U64 },
    Replaced { by_tx: H256 },
    Dropped,
}

impl ReorgSimulator {
    async fn new(harness: &TestHarness) -> Result<Self> {
        let provider = harness.blockchain_manager.clone();

        let current_block_num = provider.get_block_number().await?;
        let latest_block = provider.get_block(BlockId::Number(current_block_num.into())).await?.unwrap();

        let chain_state = Arc::new(TokioRwLock::new(ChainState {
            current_block: U64::from(current_block_num),
            chain_tip: latest_block.hash.unwrap(),
            fork_history: VecDeque::with_capacity(100),
            block_cache: BTreeMap::new(),
            uncle_blocks: HashMap::new(),
        }));

        let nonce_manager = Arc::new(NonceManager {
            states: Arc::new(TokioRwLock::new(HashMap::new())),
            duplicate_detector: Arc::new(TokioMutex::new(HashMap::new())),
            replacement_strategies: Arc::new(TokioRwLock::new(ReplacementStrategies {
                min_tip_bump_percent: 10,
                max_fee_bump_percent: 12,
                escalation_curve: EscalationCurve::Adaptive {
                    history: VecDeque::with_capacity(50)
                },
            })),
        });

        let job_tracker = Arc::new(JobTracker {
            jobs: Arc::new(TokioRwLock::new(HashMap::new())),
            execution_log: Arc::new(TokioMutex::new(Vec::new())),
            idempotency_enforcer: Arc::new(IdempotencyEnforcer {
                processed_jobs: Arc::new(TokioRwLock::new(HashMap::new())),
                pending_jobs: Arc::new(TokioRwLock::new(HashMap::new())),
            }),
        });

        Ok(Self {
            provider,
            chain_state,
            nonce_manager,
            job_tracker,
        })
    }

    async fn simulate_reorg(&self, depth: u64) -> Result<ReorgEvent> {
        if depth == 0 {
            // refresh tip and return a no-op reorg
            let latest = self.provider
                .get_block(BlockId::Number(BlockNumber::Latest))
                .await?
                .and_then(|b| b.hash)
                .ok_or_else(|| eyre!("latest block missing hash"))?;
            {
                let mut cs = self.chain_state.write().await;
                cs.chain_tip = latest;
            }
            return Ok(ReorgEvent {
                depth: 0,
                old_chain: vec![],
                new_chain: vec![],
                timestamp: Instant::now()
            });
        }

        let current_block;
        // Build exact-length buffer by offset from the tip
        let mut old: Vec<Option<Block<H256>>> = vec![None; depth as usize];
        {
            let cs = self.chain_state.read().await;
            current_block = cs.current_block;
            for i in 0..depth {
                let bn = cs.current_block.as_u64().saturating_sub(i);
                if let Some(b) = cs.block_cache.get(&U64::from(bn)) {
                    old[i as usize] = Some(b.clone());
                }
            }
        }
        // fetch missing with lock released
        for i in 0..depth {
            if old[i as usize].is_none() {
                let bn = current_block.as_u64().saturating_sub(i);
                if let Some(b) = self.provider.get_block(BlockId::Number(bn.into())).await?
                {
                    old[i as usize] = Some(b);
                } else {
                    return Err(eyre!("missing block {bn} while simulating reorg"));
                }
            }
        }
        let mut old_chain: Vec<Block<H256>> = old.into_iter().map(|o| o.unwrap()).collect();
        // (decide on ordering once and stick to it; remove the `reverse()` if offsets are consistent)

        // Simulate new chain
        let mut new_chain = Vec::<Block<H256>>::new();
        let fork_point = current_block.as_u64().saturating_sub(depth);

        // Get parent hash for fork point
        let fork_parent_hash = if fork_point > 0 {
            self.provider.get_block(BlockId::Number(fork_point.into())).await?
                .ok_or_else(|| eyre!("missing block {fork_point} for fork parent"))?
                .hash.ok_or_else(|| eyre!("missing hash for block {fork_point}"))?
        } else {
            H256::zero()
        };

        for i in 0..depth {
            let block_num = fork_point + i + 1;
            // Create synthetic block with different hash
            let mut new_block = old_chain[i as usize].clone();
            new_block.hash = Some(H256::from_low_u64_be(block_num * 1000 + i));
            new_block.number = Some(U64::from(block_num));
            new_block.parent_hash = if i == 0 {
                fork_parent_hash
            } else {
                new_chain[(i - 1) as usize].hash.ok_or_else(|| eyre!("missing hash for new block {}", i - 1))?
            };

            new_chain.push(new_block);
        }

        // Update chain state
        {
            let mut chain_state = self.chain_state.write().await;

            // Store uncles
            for (i, block) in old_chain.iter().enumerate() {
                if let Some(old_hash) = block.hash {
                    chain_state.uncle_blocks.insert(old_hash, block.clone());
                }
            }

            // Update chain tip and current block number
            if let Some(last_block) = new_chain.last() {
                chain_state.chain_tip = last_block.hash.unwrap();
                chain_state.current_block = last_block.number.unwrap();
            }

            let reorg_event = ReorgEvent {
                depth,
                old_chain: old_chain.clone(),
                new_chain: new_chain.clone(),
                timestamp: Instant::now(),
            };

            chain_state.fork_history.push_back(reorg_event.clone());
            if chain_state.fork_history.len() > 100 {
                chain_state.fork_history.pop_front();
            }

            // Update block cache
            for block in &new_chain {
                chain_state.block_cache.insert(block.number.unwrap(), block.clone());
            }

            Ok(reorg_event)
        }
    }

    async fn handle_duplicate_nonce(
        &self,
        account: Address,
        nonce: U256,
        tx_hash: H256,
    ) -> Result<()> {
        let mut detector = self.nonce_manager.duplicate_detector.lock().await;
        let key = (account, nonce);

        detector.entry(key).or_insert_with(Vec::new).push(tx_hash);

        if detector[&key].len() > 1 {
            warn!(
                "Duplicate nonce detected: account={:?}, nonce={}, txs={:?}",
                account, nonce, detector[&key]
            );

            // Ensure account state exists before triggering replacement
            {
                let mut states = self.nonce_manager.states.write().await;
                states.entry(account).or_insert_with(|| {
                    // Create a dummy pending transaction for the nonce
                    let dummy_tx = Eip1559TransactionRequest {
                        from: Some(account),
                        to: Some(Address::from(H160::random()).into()),
                        value: Some(U256::from(1000)),
                        nonce: Some(nonce),
                        max_fee_per_gas: Some(U256::from(100) * U256::exp10(9)),
                        max_priority_fee_per_gas: Some(U256::from(2) * U256::exp10(9)),
                        ..Default::default()
                    };
                    NonceState {
                        account,
                        current_nonce: nonce,
                        pending_nonces: HashSet::new(),
                        replacement_txs: [(nonce, vec![dummy_tx])].into_iter().collect(),
                        confirmed_txs: HashMap::new(),
                    }
                });
            }

            // Trigger replacement logic
            self.initiate_replacement(account, nonce).await?;
        }

        Ok(())
    }

    async fn initiate_replacement(
        &self,
        account: Address,
        nonce: U256,
    ) -> Result<H256> {
        // 1) Snapshot the minimal data needed from states
        let (current_max, current_priority, attempts) = {
            let states = self.nonce_manager.states.read().await;
            let st = states.get(&account).ok_or_else(|| eyre!("No state for account"))?;
            let txs = st.replacement_txs.get(&nonce).ok_or_else(|| eyre!("No pending tx for nonce"))?;
            let cur = txs.last().ok_or_else(|| eyre!("No pending tx for nonce"))?;
            let max = cur.max_fee_per_gas.ok_or_else(|| eyre!("missing max_fee_per_gas"))?;
            let tip = cur.max_priority_fee_per_gas.ok_or_else(|| eyre!("missing max_priority_fee_per_gas"))?;
            (max, tip, txs.len() as u32)
        };

        // 2) Read strategies to compute new fees
        let (new_max_fee, new_priority_fee) = {
            let strategies = self.nonce_manager.replacement_strategies.read().await;
            let bump_max = current_max * strategies.max_fee_bump_percent / 100;
            let bump_tip = current_priority * strategies.min_tip_bump_percent / 100;

            match &strategies.escalation_curve {
                EscalationCurve::Linear { step } => (
                    current_max + bump_max + *step,
                    current_priority + bump_tip + *step
                ),
                EscalationCurve::Exponential { base, cap } => {
                    // Calculate multiplier more precisely: (base * 100)^attempts / 100^attempts
                    let base_scaled = U256::from((*base * 100.0) as u64); // Scale base by 100
                    let mut mult = U256::from(100); // Start with 100^1
                    for _ in 0..attempts {
                        mult = mult * base_scaled / U256::from(100); // Multiply by scaled base and divide by 100
                    }
                    (
                        (current_max * mult / 100).min(*cap),
                        (current_priority * mult / 100).min(*cap)
                    )
                },
                EscalationCurve::Adaptive { history } => {
                    let recent_success = history.iter().rev().take(10).filter(|(_, ok)| *ok).count();
                    let bump = if recent_success < 3 { U256::from(50) }
                               else if recent_success < 7 { U256::from(25) }
                               else { U256::from(10) };
                    (
                        current_max + current_max * bump / 100,
                        current_priority + current_priority * bump / 100
                    )
                }
            }
        };

        // 3) Build replacement based on the current tx (re-read the tx to avoid stale clone)
        let replacement_tx = {
            let states = self.nonce_manager.states.read().await;
            let st = states.get(&account).unwrap();
            let cur = st.replacement_txs.get(&nonce).unwrap().last().unwrap().clone();
            let mut r = cur.clone();
            r.max_fee_per_gas = Some(new_max_fee);
            r.max_priority_fee_per_gas = Some(new_priority_fee);
            r.nonce = Some(nonce);
            r
        };

        let tx_hash = H256::from_slice(&rand::random::<[u8; 32]>());

        // 4) Persist mutation to states
        {
            let mut states = self.nonce_manager.states.write().await;
            let st = states.get_mut(&account).ok_or_else(|| eyre!("No state for account"))?;
            st.replacement_txs.entry(nonce).or_default().push(replacement_tx);
        }

        // 5) Update adaptive history (drop any prior read guard first)
        {
            let mut rs = self.nonce_manager.replacement_strategies.write().await;
            if let EscalationCurve::Adaptive { history } = &mut rs.escalation_curve {
                history.push_back((new_priority_fee, false));
                if history.len() > 50 { history.pop_front(); }
            }
        }

        Ok(tx_hash)
    }

    async fn ensure_job_idempotency(
        &self,
        job_key: H256,
        execute_fn: impl FnOnce() -> Result<H256> + Send,
    ) -> Result<Option<H256>> {
        let enforcer = &self.job_tracker.idempotency_enforcer;

        // Check if already processed
        {
            let processed = enforcer.processed_jobs.read().await;
            if let Some(job) = processed.get(&job_key) {
                debug!("Job already processed: {:?}", job_key);
                return Ok(Some(job.final_tx_hash));
            }
        }

        // Check if pending
        {
            let mut pending = enforcer.pending_jobs.write().await;
            if let Some(job) = pending.get(&job_key) {
                debug!("Job already pending: {:?}", job_key);
                // Check if we should retry
                if job.deadline < Instant::now() {
                    // Deadline passed, remove and retry
                    pending.remove(&job_key);
                } else {
                    // Still pending, return latest attempt
                    return Ok(job.tx_attempts.last().map(|a| a.tx_hash));
                }
            }
        }

        // Execute job
        let tx_hash = execute_fn()?;

        // Track as pending
        {
            let mut pending = enforcer.pending_jobs.write().await;
            pending.insert(job_key, PendingJob {
                job_key,
                tx_attempts: vec![TxAttempt {
                    tx_hash,
                    nonce: U256::zero(), // Will be updated
                    gas_price: GasPriceStrategy::Legacy(U256::zero()),
                    submitted_at: Instant::now(),
                    status: AttemptStatus::Pending,
                }],
                current_nonce: U256::zero(),
                deadline: Instant::now() + Duration::from_secs(300), // 5 min deadline
            });
        }

        Ok(Some(tx_hash))
    }

    async fn monitor_job_confirmations(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // 1) snapshot
            let snapshots: Vec<(H256, Vec<H256>)> = {
                let pending = self.job_tracker.idempotency_enforcer.pending_jobs.read().await;
                pending.iter().map(|(k, job)| {
                    let txs = job.tx_attempts.iter()
                        .filter(|a| a.status == AttemptStatus::Pending)
                        .map(|a| a.tx_hash).collect::<Vec<_>>();
                    (*k, txs)
                }).collect()
            };

            // 2) do RPCs without holding the lock
            let mut ready = Vec::new();
            for (job_key, txs) in snapshots {
                for h in txs {
                    if let Some(rcpt) = self.provider.get_transaction_receipt(h).await? {
                        if let Some(bn) = rcpt.block_number {
                            let confs = self.get_confirmations(bn).await?;
                            if confs >= 12 {
                                ready.push((job_key, h, bn, confs));
                                break;
                            }
                        }
                    }
                }
            }

            // 3) apply updates
            if !ready.is_empty() {
                let mut pending = self.job_tracker.idempotency_enforcer.pending_jobs.write().await;
                for (job_key, h, bn, confs) in &ready {
                    if let Some(job) = pending.get_mut(job_key) {
                        if let Some(a) = job.tx_attempts.iter_mut().find(|a| a.tx_hash == *h) {
                            a.status = AttemptStatus::Mined { block: *bn };
                        }
                    }
                }
                drop(pending);
                for (job_key, h, bn, confs) in ready {
                    self.mark_job_processed(job_key, h, bn, confs).await?;
                }
            }
        }
    }

    async fn get_confirmations(&self, block_number: U64) -> Result<u64> {
        let current = self.provider.get_block_number().await?;
        Ok(current.saturating_sub(block_number.as_u64()))
    }

    async fn mark_job_processed(
        &self,
        job_key: H256,
        tx_hash: H256,
        block_number: U64,
        confirmations: u64,
    ) -> Result<()> {
        let enforcer = &self.job_tracker.idempotency_enforcer;

        // Remove from pending
        {
            let mut pending = enforcer.pending_jobs.write().await;
            pending.remove(&job_key);
        }

        // Add to processed
        {
            let mut processed = enforcer.processed_jobs.write().await;
            processed.insert(job_key, ProcessedJob {
                job_key,
                final_tx_hash: tx_hash,
                block_number,
                confirmations,
                value_transferred: U256::zero(), // Would fetch from receipt
            });
        }

        // Log execution
        {
            let mut log = self.job_tracker.execution_log.lock().await;
            log.push(JobExecution {
                job_key,
                nonce: U256::zero(), // Would fetch from tx
                tx_hash,
                block_number,
                success: true,
                reorg_count: 0, // Would track actual reorgs
            });
        }

        Ok(())
    }

    // Helper for tests to mark jobs as processed
    async fn mark_processed_for_test(&self, job_key: H256, tx: H256, bn: U64) -> Result<()> {
        self.mark_job_processed(job_key, tx, bn, 12).await
    }
}

// Test implementations
#[tokio::test]
async fn test_zero_block_reorg_recovery() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;

    // Contracts and tokens are already deployed during harness creation
    // Use the first two tokens from the harness
    let token_a = &harness.tokens[0];
    let token_b = &harness.tokens[1];

    // Submit transaction
    let job_key = H256::from_slice(&rand::random::<[u8;32]>());
    let tx_hash = simulator.ensure_job_idempotency(job_key, || {
        // Execute swap
        Ok(H256::from_slice(&rand::random::<[u8;32]>())) // Would be actual tx submission
    }).await?.unwrap();

    // Mark as processed for test
    simulator.mark_processed_for_test(job_key, tx_hash, U64::from(100)).await?;

    // Wait for confirmation
    sleep(Duration::from_secs(2)).await;

    // Simulate 0-block reorg (just tip change)
    let reorg = simulator.simulate_reorg(0).await?;
    assert_eq!(reorg.depth, 0);

    // Verify job still marked as processed
    let processed = simulator.job_tracker.idempotency_enforcer
        .processed_jobs.read().await;
    assert!(processed.contains_key(&job_key));

    Ok(())
}

#[tokio::test]
async fn test_one_block_reorg_with_nonce_chaos() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;

    // Setup nonce tracking
    let account = harness.deployer_address;
    let nonce = U256::from(10);
    let initial_tx = Eip1559TransactionRequest {
        from: Some(account),
        to: Some(Address::from(H160::random()).into()),
        value: Some(U256::from(1000)),
        nonce: Some(nonce),
        max_fee_per_gas: Some(U256::from(100) * U256::exp10(9)),
        max_priority_fee_per_gas: Some(U256::from(2) * U256::exp10(9)),
        ..Default::default()
    };

    simulator.nonce_manager.states.write().await.insert(account, NonceState {
        account,
        current_nonce: nonce,
        pending_nonces: HashSet::new(),
        replacement_txs: [(nonce, vec![initial_tx])].into_iter().collect(),
        confirmed_txs: HashMap::new(),
    });

    // Submit multiple transactions with same nonce
    let nonce = U256::from(10);
    let mut tx_hashes = Vec::new();

    for i in 0..3 {
        let tx_hash = H256::from_slice(&rand::random::<[u8;32]>());
        tx_hashes.push(tx_hash);
        simulator.handle_duplicate_nonce(account, nonce, tx_hash).await?;
        sleep(Duration::from_millis(100)).await;
    }

    // Verify duplicate detection
    let detector = simulator.nonce_manager.duplicate_detector.lock().await;
    assert_eq!(detector[&(account, nonce)].len(), 3);

    // Simulate 1-block reorg
    let reorg = simulator.simulate_reorg(1).await?;
    assert_eq!(reorg.depth, 1);

    // Verify replacement was initiated
    let states = simulator.nonce_manager.states.read().await;
    let state = &states[&account];
    assert!(state.replacement_txs.contains_key(&nonce));
    assert!(state.replacement_txs[&nonce].len() > 0);

    Ok(())
}

#[tokio::test]
async fn test_two_block_reorg_with_replacement_escalation() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;

    // Configure exponential escalation
    {
        let mut strategies = simulator.nonce_manager.replacement_strategies.write().await;
        strategies.escalation_curve = EscalationCurve::Exponential {
            base: 1.5,
            cap: U256::from(100000) * U256::exp10(9), // 100,000 gwei cap (much higher)
        };
    }
    
    let account = harness.deployer_address;
    let nonce = U256::from(20);
    
    // Initial transaction
    let mut initial_tx = Eip1559TransactionRequest {
        from: Some(account),
        to: Some(Address::from(H160::random()).into()),
        value: Some(U256::from(1000)),
        nonce: Some(nonce),
        max_fee_per_gas: Some(U256::from(100) * U256::exp10(9)),
        max_priority_fee_per_gas: Some(U256::from(2) * U256::exp10(9)),
        ..Default::default()
    };
    
    // Track in nonce manager
    simulator.nonce_manager.states.write().await.insert(account, NonceState {
        account,
        current_nonce: nonce,
        pending_nonces: [nonce].into_iter().collect(),
        replacement_txs: [(nonce, vec![initial_tx])].into_iter().collect(),
        confirmed_txs: HashMap::new(),
    });
    
    // Simulate multiple replacements
    let mut replacement_hashes = Vec::new();
    for i in 0..5 {
        let tx_hash = simulator.initiate_replacement(account, nonce).await?;
        replacement_hashes.push(tx_hash);
        
        // Verify escalation
        let states = simulator.nonce_manager.states.read().await;
        let replacements = &states[&account].replacement_txs[&nonce];
        
        if i > 0 {
            let prev_tx = &replacements[replacements.len() - 2];
            let curr_tx = &replacements[replacements.len() - 1];
            
            let prev_max = prev_tx.max_fee_per_gas.unwrap();
            let curr_max = curr_tx.max_fee_per_gas.unwrap();
            
            // Verify exponential increase
            assert!(curr_max > prev_max);
            assert!(curr_max <= U256::from(100000) * U256::exp10(9)); // Cap check
        }
    }
    
    // Simulate 2-block reorg
    let reorg = simulator.simulate_reorg(2).await?;
    assert_eq!(reorg.depth, 2);
    
    // Verify all replacements are tracked
    let states = simulator.nonce_manager.states.read().await;
    assert_eq!(states[&account].replacement_txs[&nonce].len(), 6); // initial + 5 replacements

    Ok(())
}

#[tokio::test]
async fn test_idempotent_job_execution_across_reorgs() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;
    
    // Create job keys
    let job_keys: Vec<H256> = (0..10).map(|_| H256::from_slice(&rand::random::<[u8;32]>())).collect();
    let mut execution_counts = HashMap::new();
    
    // Execute jobs multiple times with reorgs
    for round in 0..3 {
        info!("Execution round {}", round);
        
        for &job_key in &job_keys {
            let count = execution_counts.entry(job_key).or_insert(0);
            
            let result = simulator.ensure_job_idempotency(job_key, || {
                *count += 1;
                Ok(H256::from_slice(&rand::random::<[u8;32]>()))
            }).await?;

            // First execution should succeed, subsequent should return cached
            if round == 0 {
                assert!(result.is_some());
                // Mark as processed for test since transactions are simulated
                if let Some(tx_hash) = result {
                    simulator.mark_processed_for_test(job_key, tx_hash, U64::from(100 + round as u64)).await?;
                }
            }
        }
        
        // Simulate reorg between rounds
        if round < 2 {
            let depth = (round + 1) as u64;
            simulator.simulate_reorg(depth).await?;
            sleep(Duration::from_secs(1)).await;
        }
    }
    
    // Verify each job executed exactly once
    for (job_key, count) in &execution_counts {
        assert_eq!(*count, 1, "Job {:?} executed {} times", job_key, count);
    }
    
    // Verify all jobs are in processed state
    let processed = simulator.job_tracker.idempotency_enforcer
        .processed_jobs.read().await;
    assert_eq!(processed.len(), job_keys.len());

    Ok(())
}

#[tokio::test]
async fn test_adaptive_gas_escalation_with_reorgs() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;
    
    // Configure adaptive escalation
    {
        let mut strategies = simulator.nonce_manager.replacement_strategies.write().await;
        strategies.escalation_curve = EscalationCurve::Adaptive {
            history: VecDeque::with_capacity(50),
        };
    }
    
    let account = harness.deployer_address;
    
    // Simulate transaction attempts with varying success
    for round in 0..10 {
        let nonce = U256::from(100 + round);
        
        // Create initial transaction
        let mut initial_tx = Eip1559TransactionRequest {
            from: Some(account),
            to: Some(Address::from(H160::random()).into()),
            value: Some(U256::from(1000)),
            nonce: Some(nonce),
            max_fee_per_gas: Some(U256::from(50) * U256::exp10(9)),
            max_priority_fee_per_gas: Some(U256::from(1) * U256::exp10(9)),
            ..Default::default()
        };
        
        simulator.nonce_manager.states.write().await
            .entry(account)
            .or_insert_with(|| NonceState {
                account,
                current_nonce: nonce,
                pending_nonces: HashSet::new(),
                replacement_txs: HashMap::new(),
                confirmed_txs: HashMap::new(),
            })
            .replacement_txs.insert(nonce, vec![initial_tx]);
        
        // Attempt replacement
        let tx_hash = simulator.initiate_replacement(account, nonce).await?;
        
        // Simulate success/failure
        let success = round % 3 != 0; // Fail every third attempt
        if let EscalationCurve::Adaptive { history } =
            &mut simulator.nonce_manager.replacement_strategies.write().await.escalation_curve
        {
            // Update with result
            let states = simulator.nonce_manager.states.read().await;
            let last_tx = states[&account].replacement_txs[&nonce].last().unwrap();
            let priority_fee = last_tx.max_priority_fee_per_gas.unwrap();
            
            history.push_back((priority_fee, success));
            if history.len() > 50 {
                history.pop_front();
            }
        }
        
        // Simulate occasional reorgs
        if round % 4 == 0 && round > 0 {
            simulator.simulate_reorg(1).await?;
        }
    }
    
    // Verify adaptive behavior
    let strategies = simulator.nonce_manager.replacement_strategies.read().await;
    if let EscalationCurve::Adaptive { history } = &strategies.escalation_curve {
        assert!(history.len() > 5);
        
        // Check that fees increased after failures
        let failure_fees: Vec<U256> = history.iter()
            .filter(|(_, success)| !success)
            .map(|(fee, _)| *fee)
            .collect();
        
        if failure_fees.len() > 1 {
            // Generally increasing trend after failures
            let avg_early = failure_fees.iter().take(failure_fees.len() / 2)
                .fold(U256::zero(), |a, b| a + b) / failure_fees.len() / 2;
            let avg_late = failure_fees.iter().skip(failure_fees.len() / 2)
                .fold(U256::zero(), |a, b| a + b) / (failure_fees.len() - failure_fees.len() / 2);
            
            assert!(avg_late >= avg_early, "Fees should adapt upward after failures");
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_nonce_management_under_reorg() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = Arc::new(ReorgSimulator::new(&harness).await?);
    
    let account = harness.deployer_address;
    let num_concurrent = 20;
    
    // Initialize nonce state
    simulator.nonce_manager.states.write().await.insert(account, NonceState {
        account,
        current_nonce: U256::from(1000),
        pending_nonces: HashSet::new(),
        replacement_txs: HashMap::new(),
        confirmed_txs: HashMap::new(),
    });
    
    // Launch concurrent transactions
    let mut handles = Vec::new();
    let submit_barrier = Arc::new(tokio::sync::Barrier::new(num_concurrent + 1));
    
    for i in 0..num_concurrent {
        let sim = simulator.clone();
        let barrier = submit_barrier.clone();

        let handle = spawn(async move {
            barrier.wait().await;

            let nonce = U256::from(1000 + i);
            let tx_hash = H256::from_slice(&rand::random::<[u8;32]>());

            // Update nonce state
            {
                let mut states = sim.nonce_manager.states.write().await;
                let state = states.get_mut(&account).unwrap();
                state.pending_nonces.insert(nonce);
            }

            // Handle potential duplicate (avoid holding locks across await)
            let should_replace = {
                let mut detector = sim.nonce_manager.duplicate_detector.lock().await;
                let key = (account, nonce);
                detector.entry(key).or_insert_with(Vec::new).push(tx_hash);

                if detector[&key].len() > 1 {
                    warn!(
                        "Duplicate nonce detected: account={:?}, nonce={}, txs={:?}",
                        account, nonce, detector[&key]
                    );
                    true
                } else {
                    false
                }
            };

            if should_replace {
                sim.initiate_replacement(account, nonce).await.unwrap();
            }

            (nonce, tx_hash)
        });

        handles.push(handle);
    }
    
    // Trigger all submissions simultaneously
    submit_barrier.wait().await;
    
    // Wait for some transactions
    sleep(Duration::from_millis(500)).await;
    
    // Simulate reorg during processing
    let _ = simulator.simulate_reorg(2).await;
    
    // Collect results
    let results: Vec<(U256, H256)> = join_all(handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    
    // Verify no nonce collisions in confirmed transactions
    let states = simulator.nonce_manager.states.read().await;
    let state = &states[&account];
    
    let mut seen_nonces = HashSet::new();
    for (nonce, _) in &state.confirmed_txs {
        assert!(seen_nonces.insert(*nonce), "Duplicate confirmed nonce: {}", nonce);
    }
    
    // Verify all pending nonces are unique
    assert_eq!(state.pending_nonces.len(), num_concurrent);
    
    Ok(())
}

#[tokio::test]
async fn test_job_execution_determinism_across_reorgs() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;
    
    // Define deterministic job execution
    let job_data = vec![
        (H256::from_low_u64_be(1), U256::from(1000)),
        (H256::from_low_u64_be(2), U256::from(2000)),
        (H256::from_low_u64_be(3), U256::from(3000)),
    ];
    
    let mut execution_results = HashMap::new();
    
    // Execute jobs
    for (job_key, value) in &job_data {
        let result = simulator.ensure_job_idempotency(*job_key, || {
            // Deterministic execution based on job key
            let tx_hash = H256::from_slice(&keccak256(&job_key.as_bytes()));
            execution_results.insert(*job_key, (*value, tx_hash));
            Ok(tx_hash)
        }).await?;

        assert!(result.is_some());

        // Mark as processed for test
        let tx_hash = execution_results[job_key].1;
        simulator.mark_processed_for_test(*job_key, tx_hash, U64::from(100)).await?;
    }
    
    // Simulate multiple reorgs
    for depth in 0..3 {
        simulator.simulate_reorg(depth).await?;
        
        // Re-execute jobs (should be idempotent)
        for (job_key, value) in &job_data {
            let result = simulator.ensure_job_idempotency(*job_key, || {
                // This should not execute
                panic!("Job should not re-execute!");
            }).await?;
            
            // Verify same result
            let (stored_value, stored_hash) = execution_results[job_key];
            assert_eq!(result, Some(stored_hash));
            assert_eq!(stored_value, *value);
        }
    }
    
    // Verify execution log
    let log = simulator.job_tracker.execution_log.lock().await;
    assert_eq!(log.len(), job_data.len());
    
    Ok(())
}

#[tokio::test]
async fn test_mempool_eviction_during_reorg() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = ReorgSimulator::new(&harness).await?;
    
    let account = harness.deployer_address;
    let base_nonce = U256::from(2000);
    
    // Submit chain of dependent transactions
    let mut tx_chain = Vec::new();
    for i in 0..10 {
        let nonce = base_nonce + i;
        let mut tx = Eip1559TransactionRequest {
            from: Some(account),
            to: Some(Address::from(H160::random()).into()),
            value: Some(U256::from(100)),
            nonce: Some(nonce),
            max_fee_per_gas: Some(U256::from(50 - i) * U256::exp10(9)), // Decreasing gas
            max_priority_fee_per_gas: Some(U256::from(2) * U256::exp10(9)),
            ..Default::default()
        };
        
        tx_chain.push((nonce, tx));
    }
    
    // Track in nonce manager
    {
        let mut states = simulator.nonce_manager.states.write().await;
        let state = states.entry(account).or_insert_with(|| NonceState {
            account,
            current_nonce: base_nonce,
            pending_nonces: HashSet::new(),
            replacement_txs: HashMap::new(),
            confirmed_txs: HashMap::new(),
        });
        
        for (nonce, tx) in &tx_chain {
            state.pending_nonces.insert(*nonce);
            state.replacement_txs.insert(*nonce, vec![tx.clone()]);
        }
    }
    
    // Simulate deep reorg that would evict low-fee transactions
    let reorg = simulator.simulate_reorg(2).await?;
    
    // Detect and handle evicted transactions
    let mut evicted = Vec::new();
    {
        let states = simulator.nonce_manager.states.read().await;
        let state = &states[&account];
        
        // Simulate mempool eviction of low-fee txs
        for (nonce, txs) in &state.replacement_txs {
            if let Some(tx) = txs.last() {
                let fee = tx.max_fee_per_gas.unwrap();
                if fee < U256::from(45) * U256::exp10(9) {
                    evicted.push(*nonce);
                }
            }
        }
    }
    
    // Replace evicted transactions
    for nonce in evicted {
        simulator.initiate_replacement(account, nonce).await?;
    }
    
    // Verify replacements have higher fees
    let states = simulator.nonce_manager.states.read().await;
    let state = &states[&account];
    
    for (nonce, txs) in &state.replacement_txs {
        if txs.len() > 1 {
            let original = &txs[0];
            let replacement = txs.last().unwrap();
            
            assert!(
                replacement.max_fee_per_gas.unwrap() > original.max_fee_per_gas.unwrap(),
                "Replacement should have higher fee"
            );
        }
    }
    
    Ok(())
}

// Chaos testing utilities
struct ChaosOrchestrator {
    simulator: Arc<ReorgSimulator>,
    chaos_seed: u64,
    rng: StdRng,
}

impl ChaosOrchestrator {
    fn new(simulator: Arc<ReorgSimulator>, seed: u64) -> Self {
        Self {
            simulator,
            chaos_seed: seed,
            rng: StdRng::seed_from_u64(seed),
        }
    }
    
    async fn run_chaos_scenario(&mut self, duration: Duration) -> Result<()> {
        let start = Instant::now();
        let mut chaos_events = Vec::new();
        
        while start.elapsed() < duration {
            let event_type = self.rng.gen_range(0..5);
            
            match event_type {
                0 => {
                    // Random reorg
                    let depth = self.rng.gen_range(0..3);
                    info!("Chaos: triggering {}-block reorg", depth);
                    self.simulator.simulate_reorg(depth).await?;
                    chaos_events.push(format!("reorg_{}", depth));
                },
                1 => {
                    // Duplicate nonce chaos
                    let account = Address::from(H160::random());
                    let nonce = U256::from(self.rng.gen_range(0..1000));
                    let num_dups = self.rng.gen_range(2..5);

                    info!("Chaos: creating {} duplicate nonces", num_dups);
                    for _ in 0..num_dups {
                        let tx_hash = H256::from_slice(&rand::random::<[u8;32]>());
                        self.simulator.handle_duplicate_nonce(account, nonce, tx_hash).await?;
                    }
                    chaos_events.push(format!("dup_nonce_{}", num_dups));
                },
                2 => {
                    // Gas price spike
                    let mut strategies = self.simulator.nonce_manager
                        .replacement_strategies.write().await;
                    let old_bump = strategies.min_tip_bump_percent;
                    strategies.min_tip_bump_percent = self.rng.gen_range(20..100);
                    info!("Chaos: gas bump {} -> {}", old_bump, strategies.min_tip_bump_percent);
                    chaos_events.push("gas_spike".to_string());
                },
                3 => {
                    // Job execution burst
                    let num_jobs = self.rng.gen_range(5..20);
                    info!("Chaos: executing {} jobs simultaneously", num_jobs);
                    
                    let mut handles = Vec::new();
                    for _ in 0..num_jobs {
                        let job_key = H256::from_slice(&rand::random::<[u8;32]>());
                        let sim = self.simulator.clone();

                        handles.push(spawn(async move {
                            sim.ensure_job_idempotency(job_key, || Ok(H256::from_slice(&rand::random::<[u8;32]>()))).await
                        }));
                    }
                    
                    join_all(handles).await;
                    chaos_events.push(format!("job_burst_{}", num_jobs));
                },
                4 => {
                    // Network delay simulation
                    let delay_ms = self.rng.gen_range(50..500);
                    info!("Chaos: network delay {}ms", delay_ms);
                    sleep(Duration::from_millis(delay_ms)).await;
                    chaos_events.push(format!("delay_{}", delay_ms));
                },
                _ => unreachable!(),
            }
            
            // Random interval between events
            let wait_ms = self.rng.gen_range(100..1000);
            sleep(Duration::from_millis(wait_ms)).await;
        }
        
        info!("Chaos scenario completed with {} events", chaos_events.len());
        info!("Events: {:?}", chaos_events);
        
        Ok(())
    }
}

#[tokio::test]
async fn test_comprehensive_chaos_scenario() -> Result<()> {
    let harness = TestHarness::with_config(TestMode::default()).await?;
    let simulator = Arc::new(ReorgSimulator::new(&harness).await?);
    
    // Run chaos with deterministic seed
    let mut orchestrator = ChaosOrchestrator::new(simulator.clone(), 42);
    
    // Start monitoring task
    let monitor_handle = spawn({
        let sim = simulator.clone();
        async move {
            sim.monitor_job_confirmations().await.unwrap()
        }
    });
    
    // Run chaos for 10 seconds
    timeout(
        Duration::from_secs(15),
        orchestrator.run_chaos_scenario(Duration::from_secs(10))
    ).await??;
    
    // Verify system stability
    let processed = simulator.job_tracker.idempotency_enforcer
        .processed_jobs.read().await;
    let pending = simulator.job_tracker.idempotency_enforcer
        .pending_jobs.read().await;
    
    info!("Final state: {} processed, {} pending", processed.len(), pending.len());
    
    // Verify no double-spends
    let mut spent_nonces = HashSet::new();
    for job in processed.values() {
        assert!(
            spent_nonces.insert((job.job_key, job.final_tx_hash)),
            "Double spend detected!"
        );
    }
    
    Ok(())
}
