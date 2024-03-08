mod event_loops;
mod amm;
use amm::raydium::pools::calculate_pool_swap_price;
//use amm::raydium::clmm::calculate_clmm_swap_price;
//use amm::orca::calculate_swap_price;
//use amm::orca::legacy_pools::get_legacy_pool;'

use num_traits::ToPrimitive;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use futures::{stream::{FuturesOrdered, FuturesUnordered}, Future, StreamExt};
use std::{
    borrow::BorrowMut, collections::{hash_map::Entry, HashMap, HashSet}, path::PathBuf, result, str::FromStr, sync::Arc, time::{Duration, Instant}
};
use solana_rpc_client_api::{
    bundles::{
        RpcBundleRequest, RpcSimulateBundleConfig, RpcSimulateBundleResult,
        SimulationSlotConfig,
    }};
use clap::Parser;
use env_logger::TimestampPrecision;
use histogram::Histogram;
use jito_protos::{
    bundle::BundleResult,
    convert::versioned_tx_from_packet,
    searcher::{
        searcher_service_client::SearcherServiceClient, ConnectedLeadersRequest,
        NextScheduledLeaderRequest, PendingTxNotification, SendBundleResponse,
    },
};
use jito_searcher_client::{
    get_searcher_client, send_bundle_no_wait, token_authenticator::ClientInterceptor,
    BlockEngineConnectionError,
};
use log::*;
use rand::{rngs::ThreadRng, thread_rng, Rng};
use solana_client::{
    client_error::ClientError, nonblocking::{pubsub_client::PubsubClientError, rpc_client::RpcClient}, rpc_config::RpcSimulateTransactionAccountsConfig, rpc_response::{self, RpcBlockUpdate}
};
use solana_metrics::{datapoint_info, set_host_id};
use solana_sdk::{
    bundle::VersionedBundle, clock::Slot, commitment_config::{CommitmentConfig, CommitmentLevel}, hash::Hash, pubkey::Pubkey, signature::{read_keypair_file, Keypair, Signature, Signer}, system_instruction::transfer, transaction::{Transaction, VersionedTransaction}
};
use spl_memo::{build_memo, solana_program::program_pack::Pack};
use thiserror::Error;
use tokio::{
    runtime::Builder,
    sync::mpsc::{channel, Receiver},
    time::interval,
};
use tonic::{codegen::InterceptedService, transport::Channel, Response, Status};
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use crate::{amm::raydium::pools::{calculate_plain_old_pool_swap_price, get_raydium_pool}, event_loops::{
    block_subscribe_loop, bundle_results_loop, pending_tx_loop, slot_subscribe_loop,
}};
use std::collections::{BinaryHeap};
use std::cmp::Ordering;

// Define a struct for Tokens with a name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Token {
    name: String,
}
lazy_static! {
    static ref FLASHABLE_TOKEN_SET : HashSet<String> = {
        let flashable_tokens = std::fs::read_to_string("flashableTokens.json").unwrap();
        let flashable_tokens: Vec<String> = serde_json::from_str(&flashable_tokens).unwrap();
        flashable_tokens.into_iter().collect::<HashSet<String>>()
    };
}
#[derive(Debug, Clone)]
struct Edge {
    to: Token,
    pool_id: String, // Unique identifier for the AMM liquidity pool
    base_reserve: u64,
    quote_reserve: u64,
    fee_numerator: u64,
    fee_denominator: u64,
}

// Define the Graph struct containing a mapping from each token to its outgoing edges
#[derive(Debug, Clone)]
struct Graph {
    edges: HashMap<Token, Vec<Edge>>,
}

impl Graph {
    // Constructs a new, empty Graph
    fn new() -> Self {
        Graph {
            edges: HashMap::new(),
        }
    }

    // Adds a token to the graph. If the token already exists, this function does nothing.
    fn add_token(&mut self, token: Token) {
        self.edges.entry(token).or_insert_with(Vec::new);
    }

    // Adds an edge from one token to another with the specified liquidity pool information
    fn add_edge(&mut self, from: Token, to: Token, base_reserve: u64, quote_reserve: u64, fee_numerator: u64, fee_denominator: u64, pool_id: String) {
        if let Some(edges) = self.edges.get_mut(&from) {
            edges.push(Edge { to, base_reserve, quote_reserve, fee_numerator, fee_denominator, pool_id });
        } else {
            // If the 'from' token doesn't exist yet, add it and then add the edge
            self.edges.insert(from.clone(), vec![Edge { to, base_reserve, quote_reserve, fee_numerator, fee_denominator, pool_id }]);
        }
    }
}
impl Graph {
    async fn find_arbitrage_opportunities(&self, start: Token, rpc_client: &RpcClient) -> Vec<(Vec<Token>, f64)> {
        let mut opportunities = Vec::new();
        let mut heap = BinaryHeap::new();

        // Start with a profit of 1.0 (no profit or loss)
        heap.push(NodeProfit { token: start.clone(), profit: 1.0, path: vec![start.clone()] });

        while let Some(NodeProfit { token, profit, path }) = heap.pop() {
            if let Some(edges) = self.edges.get(&token) {
                for edge in edges {
                    // Dynamically calculate the profit based on the current state of the liquidity pool
                    let swap_profit = calculate_pool_swap_price(&rpc_client, edge.pool_id.clone(), edge.base_reserve, edge.quote_reserve).await.unwrap();
                    let next_profit = profit.to_f64().unwrap() * swap_profit.to_f64().unwrap();
                    let mut next_path = path.clone();
                    next_path.push(edge.to.clone());

                    // Check if the loop is closed and the profit is greater than 1
                    if edge.to == start && next_profit > 1.0 {
                        opportunities.push((next_path, next_profit));
                    } else if !next_path.contains(&edge.to) {
                        // Continue traversing only if the next token hasn't been visited yet to avoid cycles
                        heap.push(NodeProfit { token: edge.to.clone(), profit: next_profit, path: next_path });
                    }
                }
            }
        }

        opportunities
    }
}

#[derive(Clone, PartialEq)]
struct NodeProfit {
    token: Token,
    profit: f64,
    path: Vec<Token>,
}

impl Eq for NodeProfit {}

impl Ord for NodeProfit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.profit.partial_cmp(&other.profit).unwrap()
    }
}

impl PartialOrd for NodeProfit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}



#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// URL of the block engine.
    /// See: https://jito-labs.gitbook.io/mev/searcher-resources/block-engine#connection-details
    #[arg(long, env)]
    block_engine_url: String,

    /// Account pubkeys to backrun
    #[arg(long, env)]
    backrun_accounts: Vec<Pubkey>,

    /// Path to keypair file used to sign and pay for transactions
    #[arg(long, env)]
    payer_keypair: PathBuf,

    /// Path to keypair file used to authenticate with the Jito Block Engine
    /// See: https://jito-labs.gitbook.io/mev/searcher-resources/getting-started#block-engine-api-key
    #[arg(long, env)]
    auth_keypair: PathBuf,

    /// RPC Websocket URL.
    /// See: https://solana.com/docs/rpc/websocket
    /// Note that this RPC server must have --rpc-pubsub-enable-block-subscription enabled
    #[arg(long, env)]
    pubsub_url: String,

    /// RPC HTTP URL.
    #[arg(long, env)]
    rpc_url: String,

    /// Message to pass into the memo program as part of a bundle.
    #[arg(long, env, default_value = "jito backrun")]
    message: String,

    /// Tip payment program public key
    /// See: https://jito-foundation.gitbook.io/mev/mev-payment-and-distribution/on-chain-addresses
    #[arg(long, env)]
    tip_program_id: Pubkey,

    /// Comma-separated list of regions to request cross-region data from.
    /// If no region specified, then default to the currently connected block engine's region.
    /// Details: https://jito-labs.gitbook.io/mev/searcher-services/recommendations#cross-region
    /// Available regions: https://jito-labs.gitbook.io/mev/searcher-resources/block-engine#connection-details
    #[arg(long, env, value_delimiter = ',')]
    regions: Vec<String>,

    /// Subscribe and print bundle results.
    #[arg(long, env, default_value_t = true)]
    subscribe_bundle_results: bool,
}

#[derive(Debug, Error)]
enum BackrunError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("PubSubError {0}")]
    PubSubError(#[from] PubsubClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("Shutdown")]
    Shutdown,
}

#[derive(Clone)]
struct BundledTransactions {
    mempool_txs: Vec<VersionedTransaction>,
    backrun_txs: Vec<VersionedTransaction>,
}

#[derive(Default)]
struct BlockStats {
    bundles_sent: Vec<(
        BundledTransactions,
        tonic::Result<Response<SendBundleResponse>>,
    )>,
    send_elapsed: u64,
    send_rt_per_packet: Histogram,
}

type Result<T> = result::Result<T, BackrunError>;
use lazy_static::lazy_static;

lazy_static! {
    static ref RAYDIUM_KEYS_SET: HashSet<String> = {
        let raydium_keys = std::fs::read_to_string("raydiumKeys.json").unwrap();
        let raydium_keys: Vec<String> = serde_json::from_str(&raydium_keys).unwrap();
        raydium_keys.into_iter().collect::<HashSet<String>>()
    };
}
async fn build_bundles(
    rpc_client: &RpcClient,
    pending_tx_notification: PendingTxNotification,
    keypair: &Keypair,
    blockhash: &Hash,
    tip_accounts: &[Pubkey],
    rng: &mut ThreadRng,
    message: &str,
    graph: &mut Graph,
    tokens: &mut HashSet<Token>,
    seen_pool_ids: &mut HashSet<String>,
) -> Vec<BundledTransactions> {
    let mut bundles: Vec<BundledTransactions> = vec![];
    // load raydiumKeys.json, wpKeys.json, raydium_clmmKeys.json
    for packet in pending_tx_notification.transactions.iter() {
            let mempool_tx = versioned_tx_from_packet(&packet);

            if mempool_tx.is_none() {
                continue;
            }
            let mempool_tx = mempool_tx.unwrap();
            for account_key in mempool_tx.message.static_account_keys().iter() {
            if RAYDIUM_KEYS_SET.contains(&account_key.to_string()) {
                let pool = get_raydium_pool(&account_key.to_string()).await;
                if pool.is_none() {
                    println!("pool is none");
                    continue;
                }
                    let pool = pool.unwrap();
                    let base_reserve = pool.base_vault;
                    let quote_reserve = pool.quote_vault;
            let simulation_result = rpc_client
                .simulate_bundle_with_config(
                    &VersionedBundle {
                        transactions: vec![mempool_tx.clone()],
                    },
                    RpcSimulateBundleConfig {
                        simulation_bank: Some(SimulationSlotConfig::Commitment(rpc_client.commitment())),
                        pre_execution_accounts_configs: vec![Some(
                            RpcSimulateTransactionAccountsConfig {
                                encoding: Some(UiAccountEncoding::JsonParsed),
                                addresses: vec![base_reserve.clone(), quote_reserve.clone()],
                            }
                        )],
                        post_execution_accounts_configs: vec![Some(
                            RpcSimulateTransactionAccountsConfig {
                                encoding: Some(UiAccountEncoding::JsonParsed),
                                addresses: vec![base_reserve, quote_reserve],
                            }
                        )],
                        ..RpcSimulateBundleConfig::default()
                    }
                )
                .await;

            if simulation_result.is_err() {
                println!("simulation_result error: {:?}", simulation_result);
                continue;
            }
            let simulation_result = simulation_result.unwrap();
            if simulation_result.value.transaction_results.len() == 0 {

                
                println!("simulation_result.value.transaction_results.len() == 0");
                println!("simulation_result: {:?}", simulation_result);

                let swap_price_after = calculate_plain_old_pool_swap_price(&rpc_client, account_key.to_string()).await.unwrap();
                println !("plain_old_pool_swap_price {:?}", swap_price_after);
                if tokens.contains(&Token { name: swap_price_after.3.to_string() }) {
                    tokens.insert(Token { name: swap_price_after.3.to_string() });
                }
                if tokens.contains(&Token { name: swap_price_after.4.to_string() }) {
                    tokens.insert(Token { name: swap_price_after.4.to_string() });
                }
                graph.add_token(Token { name: swap_price_after.3.to_string() });
                graph.add_token(Token { name: swap_price_after.4.to_string() });
                graph.add_edge(Token { name: swap_price_after.3.to_string() }, Token { name: swap_price_after.4.to_string() }, swap_price_after.1, swap_price_after.2, 30, 10000, account_key.to_string());
                graph.add_edge(Token { name: swap_price_after.4.to_string() }, Token { name: swap_price_after.3.to_string() }, swap_price_after.2, swap_price_after.1, 30, 10000, account_key.to_string());

            }
            else {
            let pre_execution_accounts = simulation_result.value.transaction_results[0].pre_execution_accounts.clone().unwrap();
            let post_execution_accounts = simulation_result.value.transaction_results[0].post_execution_accounts.clone().unwrap();
            let base_reserve = pre_execution_accounts[0].clone();
            let quote_reserve = pre_execution_accounts[1].clone();
            let base_reserve_post = post_execution_accounts[0].clone();
            let quote_reserve_post = post_execution_accounts[1].clone();
            // decode the UiAccountDataBinary
            let base_reserve_data = &base_reserve.decode::<solana_sdk::account::Account >().unwrap().data;
            let quote_reserve_data = &quote_reserve.decode::<solana_sdk::account::Account >().unwrap().data;
            let base_reserve_post_data = &base_reserve_post.decode::<solana_sdk::account::Account >().unwrap().data;
            let quote_reserve_post_data = &quote_reserve_post.decode::<solana_sdk::account::Account >().unwrap().data;
            let base_reserve_amount = spl_token::state::Account::unpack(base_reserve_data).unwrap();
            let quote_reserve_amount = spl_token::state::Account::unpack(quote_reserve_data).unwrap();
            let base_reserve_post_amount = spl_token::state::Account::unpack(base_reserve_post_data).unwrap();
            let quote_reserve_post_amount = spl_token::state::Account::unpack(quote_reserve_post_data).unwrap();
            
            let swap_price_before = calculate_pool_swap_price(&rpc_client, account_key.to_string(), base_reserve_amount.amount, quote_reserve_amount.amount).await.unwrap();
            let swap_price_after = calculate_pool_swap_price(&rpc_client, account_key.to_string(), base_reserve_post_amount.amount, quote_reserve_post_amount.amount).await.unwrap();
            let swap_price = swap_price_before / swap_price_after;
            println !("swap price before  {:?}, after  {:?}, delta {:?}", swap_price_before, swap_price_after, swap_price);
            if tokens.contains(&Token { name: base_reserve_amount.mint.to_string() }) {
                tokens.insert(Token { name: base_reserve_amount.mint.to_string() });
            }
            if tokens.contains(&Token { name: quote_reserve_amount.mint.to_string() }) {
                tokens.insert(Token { name: quote_reserve_amount.mint.to_string() });
            }
            graph.add_token(Token { name: base_reserve_amount.mint.to_string() });
            graph.add_token(Token { name: quote_reserve_amount.mint.to_string() });
            graph.add_edge(Token { name: base_reserve_amount.mint.to_string() }, Token { name: quote_reserve_amount.mint.to_string() }, base_reserve_post_amount.amount, quote_reserve_post_amount.amount, 30, 10000, account_key.to_string());
            graph.add_edge(Token { name: quote_reserve_amount.mint.to_string() }, Token { name: base_reserve_amount.mint.to_string() }, quote_reserve_post_amount.amount, base_reserve_post_amount.amount, 30, 10000, account_key.to_string());

            let mut max_profit = 0.0;
            let mut max_profitable_route = vec![];
            for token in FLASHABLE_TOKEN_SET.iter() {
                let most_profitable_route = graph.find_arbitrage_opportunities(Token { name: token.to_string() }, rpc_client).await;

                println!("most_profitable_route: {:?}", most_profitable_route);
                if most_profitable_route.len() > 0 {
                    let (route, profit) = most_profitable_route[0].clone();
                    if profit > max_profit {
                        max_profit = profit;
                        max_profitable_route = route;
                    }
                }
                println!("max_profitable_route: {:?}", max_profitable_route);
            }
        }
    }

    }
        
            let tip_account = tip_accounts[rng.gen_range(0..tip_accounts.len())];

            let backrun_tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
                &[
                    /*build_memo(
                        format!("{}: {:?}", message, mempool_tx.signatures[0].to_string())
                            .as_bytes(),
                        &[],
                    ),
                    transfer(&keypair.pubkey(), &tip_account, 10_000),
                */],
                Some(&keypair.pubkey()),
                &[keypair],
                *blockhash,
            ));
            let bundled_txs = BundledTransactions {
                mempool_txs: vec![mempool_tx],
                backrun_txs: vec![backrun_tx],
            };
            bundles.append(&mut vec![bundled_txs]);
        }
        bundles
}

async fn send_bundles(
    searcher_client: &mut SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    bundles: &[BundledTransactions],
) -> Result<Vec<result::Result<Response<SendBundleResponse>, Status>>> {
    let mut futs = Vec::with_capacity(bundles.len());
    for b in bundles {
        let mut searcher_client = searcher_client.clone();
        let txs = b
            .mempool_txs
            .clone()
            .into_iter()
            .chain(b.backrun_txs.clone().into_iter())
            .collect::<Vec<VersionedTransaction>>();
        let task =
            tokio::spawn(async move { send_bundle_no_wait(&txs, &mut searcher_client).await });
        futs.push(task);
    }

    let responses = futures_util::future::join_all(futs).await;
    let send_bundle_responses = responses.into_iter().map(|r| r.unwrap()).collect();
    Ok(send_bundle_responses)
}

fn generate_tip_accounts(tip_program_pubkey: &Pubkey) -> Vec<Pubkey> {
    let tip_pda_0 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_0"], tip_program_pubkey).0;
    let tip_pda_1 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_1"], tip_program_pubkey).0;
    let tip_pda_2 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_2"], tip_program_pubkey).0;
    let tip_pda_3 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_3"], tip_program_pubkey).0;
    let tip_pda_4 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_4"], tip_program_pubkey).0;
    let tip_pda_5 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_5"], tip_program_pubkey).0;
    let tip_pda_6 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_6"], tip_program_pubkey).0;
    let tip_pda_7 = Pubkey::find_program_address(&[b"TIP_ACCOUNT_7"], tip_program_pubkey).0;

    vec![
        tip_pda_0, tip_pda_1, tip_pda_2, tip_pda_3, tip_pda_4, tip_pda_5, tip_pda_6, tip_pda_7,
    ]
}

async fn maintenance_tick(
    searcher_client: &mut SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    rpc_client: &RpcClient,
    leader_schedule: &mut HashMap<Pubkey, HashSet<Slot>>,
    blockhash: &mut Hash,
    regions: Vec<String>,
) -> Result<()> {
    *blockhash = rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        })
        .await?
        .0;
    let new_leader_schedule = searcher_client
        .get_connected_leaders(ConnectedLeadersRequest {})
        .await?
        .into_inner()
        .connected_validators
        .iter()
        .fold(HashMap::new(), |mut hmap, (pubkey, slot_list)| {
            hmap.insert(
                Pubkey::from_str(pubkey).unwrap(),
                slot_list.slots.iter().cloned().collect(),
            );
            hmap
        });
    if new_leader_schedule != *leader_schedule {
        info!("connected_validators: {:?}", new_leader_schedule.keys());
        *leader_schedule = new_leader_schedule;
    }

    let next_scheduled_leader = searcher_client
        .get_next_scheduled_leader(NextScheduledLeaderRequest { regions })
        .await?
        .into_inner();
    info!(
        "next_scheduled_leader: {} in {} slots from {}",
        next_scheduled_leader.next_leader_identity,
        next_scheduled_leader.next_leader_slot - next_scheduled_leader.current_slot,
        next_scheduled_leader.next_leader_region
    );

    Ok(())
}

fn print_block_stats(
    block_stats: &mut HashMap<Slot, BlockStats>,
    block: SubscribeUpdateBlock,
    leader_schedule: &HashMap<Pubkey, HashSet<Slot>>,
    block_signatures: &mut HashMap<Slot, HashSet<Signature>>,
) {
    const KEEP_SIGS_SLOTS: u64 = 20;

    if let Some(stats) = block_stats.get(&block.slot) {
        datapoint_info!(
            "bundles-sent",
            ("slot", block.slot, i64),
            ("bundles", stats.bundles_sent.len(), i64),
            ("total_send_elapsed_us", stats.send_elapsed, i64),
            (
                "sent_rt_pp_min",
                stats.send_rt_per_packet.minimum().unwrap_or_default(),
                i64
            ),
            (
                "sent_rt_pp_max",
                stats.send_rt_per_packet.maximum().unwrap_or_default(),
                i64
            ),
            (
                "sent_rt_pp_avg",
                stats.send_rt_per_packet.mean().unwrap_or_default(),
                i64
            ),
            (
                "sent_rt_pp_p50",
                stats
                    .send_rt_per_packet
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "sent_rt_pp_p90",
                stats
                    .send_rt_per_packet
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "sent_rt_pp_p95",
                stats
                    .send_rt_per_packet
                    .percentile(95.0)
                    .unwrap_or_default(),
                i64
            ),
        );
    }

    let maybe_leader = leader_schedule
        .iter()
        .find(|(_, slots)| slots.contains(&block.slot))
        .map(|(leader, _)| leader);

    if let (b) = &block.transactions {
        if let (sigs) = &b.iter().map(|t| t.signature.clone() ) {
            let block_signatures: HashSet<Signature> = sigs.clone()
            .into_iter()
                .map(|s| bincode::deserialize::<Signature>(&s).unwrap())
                .collect();

            // bundles that were sent before or during this slot
            #[allow(clippy::type_complexity)]
            let bundles_sent_before_slot: HashMap<
                Slot,
                &[(
                    BundledTransactions,
                    tonic::Result<Response<SendBundleResponse>>,
                )],
            > = block_stats
                .iter()
                .filter(|(slot, _)| **slot <= block.slot)
                .map(|(slot, stats)| (*slot, stats.bundles_sent.as_ref()))
                .collect();

            if let Some(leader) = maybe_leader {
                // number of bundles sent before or during this slot
                let num_bundles_sent: usize = bundles_sent_before_slot
                    .values()
                    .map(|bundles_sent| bundles_sent.len())
                    .sum();

                // number of bundles where sending returned ok
                let num_bundles_sent_ok: usize = bundles_sent_before_slot
                    .values()
                    .map(|bundles_sent| {
                        bundles_sent
                            .iter()
                            .filter(|(_, send_response)| send_response.is_ok())
                            .count()
                    })
                    .sum();

                // a list of all bundles landed this slot that were sent before or during this slot
                let bundles_landed: Vec<(Slot, &BundledTransactions)> = bundles_sent_before_slot
                    .iter()
                    .flat_map(|(slot, bundles_sent_slot)| {
                        bundles_sent_slot
                            .iter()
                            .filter(|(_, send_response)| send_response.is_ok())
                            .filter_map(|(bundle_sent, _)| {
                                if bundle_sent
                                    .backrun_txs
                                    .iter()
                                    .chain(bundle_sent.mempool_txs.iter())
                                    .all(|tx| block_signatures.contains(&tx.signatures[0]))
                                {
                                    Some((*slot, bundle_sent))
                                } else {
                                    None
                                }
                            })
                    })
                    .collect();

                let mempool_txs_landed_no_bundle: Vec<(Slot, &BundledTransactions)> =
                    bundles_sent_before_slot
                        .iter()
                        .flat_map(|(slot, bundles_sent_slot)| {
                            bundles_sent_slot
                                .iter()
                                .filter(|(_, send_response)| send_response.is_ok())
                                .filter_map(|(bundle_sent, _)| {
                                    if bundle_sent
                                        .mempool_txs
                                        .iter()
                                        .any(|tx| block_signatures.contains(&tx.signatures[0]))
                                        && !bundle_sent
                                            .backrun_txs
                                            .iter()
                                            .any(|tx| block_signatures.contains(&tx.signatures[0]))
                                    {
                                        Some((*slot, bundle_sent))
                                    } else {
                                        None
                                    }
                                })
                        })
                        .collect();

                // find the min and max distance from when the bundle was sent to what block it landed in
                let min_bundle_send_slot = bundles_landed
                    .iter()
                    .map(|(slot, _)| *slot)
                    .min()
                    .unwrap_or(0);
                let max_bundle_send_slot = bundles_landed
                    .iter()
                    .map(|(slot, _)| *slot)
                    .max()
                    .unwrap_or(0);

                datapoint_info!(
                    "leader-bundle-stats",
                    ("slot", block.slot, i64),
                    ("leader", leader.to_string(), String),
                    ("block_txs", block_signatures.len(), i64),
                    ("num_bundles_sent", num_bundles_sent, i64),
                    ("num_bundles_sent_ok", num_bundles_sent_ok, i64),
                    (
                        "num_bundles_sent_err",
                        num_bundles_sent - num_bundles_sent_ok,
                        i64
                    ),
                    ("num_bundles_landed", bundles_landed.len(), i64),
                    (
                        "num_bundles_dropped",
                        num_bundles_sent - bundles_landed.len(),
                        i64
                    ),
                    ("min_bundle_send_slot", min_bundle_send_slot, i64),
                    ("max_bundle_send_slot", max_bundle_send_slot, i64),
                    (
                        "mempool_txs_landed_no_bundle",
                        mempool_txs_landed_no_bundle.len(),
                        i64
                    ),
                );

                // leaders last slot, clear everything out
                // might mess up metrics if leader doesn't produce a last slot or there's lots of slots
                // close to each other
                if block.slot % 4 == 3 {
                    block_stats.clear();
                }
            } else {
                // figure out how many transactions in bundles landed in slots other than our leader
                let num_mempool_txs_landed: usize = bundles_sent_before_slot
                    .values()
                    .map(|bundles| {
                        bundles
                            .iter()
                            .filter(|(bundle, _)| {
                                bundle
                                    .mempool_txs
                                    .iter()
                                    .any(|tx| block_signatures.contains(&tx.signatures[0]))
                            })
                            .count()
                    })
                    .sum();
                if num_mempool_txs_landed > 0 {
                    datapoint_info!(
                        "non-leader-bundle-stats",
                        ("slot", block.slot, i64),
                        ("mempool_txs_landed", num_mempool_txs_landed, i64),
                    );
                }
            }
        }
    }

     let (b) = &block.transactions;
for packet in b.iter() {
    let sig = bincode::deserialize::<Signature>(&packet.signature).unwrap();
    block_signatures
        .entry(block.slot)
        .or_insert_with(HashSet::new)
        .insert(sig);
}

    // throw away signatures for slots > KEEP_SIGS_SLOTS old
    block_signatures.retain(|slot, _| *slot > block.slot - KEEP_SIGS_SLOTS);
}

#[allow(clippy::too_many_arguments)]
async fn run_searcher_loop(
    block_engine_url: String,
    auth_keypair: Arc<Keypair>,
    keypair: &Keypair,
    rpc_url: String,
    regions: Vec<String>,
    message: String,
    tip_program_pubkey: Pubkey,
    mut slot_receiver: Receiver<Slot>,
    mut block_receiver: Receiver<SubscribeUpdateBlock>,
    mut bundle_results_receiver: Receiver<BundleResult>,
    mut pending_tx_receiver: Receiver<PendingTxNotification>,
    graph: &mut Graph,
    tokens: &mut HashSet<Token>,
    seen_pool_ids: &mut HashSet<String>,
) -> Result<()> {
    let mut leader_schedule: HashMap<Pubkey, HashSet<Slot>> = HashMap::new();
    let mut block_stats: HashMap<Slot, BlockStats> = HashMap::new();
    let mut block_signatures: HashMap<Slot, HashSet<Signature>> = HashMap::new();

    let mut searcher_client = get_searcher_client(&block_engine_url, &auth_keypair).await?;

    let mut rng = thread_rng();

    let tip_accounts = generate_tip_accounts(&tip_program_pubkey);
    info!("tip accounts: {:?}", tip_accounts);

    let rpc_client = RpcClient::new(rpc_url);
    let mut blockhash = rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        })
        .await?
        .0;

    let mut highest_slot = 0;
    let mut is_leader_slot = false;

    let mut tick = interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                maintenance_tick(&mut searcher_client, &rpc_client, &mut leader_schedule, &mut blockhash, regions.clone()).await?;
            }
            maybe_bundle_result = bundle_results_receiver.recv() => {
                let bundle_result: BundleResult = maybe_bundle_result.ok_or(BackrunError::Shutdown)?;
              //  info!("received bundle_result: [bundle_id={:?}, result={:?}]", bundle_result.bundle_id, bundle_result.result);
            }
            maybe_pending_tx_notification = pending_tx_receiver.recv() => {
                // block engine starts forwarding a few slots early, for super high activity accounts
                // it might be ideal to wait until the leader slot is up
                if is_leader_slot {
                    let pending_tx_notification = maybe_pending_tx_notification.ok_or(BackrunError::Shutdown)?;
                    if pending_tx_notification.server_side_ts.as_ref().unwrap().seconds < (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() - 5) as i64 {
                        continue;
                    }
                    let bundles = build_bundles(&rpc_client, pending_tx_notification, keypair, &blockhash, &tip_accounts, &mut rng, &message,  graph,  tokens,  seen_pool_ids).await;
                    if !bundles.is_empty() {
                        /*let now = Instant::now();
                        let results = send_bundles(&mut searcher_client, &bundles).await?;
                        let send_elapsed = now.elapsed().as_micros() as u64;
                        let send_rt_pp_us = send_elapsed / bundles.len() as u64;

                        match block_stats.entry(highest_slot) {
                            Entry::Occupied(mut entry) => {
                                let stats = entry.get_mut();
                                stats.bundles_sent.extend(bundles.into_iter().zip(results.into_iter()));
                                stats.send_elapsed += send_elapsed;
                                let _ = stats.send_rt_per_packet.increment(send_rt_pp_us);
                            }
                            Entry::Vacant(entry) => {
                                let mut send_rt_per_packet = Histogram::new();
                                let _ = send_rt_per_packet.increment(send_rt_pp_us);
                                entry.insert(BlockStats {
                                    bundles_sent: bundles.into_iter().zip(results.into_iter()).collect(),
                                    send_elapsed,
                                    send_rt_per_packet
                                });
                            }
                        }*/
                    }
                }
            }
            maybe_slot = slot_receiver.recv() => {
                highest_slot = maybe_slot.ok_or(BackrunError::Shutdown)?;
                is_leader_slot = leader_schedule.iter().any(|(_, slots)| slots.contains(&highest_slot));
            }
            maybe_block = block_receiver.recv() => {
                let block = maybe_block.ok_or(BackrunError::Shutdown)?;
                print_block_stats(&mut block_stats, block, &leader_schedule, &mut block_signatures);
            }
        }
    }
}

fn main() -> Result<()> {
    env_logger::builder()
        .format_timestamp(Some(TimestampPrecision::Micros))
        .init();
    let args: Args = Args::parse();

    let payer_keypair = Arc::new(read_keypair_file(&args.payer_keypair).expect("parse kp file"));
    let auth_keypair = Arc::new(read_keypair_file(&args.auth_keypair).expect("parse kp file"));
    let mut graph = Graph::new();
    let mut tokens = HashSet::new();
    let mut seen_pool_ids = HashSet::new();
    set_host_id(auth_keypair.pubkey().to_string());

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let (slot_sender, slot_receiver) = channel(100);
        let (block_sender, block_receiver) = channel(100);
        let (bundle_results_sender, bundle_results_receiver) = channel(100);
        let (pending_tx_sender, pending_tx_receiver) = channel(100);

        tokio::spawn(slot_subscribe_loop(args.pubsub_url.clone(), slot_sender));
        tokio::spawn(block_subscribe_loop(args.pubsub_url.clone(), block_sender));
        tokio::spawn(pending_tx_loop(
            args.block_engine_url.clone(),
            auth_keypair.clone(),
            pending_tx_sender,
            vec![Pubkey::from_str(&"5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h").unwrap()
        ,Pubkey::from_str(&"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(),
        Pubkey::from_str(&"whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap()],
        ));

        if args.subscribe_bundle_results {
            tokio::spawn(bundle_results_loop(
                args.block_engine_url.clone(),
                bundle_results_sender,
            ));
        }
        let rpc_client = RpcClient::new(args.rpc_url.clone());
let mut futures = FuturesOrdered::new();
for account_key in RAYDIUM_KEYS_SET.iter() {

        futures.push_back(calculate_plain_old_pool_swap_price(&rpc_client, account_key.to_string()));

}
let mut index = 0;
while let Some(result) = futures.next().await {
        let account_key = RAYDIUM_KEYS_SET.iter().nth(index).unwrap();
        index += 1;
    let swap_price_after = result.unwrap();
        println !("plain_old_pool_swap_price {:?}", swap_price_after);
        if tokens.contains(&Token { name: swap_price_after.3.to_string() }) {
            tokens.insert(Token { name: swap_price_after.3.to_string() });
        }
        if tokens.contains(&Token { name: swap_price_after.4.to_string() }) {
            tokens.insert(Token { name: swap_price_after.4.to_string() });
        }
        graph.add_token(Token { name: swap_price_after.3.to_string() });
        graph.add_token(Token { name: swap_price_after.4.to_string() });
        graph.add_edge(Token { name: swap_price_after.3.to_string() }, Token { name: swap_price_after.4.to_string() }, swap_price_after.1, swap_price_after.2, 30, 10000, account_key.to_string());
        graph.add_edge(Token { name: swap_price_after.4.to_string() }, Token { name: swap_price_after.3.to_string() }, swap_price_after.2, swap_price_after.1, 30, 10000, account_key.to_string());
    }
        let result = run_searcher_loop(
            args.block_engine_url,
            auth_keypair,
            &payer_keypair,
            args.rpc_url,
            args.regions,
            args.message,
            args.tip_program_id,
            slot_receiver,
            block_receiver,
            bundle_results_receiver,
            pending_tx_receiver,
            &mut graph,
            &mut tokens,
            &mut seen_pool_ids,
        )
        .await;
        error!("searcher loop exited result: {result:?}");

        Ok(())
    })
}
