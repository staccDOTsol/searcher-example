mod event_loops;
mod amm;
use amm::raydium::pools::calculate_pool_swap_price;
//use amm::raydium::clmm::calculate_clmm_swap_price;
//use amm::orca::calculate_swap_price;
//use amm::orca::legacy_pools::get_legacy_pool;'
use anyhow::anyhow;
mod serde_helpers;
use serde_helpers::field_as_string;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use futures::{stream::{FuturesOrdered, FuturesUnordered}, Future, StreamExt};
use std::{
    borrow::BorrowMut, collections::{hash_map::Entry, HashMap, HashSet}, path::PathBuf, result, str::FromStr, sync::Arc, time::{Duration, Instant}
};
use solana_sdk::message::v0;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;

use solana_sdk::message::VersionedMessage;

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
    get_searcher_client, send_bundle_no_wait, send_bundle_with_confirmation, token_authenticator::ClientInterceptor, BlockEngineConnectionError
};
use log::*;
use rand::{rngs::ThreadRng, thread_rng, Rng};
use solana_client::{
    client_error::ClientError, nonblocking::{pubsub_client::PubsubClientError, rpc_client::RpcClient}, rpc_config::RpcSimulateTransactionAccountsConfig, rpc_response::{self, RpcBlockUpdate}
};
use solana_metrics::{datapoint_info, set_host_id};
use solana_sdk::{
    bundle::VersionedBundle, clock::Slot, commitment_config::{CommitmentConfig, CommitmentLevel}, hash::Hash, instruction::Instruction, pubkey::Pubkey, signature::{read_keypair_file, Keypair, Signature, Signer}, system_instruction::transfer, transaction::{Transaction, VersionedTransaction}
};
use spl_memo::{build_memo, solana_program::program_pack::Pack};
use thiserror::Error;
use tokio::{
    runtime::Builder,
    sync::mpsc::{channel, Receiver},
    time::interval,
};
use tonic::{codegen::InterceptedService, transport::Channel, Response, Status, Streaming};
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


#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HashedAccount{

    pub is_signer: bool,
    pub is_writable: bool,
    pub pubkey: String,
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HashedIx{
    pub program_id: String,
    pub accounts: Vec<HashedAccount>,
    pub data: String
}


#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFee {
    #[serde(with = "field_as_string")]
    pub amount: u64,
    pub fee_bps: u8,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub enum SwapMode {
    #[default]
    ExactIn,
    ExactOut,
}

impl FromStr for SwapMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "ExactIn" => Ok(Self::ExactIn),
            "ExactOut" => Ok(Self::ExactOut),
            _ => Err(anyhow!("{} is not a valid SwapMode", s)),
        }
    }
}
/// Topologically sorted DAG with additional metadata for rendering
pub type RoutePlanWithMetadata = Vec<RoutePlanStep>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlanStep {
    pub swap_info: SwapInfo,
    pub percent: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SwapInfo {
    #[serde(with = "field_as_string")]
    pub amm_key: Pubkey,
    pub label: String,
    #[serde(with = "field_as_string")]
    pub input_mint: Pubkey,
    #[serde(with = "field_as_string")]
    pub output_mint: Pubkey,
    /// An estimation of the input amount into the AMM
    #[serde(with = "field_as_string")]
    pub in_amount: u64,
    /// An estimation of the output amount into the AMM
    #[serde(with = "field_as_string")]
    pub out_amount: u64,
    #[serde(with = "field_as_string")]
    pub fee_amount: u64,
    #[serde(with = "field_as_string")]
    pub fee_mint: Pubkey,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    #[serde(with = "field_as_string")]
    pub input_mint: Pubkey,
    #[serde(with = "field_as_string")]
    pub in_amount: u64,
    #[serde(with = "field_as_string")]
    pub output_mint: Pubkey,
    #[serde(with = "field_as_string")]
    pub out_amount: u64,
    /// Not used by build transaction
    #[serde(with = "field_as_string")]
    pub other_amount_threshold: u64,
    pub swap_mode: SwapMode,
    pub slippage_bps: u16,
    pub platform_fee: Option<PlatformFee>,
    pub price_impact_pct: String,
    pub route_plan: RoutePlanWithMetadata,
    #[serde(default)]
    pub context_slot: u64,
    #[serde(default)]
    pub time_taken: f64,
}
impl Default for QuoteResponse {
    fn default() -> Self {
        Self {
            input_mint: Pubkey::default(),
            in_amount: 0,
            output_mint: Pubkey::default(),
            out_amount: 0,
            other_amount_threshold: 0,
            swap_mode: SwapMode::ExactIn,
            slippage_bps: 0,
            platform_fee: None,
            price_impact_pct: "0".to_string(),
            route_plan: vec![],
            context_slot: 0,
            time_taken: 0.0,
        }
    }
}
impl QuoteResponse {
    pub async fn try_from_response(response: reqwest::Response) -> anyhow::Result<Self, anyhow::Error> {
        Ok(response.json::<QuoteResponse>().await.unwrap_or_default())
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructions{
    //"swapInstruction\":{\"programId\":\"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4\",\"accounts\":[{\"
    pub token_ledger_instruction: Option<HashedIx>,
    pub setup_instructions:Option<Vec<HashedIx>>,
    pub swap_instruction: HashedIx,
    pub cleanup_instruction: Option<HashedIx>,
    pub address_lookup_table_addresses: Vec<String>
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
    txs: Vec<VersionedTransaction>,
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
fn calculate_ideal_trade_sizes(
    max_quote_currency: f64,
    p_initial: f64,
    p_final: f64,
    delta: f64,
    trade_cost_percentage: f64,
) -> (f64, f64) {
    // Ensure p_initial and p_final are not too small to avoid division overflow
    if p_initial.abs() < 1e-30 || p_final.abs() < 1e-30 {
        return (0.0, 0.0); // Avoid calculations that might lead to overflow
    }
    if delta < 1.0 {
        let break_even_delta = 1.0 - trade_cost_percentage*2.0;
        let break_even_price = p_initial * break_even_delta;
        if p_final < break_even_price {
            // Price decreased, strategy might involve buying the base currency at the lower price
            let base_currency_acquired = max_quote_currency / p_final; // Acquire more base currency after price drop
            let total_trade_cost = max_quote_currency * trade_cost_percentage; // Only buying involved, so single trade cost
            let net_quote_currency = max_quote_currency - total_trade_cost;

            // Check if the operation is profitable or feasible
            if base_currency_acquired * p_final > net_quote_currency {
                (net_quote_currency, base_currency_acquired)
            } else {
                (0.0, 0.0) // Not profitable or feasible
            }
        } else {
            // Price increased, strategy might involve selling base currency at the higher price
            let base_currency_sold = max_quote_currency * p_final; // Sell base currency at the higher price
            let total_trade_cost = max_quote_currency * trade_cost_percentage; // Only selling involved, so single trade cost
            let net_quote_currency = max_quote_currency - total_trade_cost;

            // Check if the operation is profitable or feasible
            if base_currency_sold > net_quote_currency {
                (net_quote_currency, base_currency_sold)
            } else {
                (0.0, 0.0) // Not profitable or feasible
            }
            
                    }

                }
                    else {
                        let break_even_delta = 1.0 + trade_cost_percentage*2.0;
                        let break_even_price = p_initial * break_even_delta;
                        if p_final > break_even_price {
                            // Price increased, strategy might involve buying the base currency at the higher price
                            let base_currency_acquired = max_quote_currency / p_final; // Acquire more base currency after price increase
                            let total_trade_cost = max_quote_currency * trade_cost_percentage; // Only buying involved, so single trade cost
                            let net_quote_currency = max_quote_currency - total_trade_cost;

                            // Check if the operation is profitable or feasible
                            if base_currency_acquired * p_final > net_quote_currency {
                                (net_quote_currency, base_currency_acquired)
                            } else {
                                (0.0, 0.0) // Not profitable or feasible
                            }
                        } else {
                            // Price decreased, strategy might involve selling base currency at the lower price
                            let base_currency_sold = max_quote_currency * p_final; // Sell base currency at the lower price
                            let total_trade_cost = max_quote_currency * trade_cost_percentage; // Only selling involved, so single trade cost
                            let net_quote_currency = max_quote_currency - total_trade_cost;

                            // Check if the operation is profitable or feasible
                            if base_currency_sold > net_quote_currency {
                                (net_quote_currency, base_currency_sold)
                            } else {
                                (0.0, 0.0) // Not profitable or feasible
                            }
                        }
                    }

            }
fn deserialize_instruction (instruction: HashedIx) -> Instruction {
    let mut accounts = instruction.accounts.clone();
    for i in 0..accounts.len() {
        accounts[i].pubkey = Pubkey::from_str(&accounts[i].pubkey).unwrap().to_string();
    }
    let data = base64::decode(&instruction.data).unwrap();
    let program_id = Pubkey::from_str(&instruction.program_id).unwrap();
    let instruction = Instruction {
        program_id,
        accounts: accounts.iter().map(|account| {
            solana_sdk::instruction::AccountMeta {
                pubkey: Pubkey::from_str(&account.pubkey).unwrap(),
                is_signer: account.is_signer,
                is_writable: account.is_writable,
            }
        }).collect::<Vec<solana_sdk::instruction::AccountMeta>>(),
        data,
    };
    instruction
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

                
                println!("simulation_result.value.transaction_results.len() == 0");/*
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
                */
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
            let to_report_base ;
            if base_reserve_post_amount.amount > base_reserve_amount.amount {
                to_report_base = base_reserve_post_amount.amount - base_reserve_amount.amount;
            }
            else {
                to_report_base = base_reserve_amount.amount - base_reserve_post_amount.amount;
            }
            let to_report_quote;
            if quote_reserve_post_amount.amount > quote_reserve_amount.amount {
                to_report_quote = quote_reserve_post_amount.amount - quote_reserve_amount.amount;
            }
            else {
                to_report_quote = quote_reserve_amount.amount - quote_reserve_post_amount.amount;
            }
            let swap_price = swap_price_after / swap_price_before;
            println !("swap price before {:?}, after {:?}, delta {:?}, base amount diff {:?}, quote amount diff {:?}", swap_price_before, swap_price_after, swap_price, to_report_base, to_report_quote);
            let max_quote_currency = to_report_quote as f64 / 2.0;
            let p_initial = swap_price_before; // Initial price before the swap
            let p_final = swap_price_after; // Final price after the swap
            let delta = (swap_price_after - swap_price_before) / swap_price_before; // Percentage change, not ratio
            let trade_cost_percentage = 0.0025; // 25 bps
        
            let (ideal_quote_size, ideal_base_size) = calculate_ideal_trade_sizes(
                max_quote_currency.to_f64().unwrap(),
                p_initial.to_f64().unwrap(),
                p_final.to_f64().unwrap(),
                delta.to_f64().unwrap(),
                trade_cost_percentage,
            );
        
                
            println!("Ideal Quote Size: {}", ideal_quote_size);
            println!("Ideal Base Size: {}", ideal_base_size);
            let mut quote= Value::default();
            let mut quote0=     Value::default();
            let mut quote2= Value::default();
            let mut go = false;
            if ideal_quote_size > 0.0 && ideal_base_size > 0.0 {
               let ideal_quote_size = ideal_quote_size as u64;
               let ideal_base_size = ideal_base_size as u64;
                if swap_price.to_f64().unwrap() > 1.0 {

                    // buy quote, sell base 
                let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
                .to_owned()
                +&"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()+"&outputMint="
                +&base_reserve_amount.mint.to_string()+"&amount="
                +&ideal_base_size.to_string()
                +&"&swapMode=ExactOut";
                let quote00= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
                 quote0 = serde_json::from_str::<serde_json::Value>(quote00).unwrap();
                let input_amount = quote0["outAmount"].to_string();
                let input_amount = input_amount[1..input_amount.len()-1].parse::<f64>().unwrap_or_default();

                let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
                .to_owned()
                +&base_reserve_amount.mint.to_string()+"&outputMint="
                +&quote_reserve_amount.mint.to_string()+"&amount="
                +&input_amount.to_string()
                +&"&swapMode=ExactIn&"
                +&"dexes=Raydium";
                let quote11= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
                 quote = serde_json::from_str::<serde_json::Value>(quote11).unwrap();
                let output_amount = quote["outAmount"].to_string();
                let output_amount = output_amount[1..output_amount.len()-1].parse::<f64>().unwrap_or_default();
                
                if let Some(route_plan) = quote["routePlan"].as_array() {
                    let filtered_route_plan: Vec<Value> = route_plan.iter()
                        .filter(|swap| {
                            if let Some(amm_key) = swap["swapInfo"]["ammKey"].as_str() {
                                amm_key == account_key.to_string()
                            } else {
                                false
                            }
                        })
                        // deref
                        .cloned()
                        .collect();

                    quote["routePlan"] = serde_json::Value::Array(filtered_route_plan.clone());

                    // Now filtered_route_plan contains only the swap info with ammKey matching account_key
                    println!("Filtered Route Plan: {:?}", filtered_route_plan);


                let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
                .to_owned()
                +&quote_reserve_amount.mint.to_string()+"&outputMint="
                +&base_reserve_amount.mint.to_string()+"&amount="
                +&ideal_base_size.to_string()
                +&"&swapMode=ExactIn&"
                +&"dexes=Raydium";
                let quote22= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
                 quote2 = serde_json::from_str::<serde_json::Value>(quote22).unwrap();
                let output_amount = quote2["outAmount"].to_string();
                let output_amount = output_amount[1..output_amount.len()-1].parse::<f64>().unwrap_or_default();
                
                if let Some(route_plan) = quote2["routePlan"].as_array() {
                    let filtered_route_plan: Vec<Value> = route_plan.iter()
                        .filter(|swap| {
                            if let Some(amm_key) = swap["swapInfo"]["ammKey"].as_str() {
                                amm_key == account_key.to_string()
                            } else {
                                false
                            }
                        })
                        .cloned()
                        .collect();

                    quote2["routePlan"] = serde_json::Value::Array(filtered_route_plan.clone());

                }

                    // Now filtered_route_plan contains only the swap info with ammKey matching account_key
                    println!("Filtered Route Plan: {:?}", filtered_route_plan);

            }
            go = true;
        }
        else {
            // sell quote, buy base
            let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
            .to_owned()
            +&"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()+"&outputMint="
            +&quote_reserve_amount.mint.to_string()+"&amount="
            +&ideal_quote_size.to_string()
            +&"&swapMode=ExactIn";
            let quote00= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
             quote0 = serde_json::from_str::<serde_json::Value>(quote00).unwrap();
            let input_amount = quote0["outAmount"].to_string();
            let input_amount = input_amount[1..input_amount.len()-1].parse::<f64>().unwrap_or_default();

            let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
            .to_owned()
            +&quote_reserve_amount.mint.to_string()+"&outputMint="
            +&base_reserve_amount.mint.to_string()+"&amount="
            +&input_amount.to_string()
            +&"&swapMode=ExactOut&"
            +&"dexes=Raydium";
            let quote11= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
             quote = serde_json::from_str::<serde_json::Value>(quote11).unwrap();
            let output_amount = quote["outAmount"].to_string();
            let output_amount = output_amount[1..output_amount.len()-1].parse::<f64>().unwrap_or_default();
            
            if let Some(route_plan) = quote["routePlan"].as_array() {
                let filtered_route_plan: Vec<Value> = route_plan.iter()
                    .filter(|swap| {
                        if let Some(amm_key) = swap["swapInfo"]["ammKey"].as_str() {
                            amm_key == account_key.to_string()
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();

                quote["routePlan"] = serde_json::Value::Array(filtered_route_plan.clone());

            }

            let url = "https://quote-api.jup.ag/v6//quote?slippageBps=9999&asLegacyTransaction=false&inputMint="
            .to_owned()
            +&base_reserve_amount.mint.to_string()+"&outputMint="
            +&quote_reserve_amount.mint.to_string()+"&amount="
            +&ideal_quote_size.to_string()
            +&"&swapMode=ExactOut&"
            +&"dexes=Raydium";
            let quote22= &reqwest::get(url.clone()).await.unwrap().text().await.unwrap();
             quote2 = serde_json::from_str::<serde_json::Value>(quote22).unwrap();
            let output_amount = quote2["outAmount"].to_string();
            let output_amount = output_amount[1..output_amount.len()-1].parse::<f64>().unwrap_or_default();
            
            if let Some(route_plan) = quote2["routePlan"].as_array() {
                let filtered_route_plan: Vec<Value> = route_plan.iter()
                    .filter(|swap| {
                        if let Some(amm_key) = swap["swapInfo"]["ammKey"].as_str() {
                            amm_key == account_key.to_string()
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();

                quote2["routePlan"] = serde_json::Value::Array(filtered_route_plan.clone());
go = true;
            }
        }
            }
let mut txs: Vec<VersionedTransaction> = vec![];
let mut index = 0;
if go {

    let mut lutties = vec![];
for swap in vec![quote0, quote, quote2].iter() {
    let mut blockhash = rpc_client
    .get_latest_blockhash_with_commitment(CommitmentConfig {
        commitment: CommitmentLevel::Finalized,
    })
    .await.unwrap()
    .0;
    let mut ixs = vec![];
    index += 1;
    if index == 2 {
        txs.append(&mut vec![mempool_tx.clone()]);
    }
    let reqclient = reqwest::Client::new();
    println!("swap: {:?}", swap);
    let request_body: reqwest::Body = reqwest::Body::from(serde_json::json!({
        "quoteResponse": swap,
        "userPublicKey": "7ihN8QaTfNoDTRTQGULCzbUT3PHwPDTu5Brcu4iT2paP".to_string(),
        "restrictIntermediateTokens": false,
        "useSharedAccounts": true,
        "useTokenLedger": false,
        "asLegacyTransaction": false,
        "wrapAndUnwrapSol": false,
    }).to_string());
    let swap_transaction = reqclient.post("https://quote-api.jup.ag/v6//swap-instructions")
    .body(request_body
    ).send().await.unwrap().json::<SwapInstructions>().await;
    if swap_transaction.is_err() {
        println!("swap_transaction error: {:?}", swap_transaction);
        continue;
    }
    let swap_transaction = swap_transaction.unwrap();

    for lut_str in &swap_transaction.address_lookup_table_addresses {
        let lut = Pubkey::from_str(lut_str).unwrap();
        let account = rpc_client.get_account(&lut).await.unwrap();
        let account = solana_sdk::address_lookup_table::state::AddressLookupTable::deserialize(&account.data).unwrap();
        let lookup_table_address_account = AddressLookupTableAccount {
            key: lut,
            addresses: account.addresses.to_vec(),
        };
        lutties.push(lookup_table_address_account);
    }
    
    if swap_transaction.setup_instructions.is_some() {

        let maybe_setup_ixs = swap_transaction.setup_instructions.clone().unwrap().iter().map(|instruction| {
            deserialize_instruction(instruction.clone())
        }).collect::<Vec<Instruction>>();

        ixs.extend(maybe_setup_ixs);
    }
    let maybe_swap_ix = deserialize_instruction(swap_transaction.swap_instruction.clone());
    ixs.extend(vec![maybe_swap_ix]);
    if swap_transaction.cleanup_instruction.is_some() {
        let maybe_cleanup_ix = deserialize_instruction(swap_transaction.cleanup_instruction.clone().unwrap());
        ixs.extend(vec![maybe_cleanup_ix]);
    }
    
    txs.append(&mut vec![VersionedTransaction::try_new(
        VersionedMessage::V0(v0::Message::try_compile(
            &keypair.pubkey(),
            &ixs,
            &lutties,
            blockhash,
        ).unwrap()),
        &[keypair],
    ).unwrap()]);
}

let mut blockhash = rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        })
        .await.unwrap()
        .0;
let tip_account = tip_accounts[rng.gen_range(0..tip_accounts.len())];

let backrun_tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
    &[
       
        transfer(&keypair.pubkey(), &tip_account, 100_000),
    ],
    Some(&keypair.pubkey()),
    &[keypair],
    blockhash,
));
txs.append(&mut vec![backrun_tx]);
if (txs.len() > 2){
bundles.append(&mut vec![BundledTransactions {
    txs: txs.clone() }]);
}
}
            /*if tokens.contains(&Token { name: base_reserve_amount.mint.to_string() }) {
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
            }*/
        }
    }

    }
        
        }
        bundles
}

async fn send_bundles(
    searcher_client: &mut SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    rpc_client: &RpcClient, 
    bundles: &[BundledTransactions],
) -> Result<()> {
    for b in bundles {
        println!("sending bundle {:?} to searcher", b.txs);
        let mut searcher_client = searcher_client.clone();
        let txs = b
            .txs
            .iter()
            .map(|tx| tx.clone().into())
            .collect::<Vec<VersionedTransaction>>();
        send_bundle_with_confirmation(&txs, rpc_client, &mut searcher_client).await.unwrap();
    }
      Ok(())
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
                                    .txs
                                    .iter()

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
                                        .txs
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
                                    .txs
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

    let mut tick = interval(Duration::from_secs(59));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                maintenance_tick(&mut searcher_client, &rpc_client, &mut leader_schedule, &mut blockhash, regions.clone()).await?;
            }

            maybe_bundle_result = bundle_results_receiver.recv() => {
                let bundle_result: BundleResult = maybe_bundle_result.ok_or(BackrunError::Shutdown)?;
                if bundle_result.result.is_none() {
                    continue;
                }
                info!("received bundle_result: [bundle_id={:?}, result={:?}]", bundle_result.bundle_id, bundle_result.result);
            }
            maybe_pending_tx_notification = pending_tx_receiver.recv() => {
                // block engine starts forwarding a few slots early, for super high activity accounts
                // it might be ideal to wait until the leader slot is up
                if is_leader_slot {
                    if maybe_pending_tx_notification.is_none() {
                        continue;
                    }
                    let pending_tx_notification = maybe_pending_tx_notification.unwrap();
                    if pending_tx_notification.server_side_ts.as_ref().unwrap().seconds < (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() - 5) as i64 {
                        continue;
                    }
                    let bundles = build_bundles(&rpc_client, pending_tx_notification, keypair, &blockhash, &tip_accounts, &mut rng, &message,  graph,  tokens,  seen_pool_ids).await;
                    if !bundles.is_empty() {
                        println!("sending bundles: {:?}", bundles.len());

                        let now = Instant::now();
                        send_bundles(&mut searcher_client, &rpc_client, &bundles).await?;
                        
                    }
                }
            }
            maybe_slot = slot_receiver.recv() => {
                highest_slot = maybe_slot.ok_or(BackrunError::Shutdown)?;
                is_leader_slot = leader_schedule.iter().any(|(_, slots)| slots.contains(&highest_slot));
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
            vec![Pubkey::from_str(&"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap()
        ,Pubkey::from_str(&"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(),
        Pubkey::from_str(&"whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap()],
        ));

        if args.subscribe_bundle_results {
            tokio::spawn(bundle_results_loop(
                args.block_engine_url.clone(),
                bundle_results_sender,
            ));
        }
        /*let rpc_client = RpcClient::new(args.rpc_url.clone());
        
let mut futures = FuturesOrdered::new();
for account_key in RAYDIUM_KEYS_SET.iter() {

        futures.push_back(calculate_plain_old_pool_swap_price(&rpc_client, account_key.to_string()));

}
let mut index = 0;
while let Some(result) = futures.next().await {
        let account_key = RAYDIUM_KEYS_SET.iter().nth(index).unwrap();
        index += 1;
        if result.is_err() {
            println!("result error: {:?}", result);
            continue;
        }

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
    }*/
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
