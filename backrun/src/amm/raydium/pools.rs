use crate::amm::rescale;
use cached::proc_macro::once;
use regex::Regex;
use bytemuck::{Pod, Zeroable};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::message::Message;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey,
    pubkey::Pubkey,
    transaction::Transaction,
};
use std::collections::HashMap; // Add this import
use std::convert::TryInto;

use std::str::FromStr;
use std::sync::Arc;
use switchboard_common::error::SbError;
use borsh::{BorshDeserialize, BorshSerialize};

// Raydium API URL
pub const RAYDIUM_POOL_AMM_STABLE: Pubkey = pubkey!("5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h");
pub const RAYDIUM_POOLS_API: &str = "https://api.raydium.io/v2/sdk/liquidity/mainnet.json";

const LIQUIDITY_FEES_NUMERATOR: u64 = 25;
const LIQUIDITY_FEES_DENOMINATOR: u64 = 10000;

// Response from the Raydium API https://api.raydium.io/v2/sdk/liquidity/mainnet.json
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaydiumLiquidityPools {
    pub name: String,
    pub version: Option<Version>,
    pub official: Vec<RaydiumLiquidityPool>,
    pub un_official: Vec<RaydiumLiquidityPool>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Version {
    pub major: i64,
    pub minor: i64,
    pub patch: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaydiumLiquidityPool {
    pub id: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub lp_mint: String,
    pub version: Option<i64>,
    pub program_id: String,
    pub authority: String,
    pub open_orders: String,
    pub target_orders: String,
    pub base_vault: String,
    pub quote_vault: String,
    pub withdraw_queue: String,
    pub lp_vault: String,
    pub market_version: i64,
    pub market_program_id: String,
    pub market_id: String,
    pub market_authority: String,
    pub market_base_vault: String,
    pub market_quote_vault: String,
    pub market_bids: String,
    pub market_asks: String,
    pub market_event_queue: String,
}

// try_into conversion from RaydiumLiquidityPool to LiquidityPoolKeys
impl TryInto<LiquidityPoolKeys> for RaydiumLiquidityPool {
    type Error = SbError;
    fn try_into(self) -> Result<LiquidityPoolKeys, SbError> {
        Ok(LiquidityPoolKeys {
            id: Pubkey::from_str(&self.id).map_err(|_e| SbError::KeyParseError)?,
            authority: Pubkey::from_str(&self.authority).map_err(|_e| SbError::KeyParseError)?,
            open_orders: Pubkey::from_str(&self.open_orders)
                .map_err(|_e| SbError::KeyParseError)?,
            base_vault: Pubkey::from_str(&self.base_vault).map_err(|_e| SbError::KeyParseError)?,
            quote_vault: Pubkey::from_str(&self.quote_vault)
                .map_err(|_e| SbError::KeyParseError)?,
            lp_mint: Pubkey::from_str(&self.lp_mint).map_err(|_e| SbError::KeyParseError)?,
            market_id: Pubkey::from_str(&self.market_id).map_err(|_e| SbError::KeyParseError)?,
            market_event_queue: Pubkey::from_str(&self.market_event_queue)
                .map_err(|_e| SbError::KeyParseError)?,
            program_id: Pubkey::from_str(&self.program_id).map_err(|_e| SbError::KeyParseError)?,
        })
    }
}

#[derive(Default, Debug, Clone)]
pub struct LiquidityPoolKeys {
    pub id: Pubkey,
    pub authority: Pubkey,
    pub open_orders: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub market_id: Pubkey,
    pub market_event_queue: Pubkey,
    pub program_id: Pubkey,
    // Assume other fields as necessary
}

#[repr(C)]

#[derive(Clone, Copy, Zeroable, Pod)]
pub struct PoolInfoInstructionData {
    instruction: u8,
    simulate_type: u8,
}

#[allow(unused)]
#[derive(Default, Debug, Clone)]
struct PoolInfo {
    status: u64,
    base_decimals: u8,
    quote_decimals: u8,
    lp_decimals: u8,
    base_reserve: u64,
    quote_reserve: u64,
    lp_supply: u64,
    start_time: i64,
}

// Get Raydium liquidity pool
pub async fn get_raydium_pool(pool_id: &str) -> Option<RaydiumLiquidityPool> {
    let pools = fetch_async().await;
    if pools.is_ok() {
        let pools = pools.unwrap();
        let pool = pools.get(pool_id)?;
        Some(pool.clone())
    } else {
        None
    }
}

// Refresh every 12 hours (~160MB call)
#[once(result = true, time = 43200)]
pub async fn fetch_async() -> Result<HashMap<String, RaydiumLiquidityPool>, SbError> {
    #[cfg(not(test))]
    {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| SbError::CustomError {
                message: "Failed to build reqwest client".to_string(),
                source: Arc::new(e),
            })?;
        let res = client
            .get(RAYDIUM_POOLS_API.to_string())
            .send()
            .await
            .map_err(|e| SbError::CustomError {
                message: "Failed to get raydium pools".to_string(),
                source: Arc::new(e),
            })?;
        let json = res
            .json::<RaydiumLiquidityPools>()
            .await
            .map_err(|e| SbError::CustomError {
                message: "Failed to parse raydium api json".to_string(),
                source: Arc::new(e),
            })?;

        // Create a map of pool id to pool
        let mut map: HashMap<String, RaydiumLiquidityPool> = HashMap::new();

        // Add official pools
        for pool in json.official {
            map.insert(pool.id.clone(), pool);
        }

        // Add unofficial pools
        for pool in json.un_official {
            map.insert(pool.id.clone(), pool);
        }

        Ok(map)
    }

    // use local JSON file for testing
    #[cfg(test)]
    {
        use std::fs;

        // let file_path = "path/to/your/local/results.json"; // Adjust the path to your "results.json"
        let file_path = "raydium_pool_results.json";
        let file_contents = fs::read_to_string(file_path).map_err(|_| {
            SbError::CustomMessage("Failed to read local Raydium JSON file".to_string())
        })?;
        let json: RaydiumLiquidityPools = serde_json::from_str(&file_contents).map_err(|_| {
            SbError::CustomMessage("Failed to parse local Raydium JSON file".to_string())
        })?;

        let mut map: HashMap<String, RaydiumLiquidityPool> = HashMap::new();

        // Add official pools
        for pool in json.official {
            map.insert(pool.id.clone(), pool);
        }

        // Add unofficial pools
        for pool in json.un_official {
            map.insert(pool.id.clone(), pool);
        }

        Ok(map)
    }
}

pub async fn calculate_pool_swap_price(
    rpc_client: &RpcClient,
    pool: String,
    base_reserve: u64,
    quote_reserve: u64,
) -> Result<Decimal, SbError> {
    let raydium_pool = get_raydium_pool(&pool).await;
    if raydium_pool.is_some() {
        let raydium_pool = raydium_pool.unwrap();
        let program_id = Pubkey::from_str(&raydium_pool.program_id).unwrap();
        if raydium_pool.version == Some(5) || RAYDIUM_POOL_AMM_STABLE == program_id {
            Err(SbError::CustomMessage(
                "StableCurve is not enabled for Raydium Pools".to_string(),
            ))
        } else {
            // get raydium pool as liquidity pool keys
            let pool_keys: LiquidityPoolKeys =
                raydium_pool.try_into().map_err(|e| SbError::CustomError {
                    message: "Failed to convert raydium pool to liquidity pool keys".to_string(),
                    source: std::sync::Arc::new(e),
                })?;

            // simulate the instructions
            let simulate_pool_info_instruction = make_simulate_pool_info_instruction(&pool_keys)?;
            let simulate_payer_pubkey = pubkey!("RaydiumSimuLateTransaction11111111111111111");

            let blockhash =
                rpc_client
                    .get_latest_blockhash()
                    .await
                    .map_err(|e| SbError::CustomError {
                        message: "Failed to get recent blockhash".to_string(),
                        source: std::sync::Arc::new(e),
                    })?;

            let transaction = Transaction::new_unsigned(Message::new_with_blockhash(
                &[simulate_pool_info_instruction],
                Some(&simulate_payer_pubkey),
                &blockhash,
            ));

            let simulate_pool_info_result = rpc_client
                .simulate_transaction(&transaction)
                .await
                .map_err(|e| SbError::CustomError {
                    message: "Failed to simulate pool info instruction".to_string(),
                    source: std::sync::Arc::new(e),
                })?;

            // Get the simulated pool info result
            let logs: Vec<String> = match simulate_pool_info_result.value.logs {
                Some(logs) => logs,
                None => {
                    return Err(SbError::CustomMessage(
                        "Failed to get logs from simulate pool info result".to_string(),
                    ));
                }
            };

            // map the logs to the pools info
            // filter logs that don't contain "GetPoolData"

            let pools_info: Vec<PoolInfo> = logs
                .iter()
                .filter_map(|log| {
                    if log.contains("GetPoolData") {
                        Some(log)
                    } else {
                        None
                    }
                })
                .map(|log| {
                    let json = parse_simulate_log_to_json(log, "GetPoolData")?;
                    let status = parse_simulate_value(&json, "status")?;
                    let base_decimals = parse_simulate_value(&json, "coin_decimals")?;
                    let quote_decimals = parse_simulate_value(&json, "pc_decimals")?;
                    let lp_decimals = parse_simulate_value(&json, "lp_decimals")?;
                    //let base_reserve = parse_simulate_value(&json, "pool_coin_amount")?;
                    //let quote_reserve = parse_simulate_value(&json, "pool_pc_amount")?;
                    let lp_supply = parse_simulate_value(&json, "pool_lp_supply")?;
                    let start_time = parse_simulate_value(&json, "pool_open_time")?;

                    // get start time as i64
                    let start_time =
                        start_time
                            .parse::<i64>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse start time".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;

                    // get status as u64
                    let status = status.parse::<u64>().map_err(|e| SbError::CustomError {
                        message: "Failed to parse status".to_string(),
                        source: std::sync::Arc::new(e),
                    })?;

                    // get base decimals as u8
                    let base_decimals =
                        base_decimals
                            .parse::<u8>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse base decimals".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;

                    // get quote decimals as u8
                    let quote_decimals =
                        quote_decimals
                            .parse::<u8>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse quote decimals".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;

                    // get lp decimals as u8
                    let lp_decimals =
                        lp_decimals
                            .parse::<u8>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse lp decimals".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;

                    /*// get base reserve as u64
                    let base_reserve =
                        base_reserve
                            .parse::<u64>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse base reserve".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;

                    // get quote reserve as u64
                    let quote_reserve =
                        quote_reserve
                            .parse::<u64>()
                            .map_err(|e| SbError::CustomError {
                                message: "Failed to parse quote reserve".to_string(),
                                source: std::sync::Arc::new(e),
                            })?;
                    */
                    // get lp supply as u64
                    let lp_supply = lp_supply.parse::<u64>().map_err(|e| SbError::CustomError {
                        message: "Failed to parse lp supply".to_string(),
                        source: std::sync::Arc::new(e),
                    })?;

                    Ok(PoolInfo {
                        status,
                        base_decimals,
                        quote_decimals,
                        lp_decimals,
                        base_reserve,
                        quote_reserve,
                        lp_supply,
                        start_time,
                    })
                })
                .collect::<Result<Vec<PoolInfo>, SbError>>()?;

            // ensure there's only 1 pool info
            if pools_info.len() != 1 {
                return Err(SbError::CustomMessage(
                    "Failed to get pool info".to_string(),
                ));
            }

            // get the pool info for the first pool (the only one we're fetching data for)
            let pool_info = pools_info[0].clone();

            // compute amount out
            let (current_price, _) = compute_amount_out(pool_keys, pool_info)?;

            // check that current price is not zero
            if current_price.is_zero() {
                return Err(SbError::CustomMessage(
                    "Failed to calculate current price".to_string(),
                ));
            }

            Ok(current_price)
        }
    } else {
        Err(SbError::CustomMessage(
            "Failed to get raydium pool".to_string(),
        ))
    }
}

fn compute_amount_out(
    _pool_keys: LiquidityPoolKeys,
    pool_info: PoolInfo,
) -> Result<(Decimal, Decimal), SbError> {
    // Logic to determine tokenIn and tokenOut from the inputs (omitted for brevity)
    let PoolInfo {
        base_reserve,
        base_decimals,
        quote_reserve,
        quote_decimals,
        ..
    } = pool_info;

    // Example of checked arithmetic with Decimal
    let reserve_in = base_reserve;
    let reserve_out = quote_reserve;

    // https://github.com/switchboard-xyz/sbv3/blob/main/javascript/task-runner/src/clients/raydium.ts#L207
    let amount_in = 100u64 * 10u64.pow(base_decimals as u32);

    let reserve_in_decimal = rescale(Decimal::from(reserve_in), base_decimals);
    let reserve_out_decimal = rescale(Decimal::from(reserve_out), quote_decimals);

    // Get current price
    let current_price =
        reserve_out_decimal
            .checked_div(reserve_in_decimal)
            .ok_or(SbError::CustomMessage(
                "Failed to calculate current price".to_string(),
            ))?;

    let fee: u64 = amount_in
        .checked_mul(LIQUIDITY_FEES_NUMERATOR)
        .ok_or(SbError::CustomMessage(
            "Failed to calculate fee".to_string(),
        ))?
        .checked_div(LIQUIDITY_FEES_DENOMINATOR)
        .ok_or(SbError::CustomMessage(
            "Failed to calculate fee".to_string(),
        ))?;

    let amount_in_with_fee: u64 = amount_in.checked_sub(fee).ok_or(SbError::CustomMessage(
        "Failed to calculate amount in with fee".to_string(),
    ))?;

    let amount_out = {
        let denominator =
            reserve_in
                .checked_add(amount_in_with_fee)
                .ok_or(SbError::CustomMessage(
                    "Failed to calculate denominator".to_string(),
                ))?;
        reserve_out_decimal
            .checked_mul(amount_in_with_fee.into())
            .ok_or(SbError::CustomMessage(
                "Failed to calculate amount out".to_string(),
            ))?
            .checked_div(denominator.into())
            .ok_or(SbError::CustomMessage(
                "Failed to calculate amount out".to_string(),
            ))?
    };

    let amount_out_decimal = rescale(Decimal::from(amount_out), quote_decimals);
    let amount_in_with_fee_decimal = rescale(Decimal::from(amount_in_with_fee), base_decimals);
    let execution_price = amount_out_decimal
        .checked_div(amount_in_with_fee_decimal)
        .ok_or(SbError::CustomMessage(
            "Failed to calculate execution price".to_string(),
        ))?;
    Ok((current_price, execution_price))
}

pub fn make_simulate_pool_info_instruction(
    pool_keys: &LiquidityPoolKeys,
) -> Result<Instruction, SbError> {
    let data: Vec<u8> = bytemuck::bytes_of(&PoolInfoInstructionData {
        instruction: 12,
        simulate_type: 0,
    }).to_vec();
    let keys = vec![
        AccountMeta::new_readonly(pool_keys.id, false),
        AccountMeta::new_readonly(pool_keys.authority, false),
        AccountMeta::new_readonly(pool_keys.open_orders, false),
        AccountMeta::new_readonly(pool_keys.base_vault, false),
        AccountMeta::new_readonly(pool_keys.quote_vault, false),
        AccountMeta::new_readonly(pool_keys.lp_mint, false),
        AccountMeta::new_readonly(pool_keys.market_id, false),
        AccountMeta::new_readonly(pool_keys.market_event_queue, false),
    ];

    Ok(Instruction {
        program_id: pool_keys.program_id,
        accounts: keys,
        data,
    })
}

fn parse_simulate_log_to_json(log: &str, keyword: &str) -> Result<String, SbError> {
    // Check if the keyword exists in the log
    if !log.contains(keyword) {
        return Err(SbError::CustomMessage(
            "Failed to find keyword in log".to_string(),
        ));
    }

    // Regex to match JSON object that might not be isolated
    let re = Regex::new(r"\{[^{}]+\}")
        .map_err(|_| SbError::CustomMessage("Failed to compile regex".to_string()))?;

    // Find the JSON object after the keyword
    let log_part = log.split(keyword).nth(1).ok_or(SbError::CustomMessage(
        "Failed to split log by keyword".to_string(),
    ))?;

    let results: Vec<_> = re.find_iter(log_part).collect();

    if results.is_empty() {
        return Err(SbError::CustomMessage(
            "Failed to find JSON object".to_string(),
        ));
    }

    Ok(results[0].as_str().to_string())
}

fn parse_simulate_value(log: &str, key: &str) -> Result<String, SbError> {
    let re = Regex::new(&format!(r#""{}":(\d+)"#, key)).map_err(|e| SbError::CustomError {
        message: "Failed to compile regex".to_string(),
        source: Arc::new(e),
    })?;

    let caps = re.captures(log).ok_or_else(|| SbError::CustomError {
        message: "simulate log failed to match key".to_string(),
        source: Arc::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "No match found",
        )),
    })?;

    if caps.len() == 2 {
        Ok(caps.get(1).unwrap().as_str().to_string())
    } else {
        Err(SbError::CustomError {
            message: "simulate log fail to match key".to_string(),
            source: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Incorrect number of captures",
            )),
        })
    }
}