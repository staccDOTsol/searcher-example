use crate::amm::rescale;
use arrayref::array_ref;
use cached::proc_macro::once;
use raydium_amm_v3::states::{AmmConfig, PoolState, TickArrayBitmapExtension};
use raydium_client::instructions::{
    deserialize_anchor_account, get_out_put_amount_and_remaining_accounts,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey, pubkey::Pubkey};
use spl_token_2022::{
    extension::{transfer_fee::TransferFeeConfig, BaseStateWithExtensions, StateWithExtensions},
    state::Mint,
};
use std::collections::HashMap; // Add this import
use std::collections::VecDeque;
use std::str::FromStr;
#[allow(unused_imports)]
use std::sync::Arc;
use switchboard_common::error::SbError;

#[allow(unused)]
pub const RAYDIUM_V3_AMM_POOLS_API: &str = "https://api.raydium.io/v2/ammV3/ammPools";
#[allow(unused)]
pub const RAYDIUM_V3_PROGRAM_MAINNET: Pubkey =
    pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");

#[allow(unused)]
const LIQUIDITY_FEES_NUMERATOR: u64 = 25;
#[allow(unused)]
const LIQUIDITY_FEES_DENOMINATOR: u64 = 10000;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct JsonAmmConfig {
    id: String,
    index: u16,
    protocol_fee_rate: u64,
    trade_fee_rate: u64,
    tick_spacing: u64,
    fund_fee_rate: u64,
    fund_owner: String,
    description: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct RewardApr {
    a: f64,
    b: f64,
    c: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Stats {
    volume: f64,
    volume_fee: f64,
    fee_a: f64,
    fee_b: f64,
    fee_apr: f64,
    reward_apr: RewardApr,
    apr: f64,
    price_min: f64,
    price_max: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AmmV3Pool {
    id: String,
    mint_program_id_a: String,
    mint_program_id_b: String,
    mint_a: String,
    mint_b: String,
    vault_a: String,
    vault_b: String,
    mint_decimals_a: u8,
    mint_decimals_b: u8,
    amm_config: JsonAmmConfig,
    reward_infos: Vec<RewardInfo>,
    tvl: f64,
    day: Stats,
    week: Stats,
    month: Stats,
    lookup_table_account: String,
    open_time: i64,
    price: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct RewardInfo {
    mint: String,
    program_id: String,
}

// Raydium AmmV3 configs from "https://api.raydium.io/v2/ammV3/ammPools"
#[derive(Debug, Deserialize, Serialize, Clone)]
struct ApiResponse {
    data: Vec<AmmV3Pool>,
}

// Get Raydium liquidity pool
pub async fn get_raydium_pool(pool_id: String) -> Result<AmmV3Pool, SbError> {
    let pools = fetch_async().await.map_err(|e| SbError::CustomError {
        message: "Failed to fetch raydium v3 amm pools".to_string(),
        source: std::sync::Arc::new(e),
    })?;

    // Return the pool if it exists
    match pools.get(&pool_id).clone() {
        Some(pool) => Ok(pool.clone()),
        None => Err(SbError::CustomMessage("Raydium pool not found".to_string())),
    }
}

// Refresh every 12 hours (~smaller call)
#[once(result = true, time = 43200)]
pub async fn fetch_async() -> Result<HashMap<String, AmmV3Pool>, SbError> {
    #[cfg(not(test))]
    {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| SbError::CustomError {
                message: "Failed to build reqwest client".to_string(),
                source: Arc::new(e),
            })?;
        let res = client
            .get(RAYDIUM_V3_AMM_POOLS_API.to_string())
            .send()
            .await
            .map_err(|e| SbError::CustomError {
                message: "Failed to get raydium v3 amm pools".to_string(),
                source: Arc::new(e),
            })?;
        let json = res
            .json::<ApiResponse>()
            .await
            .map_err(|e| SbError::CustomError {
                message: "Failed to parse raydium api json".to_string(),
                source: Arc::new(e),
            })?;

        // Create a map of pool id to pool
        let mut map: HashMap<String, AmmV3Pool> = HashMap::new();
        for pool in json.data {
            map.insert(pool.id.clone(), pool);
        }

        Ok(map)
    }

    // use local JSON file for testing
    #[cfg(test)]
    {
        use std::fs;

        // let file_path = "path/to/your/local/results.json"; // Adjust the path to your "results.json"
        let file_path = "raydium_amm_results.json";
        let file_contents = fs::read_to_string(file_path)
            .map_err(|_| SbError::CustomMessage("Failed to read local JSON file".to_string()))?;
        let json: ApiResponse = serde_json::from_str(&file_contents)
            .map_err(|_| SbError::CustomMessage("Failed to parse local JSON file".to_string()))?;

        let mut map: HashMap<String, AmmV3Pool> = HashMap::new();
        for pool in json.data {
            map.insert(pool.id.clone(), pool);
        }

        Ok(map)
    }
}

pub async fn calculate_clmm_swap_price(
    rpc_client: &RpcClient,
    pool: String,
) -> Result<Decimal, SbError> {
    let raydium_pool = get_raydium_pool(pool).await?;

    //=============================================================================
    // Get Account Keys
    //=============================================================================

    // https://github.com/raydium-io/raydium-clmm/blob/master/client/src/main.rs#L120
    let amm_config_index = raydium_pool.amm_config.index;
    let (amm_config_key, _) = Pubkey::find_program_address(
        &[
            raydium_amm_v3::states::AMM_CONFIG_SEED.as_bytes(),
            &amm_config_index.to_be_bytes(),
        ],
        &RAYDIUM_V3_PROGRAM_MAINNET,
    );

    // https://github.com/raydium-io/raydium-clmm/blob/master/client/src/main.rs#L128
    let mut mint_a = Pubkey::from_str(&raydium_pool.mint_a).map_err(|e| SbError::CustomError {
        message: "Failed to parse mint A".to_string(),
        source: std::sync::Arc::new(e),
    })?;
    let mut mint_b = Pubkey::from_str(&raydium_pool.mint_b).map_err(|e| SbError::CustomError {
        message: "Failed to parse mint B".to_string(),
        source: std::sync::Arc::new(e),
    })?;
    if mint_a > mint_b {
        std::mem::swap(&mut mint_a, &mut mint_b);
    }

    // https://github.com/raydium-io/raydium-clmm/blob/master/client/src/main.rs#L135
    let (pool_id_account, _) = Pubkey::find_program_address(
        &[
            raydium_amm_v3::states::POOL_SEED.as_bytes(),
            amm_config_key.to_bytes().as_ref(),
            mint_a.to_bytes().as_ref(),
            mint_b.to_bytes().as_ref(),
        ],
        &RAYDIUM_V3_PROGRAM_MAINNET,
    );

    // https://github.com/raydium-io/raydium-clmm/blob/master/client/src/main.rs#L149
    let (tickarray_bitmap_extension, _) = Pubkey::find_program_address(
        &[
            raydium_amm_v3::states::POOL_TICK_ARRAY_BITMAP_SEED.as_bytes(),
            pool_id_account.to_bytes().as_ref(),
        ],
        &RAYDIUM_V3_PROGRAM_MAINNET,
    );

    //=============================================================================
    // Get Account Data
    //=============================================================================

    let accounts = rpc_client
        .get_multiple_accounts(&[
            amm_config_key,
            pool_id_account,
            tickarray_bitmap_extension,
            mint_a,
            mint_b,
        ])
        .await
        .map_err(|e| SbError::CustomError {
            message: "Failed to fetch accounts".to_string(),
            source: std::sync::Arc::new(e),
        })?
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(SbError::CustomMessage(
            "Failed to fetch accounts".to_string(),
        ))?;

    let [amm_config_account, pool_account, tickarray_bitmap_extension_account, mint_a_account, mint_b_account] =
        array_ref![accounts, 0, 5];

    //=============================================================================
    // Deserialize Account Data
    //=============================================================================

    let amm_config_state =
        deserialize_anchor_account::<AmmConfig>(amm_config_account).map_err(|_| {
            SbError::CustomMessage("Failed to deserialize amm config account".to_string())
        })?;
    let pool_state = deserialize_anchor_account::<PoolState>(pool_account)
        .map_err(|_| SbError::CustomMessage("Failed to deserialize pool account".to_string()))?;
    let tickarray_bitmap_extension =
        deserialize_anchor_account::<TickArrayBitmapExtension>(tickarray_bitmap_extension_account)
            .map_err(|_| {
                SbError::CustomMessage(
                    "Failed to deserialize tickarray bitmap extension account".to_string(),
                )
            })?;

    let _mint_a_transfer_fee = StateWithExtensions::<Mint>::unpack(&mint_a_account.data)
        .map_err(|_| SbError::CustomMessage("Failed to deserialize mint A".to_string()))?
        .get_extension::<TransferFeeConfig>()
        .ok()
        .cloned();
    let _mint_b_transfer_fee = StateWithExtensions::<Mint>::unpack(&mint_b_account.data)
        .map_err(|_| SbError::CustomMessage("Failed to deserialize mint B".to_string()))?
        .get_extension::<TransferFeeConfig>()
        .ok()
        .cloned();

    //=============================================================================
    // Get Tick Arrays
    //=============================================================================

    let mut tick_array_indexes = Vec::new();
    let (_, mut current_tick_array_start_index) =                                    // vvv zero for one
        pool_state.get_first_initialized_tick_array(&Some(tickarray_bitmap_extension), true).map_err(|e| {
            SbError::CustomError {
                message: "Failed to get first initialized tick array".to_string(),
                source: std::sync::Arc::new(e),
            }
        })?;
    tick_array_indexes.push(current_tick_array_start_index);

    while let Some(next_tick_array_index) = pool_state
        .next_initialized_tick_array_start_index(
            &Some(tickarray_bitmap_extension),
            current_tick_array_start_index,
            true, // zero for one (means base token for quote token)
        )
        .map_err(|e| SbError::CustomError {
            message: "Failed to get next initialized tick array".to_string(),
            source: std::sync::Arc::new(e),
        })?
    {
        tick_array_indexes.push(next_tick_array_index);
        current_tick_array_start_index = next_tick_array_index;
    }

    let tick_array_keys: Vec<Pubkey> = tick_array_indexes
        .iter()
        .map(|start_index| {
            Pubkey::find_program_address(
                &[
                    raydium_amm_v3::states::TICK_ARRAY_SEED.as_bytes(),
                    pool_id_account.to_bytes().as_ref(),
                    &start_index.to_be_bytes(),
                ],
                &RAYDIUM_V3_PROGRAM_MAINNET,
            )
            .0
        })
        .collect();

    // Get all the latest ticks in chunks of 100 accts at a time
    let mut result = VecDeque::new();
    for chunk in tick_array_keys.chunks(100) {
        let accts = rpc_client
            .get_multiple_accounts(chunk)
            .await
            .map_err(|_| SbError::CustomMessage("Failed to fetch tick array accounts".to_string()))?
            .iter()
            .map(|tick_array| {
                deserialize_anchor_account::<raydium_amm_v3::states::TickArrayState>(
                    tick_array.as_ref().ok_or_else(|| {
                        SbError::CustomMessage("Failed to fetch tick array accounts".to_string())
                    })?,
                )
                .map_err(|_| {
                    SbError::CustomMessage("Failed to deserialize tick array account".to_string())
                })
            })
            .collect::<Result<Vec<_>, SbError>>()?; // This should now work as expected
        result.extend(accts.into_iter());
    }

    //=============================================================================
    // Calculate Swap Price
    //=============================================================================

    let (amount_calculated, _) = get_out_put_amount_and_remaining_accounts(
        1 * 10u64.pow(raydium_pool.mint_decimals_a.into()),
        None,
        true, // zero for one
        true, // is base input
        &amm_config_state,
        &pool_state,
        &tickarray_bitmap_extension,
        &mut result,
    )
    .map_err(|_| SbError::CustomMessage("Failed to calculate swap price".to_string()))?;

    // Get the output amount as decimals
    let output_amount = rescale(
        Decimal::from(amount_calculated),
        raydium_pool.mint_decimals_b.into(),
    );

    // Return the output amount
    Ok(output_amount)
}