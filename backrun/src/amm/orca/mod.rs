use num_traits::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::pubkey;
use spl_math::{checked_ceil_div::CheckedCeilDiv, uint::U256};
use std::convert::TryFrom;
use std::str::FromStr;
use switchboard_common::error::SbError;
use whirlpool::state::{TickArray, Whirlpool};

pub mod legacy_pools;
use legacy_pools::{get_legacy_pool, get_legacy_pool_map, CurveType, LegacyPool};

const N_COINS: u8 = 2;
const N_COINS_SQUARED: u8 = 4;
#[allow(unused)]
const USDC_DECIMALS: u8 = 6;
#[allow(unused)]
const USDC_MINT: Pubkey = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
#[allow(unused)]
const ORCA_WHIRLPOOL_PROGRAM_ID: Pubkey = pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");

// TODO(ahermida): Sync with orca to ensure robustness

//=============================================================================
// LpExchangeRateTask
//=============================================================================

// Calculate Swap price for LP ExchangeRateTask
pub async fn calculate_swap_price(
    rpc_client: &RpcClient,
    pool: String,
) -> Result<Decimal, SbError> {
    // check if legacy pool
    // https://github.com/orca-so/typescript-sdk/blob/a551fa7a3365fb23ac41a0b9b9a1303798c1fca5/src/model/orca/pool/orca-pool.ts#L92
    let legacy_pool = get_legacy_pool(&pool);
    if legacy_pool.is_some() {
        let legacy_pool = legacy_pool.unwrap();
        match legacy_pool.curve_type {
            CurveType::ConstantProduct => {
                get_legacy_quote_constant_product(rpc_client, &legacy_pool, 1.0).await
            }
            CurveType::Stable => get_legacy_quote_stable(rpc_client, &legacy_pool, 1.0).await,
        }
    } else {
        calculate_whirlpool_swap_price(rpc_client, pool, 1.0).await
    }
}

//=============================================================================
// Get whirlpool quote
//=============================================================================

pub async fn calculate_whirlpool_swap_price(
    rpc_client: &RpcClient,
    pool: String,
    input_amount: f64,
) -> Result<Decimal, SbError> {
    let _input_amount = Decimal::from_f64(input_amount).ok_or(SbError::Message(
        "Failed to convert input amount to decimal",
    ))?;

    // get pool as pubkey
    let pool = Pubkey::from_str(&pool).map_err(|e| SbError::CustomError {
        message: "Failed to parse pool pubkey".to_string(),
        source: std::sync::Arc::new(e),
    })?;

    // Fetch whirepool accounts
    let whirlpool = fetch_whirlpool_account(&rpc_client, pool)
        .await
        .expect("Whirlpool account not valid");

    // Get 10^decimals for in
    let decimals_in = fetch_mint_decimals(&rpc_client, whirlpool.token_mint_a.clone())
        .await
        .expect("Invalid input token");

    // Get 10^decimals for out
    let decimals_out = fetch_mint_decimals(&rpc_client, whirlpool.token_mint_b.clone())
        .await
        .expect("Invalid input token");

    let quote = sqrt_price_x64_to_price(
        whirlpool.sqrt_price,
        decimals_in.try_into().unwrap(),
        decimals_out.try_into().unwrap(),
    );

    Ok(quote)
}

// div floor helper
#[allow(unused)]
fn div_floor(a: i32, b: i32) -> i32 {
    if a < 0 {
        a / b - 1
    } else {
        a / b
    }
}

// Used for calculating price
// ex: sqrt_price_x64_to_price(whirlpool.sqrt_price, SOL_DECIMALS, USDC_DECIMALS)
fn sqrt_price_x64_to_price(sqrt_price_x64: u128, decimals_a: i8, decimals_b: i8) -> Decimal {
    let sqrt_price_x64_decimal = Decimal::from(sqrt_price_x64);
    let sqrt_price = sqrt_price_x64_decimal
        .checked_div(Decimal::TWO.powu(64))
        .unwrap()
        .powu(2)
        .checked_mul(Decimal::TEN.powi((decimals_a - decimals_b) as i64))
        .unwrap();
    sqrt_price
}

//=============================================================================
// Get legacy pool quote
//=============================================================================

pub async fn get_legacy_quote_stable(
    rpc_client: &RpcClient,
    legacy_pool: &LegacyPool,
    input_amount: f64,
) -> Result<Decimal, SbError> {
    // Decimal input amount
    let input_amount = Decimal::from_f64(input_amount).ok_or(SbError::Message(
        "Failed to convert input amount to decimal",
    ))?;

    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L31
    if input_amount.is_zero() {
        return Err(SbError::Message("Input amount is zero"));
    }

    // Calculate fees
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L26C10-L26C32
    let lp_fees = {
        let trading_fee = calculate_legacy_fee(
            input_amount,
            Decimal::from(legacy_pool.fee_numerator),
            Decimal::from(legacy_pool.fee_denominator),
        );
        let owner_fee = calculate_legacy_fee(
            input_amount,
            Decimal::from(legacy_pool.owner_trade_fee_numerator),
            Decimal::from(legacy_pool.owner_trade_fee_denominator),
        );

        trading_fee
            .checked_add(owner_fee)
            .ok_or(SbError::Message("Overflow from fees"))?
    };

    // Input amount minus fees
    let input_amount_minus_fees = if lp_fees.gt(&input_amount) {
        Decimal::ZERO
    } else {
        input_amount
            .checked_sub(lp_fees)
            .ok_or(SbError::Message("Underflow from fees"))?
    };

    // Get legacy pool token balances
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L111
    let token_a_balance = rpc_client
        .get_token_account_balance(&legacy_pool.token_account_a)
        .await
        .map_err(|e| SbError::CustomError {
            message: "Failed to get token a balance".to_string(),
            source: std::sync::Arc::new(e),
        })?
        .amount
        .parse::<u64>()
        .map_err(|e| SbError::CustomError {
            message: "Failed to parse token a balance".to_string(),
            source: std::sync::Arc::new(e),
        })?;
    let token_b_balance = rpc_client
        .get_token_account_balance(&legacy_pool.token_account_b)
        .await
        .map_err(|e| SbError::CustomError {
            message: "Failed to get token b balance".to_string(),
            source: std::sync::Arc::new(e),
        })?
        .amount
        .parse::<u64>()
        .map_err(|e| SbError::CustomError {
            message: "Failed to parse token b balance".to_string(),
            source: std::sync::Arc::new(e),
        })?;

    // Get 10^decimals for in
    let decimals_in_pow = 10u64
        .checked_pow(legacy_pool.token_a_decimals.into())
        .ok_or(SbError::Message("Overflow from input token decimals"))?;
    let decimals_in_pow = Decimal::from(decimals_in_pow);

    // Get 10^decimals for out
    let decimals_out_pow = 10u64
        .checked_pow(legacy_pool.token_b_decimals.into())
        .ok_or(SbError::Message("Overflow from output decimals"))?;
    let decimals_out_pow = Decimal::from(decimals_out_pow);

    // Get input amount as u64 scaled to decimals
    let scaled_input_amount = input_amount_minus_fees
        .checked_mul(decimals_in_pow)
        .ok_or(SbError::Message("Overflow from scaling input amount"))?
        .to_u64()
        .ok_or(SbError::Message("Overflow from scaling input amount"))?;

    // Calculate stablecurve result
    let leverage = legacy_pool
        .amp
        .checked_mul(N_COINS.into())
        .ok_or(SbError::Message("Overflow from leverage"))?;

    // Get the new pool amount
    let new_input_pool_amount = scaled_input_amount
        .checked_add(token_a_balance)
        .ok_or(SbError::Message("Overflow from in token balance"))?;

    // Get the u64 invariant for the pool
    let d = compute_d(leverage, token_a_balance.into(), token_b_balance.into())
        .ok_or(SbError::Message("Overflow from invariant"))?;

    let new_output_pool_amount: u64 =
        compute_new_destination_amount(leverage, new_input_pool_amount.into(), d)
            .ok_or(SbError::Message("Overflow from new destination amount"))?
            .try_into()
            .map_err(|e| SbError::CustomError {
                message: "Failed to convert new destination amount to u64".to_string(),
                source: std::sync::Arc::new(e),
            })?;

    // Get the u64 output amount
    let amount_out: u64 = token_b_balance
        .checked_sub(new_output_pool_amount)
        .ok_or(SbError::Message("Underflow from out token balance"))?;

    // Convert back to Decimals
    let amount_out = Decimal::from(amount_out)
        .checked_div(decimals_out_pow)
        .ok_or(SbError::Message("Underflow from out token balance"))?;

    // Get the quote in decimals
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/stable-quote.ts#L69C32-L69C48
    let quote = amount_out
        .checked_div(input_amount)
        .ok_or(SbError::Message("Underflow from quote"))?;

    // Return quote
    Ok(quote)
}

pub async fn get_legacy_quote_constant_product(
    rpc_client: &RpcClient,
    legacy_pool: &LegacyPool,
    input_amount: f64,
) -> Result<Decimal, SbError> {
    // Decimal input amount
    let input_amount = Decimal::from_f64(input_amount).ok_or(SbError::Message(
        "Failed to convert input amount to decimal",
    ))?;

    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L31
    if input_amount.is_zero() {
        return Err(SbError::Message("Input amount is zero"));
    }

    // Calculate fees
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L26C10-L26C32
    let lp_fees = {
        let trading_fee = calculate_legacy_fee(
            input_amount,
            Decimal::from(legacy_pool.fee_numerator),
            Decimal::from(legacy_pool.fee_denominator),
        );
        let owner_fee = calculate_legacy_fee(
            input_amount,
            Decimal::from(legacy_pool.owner_trade_fee_numerator),
            Decimal::from(legacy_pool.owner_trade_fee_denominator),
        );
        trading_fee
            .checked_add(owner_fee)
            .ok_or(SbError::Message("Trading fee overflow"))?
    };

    // Input amount minus fees
    let input_amount_minus_fees = if lp_fees.gt(&input_amount) {
        Decimal::ZERO
    } else {
        input_amount
            .checked_sub(lp_fees)
            .ok_or(SbError::Message("Underflow from fees"))?
    };

    // Get legacy pool token balances
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L111
    let token_a_balance = rpc_client
        .get_token_account_balance(&legacy_pool.token_account_a)
        .await
        .map_err(|e| SbError::CustomError {
            message: "Failed to get token a balance".to_string(),
            source: std::sync::Arc::new(e),
        })?
        .amount
        .parse::<u64>()
        .map_err(|e| SbError::CustomError {
            message: "Failed to parse token a balance".to_string(),
            source: std::sync::Arc::new(e),
        })?;

    let token_b_balance = rpc_client
        .get_token_account_balance(&legacy_pool.token_account_b)
        .await
        .map_err(|e| SbError::CustomError {
            message: "Failed to get token b balance".to_string(),
            source: std::sync::Arc::new(e),
        })?
        .amount
        .parse::<u64>()
        .map_err(|e| SbError::CustomError {
            message: "Failed to parse token b balance".to_string(),
            source: std::sync::Arc::new(e),
        })?;

    // Get decimals for in & out token
    let decimals_in_pow = 10u64
        .checked_pow(legacy_pool.token_a_decimals.into())
        .ok_or(SbError::Message("Overflow from input token decimals"))?;
    let decimals_in_pow = Decimal::from(decimals_in_pow);

    // Get 10^decimals for out
    let decimals_out_pow = 10u64
        .checked_pow(legacy_pool.token_b_decimals.into())
        .ok_or(SbError::Message("Overflow from output decimals"))?;
    let decimals_out_pow = Decimal::from(decimals_out_pow);

    // Get input amount as u64 scaled to decimals
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L39
    let scaled_input_amount = input_amount_minus_fees
        .checked_mul(decimals_in_pow)
        .ok_or(SbError::Message("Overflow from scaling input amount"))?
        .to_u64()
        .ok_or(SbError::Message("Failed to convert input amount to u64"))?;

    // Calculate constant product result
    // https://github.com/orca-so/typescript-sdk/blob/main/src/model/quote/constant-product-quote.ts#L114
    // Get the u64 invariant for the pool
    let invariant: u128 = (token_a_balance as u128)
        .checked_mul(token_b_balance as u128)
        .ok_or(SbError::Message("Overflow from invariant"))?;

    let divisor: u128 = token_a_balance
        .checked_add(scaled_input_amount)
        .ok_or(SbError::Message("Overflow from scaled_input_amount"))?
        .into();

    // Get the new pool amount
    let new_pool_output_amount: u128 = invariant
        .checked_ceil_div(divisor.into())
        .ok_or(SbError::Message("Overflow from new pool output amount"))?
        .0; // Get the output amount

    // Convert back to u64
    let new_pool_output_amount: u64 =
        new_pool_output_amount
            .try_into()
            .map_err(|e| SbError::CustomError {
                message: "Failed to convert new pool output amount to u64".to_string(),
                source: std::sync::Arc::new(e),
            })?;

    // Get the output amount
    let amount_out: u64 = token_b_balance
        .checked_sub(new_pool_output_amount)
        .ok_or(SbError::Message("Underflow from out token balance"))?;

    // Convert back to Decimals
    let amount_out = Decimal::from(amount_out)
        .checked_div(decimals_out_pow)
        .ok_or(SbError::Message("Underflow from out token balance"))?;

    // Get the quote in decimals
    let quote = amount_out
        .checked_div(input_amount)
        .ok_or(SbError::Message("Underflow from quote"))?;

    // Rescale quote to output decimals
    // let quote = rescale(quote, legacy_pool.token_b_decimals.into());

    Ok(quote)
}

//=============================================================================
// Fetch functions
//=============================================================================

pub async fn fetch_whirlpool_account(
    rpc_client: &RpcClient,
    pda: Pubkey,
) -> Result<Whirlpool, SbError> {
    let account = rpc_client
        .get_account(&pda)
        .await
        .map_err(|e| SbError::CustomError {
            message: format!("Failed to get whirlpool account {}", pda),
            source: std::sync::Arc::new(e),
        })?;
    let whirlpool = Whirlpool::try_to_deserialize(&mut account.data.as_slice()).map_err(|e| {
        SbError::CustomError {
            message: "Failed to deserialize whirlpool account".to_string(),
            source: std::sync::Arc::new(e),
        }
    })?;

    Ok(whirlpool)
}

#[allow(unused)]
pub async fn fetch_tick_arrays(
    rpc_client: &RpcClient,
    keys: Vec<Pubkey>,
) -> Result<Vec<TickArray>, SbError> {
    let accounts =
        rpc_client
            .get_multiple_accounts(&keys)
            .await
            .map_err(|e| SbError::CustomError {
                message: "Failed to get tick array accounts".to_string(),
                source: std::sync::Arc::new(e),
            })?;
    let mut tick_arrays: Vec<TickArray> = vec![];
    for account in accounts.iter() {
        let tick_data = account
            .clone()
            .ok_or(SbError::Message("Failed to get tick array account"))?;
        let mut tick_data = tick_data.data.as_slice();
        let tick_array =
            TickArray::try_to_deserialize(&mut tick_data).map_err(|e| SbError::CustomError {
                message: "Failed to deserialize tick array account".to_string(),
                source: std::sync::Arc::new(e),
            })?;
        tick_arrays.push(tick_array);
    }

    Ok(tick_arrays)
}

pub async fn fetch_mint_decimals(rpc_client: &RpcClient, mint: Pubkey) -> Result<u8, SbError> {
    let data = rpc_client
        .get_account_data(&mint)
        .await
        .map_err(|e| SbError::CustomError {
            message: format!("Failed to get mint account {}", mint),
            source: std::sync::Arc::new(e),
        })?;
    Ok(data[44])
}

//=============================================================================
// Helper functions
//=============================================================================

#[allow(unused)]
pub fn find_pool(token_a_mint: Pubkey, token_b_mint: Pubkey) -> Option<LegacyPool> {
    let legacy_pool_map = get_legacy_pool_map();
    for pool in legacy_pool_map.values() {
        if pool.token_a_mint == token_a_mint && pool.token_b_mint == token_b_mint {
            return Some(pool.clone());
        }
    }
    None
}

pub fn calculate_legacy_fee(
    input_amount: Decimal,
    fee_numerator: Decimal,
    fee_denominator: Decimal,
) -> Decimal {
    if fee_numerator.is_zero() || input_amount.is_zero() {
        return Decimal::ZERO;
    }
    let fee = input_amount * fee_numerator / fee_denominator;
    if fee.is_zero() {
        Decimal::ONE
    } else {
        fee
    }
}

/// Compute stable swap invariant (D)
//  from the 2 following sources:
// https://github.com/bonedaddy/solana-arbitrage-bot/blob/ccf1e6e4579fae504d5966e30fdae96a6dbe85df/client/src/pool_utils/stable.rs#L120
// https://github.com/orca-so/stablecurve/blob/f6219fbdb4246bdcb1f3e0e4afac8ec65120af5a/index.ts#L35
pub fn compute_d(leverage: u64, amount_a: u128, amount_b: u128) -> Option<u128> {
    let amount_a_times_coins =
        checked_u8_mul(&U256::from(amount_a), N_COINS)?.checked_add(U256::one())?;

    let amount_b_times_coins =
        checked_u8_mul(&U256::from(amount_b), N_COINS)?.checked_add(U256::one())?;

    let sum_x = amount_a.checked_add(amount_b)?; // sum(x_i), a.k.a S
    if sum_x == 0 {
        Some(0)
    } else {
        let mut d_previous: U256;
        let mut d: U256 = sum_x.into();

        // Newton's method to approximate D
        for _ in 0..32 {
            let mut d_product = d;
            d_product = d_product
                .checked_mul(d)?
                .checked_div(amount_a_times_coins)?;
            d_product = d_product
                .checked_mul(d)?
                .checked_div(amount_b_times_coins)?;
            d_previous = d;
            //d = (leverage * sum_x + d_p * n_coins) * d / ((leverage - 1) * d + (n_coins + 1) * d_p);
            d = calculate_step(&d, leverage, sum_x, &d_product)?;
            // Equality with the precision of 1
            if d == d_previous {
                break;
            }
        }

        u128::try_from(d).ok()
    }
}

/// d = (leverage * sum_x + d_product * n_coins) * initial_d / ((leverage - 1) * initial_d + (n_coins + 1) * d_product)
// https://github.com/bonedaddy/solana-arbitrage-bot/blob/ccf1e6e4579fae504d5966e30fdae96a6dbe85df/client/src/pool_utils/stable.rs#L103
// https://github.com/orca-so/stablecurve/blob/f6219fbdb4246bdcb1f3e0e4afac8ec65120af5a/index.ts#L16
fn calculate_step(initial_d: &U256, leverage: u64, sum_x: u128, d_product: &U256) -> Option<U256> {
    let leverage_mul = U256::from(leverage).checked_mul(sum_x.into())?;
    let d_p_mul = checked_u8_mul(d_product, N_COINS)?;

    let l_val = leverage_mul.checked_add(d_p_mul)?.checked_mul(*initial_d)?;

    let leverage_sub = initial_d.checked_mul((leverage.checked_sub(1)?).into())?;
    let n_coins_sum = checked_u8_mul(d_product, N_COINS.checked_add(1)?)?;

    let r_val = leverage_sub.checked_add(n_coins_sum)?;

    l_val.checked_div(r_val)
}

// Compute swap amount `y` in proportion to `x`
// Solve for y:
// y**2 + y * (sum' - (A*n**n - 1) * D / (A * n**n)) = D ** (n + 1) / (n ** (2 * n) * prod' * A)
// y**2 + b*y = c
// https://github.com/solana-labs/solana-program-library/blob/f568413503d1b5bc6ca59b13f86e094d4d2516d6/token-swap/program/src/curve/stable.rs#L76
pub fn compute_new_destination_amount(
    leverage: u64,
    new_source_amount: u128,
    d_val: u128,
) -> Option<u128> {
    // Upscale to U256
    let leverage: U256 = leverage.into();
    let new_source_amount: U256 = new_source_amount.into();
    let d_val: U256 = d_val.into();

    // sum' = prod' = x
    // c =  D ** (n + 1) / (n ** (2 * n) * prod' * A)
    let c = checked_u8_power(&d_val, N_COINS.checked_add(1)?)?
        .checked_div(checked_u8_mul(&new_source_amount, N_COINS_SQUARED)?.checked_mul(leverage)?)?;

    // b = sum' - (A*n**n - 1) * D / (A * n**n)
    let b = new_source_amount.checked_add(d_val.checked_div(leverage)?)?;

    // Solve for y by approximating: y**2 + b*y = c
    let mut y = d_val;
    for _ in 0..32 {
        let (y_new, _) = (checked_u8_power(&y, 2)?.checked_add(c)?)
            .checked_ceil_div(checked_u8_mul(&y, 2)?.checked_add(b)?.checked_sub(d_val)?)?;
        if y_new == y {
            break;
        } else {
            y = y_new;
        }
    }
    u128::try_from(y).ok()
}

// https://github.com/switchboard-xyz/sbv3/blob/345e68b3d9acf30e341e39060882935f8ec995ae/javascript/task-runner/src/utils/amm.ts#L119
#[allow(unused)]
fn calculate_virtual_price(
    amp_factor: u64,
    lp_token_supply: u64,
    lp_token_decimals: u8,
    token_a_balance: u64,
    token_a_decimals: u8,
    token_b_balance: u64,
    token_b_decimals: u8,
) -> Result<Decimal, SbError> {
    let scale: u8 = std::cmp::max(
        std::cmp::max(lp_token_decimals, token_a_decimals),
        token_b_decimals,
    );

    // Normalize balances
    let token_a_balance_normalized = token_a_balance
        .checked_mul(10u64.pow((scale - token_a_decimals) as u32))
        .ok_or(SbError::Message(
            "Overflow from token a balance normalization",
        ))?;
    let token_b_balance_normalized = token_b_balance
        .checked_mul(10u64.pow((scale - token_b_decimals) as u32))
        .ok_or(SbError::Message(
            "Overflow from token b balance normalization",
        ))?;

    // Compute D
    let d = compute_d(
        amp_factor,
        token_a_balance_normalized.into(),
        token_b_balance_normalized.into(),
    )
    .ok_or(SbError::Message("Failed to compute D"))?; // Implement compute_d similarly

    // Get the virtual price decimal

    let numerator = Decimal::from(d)
        .checked_div(Decimal::from(lp_token_supply))
        .ok_or(SbError::Message("Unsafe division from virtual price"))?;

    let denominator = Decimal::from(10u64.pow((scale - lp_token_decimals + 1) as u32));

    let virtual_price = numerator
        .checked_div(denominator)
        .ok_or(SbError::Message("Unsafe division for virtual price"))?;

    // Return virtual price
    Ok(virtual_price)
}

fn checked_u8_mul(a: &U256, b: u8) -> Option<U256> {
    a.checked_mul(U256::from(b))
}

fn checked_u8_power(a: &U256, b: u8) -> Option<U256> {
    a.checked_pow(b.into())
}