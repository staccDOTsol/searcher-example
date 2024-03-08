use rust_decimal::Decimal;
use solana_client::nonblocking::rpc_client::RpcClient;

use switchboard_common::error::SbError;

pub mod pools;
use pools::{calculate_pool_swap_price, get_raydium_pool};

//pub mod clmm;
//use clmm::calculate_clmm_swap_price;

//=============================================================================
// LpExchangeRateTask
//=============================================================================

// Calculate Swap price for LP ExchangeRateTask
pub async fn calculate_swap_price(
    rpc_client: &RpcClient,
    pool: String,
    base_reserve: u64,
    quote_reserve: u64,
) -> Result<Decimal, SbError> {
    let raydium_pool = get_raydium_pool(&pool).await;
    if raydium_pool.is_some() {
        // Check normal radium pool
        let _raydium_pool = raydium_pool.unwrap();
        calculate_pool_swap_price(rpc_client, pool, base_reserve, quote_reserve).await
        // Check CLMM pool
    //    calculate_clmm_swap_price(rpc_client, pool).await
    }
    else {
        Err(SbError::FunctionResultAccountsMismatch)
    }
}