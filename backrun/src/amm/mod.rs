//pub mod orca;

pub mod utils;

pub mod raydium;

use rust_decimal::Decimal;

pub fn rescale(amount: Decimal, decimals: u8) -> Decimal {
    let scale = Decimal::new(1, decimals.into());
    amount * scale
}