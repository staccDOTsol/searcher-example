use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use switchboard_common::error::SbError;

// Need: orca/raydium
use async_recursion::async_recursion;

#[allow(unused)]
fn constant_product_calculate_fair_lp_price(
    total_supply: Decimal,
    reserves: Vec<Decimal>,
    prices: Vec<Decimal>,
) -> Result<Decimal, SbError> {
    let num_reserves = reserves.len();
    let num_prices = prices.len();

    if num_reserves != num_prices || num_reserves == 0 {
        return Err(SbError::CustomMessage(format!(
            "Mismatch in number of reserves ({}) and prices ({})",
            num_reserves, num_prices
        )));
    }

    let mut numerator = Decimal::ZERO;

    for i in 0..num_reserves {
        let reserve = &reserves[i];
        let price = &prices[i];
        let product = reserve * price;
        numerator += product;
    }

    let k_root = kth_root(&reserves, num_reserves);
    let p_root = kth_root(&prices, num_prices);

    // TODO: checked_math
    let result = Decimal::from(num_reserves) * k_root * p_root / total_supply;

    println!(
        "{} = {} * ({})^0.5 * ({})^0.5 / {} = {} / {}",
        result,
        num_reserves,
        reserves
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<String>>()
            .join("*"),
        prices
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<String>>()
            .join("*"),
        total_supply,
        numerator,
        total_supply
    );

    Ok(result)
}

#[allow(unused)]
fn kth_root(numbers: &[Decimal], k: usize) -> Decimal {
    numbers
        .iter()
        .fold(Decimal::ZERO, |acc, &num| acc + num)
        .powf(1.0 / k as f64)
}

pub struct TokenAmount {
    amount: Decimal,
    decimals: i32, // Adjust the type accordingly
}

pub struct StableCurveUtils;

#[allow(unused)]
impl StableCurveUtils {
    fn calculate_step(
        &self,
        initial_d: Decimal,
        leverage: f64,
        sum_x: Decimal,
        d_product: Decimal,
        n_coins: usize,
    ) -> Result<Decimal, SbError> {
        let leverage_mul = Decimal::from_f64(leverage).unwrap() * sum_x;
        let dp_mul = d_product * Decimal::from(n_coins);

        let leverage_val = (leverage_mul + dp_mul) * initial_d;

        let leverage_sub = initial_d * Decimal::from_f64(leverage - 1.0).unwrap();
        let n_coins_sum = d_product * Decimal::from(n_coins + 1);

        let r_val = leverage_sub + n_coins_sum;

        Ok(leverage_val / r_val)
    }

    fn calculate_d(&self, amp: f64, amounts: Vec<Decimal>) -> Result<Decimal, SbError> {
        let amp = Decimal::from_f64(amp).unwrap();
        let n_coins = Decimal::from(amounts.len());
        let leverage: Decimal = amp * n_coins;

        let sum_x: Decimal = amounts.iter().cloned().sum();
        if sum_x == Decimal::new(0, 0) {
            return Ok(Decimal::new(0, 0));
        }

        let mut d_previous: Decimal;
        let mut d = sum_x;

        for _ in 0..32 {
            let d_product = d;
            let mut product = d_product;

            for amount in &amounts {
                product *= d / amount;
            }

            d_previous = d;
            d = (leverage * sum_x + d_product * n_coins) * d
                / ((leverage - Decimal::ONE) * d + (n_coins + Decimal::ONE) * d_product);

            if d == d_previous {
                break;
            }
        }

        Ok(d)
    }
    fn calculate_virtual_price(
        &self,
        amp_factor: f64,
        lp_pool: &TokenAmount,
        reserves: &[TokenAmount],
    ) -> Result<Decimal, SbError> {
        // Prevent division by zero
        if lp_pool.amount.is_zero() {
            return Ok(Decimal::ZERO);
        }
        let scale = lp_pool
            .decimals
            .max(reserves.iter().map(|r| r.decimals).max().unwrap_or(0));
        let normalized_reserves: Vec<Decimal> = reserves
            .iter()
            .map(|r| r.amount * Decimal::from(10_i32.pow((scale - r.decimals).try_into().unwrap())))
            .collect();
        let d = self.calculate_d(amp_factor, normalized_reserves);

        match d {
            Ok(d) => {
                if d.is_sign_positive() {
                    return Ok(d
                        / (lp_pool.amount
                            * Decimal::from(
                                10_i32.pow((scale - lp_pool.decimals + 1).try_into().unwrap()),
                            )));
                } else {
                    return Ok(Decimal::ZERO);
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    fn calculate_fair_lp_price(
        &self,
        amp_factor: f64,
        total_supply: &TokenAmount,
        reserves: &[TokenAmount],
        prices: &[Decimal],
    ) -> Result<Decimal, SbError> {
        if reserves.len() != prices.len() || reserves.is_empty() {
            return Err(SbError::CustomMessage(format!(
                "Mismatch in number of reserves ({}) and prices ({})",
                reserves.len(),
                prices.len()
            )));
        }
        println!(
            "[{}] {}%",
            reserves
                .iter()
                .map(|r| r.amount.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            (reserves[0].amount * prices[0])
                / (reserves[0].amount * prices[0] + reserves[1].amount * prices[1])
                * Decimal::from(100)
        );

        let virtual_price = self.calculate_virtual_price(amp_factor, total_supply, reserves)?;

        let mut sorted_prices = prices.to_vec();
        sorted_prices.sort();

        if sorted_prices.is_empty() {
            panic!("EmptyPriceError");
        }

        let min_price = sorted_prices[0];

        let fair_lp_price = min_price * virtual_price;

        println!(
            "{} = {} * {}",
            fair_lp_price.to_string(),
            min_price.to_string(),
            virtual_price.to_string(),
        );

        Ok(fair_lp_price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn approx_eq(a: Decimal, b: Decimal, epsilon: Decimal) -> bool {
        (a - b).abs() <= epsilon
    }

    #[test]
    fn test_constant_product_calculate_fair_lp_price() {
        let total_supply = dec!(1000);
        let reserves = vec![dec!(500), dec!(500)];
        let prices = vec![dec!(2), dec!(2)];

        let result =
            constant_product_calculate_fair_lp_price(total_supply, reserves, prices).unwrap();

        // Use a tolerance for floating-point comparison
        let tolerance = dec!(0.0000000001);
        let expected = dec!(0.126491105816061890970051694);
        assert!(
            (result - expected).abs() < tolerance,
            "constant_product_calculate_fair_lp_price: Mismatch in results"
        );

        // Test with mismatched reserves and prices
        let mismatched_reserves = vec![dec!(500)];
        let mismatched_prices = vec![dec!(2), dec!(2)];
        assert!(constant_product_calculate_fair_lp_price(
            total_supply,
            mismatched_reserves,
            mismatched_prices
        )
        .is_err());
    }

    #[test]
    fn test_kth_root() {
        let numbers = vec![dec!(4), dec!(9)];
        let result = kth_root(&numbers, 2);
        let expected_value = dec!(3.605551275463989);
        assert!(
            approx_eq(result, expected_value, Decimal::new(1, 6)),
            "kth_root: Precision error"
        );
    }

    #[test]
    fn test_calculate_d() {
        let amp = 1.0;
        let amounts = vec![dec!(100), dec!(100)];
        let result = StableCurveUtils.calculate_d(amp, amounts.clone());
        match result {
            Ok(d) => {
                assert!(
                    d.is_sign_positive(),
                    "calculate_d: Division by zero or negative result"
                );
                assert!(d > Decimal::ZERO); // Expected non-zero result
            }
            Err(e) => {
                panic!("calculate_d: {}", e);
            }
        }
        // Test with zero amp factor
        let zero_amp = 0.0;
        let result = StableCurveUtils.calculate_d(zero_amp, amounts);
        match result {
            Ok(d) => {
                assert!(
                    d.is_sign_positive(),
                    "calculate_d: Division by zero or negative result"
                );
                assert_eq!(d, Decimal::from(200)); // Expected zero result
            }
            Err(e) => {
                panic!("calculate_d: {}", e);
            }
        }
    }

    #[test]
    fn test_calculate_virtual_price() {
        let amp_factor = 1.0;
        let lp_pool = TokenAmount {
            amount: dec!(100),
            decimals: 6,
        };
        let reserves = vec![
            TokenAmount {
                amount: dec!(500),
                decimals: 6,
            },
            TokenAmount {
                amount: dec!(500),
                decimals: 6,
            },
        ];
        let result = StableCurveUtils.calculate_virtual_price(amp_factor, &lp_pool, &reserves);

        // Properly handle the Result
        match result {
            Ok(virtual_price) => {
                assert!(
                    virtual_price.is_sign_positive(),
                    "calculate_virtual_price: Division by zero or negative result"
                );
                assert!(virtual_price > Decimal::ZERO); // Expected non-zero result
            }
            Err(e) => {
                panic!("calculate_virtual_price: {}", e);
            }
        }

        // Test with zero LP pool amount
        let zero_lp_pool = TokenAmount {
            amount: Decimal::ZERO,
            decimals: 6,
        };
        let result = StableCurveUtils.calculate_virtual_price(amp_factor, &zero_lp_pool, &reserves);

        // Properly handle the Result
        match result {
            Ok(virtual_price) => {
                assert!(
                    virtual_price.is_sign_positive(),
                    "calculate_virtual_price: Division by zero or negative result"
                );
                assert_eq!(virtual_price, Decimal::ZERO); // Expected zero result
            }
            Err(e) => {
                panic!("calculate_virtual_price: {}", e);
            }
        }
    }

    #[test]
    fn test_calculate_fair_lp_price() {
        let amp_factor = 1.0;
        let total_supply = TokenAmount {
            amount: dec!(1000),
            decimals: 6,
        };
        let reserves = vec![
            TokenAmount {
                amount: dec!(500),
                decimals: 6,
            },
            TokenAmount {
                amount: dec!(500),
                decimals: 6,
            },
        ];
        let prices = vec![dec!(1), dec!(1)];
        let result = StableCurveUtils
            .calculate_fair_lp_price(amp_factor, &total_supply, &reserves, &prices)
            .unwrap();
        assert!(result > Decimal::ZERO); // Expected non-zero result

        // Test with mismatched reserves and prices
        let expected_value = dec!(0.1); // Adjust this based on correct logic
        assert_eq!(
            result, expected_value,
            "constant_product_calculate_fair_lp_price: Mismatch in results"
        );
    }
}