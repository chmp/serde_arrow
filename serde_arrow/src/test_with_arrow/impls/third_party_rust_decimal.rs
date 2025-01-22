use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::_impl::arrow;

use super::utils::Test;

fn get_i128_values(test: &Test) -> &[i128] {
    let arrays = test.arrays.arrow.as_ref().unwrap();
    let arr = arrays[0]
        .as_any()
        .downcast_ref::<arrow::_raw::array::PrimitiveArray<arrow::_raw::array::types::Decimal128Type>>()
        .unwrap();
    arr.values()
}

#[test]
fn rust_decimal_str_repr() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "rust_decimal::serde::str")]
        value: Decimal,
    }

    let items = [
        Wrapper {
            value: Decimal::from_str("0.20").unwrap(),
        },
        Wrapper {
            value: Decimal::from_str("0.42").unwrap(),
        },
    ];

    Test::new()
        .with_schema(json!([
            {"name": "value", "data_type": "Decimal128(5, 2)"},
        ]))
        .serialize(&items)
        .also(|it| assert_eq!(get_i128_values(it), &[20, 42]))
        .deserialize(&items);
}

#[test]
fn rust_decimal_float_repr() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "rust_decimal::serde::float")]
        value: Decimal,
    }

    let items = [
        Wrapper {
            value: Decimal::from_str("0.20").unwrap(),
        },
        Wrapper {
            value: Decimal::from_str("0.42").unwrap(),
        },
    ];

    Test::new()
        .with_schema(json!([
            {"name": "value", "data_type": "Decimal128(5, 2)"},
        ]))
        .serialize(&items)
        .also(|it| assert_eq!(get_i128_values(it), &[20, 42]))
        .deserialize(&items);
}
