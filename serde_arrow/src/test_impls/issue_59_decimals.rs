use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::_impl::arrow;

use super::utils::Test;

#[test]
fn example_str_repr() {
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
        .also(|it| {
            let arrays = it.arrays.arrow.as_ref().unwrap();
            let arr = arrays[0]
                .as_any()
                .downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::Decimal128Type>>()
                .unwrap();

            assert_eq!(arr.value(0), 20);
            assert_eq!(arr.value(1), 42);
        })
        .deserialize(&items);
}

#[test]
fn example_float_repr() {
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
        .also(|it| {
            let arrays = it.arrays.arrow.as_ref().unwrap();
            let arr = arrays[0]
                .as_any()
                .downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::Decimal128Type>>()
                .unwrap();

            assert_eq!(arr.value(0), 20);
            assert_eq!(arr.value(1), 42);
        })
        .deserialize(&items);
}
