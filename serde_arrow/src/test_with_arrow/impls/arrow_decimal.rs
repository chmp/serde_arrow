use serde_json::json;

use crate::{_impl::arrow, utils::Item};

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
fn example() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(5, 2)"}]))
        .serialize(&[Item(String::from("0.20")), Item(String::from("0.42"))])
        .deserialize(&[Item(String::from("0.20")), Item(String::from("0.42"))])
        .also(|it| assert_eq!(get_i128_values(it), &[20, 42]));
}

#[test]
fn string_to_decimal() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(5, 2)"}]))
        .serialize(&[Item(String::from("1.23")), Item(String::from("4.56"))])
        .also(|it| assert_eq!(get_i128_values(it), &[123, 456]));
}

#[test]
fn string_to_decimal_with_underscores() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(8, 2)"}]))
        .serialize(&[
            Item(String::from("1_234.56")),
            Item(String::from("-7_890.12")),
        ])
        .also(|it| assert_eq!(get_i128_values(it), &[123456, -789012]));
}

#[test]
fn decimal_to_string() {
    let items = &[Item(String::from("1.23")), Item(String::from("4.56"))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(5, 2)"}]))
        .serialize(items)
        .deserialize(items);
}

/// Decimals with too many digits are truncated in serialization
#[test]
fn truncation() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(5, 2)"}]))
        .serialize(&[Item(String::from("0.2012")), Item(String::from("0.4234"))])
        .deserialize(&[Item(String::from("0.20")), Item(String::from("0.42"))])
        .also(|it| assert_eq!(get_i128_values(it), &[20, 42]));
}

#[test]
fn negative_scale() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Decimal128(5, -2)"}]))
        .serialize(&[Item(String::from("1300.00")), Item(String::from("4200.00"))])
        .deserialize(&[Item(String::from("1300")), Item(String::from("4200"))])
        .also(|it| assert_eq!(get_i128_values(it), &[13, 42]));
}

#[test]
fn too_small_precision() {
    let items = &[Item(String::from("1.23")), Item(String::from("4.56"))];

    let mut test =
        Test::new().with_schema(json!([{"name": "item", "data_type": "Decimal128(2, 2)"}]));

    let err = test.try_serialize_arrow(items).expect_err("Expected error");
    assert!(err.to_string().contains("configured precision"));
}

mod decimal256 {
    use crate::_impl::arrow::_raw::array::{
        types::Decimal256Type, ArrowPrimitiveType, PrimitiveArray,
    };

    use super::*;

    type I256 = <Decimal256Type as ArrowPrimitiveType>::Native;

    fn get_i256_values(test: &Test) -> Vec<String> {
        let arrays = test.arrays.arrow.as_ref().unwrap();
        let arr = arrays[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal256Type>>()
            .unwrap();
        arr.values().iter().map(I256::to_string).collect()
    }

    #[test]
    fn example() {
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(5, 2)"}]))
            .serialize(&[Item(String::from("0.20")), Item(String::from("-0.42"))])
            .deserialize(&[Item(String::from("0.20")), Item(String::from("-0.42"))])
            .also(|it| assert_eq!(get_i256_values(it), ["20", "-42"]));
    }

    /// Decimals with too many digits are truncated in serialization
    #[test]
    fn truncation() {
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(5, 2)"}]))
            .serialize(&[Item(String::from("0.2012")), Item(String::from("0.4234"))])
            .deserialize(&[Item(String::from("0.20")), Item(String::from("0.42"))]);
    }

    #[test]
    fn negative_scale() {
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(5, -2)"}]))
            .serialize(&[Item(String::from("1300.00")), Item(String::from("4200.00"))])
            .deserialize(&[Item(String::from("1300")), Item(String::from("4200"))])
            .also(|it| assert_eq!(get_i256_values(it), ["13", "42"]));
    }

    #[test]
    fn too_small_precision() {
        let mut test =
            Test::new().with_schema(json!([{"name": "item", "data_type": "Decimal256(2, 2)"}]));

        let err = test
            .try_serialize_arrow(&[Item(String::from("1.23"))])
            .expect_err("Expected error");
        assert!(err.to_string().contains("configured precision"));
    }

    /// `Decimal256(76, 38)` is used for example by the BigQuery Storage Read API
    #[test]
    fn full_precision() {
        let items = &[
            Item(format!("{}.{}", "9".repeat(38), "9".repeat(38))),
            Item(format!("-{}.{}", "9".repeat(38), "9".repeat(38))),
            Item(format!("0.{}1", "0".repeat(37))),
        ];
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(76, 38)"}]))
            .serialize(items)
            .deserialize(items)
            .also(|it| {
                assert_eq!(
                    get_i256_values(it),
                    [
                        "9".repeat(76),
                        format!("-{}", "9".repeat(76)),
                        "1".to_owned()
                    ]
                )
            });
    }

    #[test]
    fn floats() {
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(10, 2)"}]))
            .serialize(&[Item(1.25_f64), Item(-2.5_f64)])
            .also(|it| assert_eq!(get_i256_values(it), ["125", "-250"]));

        // floats outside of the i128 range are converted exactly
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Decimal256(76, 0)"}]))
            .serialize(&[Item(-1e60_f64)])
            .also(|it| {
                assert_eq!(
                    get_i256_values(it),
                    ["-999999999999999949387135297074018866963645011013410073083904"]
                )
            });
    }
}
