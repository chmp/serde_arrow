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
        // NOTE: arrow2 only supports positive scale
        .skip_arrow2()
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
    assert!(err.to_string().contains("not enough precision"));

    let err = test
        .try_serialize_arrow2(items)
        .expect_err("Expected error");
    assert!(err.to_string().contains("not enough precision"));
}
