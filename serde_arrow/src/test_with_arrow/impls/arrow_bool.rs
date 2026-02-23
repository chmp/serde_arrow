use serde_json::json;

use crate::internal::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn bool() {
    let items = &[Item(true), Item(false)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Bool"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<bool>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn nullable_bool() {
    let items = &[Item(Some(true)), Item(None), Item(Some(false))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Bool", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<bool>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false]]);
}

fn test_bool_from_int<T: serde::Serialize>(zero: T, nonzero: T) {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Bool"}]))
        .serialize(&[Item(zero), Item(nonzero)])
        .deserialize(&[Item(false), Item(true)]);
}

#[test]
fn bool_from_i8() {
    test_bool_from_int(0_i8, 1_i8);
    test_bool_from_int(0_i8, 32_i8);
    test_bool_from_int(0_i8, -1_i8);
}

#[test]
fn bool_from_i16() {
    test_bool_from_int(0_i16, 1_i16);
    test_bool_from_int(0_i16, 32_i16);
    test_bool_from_int(0_i16, -1_i16);
}

#[test]
fn bool_from_i32() {
    test_bool_from_int(0_i32, 1_i32);
    test_bool_from_int(0_i32, 32_i32);
    test_bool_from_int(0_i32, -1_i32);
}

#[test]
fn bool_from_i64() {
    test_bool_from_int(0_i64, 1_i64);
    test_bool_from_int(0_i64, 32_i64);
    test_bool_from_int(0_i64, -1_i64);
}

#[test]
fn bool_from_u8() {
    test_bool_from_int(0_u8, 1_u8);
    test_bool_from_int(0_u8, 32_u8);
}

#[test]
fn bool_from_u16() {
    test_bool_from_int(0_u16, 1_u16);
    test_bool_from_int(0_u16, 32_u16);
}

#[test]
fn bool_from_u32() {
    test_bool_from_int(0_u32, 1_u32);
    test_bool_from_int(0_u32, 32_u32);
}

#[test]
fn bool_from_u64() {
    test_bool_from_int(0_u64, 1_u64);
    test_bool_from_int(0_u64, 32_u64);
}
