use serde_json::json;

use crate::internal::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn f32() {
    let items: &[Item<f32>] = &[Item(-1.0), Item(2.0), Item(-3.0), Item(4.0)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<f32>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_f32() {
    let items: &[Item<Option<f32>>] = &[
        Item(Some(-1.0)),
        Item(None),
        Item(Some(-3.0)),
        Item(Some(4.0)),
    ];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<f32>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn f32_from_u8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(1_u8), Item(2), Item(4), Item(8)]);
}

#[test]
fn f16_from_u8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(1_u8), Item(2), Item(4), Item(8)]);
}

#[test]
fn f16_from_u16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(1_u16), Item(2), Item(4), Item(8)]);
}

#[test]
fn f16_from_u32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(1_u32), Item(2), Item(4), Item(8)]);
}

#[test]
fn f16_from_u64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(1_u64), Item(2), Item(4), Item(8)]);
}

#[test]
fn f16_from_i8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1_i8), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f16_from_i16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1_i16), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f16_from_i32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1_i32), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f16_from_i64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1_i64), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f32_from_u16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(1_u16), Item(2), Item(4), Item(8)]);
}

#[test]
fn f32_from_u32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(1_u32), Item(2), Item(4), Item(8)]);
}

#[test]
fn f32_from_u64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(1_u64), Item(2), Item(4), Item(8)]);
}

#[test]
fn f32_from_i8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(-1_i8), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f32_from_i16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(-1_i16), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f32_from_i32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(-1_i32), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f32_from_i64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(-1_i64), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f64_from_u8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(1_u8), Item(2), Item(4), Item(8)]);
}

#[test]
fn f64_from_u16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(1_u16), Item(2), Item(4), Item(8)]);
}

#[test]
fn f64_from_u32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(1_u32), Item(2), Item(4), Item(8)]);
}

#[test]
fn f64_from_u64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(1_u64), Item(2), Item(4), Item(8)]);
}

#[test]
fn f64_from_i8() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(-1_i8), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f64_from_i16() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(-1_i16), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f64_from_i32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(-1_i32), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f64_from_i64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(-1_i64), Item(2), Item(-4), Item(8)]);
}

#[test]
fn f16_from_string() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item("-1"), Item("2"), Item("-3"), Item("4")]);
}

#[test]
fn f16_from_string_with_underscores() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item("-1_0"), Item("2_0"), Item("-3_0"), Item("4_0")]);
}

#[test]
fn string_from_f16() {
    let values = [Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)];
    let expected = [
        Item(String::from("-1")),
        Item(String::from("2")),
        Item(String::from("-3")),
        Item(String::from("4")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&values)
        .deserialize(&expected);
}

#[test]
fn f32_from_string() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item("-1"), Item("2"), Item("-3"), Item("4")]);
}

#[test]
fn f32_from_string_with_underscores() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[
            Item("-1_000.5"),
            Item("2_000.0"),
            Item("-3_000"),
            Item("4_000.25"),
        ]);
}

#[test]
fn string_from_f32() {
    let values = [Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)];
    let expected = [
        Item(String::from("-1")),
        Item(String::from("2")),
        Item(String::from("-3")),
        Item(String::from("4")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&values)
        .deserialize(&expected);
}

#[test]
fn f64_from_string() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item("-1"), Item("2"), Item("-3"), Item("4")]);
}

#[test]
fn f64_from_string_with_underscores() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[
            Item("-1_000.5"),
            Item("2_000.0"),
            Item("-3_000"),
            Item("4_000.25"),
        ]);
}

#[test]
fn string_from_f64() {
    let values = [Item(-1.0_f64), Item(2.0), Item(-3.0), Item(4.0)];
    let expected = [
        Item(String::from("-1")),
        Item(String::from("2")),
        Item(String::from("-3")),
        Item(String::from("4")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&values)
        .deserialize(&expected);
}

#[test]
fn f64() {
    let items: &[Item<f64>] = &[Item(-1.0), Item(2.0), Item(-3.0), Item(4.0)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<f64>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_f64() {
    let items: &[Item<Option<f64>>] = &[
        Item(Some(-1.0)),
        Item(None),
        Item(Some(-3.0)),
        Item(Some(4.0)),
    ];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<f64>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn f32_from_f64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F32"}]))
        .serialize(&[Item(-1.0_f64), Item(2.0), Item(-3.0), Item(4.0)]);
}

#[test]
fn f64_from_f32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F64"}]))
        .serialize(&[Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)]);
}

#[test]
fn f16_from_f32() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)]);
}

#[test]
fn f16_from_f64() {
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "F16"}]))
        .serialize(&[Item(-1.0_f64), Item(2.0), Item(-3.0), Item(4.0)]);
}
