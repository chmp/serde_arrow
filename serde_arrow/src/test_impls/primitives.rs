use serde_json::json;

use crate::{schema::TracingOptions, utils::Item};

use super::{macros::test_example, utils::Test};

#[test]
fn null() {
    let items = &[Item(()), Item(()), Item(())];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Null", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default().allow_null_fields(true))
        .trace_schema_from_type::<Item<()>>(TracingOptions::default().allow_null_fields(true))
        .serialize(items)
        .deserialize(items);

    // NOTE: arrow2 has an incorrect is_null impl for NullArray
    // nulls = [true, true, true],
}

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

#[test]
fn u8() {
    let items: &[Item<u8>] = &[Item(1), Item(2), Item(3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<u8>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_u8() {
    let items: &[Item<Option<u8>>] = &[Item(Some(1)), Item(None), Item(Some(3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<u8>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn u16() {
    let items: &[Item<u16>] = &[Item(1), Item(2), Item(3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U16"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<u16>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_u16() {
    let items: &[Item<Option<u16>>] = &[Item(Some(1)), Item(None), Item(Some(3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U16", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<u16>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn u32() {
    let items: &[Item<u32>] = &[Item(1), Item(2), Item(3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U32"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<u32>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_u32() {
    let items: &[Item<Option<u32>>] = &[Item(Some(1)), Item(None), Item(Some(3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U32", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<u32>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn u64() {
    let items: &[Item<u64>] = &[Item(1), Item(2), Item(3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U64"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<u64>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_u64() {
    let items: &[Item<Option<u64>>] = &[Item(Some(1)), Item(None), Item(Some(3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U64", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<u64>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn i8() {
    let items: &[Item<i8>] = &[Item(-1), Item(2), Item(-3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I8"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<i8>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_i8() {
    let items: &[Item<Option<i8>>] = &[Item(Some(-1)), Item(None), Item(Some(-3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I8", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<i8>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn i16() {
    let items: &[Item<i16>] = &[Item(-1), Item(2), Item(-3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I16"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<i16>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_i16() {
    let items: &[Item<Option<i16>>] = &[Item(Some(-1)), Item(None), Item(Some(-3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I16", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<i16>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn i32() {
    let items: &[Item<i32>] = &[Item(-1), Item(2), Item(-3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I32"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<i32>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_i32() {
    let items: &[Item<Option<i32>>] = &[Item(Some(-1)), Item(None), Item(Some(-3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I32", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<i32>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

#[test]
fn i64() {
    let items: &[Item<i64>] = &[Item(-1), Item(2), Item(-3), Item(4)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I64"}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<i64>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn nullable_i64() {
    let items: &[Item<Option<i64>>] = &[Item(Some(-1)), Item(None), Item(Some(-3)), Item(Some(4))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "I64", "nullable": true}]))
        .trace_schema_from_samples(items, TracingOptions::default())
        .trace_schema_from_type::<Item<Option<i64>>>(TracingOptions::default())
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false, false]]);
}

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

test_example!(
    test_name = f32_from_f64,
    field = GenericField::new("item", GenericDataType::F64, false),
    overwrite_field = GenericField::new("item", GenericDataType::F32, false),
    ty = f64,
    values = [-1.0, 2.0, -3.0, 4.0],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = f64_from_f32,
    field = GenericField::new("item", GenericDataType::F32, false),
    overwrite_field = GenericField::new("item", GenericDataType::F64, false),
    ty = f32,
    values = [-1.0, 2.0, -3.0, 4.0],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = f16_from_f32,
    field = GenericField::new("item", GenericDataType::F32, false),
    overwrite_field = GenericField::new("item", GenericDataType::F16, false),
    ty = f32,
    values = [-1.0, 2.0, -3.0, 4.0],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = f16_from_f64,
    field = GenericField::new("item", GenericDataType::F64, false),
    overwrite_field = GenericField::new("item", GenericDataType::F16, false),
    ty = f64,
    values = [-1.0, 2.0, -3.0, 4.0],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = str,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = String,
    values = [
        String::from("a"),
        String::from("b"),
        String::from("c"),
        String::from("d")
    ],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = nullable_str,

    field = GenericField::new("item", GenericDataType::LargeUtf8, true),
    ty = Option<String>,
    values = [Some(String::from("a")), None, None, Some(String::from("d"))],
    nulls = [false, true, true, false],
);

test_example!(
    test_name = str_u32,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("item", GenericDataType::Utf8, false),
    ty = String,
    values = [
        String::from("a"),
        String::from("b"),
        String::from("c"),
        String::from("d")
    ],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = nullable_str_u32,

    field = GenericField::new("item", GenericDataType::LargeUtf8, true),
    overwrite_field = GenericField::new("item", GenericDataType::Utf8, true),
    ty = Option<String>,
    values = [Some(String::from("a")), None, None, Some(String::from("d"))],
    nulls = [false, true, true, false],
);

test_example!(
    test_name = newtype_i64,
    field = GenericField::new("item", GenericDataType::I64, false),
    ty = I64,
    values = [I64(-1), I64(2), I64(3), I64(-4)],
    nulls = [false, false, false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct I64(i64);
    },
);

test_example!(
    test_name = u8_to_u16,
    field = GenericField::new("item", GenericDataType::U8, false),
    overwrite_field = GenericField::new("item", GenericDataType::U16, false),
    ty = u8,
    values = [1, 2, 3, 4],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = u32_to_i64,
    field = GenericField::new("item", GenericDataType::U32, false),
    overwrite_field = GenericField::new("item", GenericDataType::I64, false),
    ty = u32,
    values = [1, 2, 3, 4],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = chars,
    field = GenericField::new("item", GenericDataType::U32, false),
    ty = char,
    values = ['a', 'b', 'c'],
    nulls = [false, false, false],
);
