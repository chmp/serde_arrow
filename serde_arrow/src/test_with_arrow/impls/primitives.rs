use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::internal::{
    arrow::{DataType, Field},
    schema::TracingOptions,
    utils::Item,
};

use super::utils::Test;

fn new_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field {
        name: name.to_owned(),
        data_type,
        nullable,
        metadata: Default::default(),
    }
}

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

#[test]
fn f32_from_f64() {
    let values = [Item(-1.0_f64), Item(2.0), Item(-3.0), Item(4.0)];
    let field = new_field("item", DataType::Float32, false);

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn f64_from_f32() {
    let field = new_field("item", DataType::Float64, false);
    let values = [Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn f16_from_f32() {
    let field = new_field("item", DataType::Float16, false);
    let values = [Item(-1.0_f32), Item(2.0), Item(-3.0), Item(4.0)];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn f16_from_f64() {
    let field = new_field("item", DataType::Float16, false);
    let values = [Item(-1.0_f64), Item(2.0), Item(-3.0), Item(4.0)];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn str() {
    let field = new_field("item", DataType::LargeUtf8, false);
    type Ty = String;
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn str_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    type Ty = String;
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(
            &values,
            TracingOptions::default().strings_as_large_utf8(false),
        )
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default().strings_as_large_utf8(false))
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_str() {
    let field = new_field("item", DataType::LargeUtf8, true);
    type Ty = Option<String>;
    let values = [
        Item(Some(String::from("a"))),
        Item(None),
        Item(None),
        Item(Some(String::from("d"))),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn str_u32() {
    let field = new_field("item", DataType::Utf8, false);
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_str_u32() {
    let field = new_field("item", DataType::Utf8, true);
    let values = [
        Item(Some(String::from("a"))),
        Item(None),
        Item(None),
        Item(Some(String::from("d"))),
    ];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn borrowed_str() {
    let field = new_field("item", DataType::LargeUtf8, false);

    type Ty<'a> = &'a str;

    let values = [Item("a"), Item("b"), Item("c"), Item("d")];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn nullabe_borrowed_str() {
    let field = new_field("item", DataType::LargeUtf8, true);

    type Ty<'a> = Option<&'a str>;

    let values = [Item(Some("a")), Item(None), Item(None), Item(Some("d"))];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn borrowed_str_u32() {
    let field = new_field("item", DataType::Utf8, false);

    let values = [Item("a"), Item("b"), Item("c"), Item("d")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn nullabe_borrowed_str_u32() {
    let field = new_field("item", DataType::Utf8, true);

    let values = [Item(Some("a")), Item(None), Item(None), Item(Some("d"))];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn newtype_i64() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct I64(i64);

    let field = new_field("item", DataType::Int64, false);
    type Ty = I64;

    let values = [Item(I64(-1)), Item(I64(2)), Item(I64(3)), Item(I64(-4))];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn u8_to_u16() {
    let field = new_field("item", DataType::UInt16, false);
    let values = [Item(1_u8), Item(2), Item(3), Item(4)];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn u32_to_i64() {
    let field = new_field("item", DataType::Int64, false);
    let values = [Item(1_u32), Item(2), Item(3), Item(4)];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn chars() {
    let field = new_field("item", DataType::UInt32, false);
    type Ty = char;
    let values = [Item('a'), Item('b'), Item('c')];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}
