//! Test the support struct wrappers
use super::utils::Test;

use crate::internal::utils::value::{Value, Variant};

use serde_json::json;

#[test]
fn r#struct() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::Struct(
            "Record",
            vec![("a", Value::U8(0))],
        )]));
}

#[test]
fn newtype_wrapper() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::NewtypeStruct(
            "Wrapper",
            Box::new(Value::Struct("Record", vec![("a", Value::U8(0))])),
        )]));
}

#[test]
fn newtype_variant_wrapper() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::NewtypeVariant(
            Variant("Wrapper", 0, "Variant"),
            Box::new(Value::Struct("Record", vec![("a", Value::U8(0))])),
        )]));
}

#[test]
fn struct_variant() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::StructVariant(
            Variant("Record", 0, "Variant"),
            vec![("a", Value::U8(0))],
        )]));
}

#[test]
fn seq() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::Seq(vec![Value::U8(0)])]));
}

#[test]
fn tuple() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::Tuple(vec![Value::U8(0)])]));
}

#[test]
fn tuple_struct() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::TupleStruct(
            "Tuple",
            vec![Value::U8(0)],
        )]));
}

#[test]
fn tuple_variant() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::TupleVariant(
            Variant("Tuple", 0, "Variant"),
            vec![Value::U8(0)],
        )]));
}

#[test]
fn map() {
    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
        .serialize(&Value::Seq(vec![Value::Map(vec![(
            Value::StaticStr("a"),
            Value::U8(0),
        )])]));
}
