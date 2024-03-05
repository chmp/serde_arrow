use serde::{Deserialize, Serialize};

use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::{Strategy, TracingOptions},
    utils::Item,
};

use super::utils::Test;

#[test]
fn tuple_u64_bool() {
    let items = [Item((1_u64, true)), Item((2_u64, false))];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            false,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false))])
        .trace_schema_from_type::<Item<(u64, bool)>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn tuple_struct_u64_bool() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct S(u64, bool);

    let items = [Item(S(1_u64, true)), Item(S(2_u64, false))];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            false,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false))])
        .trace_schema_from_type::<Item<S>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn nullbale_tuple_u64_bool() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct S(u64, bool);

    let items = [
        Item(None),
        Item(Some(S(1_u64, true))),
        Item(Some(S(2_u64, false))),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            true,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false))])
        .trace_schema_from_type::<Item<Option<S>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[true, false, false]]);
}

#[test]
fn tuple_nullable_u64() {
    let items = [Item((Some(1_u64),)), Item((Some(2_u64),)), Item((None,))];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            false,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, true))])
        .trace_schema_from_type::<Item<(Option<u64>,)>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn tuple_nested() {
    let items = [Item(((1_u64,),)), Item(((2_u64,),))];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            false,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(
            GenericField::new("0", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U64, false)),
        )])
        .trace_schema_from_type::<Item<((u64,),)>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn tuple_nullable() {
    let items = [
        Item(Some((true, 21_i64))),
        Item(None),
        Item(Some((false, 42_i64))),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            true,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::Bool, false))
        .with_child(GenericField::new("1", GenericDataType::I64, false))])
        .trace_schema_from_type::<Item<Option<(bool, i64)>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn tuple_nullable_nested() {
    let items = [
        Item(Some(((true, 21_i64), 7_i64))),
        Item(None),
        Item(Some(((false, 42_i64), 13_i64))),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::Struct,
            true,
        )
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(
            GenericField::new("0", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::Bool, false))
                .with_child(GenericField::new("1", GenericDataType::I64, false)),
        )
        .with_child(GenericField::new("1", GenericDataType::I64, false))])
        .trace_schema_from_type::<Item<Option<((bool, i64), i64)>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}
