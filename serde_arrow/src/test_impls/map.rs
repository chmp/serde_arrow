use std::collections::{BTreeMap, HashMap};

use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::{Strategy, TracingOptions},
    test_impls::utils::Test,
    utils::Item,
};

use super::macros::{btree_map, hash_map};

// NOTE: Use BTreeMap to guarantee the order of fields

#[test]
fn map_as_struct() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false));
    type Ty = BTreeMap<String, u32>;

    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_map_as_struct() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false));
    type Ty = HashMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(hash_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_struct_nullable() {
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false));
    type Ty = Option<BTreeMap<String, u32>>;
    let values: &[Item<Ty>] = &[
        Item(Some(btree_map! { "a" => 1_u32, "b" => 2_u32 })),
        Item(None),
        Item(Some(btree_map! { "a" => 3_u32, "b" => 4_u32 })),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_struct_missing_fields() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, true));
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_missing_fields_2() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true));
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 6_u32 }),
        Item(btree_map! {}),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_missing_fields_3() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true));
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {}),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 6_u32 }),
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_nullable_fields() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true));
    type Ty = BTreeMap<String, Option<u32>>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => Some(1_u32), "b" => Some(4_u32) }),
        Item(btree_map! { "a" => Some(3_u32), "b" => None }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::LargeUtf8, false))
            .with_child(GenericField::new("value", GenericDataType::U32, false)),
    );
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map_empty() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::LargeUtf8, false))
            .with_child(GenericField::new("value", GenericDataType::U32, false)),
    );
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {}),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 3_u32, "c" => 3_u32 }),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map_int_keys() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I32, false))
            .with_child(GenericField::new("value", GenericDataType::U32, false)),
    );
    type Ty = BTreeMap<i32, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { -1_i32 => 1_u32, -2_i32 => 2_u32 }),
        Item(btree_map! { -2_i32 => 3_u32, -4_i32 => 4_u32 }),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false)),
    );
    type Ty = HashMap<i64, bool>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {0 => true, 1 => false, 2 => true}),
        Item(hash_map! {3 => false, 4 => true}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, true).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false)),
    );
    type Ty = Option<HashMap<i64, bool>>;
    let values: &[Item<Ty>] = &[
        Item(Some(hash_map! {0 => true, 1 => false, 2 => true})),
        Item(Some(hash_map! {3 => false, 4 => true})),
        Item(Some(hash_map! {})),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable_keys() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, true))
            .with_child(GenericField::new("value", GenericDataType::Bool, false)),
    );
    type Ty = HashMap<Option<i64>, bool>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {Some(0) => true, Some(1) => false, Some(2) => true}),
        Item(hash_map! {Some(3) => false, Some(4) => true}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable_values() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, true)),
    );
    type Ty = HashMap<i64, Option<bool>>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {0 => Some(true), 1 => Some(false), 2 => Some(true)}),
        Item(hash_map! {3 => Some(false), 4 => Some(true)}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn btree_maps() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    let field = GenericField::new("item", GenericDataType::Map, false).with_child(
        GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false)),
    );
    type Ty = BTreeMap<i64, bool>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {0 => true, 1 => false, 2 => true}),
        Item(btree_map! {3 => false, 4 => true}),
        Item(btree_map! {}),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}
