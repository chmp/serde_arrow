use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use crate::internal::{
    schema::TracingOptions,
    testing::hash_map,
    utils::{btree_map, Item},
};

use super::utils::Test;

// NOTE: Use BTreeMap to guarantee the order of fields

#[test]
fn map_as_struct() {
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "U32"},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_map_as_struct() {
    type Ty = HashMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(hash_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "U32"},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_struct_nullable() {
    type Ty = Option<BTreeMap<String, u32>>;
    let values: &[Item<Ty>] = &[
        Item(Some(btree_map! { "a" => 1_u32, "b" => 2_u32 })),
        Item(None),
        Item(Some(btree_map! { "a" => 3_u32, "b" => 4_u32 })),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "U32"},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_struct_missing_fields() {
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "U32", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_missing_fields_2() {
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 6_u32 }),
        Item(btree_map! {}),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32", "nullable": true},
                    {"name": "b", "data_type": "U32", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_missing_fields_3() {
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {}),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 6_u32 }),
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32", "nullable": true},
                    {"name": "b", "data_type": "U32", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values);
}

#[test]
fn map_as_struct_nullable_fields() {
    type Ty = BTreeMap<String, Option<u32>>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => Some(1_u32), "b" => Some(4_u32) }),
        Item(btree_map! { "a" => Some(3_u32), "b" => None }),
    ];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "U32", "nullable": true},
                    {"name": "b", "data_type": "U32", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { "a" => 1_u32, "b" => 2_u32 }),
        Item(btree_map! { "a" => 3_u32, "b" => 4_u32 }),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "LargeUtf8"},
                            {"name": "value", "data_type": "U32"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map_empty() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    type Ty = BTreeMap<String, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {}),
        Item(btree_map! { "a" => 3_u32 }),
        Item(btree_map! { "b" => 3_u32, "c" => 3_u32 }),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "LargeUtf8"},
                            {"name": "value", "data_type": "U32"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn map_as_map_int_keys() {
    let tracing_options = TracingOptions::default().map_as_struct(false);
    type Ty = BTreeMap<i32, u32>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! { -1_i32 => 1_u32, -2_i32 => 2_u32 }),
        Item(btree_map! { -2_i32 => 3_u32, -4_i32 => 4_u32 }),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I32"},
                            {"name": "value", "data_type": "U32"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    type Ty = HashMap<i64, bool>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {0 => true, 1 => false, 2 => true}),
        Item(hash_map! {3 => false, 4 => true}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I64"},
                            {"name": "value", "data_type": "Bool"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    type Ty = Option<HashMap<i64, bool>>;
    let values: &[Item<Ty>] = &[
        Item(Some(hash_map! {0 => true, 1 => false, 2 => true})),
        Item(Some(hash_map! {3 => false, 4 => true})),
        Item(Some(hash_map! {})),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "nullable": true,
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I64"},
                            {"name": "value", "data_type": "Bool"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable_keys() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    type Ty = HashMap<Option<i64>, bool>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {Some(0) => true, Some(1) => false, Some(2) => true}),
        Item(hash_map! {Some(3) => false, Some(4) => true}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I64", "nullable": true},
                            {"name": "value", "data_type": "Bool"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn hash_maps_nullable_values() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    type Ty = HashMap<i64, Option<bool>>;
    let values: &[Item<Ty>] = &[
        Item(hash_map! {0 => Some(true), 1 => Some(false), 2 => Some(true)}),
        Item(hash_map! {3 => Some(false), 4 => Some(true)}),
        Item(hash_map! {}),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I64"},
                            {"name": "value", "data_type": "Bool", "nullable": true},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}

#[test]
fn btree_maps() {
    let tracing_options = TracingOptions::new().map_as_struct(false);
    type Ty = BTreeMap<i64, bool>;
    let values: &[Item<Ty>] = &[
        Item(btree_map! {0 => true, 1 => false, 2 => true}),
        Item(btree_map! {3 => false, 4 => true}),
        Item(btree_map! {}),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Map",
                "children": [
                    {
                        "name": "entries",
                        "data_type": "Struct",
                        "children": [
                            {"name": "key", "data_type": "I64"},
                            {"name": "value", "data_type": "Bool"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_samples(values, tracing_options.clone())
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .serialize(values)
        .deserialize(values);
}
