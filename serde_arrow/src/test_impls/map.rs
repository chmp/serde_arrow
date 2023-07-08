use super::macros::{test_events, test_example};

// NOTE: Use BTreeMap to guarantee the order of fields

test_example!(
    test_name = map_as_struct,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false)),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ "a" => 1_u32, "b" => 2_u32 },
        btree_map!{ "a" => 3_u32, "b" => 4_u32 },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = hash_map_as_struct,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false)),
    ty = HashMap<String, u32>,
    values = [
        hash_map!{ "a" => 1_u32, "b" => 2_u32 },
        hash_map!{ "a" => 3_u32, "b" => 4_u32 },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = map_as_struct_nullable,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, false)),
    ty = Option<BTreeMap<String, u32>>,
    values = [
        Some(btree_map!{ "a" => 1_u32, "b" => 2_u32 }),
        None,
        Some(btree_map!{ "a" => 3_u32, "b" => 4_u32 }),
    ],
    nulls = [false, true, false],
);

test_example!(
    test_name = map_as_struct_missing_fields,
    test_bytecode_deserialization = true,
    test_deserialization = [],
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::U32, true)),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ "a" => 1_u32 },
        btree_map!{ "a" => 3_u32, "b" => 4_u32 },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = map_as_struct_missing_fields_2,
    test_bytecode_deserialization = true,
    test_deserialization = [],
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true)),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ "a" => 1_u32, "b" => 2_u32 },
        btree_map!{ "a" => 3_u32 },
        btree_map!{ "b" => 6_u32 },
        btree_map!{ },
    ],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = map_as_struct_missing_fields_3,
    test_bytecode_deserialization = true,
    test_deserialization = [],
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true)),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ },
        btree_map!{ "a" => 3_u32 },
        btree_map!{ "b" => 6_u32 },
        btree_map!{ "a" => 1_u32, "b" => 2_u32 },
    ],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = map_as_struct_nullable_fields,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::U32, true)),
    ty = BTreeMap<String, Option<u32>>,
    values = [
        btree_map!{ "a" => Some(1_u32), "b" => Some(4_u32) },
        btree_map!{ "a" => Some(3_u32), "b" => None },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = map_as_map,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(
            GenericField::new("entries", GenericDataType::Struct, false)
                .with_child(GenericField::new("key", GenericDataType::LargeUtf8, false))
                .with_child(GenericField::new("value", GenericDataType::U32, false))
        ),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ "a" => 1_u32, "b" => 2_u32 },
        btree_map!{ "a" => 3_u32, "b" => 4_u32 },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = map_as_map_empty,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(
            GenericField::new("entries", GenericDataType::Struct, false)
                .with_child(GenericField::new("key", GenericDataType::LargeUtf8, false))
                .with_child(GenericField::new("value", GenericDataType::U32, false))
        ),
    ty = BTreeMap<String, u32>,
    values = [
        btree_map!{ },
        btree_map!{ "a" => 3_u32 },
        btree_map!{ "b" => 3_u32, "c" => 3_u32 },
    ],
    nulls = [false, false, false],
);

test_example!(
    test_name = map_as_map_int_keys,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(
            GenericField::new("entries", GenericDataType::Struct, false)
                .with_child(GenericField::new("key", GenericDataType::I32, false))
                .with_child(GenericField::new("value", GenericDataType::U32, false))
        ),
    ty = BTreeMap<i32, u32>,
    values = [
        btree_map!{ -1_i32 => 1_u32, -2_i32 => 2_u32 },
        btree_map!{ -2_i32 => 3_u32, -4_i32 => 4_u32 },
    ],
    nulls = [false, false],
);

test_example!(
    test_name = hash_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false))),
    ty = HashMap<i64, bool>,
    values = [
        hash_map!{0 => true, 1 => false, 2 => true},
        hash_map!{3 => false, 4 => true},
        hash_map!{},
    ],
);

test_example!(
    test_name = hash_maps_nullable,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, true)
        .with_child(GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false))),
    ty = Option<HashMap<i64, bool>>,
    values = [
        Some(hash_map!{0 => true, 1 => false, 2 => true}),
        Some(hash_map!{3 => false, 4 => true}),
        Some(hash_map!{}),
    ],
);

test_example!(
    test_name = hash_maps_nullable_keys,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, true))
            .with_child(GenericField::new("value", GenericDataType::Bool, false))),
    ty = HashMap<Option<i64>, bool>,
    values = [
        hash_map!{Some(0) => true, Some(1) => false, Some(2) => true},
        hash_map!{Some(3) => false, Some(4) => true},
        hash_map!{},
    ],
);

test_example!(
    test_name = hash_maps_nullable_values,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, true))),
    ty = HashMap<i64, Option<bool>>,
    values = [
        hash_map!{0 => Some(true), 1 => Some(false), 2 => Some(true)},
        hash_map!{3 => Some(false), 4 => Some(true)},
        hash_map!{},
    ],
);

test_example!(
    test_name = btree_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = GenericField::new("root", GenericDataType::Map, false)
        .with_child(GenericField::new("entries", GenericDataType::Struct, false)
            .with_child(GenericField::new("key", GenericDataType::I64, false))
            .with_child(GenericField::new("value", GenericDataType::Bool, false))),
    ty = BTreeMap<i64, bool>,
    values = [
        btree_map!{0 => true, 1 => false, 2 => true},
        btree_map!{3 => false, 4 => true},
        btree_map!{},
    ],
);

test_events!(
    test_name = out_of_order_fields,
    fields = [
        // NOTE: map fields are always sorted
        GenericField::new("bar", GenericDataType::U32, false),
        GenericField::new("foo", GenericDataType::U32, false),
    ],
    events = [
        Event::StartSequence,
        Event::Item,
        Event::StartMap,
        Event::Str("foo"),
        Event::U32(0),
        Event::Str("bar"),
        Event::U32(1),
        Event::EndMap,
        Event::Item,
        Event::StartMap,
        Event::Str("bar"),
        Event::U32(2),
        Event::Str("foo"),
        Event::U32(3),
        Event::EndMap,
        Event::EndSequence,
    ],
);
