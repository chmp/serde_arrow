use super::macros::test_example;

// NOTE: Use BTreeMap to guarantee the order of fields

test_example!(
    test_name = map_as_struct,
    test_compilation = false,
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
    test_compilation = false,
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
    test_compilation = false,
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
    test_name = map_as_struct_nullable_fields,
    test_compilation = false,
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
    test_name = map_as_map,
    test_compilation = false,
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
    test_name = map_as_map_int_keys,
    test_compilation = false,
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
