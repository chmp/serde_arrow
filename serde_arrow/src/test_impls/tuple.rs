use super::macros::test_example;

test_example!(
    test_name = tuple_u64_bool,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false)),
    ty = (u64, bool),
    values = [(1, true), (2, false)],
    nulls = [false, false],
);

test_example!(
    test_name = tuple_struct_u64_bool,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false)),
    ty = S,
    values = [S(1, true), S(2, false)],
    nulls = [false, false],
    define = {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct S(u64, bool);
    },
);

test_example!(
    test_name = nullbale_tuple_u64_bool,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false)),
    ty = Option<(u64, bool)>,
    values = [None, Some((1, true)), Some((2, false))],
    nulls = [true, false, false],
);

test_example!(
    test_name = tuple_nullable_u64,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, true)),
    ty = (Option<u64>,),
    values = [(Some(1),), (Some(2),), (None,)],
    nulls = [false, false, false],
);

test_example!(
    test_name = tuple_nested,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(
            GenericField::new("0", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U64, false))
        ),
    ty = ((u64,),),
    values = [((1,),), ((2,),)],
    nulls = [false, false],
);

test_example!(
    test_name = tuple_nullable,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::Bool, false))
        .with_child(GenericField::new("1", GenericDataType::I64, false)),
    ty = Option<(bool, i64)>,
    values = [
        Some((true, 21)),
        None,
        Some((false, 42)),
    ],
);

test_example!(
    test_name = tuple_nullable_nested,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::Struct, false)
            .with_strategy(Strategy::TupleAsStruct)
            .with_child(GenericField::new("0", GenericDataType::Bool, false))
            .with_child(GenericField::new("1", GenericDataType::I64, false)))
        .with_child(GenericField::new("1", GenericDataType::I64, false)),
    ty = Option<((bool, i64), i64)>,
    values = [
        Some(((true, 21), 7)),
        None,
        Some(((false, 42), 13)),
    ],
);
