use super::macros::test_example;

test_example!(
    test_name = tuple_u64_bool,
    test_compilation = false,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, false))
        .with_child(GenericField::new("1", GenericDataType::Bool, false)),
    ty = (u64, bool),
    values = [(1, true), (2, false)],
    nulls = [false, false],
);

test_example!(
    test_name = nullbale_tuple_u64_bool,
    test_compilation = false,
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
    test_compilation = false,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericDataType::U64, true)),
    ty = (Option<u64>,),
    values = [(Some(1),), (Some(2),), (None,)],
    nulls = [false, false, false],
);

test_example!(
    test_name = tuple_nested,
    test_compilation = false,
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
