use super::macros::test_example;

test_example!(
    test_name = list_u32,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::LargeList, false)
        .with_child(GenericField::new("element", GenericDataType::U32, false)),
    ty = Vec<u32>,
    values = [vec![0, 1, 2], vec![3, 4], vec![]],
    nulls = [false, false, false],
);

test_example!(
    test_name = list_nullable_u64,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::LargeList, false)
        .with_child(GenericField::new("element", GenericDataType::U64, true)),
    ty = Vec<Option<u64>>,
    values = [vec![Some(0), None, Some(2)], vec![Some(3)], vec![None], vec![]],
    nulls = [false, false, false, false],
);

test_example!(
    test_name = nullable_list_u32,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::U32, false)),
    ty = Option<Vec<u32>>,
    values = [Some(vec![0, 1, 2]), None, Some(vec![3, 4]), Some(vec![])],
    nulls = [false, true, false, false],
);
