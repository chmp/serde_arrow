use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::TracingOptions,
    utils::Item,
};

use super::utils::Test;

#[test]
fn large_list_u32() {
    let items = [Item(vec![0_u32, 1, 2]), Item(vec![3, 4]), Item(vec![])];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            false,
        )
        .with_child(GenericField::new("element", GenericDataType::U32, false))])
        .trace_schema_from_type::<Item<Vec<u32>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn large_list_nullable_u64() {
    let items = [
        Item(vec![Some(0_u64), None, Some(2)]),
        Item(vec![Some(3)]),
        Item(vec![None]),
        Item(vec![]),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            false,
        )
        .with_child(GenericField::new("element", GenericDataType::U64, true))])
        .trace_schema_from_type::<Item<Vec<Option<u64>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn nullable_large_list_u32() {
    let items = [
        Item(Some(vec![0_u32, 1, 2])),
        Item(None),
        Item(Some(vec![3, 4])),
        Item(Some(vec![])),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            true,
        )
        .with_child(GenericField::new("element", GenericDataType::U32, false))])
        .trace_schema_from_type::<Item<Option<Vec<u32>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn list_u32() {
    let items = [Item(vec![0_u32, 1, 2]), Item(vec![3, 4]), Item(vec![])];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::List,
            false,
        )
        .with_child(GenericField::new("element", GenericDataType::U32, false))])
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn nested_large_list_u32() {
    let items = [
        Item(vec![vec![0_u32, 1, 2], vec![3, 4]]),
        Item(vec![vec![5, 6], vec![]]),
        Item(vec![]),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            false,
        )
        .with_child(
            GenericField::new("element", GenericDataType::LargeList, false)
                .with_child(GenericField::new("element", GenericDataType::U32, false)),
        )])
        .trace_schema_from_type::<Item<Vec<Vec<u32>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn nullable_vec_bool() {
    let items = [
        Item(Some(vec![true, false])),
        Item(None),
        Item(Some(vec![])),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            true,
        )
        .with_child(GenericField::new("element", GenericDataType::Bool, false))])
        .trace_schema_from_type::<Item<Option<Vec<bool>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn nullable_vec_bool_nested() {
    let items = [
        Item(Some(vec![vec![true], vec![false, false]])),
        Item(None),
        Item(Some(vec![vec![]])),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            true,
        )
        .with_child(
            GenericField::new("element", GenericDataType::LargeList, false)
                .with_child(GenericField::new("element", GenericDataType::Bool, false)),
        )])
        .trace_schema_from_type::<Item<Option<Vec<Vec<bool>>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn vec_nullable_bool() {
    let items = [
        Item(vec![Some(true), Some(false)]),
        Item(vec![]),
        Item(vec![None, Some(false)]),
    ];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            false,
        )
        .with_child(GenericField::new("element", GenericDataType::Bool, true))])
        .trace_schema_from_type::<Item<Vec<Option<bool>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn byte_arrays() {
    let items = [Item(b"hello".to_vec()), Item(b"world!".to_vec())];

    Test::new()
        .with_schema(vec![GenericField::new(
            "item",
            GenericDataType::LargeList,
            false,
        )
        .with_child(GenericField::new("element", GenericDataType::U8, false))])
        .trace_schema_from_type::<Item<Vec<u8>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}
