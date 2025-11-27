use serde_json::json;

use crate::internal::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn large_list_u32() {
    let items = [Item(vec![0_u32, 1, 2]), Item(vec![3, 4]), Item(vec![])];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [{"name": "element", "data_type": "U32"}],
        }]))
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [{"name": "element", "data_type": "U64", "nullable": true}],
        }]))
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "nullable": true,
            "children": [{"name": "element", "data_type": "U32"}],
        }]))
        .trace_schema_from_type::<Item<Option<Vec<u32>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn list_u32() {
    let items = [Item(vec![0_u32, 1, 2]), Item(vec![3, 4]), Item(vec![])];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "List",
            "children": [{"name": "element", "data_type": "U32"}],
        }]))
        .trace_schema_from_type::<Item<Vec<u32>>>(
            TracingOptions::default().sequence_as_large_list(false),
        )
        .trace_schema_from_samples(
            &items,
            TracingOptions::default().sequence_as_large_list(false),
        )
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [{
                "name": "element",
                "data_type": "LargeList",
                "children": [{
                    "name": "element",
                    "data_type": "U32",
                }],
            }],
        }]))
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "nullable": true,
            "children": [{"name": "element", "data_type": "Bool"}],
        }]))
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "nullable": true,
            "children": [{
                "name": "element",
                "data_type": "LargeList",
                "children": [{"name": "element", "data_type": "Bool"}],
            }],
        }]))
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
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [{"name": "element", "data_type": "Bool", "nullable": true}],
        }]))
        .trace_schema_from_type::<Item<Vec<Option<bool>>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn byte_arrays() {
    let items = [Item(b"hello".to_vec()), Item(b"world!".to_vec())];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .trace_schema_from_type::<Item<Vec<u8>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn tuple_as_list() {
    use crate::internal::utils::value::Value;

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "List",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .serialize(&[Item(Value::Tuple(vec![
            Value::U8(0),
            Value::U8(1),
            Value::U8(2),
        ]))]);
}

#[test]
fn tuple_struct_as_list() {
    use crate::internal::utils::value::Value;

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "List",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .serialize(&[Item(Value::TupleStruct(
            "Tuple",
            vec![Value::U8(0), Value::U8(1), Value::U8(2)],
        ))]);
}

#[test]
fn tuple_variant_as_list() {
    use crate::internal::utils::value::{Value, Variant};

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "List",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .serialize(&[Item(Value::TupleVariant(
            Variant("Tuple", 0, "Variant"),
            vec![Value::U8(0), Value::U8(1), Value::U8(2)],
        ))]);
}
