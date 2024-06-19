use serde_json::json;

use super::utils::Test;

use crate::_impl::arrow::datatypes::FieldRef;
use crate::internal::testing::ResultAsserts;
use crate::internal::utils::Item;
use crate::schema::SchemaLike;

#[test]
fn example() {
    let items = [Item(vec![0_u8, 1]), Item(vec![2, 3]), Item(vec![4, 5])];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_nullable_no_nulls() {
    let items = [Item(vec![0_u16, 1]), Item(vec![2, 3]), Item(vec![4, 5])];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "nullable": true,
            "children": [{"name": "element", "data_type": "U16"}],
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn example_nullable_with_nulls() {
    let items = [
        Item(Some(vec![0_u16, 1])),
        Item(None),
        Item(Some(vec![4, 5])),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "nullable": true,
            "children": [{"name": "element", "data_type": "U16"}],
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn incorrect_number_of_elements() {
    let items = [Item(vec![0_u8, 1]), Item(vec![2])];

    let fields = Vec::<FieldRef>::from_value(&json!([{
        "name": "item",
        "data_type": "FixedSizeList(2)",
        "children": [{"name": "element", "data_type": "U8"}],
    }]))
    .unwrap();

    crate::to_record_batch(&fields, &items)
        .assert_error("Invalid number of elements for FixedSizedList(2).");
}

#[test]
fn deserialize_from_schema() {
    let fields = Vec::<FieldRef>::from_value(&json!([{
        "name": "item",
        "data_type": "FixedSizeList(2)",
        "children": [{"name": "element", "data_type": "U8"}],
    }]))
    .unwrap();

    let fields_from_fields = Vec::<FieldRef>::from_value(&fields).unwrap();

    assert_eq!(fields, fields_from_fields);
}
