use serde_json::json;

use super::utils::Test;

use crate::_impl::arrow::datatypes::FieldRef;
use crate::internal::testing::assert_error;
use crate::internal::utils::Item;
use crate::schema::SchemaLike;

#[test]
fn example() {
    let items = [Item(vec![0_u8, 1]), Item(vec![2, 3]), Item(vec![4, 5])];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "children": [{"name": "element", "data_type": "U8"}],
        }]))
        .serialize(&items);
    // .deserialize(&items);
}

#[test]
fn example_nullable_no_nulls() {
    let items = [Item(vec![0_u16, 1]), Item(vec![2, 3]), Item(vec![4, 5])];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "nullable": true,
            "children": [{"name": "element", "data_type": "U16"}],
        }]))
        .serialize(&items)
        .check_nulls(&[&[false, false, false]]);
    // .deserialize(&items);
}

#[test]
fn example_nullable_with_nulls() {
    let items = [
        Item(Some(vec![0_u16, 1])),
        Item(None),
        Item(Some(vec![4, 5])),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{
            "name": "item",
            "data_type": "FixedSizeList(2)",
            "nullable": true,
            "children": [{"name": "element", "data_type": "U16"}],
        }]))
        .serialize(&items)
        .check_nulls(&[&[false, true, false]]);
    // .deserialize(&items);
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

    let res = crate::to_record_batch(&fields, &items);
    assert_error(&res, "Invalid number of elements for FixedSizedList(2).");
}
