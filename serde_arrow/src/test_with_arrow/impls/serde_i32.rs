use serde_json::json;

use crate::utils::Item;

use super::utils::Test;

fn items() -> Vec<Item<i32>> {
    vec![Item(i32::MIN), Item(0), Item(100), Item(i32::MAX)]
}

#[test]
fn as_date32() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date32",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_time32_second() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time32(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_time32_millisecond() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time32(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}
