use serde_json::json;

use crate::utils::Item;

use super::utils::Test;

fn items() -> Vec<Item<i64>> {
    vec![Item(i64::MIN), Item(0), Item(100), Item(i64::MAX)]
}

#[test]
fn as_date64() {
    let items = items();

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_time64_nanosecond() {
    let items = items();

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_time64_microsecond() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Microsecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_duration_second() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_duration_millisecond() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_duration_microsecond() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Microsecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn as_duration_nanosecond() {
    let items = items();
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}
