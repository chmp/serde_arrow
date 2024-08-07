use serde_json::json;

use crate::internal::utils::Item;

use super::utils::Test;

#[test]
fn bool_as_int8() {
    let items = &[Item(true), Item(false)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8"}]))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn nullable_bool_as_int8() {
    let items = &[Item(Some(true)), Item(None), Item(Some(false))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8", "nullable": true}]))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false]]);
}
