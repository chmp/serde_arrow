use serde_json::json;

use crate::schema::TracingOptions;

use super::utils::{ResultAsserts, Test};

#[test]
fn serde_json_example() {
    let items = vec![json!({ "a": 1, "b": 2 }), json!({ "a": 3, "b": 4 })];
    Test::new()
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items);
}

#[test]
fn serde_json_mixed_ints() {
    let items = vec![json!({ "a": 1, "b": -2 }), json!({ "a": 3.0, "b": 4 })];
    Test::new()
        .trace_schema_from_samples(&items, TracingOptions::default().coerce_numbers(true))
        .serialize(&items);
}

#[test]
fn serde_json_mixed_fixed_schema() {
    let items = vec![json!({ "a": 1, "b": -2 }), json!({ "a": 3.0, "b": 4 })];
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "F64"},
            {"name": "b", "data_type": "I64"},
        ]))
        .serialize(&items);
}

#[test]
fn serde_json_mixed_fixed_schema_outer_array() {
    let items = json!([{ "a": 1, "b": -2 }, { "a": 3.0, "b": 4 }]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "F64"},
            {"name": "b", "data_type": "I64"},
        ]))
        .serialize(&items);
}

#[test]
fn serde_json_strings() {
    let items = json!([{ "a": "hello", "b": "foo" }, { "a": "world", "b": "bar" }]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "Utf8"},
            {"name": "b", "data_type": "Utf8"},
        ]))
        .serialize(&items);
}

#[test]
fn serde_json_out_of_order() {
    // Note: if serde_json is compiled with the preserver_order feature, the
    // keys will be "a", "b" or the keys are sorted, in which keys the key
    // order is also "a", "b".
    let items = json!([{ "a": "hello", "b": true }, { "a": "world", "b": false }]);

    // Here the key "b" is encountered in the OuterRecordEnd state. This was
    // previously not correctly handled (issue #80).
    Test::new()
        .with_schema(json!([
            {"name": "b", "data_type": "Bool"},
            {"name": "a", "data_type": "Utf8"},
        ]))
        .serialize(&items);
}

#[test]
fn serde_json_nullable_strings_non_nullable_field() {
    let items = json!([{ "a": "hello" }, { "a": null }]);

    let mut test = Test::new().with_schema(json!([
        {"name": "a", "data_type": "Utf8"},
    ]));

    test.try_serialize_arrow(&items)
        .assert_error("cannot push null for non-nullable array");
    test.try_serialize_arrow2(&items)
        .assert_error("cannot push null for non-nullable array");
}
