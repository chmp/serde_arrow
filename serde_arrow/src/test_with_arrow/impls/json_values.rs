use std::collections::HashMap;

use serde_json::{json, Value};

use crate::{internal::testing::assert_error_contains, schema::TracingOptions};

use super::utils::Test;

struct ApproxEq<'a>(&'a Value);

impl<'a> PartialEq for ApproxEq<'a> {
    fn eq(&self, other: &Self) -> bool {
        use Value as V;
        match (self.0, other.0) {
            (V::Null, V::Null) => true,
            (V::Bool(a), V::Bool(b)) => a == b,
            (V::String(a), V::String(b)) => a == b,
            (V::Number(a), V::Number(b)) => {
                f64::abs(a.as_f64().unwrap() - b.as_f64().unwrap()) < 1e-6
            }
            (V::Object(a), V::Object(b)) => {
                let a = a
                    .iter()
                    .map(|(k, v)| (k, ApproxEq(v)))
                    .collect::<HashMap<_, _>>();
                let b = b
                    .iter()
                    .map(|(k, v)| (k, ApproxEq(v)))
                    .collect::<HashMap<_, _>>();
                a == b
            }
            (V::Array(a), V::Array(b)) => {
                a.len() == b.len() && std::iter::zip(a, b).all(|(a, b)| ApproxEq(a) == ApproxEq(b))
            }
            _ => false,
        }
    }
}

impl<'a> std::fmt::Debug for ApproxEq<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Test {
    pub fn deserialize_json(self, items: &Value) -> Self {
        if self.impls.arrow {
            let fields = self.get_arrow_fields();
            let roundtripped: Value = crate::from_arrow(
                &fields,
                self.arrays
                    .arrow
                    .as_ref()
                    .expect("Deserialization requires known arrow arrays"),
            )
            .expect("Failed arrow deserialization");
            assert_eq!(ApproxEq(&roundtripped), ApproxEq(items));
        }

        if self.impls.arrow2 {
            let fields = self.get_arrow2_fields();
            let roundtripped: Value = crate::from_arrow2(
                &fields,
                self.arrays
                    .arrow2
                    .as_ref()
                    .expect("Deserialization requires known arrow2 arrays"),
            )
            .expect("Failed arrow2 deserialization");
            assert_eq!(ApproxEq(&roundtripped), ApproxEq(items));
        }

        self
    }
}

#[test]
fn serde_json_example() {
    let items = json!([{ "a": 1, "b": 2 }, { "a": 3, "b": 4 }]);
    Test::new()
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_mixed_ints() {
    let items = json!([{ "a": 1, "b": -2 }, { "a": 3.0, "b": 4 }]);
    Test::new()
        .trace_schema_from_samples(&items, TracingOptions::default().coerce_numbers(true))
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_mixed_fixed_schema() {
    let items = json!([
        { "a": 1, "b": -2 },
        { "a": 3.0, "b": 4 },
    ]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "F64"},
            {"name": "b", "data_type": "I64"},
        ]))
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_mixed_fixed_schema_nullable() {
    let items = json!([
        { "a": 1, "b": -2, "c": "hello", "d": null },
        { "a": null, "b": 4, "c": null, "d": true },
        { "a": 3.0, "b": null, "c": "world", "d": false },
    ]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "F64", "nullable": true},
            {"name": "b", "data_type": "I64", "nullable": true},
            {"name": "c", "data_type": "Utf8", "nullable": true},
            {"name": "d", "data_type": "Bool", "nullable": true},
        ]))
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_mixed_fixed_schema_outer_array() {
    let items = json!([{ "a": 1, "b": -2 }, { "a": 3.0, "b": 4 }]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "F64"},
            {"name": "b", "data_type": "I64"},
        ]))
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_strings() {
    let items = json!([{ "a": "hello", "b": "foo" }, { "a": "world", "b": "bar" }]);
    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "Utf8"},
            {"name": "b", "data_type": "Utf8"},
        ]))
        .serialize(&items)
        .deserialize_json(&items);
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
        .serialize(&items)
        .deserialize_json(&items);
}

#[test]
fn serde_json_nullable_strings_non_nullable_field() {
    let items = json!([{ "a": "hello" }, { "a": null }]);

    let mut test = Test::new().with_schema(json!([
        {"name": "a", "data_type": "Utf8"},
    ]));

    let res = test.try_serialize_arrow(&items);
    assert_error_contains(&res, "Cannot push null for non-nullable array");
    assert_error_contains(&res, "field: \"$.a\"");

    let res = test.try_serialize_arrow2(&items);
    assert_error_contains(&res, "Cannot push null for non-nullable array");
    assert_error_contains(&res, "field: \"$.a\"");
}
