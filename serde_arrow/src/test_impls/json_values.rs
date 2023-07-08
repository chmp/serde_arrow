use super::macros::test_serialize_into_arrays;

test_serialize_into_arrays!(
    test_name = serde_json,
    values = vec![json!({ "a": 1, "b": 2 }), json!({ "a": 3, "b": 4 }),],
    define = {
        use serde_json::json;
    },
);

test_serialize_into_arrays!(
    test_name = serde_json_mixed_ints,
    tracing_options = TracingOptions::default().coerce_numbers(true),
    values = vec![json!({ "a": 1, "b": -2 }), json!({ "a": 3.0, "b": 4 }),],
    define = {
        use serde_json::json;
    },
);
