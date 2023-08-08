use super::macros::test_generic;

test_generic!(
    fn serde_json_example() {
        use serde_json::json;

        let tracing_options = TracingOptions::default();

        let items = vec![json!({ "a": 1, "b": 2 }), json!({ "a": 3, "b": 4 })];
        let fields = serialize_into_fields(&items, tracing_options).unwrap();
        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn serde_json_mixed_ints() {
        use serde_json::json;

        let tracing_options = TracingOptions::default().coerce_numbers(true);

        let items = vec![json!({ "a": 1, "b": -2 }), json!({ "a": 3.0, "b": 4 })];
        let fields = serialize_into_fields(&items, tracing_options).unwrap();
        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        drop(arrays);
    }
);
