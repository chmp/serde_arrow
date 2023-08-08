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

test_generic!(
    fn serde_json_mixed_fixed_schema() {
        use serde_json::json;

        let items = vec![json!({ "a": 1, "b": -2 }), json!({ "a": 3.0, "b": 4 })];

        let fields = vec![
            Field::try_from(&GenericField::new("a", GenericDataType::F64, false)).unwrap(),
            Field::try_from(&GenericField::new("b", GenericDataType::I64, false)).unwrap(),
        ];

        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn serde_json_mixed_fixed_schema_outer_array() {
        use serde_json::json;

        let items = json!([{ "a": 1, "b": -2 }, { "a": 3.0, "b": 4 }]);

        let fields = vec![
            Field::try_from(&GenericField::new("a", GenericDataType::F64, false)).unwrap(),
            Field::try_from(&GenericField::new("b", GenericDataType::I64, false)).unwrap(),
        ];

        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn serde_json_strings() {
        use serde_json::json;

        let items = json!([{ "a": "hello", "b": -2 }, { "a": "world", "b": 4 }]);

        let fields = vec![
            Field::try_from(&GenericField::new("a", GenericDataType::Utf8, false)).unwrap(),
            Field::try_from(&GenericField::new("b", GenericDataType::I64, false)).unwrap(),
        ];

        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn serde_json_nullable_strings_non_nullable_field() {
        use serde_json::json;

        let items = json!([{ "a": "hello", "b": -2 }, { "a": null, "b": 4 }]);

        let fields = vec![
            Field::try_from(&GenericField::new("a", GenericDataType::Utf8, false)).unwrap(),
            Field::try_from(&GenericField::new("b", GenericDataType::I64, false)).unwrap(),
        ];

        let Err(err) = serialize_into_arrays(&fields, &items) else {
            panic!("expected an error, but no error was raised");
        };

        let err = err.to_string();
        if !err.contains("PushUtf8 cannot accept Null") {
            panic!("Error did not contain \"PushUtf8 cannot accept Null\". Full error: {err}");
        }
    }
);
