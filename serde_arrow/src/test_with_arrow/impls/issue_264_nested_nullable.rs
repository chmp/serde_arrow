use super::utils::Test;

use serde::Serialize;
use serde_json::json;

#[test]
fn nested_nullable() {
    #[derive(Serialize)]
    struct S {
        a: Option<T>,
    }

    #[derive(Serialize)]
    struct T {
        a: U,
    }

    #[derive(Serialize)]
    enum U {
        #[allow(unused)]
        A,
    }

    Test::new()
        .with_schema(
            &json!([{"name": "a", "data_type": "Struct", "nullable": true, "children": [
                {"name": "a", "data_type": "Struct", "children": [
                    {"name": "a", "data_type": "Dictionary", "children": [
                        {"name": "key", "data_type": "U32"},
                        {"name": "value", "data_type": "LargeUtf8"},
                    ]}
                ]}
            ]}]),
        )
        .serialize(&[S { a: None }]);

    Test::new()
        .with_schema(
            &json!([{"name": "a", "data_type": "Struct", "nullable": true, "children": [
                {"name": "a", "data_type": "Struct", "children": [
                    {"name": "a", "data_type": "Union", "children": [
                        {"name": "A", "data_type": "Null", "nullable": true},
                    ]}
                ]}
            ]}]),
        )
        .serialize(&[S { a: None }]);
}
