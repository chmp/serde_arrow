use super::utils::Test;

use serde::Serialize;
use serde_json::json;

mod enum_without_data {
    use super::*;

    #[derive(Serialize)]
    struct Outer {
        a: Option<Inner>,
    }

    #[derive(Serialize)]
    struct Inner {
        a: Enum,
    }

    #[derive(Serialize)]
    enum Enum {
        #[allow(unused)]
        A,
    }

    #[test]
    fn as_dictionary() {
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
            .serialize(&[Outer { a: None }]);
    }

    #[test]
    fn as_enum() {
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
            .serialize(&[Outer { a: None }]);
    }
}
