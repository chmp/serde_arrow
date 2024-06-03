use super::utils::Test;
use crate::{schema::TracingOptions, utils::Item};

use serde_json::json;

#[test]
fn tracing() {
    let items = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("a")),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Dictionary",
            "children": [
                {"name": "key", "data_type": "U32"},
                {"name": "value", "data_type": "LargeUtf8"},
            ]
        }]))
        .trace_schema_from_samples(
            &items,
            TracingOptions::new().string_dictionary_encoding(true),
        )
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn examples() {
    let items = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("a")),
    ];

    for index_ty in ["I8", "I16", "I32", "I64", "U8", "U16", "U32", "U64"] {
        for value_ty in ["Utf8", "LargeUtf8"] {
            Test::new()
                .with_schema(json!([{
                    "name": "item",
                    "data_type": "Dictionary",
                    "children": [
                        {"name": "key", "data_type": index_ty},
                        {"name": "value", "data_type": value_ty},
                    ]
                }]))
                .serialize(&items)
                .deserialize(&items);
        }
    }
}

#[test]
fn examples_nullable() {
    let items = [
        Item(Some(String::from("a"))),
        Item(None),
        Item(Some(String::from("a"))),
    ];

    for index_ty in ["I8", "I16", "I32", "I64", "U8", "U16", "U32", "U64"] {
        for value_ty in ["Utf8", "LargeUtf8"] {
            Test::new()
                .with_schema(json!([{
                    "name": "item",
                    "data_type": "Dictionary",
                    "nullable": true,
                    "children": [
                        {"name": "key", "data_type": index_ty},
                        {"name": "value", "data_type": value_ty},
                    ]
                }]))
                .serialize(&items)
                .deserialize(&items);
        }
    }
}
