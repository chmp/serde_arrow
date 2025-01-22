use serde_bytes::{ByteBuf, Bytes};
use serde_json::json;

use crate::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn example_as_list() {
    let items = [
        Item(ByteBuf::from(b"foo")),
        Item(ByteBuf::from(b"bar")),
        Item(ByteBuf::from(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [
                {"name": "element", "data_type": "U8"},
            ],
        }]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_as_binary() {
    let items = [
        Item(ByteBuf::from(b"foo")),
        Item(ByteBuf::from(b"bar")),
        Item(ByteBuf::from(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Binary"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_large_binary() {
    let items = [
        Item(ByteBuf::from(b"foo")),
        Item(ByteBuf::from(b"bar")),
        Item(ByteBuf::from(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary"}]))
        .trace_schema_from_type::<Item<ByteBuf>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_large_binary_nullable() {
    let items = [
        Item(Some(ByteBuf::from(b"foo"))),
        Item(None),
        Item(Some(ByteBuf::from(b"baz"))),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary", "nullable": true}]))
        .trace_schema_from_type::<Item<Option<ByteBuf>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn example_vec_as_large_binary() {
    let items = [
        Item(b"foo".to_vec()),
        Item(b"bar".to_vec()),
        Item(b"baz".to_vec()),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_vec_as_large_binary_nullable() {
    let items = [
        Item(Some(b"foo".to_vec())),
        Item(None),
        Item(Some(b"baz".to_vec())),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary", "nullable": true}]))
        .serialize(&items)
        .check_nulls(&[&[false, true, false]])
        .deserialize(&items);
}

#[test]
fn example_vec_i64_as_large_binary() {
    let items = [Item(vec![1_i64, 2, 3]), Item(vec![128, 255, 75])];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_borrowed() {
    let items = [
        Item(Bytes::new(b"foo")),
        Item(Bytes::new(b"bar")),
        Item(Bytes::new(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary"}]))
        .trace_schema_from_type::<Item<ByteBuf>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize_borrowed(&items);
}

#[test]
fn example_borrowed_nullable() {
    let items = [
        Item(Some(Bytes::new(b"foo"))),
        Item(None),
        Item(Some(Bytes::new(b"baz"))),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeBinary", "nullable": true}]))
        .trace_schema_from_type::<Item<Option<&Bytes>>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .check_nulls(&[&[false, true, false]])
        .deserialize_borrowed(&items);
}

/// test that byte can be deserialized from string arrays
mod deserialize_bytes_from_strings {
    use super::*;

    fn items() -> (Vec<Item<String>>, Vec<Item<ByteBuf>>) {
        let input = vec![
            Item(String::from("foo")),
            Item(String::from("bar")),
            Item(String::from("baz")),
        ];
        let output = vec![
            Item(ByteBuf::from(b"foo")),
            Item(ByteBuf::from(b"bar")),
            Item(ByteBuf::from(b"baz")),
        ];
        (input, output)
    }

    #[test]
    fn as_large_utf8() {
        let (input, output) = items();

        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
            .serialize(&input)
            .deserialize(&output);
    }

    #[test]
    fn as_utf8() {
        let (input, output) = items();

        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
            .serialize(&input)
            .deserialize(&output);
    }
}
