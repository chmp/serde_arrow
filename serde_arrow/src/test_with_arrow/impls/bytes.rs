use serde_bytes::{ByteBuf, Bytes};
use serde_json::json;

use crate::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn example() {
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
        .trace_schema_from_type::<Item<ByteBuf>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

// NOTE: borrowed binary data requires specialized Binary Deserializer
#[test]
#[ignore]
fn example_borrowed() {
    let items = [
        Item(Bytes::new(b"foo")),
        Item(Bytes::new(b"bar")),
        Item(Bytes::new(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeList",
            "children": [
                {"name": "element", "data_type": "U8"},
            ],
        }]))
        .trace_schema_from_type::<Item<ByteBuf>>(TracingOptions::default())
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize_borrowed(&items);
}
