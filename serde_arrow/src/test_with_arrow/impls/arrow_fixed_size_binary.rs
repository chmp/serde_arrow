use serde_bytes::{ByteBuf, Bytes};
use serde_json::json;

use crate::utils::Item;

use super::utils::Test;

#[test]
fn example_vec_as_fixed_size_binary() {
    let items = [
        Item(b"foo".to_vec()),
        Item(b"bar".to_vec()),
        Item(b"baz".to_vec()),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "FixedSizeBinary(3)"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_as_fixed_size_binary() {
    let items = [
        Item(ByteBuf::from(b"foo")),
        Item(ByteBuf::from(b"bar")),
        Item(ByteBuf::from(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "FixedSizeBinary(3)"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn example_borrowed_fixed_size_binary() {
    let items = [
        Item(Bytes::new(b"foo")),
        Item(Bytes::new(b"bar")),
        Item(Bytes::new(b"baz")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "FixedSizeBinary(3)"}]))
        .serialize(&items)
        .deserialize_borrowed(&items);
}
