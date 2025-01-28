use serde_bytes::ByteBuf;
use serde_json::json;

use crate::utils::Item;

use super::utils::Test;

#[test]
fn as_binary_view() {
    let items = [
        Item(ByteBuf::from(b"foo")),
        Item(ByteBuf::from(b"a very long string")),
        Item(ByteBuf::from(b"baz")),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{"name": "item", "data_type": "BinaryView"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn nullable_as_binary_view() {
    let items = [
        Item(Some(ByteBuf::from(b"foo"))),
        Item(None),
        Item(Some(ByteBuf::from(b"a very long string"))),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{"name": "item", "data_type": "BinaryView", "nullable": true}]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn vec_as_binary_view() {
    let items = [
        Item(b"foo".to_vec()),
        Item(b"a very long string".to_vec()),
        Item(b"baz".to_vec()),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{"name": "item", "data_type": "BinaryView"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn vec_of_nullable_as_binary_view() {
    let items = [
        Item(Some(b"foo".to_vec())),
        Item(None),
        Item(Some(b"a very long string".to_vec())),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([{"name": "item", "data_type": "BinaryView", "nullable": true}]))
        .serialize(&items)
        .check_nulls(&[&[false, true, false]])
        .deserialize(&items);
}
