use serde_json::json;

use crate::{internal::utils::array_ext::bytes_view, utils::Item};

use super::utils::Test;

#[test]
fn non_nullable() {
    let items = &[
        Item(String::from("foo")),
        Item(String::from("bar")),
        Item(String::from("a long string")),
        Item(String::from("an even longer string")),
        Item(String::from("baz")),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([
            {"name": "item", "data_type": "Utf8View"},
        ]))
        .serialize(items)
        .also(|test| {
            let array = test.arrays.marrow.as_ref().unwrap().get(0).unwrap();
            let marrow::array::Array::Utf8View(array) = array else {
                panic!("expected utf8 array");
            };
            assert_eq!(
                array.data,
                &[
                    bytes_view::pack_inline(b"foo"),
                    bytes_view::pack_inline(b"bar"),
                    bytes_view::pack_extern(b"a long string", 0, 0),
                    bytes_view::pack_extern(b"an even longer string", 0, b"a long string".len()),
                    bytes_view::pack_inline(b"baz"),
                ],
            );
            assert_eq!(array.buffers[0], b"a long stringan even longer string");
        });
}

#[test]
fn nullable() {
    let items = &[
        Item(Some(String::from("foo"))),
        Item(None),
        Item(None),
        Item(Some(String::from("an even longer string"))),
        Item(Some(String::from("baz"))),
    ];

    Test::new()
        .skip_arrow2()
        .with_schema(json!([
            {"name": "item", "data_type": "Utf8View", "nullable": true},
        ]))
        .serialize(items)
        .also(|test| {
            let array = test.arrays.marrow.as_ref().unwrap().get(0).unwrap();
            let marrow::array::Array::Utf8View(array) = array else {
                panic!("expected utf8 array");
            };
            assert_eq!(
                array.data,
                &[
                    bytes_view::pack_inline(b"foo"),
                    bytes_view::pack_inline(b""),
                    bytes_view::pack_inline(b""),
                    bytes_view::pack_extern(b"an even longer string", 0, 0),
                    bytes_view::pack_inline(b"baz"),
                ],
            );

            assert_eq!(array.buffers[0], b"an even longer string");
        });
}
