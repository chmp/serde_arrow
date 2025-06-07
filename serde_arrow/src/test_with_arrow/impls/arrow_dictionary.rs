use super::utils::Test;
use crate::{
    internal::deserialization::{
        array_deserializer::ArrayDeserializer, random_access_deserializer::RandomAccessDeserializer,
    },
    schema::TracingOptions,
    utils::{Item, Items},
};

use marrow::{
    array::{Array, BytesArray, DictionaryArray, PrimitiveArray},
    datatypes::{DataType, Field},
    view::{DictionaryView, View},
};
use serde::Deserialize;
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

macro_rules! define_tests {
    ($mod_name:ident, $index_ty:expr, $value_ty:expr) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn examples() {
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
                            {"name": "key", "data_type": $index_ty},
                            {"name": "value", "data_type": $value_ty},
                        ]
                    }]))
                    .serialize(&items)
                    .deserialize(&items);
            }

            #[test]
            fn examples_nullable() {
                let items = [
                    Item(Some(String::from("a"))),
                    Item(None),
                    Item(Some(String::from("a"))),
                ];

                Test::new()
                    .with_schema(json!([{
                        "name": "item",
                        "data_type": "Dictionary",
                        "nullable": true,
                        "children": [
                            {"name": "key", "data_type": $index_ty},
                            {"name": "value", "data_type": $value_ty},
                        ]
                    }]))
                    .serialize(&items)
                    .deserialize(&items);
            }


        }
    };
}

define_tests!(i8_utf8, "I8", "Utf8");
define_tests!(i8_large_utf8, "I8", "LargeUtf8");
define_tests!(i16_utf8, "I16", "Utf8");
define_tests!(i16_large_utf8, "I16", "LargeUtf8");
define_tests!(i32_utf8, "I32", "Utf8");
define_tests!(i32_large_utf8, "I32", "LargeUtf8");
define_tests!(i64_utf8, "I64", "Utf8");
define_tests!(i64_large_utf8, "I64", "LargeUtf8");
define_tests!(u8_utf8, "U8", "Utf8");
define_tests!(u8_large_utf8, "U8", "LargeUtf8");
define_tests!(u16_utf8, "U16", "Utf8");
define_tests!(u16_large_utf8, "U16", "LargeUtf8");
define_tests!(u32_utf8, "U32", "Utf8");
define_tests!(u32_large_utf8, "U32", "LargeUtf8");
define_tests!(u64_utf8, "U64", "Utf8");
define_tests!(u64_large_utf8, "U64", "LargeUtf8");

#[test]
fn dictionary_deserializer_construction() {
    fn to_array<const N: usize, T: serde::Serialize>(
        data_type: DataType,
        nullable: bool,
        values: [T; N],
    ) -> marrow::array::Array {
        let fields = vec![Field {
            name: String::from("item"),
            data_type,
            nullable,
            ..Default::default()
        }];
        let arrays = crate::to_marrow(&fields, Items(values)).unwrap();
        arrays.into_iter().next().unwrap()
    }

    let array = Array::Dictionary(DictionaryArray {
        keys: Box::new(to_array(DataType::Int8, false, [1, 1, 0])),
        values: Box::new(to_array(DataType::Utf8, true, ["foo", "bar"])),
    });

    let deserializer = ArrayDeserializer::new(String::from("$"), None, array.as_view()).unwrap();

    assert_eq!(
        String::deserialize(deserializer.at(0)).unwrap(),
        String::from("bar")
    );
    assert_eq!(
        String::deserialize(deserializer.at(1)).unwrap(),
        String::from("bar")
    );
    assert_eq!(
        String::deserialize(deserializer.at(2)).unwrap(),
        String::from("foo")
    );
}
