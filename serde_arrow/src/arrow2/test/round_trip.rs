// use the deprecated chrono API for now
#![allow(deprecated)]

use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::utils::{collect_events_from_array, field};
use crate::{
    arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
    impls::arrow2::{
        array::PrimitiveArray,
        datatypes::{DataType, Field},
    },
    internal::{event::Event, schema::Strategy},
};

/// Test that dates as RFC 3339 strings are correctly handled
#[test]
fn dtype_date64_naive_str() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        val: NaiveDateTime,
    }

    let records: &[Record] = &[
        Record {
            val: NaiveDateTime::from_timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: NaiveDateTime::from_timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records, Default::default()).unwrap();

    let val_field = fields.iter_mut().find(|field| field.name == "val").unwrap();
    val_field.data_type = DataType::Date64;
    val_field.metadata = Strategy::NaiveStrAsDate64.into();

    println!("{fields:?}");

    let arrays = serialize_into_arrays(&fields, records).unwrap();

    assert_eq!(arrays.len(), 1);

    let actual = arrays[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    let expected = PrimitiveArray::<i64>::from_slice([12_000 * 60 * 60 * 24, 9_000 * 60 * 60 * 24]);

    assert_eq!(actual, &expected);

    let events = collect_events_from_array(&fields, &arrays).unwrap();
    let expected_events = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-13T00:00:00").to_static(),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-10T00:00:00").to_static(),
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected_events);

    let round_tripped: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(round_tripped, records);
}

#[test]
fn dtype_date64_str() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        val: DateTime<Utc>,
    }

    let records: &[Record] = &[
        Record {
            val: Utc.timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: Utc.timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records, Default::default()).unwrap();
    let val_field = fields.iter_mut().find(|field| field.name == "val").unwrap();
    val_field.data_type = DataType::Date64;
    val_field.metadata = Strategy::UtcStrAsDate64.into();

    let arrays = serialize_into_arrays(&fields, records).unwrap();

    assert_eq!(arrays.len(), 1);

    let actual = arrays[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    let expected = PrimitiveArray::<i64>::from_slice([12_000 * 60 * 60 * 24, 9_000 * 60 * 60 * 24]);

    assert_eq!(actual, &expected);

    let events = collect_events_from_array(&fields, &arrays).unwrap();
    let expected_events = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-13T00:00:00Z").to_static(),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-10T00:00:00Z").to_static(),
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected_events);

    let round_tripped: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(round_tripped, records);
}

#[test]
fn nested_list_structs() {
    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Item {
        a: Vec<Inner>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Inner {
        a: i8,
        b: i32,
    }

    let items = vec![
        Item {
            a: vec![Inner { a: 0, b: 1 }, Inner { a: 2, b: 3 }],
        },
        Item { a: vec![] },
        Item {
            a: vec![Inner { a: 4, b: 5 }],
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();

    let expected_fields = vec![field::large_list(
        "a",
        false,
        field::r#struct(
            "element",
            false,
            [field::int8("a", false), field::int32("b", false)],
        ),
    )];
    assert_eq!(fields, expected_fields);

    let values = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_array: Vec<Item> = deserialize_from_arrays(&fields, &values).unwrap();
    assert_eq!(items_from_array, items);
}

#[test]
fn nested_structs_lists_lists() {
    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Item {
        a: A,
        b: u16,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct A {
        c: Vec<C>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct C {
        d: Vec<u8>,
    }

    let items = vec![
        Item {
            a: A {
                c: vec![C { d: vec![0, 1] }, C { d: vec![2] }],
            },
            b: 3,
        },
        Item {
            a: A { c: vec![] },
            b: 4,
        },
        Item {
            a: A {
                c: vec![C { d: vec![] }],
            },
            b: 5,
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected_fields = vec![
        field::r#struct(
            "a",
            false,
            [field::large_list(
                "c",
                false,
                field::r#struct(
                    "element",
                    false,
                    [field::large_list(
                        "d",
                        false,
                        field::uint8("element", false),
                    )],
                ),
            )],
        ),
        field::uint16("b", false),
    ];
    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn byte_arrays() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Vec<u8>,
    }

    let items = vec![
        Item {
            a: b"hello".to_vec(),
        },
        Item {
            a: b"world!".to_vec(),
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn new_type_structs() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: U64,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct U64(u64);

    let items = vec![Item { a: U64(21) }, Item { a: U64(42) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

macro_rules! define_wrapper_test {
    ($test_name:ident, $struct_name:ident, $init:expr) => {
        #[test]
        fn $test_name() {
            #[derive(Debug, PartialEq, Serialize, Deserialize)]
            struct $struct_name {
                a: u32,
            }

            let items = $init;

            let fields = serialize_into_fields(&items, Default::default()).unwrap();
            let arrays = serialize_into_arrays(&fields, &items).unwrap();

            let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

            assert_eq!(items_from_arrays, items);
        }
    };
}

define_wrapper_test!(
    wrapper_outer_vec,
    Item,
    vec![Item { a: 21 }, Item { a: 42 }]
);
define_wrapper_test!(
    wrapper_outer_slice,
    Item,
    [Item { a: 21 }, Item { a: 42 }].as_slice()
);
define_wrapper_test!(wrapper_const_array, Item, [Item { a: 21 }, Item { a: 42 }]);

#[test]
fn wrapper_tuple() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: u32,
    }

    let items = (Item { a: 21 }, Item { a: 42 });

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    let items = vec![items.0, items.1];
    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_string_as_large_utf8() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: String,
    }

    let items = vec![
        Item {
            a: String::from("hello"),
        },
        Item {
            a: String::from("world"),
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected_fields = vec![Field::new("a", DataType::LargeUtf8, false)];

    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_string_as_utf8() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: String,
    }

    let items = vec![
        Item {
            a: String::from("hello"),
        },
        Item {
            a: String::from("world"),
        },
    ];

    let fields = vec![Field::new("a", DataType::Utf8, false)];

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_unit() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: (),
    }

    let items = vec![Item { a: () }, Item { a: () }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected_fields = vec![Field::new("a", DataType::Null, true)];

    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_tuple() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: (u8, u8),
    }

    let items = vec![Item { a: (0, 1) }, Item { a: (2, 3) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_tuple_struct() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Inner,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Inner(u8, u8);

    let items = vec![Item { a: Inner(0, 1) }, Item { a: Inner(2, 3) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_struct_with_options() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Inner,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Inner {
        foo: Option<u8>,
        bar: u32,
    }

    let items = vec![
        Item {
            a: Inner { foo: None, bar: 13 },
        },
        Item {
            a: Inner {
                foo: Some(0),
                bar: 21,
            },
        },
        Item {
            a: Inner {
                foo: Some(1),
                bar: 42,
            },
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_complex_benchmark_example() {
    use rand::{
        distributions::{Distribution, Standard, Uniform},
        Rng,
    };

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Item {
        string: String,
        points: Vec<(f32, f32)>,
        float: Float,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Float {
        F32(f32),
        F64(f64),
    }

    impl Item {
        fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
            let n_string = Uniform::new(1, 20).sample(rng);
            let n_points = Uniform::new(1, 20).sample(rng);
            let is_f32: bool = Standard.sample(rng);

            Self {
                string: (0..n_string)
                    .map(|_| -> char { Standard.sample(rng) })
                    .collect(),
                points: (0..n_points)
                    .map(|_| (Standard.sample(rng), Standard.sample(rng)))
                    .collect(),
                float: if is_f32 {
                    Float::F32(Standard.sample(rng))
                } else {
                    Float::F64(Standard.sample(rng))
                },
            }
        }
    }

    let mut rng = rand::thread_rng();
    let items: Vec<Item> = (0..10).map(|_| Item::random(&mut rng)).collect();

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let round_tripped: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items, round_tripped);
}

#[test]
fn test_maps_with_missing_items() {
    let mut items: Vec<HashMap<String, i32>> = Vec::new();
    let mut item = HashMap::new();
    item.insert(String::from("a"), 0);
    item.insert(String::from("b"), 1);
    items.push(item);

    let mut item = HashMap::new();
    item.insert(String::from("a"), 2);
    item.insert(String::from("c"), 3);
    items.push(item);

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let actual: Vec<HashMap<String, Option<i32>>> =
        deserialize_from_arrays(&fields, &arrays).unwrap();

    // Note: missing items are serialized as null, therefore the deserialized
    // type must support them
    let mut expected: Vec<HashMap<String, Option<i32>>> = Vec::new();
    let mut item = HashMap::new();
    item.insert(String::from("a"), Some(0));
    item.insert(String::from("b"), Some(1));
    item.insert(String::from("c"), None);
    expected.push(item);

    let mut item = HashMap::new();
    item.insert(String::from("a"), Some(2));
    item.insert(String::from("b"), None);
    item.insert(String::from("c"), Some(3));
    expected.push(item);

    assert_eq!(actual, expected);
}
