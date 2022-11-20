use arrow2::array::PrimitiveArray;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
    base::Event,
    schema::{lookup_field_mut, GenericField, Strategy},
    test::arrow2::utils::{
        access::{self, Value},
        collect_events_from_array, field,
    },
};

/// Test that dates as RFC 3339 strings are correctly handled
#[test]
fn dtype_date64_naive_str() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        val: NaiveDateTime,
    }

    let records = &[
        Record {
            val: NaiveDateTime::from_timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: NaiveDateTime::from_timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records).unwrap();
    lookup_field_mut(&mut fields, ("val",))
        .unwrap()
        .configure_serde_arrow_strategy(Strategy::NaiveDateTimeStr)
        .unwrap();

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
        Event::StartMap,
        Event::Key("val").to_static(),
        Event::Str("1970-01-13T00:00:00").to_static(),
        Event::EndMap,
        Event::StartMap,
        Event::Key("val").to_static(),
        Event::Str("1970-01-10T00:00:00").to_static(),
        Event::EndMap,
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

    let records = &[
        Record {
            val: Utc.timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: Utc.timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records).unwrap();
    lookup_field_mut(&mut fields, ("val",))
        .unwrap()
        .configure_serde_arrow_strategy(Strategy::UtcDateTimeStr)
        .unwrap();

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
        Event::StartMap,
        Event::Key("val").to_static(),
        Event::Str("1970-01-13T00:00:00Z").to_static(),
        Event::EndMap,
        Event::StartMap,
        Event::Key("val").to_static(),
        Event::Str("1970-01-10T00:00:00Z").to_static(),
        Event::EndMap,
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

    let fields = serialize_into_fields(&items).unwrap();

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

    // the overall structure
    assert_eq!(
        access::get_value(values[0].as_ref(), ()).unwrap(),
        Value::List(3)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (0,)).unwrap(),
        Value::Struct(2, 2)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (1,)).unwrap(),
        Value::Struct(0, 2)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (2,)).unwrap(),
        Value::Struct(1, 2)
    );

    // random examples
    assert_eq!(
        access::get_value(values[0].as_ref(), (0, "b", 0)).unwrap(),
        Value::Int32(1)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (0, "b", 1)).unwrap(),
        Value::Int32(3)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (2, "a", 0)).unwrap(),
        Value::Int8(4)
    );
    assert_eq!(
        access::get_value(values[0].as_ref(), (2, "b", 0)).unwrap(),
        Value::Int32(5)
    );

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

    let fields = serialize_into_fields(&items).unwrap();
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
