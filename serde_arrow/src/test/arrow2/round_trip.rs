use arrow2::array::PrimitiveArray;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{
        collect_events_from_array, deserialize_from_arrays, serialize_into_arrays,
        serialize_into_fields,
    },
    base::Event, generic::schema::{Strategy, configure_serde_arrow_strategy},
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
    configure_serde_arrow_strategy(&mut fields, ("val",), Strategy::NaiveDateTimeStr).unwrap();

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
        Event::owned_key("val"),
        Event::string("1970-01-13T00:00:00"),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("val"),
        Event::string("1970-01-10T00:00:00"),
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
    configure_serde_arrow_strategy(&mut fields, ("val",), Strategy::DateTimeStr).unwrap();

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
        Event::owned_key("val"),
        Event::string("1970-01-13T00:00:00Z"),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("val"),
        Event::string("1970-01-10T00:00:00Z"),
        Event::EndMap,
        Event::EndSequence,
    ];
    assert_eq!(events, expected_events);

    let round_tripped: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(round_tripped, records);
}
