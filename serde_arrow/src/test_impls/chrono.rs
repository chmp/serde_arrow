use super::utils::Test;
use crate::{schema::TracingOptions, utils::Item};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[test]
fn utc_as_str() {
    let items = [
        Item(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
        Item(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn naive_as_str() {
    let items = [
        Item(NaiveDateTime::from_timestamp_millis(1662921288000).unwrap()),
        Item(NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn utc_as_date64() {
    let items = [
        Item(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
        Item(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "UtcStrAsDate64",
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn naive_as_date64() {
    let items = [
        Item(NaiveDateTime::from_timestamp_millis(1662921288000).unwrap()),
        Item(NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "NaiveStrAsDate64",
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn i32_as_date32() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        item: i32,
    }

    let items = [
        T { item: i32::MIN },
        T { item: 0 },
        T { item: 100 },
        T { item: i32::MAX },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date32",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn i64_as_time64_nanoseconds() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        item: i64,
    }

    let items = [
        T { item: i64::MIN },
        T { item: 0 },
        T { item: 100 },
        T { item: i64::MAX },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn i64_as_time64_microseconds() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        item: i64,
    }

    let items = [
        T { item: i64::MIN },
        T { item: 0 },
        T { item: 100 },
        T { item: i64::MAX },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Microseconds)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

// TODO: remove the restriction (if it's only i64 <-> i64 there is no need to restrict the impl)
#[test]
#[should_panic]
fn i64_as_time64_millisecond() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        item: i64,
    }

    let items = [
        T { item: i64::MIN },
        T { item: 0 },
        T { item: 100 },
        T { item: i64::MAX },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

// TODO: remove the restriction (if it's only i64 <-> i64 there is no need to restrict the impl)
#[test]
#[should_panic]
fn i64_as_time64_second() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        item: i64,
    }

    let items = [
        T { item: i64::MIN },
        T { item: 0 },
        T { item: 100 },
        T { item: i64::MAX },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn utc_as_date64_as_millis() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct T {
        #[serde(with = "chrono::serde::ts_milliseconds")]
        item: DateTime<Utc>,
    }

    let items = [
        T {
            item: Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap(),
        },
        T {
            item: Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap(),
        },
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn utc_str_as_date64_as_timestamp() {
    let items = [
        Item(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
        Item(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Timestamp(Millisecond, Some(\"Utc\"))",
            "strategy": "UtcStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn naive_as_timestamp() {
    let items = [
        Item(NaiveDateTime::from_timestamp_millis(1662921288000).unwrap()),
        Item(NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type":
            "Timestamp(Millisecond, None)",
            "strategy": "NaiveStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn utc_as_date64_tracing_string_only() {
    let items = [
        Item(String::from("2015-09-18T23:56:04Z")),
        Item(String::from("2023-08-14T17:00:04Z")),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "UtcStrAsDate64",
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn utc_as_date64_tracing_string_nullable() {
    let items = [
        Item(Some(String::from("2015-09-18T23:56:04Z"))),
        Item(None),
        Item(Some(String::from("2023-08-14T17:00:04Z"))),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "UtcStrAsDate64",
            "nullable": true,
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn utc_as_date64_tracing_string_only_with_invalid() {
    let items = [
        Item(String::from("2015-09-18T23:56:04Z")),
        Item(String::from("2023-08-14T17:00:04Z")),
        Item(String::from("not a date")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item",  "data_type": "LargeUtf8"}]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn naive_as_date64_tracing_string_only() {
    let items = [
        Item(String::from("2015-09-18T23:56:04")),
        Item(String::from("2023-08-14T17:00:04")),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "NaiveStrAsDate64",
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn naive_as_date64_tracing_string_nullable() {
    let items = [
        Item(Some(String::from("2015-09-18T23:56:04"))),
        Item(None),
        Item(Some(String::from("2023-08-14T17:00:04"))),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
            "strategy": "NaiveStrAsDate64",
            "nullable": true,
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, true, false]]);
}

#[test]
fn naive_as_date64_tracing_string_with_invalid() {
    let items = [
        Item(String::from("2015-09-18T23:56:04")),
        Item(String::from("2023-08-14T17:00:04")),
        Item(String::from("not a date")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn incompatible_date_formats_tracing() {
    let items = [
        Item(String::from("2015-09-18T23:56:04")),
        Item(String::from("2023-08-14T17:00:04Z")),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}
