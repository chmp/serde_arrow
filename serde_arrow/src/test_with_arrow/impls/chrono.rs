use core::str;

use super::utils::Test;
use crate::{
    internal::{
        arrow::DataType,
        testing::{assert_error_contains, ArrayAccess},
        utils::value,
    },
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::Item,
    ArrayBuilder,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[test]
fn trace_from_type_does_not_work() {
    let res = SerdeArrowSchema::from_type::<Item<DateTime<Utc>>>(TracingOptions::default());
    assert_error_contains(&res, "premature end of input");
}

#[test]
fn temporal_formats() {
    assert_error_contains(
        &value::transmute::<DateTime<Utc>>("2023-12-01T12:22:33[UTC]"),
        "",
    );

    assert_eq!(
        value::transmute::<String>(NaiveDate::from_ymd_opt(-10, 10, 30).unwrap()).unwrap(),
        "-0010-10-30"
    );
    assert_eq!(
        value::transmute::<NaiveDate>("-0010-10-30").unwrap(),
        NaiveDate::from_ymd_opt(-10, 10, 30).unwrap()
    );
    // chrono also supports 6 digit dates, jiff requires them
    assert_eq!(
        value::transmute::<NaiveDate>("-000010-10-30").unwrap(),
        NaiveDate::from_ymd_opt(-10, 10, 30).unwrap()
    );
}

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
    #[allow(deprecated)]
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
fn utc_as_date64_without_strategy() {
    let items = [
        Item(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
        Item(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()),
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
fn naive_as_date64() {
    #[allow(deprecated)]
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
fn date32_chrono() {
    let items = [
        Item(NaiveDate::from_ymd_opt(2024, 3, 17).unwrap()),
        Item(NaiveDate::from_ymd_opt(1700, 12, 24).unwrap()),
        Item(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date32",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn time_i64() {
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
    let nulls: &[&[bool]] = &[&[false, false, false, false]];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Date64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Microsecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Microsecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Duration(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(nulls);
}

#[test]
fn time_i32() {
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
            "data_type": "Time32(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time32(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false, false]]);
}

#[test]
fn time_chrono() {
    let items = [
        Item(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        Item(NaiveTime::from_hms_opt(23, 31, 12).unwrap()),
        Item(NaiveTime::from_hms_opt(3, 2, 58).unwrap()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time32(Second)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time32(Millisecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Microsecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Time64(Nanosecond)",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn time64_type_invalid_units() {
    // Note: the arrow docs state: that the time unit "[m]ust be either
    // microseconds or nanoseconds."

    assert_error_contains(
        &SerdeArrowSchema::from_value(&json!([{
            "name": "item",
            "data_type": "Time64(Millisecond)",
        }])),
        "Error: Time64 field must have Microsecond or Nanosecond unit",
    );
    assert_error_contains(
        &SerdeArrowSchema::from_value(&json!([{
            "name": "item",
            "data_type": "Time64(Second)",
        }])),
        "Error: Time64 field must have Microsecond or Nanosecond unit",
    );

    assert_error_contains(
        &SerdeArrowSchema::from_value(&json!([{
            "name": "item",
            "data_type": "Time32(Microsecond)",
        }])),
        "Error: Time32 field must have Second or Millisecond unit",
    );
    assert_error_contains(
        &SerdeArrowSchema::from_value(&json!([{
            "name": "item",
            "data_type": "Time32(Nanosecond)",
        }])),
        "Error: Time32 field must have Second or Millisecond unit",
    );
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
            "data_type": "Timestamp(Second, Some(\"Utc\"))",
            "strategy": "UtcStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Timestamp(Millisecond, Some(\"Utc\"))",
            "strategy": "UtcStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Timestamp(Microsecond, Some(\"Utc\"))",
            "strategy": "UtcStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Timestamp(Nanosecond, Some(\"Utc\"))",
            "strategy": "UtcStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn naive_as_timestamp() {
    // The 001 in the end makes sure that we handle fractional seconds correctly
    // in both positive and negative timestamps.
    let items = [
        Item(
            DateTime::from_timestamp_millis(1662921288001)
                .unwrap()
                .naive_utc(),
        ),
        Item(
            DateTime::from_timestamp_millis(-2208936075001)
                .unwrap()
                .naive_utc(),
        ),
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

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type":
            "Timestamp(Microsecond, None)",
            "strategy": "NaiveStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type":
            "Timestamp(Nanosecond, None)",
            "strategy": "NaiveStrAsDate64",
        }]))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false]]);
}

// Handle seconds as a special case, since this encoding does not support
// fractional seconds.
#[test]
fn naive_as_timestamp_seconds() {
    let items = [
        Item(
            DateTime::from_timestamp_millis(1662921288000)
                .unwrap()
                .naive_utc(),
        ),
        Item(
            DateTime::from_timestamp_millis(-2208936075000)
                .unwrap()
                .naive_utc(),
        ),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type":
            "Timestamp(Second, None)",
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

#[test]
fn duration_example_as_string_details() {
    let items = [
        Item(NaiveTime::from_hms_opt(12, 10, 42).unwrap()),
        Item(NaiveTime::from_hms_opt(22, 10, 00).unwrap()),
        Item(NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap()),
    ];

    let schema = SerdeArrowSchema::from_samples(&items, TracingOptions::default()).unwrap();
    assert_eq!(schema.fields[0].data_type, DataType::LargeUtf8);

    let mut builder = ArrayBuilder::new(schema).unwrap();
    builder.extend(&items).unwrap();

    let arrays = builder.build_arrays().unwrap();
    let [array] = arrays.try_into().unwrap();

    assert_eq!(array.get_utf8(0).unwrap(), Some("12:10:42"));
    assert_eq!(array.get_utf8(1).unwrap(), Some("22:10:00"));
    assert_eq!(array.get_utf8(2).unwrap(), Some("23:59:59.999"));
}

mod naive_time {
    use super::*;

    fn items() -> Vec<Item<NaiveTime>> {
        vec![
            Item(NaiveTime::from_hms_opt(12, 10, 42).unwrap()),
            Item(NaiveTime::from_hms_opt(22, 10, 00).unwrap()),
            Item(NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap()),
        ]
    }

    #[test]
    fn as_large_utf8() {
        let items = &items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
            .trace_schema_from_samples(&items, TracingOptions::default())
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_utf8() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time64() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time64(Nanosecond)"}]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }
}

mod naive_date {
    use super::*;

    fn items() -> Vec<Item<NaiveDate>> {
        vec![
            Item(NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()),
            Item(NaiveDate::from_ymd_opt(-10, 10, 30).unwrap()),
            Item(NaiveDate::from_ymd_opt(-1000, 9, 23).unwrap()),
        ]
    }

    #[test]
    fn as_large_utf8() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
            .trace_schema_from_samples(&items, TracingOptions::default())
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_utf8() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_date32() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Date32"}]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }
}
