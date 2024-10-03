use jiff::{
    civil::{date, time, Date, DateTime, Time},
    Span, Timestamp, Zoned,
};
use serde_json::json;

use crate::{
    internal::{
        schema::TracingOptions,
        testing::assert_error_contains,
        utils::{value, Item},
    },
    test_with_arrow::impls::utils::Test,
};

#[test]
fn string_repr_examples() {
    // date
    let obj = date(2023, 12, 31);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "2023-12-31");

    let obj = date(-10, 10, 30);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "-000010-10-30");
    assert_eq!(value::transmute::<Date>("-000010-10-30").unwrap(), obj);
    assert_error_contains(
        &value::transmute::<Date>("-0010-10-30"),
        "six digit integer",
    );

    // date time without time zone
    let obj = date(2023, 12, 31).at(18, 30, 0, 0);
    assert_eq!(
        value::transmute::<String>(&obj).unwrap(),
        "2023-12-31T18:30:00"
    );

    // date time with timezone
    let obj = date(2023, 12, 31).at(18, 30, 0, 0).intz("UTC").unwrap();
    assert_eq!(
        value::transmute::<String>(&obj).unwrap(),
        "2023-12-31T18:30:00+00:00[UTC]"
    );

    // time without fractional part
    let obj = time(16, 56, 42, 0);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "16:56:42");

    // time with fractional part
    let obj = time(16, 56, 42, 123_000_000);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "16:56:42.123");

    // day span
    let obj = Span::new().days(32);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "P32d");

    // year month span
    let obj = Span::new().years(4).months(7);
    assert_eq!(value::transmute::<String>(&obj).unwrap(), "P4y7m");
}

/// Test that the different reprs between chrono and jiff are compatible
#[test]
fn transmute_jiff_chrono() {
    // date
    let chrono = chrono::NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
    let jiff = date(2023, 12, 31);

    assert_eq!(value::transmute::<Date>(&chrono).unwrap(), jiff);
    assert_eq!(
        value::transmute::<chrono::NaiveDate>(&jiff).unwrap(),
        chrono
    );

    // time without fractional part
    let chrono = chrono::NaiveTime::from_hms_opt(19, 31, 22).unwrap();
    let jiff = time(19, 31, 22, 0);

    assert_eq!(value::transmute::<Time>(&chrono).unwrap(), jiff);
    assert_eq!(
        value::transmute::<chrono::NaiveTime>(&jiff).unwrap(),
        chrono
    );

    // time with fractional part
    let chrono = chrono::NaiveTime::from_hms_nano_opt(19, 31, 22, 123_456_789).unwrap();
    let jiff = time(19, 31, 22, 123_456_789);

    assert_eq!(value::transmute::<Time>(&chrono).unwrap(), jiff);
    assert_eq!(
        value::transmute::<chrono::NaiveTime>(&jiff).unwrap(),
        chrono
    );

    // date time
    let chrono = chrono::NaiveDate::from_ymd_opt(1234, 5, 6)
        .unwrap()
        .and_hms_opt(7, 8, 9)
        .unwrap();
    let jiff = date(1234, 5, 6).at(7, 8, 9, 0);

    assert_eq!(value::transmute::<DateTime>(&chrono).unwrap(), jiff);
    assert_eq!(
        value::transmute::<chrono::NaiveDateTime>(&jiff).unwrap(),
        chrono
    );

    // date times with timezone are not compatible
    let chrono = chrono::NaiveDate::from_ymd_opt(1234, 5, 6)
        .unwrap()
        .and_hms_opt(7, 8, 9)
        .unwrap()
        .and_utc();
    let jiff = date(1234, 5, 6).at(7, 8, 9, 0).intz("UTC").unwrap();

    assert_error_contains(&value::transmute::<Zoned>(&chrono), "");
    assert_error_contains(
        &value::transmute::<chrono::DateTime<chrono::Utc>>(&jiff),
        "",
    );

    // use timestamp
    let jiff_timestamp = jiff.timestamp();
    assert_eq!(
        value::transmute::<Timestamp>(&chrono).unwrap(),
        jiff_timestamp
    );
    assert_eq!(
        value::transmute::<chrono::DateTime<chrono::Utc>>(&jiff_timestamp).unwrap(),
        chrono
    );
}

#[test]
fn invalid_utc_formats() {
    assert_error_contains(&value::transmute::<Zoned>("2023-12-31T18:30:00+00:00"), "");
    assert_error_contains(&value::transmute::<Zoned>("2023-12-31T18:30:00Z"), "");
}

#[test]
fn empty_string_errors() {
    assert_error_contains(&value::transmute::<Date>(""), "found end of input");
    assert_error_contains(&value::transmute::<Time>(""), "found end of input");
    assert_error_contains(&value::transmute::<Zoned>(""), "found end of input");
}

mod time {
    use super::*;

    fn items() -> Vec<Item<Time>> {
        vec![
            Item(time(12, 0, 0, 0)),
            Item(time(23, 31, 12, 0)),
            Item(time(3, 2, 58, 0)),
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
    fn as_time32_second() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time32(Second)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time32_miliseconds() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time32(Millisecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time32_microseconds() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time64(Microsecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time64_nanosecond() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time64(Nanosecond)"}]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }
}

mod date {
    use super::*;

    fn items() -> Vec<Item<Date>> {
        vec![
            Item(date(1234, 5, 6)),
            Item(date(-10, 10, 30)),
            Item(date(2024, 10, 1)),
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

mod date_time {
    use super::*;

    fn items() -> Vec<Item<DateTime>> {
        vec![
            Item(date(2024, 10, 2).at(20, 26, 12, 0)),
            Item(date(-10, 10, 30).at(0, 0, 0, 0)),
            Item(date(-1000, 1, 12).at(23, 59, 59, 0)),
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
    fn as_timestamp_second() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Timestamp(Second, None)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_millisecond() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Timestamp(Millisecond, None)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_microsecond() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Timestamp(Microsecond, None)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_nanosecond() {
        // the make sure the date can be represented as i64 with nanosecond resolution
        let items = items()
            .into_iter()
            .filter(|Item(dt)| *dt >= date(1677, 9, 21).at(0, 12, 44, 0))
            .collect::<Vec<_>>();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Timestamp(Nanosecond, None)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_date64() {
        let items = items();
        Test::new()
            .with_schema(
                json!([{"name": "item", "data_type": "Date64", "strategy": "NaiveStrAsDate64"}]),
            )
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }
}

mod timestamp {
    use super::*;

    fn items() -> Vec<Item<Timestamp>> {
        vec![
            Item(
                date(2024, 10, 2)
                    .at(20, 26, 12, 0)
                    .intz("UTC")
                    .unwrap()
                    .timestamp(),
            ),
            Item(
                date(-10, 10, 30)
                    .at(0, 0, 0, 0)
                    .intz("UTC")
                    .unwrap()
                    .timestamp(),
            ),
            Item(
                date(-1000, 1, 12)
                    .at(23, 59, 59, 0)
                    .intz("UTC")
                    .unwrap()
                    .timestamp(),
            ),
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
    fn as_timestamp_second() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Timestamp(Second, Some(\"UTC\"))"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_millisecond() {
        let items = items();
        Test::new()
            .with_schema(
                json!([{"name": "item", "data_type": "Timestamp(Millisecond, Some(\"UTC\"))"}]),
            )
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_microsecond() {
        let items = items();
        Test::new()
            .with_schema(
                json!([{"name": "item", "data_type": "Timestamp(Microsecond, Some(\"UTC\"))"}]),
            )
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_timestamp_nanosecond() {
        // the make sure the date can be represented as i64 with nanosecond resolution
        let items = items()
            .into_iter()
            .filter(|Item(dt)| {
                *dt >= date(1677, 9, 21)
                    .at(0, 12, 44, 0)
                    .intz("UTC")
                    .unwrap()
                    .timestamp()
            })
            .collect::<Vec<_>>();
        Test::new()
            .with_schema(
                json!([{"name": "item", "data_type": "Timestamp(Nanosecond, Some(\"UTC\"))"}]),
            )
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_date64() {
        let items = items();
        Test::new()
            .with_schema(
                json!([{"name": "item", "data_type": "Date64", "strategy": "UtcStrAsDate64"}]),
            )
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }
}

mod span {
    use super::*;
    use crate::internal::arrow::TimeUnit;
    use jiff::{RoundMode, SpanRound, Unit};
    use serde::{Deserialize, Serialize};

    // wrapper around spans that uses compare for PartialEq
    #[derive(Debug, Serialize, Deserialize)]
    struct EquivalentSpan(Span);

    impl EquivalentSpan {
        pub fn round(self, unit: TimeUnit) -> Self {
            let unit = match unit {
                TimeUnit::Second => Unit::Second,
                TimeUnit::Millisecond => Unit::Millisecond,
                TimeUnit::Microsecond => Unit::Microsecond,
                TimeUnit::Nanosecond => Unit::Nanosecond,
            };
            Self(
                self.0
                    .round(SpanRound::new().smallest(unit).mode(RoundMode::Trunc))
                    .unwrap(),
            )
        }
    }

    impl std::cmp::PartialEq for EquivalentSpan {
        fn eq(&self, other: &Self) -> bool {
            match self.0.compare(&other.0) {
                Ok(ordering) => ordering == std::cmp::Ordering::Equal,
                Err(_) => false,
            }
        }
    }

    fn items(unit: TimeUnit) -> Vec<Item<EquivalentSpan>> {
        use std::ops::Neg;

        // Note: weeks are always considered non-uniform
        let items = vec![
            Span::new().hours(5).seconds(20),
            Span::new().minutes(20).seconds(32),
            Span::new().days(5).hours(12),
            Span::new().days(1).hours(2).minutes(3).seconds(4).neg(),
            Span::new()
                .hours(5)
                .milliseconds(10)
                .microseconds(20)
                .nanoseconds(30),
            Span::new()
                .hours(5)
                .milliseconds(10)
                .microseconds(20)
                .nanoseconds(30)
                .neg(),
        ];
        items
            .into_iter()
            .map(|span| Item(EquivalentSpan(span).round(unit)))
            .collect()
    }

    #[test]
    fn as_large_utf8() {
        let items = items(TimeUnit::Nanosecond);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
            .trace_schema_from_samples(&items, TracingOptions::default())
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_utf8() {
        let items = items(TimeUnit::Nanosecond);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_duration_second() {
        let items = items(TimeUnit::Second);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Duration(Second)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_duration_microsecond() {
        let items = items(TimeUnit::Microsecond);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Duration(Microsecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_duration_millisecond() {
        let items = items(TimeUnit::Millisecond);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Duration(Millisecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_duration_nanosecond() {
        let items = items(TimeUnit::Nanosecond);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Duration(Nanosecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }
}
