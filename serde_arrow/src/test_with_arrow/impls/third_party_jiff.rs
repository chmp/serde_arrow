use jiff::{
    civil::{date, time, Date, DateTime, Time},
    Span, Timestamp,
};
use serde_json::json;

use crate::{
    internal::{schema::TracingOptions, utils::Item},
    test_with_arrow::impls::utils::Test,
};

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

    #[test]
    fn as_date64() {
        let items = items();
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Date64"}]))
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
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
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
}

mod timestamp {
    use super::*;

    fn items() -> Vec<Item<Timestamp>> {
        vec![
            Item(
                date(2024, 10, 2)
                    .at(20, 26, 12, 0)
                    .in_tz("UTC")
                    .unwrap()
                    .timestamp(),
            ),
            Item(
                date(-10, 10, 30)
                    .at(0, 0, 0, 0)
                    .in_tz("UTC")
                    .unwrap()
                    .timestamp(),
            ),
            Item(
                date(-1000, 1, 12)
                    .at(23, 59, 59, 0)
                    .in_tz("UTC")
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
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
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
                    .in_tz("UTC")
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
}

mod span {
    use super::*;
    use jiff::{RoundMode, SpanCompare, SpanRound, Unit};
    use marrow::datatypes::TimeUnit;
    use serde::{Deserialize, Serialize};

    // wrapper around spans that uses compare for PartialEq
    #[derive(Debug, Serialize, Deserialize)]
    pub struct EquivalentSpan(pub Span);

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
                    .round(
                        SpanRound::new()
                            .smallest(unit)
                            .mode(RoundMode::Trunc)
                            .days_are_24_hours(),
                    )
                    .unwrap(),
            )
        }
    }

    impl std::cmp::PartialEq for EquivalentSpan {
        fn eq(&self, other: &Self) -> bool {
            match self
                .0
                .compare(SpanCompare::from(other.0).days_are_24_hours())
            {
                Ok(ordering) => ordering == std::cmp::Ordering::Equal,
                Err(_) => false,
            }
        }
    }

    pub fn items(unit: TimeUnit) -> Vec<Item<EquivalentSpan>> {
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

mod span_interval {
    use super::*;
    use crate::internal::testing::assert_error_contains;
    use serde::{Deserialize, Serialize};

    /// Wrapper around spans that compares the individual units
    #[derive(Debug, Serialize, Deserialize)]
    pub struct FieldwiseSpan(pub Span);

    impl std::cmp::PartialEq for FieldwiseSpan {
        fn eq(&self, other: &Self) -> bool {
            self.0.fieldwise() == other.0.fieldwise()
        }
    }

    /// Spans are compared fieldwise, therefore the items use the units stored in intervals
    #[test]
    fn as_interval_month_day_nano() {
        use std::ops::Neg;

        let items = [
            Span::new()
                .months(14)
                .days(3)
                .seconds(4)
                .milliseconds(5)
                .microseconds(6)
                .nanoseconds(7),
            Span::new().months(1).seconds(43_200).neg(),
            Span::new().months(239_976),
            Span::new(),
        ]
        .map(|span| Item(FieldwiseSpan(span)));
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Interval(MonthDayNano)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    /// Intervals can exceed the ranges of spans, e.g., more than 239976 months
    #[test]
    fn interval_outside_of_span_range() {
        let items = [Item(
            json!({"months": 239_977, "days": 0, "nanoseconds": 0}),
        )];
        let test = Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Interval(MonthDayNano)"}]))
            .serialize(&items);
        let fields = test.get_arrow_fields();
        let arrays = test.arrays.arrow.as_ref().unwrap();

        let actual: Vec<Item<String>> = crate::from_arrow(&fields, arrays).unwrap();
        assert_eq!(actual, vec![Item(String::from("P239977m"))]);

        let err = crate::from_arrow::<Vec<Item<Span>>, _>(&fields, arrays).unwrap_err();
        assert_error_contains(&err, "parameter 'months' is not in the required range");
    }
}

mod signed_duration {
    use super::*;
    use jiff::{SignedDuration, SpanRelativeTo};
    use marrow::datatypes::TimeUnit;

    fn items(unit: TimeUnit) -> Vec<Item<SignedDuration>> {
        super::span::items(unit)
            .into_iter()
            .map(|span| {
                Item(
                    span.0
                         .0
                        .to_duration(SpanRelativeTo::days_are_24_hours())
                        .unwrap(),
                )
            })
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
