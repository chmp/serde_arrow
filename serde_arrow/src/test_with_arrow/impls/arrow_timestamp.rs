use chrono::NaiveDateTime;
use marrow::{
    array::{Array, TimestampArray},
    datatypes::{DataType, Field, TimeUnit},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::utils::Test;
use crate::internal::{
    array_builder::ArrayBuilder, deserializer::Deserializer, schema::TracingOptions,
    serializer::Serializer, utils::Item,
};

mod timestamp_string_conversion {
    use super::*;

    #[test]
    fn second() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42", "1970-01-01T00:00:00"],
            "%Y-%m-%dT%H:%M:%S",
            TimeUnit::Second,
        );
    }

    #[test]
    fn millisecond() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42", "1970-01-01T00:00:00"],
            "%Y-%m-%dT%H:%M:%S",
            TimeUnit::Millisecond,
        );
    }

    #[test]
    fn millisecond_fractional() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42", "1970-01-01T00:00:00.123"],
            "%Y-%m-%dT%H:%M:%S%.f",
            TimeUnit::Millisecond,
        );
    }

    #[test]
    fn microsecond() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42", "1970-01-01T00:00:00"],
            "%Y-%m-%dT%H:%M:%S",
            TimeUnit::Microsecond,
        );
    }

    #[test]
    fn microsecond_fractional() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42.123456", "1970-01-01T00:00:00.123"],
            "%Y-%m-%dT%H:%M:%S%.f",
            TimeUnit::Microsecond,
        );
    }

    #[test]
    fn nanosecond() {
        assert_timestamp_string_conversion(
            &["2025-01-20T19:30:42", "1970-01-01T00:00:00"],
            "%Y-%m-%dT%H:%M:%S",
            TimeUnit::Nanosecond,
        );
    }

    #[test]
    fn nanosecond_fractional() {
        assert_timestamp_string_conversion(
            &[
                "2025-01-20T19:30:42.123456",
                "1970-01-01T00:00:00.123456789",
            ],
            "%Y-%m-%dT%H:%M:%S%.f",
            TimeUnit::Nanosecond,
        );
    }

    fn assert_timestamp_string_conversion(strings: &[&str], format: &str, unit: TimeUnit) {
        let timestamps = strings
            .iter()
            .map(|s| timestamp(s, format, unit))
            .collect::<Vec<_>>();
        let items = strings
            .iter()
            .map(|s| Item(String::from(*s)))
            .collect::<Vec<_>>();

        assert_deserialization(&items, &timestamps, unit);
        assert_serializations(&items, &timestamps, unit);
    }

    fn assert_deserialization(items: &[Item<String>], timestamps: &[i64], unit: TimeUnit) {
        let array = Array::Timestamp(TimestampArray {
            unit,
            timezone: None,
            validity: None,
            values: timestamps.to_vec(),
        });
        let view = array.as_view();
        let deserializer = Deserializer::from_marrow(&[field(unit)], &[view]).unwrap();
        let actual = Vec::<Item<String>>::deserialize(deserializer).unwrap();

        assert_eq!(actual, items);
    }

    fn assert_serializations(items: &[Item<String>], timestamps: &[i64], unit: TimeUnit) {
        let mut builder = ArrayBuilder::from_marrow(&[field(unit)]).unwrap();
        items.serialize(Serializer::new(&mut builder)).unwrap();

        let arrays = builder.to_marrow().unwrap();
        let [array] = <[_; 1]>::try_from(arrays).unwrap();
        let Array::Timestamp(array) = array else {
            panic!();
        };

        assert_eq!(array.values, timestamps);
    }

    fn field(unit: TimeUnit) -> Field {
        Field {
            name: String::from("item"),
            data_type: DataType::Timestamp(unit, None),
            nullable: false,
            metadata: Default::default(),
        }
    }

    fn timestamp(s: &str, format: &str, unit: TimeUnit) -> i64 {
        let dt = NaiveDateTime::parse_from_str(s, format).unwrap().and_utc();

        match unit {
            TimeUnit::Second => dt.timestamp(),
            TimeUnit::Millisecond => dt.timestamp_millis(),
            TimeUnit::Microsecond => dt.timestamp_micros(),
            TimeUnit::Nanosecond => dt.timestamp_nanos_opt().unwrap(),
        }
    }
}

mod schema_tracing {
    use super::*;

    #[test]
    fn utc_as_timestamp_tracing_string_only() {
        let items = [
            Item(String::from("2015-09-18T23:56:04Z")),
            Item(String::from("2023-08-14T17:00:04Z")),
        ];

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Millisecond, Some(\"UTC\"))",
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn utc_as_timestamp_tracing_string_nullable() {
        let items = [
            Item(Some(String::from("2015-09-18T23:56:04Z"))),
            Item(None),
            Item(Some(String::from("2023-08-14T17:00:04Z"))),
        ];

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Millisecond, Some(\"UTC\"))",
                "nullable": true,
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, true, false]]);
    }

    #[test]
    fn utc_zero_offset_designators_as_timestamp_tracing() {
        let items = [
            Item(String::from("2025-01-20T19:30:42+0000")),
            Item(String::from("2025-01-20T19:30:42+00:00")),
            Item(String::from("2025-01-20T19:30:42-0000")),
            Item(String::from("2025-01-20T19:30:42-00:00")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42z")),
        ];
        let expected = [
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42Z")),
        ];

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Millisecond, Some(\"UTC\"))",
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&expected)
            .check_nulls(&[&[false, false, false, false, false, false]]);
    }

    #[test]
    fn utc_tracing_string_only_with_invalid() {
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
    fn naive_as_timestamp_tracing_string_only() {
        let items = [
            Item(String::from("2015-09-18T23:56:04")),
            Item(String::from("2023-08-14T17:00:04")),
        ];

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Millisecond, None)",
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn naive_as_timestamp_tracing_string_nullable() {
        let items = [
            Item(Some(String::from("2015-09-18T23:56:04"))),
            Item(None),
            Item(Some(String::from("2023-08-14T17:00:04"))),
        ];

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Millisecond, None)",
                "nullable": true,
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, true, false]]);
    }

    #[test]
    fn naive_tracing_string_with_invalid() {
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
}

/// A timezone-aware Arrow timestamp is a UTC instant, so every zero-offset
/// timezone designator must deserialize. Iceberg tags its `timestamptz` columns
/// with the fixed offset `+00:00`.
mod timezone_utc_designators {
    use super::*;

    const UTC_DESIGNATORS: &[&str] =
        &["+00:00", "-00:00", "+0000", "-0000", "Z", "z", "UTC", "utc"];

    #[test]
    fn deserializes_every_utc_designator_as_zulu() {
        for tz in UTC_DESIGNATORS {
            let array = Array::Timestamp(TimestampArray {
                unit: TimeUnit::Microsecond,
                timezone: Some(String::from(*tz)),
                validity: None,
                values: vec![zulu_micros()],
            });
            let view = array.as_view();
            let deserializer = Deserializer::from_marrow(&[tz_field(tz)], &[view])
                .unwrap_or_else(|e| panic!("timezone {tz:?} rejected: {e}"));
            let actual = Vec::<Item<String>>::deserialize(deserializer).unwrap();

            assert_eq!(
                actual,
                [Item(String::from("2025-01-20T19:30:42Z"))],
                "timezone {tz:?}"
            );
        }
    }

    /// Tagging the schema with a zero-offset designator sets the builder to UTC,
    /// so a `Z`-suffixed string parses instead of being rejected as non-naive.
    #[test]
    fn serializes_every_utc_designator_from_zulu_string() {
        for tz in UTC_DESIGNATORS {
            let mut builder = ArrayBuilder::from_marrow(&[tz_field(tz)])
                .unwrap_or_else(|e| panic!("timezone {tz:?} rejected: {e}"));
            [Item(String::from("2025-01-20T19:30:42Z"))]
                .serialize(Serializer::new(&mut builder))
                .unwrap_or_else(|e| panic!("timezone {tz:?}: {e}"));

            let arrays = builder.to_marrow().unwrap();
            let [array] = <[_; 1]>::try_from(arrays).unwrap();
            let Array::Timestamp(array) = array else {
                panic!("timezone {tz:?}: expected a timestamp array");
            };

            assert_eq!(array.values, [zulu_micros()], "timezone {tz:?}");
            assert_eq!(array.timezone.as_deref(), Some(*tz), "timezone {tz:?}");
        }
    }

    #[test]
    fn serializes_zero_offset_designators_from_string_values() {
        let items = [
            Item(String::from("2025-01-20T19:30:42+0000")),
            Item(String::from("2025-01-20T19:30:42+00:00")),
            Item(String::from("2025-01-20T19:30:42-0000")),
            Item(String::from("2025-01-20T19:30:42-00:00")),
            Item(String::from("2025-01-20T19:30:42Z")),
            Item(String::from("2025-01-20T19:30:42z")),
        ];
        let mut builder = ArrayBuilder::from_marrow(&[tz_field("UTC")]).unwrap();
        items
            .serialize(Serializer::new(&mut builder))
            .unwrap_or_else(|e| panic!("{e}"));

        let arrays = builder.to_marrow().unwrap();
        let [array] = <[_; 1]>::try_from(arrays).unwrap();
        let Array::Timestamp(array) = array else {
            panic!("expected a timestamp array");
        };

        assert_eq!(array.values, vec![zulu_micros(); items.len()]);
        assert_eq!(array.timezone.as_deref(), Some("UTC"));
    }

    #[test]
    fn rejects_non_utc_offset() {
        let array = Array::Timestamp(TimestampArray {
            unit: TimeUnit::Microsecond,
            timezone: Some(String::from("+01:00")),
            validity: None,
            values: vec![0],
        });
        let view = array.as_view();
        let err = match Deserializer::from_marrow(&[tz_field("+01:00")], &[view]) {
            Ok(_) => panic!("expected the +01:00 offset to be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("+01:00"),
            "unexpected error: {err}"
        );
    }

    /// `2025-01-20T19:30:42Z` as microseconds since the epoch.
    fn zulu_micros() -> i64 {
        NaiveDateTime::parse_from_str("2025-01-20T19:30:42", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    fn tz_field(tz: &str) -> Field {
        Field {
            name: String::from("item"),
            data_type: DataType::Timestamp(TimeUnit::Microsecond, Some(String::from(tz))),
            nullable: false,
            metadata: Default::default(),
        }
    }
}
