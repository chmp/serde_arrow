use chrono::NaiveDateTime;
use marrow::{
    array::{Array, TimestampArray},
    datatypes::{DataType, Field, TimeUnit},
};
use serde::{Deserialize, Serialize};

use crate::{utils::Item, ArrayBuilder, Deserializer, Serializer};

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
            .map(|s| timestamp(*s, format, unit))
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
