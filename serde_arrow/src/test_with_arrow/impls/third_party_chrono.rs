use marrow::datatypes::DataType;

use super::utils::Test;
use crate::{
    internal::{
        testing::{assert_error_contains, ArrayAccess},
        utils::value,
    },
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::Item,
    ArrayBuilder,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use serde_json::json;

#[test]
fn temporal_formats() {
    assert_error_contains(
        &value::transmute::<DateTime<Utc>>("2023-12-01T12:22:33[UTC]").unwrap_err(),
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

mod datetime_utc {
    use super::*;

    fn items() -> Vec<Item<DateTime<Utc>>> {
        vec![
            Item(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
            Item(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()),
        ]
    }

    #[test]
    fn trace_from_type_does_not_work() {
        let err = SerdeArrowSchema::from_type::<Item<DateTime<Utc>>>(TracingOptions::default())
            .unwrap_err();
        assert_error_contains(&err, "premature end of input");
    }

    #[test]
    fn as_timestamp_second() {
        let items = items();
        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Second, Some(\"UTC\"))",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_millisecond() {
        let items = items();
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
    fn as_timestamp_microsecond() {
        let items = items();
        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Microsecond, Some(\"UTC\"))",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_nanosecond() {
        let items = items();
        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Timestamp(Nanosecond, Some(\"UTC\"))",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }
}

mod naive_date_time {
    use super::*;

    fn items(supports_milliseconds: bool) -> Vec<Item<NaiveDateTime>> {
        // The 001 in the end makes sure that we handle fractional seconds correctly
        // in both positive and negative timestamps.
        vec![
            Item(
                DateTime::from_timestamp_millis(
                    1662921288000 + if supports_milliseconds { 1 } else { 0 },
                )
                .unwrap()
                .naive_utc(),
            ),
            Item(
                DateTime::from_timestamp_millis(
                    -2208936075000 - if supports_milliseconds { 1 } else { 0 },
                )
                .unwrap()
                .naive_utc(),
            ),
        ]
    }

    #[test]
    fn trace_from_type_does_not_work() {
        let err = SerdeArrowSchema::from_type::<Item<NaiveDateTime>>(TracingOptions::default())
            .unwrap_err();
        assert_error_contains(&err, "premature end of input");
    }

    #[test]
    fn as_large_utf8() {
        let items = items(false);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "LargeUtf8",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_utf8() {
        let items = items(false);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type": "Utf8",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_second() {
        let items = items(false);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type":
                "Timestamp(Second, None)",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_millisecond() {
        let items = items(true);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type":
                "Timestamp(Millisecond, None)",
            }]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_microsecond() {
        let items = items(true);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type":
                "Timestamp(Microsecond, None)",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }

    #[test]
    fn as_timestamp_nanosecond() {
        let items = items(true);

        Test::new()
            .with_schema(json!([{
                "name": "item",
                "data_type":
                "Timestamp(Nanosecond, None)",
            }]))
            .serialize(&items)
            .deserialize(&items)
            .check_nulls(&[&[false, false]]);
    }
}

mod naive_time {
    use super::*;

    fn items(supports_milliseconds: bool) -> Vec<Item<NaiveTime>> {
        vec![
            Item(NaiveTime::from_hms_opt(12, 10, 42).unwrap()),
            Item(NaiveTime::from_hms_opt(22, 10, 00).unwrap()),
            Item(
                NaiveTime::from_hms_milli_opt(
                    23,
                    59,
                    59,
                    if supports_milliseconds { 999 } else { 0 },
                )
                .unwrap(),
            ),
        ]
    }

    #[test]
    fn trace_from_type_does_not_work() {
        let err =
            SerdeArrowSchema::from_type::<Item<NaiveTime>>(TracingOptions::default()).unwrap_err();
        assert_error_contains(&err, "premature end of input");
    }

    #[test]
    fn as_large_utf8() {
        let items = items(true);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
            .trace_schema_from_samples(&items, TracingOptions::default())
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_utf8() {
        let items = items(true);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time32_second() {
        let items = items(false);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time32(Second)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time32_millisecond() {
        let items = items(true);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time32(Millisecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time64_microsecond() {
        let items = items(true);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time64(Microsecond)"}]))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn as_time64_nanosecond() {
        let items = items(true);
        Test::new()
            .with_schema(json!([{"name": "item", "data_type": "Time64(Nanosecond)"}]))
            .trace_schema_from_samples(&items, TracingOptions::default().guess_dates(true))
            .serialize(&items)
            .deserialize(&items);
    }

    #[test]
    fn time_example_as_string_fractional() {
        let items = [
            Item(NaiveTime::from_hms_opt(12, 10, 42).unwrap()),
            Item(NaiveTime::from_hms_opt(22, 10, 00).unwrap()),
            Item(NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap()),
        ];

        let schema = SerdeArrowSchema::from_samples(&items, TracingOptions::default()).unwrap();
        assert_eq!(schema.fields[0].data_type, DataType::LargeUtf8);

        let mut builder = ArrayBuilder::new(schema).unwrap();
        builder.extend(&items).unwrap();

        let arrays = builder.into_marrow().unwrap();
        let [array] = arrays.try_into().unwrap();

        assert_eq!(array.get_utf8(0).unwrap(), Some("12:10:42"));
        assert_eq!(array.get_utf8(1).unwrap(), Some("22:10:00"));
        assert_eq!(array.get_utf8(2).unwrap(), Some("23:59:59.999"));
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
    fn trace_from_type_does_not_work() {
        let err =
            SerdeArrowSchema::from_type::<Item<NaiveDate>>(TracingOptions::default()).unwrap_err();
        assert_error_contains(&err, "premature end of input");
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
