use super::macros::test_example;

test_example!(
    test_name = utc_as_str,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = DateTime<Utc>,
    values = [
        Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()
    ],
    nulls = [false, false],
    define = {
        use chrono::{DateTime, Utc, TimeZone};
    },
);

test_example!(
    test_name = naive_as_str,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = NaiveDateTime,
    values = [
        NaiveDateTime::from_timestamp_millis(1662921288000).unwrap(),
        NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap(),
    ],
    nulls = [false, false],
    define = {
        use chrono::NaiveDateTime;
    },
);

test_example!(
    test_name = utc_as_date64,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::UtcStrAsDate64),
    ty = DateTime<Utc>,
    values = [
        Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()
    ],
    nulls = [false, false],
    define = {
        use chrono::{DateTime, Utc, TimeZone};
    },
);

test_example!(
    test_name = naive_as_date64,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::NaiveStrAsDate64),
    ty = NaiveDateTime,
    values = [
        NaiveDateTime::from_timestamp_millis(1662921288000).unwrap(),
        NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap(),
    ],
    nulls = [false, false],
    define = {
        use chrono::NaiveDateTime;
    },
);

test_example!(
    test_name = utc_as_date64_as_millis,
    field = GenericField::new("item", GenericDataType::I64, false),
    overwrite_field = GenericField::new("item", GenericDataType::Date64, false),
    ty = T,
    values = [
        T(Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap()),
        T(Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap())
    ],
    nulls = [false, false],
    define = {
        use chrono::{DateTime, TimeZone, Utc};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct T(#[serde(with = "chrono::serde::ts_milliseconds")] DateTime<Utc>);
    },
);

test_example!(
    test_name = utc_as_timestamp,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("item", GenericDataType::Timestamp(GenericTimeUnit::Millisecond, Some("UTC".into())), false)
        .with_strategy(Strategy::UtcStrAsDate64),
    ty = DateTime<Utc>,
    values = [
        Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()
    ],
    nulls = [false, false],
    define = {
        use chrono::{DateTime, Utc, TimeZone};
    },
);

test_example!(
    test_name = naive_as_timestamp,
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new(
        "item",
        GenericDataType::Timestamp(GenericTimeUnit::Millisecond, None),
        false
    )
    .with_strategy(Strategy::NaiveStrAsDate64),
    ty = NaiveDateTime,
    values = [
        NaiveDateTime::from_timestamp_millis(1662921288000).unwrap(),
        NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap(),
    ],
    nulls = [false, false],
    define = {
        use chrono::NaiveDateTime;
    },
);

test_example!(
    test_name = utc_as_date64_tracing,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::UtcStrAsDate64),
    ty = DateTime<Utc>,
    values = [
        Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 5, 5, 16, 6, 0).unwrap()
    ],
    nulls = [false, false],
    define = {
        use chrono::{DateTime, Utc, TimeZone};
    },
);

test_example!(
    test_name = naive_as_date64_tracing,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::NaiveStrAsDate64),
    ty = NaiveDateTime,
    values = [
        NaiveDateTime::from_timestamp_millis(1662921288000).unwrap(),
        NaiveDateTime::from_timestamp_millis(-2208936075000).unwrap(),
    ],
    nulls = [false, false],
    define = {
        use chrono::NaiveDateTime;
    },
);

test_example!(
    test_name = utc_as_date64_tracing_string_only,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::UtcStrAsDate64),
    ty = String,
    values = [
        String::from("2015-09-18T23:56:04Z"),
        String::from("2023-08-14T17:00:04Z"),
    ],
    nulls = [false, false],
);

test_example!(
    test_name = utc_as_date64_tracing_string_nullable,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, true)
        .with_strategy(Strategy::UtcStrAsDate64),
    ty = Option<String>,
    values = [
        Some(String::from("2015-09-18T23:56:04Z")),
        None,
        Some(String::from("2023-08-14T17:00:04Z")),
    ],
    nulls = [false, true, false],
);

test_example!(
    test_name = utc_as_date64_tracing_string_only_with_invalid,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = String,
    values = [
        String::from("2015-09-18T23:56:04Z"),
        String::from("2023-08-14T17:00:04Z"),
        String::from("not a date")
    ],
    nulls = [false, false, false],
);

test_example!(
    test_name = naive_as_date64_tracing_string_only,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, false)
        .with_strategy(Strategy::NaiveStrAsDate64),
    ty = String,
    values = [
        String::from("2015-09-18T23:56:04"),
        String::from("2023-08-14T17:00:04"),
    ],
    nulls = [false, false],
);

test_example!(
    test_name = naive_as_date64_tracing_string_nullable,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::Date64, true)
        .with_strategy(Strategy::NaiveStrAsDate64),
    ty = Option<String>,
    values = [
        Some(String::from("2015-09-18T23:56:04")),
        None,
        Some(String::from("2023-08-14T17:00:04")),
    ],
    nulls = [false, true, false],
);

test_example!(
    test_name = naive_as_date64_tracing_string_only_with_invalid,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = String,
    values = [
        String::from("2015-09-18T23:56:04"),
        String::from("2023-08-14T17:00:04"),
        String::from("not a date")
    ],
    nulls = [false, false, false],
);

test_example!(
    test_name = incompatible_date_formats,
    tracing_options = TracingOptions::default().guess_dates(true),
    field = GenericField::new("item", GenericDataType::LargeUtf8, false),
    ty = String,
    values = [
        String::from("2015-09-18T23:56:04Z"),
        String::from("2023-08-14T17:00:04"),
    ],
    nulls = [false, false],
);
