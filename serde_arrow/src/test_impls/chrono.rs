use super::macros::test_example;

test_example!(
    test_name = utc_as_str,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("root", GenericDataType::Date64, false)
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("root", GenericDataType::Date64, false)
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::I64, false),
    overwrite_field = GenericField::new("root", GenericDataType::Date64, false),
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new("root", GenericDataType::Timestamp(GenericTimeUnit::Second, Some("UTC".into())), false)
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeUtf8, false),
    overwrite_field = GenericField::new(
        "root",
        GenericDataType::Timestamp(GenericTimeUnit::Second, None),
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
