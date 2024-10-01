use jiff::{
    civil::{date, time, Date, DateTime, Time},
    Span, Zoned,
};
use serde_json::json;

use crate::{
    internal::{
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
    assert_error_contains(&value::transmute::<Zoned>(&chrono), "");

    let jiff = date(1234, 5, 6).at(7, 8, 9, 0).intz("UTC").unwrap();
    assert_error_contains(
        &value::transmute::<chrono::DateTime<chrono::Utc>>(&jiff),
        "",
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

#[test]
fn test_time() {
    let items = [
        Item(time(12, 0, 0, 0)),
        Item(time(23, 31, 12, 0)),
        Item(time(3, 2, 58, 0)),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .serialize(&items)
        .deserialize(&items);

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Time32(Second)"}]))
        .serialize(&items)
        .deserialize(&items);

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Time32(Millisecond)"}]))
        .serialize(&items)
        .deserialize(&items);

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Time64(Microsecond)"}]))
        .serialize(&items)
        .deserialize(&items);

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Time64(Nanosecond)"}]))
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn test_date() {
    let items = [
        Item(date(1234, 5, 6)),
        // NOTE: chrono uses 4 digits for the year, jiff requires 6
        // Item(date(-10, 10, 30)),
        Item(date(2024, 10, 1)),
    ];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .serialize(&items)
        .deserialize(&items);

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Date32"}]))
        .serialize(&items)
        .deserialize(&items);
}
