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

    // date time without time zone
    let obj = obj.at(18, 30, 0, 0);
    assert_eq!(
        value::transmute::<String>(&obj).unwrap(),
        "2023-12-31T18:30:00"
    );

    // date time wiht timezone
    let obj = obj.intz("UTC").unwrap();
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
}
