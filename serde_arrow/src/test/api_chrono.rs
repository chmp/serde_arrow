use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};

#[test]
fn test_parse_utc() {
    let dt = "0730-12-01T02:03:50Z".parse::<DateTime<Utc>>().unwrap();
    assert_eq!(dt, Utc.with_ymd_and_hms(730, 12, 1, 2, 3, 50).unwrap());

    let dt = "2020-12-24T13:30:00+05:00"
        .parse::<DateTime<Utc>>()
        .unwrap();
    assert_eq!(dt, Utc.with_ymd_and_hms(2020, 12, 24, 8, 30, 0).unwrap());
}

#[test]
fn test_chrono_api_naive_datetime() {
    let dt = NaiveDate::from_ymd_opt(2021, 8, 3)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let dt_str = serde_json::to_string(&dt).unwrap();
    assert_eq!(dt_str, "\"2021-08-03T12:00:00\"");
}

#[test]
fn test_chrono_api_datetime() {
    let dt = Utc.with_ymd_and_hms(730, 12, 1, 2, 3, 50).unwrap();
    let dt_str = serde_json::to_string(&dt).unwrap();

    assert_eq!(dt_str, "\"0730-12-01T02:03:50Z\"");
}

#[test]
fn test_chrono_api_datetime_debug() {
    let dt = Utc.with_ymd_and_hms(730, 12, 1, 2, 3, 50).unwrap();
    let dt_str = format!("{:?}", dt);

    assert_eq!(dt_str, "0730-12-01T02:03:50Z");
}

#[test]
fn test_chrono_fixed_offset() {
    let dt = FixedOffset::east_opt(5 * 3600)
        .unwrap()
        .with_ymd_and_hms(2020, 12, 24, 13, 30, 00)
        .unwrap();
    let dt_str = serde_json::to_string(&dt).unwrap();

    assert_eq!(dt_str, "\"2020-12-24T13:30:00+05:00\"");
}
