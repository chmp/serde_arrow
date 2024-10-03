//! Support for Parsing datetime related quantities
//!
use crate::internal::{arrow::TimeUnit, error::Result};

use parsing::ParseResult;

pub use parsing::Span;

use super::error::fail;

/// Check whether `s` can be parsed as a naive datetime
pub fn matches_naive_datetime(s: &str) -> bool {
    parsing::match_naive_datetime(s).matches()
}

/// Check whether `s` can be parsed as a UTC datetime
pub fn matches_utc_datetime(s: &str) -> bool {
    parsing::match_utc_datetime(s).matches()
}

/// Check whether `s` can be parsed as a naive date
pub fn matches_naive_date(s: &str) -> bool {
    parsing::match_naive_date(s).matches()
}

/// Check whether `s` can be parsed as a naive time
pub fn matches_naive_time(s: &str) -> bool {
    parsing::match_naive_time(s).matches()
}

/// Parse `s` as a span
pub fn parse_span(s: &str) -> Result<Span<'_>> {
    parsing::match_span(s).into_result("Span")
}

// TODO: handle signs
impl<'a> parsing::Span<'a> {
    pub fn to_arrow_duration(&self, unit: TimeUnit) -> Result<i64> {
        if get_value(self.year)? != 0 || get_value(self.month)? != 0 {
            fail!("Cannot convert interval style spans to a duration");
        }

        // TODO: perform check here?
        let second_value = get_value(self.week)? * 7 * 24 * 60 * 60
            + get_value(self.day)? * 24 * 60 * 60
            + get_value(self.hour)? * 60 * 60
            + get_value(self.minute)? * 60
            + get_value(self.second)?;

        // TODO: implement subsecond values

        checked_unit_cast(second_value, unit)
    }
}

fn get_value(s: Option<&str>) -> Result<i64> {
    match s {
        Some(s) => Ok(s.parse()?),
        None => Ok(0),
    }
}

fn checked_unit_cast(second_value: i64, unit: TimeUnit) -> Result<i64> {
    match unit {
        TimeUnit::Second => Ok(second_value),
        TimeUnit::Microsecond => match second_value.checked_mul(1_000_i64) {
            Some(res) => Ok(res),
            None => fail!("Cannot represent {second_value} with Microsecond resolution"),
        },
        TimeUnit::Millisecond => match second_value.checked_mul(1_000_000_i64) {
            Some(res) => Ok(res),
            None => fail!("Cannot represent {second_value} with Millisecond resolution"),
        },
        TimeUnit::Nanosecond => match second_value.checked_mul(1_000_000_000_i64) {
            Some(res) => Ok(res),
            None => fail!("Cannot represent {second_value} with Nanosecond resolution"),
        },
    }
}

/// Minimalistic monadic parsers for datetime objects
///
/// Each parser has the the following interface:
///
/// `fn (string_to_parser, ..extra_args) -> Result<(rest, result), unmatched_string>`
///
mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    pub trait ParseResult {
        type Output;

        fn matches(&self) -> bool;
        fn into_result(self, output_type: &str) -> crate::internal::error::Result<Self::Output>;
    }

    impl<'a, 'e, R> ParseResult for Result<(&'a str, R), &'e str> {
        type Output = R;

        fn matches(&self) -> bool {
            match self {
                Ok((rest, _)) => rest.is_empty(),
                Err(_) => false,
            }
        }

        fn into_result(self, output_type: &str) -> crate::internal::error::Result<Self::Output> {
            match self {
                Ok((rest, output)) if rest.is_empty() => Ok(output),
                Ok((unmatched, _)) | Err(unmatched) => crate::internal::error::fail!(
                    "Could not parse the string as {output_type}, unmatched content: {unmatched:?}"
                ),
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct Date<'a> {
        pub sign: Option<char>,
        pub year: &'a str,
        pub month: &'a str,
        pub day: &'a str,
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct Time<'a> {
        pub hour: &'a str,
        pub minute: &'a str,
        pub second: &'a str,
        pub subsecond: Option<&'a str>,
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct DateTime<'a> {
        pub date: Date<'a>,
        pub time: Time<'a>,
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct DateTimeUtc<'a> {
        pub date: Date<'a>,
        pub time: Time<'a>,
        pub timezone: &'a str,
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct Span<'a> {
        pub year: Option<&'a str>,
        pub month: Option<&'a str>,
        pub day: Option<&'a str>,
        pub week: Option<&'a str>,
        pub hour: Option<&'a str>,
        pub minute: Option<&'a str>,
        pub second: Option<&'a str>,
        pub subsecond: Option<&'a str>,
    }

    pub fn match_utc_datetime(s: &str) -> Result<(&str, DateTimeUtc<'_>), &str> {
        let (s, DateTime { date, time }) = match_naive_datetime_with_sep(s, &['T', ' '])?;
        let (s, timezone) = match_utc_timezone(s)?;
        Ok((
            s,
            DateTimeUtc {
                date,
                time,
                timezone,
            },
        ))
    }

    pub fn match_naive_datetime(s: &str) -> Result<(&str, DateTime<'_>), &str> {
        match_naive_datetime_with_sep(s, &['T'])
    }

    pub fn match_naive_date(s: &str) -> Result<(&str, Date<'_>), &str> {
        let (s, sign) = match_optional_sign(s)?;
        let (s, year) = match_one_or_more_digits(s)?;
        let (s, _) = match_char(s, '-')?;
        let (s, month) = match_one_or_two_digits(s)?;
        let (s, _) = match_char(s, '-')?;
        let (s, day) = match_one_or_two_digits(s)?;
        Ok((
            s,
            Date {
                sign,
                year,
                month,
                day,
            },
        ))
    }

    pub fn match_naive_time(s: &str) -> Result<(&str, Time<'_>), &str> {
        let (s, hour) = match_one_or_two_digits(s)?;
        let (s, _) = match_char(s, ':')?;
        let (s, minute) = match_one_or_two_digits(s)?;
        let (s, _) = match_char(s, ':')?;
        let (s, second) = match_one_or_two_digits(s)?;

        let (s, subsecond) = if let Some(s) = s.strip_prefix('.') {
            let (s, subsecond) = match_one_or_more_digits(s)?;
            (s, Some(subsecond))
        } else {
            (s, None)
        };

        Ok((
            s,
            Time {
                hour,
                minute,
                second,
                subsecond,
            },
        ))
    }

    pub fn match_span(s: &str) -> Result<(&str, Span<'_>), &str> {
        let (s, _) = match_char_case_insensitive(s, 'P')?;
        let (s, year) = match_optional_span_value(s, 'Y')?;
        let (s, month) = match_optional_span_value(s, 'M')?;
        let (s, week) = match_optional_span_value(s, 'W')?;
        let (s, day) = match_optional_span_value(s, 'D')?;

        let (s, hour, minute, second, subsecond) = if let Some(s) = s.strip_prefix(['t', 'T']) {
            let (s, hour) = match_optional_span_value(s, 'H')?;
            let (s, minute) = match_optional_span_value(s, 'M')?;
            let (s, second, subsecond) = match_optional_span_seconds(s)?;
            (s, hour, minute, second, subsecond)
        } else {
            (s, None, None, None, None)
        };

        Ok((
            s,
            Span {
                year,
                month,
                week,
                day,
                hour,
                minute,
                second,
                subsecond,
            },
        ))
    }

    pub fn match_optional_span_seconds(
        s: &str,
    ) -> Result<(&str, Option<&str>, Option<&str>), &str> {
        let Ok((rest, second)) = match_one_or_more_digits(s) else {
            return Ok((s, None, None));
        };
        let second = Some(second);

        let (rest, subsecond) = if let Some(rest) = rest.strip_prefix('.') {
            // Q: is a subsecond part really required after a '.'?
            let (rest, subsecond) = match_one_or_more_digits(rest)?;
            (rest, Some(subsecond))
        } else {
            (rest, None)
        };

        let Ok((rest, _)) = match_char_case_insensitive(rest, 'S') else {
            return Ok((s, None, None));
        };

        Ok((rest, second, subsecond))
    }

    pub fn match_naive_datetime_with_sep<'a>(
        s: &'a str,
        sep: &'_ [char],
    ) -> Result<(&'a str, DateTime<'a>), &'a str> {
        let (s, date) = match_naive_date(s)?;
        let s = s.strip_prefix(sep).ok_or(s)?;
        let (s, time) = match_naive_time(s)?;
        Ok((s, DateTime { date, time }))
    }

    /// Match known UTC time zone designators
    ///
    /// Note: this function is more permissive than some libraries (e.g., jiff)
    pub fn match_utc_timezone(s: &str) -> Result<(&str, &str), &str> {
        for prefix in ["Z", "+0000", "+00:00"] {
            if let Some(rest) = s.strip_prefix(prefix) {
                return Ok((rest, get_prefix(s, rest)));
            }
        }
        Err(s)
    }

    fn get_prefix<'a>(s: &'a str, rest: &str) -> &'a str {
        debug_assert!(s.ends_with(rest), "Invalid call to get prefix");
        let len_prefix = s.len() - rest.len();
        &s[..len_prefix]
    }

    /// Match a value in a span
    pub fn match_optional_span_value(s: &str, unit: char) -> Result<(&str, Option<&str>), &str> {
        let Ok((rest, value)) = match_one_or_more_digits(s) else {
            return Ok((s, None));
        };
        let Ok((rest, _)) = match_char_case_insensitive(rest, unit) else {
            return Ok((s, None));
        };
        Ok((rest, Some(value)))
    }

    pub fn match_optional_sign(s: &str) -> Result<(&str, Option<char>), &str> {
        if let Some(rest) = s.strip_prefix('+') {
            Ok((rest, Some('+')))
        } else if let Some(rest) = s.strip_prefix('-') {
            Ok((rest, Some('-')))
        } else {
            Ok((s, None))
        }
    }

    pub fn match_one_or_more_digits(s: &str) -> Result<(&str, &str), &str> {
        let mut rest = s.strip_prefix(DIGIT).ok_or(s)?;
        while let Some(new_rest) = rest.strip_prefix(DIGIT) {
            rest = new_rest;
        }
        Ok((rest, get_prefix(s, rest)))
    }

    pub fn match_one_or_two_digits(s: &str) -> Result<(&str, &str), &str> {
        let rest = s.strip_prefix(DIGIT).ok_or(s)?;
        let rest = rest.strip_prefix(DIGIT).unwrap_or(rest);
        Ok((rest, get_prefix(s, rest)))
    }

    pub fn match_char(s: &str, c: char) -> Result<(&str, char), &str> {
        if let Some(rest) = s.strip_prefix(c) {
            Ok((rest, c))
        } else {
            Err(s)
        }
    }

    /// Match a character case insensitive
    ///
    /// Note: `c` must be an ASCII character and must be uppercase
    pub fn match_char_case_insensitive(s: &str, c: char) -> Result<(&str, char), &str> {
        debug_assert!(c.is_ascii());
        debug_assert!(c.is_ascii_uppercase());

        let c_lowercase = c.to_ascii_lowercase();

        if let Some(rest) = s.strip_prefix(c) {
            Ok((rest, c))
        } else if let Some(rest) = s.strip_prefix(c_lowercase) {
            Ok((rest, c_lowercase))
        } else {
            Err(s)
        }
    }
}

#[test]
fn test_match_naive_datetime() {
    // chrono examples
    assert_eq!(
        parsing::match_naive_datetime("2015-09-18T23:56:04"),
        Ok((
            "",
            parsing::DateTime {
                date: parsing::Date {
                    sign: None,
                    year: "2015",
                    month: "09",
                    day: "18"
                },
                time: parsing::Time {
                    hour: "23",
                    minute: "56",
                    second: "04",
                    subsecond: None
                }
            }
        ))
    );
    assert_eq!(
        parsing::match_naive_datetime("+12345-6-7T7:59:60.5"),
        Ok((
            "",
            parsing::DateTime {
                date: parsing::Date {
                    sign: Some('+'),
                    year: "12345",
                    month: "6",
                    day: "7"
                },
                time: parsing::Time {
                    hour: "7",
                    minute: "59",
                    second: "60",
                    subsecond: Some("5")
                },
            }
        ))
    );
}

#[test]
fn test_match_utc_datetime() {
    // examples from the chrono docs
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12T12:12:12Z"),
        Ok((
            "",
            parsing::DateTimeUtc {
                date: parsing::Date {
                    sign: None,
                    year: "2012",
                    month: "12",
                    day: "12"
                },
                time: parsing::Time {
                    hour: "12",
                    minute: "12",
                    second: "12",
                    subsecond: None
                },
                timezone: "Z",
            }
        ))
    );
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12 12:12:12Z"),
        Ok((
            "",
            parsing::DateTimeUtc {
                date: parsing::Date {
                    sign: None,
                    year: "2012",
                    month: "12",
                    day: "12"
                },
                time: parsing::Time {
                    hour: "12",
                    minute: "12",
                    second: "12",
                    subsecond: None
                },
                timezone: "Z",
            }
        ))
    );
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12 12:12:12+0000"),
        Ok((
            "",
            parsing::DateTimeUtc {
                date: parsing::Date {
                    sign: None,
                    year: "2012",
                    month: "12",
                    day: "12"
                },
                time: parsing::Time {
                    hour: "12",
                    minute: "12",
                    second: "12",
                    subsecond: None
                },
                timezone: "+0000",
            }
        ))
    );
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12 12:12:12+00:00"),
        Ok((
            "",
            parsing::DateTimeUtc {
                date: parsing::Date {
                    sign: None,
                    year: "2012",
                    month: "12",
                    day: "12"
                },
                time: parsing::Time {
                    hour: "12",
                    minute: "12",
                    second: "12",
                    subsecond: None
                },
                timezone: "+00:00",
            }
        ))
    );
}

#[test]
fn test_match_naive_date() {
    assert_eq!(
        parsing::match_naive_date("+12345-6-7"),
        Ok((
            "",
            parsing::Date {
                sign: Some('+'),
                year: "12345",
                month: "6",
                day: "7"
            }
        ))
    );
    assert_eq!(
        parsing::match_naive_date("2015-09-18"),
        Ok((
            "",
            parsing::Date {
                sign: None,
                year: "2015",
                month: "09",
                day: "18"
            }
        ))
    );

    // NOTE: the content is not verified
    assert_eq!(
        parsing::match_naive_date("-20-21-22"),
        Ok((
            "",
            parsing::Date {
                sign: Some('-'),
                year: "20",
                month: "21",
                day: "22"
            }
        ))
    );

    assert_eq!(parsing::match_naive_date("foo"), Err("foo"));

    assert_eq!(parsing::match_naive_date("2015-123-18"), Err("3-18"));

    // trailing digits are returned as rest
    assert_eq!(
        parsing::match_naive_date("2024-12-091234"),
        Ok((
            "1234",
            parsing::Date {
                sign: None,
                year: "2024",
                month: "12",
                day: "09"
            }
        ))
    );
}

#[test]
fn test_match_naive_time() {
    assert_eq!(
        parsing::match_naive_time("23:00:12"),
        Ok((
            "",
            parsing::Time {
                hour: "23",
                minute: "00",
                second: "12",
                subsecond: None
            }
        ))
    );
    assert_eq!(
        parsing::match_naive_time("23:00:12.999"),
        Ok((
            "",
            parsing::Time {
                hour: "23",
                minute: "00",
                second: "12",
                subsecond: Some("999")
            }
        ))
    );
}

#[test]
fn match_span() {
    // jiff examples
    assert_eq!(
        parsing::match_span("P40D"),
        Ok((
            "",
            parsing::Span {
                day: Some("40"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P1y1d"),
        Ok((
            "",
            parsing::Span {
                year: Some("1"),
                day: Some("1"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P1m"),
        Ok((
            "",
            parsing::Span {
                month: Some("1"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P1w"),
        Ok((
            "",
            parsing::Span {
                week: Some("1"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P1w4d"),
        Ok((
            "",
            parsing::Span {
                week: Some("1"),
                day: Some("4"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P0d"),
        Ok((
            "",
            parsing::Span {
                day: Some("0"),
                ..Default::default()
            }
        ))
    );

    assert_eq!(
        parsing::match_span("P3dT4h59m"),
        Ok((
            "",
            parsing::Span {
                day: Some("3"),
                hour: Some("4"),
                minute: Some("59"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("PT2H30M"),
        Ok((
            "",
            parsing::Span {
                hour: Some("2"),
                minute: Some("30"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("PT1m"),
        Ok((
            "",
            parsing::Span {
                minute: Some("1"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("PT0s"),
        Ok((
            "",
            parsing::Span {
                second: Some("0"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("PT0.0021s"),
        Ok((
            "",
            parsing::Span {
                second: Some("0"),
                subsecond: Some("0021"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("P1y1m1dT1h1m1.1s"),
        Ok((
            "",
            parsing::Span {
                year: Some("1"),
                month: Some("1"),
                day: Some("1"),
                hour: Some("1"),
                minute: Some("1"),
                second: Some("1"),
                subsecond: Some("1"),
                ..Default::default()
            }
        ))
    );
}

#[test]
fn match_optional_sign() {
    assert_eq!(parsing::match_optional_sign("foo"), Ok(("foo", None)));
    assert_eq!(parsing::match_optional_sign("?foo"), Ok(("?foo", None)));
    assert_eq!(parsing::match_optional_sign("+foo"), Ok(("foo", Some('+'))));
    assert_eq!(parsing::match_optional_sign("-foo"), Ok(("foo", Some('-'))));
}

#[test]
fn match_one_or_more_digits() {
    assert_eq!(parsing::match_one_or_more_digits("foo"), Err("foo"));
    assert_eq!(parsing::match_one_or_more_digits(" 1foo"), Err(" 1foo"));
    assert_eq!(parsing::match_one_or_more_digits("1foo"), Ok(("foo", "1")));
    assert_eq!(
        parsing::match_one_or_more_digits("12foo"),
        Ok(("foo", "12"))
    );
    assert_eq!(
        parsing::match_one_or_more_digits("123foo"),
        Ok(("foo", "123"))
    );
    assert_eq!(
        parsing::match_one_or_more_digits("1234foo"),
        Ok(("foo", "1234"))
    );
}

#[test]
fn match_one_or_two_digits() {
    assert_eq!(parsing::match_one_or_two_digits("foo"), Err("foo"));
    assert_eq!(parsing::match_one_or_two_digits(" 1foo"), Err(" 1foo"));
    assert_eq!(parsing::match_one_or_two_digits("1foo"), Ok(("foo", "1")));
    assert_eq!(parsing::match_one_or_two_digits("12foo"), Ok(("foo", "12")));
    assert_eq!(
        parsing::match_one_or_two_digits("123foo"),
        Ok(("3foo", "12"))
    );
    assert_eq!(
        parsing::match_one_or_two_digits("1234foo"),
        Ok(("34foo", "12"))
    );
}
