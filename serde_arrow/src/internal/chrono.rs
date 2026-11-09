//! Support for Parsing datetime related quantities
use marrow::{datatypes::TimeUnit, types::MonthDayNanoInterval};

use crate::internal::error::Result;

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

/// Check whether an Arrow timezone string denotes UTC (a zero offset).
///
/// Arrow encodes every timezone-aware timestamp as a UTC instant; the timezone
/// string only records how to display it. Recognize the name `UTC` and the fixed
/// zero-offset designators.
pub fn is_utc_timezone(tz: &str) -> bool {
    tz.eq_ignore_ascii_case("utc")
        || matches!(tz, "Z" | "z" | "+00:00" | "-00:00" | "+0000" | "-0000")
}

/// Parse `s` as a span
pub fn parse_span(s: &str) -> Result<Span<'_>> {
    parsing::match_span(s).into_result("Span")
}

impl parsing::Span<'_> {
    /// Convert the `Span` into an `i64`` with the given `unit`
    pub fn to_arrow_duration(&self, unit: TimeUnit) -> Result<i64> {
        if get_optional_digit_value(self.year)? != 0 || get_optional_digit_value(self.month)? != 0 {
            fail!("cannot convert span with years or months to a fixed duration");
        }

        let second_value = self.get_second_value()?;
        let nanosecond_value = self.get_nanosecond_value()?;
        Self::build_duration(self.sign, second_value, nanosecond_value, unit)
    }

    fn get_second_value(&self) -> Result<i64> {
        Ok(get_optional_digit_value(self.week)? * 7 * 24 * 60 * 60
            + get_optional_digit_value(self.day)? * 24 * 60 * 60
            + get_optional_digit_value(self.hour)? * 60 * 60
            + get_optional_digit_value(self.minute)? * 60
            + get_optional_digit_value(self.second)?)
    }

    fn get_nanosecond_value(&self) -> Result<i64> {
        let Some(subsecond) = self.subsecond else {
            return Ok(0);
        };
        let subsecond_val: i64 = subsecond.parse()?;
        let subsecond_len = u32::try_from(subsecond.len())?;

        if subsecond_len <= 9 {
            Ok(subsecond_val * 10_i64.pow(9 - subsecond_len))
        } else {
            Ok(subsecond_val / 10_i64.pow(subsecond_len - 9))
        }
    }

    fn build_duration(
        sign: Option<char>,
        second_value: i64,
        nanosecond_value: i64,
        unit: TimeUnit,
    ) -> Result<i64> {
        let unsigned_duration = match unit {
            TimeUnit::Second => second_value,
            TimeUnit::Millisecond => match second_value.checked_mul(1_000_i64) {
                Some(res) => res + nanosecond_value / 1_000_000,
                None => {
                    fail!("cannot represent {second_value} seconds with Millisecond resolution")
                }
            },
            TimeUnit::Microsecond => match second_value.checked_mul(1_000_000_i64) {
                Some(res) => res + nanosecond_value / 1_000,
                None => {
                    fail!("cannot represent {second_value} seconds with Microsecond resolution")
                }
            },
            TimeUnit::Nanosecond => match second_value.checked_mul(1_000_000_000_i64) {
                Some(res) => res + nanosecond_value,
                None => fail!("cannot represent {second_value} seconds with Nanosecond resolution"),
            },
        };

        if sign == Some('-') {
            Ok(-unsigned_duration)
        } else {
            Ok(unsigned_duration)
        }
    }

    /// Convert the `Span` into an Arrow `MonthDayNano` interval
    ///
    /// Years are folded into months, weeks into days and the time units into nanoseconds.
    /// Subsecond digits beyond nanoseconds are truncated.
    pub fn to_arrow_interval(&self) -> Result<MonthDayNanoInterval> {
        fn value(s: Option<&str>) -> Result<i128> {
            Ok(i128::from(get_optional_digit_value(s)?))
        }

        fn convert<T: TryFrom<i128>>(name: &str, value: i128) -> Result<T> {
            match T::try_from(value) {
                Ok(value) => Ok(value),
                Err(_) => fail!("{name} value {value} is out of range for Interval(MonthDayNano)"),
            }
        }

        // sums and products of i64 values with these factors cannot overflow i128
        let sign = if self.sign == Some('-') { -1 } else { 1 };
        let months = sign * (value(self.year)? * 12 + value(self.month)?);
        let days = sign * (value(self.week)? * 7 + value(self.day)?);
        let seconds = value(self.hour)? * 3_600 + value(self.minute)? * 60 + value(self.second)?;
        let nanoseconds =
            sign * (seconds * 1_000_000_000 + i128::from(self.get_nanosecond_value()?));

        Ok(MonthDayNanoInterval {
            months: convert("months", months)?,
            days: convert("days", days)?,
            nanoseconds: convert("nanoseconds", nanoseconds)?,
        })
    }
}

/// Format an Arrow `MonthDayNano` interval as a Span string
///
/// Spans carry a single sign, intervals whose components differ in sign cannot be formatted.
pub fn format_arrow_interval_as_span(value: MonthDayNanoInterval) -> Result<String> {
    use std::fmt::Write;

    let MonthDayNanoInterval {
        months,
        days,
        nanoseconds,
    } = value;
    let components = [i64::from(months), i64::from(days), nanoseconds];
    let is_negative = components.iter().any(|value| *value < 0);
    if is_negative && components.iter().any(|value| *value > 0) {
        fail!("cannot format interval components with different signs as a span");
    }

    let mut result = String::from(if is_negative { "-P" } else { "P" });
    if months != 0 {
        write!(result, "{}m", months.unsigned_abs())?;
    }
    if days != 0 {
        write!(result, "{}d", days.unsigned_abs())?;
    }
    if nanoseconds != 0 {
        let nanoseconds = nanoseconds.unsigned_abs();
        write!(
            result,
            "T{}.{:09}s",
            nanoseconds / 1_000_000_000,
            nanoseconds % 1_000_000_000
        )?;
    } else if months == 0 && days == 0 {
        result.push_str("T0s");
    }
    Ok(result)
}

/// Format a duration in the given unit as a Span string
pub fn format_arrow_duration_as_span(value: i64, unit: TimeUnit) -> String {
    let (value, sign) = if value < 0 {
        (-value, "-")
    } else {
        (value, "")
    };

    match unit {
        TimeUnit::Second => format!("{sign}PT{value}s"),
        TimeUnit::Millisecond => format!(
            "{sign}PT{second}.{subsecond:03}s",
            second = value / 1_000,
            subsecond = value % 1_000
        ),
        TimeUnit::Microsecond => format!(
            "{sign}PT{second}.{subsecond:06}s",
            second = value / 1_000_000,
            subsecond = value % 1_000_000
        ),
        TimeUnit::Nanosecond => format!(
            "{sign}PT{second}.{subsecond:09}s",
            second = value / 1_000_000_000,
            subsecond = value % 1_000_000_000
        ),
    }
}

fn get_optional_digit_value(s: Option<&str>) -> Result<i64> {
    match s {
        Some(s) => Ok(s.parse()?),
        None => Ok(0),
    }
}

/// Minimalistic monadic parsers for datetime objects
///
/// Each parser has the following interface:
///
/// `fn (string_to_parse, ..extra_args) -> Result<(rest, result), unmatched_string>`
///
mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    pub trait ParseResult {
        type Output;

        fn matches(&self) -> bool;
        fn into_result(self, output_type: &str) -> crate::internal::error::Result<Self::Output>;
    }

    impl<R> ParseResult for Result<(&str, R), &str> {
        type Output = R;

        fn matches(&self) -> bool {
            match self {
                Ok((rest, _)) => rest.is_empty(),
                Err(_) => false,
            }
        }

        fn into_result(self, output_type: &str) -> crate::internal::error::Result<Self::Output> {
            match self {
                Ok(("", output)) => Ok(output),
                Ok((unmatched, _)) | Err(unmatched) => crate::internal::error::fail!(
                    "could not parse the string as {output_type}, unmatched content: {unmatched:?}"
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
        pub sign: Option<char>,
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
        let (s, sign) = match_optional_sign(s)?;
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
                sign,
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
        for prefix in ["Z", "z", "+0000", "-0000", "+00:00", "-00:00"] {
            if let Some((prefix, rest)) = split_prefix(s, prefix) {
                return Ok((rest, prefix));
            }
        }
        Err(s)
    }

    fn split_prefix<'a>(s: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
        let rest = s.strip_prefix(prefix)?;
        let prefix = s
            .get(..prefix.len())
            .unwrap_or_else(|| unreachable!("s starts with prefix"));

        Some((prefix, rest))
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

        let prefix_len = s.len() - rest.len();
        let prefix = s
            .get(..prefix_len)
            .unwrap_or_else(|| unreachable!("rest ends s"));

        Ok((rest, prefix))
    }

    pub fn match_one_or_two_digits(s: &str) -> Result<(&str, &str), &str> {
        let rest = s.strip_prefix(DIGIT).ok_or(s)?;
        let rest = rest.strip_prefix(DIGIT).unwrap_or(rest);
        let prefix_len = s.len() - rest.len();
        let prefix = s
            .get(..prefix_len)
            .unwrap_or_else(|| unreachable!("rest ends s"));

        Ok((rest, prefix))
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
    assert_eq!(
        parsing::match_span("+P3dT4h59m"),
        Ok((
            "",
            parsing::Span {
                sign: Some('+'),
                day: Some("3"),
                hour: Some("4"),
                minute: Some("59"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("-P1w4d"),
        Ok((
            "",
            parsing::Span {
                sign: Some('-'),
                week: Some("1"),
                day: Some("4"),
                ..Default::default()
            }
        ))
    );
    assert_eq!(
        parsing::match_span("PT0.020s"),
        Ok((
            "",
            parsing::Span {
                second: Some("0"),
                subsecond: Some("020"),
                ..Default::default()
            }
        ))
    )
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

#[test]
fn test_parse_and_format_duration() {
    fn parse_as_duration(s: &str, unit: TimeUnit) -> i64 {
        parse_span(s).unwrap().to_arrow_duration(unit).unwrap()
    }

    assert_eq!(format_arrow_duration_as_span(20, TimeUnit::Second), "PT20s");
    assert_eq!(
        format_arrow_duration_as_span(20, TimeUnit::Millisecond),
        "PT0.020s"
    );
    assert_eq!(
        format_arrow_duration_as_span(20, TimeUnit::Microsecond),
        "PT0.000020s"
    );
    assert_eq!(
        format_arrow_duration_as_span(20, TimeUnit::Nanosecond),
        "PT0.000000020s"
    );

    assert_eq!(parse_as_duration("PT20s", TimeUnit::Second), 20);
    assert_eq!(parse_as_duration("PT0.020s", TimeUnit::Millisecond), 20);
    assert_eq!(parse_as_duration("PT0.000020s", TimeUnit::Microsecond), 20);
    assert_eq!(
        parse_as_duration("PT0.000000020s", TimeUnit::Nanosecond),
        20
    );

    assert_eq!(
        format_arrow_duration_as_span(-13, TimeUnit::Second),
        "-PT13s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-13, TimeUnit::Millisecond),
        "-PT0.013s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-13, TimeUnit::Microsecond),
        "-PT0.000013s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-13, TimeUnit::Nanosecond),
        "-PT0.000000013s"
    );

    assert_eq!(parse_as_duration("-PT13s", TimeUnit::Second), -13);
    assert_eq!(parse_as_duration("-PT0.013s", TimeUnit::Millisecond), -13);
    assert_eq!(
        parse_as_duration("-PT0.000013s", TimeUnit::Microsecond),
        -13
    );
    assert_eq!(
        parse_as_duration("-PT0.000000013s", TimeUnit::Nanosecond),
        -13
    );

    assert_eq!(
        format_arrow_duration_as_span(1234, TimeUnit::Second),
        "PT1234s"
    );
    assert_eq!(
        format_arrow_duration_as_span(1234, TimeUnit::Millisecond),
        "PT1.234s"
    );
    assert_eq!(
        format_arrow_duration_as_span(1234, TimeUnit::Microsecond),
        "PT0.001234s"
    );
    assert_eq!(
        format_arrow_duration_as_span(1234, TimeUnit::Nanosecond),
        "PT0.000001234s"
    );

    assert_eq!(parse_as_duration("PT1234s", TimeUnit::Second), 1234);
    assert_eq!(parse_as_duration("PT1.234s", TimeUnit::Millisecond), 1234);
    assert_eq!(
        parse_as_duration("PT0.001234s", TimeUnit::Microsecond),
        1234
    );
    assert_eq!(
        parse_as_duration("PT0.000001234s", TimeUnit::Nanosecond),
        1234
    );

    assert_eq!(
        format_arrow_duration_as_span(-2010, TimeUnit::Second),
        "-PT2010s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-2010, TimeUnit::Millisecond),
        "-PT2.010s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-2010, TimeUnit::Microsecond),
        "-PT0.002010s"
    );
    assert_eq!(
        format_arrow_duration_as_span(-2010, TimeUnit::Nanosecond),
        "-PT0.000002010s"
    );

    assert_eq!(parse_as_duration("-PT2010s", TimeUnit::Second), -2010);
    assert_eq!(parse_as_duration("-PT2.010s", TimeUnit::Millisecond), -2010);
    assert_eq!(
        parse_as_duration("-PT0.002010s", TimeUnit::Microsecond),
        -2010
    );
    assert_eq!(
        parse_as_duration("-PT0.000002010s", TimeUnit::Nanosecond),
        -2010
    );

    assert_eq!(
        format_arrow_duration_as_span(123456789, TimeUnit::Second),
        "PT123456789s"
    );
    assert_eq!(
        format_arrow_duration_as_span(123456789, TimeUnit::Millisecond),
        "PT123456.789s"
    );
    assert_eq!(
        format_arrow_duration_as_span(123456789, TimeUnit::Microsecond),
        "PT123.456789s"
    );
    assert_eq!(
        format_arrow_duration_as_span(123456789, TimeUnit::Nanosecond),
        "PT0.123456789s"
    );

    assert_eq!(
        parse_as_duration("PT123456789s", TimeUnit::Second),
        123456789
    );
    assert_eq!(
        parse_as_duration("PT123456.789s", TimeUnit::Millisecond),
        123456789
    );
    assert_eq!(
        parse_as_duration("PT123.456789s", TimeUnit::Microsecond),
        123456789
    );
    assert_eq!(
        parse_as_duration("PT0.123456789s", TimeUnit::Nanosecond),
        123456789
    );
}

#[test]
fn test_parse_and_format_interval() {
    fn parse_as_interval(s: &str) -> (i32, i32, i64) {
        let value = parse_span(s).unwrap().to_arrow_interval().unwrap();
        (value.months, value.days, value.nanoseconds)
    }

    fn format_interval(months: i32, days: i32, nanoseconds: i64) -> String {
        format_arrow_interval_as_span(MonthDayNanoInterval {
            months,
            days,
            nanoseconds,
        })
        .unwrap()
    }

    assert_eq!(
        parse_as_interval("P1Y2M3W4DT5H6M7.123456789S"),
        (14, 25, 18_367_123_456_789)
    );
    assert_eq!(parse_as_interval("-PT1h"), (0, 0, -3_600_000_000_000));
    assert_eq!(parse_as_interval("PT0.0000000019s"), (0, 0, 1));
    assert_eq!(parse_as_interval("PT0s"), (0, 0, 0));

    assert_eq!(
        format_interval(14, 25, 18_367_123_456_789),
        "P14m25dT18367.123456789s"
    );
    assert_eq!(format_interval(0, 0, 0), "PT0s");
    assert_eq!(format_interval(-1, 0, 0), "-P1m");
    assert_eq!(format_interval(0, 3, 0), "P3d");
    assert_eq!(
        format_interval(i32::MIN, i32::MIN, i64::MIN),
        "-P2147483648m2147483648dT9223372036.854775808s"
    );

    parse_span("P2147483648m")
        .unwrap()
        .to_arrow_interval()
        .unwrap_err();
    format_arrow_interval_as_span(MonthDayNanoInterval {
        months: 1,
        days: -1,
        nanoseconds: 0,
    })
    .unwrap_err();
}
