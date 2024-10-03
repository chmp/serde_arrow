//! Support for Parsing datetime related quantities
//!

pub fn matches_naive_datetime(s: &str) -> bool {
    parsing::match_naive_datetime(s)
        .map(|(rest, _)| rest.is_empty())
        .unwrap_or_default()
}

pub fn matches_utc_datetime(s: &str) -> bool {
    parsing::match_utc_datetime(s)
        .map(|(rest, _)| rest.is_empty())
        .unwrap_or_default()
}

pub fn matches_naive_date(s: &str) -> bool {
    parsing::match_naive_date(s)
        .map(|(rest, _)| rest.is_empty())
        .unwrap_or_default()
}

pub fn matches_naive_time(s: &str) -> bool {
    parsing::match_naive_time(s)
        .map(|(rest, _)| rest.is_empty())
        .unwrap_or_default()
}

/// minimalistic monadic parsers for datetime objects
///
/// Each parser has the the following interface:
///
/// `fn (string_to_parser, ..extra_args) -> Result<(rest, result), unmatched_string>`
///
mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Date<'a> {
        pub sign: Option<char>,
        pub year: &'a str,
        pub month: &'a str,
        pub day: &'a str,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Time<'a> {
        pub hour: &'a str,
        pub minute: &'a str,
        pub second: &'a str,
        pub subsecond: Option<&'a str>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DateTime<'a> {
        pub date: Date<'a>,
        pub time: Time<'a>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DateTimeUtc<'a> {
        pub date: Date<'a>,
        pub time: Time<'a>,
        pub timezone: &'a str,
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
