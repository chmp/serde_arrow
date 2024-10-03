//! Support for Parsing datetime related quantities
//!

pub fn matches_naive_datetime(s: &str) -> bool {
    match_parser(parsing::match_naive_datetime, s)
}

pub fn matches_utc_datetime(s: &str) -> bool {
    match_parser(parsing::match_utc_datetime, s)
}

pub fn matches_naive_date(s: &str) -> bool {
    match_parser(parsing::match_naive_date, s)
}

pub fn matches_naive_time(s: &str) -> bool {
    match_parser(parsing::match_naive_time, s)
}

fn match_parser<F: Fn(&str) -> Result<&str, &str>>(parser: F, s: &str) -> bool {
    parser(s.trim()).map(str::is_empty).unwrap_or_default()
}

/// minimalistic monadic parsers for datetime objects
///
/// Each parser has the the following interface:
///
/// `fn (string_to_parser, ..extra_args) -> Result<(rest, result), unmatched_string>`
///
mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

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

    pub fn match_one_or_two_digits(s: &str) -> Result<&str, &str> {
        let s = s.strip_prefix(DIGIT).ok_or(s)?;
        Ok(s.strip_prefix(DIGIT).unwrap_or(s))
    }

    pub fn match_char(s: &str, c: char) -> Result<&str, &str> {
        s.strip_prefix(c).ok_or(s)
    }

    pub fn match_naive_datetime_with_sep<'a>(
        s: &'a str,
        sep: &'_ [char],
    ) -> Result<&'a str, &'a str> {
        let s = match_naive_date(s)?;
        let s = s.strip_prefix(sep).ok_or(s)?;
        match_naive_time(s)
    }

    pub fn match_naive_date(s: &str) -> Result<&str, &str> {
        let (s, _sign) = match_optional_sign(s)?;
        let (s, _year) = match_one_or_more_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, '-')?;
        match_one_or_two_digits(s)
    }

    pub fn match_naive_time(s: &str) -> Result<&str, &str> {
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;

        if let Some(s) = s.strip_prefix('.') {
            let (s, _subsecond) = match_one_or_more_digits(s)?;
            Ok(s)
        } else {
            Ok(s)
        }
    }

    pub fn match_naive_datetime(s: &str) -> Result<&str, &str> {
        match_naive_datetime_with_sep(s, &['T'])
    }

    pub fn match_utc_datetime(s: &str) -> Result<&str, &str> {
        let s = match_naive_datetime_with_sep(s, &['T', ' '])?;

        if let Some(s) = s.strip_prefix('Z') {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+0000") {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+00:00") {
            Ok(s)
        } else {
            Err(s)
        }
    }
}

#[test]
fn test_match_naive_datetime() {
    // chrono examples
    assert_eq!(parsing::match_naive_datetime("2015-09-18T23:56:04"), Ok(""));
    assert_eq!(
        parsing::match_naive_datetime("+12345-6-7T7:59:60.5"),
        Ok("")
    );
}

#[test]
fn test_match_utc_datetime() {
    // examples from the chrono docs
    assert_eq!(parsing::match_utc_datetime("2012-12-12T12:12:12Z"), Ok(""));
    assert_eq!(parsing::match_utc_datetime("2012-12-12 12:12:12Z"), Ok(""));
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12 12:12:12+0000"),
        Ok("")
    );
    assert_eq!(
        parsing::match_utc_datetime("2012-12-12 12:12:12+00:00"),
        Ok("")
    );
}

#[test]
fn test_match_naive_date() {
    assert_eq!(parsing::match_naive_date("+12345-6-7"), Ok(""));
    assert_eq!(parsing::match_naive_date("2015-09-18"), Ok(""));
    assert_eq!(parsing::match_naive_date("-20-21-22"), Ok(""));
}

#[test]
fn test_match_naive_time() {
    assert_eq!(parsing::match_naive_time("23:00:12"), Ok(""));
    assert_eq!(parsing::match_naive_time("23:00:12.999"), Ok(""));
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
