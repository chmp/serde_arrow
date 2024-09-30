pub fn matches_naive_datetime(s: &str) -> bool {
    eval_parser(parsing::match_naive_datetime, s)
}

pub fn matches_utc_datetime(s: &str) -> bool {
    eval_parser(parsing::match_utc_datetime, s)
}

pub fn matches_naive_date(s: &str) -> bool {
    eval_parser(parsing::match_naive_date, s)
}

pub fn matches_naive_time(s: &str) -> bool {
    eval_parser(parsing::match_naive_time, s)
}

fn eval_parser<F: Fn(&str) -> Result<&str, &str>>(parser: F, s: &str) -> bool {
    parser(s.trim()).map(str::is_empty).unwrap_or_default()
}

/// minimalistic monadic parser
///
/// Returns the Err(unmatched_string) on error and Ok(rest) on success
mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    pub fn match_optional_sign(s: &str) -> Result<&str, &str> {
        Ok(s.strip_prefix(['+', '-']).unwrap_or(s))
    }

    pub fn match_one_or_more_digits(s: &str) -> Result<&str, &str> {
        let mut s = s.strip_prefix(DIGIT).ok_or(s)?;
        while let Some(new_s) = s.strip_prefix(DIGIT) {
            s = new_s;
        }
        Ok(s)
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
        let s = match_optional_sign(s)?;
        let s = match_one_or_more_digits(s)?;
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
            match_one_or_more_digits(s)
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
