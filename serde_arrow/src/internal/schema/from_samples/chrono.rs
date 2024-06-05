pub fn matches_naive_datetime(s: &str) -> bool {
    parsing::matches_naive_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

pub fn matches_utc_datetime(s: &str) -> bool {
    parsing::matches_utc_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

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

    pub fn matches_naive_datetime_with_sep<'a>(
        s: &'a str,
        sep: &'_ [char],
    ) -> Result<&'a str, &'a str> {
        let s = s.trim();
        let s = match_optional_sign(s)?;
        let s = match_one_or_more_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = s.strip_prefix(sep).ok_or(s)?;
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

    pub fn matches_naive_datetime(s: &str) -> Result<&str, &str> {
        matches_naive_datetime_with_sep(s, &['T'])
    }

    pub fn matches_utc_datetime(s: &str) -> Result<&str, &str> {
        let s = matches_naive_datetime_with_sep(s, &['T', ' '])?;

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

#[cfg(test)]
mod test_matches_naive_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_naive_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_naive_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2015-09-18T23:56:04", true),
        (example_chrono_docs_2, "+12345-6-7T7:59:60.5", true),
        (surrounding_space, "   2015-09-18T23:56:04   ", true),
    );
}

#[cfg(test)]
mod test_matches_utc_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_utc_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_utc_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2012-12-12T12:12:12Z", true),
        (example_chrono_docs_2, "2012-12-12 12:12:12Z", true),
        (example_chrono_docs_3, "2012-12-12 12:12:12+0000", true),
        (example_chrono_docs_4, "2012-12-12 12:12:12+00:00", true),
    );
}
