//! Decimal support
//!
//! Decimals are stored either as 128 or 256 bit integers. They are
//! characterized by a scale, the total number of digits, and the precision, the
//! position of the decimal point.

use crate::internal::error::{fail, Result};

pub fn parse_decimal(s: &[u8], scale: u8, precision: i8, truncate: bool) -> Result<i128> {
    let mut buffer = [0; 64];
    DecimalParser::new(scale, precision, truncate).parse_decimal128(&mut buffer, s)
}

/// Helper to parse decimals
///
/// This enum maps the tree major cases:
///
/// - integer only: ` ----XXX----.----`
/// - fraction only: `------.---XXX---`
/// - mixed: `-----XXX.XXX----`
#[derive(Debug, Clone, Copy)]
enum DecimalParser {
    IntegerOnly(usize, usize),
    IntegerOnlyTruncated(usize, usize),
    Mixed(usize, usize),
    MixedTruncated(usize, usize),
    FractionOnly(usize, usize),
    FractionOnlyTruncated(usize, usize),
}

impl DecimalParser {
    pub fn new(scale: u8, precision: i8, truncated: bool) -> Self {
        if precision <= 0 {
            if !truncated {
                Self::IntegerOnly(scale as usize, -precision as usize)
            } else {
                Self::IntegerOnlyTruncated(scale as usize, -precision as usize)
            }
        } else if (precision as usize) < (scale as usize) {
            if !truncated {
                Self::Mixed(scale as usize, precision as usize)
            } else {
                Self::MixedTruncated(scale as usize, precision as usize)
            }
        } else {
            if !truncated {
                Self::FractionOnly(scale as usize, precision as usize)
            } else {
                Self::FractionOnlyTruncated(scale as usize, precision as usize)
            }
        }
    }

    pub fn parse_decimal128(self, buffer: &mut [u8; 64], s: &[u8]) -> Result<i128> {
        let (s, sign) = parse_sign(s);
        let val: i128 = self.copy_digits(buffer, s)?.parse()?;
        let val = sign.apply_i128(val);
        Ok(val)
    }

    pub fn copy_digits<'b>(self, buffer: &'b mut [u8; 64], s: &[u8]) -> Result<&'b str> {
        use DecimalParser::*;
        match self {
            IntegerOnly(scale, precision) => {
                copy_digits_integer_only(buffer, s, scale, precision, false)
            }
            IntegerOnlyTruncated(scale, precision) => {
                copy_digits_integer_only(buffer, s, scale, precision, true)
            }
            Mixed(scale, precision) => copy_digits_mixed(buffer, s, scale, precision, false),
            MixedTruncated(scale, precision) => {
                copy_digits_mixed(buffer, s, scale, precision, true)
            }
            FractionOnly(scale, precision) => {
                copy_digits_fraction_only(buffer, s, scale, precision, false)
            }
            FractionOnlyTruncated(scale, precision) => {
                copy_digits_fraction_only(buffer, s, scale, precision, true)
            }
        }
    }
}

fn parse_sign(s: &[u8]) -> (&[u8], Sign) {
    match s.first() {
        Some(b'+') => (&s[1..], Sign::Plus),
        Some(b'-') => (&s[1..], Sign::Minus),
        _ => (s, Sign::None),
    }
}

#[derive(Debug, Copy, Clone)]
enum Sign {
    Minus,
    Plus,
    None,
}

impl Sign {
    fn apply_i128(self, val: i128) -> i128 {
        match self {
            Self::Minus => -val,
            _ => val,
        }
    }
}

fn copy_digits_integer_only<'b>(
    buffer: &'b mut [u8; 64],
    s: &[u8],
    scale: usize,
    precision: usize,
    truncate: bool,
) -> Result<&'b str> {
    let (before_period, after_period) = find_period(s);
    let end_copy = before_period.saturating_sub(precision);
    let start_copy = end_copy.saturating_sub(scale);

    check_all_ascii_zero(&s[0..start_copy])?;
    if !truncate {
        check_all_ascii_digit(&s[start_copy..end_copy])?;
        check_all_ascii_zero(&s[end_copy..before_period])?;
        check_all_ascii_zero(&s[after_period..s.len()])?;
    } else {
        check_all_ascii_digit(&s[start_copy..before_period])?;
        check_all_ascii_digit(&s[after_period..s.len()])?;
    }

    buffer[0..end_copy - start_copy].copy_from_slice(&s[start_copy..end_copy]);
    let res = std::str::from_utf8(&buffer[..end_copy - start_copy]).unwrap();
    Ok(res)
}

fn copy_digits_fraction_only<'b>(
    buffer: &'b mut [u8; 64],
    s: &[u8],
    scale: usize,
    precision: usize,
    truncate: bool,
) -> Result<&'b str> {
    debug_assert!(precision >= scale);

    let (before_period, after_period) = find_period(s);
    let start_copy = std::cmp::min(s.len(), after_period + precision - scale);
    let end_copy = std::cmp::min(s.len(), after_period + precision);
    let fill = scale - (end_copy - start_copy);

    check_all_ascii_zero(&s[0..before_period])?;
    check_all_ascii_zero(&s[after_period..start_copy])?;

    if !truncate {
        check_all_ascii_digit(&s[start_copy..end_copy])?;
        check_all_ascii_zero(&s[end_copy..s.len()])?;
    } else {
        check_all_ascii_digit(&s[start_copy..s.len()])?;
    }

    buffer[0..end_copy - start_copy].copy_from_slice(&s[start_copy..end_copy]);
    buffer[end_copy - start_copy..][..fill].fill(b'0');

    let res = std::str::from_utf8(&buffer[..end_copy - start_copy + fill]).unwrap();
    Ok(res)
}

fn copy_digits_mixed<'b>(
    buffer: &'b mut [u8; 64],
    s: &[u8],
    scale: usize,
    precision: usize,
    truncate: bool,
) -> Result<&'b str> {
    debug_assert!(precision < scale);

    let (before_period, after_period) = find_period(s);
    let start_copy = before_period.saturating_sub(scale - precision);
    let end_copy = std::cmp::min(s.len(), after_period + precision);

    let copy_1 = start_copy..before_period;
    let copy_2 = after_period..end_copy;
    let fill = precision - (end_copy - after_period);

    check_all_ascii_zero(&s[0..start_copy])?;
    check_all_ascii_digit(&s[copy_1.clone()])?;
    if !truncate {
        check_all_ascii_digit(&s[after_period..end_copy])?;
        check_all_ascii_zero(&s[end_copy..s.len()])?;
    } else {
        check_all_ascii_digit(&s[after_period..s.len()])?;
    }

    buffer[0..copy_1.len()].copy_from_slice(&s[copy_1.clone()]);
    buffer[copy_1.len()..][..copy_2.len()].copy_from_slice(&s[copy_2.clone()]);
    buffer[copy_1.len() + copy_2.len()..][..fill].fill(b'0');

    let res = std::str::from_utf8(&buffer[..copy_1.len() + copy_2.len() + fill]).unwrap();
    Ok(res)
}

fn find_period(s: &[u8]) -> (usize, usize) {
    if let Some(pos) = s.iter().position(|b| *b == b'.') {
        (pos, pos + 1)
    } else {
        (s.len(), s.len())
    }
}

fn check_all_ascii_zero(s: &[u8]) -> Result<()> {
    if s.iter().any(|c| *c != b'0') {
        fail!("invalid decimal");
    }
    Ok(())
}

fn check_all_ascii_digit(s: &[u8]) -> Result<()> {
    if s.iter().any(|c| *c < b'0' || *c > b'9') {
        fail!("invalid decimal");
    }
    Ok(())
}

#[test]
fn test_missing_number() {
    assert!(parse_decimal(b"", 5, 0, false).is_err());
    assert!(parse_decimal(b"+", 5, 0, false).is_err());
    assert!(parse_decimal(b"-", 5, 0, false).is_err());
}

#[test]
fn test_insufficient_scale_missing_number() {
    assert!(parse_decimal(b"123", 2, 0, false).is_err());
}

#[test]
fn test_examples_precision_0() {
    assert_eq!(parse_decimal(b"0", 5, 0, false), Ok(0_i128));
    assert_eq!(parse_decimal(b"1", 5, 0, false), Ok(1_i128));
    assert_eq!(parse_decimal(b"2", 5, 0, false), Ok(2_i128));
    assert_eq!(parse_decimal(b"3", 5, 0, false), Ok(3_i128));
    assert_eq!(parse_decimal(b"4", 5, 0, false), Ok(4_i128));
    assert_eq!(parse_decimal(b"5", 5, 0, false), Ok(5_i128));
    assert_eq!(parse_decimal(b"6", 5, 0, false), Ok(6_i128));
    assert_eq!(parse_decimal(b"7", 5, 0, false), Ok(7_i128));
    assert_eq!(parse_decimal(b"8", 5, 0, false), Ok(8_i128));
    assert_eq!(parse_decimal(b"9", 5, 0, false), Ok(9_i128));

    assert_eq!(parse_decimal(b"123", 5, 0, false), Ok(123_i128));
    assert_eq!(parse_decimal(b"42", 5, 0, false), Ok(42_i128));
    assert_eq!(parse_decimal(b"13", 5, 0, false), Ok(13_i128));
    assert_eq!(
        parse_decimal(b"9876543210", 10, 0, false),
        Ok(9876543210_i128)
    );
}

#[test]
fn test_negative_precision() {
    assert_eq!(
        parse_decimal(b"9876543210", 10, -1, false),
        Ok(987654321_i128)
    );
    assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
    assert_eq!(parse_decimal(b"2100", 10, -2, false), Ok(21_i128));
    assert_eq!(parse_decimal(b"21000", 10, -3, false), Ok(21_i128));
}

#[test]
fn test_negative_precision_truncation() {
    assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
    assert!(parse_decimal(b"213", 10, -1, false).is_err());
    assert_eq!(parse_decimal(b"213", 10, -1, true), Ok(21_i128));
}

#[test]
fn test_positive_precision_fraction() {
    assert_eq!(parse_decimal(b"13.0", 10, 1, false), Ok(130_i128));
    assert_eq!(parse_decimal(b"13.1", 10, 1, false), Ok(131_i128));
    assert_eq!(parse_decimal(b"13.2", 10, 1, false), Ok(132_i128));
    assert_eq!(parse_decimal(b"13.3", 10, 1, false), Ok(133_i128));
    assert_eq!(parse_decimal(b"13.4", 10, 1, false), Ok(134_i128));
    assert_eq!(parse_decimal(b"13.5", 10, 1, false), Ok(135_i128));
    assert_eq!(parse_decimal(b"13.6", 10, 1, false), Ok(136_i128));
    assert_eq!(parse_decimal(b"13.7", 10, 1, false), Ok(137_i128));
    assert_eq!(parse_decimal(b"13.8", 10, 1, false), Ok(138_i128));
    assert_eq!(parse_decimal(b"13.9", 10, 1, false), Ok(139_i128));

    assert_eq!(parse_decimal(b"+21.4", 10, 1, false), Ok(214_i128));
    assert_eq!(parse_decimal(b"-42.500", 10, 1, false), Ok(-425_i128));

    assert_eq!(parse_decimal(b"13.120", 10, 2, false), Ok(1312_i128));
    assert_eq!(parse_decimal(b"+21.45000", 10, 2, false), Ok(2145_i128));
    assert_eq!(parse_decimal(b"-42.00", 10, 2, false), Ok(-4200_i128));

    assert_eq!(parse_decimal(b"13.123", 10, 3, false), Ok(13123_i128));
    assert_eq!(parse_decimal(b"+21.123000", 10, 3, false), Ok(21123_i128));
    assert_eq!(parse_decimal(b"-42.1200", 10, 3, false), Ok(-42120_i128));
    assert_eq!(parse_decimal(b"13.12", 10, 3, false), Ok(13120_i128));
}

#[test]
fn test_positive_precision_fraction_truncation() {
    assert_eq!(parse_decimal(b"13.2", 10, 1, false), Ok(132_i128));
    assert_eq!(parse_decimal(b"-42.500", 10, 1, false), Ok(-425_i128));
    assert!(parse_decimal(b"-42.560", 10, 1, false).is_err());
    assert_eq!(parse_decimal(b"-42.560", 10, 1, true), Ok(-425_i128));
    assert_eq!(parse_decimal(b"-42.567", 10, 1, true), Ok(-425_i128));
}

#[test]
fn test_copy_digits() {
    fn copy_digits_str(s: &str, scale: u8, precision: i8) -> Result<String> {
        let mut buffer = [0; 64];
        let res =
            DecimalParser::new(scale, precision, false).copy_digits(&mut buffer, s.as_bytes())?;
        Ok(res.to_string())
    }

    assert_eq!(copy_digits_str("0", 1, 0).unwrap(), "0");
    assert_eq!(copy_digits_str("1", 1, 0).unwrap(), "1");
    assert_eq!(copy_digits_str("5", 1, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("5", 2, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("5", 3, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("125", 3, 0).unwrap(), "125");
    assert_eq!(copy_digits_str("12300", 3, -2).unwrap(), "123");
    assert_eq!(copy_digits_str("5000", 1, -3).unwrap(), "5");

    assert_eq!(copy_digits_str("0.0", 1, 0).unwrap(), "0");
    assert_eq!(copy_digits_str("1.0", 1, 0).unwrap(), "1");
    assert_eq!(copy_digits_str("5.0", 1, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("5.00", 2, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("5.0000", 3, 0).unwrap(), "5");
    assert_eq!(copy_digits_str("125.00", 3, 0).unwrap(), "125");
    assert_eq!(copy_digits_str("12300.00000", 3, -2).unwrap(), "123");
    assert_eq!(copy_digits_str("5000.0000", 1, -3).unwrap(), "5");

    assert_eq!(copy_digits_str("0", 2, 2).unwrap(), "00");
    assert_eq!(copy_digits_str("0.", 2, 2).unwrap(), "00");
    assert_eq!(copy_digits_str("0.01", 2, 2).unwrap(), "01");
    assert_eq!(copy_digits_str("0.10", 2, 2).unwrap(), "10");
    assert_eq!(copy_digits_str("0.1", 2, 2).unwrap(), "10");

    assert_eq!(copy_digits_str("0.01", 2, 3).unwrap(), "10");
    assert_eq!(copy_digits_str("0.012", 2, 3).unwrap(), "12");
    assert_eq!(copy_digits_str("0.007", 2, 3).unwrap(), "07");

    assert_eq!(copy_digits_str("01.230", 3, 2).unwrap(), "123");
    assert_eq!(copy_digits_str("1.230", 3, 2).unwrap(), "123");
    assert_eq!(copy_digits_str("1.23", 3, 2).unwrap(), "123");
    assert_eq!(copy_digits_str("1.2", 3, 2).unwrap(), "120");
    assert_eq!(copy_digits_str("1.", 3, 2).unwrap(), "100");

    assert_eq!(copy_digits_str("21.21", 4, 2).unwrap(), "2121");
    assert_eq!(copy_digits_str("2", 4, 2).unwrap(), "200");
    assert_eq!(copy_digits_str("20", 4, 2).unwrap(), "2000");
    assert_eq!(copy_digits_str("42.00", 4, 2).unwrap(), "4200");
}
