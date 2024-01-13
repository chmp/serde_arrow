//! Decimal support
//!
//! Decimals are stored either as 128 or 256 bit integers. They are
//! characterized by a precision, the total number of digits, and the scale, the
//! position of the decimal point.

use crate::internal::error::{fail, Result};

pub const BUFFER_SIZE_I128: usize = 64;

/// Helper to parse decimals
///
/// This enum maps the tree major cases:
///
/// - integer only: ` ----XXX----.----`
/// - fraction only: `------.---XXX---`
/// - mixed: `-----XXX.XXX----`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DecimalParser {
    IntegerOnly(usize, usize),
    IntegerOnlyTruncated(usize, usize),
    Mixed(usize, usize),
    MixedTruncated(usize, usize),
    FractionOnly(usize, usize),
    FractionOnlyTruncated(usize, usize),
}

impl DecimalParser {
    pub fn new(precision: u8, scale: i8, truncated: bool) -> Self {
        if scale <= 0 && !truncated {
            Self::IntegerOnly(precision as usize, -scale as usize)
        } else if scale < 0 {
            Self::IntegerOnlyTruncated(precision as usize, -scale as usize)
        } else if (scale as usize) < (precision as usize) && !truncated {
            Self::Mixed(precision as usize, scale as usize)
        } else if (scale as usize) < (precision as usize) {
            Self::MixedTruncated(precision as usize, scale as usize)
        } else if !truncated {
            Self::FractionOnly(precision as usize, scale as usize)
        } else {
            Self::FractionOnlyTruncated(precision as usize, scale as usize)
        }
    }

    pub fn parse_decimal128(self, buffer: &mut [u8], s: &[u8]) -> Result<i128> {
        let (s, sign) = parse_sign(s);
        let val: i128 = self.copy_digits(buffer, s)?.parse()?;
        let val = sign.apply_i128(val);
        Ok(val)
    }

    pub fn copy_digits<'b>(self, buffer: &'b mut [u8], s: &[u8]) -> Result<&'b str> {
        use DecimalParser::*;
        match self {
            IntegerOnly(precision, scale) => {
                copy_digits_integer_only(buffer, s, precision, scale, false)
            }
            IntegerOnlyTruncated(precision, scale) => {
                copy_digits_integer_only(buffer, s, precision, scale, true)
            }
            Mixed(precision, scale) => copy_digits_mixed(buffer, s, precision, scale, false),
            MixedTruncated(precision, scale) => {
                copy_digits_mixed(buffer, s, precision, scale, true)
            }
            FractionOnly(precision, scale) => {
                copy_digits_fraction_only(buffer, s, precision, scale, false)
            }
            FractionOnlyTruncated(precision, scale) => {
                copy_digits_fraction_only(buffer, s, precision, scale, true)
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
    buffer: &'b mut [u8],
    s: &[u8],
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<&'b str> {
    let (before_period, after_period) = find_period(s);
    let end_copy = before_period.saturating_sub(scale);
    let start_copy = end_copy.saturating_sub(precision);

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
    buffer: &'b mut [u8],
    s: &[u8],
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<&'b str> {
    debug_assert!(scale >= precision);

    let (before_period, after_period) = find_period(s);
    let start_copy = std::cmp::min(s.len(), after_period + scale - precision);
    let end_copy = std::cmp::min(s.len(), after_period + scale);
    let fill = precision - (end_copy - start_copy);

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
    buffer: &'b mut [u8],
    s: &[u8],
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<&'b str> {
    debug_assert!(scale < precision);

    let (before_period, after_period) = find_period(s);
    let start_copy = before_period.saturating_sub(precision - scale);
    let end_copy = std::cmp::min(s.len(), after_period + scale);

    let copy_1 = start_copy..before_period;
    let copy_2 = after_period..end_copy;
    let fill = scale - (end_copy - after_period);

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

#[cfg(test)]
fn parse_decimal(s: &[u8], precision: u8, scale: i8, truncate: bool) -> Result<i128> {
    let mut buffer = [0; BUFFER_SIZE_I128];
    DecimalParser::new(precision, scale, truncate).parse_decimal128(&mut buffer, s)
}

#[test]
fn test_missing_number() {
    assert!(parse_decimal(b"", 5, 0, false).is_err());
    assert!(parse_decimal(b"+", 5, 0, false).is_err());
    assert!(parse_decimal(b"-", 5, 0, false).is_err());
}

#[test]
fn test_insufficient_precision_missing_number() {
    assert!(parse_decimal(b"123", 2, 0, false).is_err());
}

#[test]
fn test_examples_scale_0() {
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
fn test_negative_scale() {
    assert_eq!(
        parse_decimal(b"9876543210", 10, -1, false),
        Ok(987654321_i128)
    );
    assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
    assert_eq!(parse_decimal(b"2100", 10, -2, false), Ok(21_i128));
    assert_eq!(parse_decimal(b"21000", 10, -3, false), Ok(21_i128));
}

#[test]
fn test_negative_scale_truncation() {
    assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
    assert!(parse_decimal(b"213", 10, -1, false).is_err());
    assert_eq!(parse_decimal(b"213", 10, -1, true), Ok(21_i128));
}

#[test]
fn test_positive_scale_fraction() {
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
fn test_positive_scale_fraction_truncation() {
    assert_eq!(parse_decimal(b"13.2", 10, 1, false), Ok(132_i128));
    assert_eq!(parse_decimal(b"-42.500", 10, 1, false), Ok(-425_i128));
    assert!(parse_decimal(b"-42.560", 10, 1, false).is_err());
    assert_eq!(parse_decimal(b"-42.560", 10, 1, true), Ok(-425_i128));
    assert_eq!(parse_decimal(b"-42.567", 10, 1, true), Ok(-425_i128));
}

#[test]
fn test_copy_digits() {
    fn copy_digits_str(s: &str, precision: u8, scale: i8) -> Result<String> {
        let mut buffer = [0; 64];
        let res =
            DecimalParser::new(precision, scale, false).copy_digits(&mut buffer, s.as_bytes())?;
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

pub fn format_decimal<'b>(buffer: &'b mut [u8], val: i128, scale: i8) -> &'b str {
    fn write_val(buffer: &mut [u8], val: i128) -> usize {
        use std::io::Write;

        let initial_length = buffer.len();

        let mut buffer = &mut *buffer;
        write!(buffer, "{val}").unwrap();
        initial_length - buffer.len()
    }

    let res = if scale == 0 {
        let num_bytes_written = write_val(buffer, val);
        &buffer[..num_bytes_written]
    } else if scale < 0 && val == 0 {
        b"0"
    } else if scale < 0 {
        let scale = -scale as usize;
        let num_bytes_written = write_val(buffer, val);

        buffer[num_bytes_written..][..scale].fill(b'0');
        &buffer[..num_bytes_written + scale]
    } else {
        let scale = scale as usize;
        let num_bytes_written = write_val(buffer, val);
        let num_sign_bytes = if val >= 0 { 0 } else { 1 };
        let num_digits_written = num_bytes_written - num_sign_bytes;

        if num_digits_written <= scale {
            let num_missing_zeros = scale - num_digits_written;
            buffer.copy_within(
                num_sign_bytes..num_bytes_written,
                num_sign_bytes + 2 + num_missing_zeros,
            );
            buffer[num_sign_bytes] = b'0';
            buffer[num_sign_bytes + 1] = b'.';
            for i in 0..num_missing_zeros {
                buffer[num_sign_bytes + 2 + i] = b'0';
            }

            &buffer[..num_bytes_written + num_missing_zeros + 2]
        } else {
            let end_integer = num_sign_bytes + num_digits_written - scale;
            buffer.copy_within(end_integer..num_bytes_written, end_integer + 1);
            buffer[end_integer] = b'.';

            &buffer[..num_bytes_written + 1]
        }
    };

    // safety only ASCII characters used -> conversion into str is safe
    std::str::from_utf8(res).unwrap()
}

#[test]
fn test_format_decimal() {
    fn format_decimal_str(val: i128, scale: i8) -> String {
        let mut buffer = [0; BUFFER_SIZE_I128];
        format_decimal(&mut buffer, val, scale).to_owned()
    }

    assert_eq!(format_decimal_str(0, 0), "0");
    assert_eq!(format_decimal_str(123, 0), "123");
    assert_eq!(format_decimal_str(13, 0), "13");
    assert_eq!(format_decimal_str(-47, 0), "-47");
    assert_eq!(format_decimal_str(-210, 0), "-210");

    assert_eq!(format_decimal_str(0, -2), "0");
    assert_eq!(format_decimal_str(123, -2), "12300");
    assert_eq!(format_decimal_str(13, -2), "1300");
    assert_eq!(format_decimal_str(-47, -2), "-4700");
    assert_eq!(format_decimal_str(-210, -2), "-21000");

    assert_eq!(format_decimal_str(0, 1), "0.0");
    assert_eq!(format_decimal_str(0, 2), "0.00");
    assert_eq!(format_decimal_str(2, 1), "0.2");
    assert_eq!(format_decimal_str(0, 2), "0.00");
    assert_eq!(format_decimal_str(2, 2), "0.02");
    assert_eq!(format_decimal_str(10, 2), "0.10");
    assert_eq!(format_decimal_str(-123, 4), "-0.0123");
    assert_eq!(format_decimal_str(-123, 3), "-0.123");

    assert_eq!(format_decimal_str(-123, -4), "-1230000");
    assert_eq!(format_decimal_str(-123, -3), "-123000");
    assert_eq!(format_decimal_str(-123, -2), "-12300");
    assert_eq!(format_decimal_str(-123, -1), "-1230");
    assert_eq!(format_decimal_str(-123, 0), "-123");
    assert_eq!(format_decimal_str(-123, 1), "-12.3");
    assert_eq!(format_decimal_str(-123, 2), "-1.23");
    assert_eq!(format_decimal_str(-123, 3), "-0.123");
    assert_eq!(format_decimal_str(-123, 4), "-0.0123");

    assert_eq!(format_decimal_str(12345, 3), "12.345");
}
