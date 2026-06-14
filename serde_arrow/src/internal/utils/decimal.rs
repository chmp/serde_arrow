//! Decimal support
//!
//! Decimals are stored either as 128 or 256 bit integers. They are
//! characterized by a precision, the total number of digits, and the scale, the
//! position of the decimal point.

use crate::internal::error::{fail, Result};
use std::ops::{Range, RangeTo};

const MAX_I128_FORMATTED_LEN_WITH_SIGN: usize = 40;
const MAX_I8_ABS: usize = i8::MIN.unsigned_abs() as usize;

/// Large enough to format any `i128` decimal with any `i8` scale.
///
/// The worst case is `i128::MIN` with scale `i8::MIN`: the 40-byte integer
/// representation, including its sign, plus 128 appended zeros.
/// Underscores do not affect the size: formatting never emits them, and parsing
/// removes them before writing into this buffer.
pub const BUFFER_SIZE_I128: usize = MAX_I128_FORMATTED_LEN_WITH_SIGN + MAX_I8_ABS;

/// Helper to parse decimals
///
/// This enum maps the tree major cases:
///
/// - integer only: ` ----XXX----.----`
/// - fraction only: `------.---XXX---`
/// - mixed: `-----XXX.XXX----`
///
/// Parsing is performed by copying the relevant digits into a temporary buffer
/// and using integer parsing.
///
/// All variants have the form `(precision, scale, truncated)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DecimalParser {
    IntegerOnly(usize, usize, bool),
    Mixed(usize, usize, bool),
    FractionOnly(usize, usize, bool),
}

impl DecimalParser {
    pub fn new(precision: u8, scale: i8, truncated: bool) -> Self {
        let precision = usize::from(precision);
        let abs_scale = usize::from(scale.unsigned_abs());
        if scale <= 0 {
            Self::IntegerOnly(precision, abs_scale, truncated)
        } else if abs_scale < precision {
            Self::Mixed(precision, abs_scale, truncated)
        } else {
            Self::FractionOnly(precision, abs_scale, truncated)
        }
    }

    pub fn parse_decimal128(self, buffer: &mut [u8], s: &[u8]) -> Result<i128> {
        let (s, sign) = parse_sign(s);
        let len = copy_into_buffer_without_underscores(buffer, s)?;
        let val: i128 = self.copy_digits(buffer, len)?.parse()?;
        let val = sign.apply_i128(val);
        Ok(val)
    }

    fn copy_digits(self, buffer: &mut [u8], len: usize) -> Result<&str> {
        let out_len = match self {
            Self::IntegerOnly(precision, scale, truncated) => {
                copy_digits_integer_only(buffer, len, precision, scale, truncated)?
            }
            Self::Mixed(precision, scale, truncated) => {
                copy_digits_mixed(buffer, len, precision, scale, truncated)?
            }
            Self::FractionOnly(precision, scale, truncated) => {
                copy_digits_fraction_only(buffer, len, precision, scale, truncated)?
            }
        };

        Ok(std::str::from_utf8(get_to(buffer, ..out_len)?)?)
    }
}

fn copy_into_buffer_without_underscores(buffer: &mut [u8], s: &[u8]) -> Result<usize> {
    let mut len = 0;
    for &byte in s {
        if byte != b'_' {
            if len >= buffer.len() {
                fail!("invalid decimal: number too long");
            }
            let Some(target) = buffer.get_mut(len) else {
                fail!("invalid decimal: number too long");
            };
            *target = byte;
            len += 1;
        }
    }
    Ok(len)
}

fn parse_sign(s: &[u8]) -> (&[u8], Sign) {
    match s.split_first() {
        Some((b'+', rest)) => (rest, Sign::Plus),
        Some((b'-', rest)) => (rest, Sign::Minus),
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

fn copy_digits_integer_only(
    buffer: &mut [u8],
    len: usize,
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<usize> {
    let s = get_to(buffer, ..len)?;
    let (before_period, after_period) = find_period(s);

    let end_copy = before_period.saturating_sub(scale);
    let start_copy = end_copy.saturating_sub(precision);

    check_all_ascii_zero(get(buffer, 0..start_copy)?, true)?;
    if !truncate {
        check_all_ascii_digit(get(buffer, start_copy..end_copy)?)?;
        check_all_ascii_zero(get(buffer, end_copy..before_period)?, false)?;
        check_all_ascii_zero(get(buffer, after_period..len)?, false)?;
    } else {
        check_all_ascii_digit(get(buffer, start_copy..before_period)?)?;
        check_all_ascii_digit(get(buffer, after_period..len)?)?;
    }

    let out_len = end_copy - start_copy;
    buffer.copy_within(start_copy..end_copy, 0);
    Ok(out_len)
}

fn copy_digits_fraction_only(
    buffer: &mut [u8],
    len: usize,
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<usize> {
    debug_assert!(scale >= precision);

    let s = get_to(buffer, ..len)?;
    let (before_period, after_period) = find_period(s);

    let start_copy = std::cmp::min(len, after_period + scale - precision);
    let end_copy = std::cmp::min(len, after_period + scale);
    let fill = precision - (end_copy - start_copy);

    check_all_ascii_zero(get(buffer, 0..before_period)?, true)?;
    check_all_ascii_zero(get(buffer, after_period..start_copy)?, true)?;

    if !truncate {
        check_all_ascii_digit(get(buffer, start_copy..end_copy)?)?;
        check_all_ascii_zero(get(buffer, end_copy..len)?, false)?;
    } else {
        check_all_ascii_digit(get(buffer, start_copy..len)?)?;
    }

    let out_len = end_copy - start_copy;
    buffer.copy_within(start_copy..end_copy, 0);
    get_mut(buffer, out_len..out_len + fill)?.fill(b'0');
    Ok(out_len + fill)
}

fn copy_digits_mixed(
    buffer: &mut [u8],
    len: usize,
    precision: usize,
    scale: usize,
    truncate: bool,
) -> Result<usize> {
    debug_assert!(scale < precision);

    let s = get_to(buffer, ..len)?;
    let (before_period, after_period) = find_period(s);

    let start_copy = before_period.saturating_sub(precision - scale);
    let end_copy = std::cmp::min(len, after_period + scale);

    let copy_1 = start_copy..before_period;
    let copy_2 = after_period..end_copy;
    let fill = scale - (end_copy - after_period);

    check_all_ascii_zero(get(buffer, 0..start_copy)?, true)?;
    check_all_ascii_digit(get(buffer, copy_1.clone())?)?;
    if !truncate {
        check_all_ascii_digit(get(buffer, after_period..end_copy)?)?;
        check_all_ascii_zero(get(buffer, end_copy..len)?, false)?;
    } else {
        check_all_ascii_digit(get(buffer, after_period..len)?)?;
    }

    let copy_1_len = copy_1.len();
    let copy_2_len = copy_2.len();

    buffer.copy_within(copy_1, 0);
    buffer.copy_within(copy_2, copy_1_len);
    get_mut(
        buffer,
        copy_1_len + copy_2_len..copy_1_len + copy_2_len + fill,
    )?
    .fill(b'0');

    Ok(copy_1_len + copy_2_len + fill)
}

fn get(buffer: &[u8], range: Range<usize>) -> Result<&[u8]> {
    let Some(slice) = buffer.get(range) else {
        fail!("invalid decimal: number too long");
    };
    Ok(slice)
}

fn get_to(buffer: &[u8], range: RangeTo<usize>) -> Result<&[u8]> {
    let Some(slice) = buffer.get(range) else {
        fail!("invalid decimal: number too long");
    };
    Ok(slice)
}

fn get_mut(buffer: &mut [u8], range: Range<usize>) -> Result<&mut [u8]> {
    let Some(slice) = buffer.get_mut(range) else {
        fail!("invalid decimal: number too long");
    };
    Ok(slice)
}

fn find_period(s: &[u8]) -> (usize, usize) {
    if let Some(pos) = s.iter().position(|b| *b == b'.') {
        (pos, pos + 1)
    } else {
        (s.len(), s.len())
    }
}

fn check_all_ascii_zero(s: &[u8], leading: bool) -> Result<()> {
    if s.iter().any(|c| *c != b'0') {
        if leading {
            fail!("invalid decimal: not enough precision");
        } else {
            fail!("invalid decimal: not enough scale, the given number would be truncated");
        }
    }
    Ok(())
}

fn check_all_ascii_digit(s: &[u8]) -> Result<()> {
    if s.iter().any(|c| *c < b'0' || *c > b'9') {
        fail!("invalid decimal: only ascii digits are supported");
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
    parse_decimal(b"", 5, 0, false).unwrap_err();
    parse_decimal(b"+", 5, 0, false).unwrap_err();
    parse_decimal(b"-", 5, 0, false).unwrap_err();
}

#[test]
fn test_insufficient_precision_missing_number() {
    parse_decimal(b"123", 2, 0, false).unwrap_err();
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
    assert_eq!(parse_decimal(b"213", 10, -1, true), Ok(21_i128));

    parse_decimal(b"213", 10, -1, false).unwrap_err();
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
    assert_eq!(parse_decimal(b"-42.560", 10, 1, true), Ok(-425_i128));
    assert_eq!(parse_decimal(b"-42.567", 10, 1, true), Ok(-425_i128));

    parse_decimal(b"-42.560", 10, 1, false).unwrap_err();
}

#[test]
fn test_underscores() {
    assert_eq!(parse_decimal(b"1_234", 10, 0, false), Ok(1234_i128));
    assert_eq!(parse_decimal(b"1_234.50", 10, 2, false), Ok(123450_i128));
    assert_eq!(parse_decimal(b"-1_234.5_0", 10, 2, false), Ok(-123450_i128));
    assert_eq!(parse_decimal(b"1_234_00", 10, -2, false), Ok(1234_i128));
}

#[test]
fn test_copy_digits() {
    fn assert_digits_value(s: &str, precision: u8, scale: i8, expected_digits: &str) {
        let expected: i128 = expected_digits.parse().unwrap();
        assert_eq!(
            parse_decimal(s.as_bytes(), precision, scale, false),
            Ok(expected)
        );
    }

    assert_digits_value("0", 1, 0, "0");
    assert_digits_value("1", 1, 0, "1");
    assert_digits_value("5", 1, 0, "5");
    assert_digits_value("5", 2, 0, "5");
    assert_digits_value("5", 3, 0, "5");
    assert_digits_value("125", 3, 0, "125");
    assert_digits_value("12300", 3, -2, "123");
    assert_digits_value("5000", 1, -3, "5");

    assert_digits_value("0.0", 1, 0, "0");
    assert_digits_value("1.0", 1, 0, "1");
    assert_digits_value("5.0", 1, 0, "5");
    assert_digits_value("5.00", 2, 0, "5");
    assert_digits_value("5.0000", 3, 0, "5");
    assert_digits_value("125.00", 3, 0, "125");
    assert_digits_value("12300.00000", 3, -2, "123");
    assert_digits_value("5000.0000", 1, -3, "5");

    assert_digits_value("0", 2, 2, "00");
    assert_digits_value("0.", 2, 2, "00");
    assert_digits_value("0.01", 2, 2, "01");
    assert_digits_value("0.10", 2, 2, "10");
    assert_digits_value("0.1", 2, 2, "10");

    assert_digits_value("0.01", 2, 3, "10");
    assert_digits_value("0.012", 2, 3, "12");
    assert_digits_value("0.007", 2, 3, "07");

    assert_digits_value("01.230", 3, 2, "123");
    assert_digits_value("1.230", 3, 2, "123");
    assert_digits_value("1.23", 3, 2, "123");
    assert_digits_value("1.2", 3, 2, "120");
    assert_digits_value("1.", 3, 2, "100");

    assert_digits_value("21.21", 4, 2, "2121");
    assert_digits_value("2", 4, 2, "200");
    assert_digits_value("20", 4, 2, "2000");
    assert_digits_value("42.00", 4, 2, "4200");
}

pub fn format_decimal(buffer: &mut [u8], val: i128, scale: i8) -> Result<&str> {
    fn write_val(buffer: &mut [u8], val: i128) -> Result<usize> {
        use std::io::Write;

        let initial_length = buffer.len();

        let mut buffer = &mut *buffer;
        if write!(buffer, "{val}").is_err() {
            fail!("decimal formatting buffer is too small");
        }
        Ok(initial_length - buffer.len())
    }

    let res = if scale == 0 {
        let num_bytes_written = write_val(buffer, val)?;
        expect_to(buffer, ..num_bytes_written)
    } else if scale < 0 && val == 0 {
        b"0"
    } else if scale < 0 {
        let scale = usize::from(scale.unsigned_abs());
        let num_bytes_written = write_val(buffer, val)?;

        expect_mut(buffer, num_bytes_written..num_bytes_written + scale)?.fill(b'0');
        expect_to(buffer, ..num_bytes_written + scale)
    } else {
        let scale = usize::from(scale.unsigned_abs());
        let num_bytes_written = write_val(buffer, val)?;
        let num_sign_bytes = if val >= 0 { 0 } else { 1 };
        let num_digits_written = num_bytes_written - num_sign_bytes;

        if num_digits_written <= scale {
            let num_missing_zeros = scale - num_digits_written;
            buffer.copy_within(
                num_sign_bytes..num_bytes_written,
                num_sign_bytes + 2 + num_missing_zeros,
            );
            *expect_byte_mut(buffer, num_sign_bytes) = b'0';
            *expect_byte_mut(buffer, num_sign_bytes + 1) = b'.';
            for i in 0..num_missing_zeros {
                *expect_byte_mut(buffer, num_sign_bytes + 2 + i) = b'0';
            }

            expect_to(buffer, ..num_bytes_written + num_missing_zeros + 2)
        } else {
            let end_integer = num_sign_bytes + num_digits_written - scale;
            buffer.copy_within(end_integer..num_bytes_written, end_integer + 1);
            *expect_byte_mut(buffer, end_integer) = b'.';

            expect_to(buffer, ..num_bytes_written + 1)
        }
    };

    Ok(std::str::from_utf8(res).unwrap_or_else(|_err| {
        unreachable!("conversion into str is safe, only ASCII characters used")
    }))
}

fn expect_to(buffer: &[u8], range: RangeTo<usize>) -> &[u8] {
    buffer
        .get(range)
        .unwrap_or_else(|| unreachable!("decimal formatting uses a sufficiently large buffer"))
}

fn expect_mut(buffer: &mut [u8], range: Range<usize>) -> Result<&mut [u8]> {
    let Some(buffer) = buffer.get_mut(range) else {
        fail!("decimal formatting buffer is too small");
    };
    Ok(buffer)
}

fn expect_byte_mut(buffer: &mut [u8], idx: usize) -> &mut u8 {
    buffer
        .get_mut(idx)
        .unwrap_or_else(|| unreachable!("decimal formatting uses a sufficiently large buffer"))
}

#[test]
fn test_format_decimal() {
    fn format_decimal_str(val: i128, scale: i8) -> String {
        let mut buffer = [0; BUFFER_SIZE_I128];
        format_decimal(&mut buffer, val, scale).unwrap().to_owned()
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

    assert_eq!(
        format_decimal_str(i128::MIN, i8::MIN),
        format!(
            "{}{}",
            i128::MIN,
            "0".repeat(usize::from(i8::MIN.unsigned_abs()))
        )
    );
    assert_eq!(
        format_decimal_str(-1, i8::MAX),
        format!(
            "-0.{}1",
            "0".repeat(usize::from(i8::MAX.unsigned_abs()) - 1)
        )
    );
}

#[test]
fn test_format_decimal_buffer_too_small() {
    let mut empty_buffer = [];
    assert!(format_decimal(&mut empty_buffer, 1, 0).is_err());

    let mut integer_only_buffer = [0; 1];
    assert!(format_decimal(&mut integer_only_buffer, 1, -1).is_err());
}
