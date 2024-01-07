//! Decimal support
//!
//! Decimals are stored either as 128 or 256 bit integers. They are
//! characterized by a scale, the total number of digits, and the precision, the
//! position of the decimal point.

use crate::internal::error::{fail, Result};

pub fn parse_decimal(val: &[u8], scale: u8, precision: i8, truncate: bool) -> Result<i128> {
    let (val, sign) = parse_sign(val);

    if val.is_empty() {
        fail!("cannot parse decimals without data");
    }

    let (val, integer) = parse_integer(val, precision, truncate)?;
    let (val, fraction) = parse_fraction(val, precision, truncate)?;

    if !val.is_empty() {
        fail!("invalid decimal");
    }

    match sign {
        Sign::Plus | Sign::None => Ok(integer + fraction),
        Sign::Minus => Ok(-(integer + fraction)),
    }
}

fn parse_sign(val: &[u8]) -> (&[u8], Sign) {
    match val.first() {
        Some(b'+') => (&val[1..], Sign::Plus),
        Some(b'-') => (&val[1..], Sign::Minus),
        _ => (val, Sign::None),
    }
}

fn parse_integer(mut val: &[u8], precision: i8, truncate: bool) -> Result<(&[u8], i128)> {
    let (buffer_size, result_multiplier) = if precision < 0 {
        (10_i128.pow(-precision as u32), 1)
    } else {
        (1, 10_i128.pow(precision as u32))
    };

    let mut result = 0;
    let mut buffer = 0;

    while let Some(c @ b'0'..=b'9') = val.first() {
        let new_digit = (c - b'0') as i128;

        val = &val[1..];
        result = 10 * result + (10 * buffer + new_digit) / buffer_size;
        buffer = (10 * buffer + new_digit) % buffer_size;
    }

    if buffer != 0 && !truncate {
        fail!("Cannot represent number with given precision");
    }
    let result = result * result_multiplier;

    Ok((val, result))
}

fn parse_fraction(val: &[u8], precision: i8, truncate: bool) -> Result<(&[u8], i128)> {
    let Some(mut val) = val.strip_prefix(b".") else {
        return Ok((val, 0));
    };

    let mut result = 0_i128;
    let mut remaining = if precision >= 0 { precision as u32 } else { 0 };

    while let Some(c @ b'0'..=b'9') = val.first() {
        val = &val[1..];
        let new_digit = (c - b'0') as i128;

        if remaining > 0 {
            result = 10 * result + new_digit;
            remaining -= 1;
        } else if new_digit != 0 && !truncate {
            fail!("Cannot represent number with given precision");
        }
    }

    let result = result * 10_i128.pow(remaining as u32);

    Ok((val, result))
}

fn parse_digit(val: &[u8]) -> Option<(&[u8], i128)> {
    match val.first() {
        Some(c @ b'0'..=b'9') => Some((&val[1..], (c - b'0').into())),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum Sign {
    Plus,
    Minus,
    None,
}

#[cfg(test)]
mod test {
    use super::parse_decimal;

    #[test]
    fn missing_number() {
        assert!(parse_decimal(b"", 5, 0, false).is_err());
        assert!(parse_decimal(b"+", 5, 0, false).is_err());
        assert!(parse_decimal(b"-", 5, 0, false).is_err());
    }

    #[test]
    fn examples_precision_0() {
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
    fn negative_precision() {
        assert_eq!(
            parse_decimal(b"9876543210", 10, -1, false),
            Ok(987654321_i128)
        );
        assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
        assert_eq!(parse_decimal(b"2100", 10, -2, false), Ok(21_i128));
        assert_eq!(parse_decimal(b"21000", 10, -3, false), Ok(21_i128));
    }

    #[test]
    fn negative_precision_truncation() {
        assert_eq!(parse_decimal(b"210", 10, -1, false), Ok(21_i128));
        assert!(parse_decimal(b"213", 10, -1, false).is_err());
        assert_eq!(parse_decimal(b"213", 10, -1, true), Ok(21_i128));
    }

    #[test]
    fn positive_precision_fraction() {
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
    fn positive_precision_fraction_truncation() {
        assert_eq!(parse_decimal(b"13.2", 10, 1, false), Ok(132_i128));
        assert_eq!(parse_decimal(b"-42.500", 10, 1, false), Ok(-425_i128));
        assert!(parse_decimal(b"-42.560", 10, 1, false).is_err());
        assert_eq!(parse_decimal(b"-42.560", 10, 1, true), Ok(-425_i128));
        assert_eq!(parse_decimal(b"-42.567", 10, 1, true), Ok(-425_i128));
    }
}
