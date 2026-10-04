//! Specialized element types of arrays

/// Represent a calendar interval as days and milliseconds
#[derive(Debug, PartialEq, Clone, Copy, bytemuck::AnyBitPattern, bytemuck::NoUninit)]
#[repr(C)]
pub struct DayTimeInterval {
    /// The number of days in the interval
    pub days: i32,
    /// The number of milliseconds in the interval
    pub milliseconds: i32,
}

/// Represent a calendar interval as months, days and nanoseconds
#[derive(Debug, Default, PartialEq, Clone, Copy, bytemuck::AnyBitPattern, bytemuck::NoUninit)]
#[repr(C)]
pub struct MonthDayNanoInterval {
    /// The number of months in the interval
    pub months: i32,
    /// The number of days in the interval
    pub days: i32,
    /// The number of nanoseconds in the interval
    pub nanoseconds: i64,
}

/// A 256 bit signed integer in two's complement, the value is `high * 2^128 + low`
///
/// The layout matches the 256 bit integers of Arrow implementations.
#[derive(
    Debug, Default, PartialEq, Eq, Hash, Clone, Copy, bytemuck::AnyBitPattern, bytemuck::NoUninit,
)]
#[repr(C)]
#[allow(
    non_camel_case_types,
    reason = "named after the primitive integer types, as in the Arrow implementations"
)]
pub struct i256 {
    /// The lower 128 bits
    pub low: u128,
    /// The upper 128 bits, including the sign
    pub high: i128,
}

#[test]
fn interval_sizes() {
    assert_eq!(
        std::mem::size_of::<DayTimeInterval>(),
        std::mem::size_of::<i64>()
    );
    assert_eq!(
        std::mem::size_of::<MonthDayNanoInterval>(),
        std::mem::size_of::<i128>()
    );
}
