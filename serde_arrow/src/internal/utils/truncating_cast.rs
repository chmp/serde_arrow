#![allow(
    clippy::cast_possible_truncation,
    reason = "this file defines explicit truncations to whitelist these conversions"
)]

pub trait TruncatingCast {
    fn truncating_cast<Target>(self, _reason: &str) -> Target
    where
        Self: Sized,
        Target: TruncatingFrom<Self>,
    {
        Target::truncating_from(self)
    }
}

impl TruncatingCast for f64 {}
impl TruncatingCast for u128 {}
impl TruncatingCast for usize {}

pub trait TruncatingFrom<Source> {
    fn truncating_from(value: Source) -> Self;
}

impl TruncatingFrom<f64> for f32 {
    fn truncating_from(value: f64) -> f32 {
        value as Self
    }
}

impl TruncatingFrom<u128> for u32 {
    fn truncating_from(value: u128) -> Self {
        value as Self
    }
}

impl TruncatingFrom<usize> for u32 {
    fn truncating_from(value: usize) -> Self {
        value as Self
    }
}
