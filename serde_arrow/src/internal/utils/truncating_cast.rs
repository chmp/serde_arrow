#![allow(
    // TODO: fix this
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
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

pub trait TruncatingFrom<Source> {
    fn truncating_from(value: Source) -> Self;
}

macro_rules! impl_truncating_cast {
    () => {
        impl_truncating_cast!(@all i8);
        impl_truncating_cast!(@all i16);
        impl_truncating_cast!(@all i32);
        impl_truncating_cast!(@all i64);
        impl_truncating_cast!(@all i128);
        impl_truncating_cast!(@all isize);
        impl_truncating_cast!(@all u8);
        impl_truncating_cast!(@all u16);
        impl_truncating_cast!(@all u32);
        impl_truncating_cast!(@all u64);
        impl_truncating_cast!(@all usize);
        impl_truncating_cast!(@all u128);
        impl_truncating_cast!(@all f32);
        impl_truncating_cast!(@all f64);
    };
    (@all $source:ty) => {
        impl_truncating_cast!(@all $source; i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64);
    };
    (@all $source:ty; $($target:ty),*) => {
        $(
            impl TruncatingFrom<$source> for $target {
                fn truncating_from(value: $source) -> Self {
                    value as Self
                }
            }
        )*
        impl TruncatingCast for $source {}
    };
}

impl_truncating_cast!();
