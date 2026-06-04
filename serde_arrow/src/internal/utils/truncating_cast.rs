#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
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
    ($source:ty => $($target:ty),* $(,)?) => {
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

impl_truncating_cast!(i8 => f32, f64);
impl_truncating_cast!(i16 => f32, f64);
impl_truncating_cast!(i32 => f32, f64);
impl_truncating_cast!(i64 => f32, f64);
impl_truncating_cast!(u8 => f32, f64);
impl_truncating_cast!(u16 => f32, f64);
impl_truncating_cast!(u32 => f32, f64);
impl_truncating_cast!(u64 => f32, f64);
impl_truncating_cast!(usize => u32);
impl_truncating_cast!(u128 => u32);
impl_truncating_cast!(f32 => f64);
impl_truncating_cast!(f64 => f32);
