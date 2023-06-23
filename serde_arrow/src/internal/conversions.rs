use half::f16;

use crate::Error;

pub trait ToBytes: Sized {
    type Bytes;

    fn to_bytes(self) -> Self::Bytes;
    fn from_bytes(val: Self::Bytes) -> Self;

    fn to_bytes_vec(items: Vec<Self>) -> Vec<Self::Bytes> {
        items.into_iter().map(Self::to_bytes).collect()
    }

    fn from_bytes_vec(items: Vec<Self::Bytes>) -> Vec<Self> {
        items.into_iter().map(Self::from_bytes).collect()
    }
}

impl ToBytes for u8 {
    type Bytes = u8;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for i8 {
    type Bytes = u8;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for f32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for f64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

pub struct WrappedF32(f32);

impl From<f32> for WrappedF32 {
    fn from(value: f32) -> Self {
        Self(value)
    }
}

impl From<f64> for WrappedF32 {
    fn from(value: f64) -> Self {
        // TODO: handle failures
        Self(value as f32)
    }
}

impl ToBytes for WrappedF32 {
    type Bytes = u32;

    fn from_bytes(val: Self::Bytes) -> Self {
        Self(f32::from_ne_bytes(val.to_ne_bytes()))
    }

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.0.to_ne_bytes())
    }
}

pub struct WrappedF64(f64);

impl From<f32> for WrappedF64 {
    fn from(value: f32) -> Self {
        Self(value as f64)
    }
}

impl From<f64> for WrappedF64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl ToBytes for WrappedF64 {
    type Bytes = u64;

    fn from_bytes(val: Self::Bytes) -> Self {
        Self(f64::from_ne_bytes(val.to_ne_bytes()))
    }

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.0.to_ne_bytes())
    }
}

pub struct WrappedF16(f16);

impl TryFrom<f32> for WrappedF16 {
    type Error = Error;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        // TODO: handle failures: f16 silently falls back to +/- inf
        Ok(WrappedF16(f16::from_f32(value)))
    }
}

impl TryFrom<f64> for WrappedF16 {
    type Error = Error;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        // TODO: handle failures: f16 silently falls back to +/- inf
        Ok(WrappedF16(f16::from_f64(value)))
    }
}

impl ToBytes for WrappedF16 {
    type Bytes = u16;

    fn from_bytes(val: Self::Bytes) -> Self {
        WrappedF16(f16::from_bits(val))
    }

    fn to_bytes(self) -> Self::Bytes {
        self.0.to_bits()
    }
}
