use half::f16;

use crate::_impl::arrow::{datatypes::Field, error::ArrowError};

use crate::internal::conversions::ToBytes;
use crate::internal::error::Error;

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}

pub trait FieldRef {
    fn as_field_ref(&self) -> &Field;
}

impl FieldRef for Field {
    fn as_field_ref(&self) -> &Field {
        self
    }
}

impl FieldRef for std::sync::Arc<Field> {
    fn as_field_ref(&self) -> &Field {
        self.as_ref()
    }
}

impl ToBytes for f16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        self.to_bits()
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_bits(val)
    }
}

// for arrow=35 ArrowPrimitiveType is private
#[cfg(not(feature = "arrow-35"))]
const _: fn(f16) -> u16 = <<crate::_impl::arrow::datatypes::Float16Type as crate::_impl::arrow::datatypes::ArrowPrimitiveType>::Native as ToBytes>::to_bytes;
