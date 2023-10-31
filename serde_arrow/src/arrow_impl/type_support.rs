use crate::_impl::arrow::{datatypes::Field, error::ArrowError};

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
