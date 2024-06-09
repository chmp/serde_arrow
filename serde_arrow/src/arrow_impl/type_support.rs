use crate::_impl::arrow::{datatypes::Field, error::ArrowError};

use crate::internal::{
    error::Error,
    schema::{extensions::FixedShapeTensorField, GenericField},
};

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}

impl TryFrom<&FixedShapeTensorField> for Field {
    type Error = Error;

    fn try_from(value: &FixedShapeTensorField) -> Result<Self, Self::Error> {
        Self::try_from(&GenericField::try_from(value)?)
    }
}

impl TryFrom<FixedShapeTensorField> for Field {
    type Error = Error;

    fn try_from(value: FixedShapeTensorField) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}
