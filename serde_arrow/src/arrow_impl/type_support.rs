use crate::_impl::arrow::{
    datatypes::{Field as ArrowField, FieldRef},
    error::ArrowError,
};

use crate::internal::{
    arrow::Field,
    error::{Error, Result},
    schema::extensions::FixedShapeTensorField,
};

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}

impl TryFrom<&FixedShapeTensorField> for ArrowField {
    type Error = Error;

    fn try_from(value: &FixedShapeTensorField) -> Result<Self, Self::Error> {
        Self::try_from(&Field::try_from(value)?)
    }
}

impl TryFrom<FixedShapeTensorField> for ArrowField {
    type Error = Error;

    fn try_from(value: FixedShapeTensorField) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

pub fn fields_from_field_refs(fields: &[FieldRef]) -> Result<Vec<Field>> {
    fields
        .iter()
        .map(|field| Field::try_from(field.as_ref()))
        .collect()
}
