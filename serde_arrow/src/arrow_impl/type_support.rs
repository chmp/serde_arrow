use crate::_impl::arrow::{
    datatypes::{Field as ArrowField, FieldRef},
    error::ArrowError,
};

use crate::internal::{
    arrow::Field,
    error::{Error, Result},
    schema::extensions::{Bool8Field, FixedShapeTensorField, VariableShapeTensorField},
};

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom_from(err.to_string(), err)
    }
}

macro_rules! impl_try_from_ext_type {
    ($ty:ty) => {
        impl TryFrom<&$ty> for ArrowField {
            type Error = Error;

            fn try_from(value: &$ty) -> Result<Self, Self::Error> {
                Self::try_from(&Field::try_from(value)?)
            }
        }

        impl TryFrom<$ty> for ArrowField {
            type Error = Error;

            fn try_from(value: $ty) -> Result<Self, Self::Error> {
                Self::try_from(&value)
            }
        }
    };
}

impl_try_from_ext_type!(Bool8Field);
impl_try_from_ext_type!(FixedShapeTensorField);
impl_try_from_ext_type!(VariableShapeTensorField);

pub fn fields_from_field_refs(fields: &[FieldRef]) -> Result<Vec<Field>> {
    fields
        .iter()
        .map(|field| Field::try_from(field.as_ref()))
        .collect()
}
