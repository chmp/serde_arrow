use marrow::{datatypes::Field, error::MarrowError};

use crate::{
    _impl::arrow2::datatypes::Field as ArrowField,
    internal::{
        error::{Error, Result},
        schema::{SchemaLike, Sealed, SerdeArrowSchema, TracingOptions},
    },
};

impl TryFrom<SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        Vec::<ArrowField>::try_from(&value)
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        Ok(value
            .fields
            .iter()
            .map(ArrowField::try_from)
            .collect::<Result<_, MarrowError>>()?)
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow2::datatype::Field>` (*requires one of the
/// `arrow2-*` features*)
impl SchemaLike for Vec<ArrowField> {
    fn from_value<T: serde::Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: serde::Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: serde::Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}
