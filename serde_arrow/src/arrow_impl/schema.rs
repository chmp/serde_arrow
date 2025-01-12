use std::sync::Arc;

use marrow::{datatypes::Field, error::MarrowError};
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::datatypes::{Field as ArrowField, FieldRef},
    internal::{
        error::{Error, Result},
        schema::{SchemaLike, Sealed, SerdeArrowSchema, TracingOptions},
    },
};

impl TryFrom<SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
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

impl TryFrom<SerdeArrowSchema> for Vec<FieldRef> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<FieldRef> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        Ok(value
            .fields
            .iter()
            .map(|f| Ok(Arc::new(ArrowField::try_from(f)?)))
            .collect::<Result<_, MarrowError>>()?)
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> Result<Self> {
        Ok(Self {
            fields: fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl<'a> TryFrom<&'a [FieldRef]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [FieldRef]) -> Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(|f| Field::try_from(f.as_ref()))
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow::datatype::Field>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<ArrowField> {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}

impl Sealed for Vec<FieldRef> {}

/// Schema support for `Vec<arrow::datatype::FieldRef>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<FieldRef> {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}
