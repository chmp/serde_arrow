//! Helpers to build arrow2 arrays from an event stream
//!
pub(crate) mod base;
pub(crate) mod builders;
pub(crate) mod primitive_sinks;
pub(crate) mod records_builder;
pub(crate) mod struct_sink;

use arrow2::{array::Array, datatypes::Field};
use serde::Serialize;

use crate::{event::serialize_into_sink, Result};

use self::records_builder::RecordsBuilder;

use super::schema::TracedSchema;

pub fn serialize_into_fields<T>(items: &T) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    serialize_into_sink(TracedSchema::new(), items)?.into_fields()
}

pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    serialize_into_sink(RecordsBuilder::new(&fields)?, items)?.into_records()
}
