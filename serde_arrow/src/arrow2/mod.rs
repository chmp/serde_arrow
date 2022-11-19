//! arrow2 dependent functionality
//!
pub(crate) mod schema;
pub(crate) mod sinks;
pub(crate) mod sources;

// TODO: re-implement io-ipc
// #[cfg(feature = "arrow2-io_ipc")]
// mod write_ipc;

use arrow2::{array::Array, datatypes::Field};
use serde::{Deserialize, Serialize};

use self::{sinks::build_records_builder, sources::build_record_source};
use crate::{
    base::{collect_events, deserialize_from_source, serialize_into_sink, Event},
    generic::schema::TracedSchema,
    Result,
};

pub fn serialize_into_fields<T>(items: &T) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut schema = TracedSchema::new();
    serialize_into_sink(&mut schema, items)?;
    schema.into_fields()
}

pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let mut builder = build_records_builder(fields)?;
    serialize_into_sink(&mut builder, items)?;
    builder.into_records()
}

pub fn deserialize_from_arrays<'de, T, A>(fields: &[Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    deserialize_from_source(build_record_source(fields, arrays)?)
}

/// Collect the events for the given array
///
/// This functionality is mostly intended as a debug functionality.
///
pub fn collect_events_from_array<A>(fields: &[Field], arrays: &[A]) -> Result<Vec<Event<'static>>>
where
    A: AsRef<dyn Array>,
{
    collect_events(build_record_source(fields, arrays)?)
}
