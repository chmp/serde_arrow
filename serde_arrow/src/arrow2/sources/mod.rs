//! Helpers to convert arrow2 arrays into event streams
//!
pub(crate) mod builder;
pub(crate) mod primitive_sources;
pub(crate) mod record_source;
pub(crate) mod struct_source;

use arrow2::{array::Array, datatypes::Field};
use serde::Deserialize;

use crate::{
    event::{collect_events, deserialize_from_source, Event},
    Result,
};

use self::builder::build_record_source;

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
