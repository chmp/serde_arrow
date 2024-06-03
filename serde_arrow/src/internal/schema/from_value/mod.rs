use serde::Serialize;

use crate::internal::{error::Result, event::Event, sink, source};

use super::SerdeArrowSchema;

pub fn schema_from_value<T: Serialize + ?Sized>(value: &T) -> Result<SerdeArrowSchema> {
    // simple version of serde-transmute
    let mut events = Vec::<Event>::new();
    sink::serialize_into_sink(&mut events, value)?;
    source::deserialize_from_source(&events)
}
