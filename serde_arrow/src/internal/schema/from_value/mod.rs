use serde::Serialize;

use crate::Result;

use super::SerdeArrowSchema;

pub fn schema_from_value<T: Serialize + ?Sized>(value: &T) -> Result<SerdeArrowSchema> {
    // simple version of serde-transmute
    let mut events = Vec::<crate::internal::event::Event>::new();
    crate::internal::sink::serialize_into_sink(&mut events, value)?;
    let this: SerdeArrowSchema = crate::internal::source::deserialize_from_source(&events)?;
    Ok(this)
}
