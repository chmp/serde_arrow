use serde::Serialize;

use crate::internal::{error::Result, utils::value};

use super::SerdeArrowSchema;

pub fn schema_from_value<T: Serialize + ?Sized>(value: &T) -> Result<SerdeArrowSchema> {
    value::transmute(value)
}

/*let mut events = Vec::<Event>::new();
sink::serialize_into_sink(&mut events, value)?;
source::deserialize_from_source(&events)*/
