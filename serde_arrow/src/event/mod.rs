mod base;
mod sink;
mod source;

pub use base::Event;
pub use sink::{serialize_into_sink, EventSink};
pub use source::{deserialize_from_source, EventSource};
