//! Common abstractions used in `serde_arrow`
pub(crate) mod error;
pub(crate) mod event;
pub(crate) mod sink;
pub(crate) mod source;

pub use event::Event;
pub use sink::{serialize_into_sink, EventSink};
pub use source::{deserialize_from_source, EventSource};
