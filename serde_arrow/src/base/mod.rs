pub(crate) mod error;
pub(crate) mod event;
pub(crate) mod sink;
pub(crate) mod source;

pub use error::{Error, Result};
pub use event::Event;
pub use sink::{serialize_into_sink, EventSink};
pub use source::{
    collect_events, deserialize_from_source, DynamicSource, EventSource, PeekableEventSource,
};
