mod base;
mod sink;
mod source;

pub use base::Event;
pub use sink::{serialize_into_sink, EventSink};
pub use source::{
    collect_events, deserialize_from_source, DynamicSource, EventSource, PeekableEventSource,
};
