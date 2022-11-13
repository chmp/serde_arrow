pub(crate) mod schema;
pub(crate) mod sinks;
pub(crate) mod sources;

// TODO: re-implement io-ipc
// #[cfg(feature = "arrow2-io_ipc")]
// mod write_ipc;

pub use schema::{editor::SchemaEditor, Strategy};

pub use sinks::{serialize_into_arrays, serialize_into_fields};
pub use sources::{collect_events_from_array, deserialize_from_arrays};
