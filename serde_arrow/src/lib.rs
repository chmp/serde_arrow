//! `serde_arrow` - convert list of structs to arrow record batches
//!
mod array_builder;
mod schema;
mod serializer;
mod util;

pub use schema::trace_schema;
pub use serializer::to_record_batch;
pub use util::error::{Error, Result};
