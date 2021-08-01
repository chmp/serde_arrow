//! # `serde_arrow` - convert list of structs / maps to arrow record batches
//!
//! Usage:
//!
//! ```rust
//! # use serde_arrow::Result;
//! # use serde::Serialize;
//! # use std::convert::TryFrom;
//! #
//! # fn main() -> Result<()> {
//! #[derive(Serialize)]
//! struct Example {
//!     a: f32,
//!     b: i32,
//! }
//!
//! let records = vec![
//!     Example { a: 1.0, b: 1 },
//!     Example { a: 2.0, b: 2 },
//!     Example { a: 3.0, b: 3 },
//! ];
//!
//! // try to auto-detect the arrow types, result can be overwritten and customized
//! let schema = serde_arrow::trace_schema(&records)?;
//! let schema = arrow::datatypes::Schema::try_from(schema)?;
//!
//! let batch = serde_arrow::to_record_batch(&records, schema)?;
//!
//! assert_eq!(batch.num_rows(), 3);
//! assert_eq!(batch.num_columns(), 2);
//! # Ok(())
//! # }
//! ```
//!
mod array_builder;
mod schema;
mod serializer;
mod util;

pub use schema::trace_schema;
pub use serializer::to_record_batch;
pub use util::error::{Error, Result};
