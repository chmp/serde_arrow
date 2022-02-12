//! # `serde_arrow` - convert sequences of structs / maps to arrow tables
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
//! let batch = serde_arrow::to_record_batch(&records, &schema)?;
//!
//! assert_eq!(batch.num_rows(), 3);
//! assert_eq!(batch.num_columns(), 2);
//! # Ok(())
//! # }
//! ```
//!
//! See [implementation] for an explanation of how this package works and its
//! underlying data model.
//!
pub(crate) mod event;
mod ops;
mod schema;
mod util;

#[cfg(test)]
mod test;

pub use schema::{DataType, Schema};
// pub use serializer::to_record_batch;
pub use util::error::{Error, Result};
pub use util::hl::to_ipc_writer;

pub use ops::{from_record_batch, to_record_batch, trace_schema};

// NOTE: hide the documentation from doctests
#[doc = include_str!("../../Implementation.md")]
#[cfg(not(doctest))]
pub mod implementation {}
