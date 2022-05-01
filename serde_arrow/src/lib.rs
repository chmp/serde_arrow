//! # `serde_arrow` - convert sequences of structs / maps to arrow tables
//!
//! Usage:
//!
//! ```rust
//! # use serde_arrow::{Result, Schema};
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
//! let schema = Schema::from_records(&records)?;
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
mod error;
pub mod event;
mod ops;
mod schema;

#[cfg(feature = "arrow")]
mod arrow_ops;

#[cfg(feature = "arrow2")]
mod arrow2_ops;

#[cfg(test)]
mod test;

#[cfg(all(test, feature = "arrow"))]
mod test_arrow;

#[cfg(all(test, feature = "arrow2"))]
mod test_arrow2;

pub use schema::{DataType, Schema};
// pub use serializer::to_record_batch;
pub use error::{Error, Result};

pub use ops::trace_schema;

#[cfg(feature = "arrow")]
pub use arrow_ops::{from_record_batch, to_ipc_writer, to_record_batch};

#[doc = include_str!("../Implementation.md")]
// NOTE: hide the implementation documentation from doctests
#[cfg(not(doctest))]
pub mod implementation {}
