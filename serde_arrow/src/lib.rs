//! # `serde_arrow` - convert sequences Rust objects to arrow2 arrays
//!
//! Usage (requires the `arrow2` feature):
//!
//! ```rust
//! # use serde::Serialize;
//! # #[cfg(feature = "arrow2")]
//! # fn main() -> serde_arrow::Result<()> {
//! use serde_arrow::arrow2::{serialize_into_fields, serialize_into_arrays};
//!
//! ##[derive(Serialize)]
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
//! // Auto-detect the arrow types. Result may need to be overwritten and
//! // customized, see serde_arrow::Strategy for details.
//! let fields = serialize_into_fields(&records, Default::default())?;
//! let batch = serialize_into_arrays(&fields, &records)?;
//!
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "arrow2"))]
//! # fn main() { }
//! ```
//!
//! See [implementation] for an explanation of how this package works and its
//! underlying data model.
//!
//! # Features:
//!
//! - `arrow2`: add support to (de)serialize to and from arrow2 arrays. This
//!   feature is activated per default
//!
pub mod base;
mod generic;

#[cfg(feature = "arrow2")]
pub mod arrow2;

#[cfg(test)]
mod test;

pub use base::error::{Error, Result};
pub use generic::schema::{SchemaTracingOptions, Strategy, STRATEGY_KEY};

#[doc = include_str!("../Implementation.md")]
// NOTE: hide the implementation documentation from doctests
#[cfg(not(doctest))]
pub mod implementation {}
