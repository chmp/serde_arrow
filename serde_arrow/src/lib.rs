//! # `serde_arrow` - convert sequences of structs / maps to arrow tables
//!
//! Usage:
//!
//! ```rust
//! # use serde_arrow::{Result, arrow2::{serialize_into_fields, serialize_into_arrays}};
//! # use serde::Serialize;
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
//! let fields = serialize_into_fields(&records)?;
//! let batch = serialize_into_arrays(&fields, &records)?;
//!
//! # Ok(())
//! # }
//! ```
//!
//! See [implementation] for an explanation of how this package works and its
//! underlying data model.
//!
pub mod base;
mod generic;

#[cfg(feature = "arrow2")]
pub mod arrow2;

#[cfg(test)]
mod test;

pub use base::{Error, Result};
pub use generic::schema::{configure_serde_arrow_strategy, Strategy};

#[doc = include_str!("../Implementation.md")]
// NOTE: hide the implementation documentation from doctests
#[cfg(not(doctest))]
pub mod implementation {}
