//! # `serde_arrow` - convert sequences Rust objects to arrow2 arrays
//!
//! Usage (requires the `arrow2` feature):
//!
//! ```rust
//! # use serde::Serialize;
//! #[cfg(feature = "arrow2")]
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
//! // try to auto-detect the arrow types, result can be overwritten and customized
//! let fields = serialize_into_fields(&records)?;
//! let batch = serialize_into_arrays(&fields, &records)?;
//!
//! # Ok(())
//! # }
//! #[cfg(not(feature = "arrow2"))]
//! # fn main() { }
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

pub use base::error::{Error, Result};

/// Helpers to modify schemas
///
/// **Warning:** this functionality is experimental and may change between
/// releases without being considered a breaking change.
pub mod schema {
    pub use crate::generic::schema::{
        lookup_field_mut, GenericField, IntoPath, PathFragment, Strategy,
    };
}

#[doc = include_str!("../Implementation.md")]
// NOTE: hide the implementation documentation from doctests
#[cfg(not(doctest))]
pub mod implementation {}
