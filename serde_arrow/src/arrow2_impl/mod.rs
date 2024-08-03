//! Support for the `arrow2` crate (*requires one the `arrow2-*` features*)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
#![deny(missing_docs)]
pub(crate) mod api;
mod array;
pub(crate) mod deserialization;
mod schema;
mod type_support;
