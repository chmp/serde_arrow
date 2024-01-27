//! Support for the `arrow` crate (*requires one the `arrow-*` features*)
//!
//! Functions to convert Rust objects into arrow Arrays. Deserialization from
//! `arrow` arrays to Rust objects is not yet supported.
//!
#![deny(missing_docs)]
pub(crate) mod api;
mod deserialization;
mod schema;
pub(crate) mod serialization;
mod type_support;
