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

#[allow(deprecated)]
pub use api::{
    deserialize_from_array, deserialize_from_arrays, serialize_into_array, serialize_into_arrays,
    serialize_into_field, serialize_into_fields, ArrayBuilder,
};

/// Build arrays record by record
#[deprecated = "serde_arrow::arrow::ArraysBuilder is deprecated. Use serde_arrow::ArrowBuilder instead."]
pub type ArraysBuilder = api::ArrowBuilder;
