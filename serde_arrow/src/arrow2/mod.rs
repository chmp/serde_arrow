//! Support for the `arrow2` crate (*requires one the `arrow2-*` features*)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
#![deny(missing_docs)]
pub(crate) mod api;
pub(crate) mod deserialization;
pub(crate) mod schema;
pub(crate) mod serialization;
mod type_support;

#[cfg(test)]
mod test;

#[allow(deprecated)]
pub use api::{
    deserialize_from_array, deserialize_from_arrays, serialize_into_array, serialize_into_arrays,
    serialize_into_field, serialize_into_fields, ArrayBuilder, ArraysBuilder,
};
