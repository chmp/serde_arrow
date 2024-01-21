//! A serialization implementation without the event model

pub mod array_builder;
pub mod bool_builder;
pub mod float_builder;
pub mod int_builder;
pub mod list_builder;
pub mod map_builder;
pub mod null_builder;
pub mod struct_builder;
pub mod utf8_builder;
pub mod utils;

// #[cfg(test)]
// mod test;

pub use array_builder::ArrayBuilder;
