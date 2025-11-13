//! A serialization implementation without the event model

pub mod array_builder;
pub mod binary_builder;
pub mod bool_builder;
pub mod date_builder;
pub mod decimal_builder;
pub mod dictionary_utf8_builder;
pub mod duration_builder;
pub mod fixed_size_binary_builder;
pub mod fixed_size_list_builder;
pub mod float_builder;
pub mod int_builder;
pub mod list_builder;
pub mod map_builder;
pub mod null_builder;
pub mod outer_sequence_builder;
pub mod struct_builder;
pub mod time_builder;
pub mod timestamp_builder;
pub mod union_builder;
pub mod unknown_variant_builder;
pub mod utf8_builder;
pub mod utils;

// #[cfg(test)]
// mod test;
pub use array_builder::ArrayBuilder;
pub use outer_sequence_builder::OuterSequenceBuilder;
