pub mod array_deserializer;
pub mod bool_deserializer;
pub mod construction;
pub mod date32_deserializer;
pub mod date64_deserializer;
pub mod decimal_deserializer;
pub mod dictionary_deserializer;
pub mod enum_deserializer;
pub mod enums_as_string_impl;
pub mod float_deserializer;
pub mod float_impls;
pub mod integer_deserializer;
pub mod integer_impls;
pub mod list_deserializer;
pub mod map_deserializer;
pub mod null_deserializer;
pub mod outer_sequence_deserializer;
pub mod simple_deserializer;
pub mod string_deserializer;
pub mod struct_deserializer;
pub mod time_deserializer;
pub mod utils;

#[cfg(test)]
mod test;
