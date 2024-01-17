//! A serialization implementation without the event model

pub mod array_builder;
pub mod i8_builder;
pub mod list_builder;
pub mod not_implemented;
pub mod struct_builder;

#[cfg(test)]
mod test;
