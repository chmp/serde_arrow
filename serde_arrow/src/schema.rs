//! The mapping between Rust and Arrow types
//!
//! To convert between Rust objects and Arrow types, `serde_arrows` requires
//! schema information as a list of Arrow fields with additional meta data. See
//! [`SchemaLike`] for details on how to specify the schema.
//!
//! The default mapping of Rust types to [Arrow types][arrow-types] is as follows:
//!
//! [arrow-types]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
//!
//! - `()`: `Null`
//! - `bool`: `Boolean`
//! - `u8`, .., `u64`, `i8`, .., `i64`: `UInt8`, .., `Uint64`, `Int8`, ..
//!   `UInt64`
//! - Floats: floats are directly mapped (`f32` -> `Float32`)
//! - Strings: `LargeUtf8` with i64 offsets
//! - Sequences: `LargeList` with i64 offsets
//! - Structs / Map / Tuples: `Struct` type
//! - Enums: dense Unions. Each variant is mapped to a separate field. Its type
//!   depends on the union type: Field-less variants are mapped to `NULL`. New
//!   type variants are mapped according to their inner type. Other variant
//!   types are mapped to struct types.
//!
//! All customization of the types happens by including a suitable [`Strategy`] in
//! the metadata of the fields. For example, to let `serde_arrow` handle date
//! time objects that are serialized to strings (chrono's default), use
//!
//! ```rust
//! # #[cfg(feature="has_arrow2")]
//! # fn main() {
//! # use arrow2::datatypes::{DataType, Field};
//! # use serde_arrow::schema::{STRATEGY_KEY, Strategy};
//! # let mut field = Field::new("my_field", DataType::Null, false);
//! field.data_type = DataType::Date64;
//! field.metadata = Strategy::UtcStrAsDate64.into();
//! # }
//! # #[cfg(not(feature="has_arrow2"))]
//! # fn main() {}
//! ```
pub use crate::internal::{
    schema::{SchemaLike, SerdeArrowSchema, Strategy, STRATEGY_KEY},
    tracing::TracingOptions,
};

/// Renamed to [`SerdeArrowSchema`]
#[deprecated = "serde_arrow::schema::Schema is deprecated. Use serde_arrow::schema::SerdeArrowSchema instead"]
pub type Schema = SerdeArrowSchema;
