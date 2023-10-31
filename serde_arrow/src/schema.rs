//! The mapping between Rust and Arrow types
//!
//! To convert between Rust objects and Arrow types, `serde_arrows` requires
//! schema information as a list of Arrow fields with additional meta data. See
//! [SerdeArrowSchema] for details how to specify the schema.
//!
//! The default mapping of Rust types to Arrow types is as follows:
//!
//! - Strings: `LargeUtf8`, i.e., i64 offsets
//! - Lists: `LargeList`, i.e., i64 offsets
//! - Strings with dictionary encoding: `UInt32` keys and `LargeUtf8` values
//!
//! All customization of the types happens by including a suitable [Strategy] in
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
    schema::{SerdeArrowSchema, Strategy, STRATEGY_KEY},
    tracing::TracingOptions,
};

/// Type alias for SerdeArrowSchema for backwards compatibility
#[deprecated = "serde_arrow::schema::Schema is deprecated. Use serde_arrow::schema::SerdeArrowSchema instead"]
pub type Schema = SerdeArrowSchema;
